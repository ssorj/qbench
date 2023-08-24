/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#define _POSIX_C_SOURCE 200112L

#include <assert.h>
#include <proton/engine.h>
#include <proton/error.h>
#include <proton/event.h>
#include <proton/listener.h>
#include <proton/message.h>
#include <proton/proactor.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "common.h"

typedef struct worker {
    pn_proactor_t* proactor;
    pn_message_t* request_message;
    pn_message_t* response_message;
    pn_rwbytes_t message_buffer;
    int id;
} worker_t;

typedef struct connection_context {
    pn_link_t* sender;
    uint32_t send_count;
} connection_context_t;

static int worker_send_response(worker_t* worker, pn_link_t* sender) {
    pn_session_t* session = pn_link_session(sender);
    pn_connection_t* connection = pn_session_connection(session);
    connection_context_t* context = pn_connection_get_context(connection);

    pn_msgid_t id = (pn_msgid_t) {
        .type = PN_ULONG,
        .u.as_ulong = context->send_count,
    };

    pn_message_set_id(worker->response_message, id);

    const char* request_reply_to = pn_message_get_reply_to(worker->request_message);
    pn_msgid_t request_id = pn_message_get_id(worker->request_message);

    pn_message_set_address(worker->response_message, request_reply_to);
    pn_message_set_correlation_id(worker->response_message, request_id);

    pn_data_t* request_properties = pn_message_properties(worker->request_message);
    pn_data_t* response_properties = pn_message_properties(worker->response_message);

    pn_data_copy(response_properties, request_properties);

    pn_data_t* request_body = pn_message_body(worker->request_message);
    pn_data_t* response_body = pn_message_body(worker->response_message);

    pn_data_copy(response_body, request_body);

    int err = message_send(worker->response_message, sender, &worker->message_buffer);
    if (err) return err;

    context->send_count += 1;

    return 0;
}

static int worker_receive_request(worker_t* worker, pn_delivery_t* delivery) {
    pn_link_t* receiver = pn_delivery_link(delivery);
    pn_session_t* session = pn_link_session(receiver);
    pn_connection_t* connection = pn_session_connection(session);
    connection_context_t* context = pn_connection_get_context(connection);

    assert(context->sender);

    int err = message_receive(worker->request_message, receiver, &worker->message_buffer);
    if (err) return err;

    worker_send_response(worker, context->sender);

    pn_delivery_update(delivery, PN_ACCEPTED);
    pn_delivery_settle(delivery);

    pn_link_flow(receiver, CREDIT_WINDOW - pn_link_credit(receiver));

    return 0;
}

static connection_context_t* connection_context_alloc() {
    connection_context_t* context = malloc(sizeof(connection_context_t));
    *context = (connection_context_t) {0};
    return context;
}

static int worker_handle_event(worker_t* worker, pn_event_t* event, bool* running) {
    switch (pn_event_type(event)) {
    case PN_LISTENER_ACCEPT: {
        pn_listener_t* listener = pn_event_listener(event);
        pn_connection_t* connection = pn_connection();

        pn_listener_accept(listener, connection);

        break;
    }
    case PN_CONNECTION_INIT: {
        pn_connection_t* connection = pn_event_connection(event);
        connection_context_t* context = connection_context_alloc();

        pn_connection_set_container(connection, "qbench-server");
        pn_connection_set_context(connection, context);

        break;
    }
    case PN_CONNECTION_REMOTE_OPEN: {
        pn_connection_open(pn_event_connection(event));
        break;
    }
    case PN_SESSION_REMOTE_OPEN: {
        pn_session_open(pn_event_session(event));
        break;
    }
    case PN_LINK_REMOTE_OPEN: {
        pn_link_t* link = pn_event_link(event);

        if (pn_link_is_receiver(link)) {
            pn_link_t* receiver = link;
            pn_terminus_t* target = pn_link_target(receiver);
            pn_terminus_t* remote_target = pn_link_remote_target(receiver);
            const char* remote_address = pn_terminus_get_address(remote_target);

            pn_terminus_set_address(target, remote_address);
            pn_link_open(receiver);
            pn_link_flow(receiver, CREDIT_WINDOW);
        } else {
            pn_link_t* sender = link;
            pn_connection_t* connection = pn_event_connection(event);
            connection_context_t* context = pn_connection_get_context(connection);

            pn_link_open(sender);

            // Save the sender for sending responses
            context->sender = sender;
        }

        break;
    }
    case PN_LINK_FLOW: {
        pn_link_t* sender = pn_event_link(event);

        if (pn_link_is_receiver(sender)) fail("Unexpected receiver");

        if (pn_link_queued(sender) >= SEND_BATCH_SIZE) {
            pn_connection_write_flush(pn_event_connection(event));
        }

        break;
    }
    case PN_DELIVERY: {
        pn_link_t* link = pn_event_link(event);

        if (pn_link_is_receiver(link)) {
            pn_delivery_t* delivery = pn_event_delivery(event);

            if (delivery_is_complete(delivery)) {
                int err = worker_receive_request(worker, delivery);
                if (err) return err;
            }
        } else {
            pn_delivery_settle(pn_event_delivery(event));
        }

        break;
    }
    case PN_LINK_REMOTE_CLOSE: {
        pn_link_t* link = pn_event_link(event);

        check_condition(event, pn_link_remote_condition(link));
        pn_link_close(link);

        break;
    }
    case PN_SESSION_REMOTE_CLOSE: {
        pn_session_t* session = pn_event_session(event);

        check_condition(event, pn_session_remote_condition(session));
        pn_session_close(session);

        break;
    }
    case PN_CONNECTION_REMOTE_CLOSE: {
        pn_connection_t* connection = pn_event_connection(event);
        connection_context_t* context = pn_connection_get_context(connection);

        check_condition(event, pn_connection_remote_condition(connection));
        pn_connection_close(connection);

        // XXX Need to free the context on abrupt connection termination as well
        // XXX Try connection_final event
        free(context);
        pn_connection_set_context(connection, NULL);

        break;
    }
    case PN_LISTENER_CLOSE: {
        info("Listener closed (port conflict?)");
        break;
    }
    case PN_PROACTOR_INTERRUPT: {
        // Interrupt the next thread
        pn_proactor_interrupt(worker->proactor);
        *running = false;
        return 0;
    }
    }

    return 0;
}

static void worker_init(worker_t* worker, int id, pn_proactor_t* proactor) {
    *worker = (worker_t) {
        .id = id,
        .proactor = proactor,
        .request_message = pn_message(),
        .response_message = pn_message(),
        .message_buffer = pn_rwbytes(64, malloc(64)),
    };
}

static void worker_free(worker_t* worker) {
    pn_message_free(worker->request_message);
    pn_message_free(worker->response_message);

    free(worker->message_buffer.start);
}

static void* worker_run(void* data) {
    worker_t* worker = (worker_t*) data;
    bool running = true;

    while (running) {
        pn_event_batch_t* batch = pn_proactor_wait(worker->proactor);
        pn_event_t* event;
        int err;

        while ((event = pn_event_batch_next(batch))) {
            err = worker_handle_event(worker, event, &running);
            if (err) error("Error handling event");
        }

        pn_proactor_done(worker->proactor, batch);
    }

    return NULL;
}

static pn_proactor_t* proactor = NULL;

static void signal_handler(int signum) {
    pn_proactor_interrupt(proactor);
}

int main(size_t argc, char** argv) {
    if (argc != 4) {
        info("Usage: qbench-server HOST PORT WORKERS");
        return 1;
    }

    char* host = argv[1];
    char* port = argv[2];
    int worker_count = atoi(argv[3]);

    proactor = pn_proactor();
    pn_listener_t* listener = pn_listener();
    worker_t workers[worker_count];
    pthread_t worker_threads[worker_count];

    signal(SIGINT, signal_handler);
    signal(SIGQUIT, signal_handler);
    signal(SIGTERM, signal_handler);

    for (int i = 0; i < worker_count; i++) {
        worker_init(&workers[i], i, proactor);
        pthread_create(&worker_threads[i], NULL, &worker_run, &workers[i]);
    }

    char address[256];
    pn_proactor_addr(address, sizeof(address), host, port);
    pn_proactor_listen(proactor, listener, address, 32);

    info("Server started");

    for (int i = 0; i < worker_count; i++) {
        pthread_join(worker_threads[i], NULL);
        worker_free(&workers[i]);
    }

    pn_proactor_free(proactor);

    info("Server stopped");
}
