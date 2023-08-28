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
#include <string.h>
#include <inttypes.h>
#include <unistd.h>

#include "common.h"

typedef struct worker {
    pn_proactor_t* proactor;
    pn_message_t* request_message;
    pn_message_t* response_message;
    pn_rwbytes_t message_buffer;
    pn_bytes_t body_bytes;
    FILE* log_file;
    int id;
} worker_t;

typedef struct connection_context {
    pn_link_t* sender;
    int64_t start_time;
    uint32_t send_count;
    int id;
} connection_context_t;

static int worker_send_request(worker_t* worker, pn_link_t* sender) {
    pn_session_t* session = pn_link_session(sender);
    pn_connection_t* connection = pn_session_connection(session);
    connection_context_t* context = pn_connection_get_context(connection);

    pn_msgid_t id = (pn_msgid_t) {
        .type = PN_ULONG,
        .u.as_ulong = context->send_count,
    };

    pn_message_set_id(worker->request_message, id);
    pn_message_set_address(worker->request_message, "responses");

    pn_data_t* properties = pn_message_properties(worker->request_message);
    pn_data_clear(properties);
    pn_data_put_map(properties);
    pn_data_enter(properties);
    pn_data_put_string(properties, pn_bytes(sizeof("send-time"), "send-time"));
    pn_data_put_long(properties, time_micros());
    pn_data_exit(properties);

    pn_data_t* body = pn_message_body(worker->request_message);
    pn_data_put_binary(body, worker->body_bytes);

    int err = message_send(worker->request_message, sender, &worker->message_buffer);
    if (err) return err;

    context->send_count += 1;

    return 0;
}

static int worker_receive_response(worker_t* worker, pn_delivery_t* delivery) {
    pn_link_t* receiver = pn_delivery_link(delivery);
    int err;

    err = message_receive(worker->response_message, receiver, &worker->message_buffer);
    if (err) return err;

    pn_data_t* properties = pn_message_properties(worker->response_message);
    pn_data_rewind(properties);
    pn_data_next(properties);
    pn_data_get_map(properties);
    pn_data_enter(properties);
    pn_data_next(properties);
    pn_bytes_t key = pn_data_get_string(properties);

    if (key.size != sizeof("send-time") || memcmp(key.start, "send-time", key.size) != 0) {
        return PN_ERR;
    }

    pn_data_next(properties);
    int64_t send_time = pn_data_get_long(properties);
    pn_data_exit(properties);

    int64_t receive_time = time_micros();
    int64_t duration = receive_time - send_time;

    // fprintf(worker->log_file, "%" PRId64 ",%" PRId64 "\n", send_time, duration);

    pn_session_t* session = pn_link_session(receiver);
    pn_connection_t* connection = pn_session_connection(session);
    connection_context_t* context = pn_connection_get_context(connection);

    size_t outgoing_bytes = pn_session_outgoing_bytes(session);
    size_t incoming_bytes = pn_session_incoming_bytes(session);

    int sender_credit = pn_link_credit(context->sender);
    int sender_queued = pn_link_queued(context->sender);
    int sender_unsettled = pn_link_unsettled(context->sender);

    int receiver_credit = pn_link_credit(receiver);
    int receiver_queued = pn_link_queued(receiver);
    int receiver_unsettled = pn_link_unsettled(receiver);

    fprintf(worker->log_file,
            "%" PRId64 ",%" PRId64 ",%" PRId64 ",%d,%d,%d,%" PRId64 ",%d,%d,%d\n",
            send_time, duration,
            outgoing_bytes, sender_credit, sender_queued, sender_unsettled,
            incoming_bytes, receiver_credit, receiver_queued, receiver_unsettled);

    pn_delivery_update(delivery, PN_ACCEPTED);
    pn_delivery_settle(delivery);

    pn_link_flow(receiver, CREDIT_WINDOW - pn_link_credit(receiver));

    return 0;
}

static int worker_handle_event(worker_t* worker, pn_event_t* event, bool* running) {
    switch (pn_event_type(event)) {
    case PN_CONNECTION_INIT: {
        pn_connection_t* connection = pn_event_connection(event);
        pn_session_t* session = pn_session(connection);

        pn_connection_set_container(connection, "qbench-client");
        pn_session_open(session);

        pn_link_t* sender = pn_sender(session, "requests");
        pn_terminus_t* sender_target = pn_link_target(sender);

        pn_terminus_set_address(sender_target, "requests");
        pn_link_open(sender);

        pn_link_t* receiver = pn_receiver(session, "responses");
        pn_terminus_t* receiver_target = pn_link_target(receiver);

        pn_terminus_set_address(receiver_target, "responses");
        pn_link_open(receiver);
        pn_link_flow(receiver, CREDIT_WINDOW);

        connection_context_t* context = pn_connection_get_context(connection);

        context->sender = sender;
        context->start_time = time_micros();

        break;
    }
    case PN_CONNECTION_WAKE: {
        pn_connection_t* connection = pn_event_connection(event);
        connection_context_t* context = pn_connection_get_context(connection);

        if (!context->sender) break;

        int64_t current_time = time_micros();
        int64_t start_time = context->start_time;
        int64_t duration = current_time - start_time; // In micros

        int64_t desired_send_count = duration / 100;
        int64_t actual_send_count = context->send_count;
        int64_t send_delta = desired_send_count - actual_send_count;

        // printf("w=%d d=%d dsc=%d asc=%d sd=%d\n", worker->id, duration, desired_send_count, actual_send_count, send_delta);

        if (send_delta < 0) fail("Send delta is negative");

        int credit = pn_link_credit(context->sender);

        for (int i = 0; i < send_delta && i < credit; i++) {
            int err = worker_send_request(worker, context->sender);
            if (err) return err;
        }

        break;
    }
    // case PN_LINK_FLOW: {
    //     pn_link_t* sender = pn_event_link(event);

    //     if (pn_link_is_receiver(sender)) fail("Unexpected receiver");

    //     pn_connection_write_flush(pn_event_connection(event));

    //     break;
    // }
    case PN_DELIVERY: {
        pn_link_t* link = pn_event_link(event);

        if (pn_link_is_receiver(link)) {
            pn_delivery_t* delivery = pn_event_delivery(event);
            int err;

            if (delivery_is_complete(delivery)) {
                err = worker_receive_response(worker, delivery);
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

        check_condition(event, pn_connection_remote_condition(connection));
        pn_connection_close(connection);

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
    char* body_bytes = (char*) malloc(BODY_SIZE);

    memset(body_bytes, 'x', BODY_SIZE);

    *worker = (worker_t) {
        .id = id,
        .proactor = proactor,
        .request_message = pn_message(),
        .response_message = pn_message(),
        .message_buffer = pn_rwbytes(64, malloc(64)),
        .body_bytes = pn_bytes(BODY_SIZE, body_bytes),
    };
}

static void worker_free(worker_t* worker) {
    pn_message_free(worker->request_message);
    pn_message_free(worker->response_message);

    free(worker->message_buffer.start);
    free((char*) worker->body_bytes.start);
}

static connection_context_t* connection_context_alloc(int id) {
    connection_context_t* context = malloc(sizeof(connection_context_t));

    *context = (connection_context_t) {
        .id = id,
    };

    return context;
}

static void* worker_run(void* data) {
    worker_t* worker = (worker_t*) data;
    int err;

    char log_file_name[256];
    snprintf(log_file_name, sizeof(log_file_name), "qbench.log.%d", worker->id);

    worker->log_file = fopen(log_file_name, "w");
    if (!worker->log_file) fail("fopen");

    bool running = true;

    while (running) {
        pn_event_batch_t* batch = pn_proactor_wait(worker->proactor);
        pn_event_t* event;

        while ((event = pn_event_batch_next(batch))) {
            err = worker_handle_event(worker, event, &running);
            if (err) error("Error handling event");
        }

        pn_proactor_done(worker->proactor, batch);
    }

    err = fclose(worker->log_file);
    if (err) fail("Error closing file");

    return NULL;
}

static pn_proactor_t* proactor = NULL;

static void signal_handler(int signum) {
    pn_proactor_interrupt(proactor);
}

int main(size_t argc, char** argv) {
    if (argc != 6) {
        info("Usage: qbench-client HOST PORT WORKERS DURATION JOBS");
        return 1;
    }

    char* host = argv[1];
    char* port = argv[2];
    int worker_count = atoi(argv[3]);
    int duration = atoi(argv[4]);
    int job_count = atoi(argv[5]);

    proactor = pn_proactor();
    worker_t workers[worker_count];
    pthread_t worker_threads[worker_count];
    pn_connection_t* connections[job_count];

    signal(SIGINT, signal_handler);
    signal(SIGQUIT, signal_handler);
    signal(SIGTERM, signal_handler);

    for (int i = 0; i < worker_count; i++) {
        worker_init(&workers[i], i, proactor);
        pthread_create(&worker_threads[i], NULL, &worker_run, &workers[i]);
    }

    for (int i = 0; i < job_count; i++) {
        connections[i] = pn_connection();
        char address[256];

        pn_connection_set_context(connections[i], connection_context_alloc(i));

        pn_proactor_addr(address, sizeof(address), host, port);
        pn_proactor_connect2(proactor, connections[i], NULL, address);
    }

    info("Client started");

    int64_t start_time = time_micros();
    int64_t end_time = start_time + (duration * 1000000);

    while (time_micros() < end_time) {
        for (int i = 0; i < job_count; i++) {
            pn_connection_wake(connections[i]);
        }

        sleep_micros(1000);
    }

    pn_proactor_interrupt(proactor);

    for (int i = 0; i < worker_count; i++) {
        pthread_join(worker_threads[i], NULL);
        worker_free(&workers[i]);
    }

    for (int i = 0; i < job_count; i++) {
        connection_context_t* context = pn_connection_get_context(connections[i]);
        free(context);
    }

    pn_proactor_free(proactor);

    info("Client stopped");
}
