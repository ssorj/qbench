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
#include <proton/message.h>
#include <stdio.h>
#include <time.h>

static const int CREDIT_WINDOW = 100;
static const int SEND_BATCH_SIZE = 10;
static const int BODY_SIZE = 100;

static void fail(char* message) {
    fprintf(stderr, "FAILED: %s\n", message);
    abort();
}

static void error(char* message) {
    fprintf(stderr, "ERROR: %s\n", message);
}

static void info(char* message) {
    printf("%s\n", message);
}

static int64_t time_micros() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
}

// XXX sleep_micros

static bool delivery_is_complete(pn_delivery_t* delivery) {
    assert(delivery);
    return pn_delivery_readable(delivery) && !pn_delivery_partial(delivery);
}

static ssize_t message_encode(pn_message_t* message, char* buffer, size_t size) {
    assert(message);
    assert(buffer);

    size_t inout_size = size;
    int err = pn_message_encode(message, buffer, &inout_size);

    if (err) return err;

    return inout_size;
}

static int message_send(pn_message_t* message, pn_link_t* sender, pn_rwbytes_t* buffer) {
    assert(message);
    assert(sender);
    assert(buffer); // API: Make this optional

    ssize_t ret;

    while (true) {
        ret = message_encode(message, buffer->start, buffer->size);

        if (ret == PN_OVERFLOW) {
            buffer->size *= 2;
            buffer->start = (char*) realloc(buffer->start, buffer->size);

            if (!buffer->start) return PN_OUT_OF_MEMORY;

            continue;
        }

        if (ret < 0) return ret;

        break;
    }

    // API: I want pn_delivery to take a NULL for the delivery tag.
    // Want it to generate the tag for me, based on the delivery ID,
    // which it is already generating.
    pn_delivery_t* delivery = pn_delivery(sender, pn_bytes_null);

    ret = pn_link_send(sender, buffer->start, ret);
    if (ret < 0) return ret;

    pn_link_advance(sender);

    return 0;
}

static int message_receive(pn_message_t* message, pn_link_t* receiver, pn_rwbytes_t* buffer) {
    assert(message);
    assert(receiver);
    assert(buffer); // API: Make this optional

    pn_delivery_t* delivery = pn_link_current(receiver);
    ssize_t size = pn_delivery_pending(delivery);
    ssize_t ret;

    if (buffer->size < size) {
        buffer->start = realloc(buffer->start, size);
        buffer->size = size;
    }

    ret = pn_link_recv(receiver, buffer->start, size);
    if (ret < 0) return ret;
    if (ret != size) return PN_ERR;

    ret = pn_message_decode(message, buffer->start, size);
    if (ret) return ret;

    pn_link_advance(receiver);

    return 0;
}

static void check_condition(pn_event_t* event, pn_condition_t* condition) {
    if (pn_condition_is_set(condition)) {
        fprintf(stderr, "ERROR: %s: %s: %s",
                pn_event_type_name(pn_event_type(event)),
                pn_condition_get_name(condition),
                pn_condition_get_description(condition));
    }
}
