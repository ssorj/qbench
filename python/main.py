#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from plano import *

import json as _json
import pandas as _pandas

def print_config(config):
    print()
    print("## Configuration")
    print()

    print(f"Host:            {config.host}")
    print(f"Port:            {config.port}")
    print(f"Workers:         {config.workers}")
    print(f"Duration:        {config.duration} {plural('second', config.duration)}")

def print_data(data):
    print()
    print("## Data")
    print()

    print(_json.dumps(data, indent=2))

def run_qbench_client(config, jobs):
    remove(list_dir(".", "qbench.log*"), quiet=True)

    args = [
        # "taskset", "--cpu-list", "0-7",
        "pidstat", "2", "--human", "-l", "-t", "-e",
        "./qbench-client",
        str(config.host),
        str(config.port),
        str(config.workers),
        str(config.duration),
        str(jobs),
        str(100), # Body size
        str(10_000), # Target rate
    ]

    run(args)

    data = process_qbench_logs()
    data["jobs"] = jobs

    return data

def process_qbench_logs():
    run("cat qbench.log.* > qbench.log", shell=True)
    run("rm qbench.log.*", shell=True)

    if get_file_size("qbench.log") == 0:
        raise Exception("No data in logs")

    data = _pandas.read_csv("qbench.log", header=None)

    start_time = data[0].min() / 1_000_000
    end = data.loc[data[0].idxmax()]
    end_time = (end[0] + end[1]) / 1_000_000

    duration = end_time - start_time
    operations = len(data)

    lat_average = data[1].mean() / 1000
    lat_p50 = data[1].quantile(0.5) / 1000
    lat_p99 = data[1].quantile(0.99) / 1000

    sob_average = data[2].mean()
    sob_p50 = int(data[2].quantile(0.5))
    sob_p99 = int(data[2].quantile(0.99))

    sc_average = data[3].mean()
    sc_p50 = int(data[3].quantile(0.5))
    sc_p99 = int(data[3].quantile(0.99))

    sq_average = data[4].mean()
    sq_p50 = int(data[4].quantile(0.5))
    sq_p99 = int(data[4].quantile(0.99))

    su_average = data[5].mean()
    su_p50 = int(data[5].quantile(0.5))
    su_p99 = int(data[5].quantile(0.99))

    sib_average = data[6].mean()
    sib_p50 = int(data[6].quantile(0.5))
    sib_p99 = int(data[6].quantile(0.99))

    rc_average = data[7].mean()
    rc_p50 = int(data[7].quantile(0.5))
    rc_p99 = int(data[7].quantile(0.99))

    rq_average = data[8].mean()
    rq_p50 = int(data[8].quantile(0.5))
    rq_p99 = int(data[8].quantile(0.99))

    ru_average = data[9].mean()
    ru_p50 = int(data[9].quantile(0.5))
    ru_p99 = int(data[9].quantile(0.99))

    data = {
        "duration": round(duration, 3),
        "operations": operations,
        "latency": {
            "average": round(lat_average, 3),
            "p50": round(lat_p50, 3),
            "p99": round(lat_p99, 3),
        },
        "session_outgoing_bytes": {
            "average": round(sob_average, 1),
            "p50": sob_p50,
            "p99": sob_p99,
        },
        "sender_credit": {
            "average": round(sc_average, 1),
            "p50": sc_p50,
            "p99": sc_p99,
        },
        "sender_queued": {
            "average": round(sq_average, 1),
            "p50": sq_p50,
            "p99": sq_p99,
        },
        "sender_unsettled": {
            "average": round(su_average, 1),
            "p50": su_p50,
            "p99": su_p99,
        },
        "session_incoming_bytes": {
            "average": round(sib_average, 1),
            "p50": sib_p50,
            "p99": sib_p99,
        },
        "receiver_credit": {
            "average": round(rc_average, 1),
            "p50": rc_p50,
            "p99": rc_p99,
        },
        "receiver_queued": {
            "average": round(rq_average, 1),
            "p50": rq_p50,
            "p99": rq_p99,
        },
        "receiver_unsettled": {
            "average": round(ru_average, 1),
            "p50": ru_p50,
            "p99": ru_p99,
        },
    }

    return data

def run_scenario(config, jobs):
    sleep(min((2, config.duration)))
    return run_qbench_client(config, jobs)

def report(config, data):
    print_config(config)
    # XXX Print location of data
    # print_data(data)

    results = data.values()

    print()
    print("## Results")
    print()

    columns = "{:>8}  {:>6}  {:>20}  {:>14}  {:>14}  {:>14}"

    print(columns.format("SCENARIO", "JOBS", "THROUGHPUT", "LATENCY AVG", "LATENCY P50", "LATENCY P99"))

    for scenario, data in enumerate(results, 1):
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(scenario,
                             data["jobs"],
                             "{:,.1f} ops/s".format(throughput),
                             "{:,.3f} ms".format(latency["average"]),
                             "{:,.3f} ms".format(latency["p50"]),
                             "{:,.3f} ms".format(latency["p99"]),
              ))

    print()
    print("## Sender metrics (P50/P99)")
    print()

    print(columns.format("SCENARIO", "JOBS", "OUTGOING BYTES", "S CREDIT", "S QUEUED", "S UNSETTLED"))

    for scenario, data in enumerate(results, 1):
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(scenario,
                             data["jobs"],
                             "{:,.0f}/{:,.0f}".format(data["session_outgoing_bytes"]["p50"], data["session_outgoing_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_credit"]["p50"], data["sender_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_queued"]["p50"], data["sender_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_unsettled"]["p50"], data["sender_unsettled"]["p99"]),
              ))

    print()
    print("## Receiver metrics (P50/P99)")
    print()

    print(columns.format("SCENARIO", "JOBS", "INCOMING BYTES", "R CREDIT", "R QUEUED", "R UNSETTLED"))

    for scenario, data in enumerate(results, 1):
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(scenario,
                             data["jobs"],
                             "{:,.0f}/{:,.0f}".format(data["session_incoming_bytes"]["p50"], data["session_incoming_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_credit"]["p50"], data["receiver_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_queued"]["p50"], data["receiver_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_unsettled"]["p50"], data["receiver_unsettled"]["p99"]),
              ))

    print()

def run_client():
    config = Namespace(host=ARGS[2], port=ARGS[3], workers=int(ARGS[4]), duration=int(ARGS[5]))

    await_port(config.port, host=config.host)

    data = {
        1: run_scenario(config, 1),
        2: run_scenario(config, 10),
        3: run_scenario(config, 100),
    }

    report(config, data)

def run_server():
    config = Namespace(host=ARGS[2], port=ARGS[3], workers=int(ARGS[4]))

    args = [
        # "taskset", "--cpu-list", "0-7",
        # "pidstat", "2", "--human", "-l", "-t", "-e",
        "./qbench-server",
        str(config.host),
        str(config.port),
        str(config.workers),
    ]

    run(args)

def main():
    role = ARGS[1]

    if role == "client":
        run_client()
    elif role == "server":
        run_server()
    else:
        fail(f"Unknown role: {role}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
