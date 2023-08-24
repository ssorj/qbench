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

import json as _json
import pandas as _pandas

from benchdog import *

def run_qbench(config, jobs):
    remove(list_dir(".", "qbench.log*"), quiet=True)

    args = [
        # "taskset", "--cpu-list", "0-7",
        "./qbench-client",
        str(config.host),
        str(config.port),
        str(jobs),
        str(jobs),
        str(config.duration),
    ]

    run(args)

    data = process_qbench_logs()
    data["jobs"] = jobs

    return data

def process_qbench_logs():
    run("cat qbench.log.* > qbench.log", shell=True)

    if get_file_size("qbench.log") == 0:
        raise Exception("No data in logs")

    data = _pandas.read_csv("qbench.log", header=None)

    start_time = data[0].min() / 1_000_000
    end = data.loc[data[0].idxmax()]
    end_time = (end[0] + end[1]) / 1_000_000

    duration = end_time - start_time
    operations = len(data)

    average = data[1].mean() / 1000
    p50 = data[1].quantile(0.5) / 1000
    p99 = data[1].quantile(0.99) / 1000

    sib_average = data[2].mean()
    sib_p50 = data[2].quantile(0.5)
    sib_p99 = data[2].quantile(0.99)

    sob_average = data[3].mean()
    sob_p50 = data[3].quantile(0.5)
    sob_p99 = data[3].quantile(0.99)

    sc_average = data[4].mean()
    sc_p50 = data[4].quantile(0.5)
    sc_p99 = data[4].quantile(0.99)

    sq_average = data[5].mean()
    sq_p50 = data[5].quantile(0.5)
    sq_p99 = data[5].quantile(0.99)

    su_average = data[6].mean()
    su_p50 = data[6].quantile(0.5)
    su_p99 = data[6].quantile(0.99)

    rc_average = data[7].mean()
    rc_p50 = data[7].quantile(0.5)
    rc_p99 = data[7].quantile(0.99)

    rq_average = data[8].mean()
    rq_p50 = data[8].quantile(0.5)
    rq_p99 = data[8].quantile(0.99)

    ru_average = data[9].mean()
    ru_p50 = data[9].quantile(0.5)
    ru_p99 = data[9].quantile(0.99)

    data = {
        "duration": round(duration, 2),
        "operations": operations,
        "latency": {
            "average": round(average, 2),
            "50": round(p50, 2),
            "99": round(p99, 2),
        },
        "session_incoming_bytes": {
            "average": round(sib_average, 2),
            "50": round(sib_p50, 2),
            "99": round(sib_p99, 2),
        },
        "session_outgoing_bytes": {
            "average": round(sob_average, 2),
            "50": round(sob_p50, 2),
            "99": round(sob_p99, 2),
        },
        "sender_credit": {
            "average": round(sc_average, 2),
            "50": sc_p50,
            "99": sc_p99,
        },
        "sender_queued": {
            "average": round(sq_average, 2),
            "50": round(sq_p50, 2),
            "99": round(sq_p99, 2),
        },
        "sender_unsettled": {
            "average": round(su_average, 2),
            "50": round(su_p50, 2),
            "99": round(su_p99, 2),
        },
        "receiver_credit": {
            "average": round(rc_average, 2),
            "50": rc_p50,
            "99": rc_p99,
        },
        "receiver_queued": {
            "average": round(rq_average, 2),
            "50": round(rq_p50, 2),
            "99": round(rq_p99, 2),
        },
        "receiver_unsettled": {
            "average": round(ru_average, 2),
            "50": round(ru_p50, 2),
            "99": round(ru_p99, 2),
        },
    }

    return data

def run_scenario(config, jobs):
    results = list()

    for i in range(config.iterations):
        sleep(min((10, config.duration)))
        results.append(run_qbench(config, jobs))

    return results

def main():
    config = load_config(default_port=5672)

    print_config(config)

    await_port(config.port, host=config.host)

    data = {
        1: run_scenario(config, 1),
        2: run_scenario(config, 10),
        3: run_scenario(config, 100),
    }

    report(config, data,
           scenario_text="Each scenario uses a single Kafka producer sending 10,000 records per second.",
           job_text="Each job is a Kafka consumer.",
           operation_text="Each operation is a Kafka record consumed.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
