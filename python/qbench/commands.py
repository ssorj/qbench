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

from .main import *

assert "QBENCH_HOME" in ENV

run_parameters = [
    CommandParameter("jobs", default=None, type=int, metavar="COUNT",
                     help="The number of concurrent client connections sending requests and receiving responses "
                     "(the default is a set of 1, 10, and 100)"),
    CommandParameter("duration", default=10, type=int, metavar="SECONDS",
                     help="The execution time in seconds"),
    CommandParameter("rate", default=10_000, type=int, metavar="REQUESTS",
                     help="The target rate for sending requests (per second per job)"),
    CommandParameter("body_size", default=100, type=int, metavar="BYTES",
                     help="The message body size in bytes"),
    CommandParameter("workers", default=4, type=int, metavar="COUNT",
                     help="The number of worker threads (per process)"),
]

client_parameters = [
    CommandParameter("host", default="localhost",
                     help="Listen for connections on HOST"),
    CommandParameter("port", default="55672",
                     help="Listen for connections on PORT"),
] + run_parameters

server_parameters = [
    CommandParameter("host", default="localhost",
                     help="Listen for connections on HOST"),
    CommandParameter("port", default="55672",
                     help="Listen for connections on PORT"),
    CommandParameter("workers", default=4, type=int, metavar="COUNT",
                     help="The number of server worker threads"),
]

@command(parameters=run_parameters)
def run_(*args, **kwargs):
    config = Namespace(**kwargs)
    runner = Runner(config)

    summary = {
        "configuration": {
            "duration": config.duration,
            "rate": config.rate,
            "body_size": config.body_size,
            "workers": config.workers,
        },
    }

    if config.jobs is None:
        summary["scenarios"] = {
            1: runner.run(1),
            10: runner.run(10),
            100: runner.run(100),
        }
    else:
        summary["scenarios"] = {
            config.jobs: runner.run(config.jobs),
        }

    pprint(summary)

    report(config, summary["scenarios"])

@command(parameters=client_parameters)
def client(*args, **kwargs):
    config = Namespace(**kwargs)
    runner = Runner(config)

    summary = {
        "configuration": {
            "duration": config.duration,
            "rate": config.rate,
            "body_size": config.body_size,
            "workers": config.workers,
        },
    }

    if config.jobs is None:
        summary["scenarios"] = {
            1: runner.run_client(1),
            10: runner.run_client(10),
            100: runner.run_client(100),
        }
    else:
        summary["scenarios"] = {
            config.jobs: runner.run(config.jobs),
        }

    pprint(summary)

    report(config, summary["scenarios"])

@command(parameters=server_parameters)
def server(*args, **kwargs):
    config = Namespace(**kwargs)
    runner = Runner(config)

    runner.run_server()

def report(config, scenarios):
    print()
    print("## Configuration")
    print()

    print(f"Duration:     {config.duration:,} {plural('second', config.duration)}")
    print(f"Target rate:  {config.rate:,} {plural('request', config.rate)} per second per job")
    print(f"Body size:    {config.body_size:,} {plural('byte', config.body_size)}")
    print(f"Workers:      {config.workers}")

    print()
    print("## Results")
    print()

    columns = "{:>4}  {:>20}  {:>14}  {:>14}  {:>14}"

    print(columns.format("JOBS", "THROUGHPUT", "LATENCY AVG", "LATENCY P50", "LATENCY P99"))

    for jobs, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(jobs,
                             "{:,.1f} ops/s".format(throughput),
                             "{:,.3f} ms".format(latency["average"]),
                             "{:,.3f} ms".format(latency["p50"]),
                             "{:,.3f} ms".format(latency["p99"]),
              ))

    print()
    print("## Sender metrics (P50/P99)")
    print()

    print(columns.format("JOBS", "OUTGOING BYTES", "S CREDIT", "S QUEUED", "S UNSETTLED"))

    for jobs, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(jobs,
                             "{:,.0f}/{:,.0f}".format(data["session_outgoing_bytes"]["p50"], data["session_outgoing_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_credit"]["p50"], data["sender_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_queued"]["p50"], data["sender_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_unsettled"]["p50"], data["sender_unsettled"]["p99"]),
              ))

    print()
    print("## Receiver metrics (P50/P99)")
    print()

    print(columns.format("JOBS", "INCOMING BYTES", "R CREDIT", "R QUEUED", "R UNSETTLED"))

    for jobs, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(jobs,
                             "{:,.0f}/{:,.0f}".format(data["session_incoming_bytes"]["p50"], data["session_incoming_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_credit"]["p50"], data["receiver_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_queued"]["p50"], data["receiver_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_unsettled"]["p50"], data["receiver_unsettled"]["p99"]),
              ))

    print()
