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
    CommandParameter("connections", default=None, type=int, metavar="COUNT",
                     help="The number of concurrent client connections "
                     "(the default is a set of 10, 100, and 500)"),
    CommandParameter("duration", default=10, type=int, metavar="SECONDS",
                     help="The execution time in seconds"),
    CommandParameter("rate", default=1000, type=int,
                     help="Send RATE requests per second per connection (0 means unlimited)"),
    CommandParameter("body_size", default=100, type=int, metavar="BYTES",
                     help="The message body size in bytes"),
    CommandParameter("output", default=None, metavar="DIR",
                     help="Save output files to DIR (default is a temp dir)"),
    CommandParameter("workers", default=4, type=int, metavar="COUNT",
                     help="The number of worker threads"),
]

client_parameters = [
    CommandParameter("host", default="localhost",
                     help="Connect to HOST"),
    CommandParameter("port", default="55672",
                     help="Connect to PORT"),
] + run_parameters

server_parameters = [
    CommandParameter("host", default="localhost",
                     help="Listen for connections on HOST"),
    CommandParameter("port", default="55672",
                     help="Listen for connections on PORT"),
    CommandParameter("workers", default=4, type=int, metavar="COUNT",
                     help="The number of server worker threads"),
    CommandParameter("duration", default=None, type=int, metavar="SECONDS",
                     help="Exit after SECONDS (by default, run forever)"),
]

@command(parameters=run_parameters)
def run_(*args, **kwargs):
    """
    Run the benchmark (client and server)
    """

    if kwargs["output"] is None:
        kwargs["output"] = make_temp_dir(prefix="qbench-")

    config = Namespace(**kwargs)
    runner = Runner(config)

    summary = {
        "configuration": kwargs,
    }

    if config.connections is None:
        summary["scenarios"] = {
            10: runner.run(10),
            100: runner.run(100),
            500: runner.run(500),
        }
    else:
        summary["scenarios"] = {
            config.connections: runner.run(config.connections),
        }

    write_json(join(config.output, "summary.json"), summary)

    report(config, summary["scenarios"])

@command(parameters=client_parameters)
def client(*args, **kwargs):
    """
    Run the benchmark client
    """

    if kwargs["output"] is None:
        kwargs["output"] = make_temp_dir(prefix="qbench-")

    config = Namespace(**kwargs)
    runner = Runner(config)

    summary = {
        "configuration": kwargs,
    }

    if config.connections is None:
        summary["scenarios"] = {
            10: runner.run_client(10),
            100: runner.run_client(100),
            500: runner.run_client(500),
        }
    else:
        summary["scenarios"] = {
            config.connections: runner.run_client(config.connections),
        }

    write_json(join(config.output, "summary.json"), summary)

    report(config, summary["scenarios"])

@command(parameters=server_parameters)
def server(*args, **kwargs):
    """
    Run the benchmark server
    """

    config = Namespace(**kwargs)
    runner = Runner(config)

    runner.run_server()

def report(config, scenarios):
    print()
    print("## Configuration")
    print()

    print(f"Duration:      {config.duration:,} {plural('second', config.duration)}")
    print(f"Target rate:   {config.rate:,} {plural('request', config.rate)} per second per connection")
    print(f"Body size:     {config.body_size:,} {plural('byte', config.body_size)}")
    print(f"Output files:  {config.output}")
    print(f"Workers:       {config.workers}")

    print()
    print("## Results")
    print()

    columns = "{:>11}  {:>20}  {:>14}  {:>14}  {:>14}"

    print(columns.format("CONNECTIONS", "THROUGHPUT", "LATENCY AVG", "LATENCY P50", "LATENCY P99"))

    for connections, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(connections,
                             "{:,.1f} ops/s".format(throughput),
                             "{:,.3f} ms".format(latency["average"]),
                             "{:,.3f} ms".format(latency["p50"]),
                             "{:,.3f} ms".format(latency["p99"]),
              ))

    print()
    print("Sender metrics (P50/P99)")
    print()

    print(columns.format("CONNECTIONS", "OUTGOING BYTES", "S CREDIT", "S QUEUED", "S UNSETTLED"))

    for connections, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(connections,
                             "{:,.0f}/{:,.0f}".format(data["session_outgoing_bytes"]["p50"], data["session_outgoing_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_credit"]["p50"], data["sender_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_queued"]["p50"], data["sender_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["sender_unsettled"]["p50"], data["sender_unsettled"]["p99"]),
              ))

    print()
    print("Receiver metrics (P50/P99)")
    print()

    print(columns.format("CONNECTIONS", "INCOMING BYTES", "R CREDIT", "R QUEUED", "R UNSETTLED"))

    for connections, data in scenarios.items():
        throughput = data["operations"] / data["duration"]
        latency = data["latency"]

        print(columns.format(connections,
                             "{:,.0f}/{:,.0f}".format(data["session_incoming_bytes"]["p50"], data["session_incoming_bytes"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_credit"]["p50"], data["receiver_credit"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_queued"]["p50"], data["receiver_queued"]["p99"]),
                             "{:,.0f}/{:,.0f}".format(data["receiver_unsettled"]["p50"], data["receiver_unsettled"]["p99"]),
              ))

    print()
