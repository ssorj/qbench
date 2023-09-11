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
import os as _os
import pandas as _pandas
import resource as _resource
import threading as _threading
import time as _time

class Runner:
    def __init__(self, config):
        self.config = config
        self.output_dir = make_temp_dir()

    def run(self, jobs):
        check_program("pidstat", "I can't find pidstat.  Run 'dnf install sysstat'.")

        remove(list_dir(".", "qbench.log*"), quiet=True)

        server_port = 55672

        with self.start_server("localhost", server_port) as server_proc:
            await_port(server_port)

            with self.start_client("localhost", server_port, jobs) as client_proc:
                pids = [str(x.pid) for x in (client_proc, server_proc)]

                with start(f"pidstat 2 --human -l -t -p {','.join(pids)}"):
                    with ProcessMonitor(pids[0]) as client_mon, ProcessMonitor(pids[1]) as server_mon:
                        sleep(self.config.duration)
                        # capture(pids[0], pids[1], self.duration, self.call_graph)

            wait(client_proc)

        results = self.process_output()

        results["client_resources"] = {
            "average_cpu": client_mon.get_cpu(),
            "max_rss": client_mon.get_rss(),
        }

        results["server_resources"] = {
            "average_cpu": server_mon.get_cpu(),
            "max_rss": server_mon.get_rss(),
        }

        return results

    def run_client(self, jobs):
        check_program("pidstat", "I can't find pidstat.  Run 'dnf install sysstat'.")

        remove(list_dir(".", "qbench.log*"), quiet=True)

        await_port(self.config.port, host=self.config.host)

        with self.start_client(self.config.host, self.config.port, jobs) as client_proc:
            with start(f"pidstat 2 --human -l -t -p {client_proc.pid}"):
                with ProcessMonitor(client_proc.pid) as client_mon:
                    sleep(self.config.duration)
                    # capture(pids[0], pids[1], self.duration, self.call_graph)

        results = self.process_output()

        results["client_resources"] = {
            "average_cpu": client_mon.get_cpu(),
            "max_rss": client_mon.get_rss(),
        }

        return results

    def run_server(self):
        check_program("pidstat", "I can't find pidstat.  Run 'dnf install sysstat'.")

        with self.start_server(self.config.host, self.config.port) as server_proc:
            with start(f"pidstat 2 --human -l -t -p {server_proc.pid}"):
                with ProcessMonitor(server_proc.pid) as server_mon:
                    if self.config.duration:
                        sleep(self.config.duration)
                    else:
                        while True:
                            sleep(86400)

                    # capture(pids[0], pids[1], self.duration, self.call_graph)

    def start_client(self, host, port, jobs):
        args = [
            "$QBENCH_HOME/c/qbench-client",
            host,
            str(port),
            str(self.config.workers),
            str(jobs),
            str(self.config.body_size),
            str(self.config.rate),
        ]

        return start(args)

    def start_server(self, host, port):
        args = [
            "$QBENCH_HOME/c/qbench-server",
            host,
            str(port),
            str(self.config.workers),
        ]

        return start(args)

    def process_output(self):
        notice("Processing output")

        run("cat qbench.log.* > qbench.log", shell=True)
        run("rm qbench.log.*", shell=True)

        if get_file_size("qbench.log") == 0:
            raise Exception("No data in logs")

        data = _pandas.read_csv("qbench.log", header=None, dtype="int")

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

        results = {
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

        return results

    # def print_summary(self):
    #     data = read_json(join(self.output_dir, "summary.json"))

    #     print_heading("Configuration")

    #     config = data["configuration"]

    #     props = [
    #         ["Jobs", config["jobs"]],
    #         ["Duration", format_duration(config["duration"])],
    #         ["Output dir", config["output_dir"]],
    #     ]

    #     print_properties(props)

    #     print_heading("Results")

    #     results = data["results"]

    #     props = [
    #         ["Duration", format_duration(results["duration"])],
    #     ]

    #     if "bits" in results:
    #         props += [
    #             ["Bits", format_quantity(results["bits"])],
    #             ["Bits/s", format_quantity(results["bits"] / results["duration"])],
    #         ]

    #     if "operations" in results:
    #         props += [
    #             ["Operations", format_quantity(results["operations"])],
    #             ["Operations/s", format_quantity(results["operations"] / results["duration"])],
    #         ]

    #     if "latency" in results:
    #         props += [
    #             ["Latency*", results["latency"]["average"]],
    #         ]

    #     print_properties(props)

    #     if "resources" in data:
    #         print_heading("Resources")

    #         resources = data["resources"]

    #         props = [
    #             ["Relay 1 average CPU", format_percent(resources["relay_1"]["average_cpu"])],
    #             ["Relay 1 max RSS", format_quantity(resources["relay_1"]["max_rss"], mode="binary")],
    #             ["Relay 2 average CPU", format_percent(resources["relay_2"]["average_cpu"])],
    #             ["Relay 2 max RSS", format_quantity(resources["relay_2"]["max_rss"], mode="binary")],
    #         ]

    #         print_properties(props)

_ticks_per_ms = _os.sysconf(_os.sysconf_names["SC_CLK_TCK"]) / 1000
_page_size = _resource.getpagesize()

class ProcessMonitor(_threading.Thread):
    def __init__(self, pid):
        super().__init__()

        self.pid = pid
        self.stopping = _threading.Event()
        self.samples = list()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        self.join()

    def read_cpu_and_rss(self):
        proc_file = _os.path.join("/", "proc", str(self.pid), "stat")

        with open(proc_file) as f:
            line = f.read()

        fields = line.split()

        time = _time.time()
        cpu = int(sum(map(int, fields[13:17])) / _ticks_per_ms)
        rss = int(fields[23]) * _page_size

        return time, cpu, rss

    def run(self):
        prev_time, prev_cpu, _ = self.read_cpu_and_rss()

        while not self.stopping.wait(1):
            curr_time, curr_cpu, rss = self.read_cpu_and_rss()

            period_cpu = curr_cpu - prev_cpu
            prev_cpu = curr_cpu

            period_time = curr_time - prev_time
            prev_time = curr_time

            cpu = period_cpu / (period_time * 1000)

            self.samples.append((cpu, rss))

    def stop(self):
        self.stopping.set()

    def get_cpu(self):
        if not self.samples:
            return 0

        return sum([x[0] for x in self.samples]) / len(self.samples)

    def get_rss(self):
        if not self.samples:
            return 0

        return max([x[1] for x in self.samples])
