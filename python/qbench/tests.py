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
from . import commands

standard_options = [
    "--duration", "1",
    # "--warmup", "0",
    # "--jobs", "1",
    # "--cpu-limit", "0"
]

# def perf_enabled():
#     try:
#         commands.check_perf()
#     except:
#         return False

#     return True

def run_command(command, *options):
    with working_dir():
        PlanoCommand(commands).main([command] + standard_options + list(options))

# def run_workload(workload):
#     with working_dir():
#         PlanoCommand(commands).main(["run", "--workload", workload, "--relay", "none"] + standard_options)

@test
def command_executable():
    run("qbench --init-only run")

@test
def command_options():
    PlanoCommand(commands).main(["--help"])

@test
def server():
    run_command("server", "--duration", "1")

@test
def client():
    config = Namespace(workers=1)
    runner = Runner(config)

    with runner.start_server("localhost", 55672):
        run_command("client", "--rate", "100")

    with runner.start_server("localhost", 55672):
        run_command("client", "--rate", "100", "--jobs", "1")

@test
def run_():
    run_command("run", "--rate", "100")
    run_command("run", "--rate", "100", "--jobs", "1")
