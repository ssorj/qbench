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

@command
def run_(host=None, port=None, client_workers=10, server_workers=10, duration=10):
    if host is None and port is None:
        with start(f"$QBENCH_HOME/c/qbench-server localhost 55155 {server_workers}"):
            run(f"python $QBENCH_HOME/python/main.py client localhost 55155 {client_workers} {duration}")
    else:
        run(f"python python/main.py client {host} {port} {workers} {duration}")