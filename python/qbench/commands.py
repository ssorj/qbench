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

@command
def run_(host=None, port=None, client_workers=10, server_workers=10, duration=10):
    runner = Runner(duration=duration)

    results = {
        "1": runner.run(1),
        "10": runner.run(10),
        "100": runner.run(100),
    }

    pprint(results)

    # print(read(join(output_dir, "summary.json")))

    # runner.report()
