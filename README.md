# Qbench

<!-- [![main](https://github.com/ssorj/qbench/actions/workflows/main.yaml/badge.svg)](https://github.com/ssorj/qbench/actions/workflows/main.yaml) -->

Qbench is an AMQP messaging benchmarking tool.

<!-- XXX Show the output -->

## Installation

Install required dependencies:

    sudo dnf -y install gcc python python-pandas qpid-proton-c-devel systools

Install Qbench itself.  The resulting executable is at
`~/.local/bin/qbench`.

    cd qbench
    ./plano install

### Building against locally installed libraries

To alter the GCC library and header search paths, use the
`LIBRARY_PATH`, `C_INCLUDE_PATH`, and`CPLUS_INCLUDE_PATH` environment
variables.

    export LIBRARY_PATH=$HOME/.local/lib64
    export C_INCLUDE_PATH=$HOME/.local/include
    ./plano clean,install

<!-- XXX qbench check -->

## Commands and options

### qbench run

~~~
usage: qbench run [-h] [--connections COUNT]
                  [--duration SECONDS] [--rate RATE]
                  [--body-size BYTES] [--output DIR]
                  [--workers COUNT]

Run the benchmark (client and server)

options:
  -h, --help           Show this help message and exit
  --connections COUNT  The number of concurrent client
                       connections (the default is a set of 1,
                       10, and 100)
  --duration SECONDS   The execution time in seconds (default
                       10)
  --rate RATE          Send RATE requests per second per
                       connection (0 means unlimited) (default
                       10000)
  --body-size BYTES    The message body size in bytes (default
                       100)
  --output DIR         Save output files to DIR (default
                       '/tmp/plano-z0xno1ln')
  --workers COUNT      The number of worker threads (default 4)
~~~

### qbench server

### qbench client

## Examples

### Running Qbench on one host

### Running Qbench across distinct hosts
