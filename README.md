# Qbench

<!-- [![main](https://github.com/ssorj/qbench/actions/workflows/main.yaml/badge.svg)](https://github.com/ssorj/qbench/actions/workflows/main.yaml) -->

Qbench is an AMQP messaging benchmarking tool.

~~~
## Configuration

Duration:      10 seconds
Target rate:   10,000 requests per second per connection
Body size:     100 bytes
Output files:  /tmp/plano-5brmejqh
Workers:       4

## Results

CONNECTIONS            THROUGHPUT     LATENCY AVG     LATENCY P50     LATENCY P99
         10       100,000.1 ops/s        0.232 ms        0.222 ms        0.440 ms
        100     1,001,011.0 ops/s        2.737 ms        0.620 ms       23.744 ms
        500     1,144,942.4 ops/s       40.607 ms       39.389 ms      135.949 ms

## Sender metrics (P50/P99)

CONNECTIONS        OUTGOING BYTES        S CREDIT        S QUEUED     S UNSETTLED
         10                   0/0         100/100             0/0           11/12
        100              0/16,900         100/100           0/100          11/200
        500         16,900/16,900           0/100         100/100         200/200

## Receiver metrics (P50/P99)

CONNECTIONS        INCOMING BYTES        R CREDIT        R QUEUED     R UNSETTLED
         10             850/1,716           99/99            5/11            6/12
        100          1,190/16,150           99/99            7/95            8/96
        500          8,330/16,660           99/99           49/98           50/99
~~~

## Installation

Install required dependencies:

    sudo dnf -y install gcc python python-pandas qpid-proton-c-devel sysstat

Install Qbench itself.  The resulting executable is at
`~/.local/bin/qbench`.

    cd qbench
    ./plano install

### Building against locally installed libraries

To alter the GCC library and header search paths, use the
`LIBRARY_PATH`, `C_INCLUDE_PATH`, and`CPLUS_INCLUDE_PATH` environment
variables.

    export LIBRARY_PATH=~/.local/lib64
    export C_INCLUDE_PATH=~/.local/include
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

<!-- ### qbench server -->

<!-- ### qbench client -->

<!-- ## Examples -->

<!-- ### Running Qbench on one host -->

<!-- ### Running Qbench across distinct hosts -->
