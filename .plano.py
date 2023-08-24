from plano import *

@command
def run_():
    build()

    with start("qbench-server localhost 5672 10"):
        with working_env(BENCHDOG_DURATION=5, BENCHDOG_ITERATIONS=1):
            run("python python/main.py")

@command
def build():
    clean()
    run("gcc c/client.c -o qbench-client -O2 -g -std=c99 -fno-omit-frame-pointer -lqpid-proton-core -lqpid-proton-proactor")
    run("gcc c/server.c -o qbench-server -O2 -g -std=c99 -fno-omit-frame-pointer -lqpid-proton-core -lqpid-proton-proactor")

@command
def clean():
    remove(find(".", "__pycache__"))
    remove("qbench-client")
    remove("qbench-server")
    remove(list_dir(".", "qbench.log*"))
