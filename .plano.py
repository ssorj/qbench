from plano import *

@command
def run_(host=None, port=None, workers=10, duration=5):
    build()

    if host is None and port is None:
        with start(f"qbench-server localhost 55155 {workers}"):
            run(f"python python/main.py localhost 55155 {workers} {duration}")
    else:
        run(f"python python/main.py {host} {port} {workers} {duration}")

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
