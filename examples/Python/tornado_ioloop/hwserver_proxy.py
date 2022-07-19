#!/usr/bin/env python

"""
synopsis:
    Hello World server in Python.
    Expects b"Hello" from client, replies with b"World".
    Uses a "proxy" task to forward requests to a worker task.
    Modified for tornado/ioloop: Dave Kuhlman <dkuhlman(at)davekuhlman(dot)org>
usage:
    python hwserver_proxy.py
notes:
    Run this script, then use hwclient.py to send requests to it.
"""

import sys
from functools import partial
import zmq
from zmq.eventloop.future import Context, Poller
from zmq.eventloop.ioloop import IOLoop
from tornado import gen


Url = 'tcp://127.0.0.1:5555'
Backend_Url = 'tcp://127.0.0.1:5556'
Ctx = Context()


@gen.coroutine
def run_server(loop):
    print("Getting ready for hello world client.  Ctrl-C to exit.\n")
    socket = Ctx.socket(zmq.ROUTER)
    socket.bind(Url)
    backend_socket = Ctx.socket(zmq.DEALER)
    backend_socket.bind(Backend_Url)
    loop.add_callback(partial(run_proxy, socket, backend_socket))


@gen.coroutine
def run_proxy(socket_from, socket_to):
    poller = Poller()
    poller.register(socket_from, zmq.POLLIN)
    poller.register(socket_to, zmq.POLLIN)
    while True:
        events = yield poller.poll()
        events = dict(events)
        if socket_from in events:
            msg = yield socket_from.recv_multipart()
            print(f'(run_proxy) received from frontend -- msg: {msg}')
            yield socket_to.send_multipart(msg)
            print(f'(run_proxy) sent to backend -- msg: {msg}')
        elif socket_to in events:
            msg = yield socket_to.recv_multipart()
            print(f'(run_proxy) received from backend -- msg: {msg}')
            yield socket_from.send_multipart(msg)
            print(f'(run_proxy) sent to frontend -- msg: {msg}')


@gen.coroutine
def run_worker():
    print('(run_worker) worker is starting')
    socket = Ctx.socket(zmq.DEALER)
    socket.connect(Backend_Url)
    poller = Poller()
    poller.register(socket, zmq.POLLIN)
    print('(run_worker) worker is waiting')
    while True:
        #  Wait for next request from client
        part1, part2, message = yield socket.recv_multipart()
        print(f"(run_worker) received: {message}")
        #  Do some 'work'
        yield gen.sleep(1)
        #  Send reply back to client
        message = message.decode('utf-8')
        message = f'{message}, world'
        message = message.encode('utf-8')
        print(f"(run_worker) sending: {message}")
        yield socket.send_multipart([part1, part2, message])
        print(f"(run_worker) sent: {message}")


@gen.coroutine
def run(loop):
    loop.add_callback(partial(run_worker))
    yield run_server(loop)


def main():
    args = sys.argv[1:]
    if len(args) != 0:
        sys.exit(__doc__)
    try:
        loop = IOLoop.current()
        loop.run_sync(partial(run, loop))
        loop.start()
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')
        sys.exit(0)


if __name__ == '__main__':
    main()
