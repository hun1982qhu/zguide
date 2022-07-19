#!/usr/bin/env python

"""
synopsis:
    Multitasking Hello World server.  Starts multiple worker callbacks
    to handle requests.
    Modified for async/ioloop: Dave Kuhlman <dkuhlman(at)davekuhlman(dot)org>
usage:
    python mtserver.py
"""


# from __future__ import print_function
import sys
from functools import partial
import zmq
from zmq.eventloop.future import Context, Poller
from zmq.eventloop.ioloop import IOLoop
from tornado import gen


Ctx = Context()
Url_worker = "inproc://workers"
Url_client = "tcp://*:5555"


DEBUG = True


def printdbg(*args):
    if DEBUG:
        print(*args)


@gen.coroutine
def worker_routine(ident):
    """Worker routine"""
    # Socket to talk to dispatcher
    socket = Ctx.socket(zmq.REP)
    socket.connect(Url_worker)
    printdbg(f'(worker_routine) {ident} started')
    while True:
        printdbg('(worker_routine) waiting for request')
        message = yield socket.recv()
        printdbg("(worker_routine) Received message parts: %s".format(
            message))
        printdbg("(worker_routine) Received message: %s".format(message))
        # Do some 'work'
        yield gen.sleep(1)
        # Send reply back to client
        message = f'{message} world, from {ident}'.encode('utf-8')
        yield socket.send(message)
        printdbg(f'(worker_routine) sent message: {message}')


@gen.coroutine
def run(loop):
    """Server routine"""
    # Prepare our context and sockets
    # Socket to talk to clients
    clients = Ctx.socket(zmq.ROUTER)
    clients.bind(Url_client)
    workers = Ctx.socket(zmq.DEALER)
    workers.bind(Url_worker)
    # Start the workers
    # Caution: Do *not* use lambda to create the function call to the worker.
    #     lambda does not work correctly inside a for-statement.
    for idx in range(5):
        ident = f'worker {idx}'
        loop.add_callback(partial(worker_routine, ident))
    poller = Poller()
    poller.register(clients, zmq.POLLIN)
    poller.register(workers, zmq.POLLIN)
    print('mtserver ready for requests')
    while True:
        events = yield poller.poll()
        events = dict(events)
        if clients in events:
            message = yield clients.recv_multipart()
            printdbg(f'(run) received from client message_parts: {message}')
            client, empty, message = message[:3]
            printdbg(f'(run) received from client message: {message}')
            yield workers.send_multipart([client, b'', message])
            printdbg(f'(run) sent message to workers: {message}')
        elif workers in events:
            message = yield workers.recv_multipart()
            printdbg(f'(run) received from worker message_parts: {message}')
            client, empty, message = message[:3]
            printdbg(f'(run) received from worker message: {message}')
            yield clients.send_multipart([client, b'', message])
            printdbg(f'(run) sent message to clients: {message}')


def main():
    args = sys.argv[1:]
    if len(args) != 0:
        sys.exit(__doc__)
    try:
        loop = IOLoop.current()
        loop.add_callback(lambda: run(loop))
        loop.start()
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')


if __name__ == "__main__":
    main()
