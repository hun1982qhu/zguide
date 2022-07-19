#!/usr/bin/env python

"""
synopsis:
    Weather update client.  Run clients in parallel.
    Connects SUB socket to tcp://localhost:5556
    Collects weather updates and finds avg temp in zipcode
    Runs multiple requestors in parallel.
    This version uses:
        - the async keyword instead of the coroutine decorator
        - the await keyword instead of yield
    Modified for async/ioloop: Dave Kuhlman <dkuhlman(at)davekuhlman(dot)org>
usage:
    python wuclient.py zipcode1 zipcode2 ...
notes:
    zipcoden should be  in the range 10000 - 10009.
"""

import sys
from functools import partial
import zmq
from zmq.eventloop.future import Context
from zmq.eventloop.ioloop import IOLoop
from tornado import gen

SERVER_ADDRESS = "tcp://localhost:5556"


async def run_client(context, zipcode):
    #  Socket to talk to server
    socket = context.socket(zmq.SUB)
    socket.connect(SERVER_ADDRESS)
    print(f'Collecting updates from weather server for zipcode: {zipcode}')
    socket.setsockopt_string(zmq.SUBSCRIBE, zipcode)
    # Process 5 updates
    total_temp = 0
    for update_nbr in range(5):
        string = await socket.recv()
        string = string.decode('utf-8')
        print(f'I: received -- string: "{string}"')
        zipcode, temperature, relhumidity = string.split()
        total_temp += int(temperature)
    result = "Average temperature for zipcode '%s' was %dF" % (
        zipcode, total_temp / update_nbr)
    raise gen.Return(result)


async def run_client_parallel(context, zipcodes):
    # Must use convert_yielded because async/await (native) coroutines
    # are not as versitile as a yield-based coroutine.
    # See the Tornado user guide.
    results = await gen.convert_yielded(
        [run_client(context, zipcode) for zipcode in zipcodes])
    for result in results:
        print(result)


async def run(loop, zipcodes):
    context = Context()
    await run_client_parallel(context, zipcodes)


def main():
    args = sys.argv[1:]
    if len(args) < 1:
        sys.exit(__doc__)
    zipcodes = args
    print('Running async/await version.')
    try:
        loop = IOLoop.current()
        loop.run_sync(partial(run, loop, zipcodes))
    except KeyboardInterrupt:
        print('\nFinished (interrupted)')


if __name__ == '__main__':
    #import pdb; pdb.set_trace()
    main()
