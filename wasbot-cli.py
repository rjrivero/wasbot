#!/usr/bin/env python3 
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


import traceback
import sys

from tornado.gen import coroutine, Task
from tornado.iostream import StreamClosedError
from tornado.process import fork_processes
from tornado.ioloop import IOLoop

from wasbot.ipc import MsgPipe


class TestQueue(object):

    def __init__(self):
        self._pqueue, self._cqueue = MsgPipe.bidi()

    @coroutine
    def producer(self):
        print("Iniciando productor")
        messages = (
            {'key': 1, 'val': "Mensaje áñ1"},
            {'key': 2, 'val': "Mensaje 2"},
            {'key': 3, 'val': "Mensaje 3"},
            {'key': 4, 'val': "Mensaje 4"},
            {'key': 5, 'val': "Mensaje 5"},
            {'key': 6, 'val': "Mensaje 6"},
            {'key': 7, 'val': "Mensaje 7"},
        )
        for msg in messages:
            print("Sending Message %s" % str(msg))
            yield self._pqueue.send(msg)
            print("Expecting response")
            msg = yield self._pqueue.recv()
            print("Response received: %s" % str(msg))
            yield Task(IOLoop.current().call_later, 1)
        self._pqueue.close()
        sys.exit(0)

    @coroutine
    def consumer(self):
        print("Iniciando consumidor")
        try:
            while True:
                msg = yield self._cqueue.recv()
                print("Received message %s" % str(msg))
                yield self._cqueue.send("ECHO %s" % str(msg))
        except StreamClosedError:
            print("Pipe cerrado!")
        except:
            traceback.print_exc()
        self._cqueue.close()
        sys.exit(0)

    def run(self):
        pid = fork_processes(2, max_restarts=0)
        if pid == 0:
            # Producer
            IOLoop.current().run_sync(self.producer)
        elif pid == 1:
            # Consumer
            IOLoop.current().run_sync(self.consumer)
        else:
            # Parent
            pass

TestQueue().run()

