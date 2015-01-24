#!/usr/bin/env python3 
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


import fcntl
import os
import msgpack

from tornado.gen import coroutine
from tornado.concurrent import Future, chain_future
from tornado.iostream import PipeIOStream
from tornado.ioloop import IOLoop

from wasbot.tools import DLList, Serializer


class MsgQueue(object):

    """Inter-process Message Queue

    This is a message queue class that manages sending messages
    between different processes, using pipes, sockets or other
    file-like objects in **asynchronous mode**. It is intended to
    be used with `Tornado`.

    Messages can be sent (with :meth:`send`) or received (with :meth:`recv`).
    Both functions return a future that is resolved as soon as a message
    is dispatched or received, respectively.

    Serialization across the pipe or socket is performed by `msgpack`.
    The message queue automatically handles packing and unpacking.
    """

    def __init__(self, read_func, write_func, io_loop=None):
        """Build a queue attached to the given read and write functions.

        The queue receives and sends messages by reading and writing bytes
        to the underlying channel (be it a socket, a pipe, or whatever) with
        the given `read_func` and `write_func`.
        
        The `read_func` and `write_func` functions **must** be
        asynchronous functions that return a Future. See :meth:`from_stream`
        and :meth:`from_pipes` if you want to build a :class:`MsgQueue` object
        straight form a :class:`tornado.iostream.IOStream` or a
        :class:`tornado.iostream.PipeIOStream` object.
        
        :Parameters:
          - `read_func`: the byte reader "coroutine". It is invoked as
             "read_func()" when the input buffer is empty. It must return a
             :class:`tornado.concurrent.Future` with new bytes from the
             underlying transport.
          - `write_func`: the byte writer "coroutine". It is invoked as
             "write_func(bytes)" when there is any message to send. It must
             return a :class:`tornado.concurrent.Future` that resolves 
             when the message is delivered to the underlying transport.
          - `io_loop`: the :class:`tornado.ioloop.IOLoop` object.
        """
        self._rfunc  = read_func
        self._wfunc  = write_func
        self._loop   = io_loop or IOLoop.current()
        # Output management
        # I must make sure we serialize sends, because tornado.BaseIOStream
        # will dispose of the "Future" returned by a write if another write
        # is issued before the "Future" is resolved.
        self._writer = Serializer(self._loop)
        # Input management
        self._buffer = msgpack.Unpacker(use_list=False, encoding='utf-8')
        self._exc    = None
        self._rqueue = DLList()

    def send(self, obj):
        """Send the message through the underlying transport.

        Returns a :class:`tornado.concurrent.Future` that resolves when
        the message is delivered to the transport, or raises an exception
        if the transport can not dispatch the message.
        
        The Future propagates any exception that may be raised by the
        `write_func` given to the Constructor. When using ``tornado.iostream``
        streams, these exceptions may be the likes of:

        - :exc:`tornado.iostream.StreamClosedError`
        - :exc:`tornado.iostream.StreamBufferFullError`

        :Parameters:
          - `obj`: the message to be serialized and sent.
        """
        return self._writer.append(lambda: self._wfunc(
            msgpack.packb(obj, use_bin_type=True)
        ))

    def recv(self):
        """Receive a message from the underlying transport.
        
        Returns a :class:`tornado.concurrent.Future` that is resolved
        when a message is received from the transport.

        The Future propagates any exception that may be raised by the
        `read_func` given to the Constructor. When using ``tornado.iostream``
        streams, these exceptions may be the likes of:

        - :exc:`tornado.iostream.StreamClosedError`
        - :exc:`tornado.iostream.StreamBufferFullError`
        """
        future = Future()
        try:
            # If the socket is still working and the Unpacker has data,
            # then get it.
            if self._exc:
                future.set_exception(self._exc)
            else:
                future.set_result(self._buffer.unpack())
        except msgpack.OutOfData:
            # Otherwise, read a chunk of data
            self._rqueue.append(future)
            self._loop.add_future(self._rfunc(), self._feed)
        return future

    def _feed(self, future):
        """Helper function to handle buffered reads.

        Dispatches the incoming messages to the waiting readers, in order.

        :Parameters:
          - `future`: the :class:`tornado.concurrent.Future` object returned
            by the last invocation to `read_func`
        """
        queue, buf, exc = self._rqueue, self._buffer, future.exception()
        if not queue:
            return
        if exc is not None:
            # If pipe closed, notify all readers
            for reader in queue:
                reader.set_exception(exc)
            queue.clear()
            self._exc = exc
            return
        # Read chunk of data and feed it to the Unpacker
        buf.feed(future.result())
        # Take an object per each reader in the queue
        while queue:
            try:
                obj = buf.unpack()
            except msgpack.OutOfData:
                break
            else:
                reader = queue.popleft()
                reader.set_result(obj)
        # If there are readers left, schedule next read.
        if queue:
            self._loop.add_future(self._rfunc(), self._feed)

    @staticmethod
    def from_stream(stream, bufsize=1024, io_loop=None):
        """Build a MsgQueue from a :class:`tornado.iostream.IOStream`s.
        
        :Parameters:
          - `stream`: the input/output :class:`tornado.iostream.IOStream`
          - `bufsize`: amount of bytes to read at once.
          - `io_loop`: the :class:`tornado.ioloop.IOLoop` object.
        """
        return MsgQueue(
            read_func  = (lambda: stream.read_bytes(bufsize, partial=True)),
            write_func = stream.write,
            io_loop    = io_loop
        )


class MsgPipe(object):

    """MsgQueue wrapper built from a couple of unidirectonal pipes"""

    def __init__(self, infd=None, outfd=None, bufsize=1024, io_loop=None):
        """Build a MsgQueue from two file descriptors.
        
        The file descriptors are set to nonblocking and wrapped in a 
        :class:`tornado.iostream.PipeIOStream`.
        
        :Parameters:
          - `infd`: the pipe file descriptor to be used for reading, or
            ``None`` to create a new one with ``os.pipe`` (in this case,
            `outfd` must also be ``None``)
          - `outfd`: the pipe file descriptor to be used for writing, or
            ``None`` to use `infd`
          - `bufsize`: amount of bytes to read at once.
          - `io_loop`: the :class:`tornado.ioloop.IOLoop` object.
        """
        if infd is None:
            assert(outfd is None)
            infd, outfd = os.pipe()
        self._infd  = infd
        self._outfd = outfd
        # Delay initialization. As soon as the MsgQueue is created,
        # IOLoop.current() is called and we can no longer fork the process
        # with tornado.process.fork_processes.
        # This would happen even if we delay calling to IOLoop.current()
        # in MsgQueue, because of wrapping the fd in a PipeIOStream.
        # So, we store the file descriptors and wait until required.
        def lazy():
            inpipe  = MsgPipe._wrap_fd(infd)
            outpipe = inpipe if (outfd == infd) else MsgPipe._wrap_fd(outfd)
            rd_func = (lambda: inpipe.read_bytes(bufsize, partial=True))
            wr_func = outpipe.write
            queue   = MsgQueue(rd_func, wr_func, io_loop)
            self.send = queue.send
            self.recv = queue.recv
            return queue
        self.send = lambda msg: lazy().send(msg)
        self.recv = lambda: lazy().recv()

    @staticmethod
    def _wrap_fd(fd):
        """Wraps a file descriptor in a :class:`tornado.iostream.PipeIOStream`

        :Parameters:
          - `fd`: the file descriptor (integer) to wrap.
        """
        # First, set non-blocking
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        # If python 3.4 or above, set fd inheritable
        try:
            os.set_inheritable(fd, True)
        except AttributeError:
            pass
        # Then, wrap it
        return PipeIOStream(fd)

    @staticmethod
    def bidi(bufsize=1024, io_loop=None):
        """Create a couple of connected :class:`MsgPipe` objects.

        What is sent to one of the pipes is received through the other, and
        viceversa, converting the pipes in a bidirectional communication
        channel.
        
        Returns a tuple with the connected pipes. Usage:

        upstream, downstream = MsgQueue.bidi()

        :Parameters:
          - `bufsize`: amount of bytes to read at once.
          - `io_loop`: the :class:`tornado.ioloop.IOLoop` object.
        """
        # This socket pair will be for commands sent downstream: Parent
        # sends a message and child recv's it.
        down_rd, up_wr = os.pipe()
        # This socket pair will be for commands sent upstream: Child
        # sends a message and parent recv's it.
        up_rd, down_wr = os.pipe()
        upstream   = MsgPipe(up_rd, up_wr, bufsize, io_loop)
        downstream = MsgPipe(down_rd, down_wr, bufsize, io_loop)
        return (upstream, downstream)

    def close(self):
        """Close the communication channel"""
        try:
            os.close(self._outfd)
        except OSError:
            pass

