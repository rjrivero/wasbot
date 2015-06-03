#!/usr/bin/env python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


from tornado.ioloop import IOLoop
from tornado.concurrent import Future
from toro import Timeout, Queue


class DLList(object):

    """Double-Linked List implementation.
    
    Simple double-linked list aimed to be used as a deque. Two
    main differences with ``collections.deque``:
    
      - It is not thread-safe
      - It provides a :meth:`remove` method to remove items
        from anywhere in the queue, without search.
      - It provides a :meth:`preempt` method to push items at
        the front of the queue, for expedited processing.
    
    .. seealso:: `http://www.kunxi.org/blog/2014/05/double-linked-list-in-python/`
    """

    __slots__ = ('root', 'num')

    def __init__(self):
        root      = []
        root[:]   = [root, root, None]
        self.root = root
        self.num  = 0

    def preempt(self, item):
        """Insert the item at the head of the queue.

        Returns the list node, that can be used to remove the
        item anytime by passing it to :meth:`remove`

        :Parameters:
          - `item`: the item to push in the head.
        """
        root = self.root
        head = root[1]
        node = [root, head, item]
        head[0] = root[1] = node
        self.num += 1
        return node

    def push(self, item):
        """Insert the item at the tail of the queue.

        Returns the list node, that can be used to remove the
        item anytime by passing it to :meth:`remove`

        :Parameters:
          - `item`: the item to push.
        """
        root = self.root
        last = root[0]
        node = [last, root, item]
        last[1] = root[0] = node
        self.num += 1
        return node

    def pop(self):
        """Remove the item at the head of the queue.
        
        Raises :exc:`toro.Queue.Empty` if empty.
        """
        if not self.num:
            raise Queue.Empty()
        return self.remove(self.root[1])

    def remove(self, node):
        """Remove the given node.

        :Parameters:
          - `node`: The node handler as returned by :meth:`push`.
            The item must not have been popped before, otherwise
            the list will become corrupted.
        """
        prev_link, next_link, item = node
        prev_link[1] = next_link
        next_link[0] = prev_link
        self.num -= 1
        return item

    def __iter__(self):
        root = self.root
        curr = root[1]
        while curr is not root:
            yield curr[2]
            curr = curr[1]

    def __len__(self):
        return self.num


class RTQueue(object):

    """Single-threaded FIFO 'real-time' message queue

    This is a simple FIFO queue just like :class:`toro.Queue`,
    but treats :meth:`put`timeouts differently. Instead of
    controlling how long it takes to store an item in the queue,
    the timeouts control how long it takes for a worker to
    :meth:`get` the message from the queue.
    
    The queue is intended to be used as a 'Rendezvous
    Point', where producers and consumers meet. The producers state
    how long they are willing to wait for its request to be
    acknowledged.
    """

    class Proxy(object):

        __slots__ = ('_dllist',)

        def __init__(self, dllist):
            self._dllist = dllist

        def __iter__(self):
            return iter(self._dllist)

        def __len__(self):
            return len(self._dllist)

        def __getattr__(self, attr):
            if attr.startswith('_'):
                raise AttributeError(attr)
            return getattr(self._dllist, attr)

        def push(self, item):
            return self.preempt(item)

    class Entry(object):

        __slots__ = ('future', 'value', 'handle')

        def __init__(self, value):
            self.value  = value
            self.future = Future()
            self.handle = None

        def schedule(self, io_loop, queue, deadline):
            node = queue.push(self)
            if deadline:
                def timeout():
                    queue.remove(node)
                    if self.future:
                        self.future.set_exception(Timeout())
                self.handle = io_loop.add_timeout(deadline, timeout)
            return self.future

        def dispatch(self, io_loop, other):
            if self.handle:
                io_loop.remove_timeout(self.handle)
            if self.future:
                self.future.set_result(other.value)
            other.future.set_result(self.value)
            return other.future

        def terminate(self, value):
            future, self.future = self.future, None
            future.set_result(value)
            return future

        def interrupt(self, exc):
            future, self.future = self.future, None
            future.set_exception(exc)
            return future

    def __init__(self, maxsize=0, io_loop=None):
        """Initialize an empty message queue
        
        :Parameters:
          - `maxsize`: maximum size of the queue.
          - `io_loop`: Optional custom IOLoop
        """
        self._queue   = DLList() # cola de mensajes
        self._urgent  = RTQueue.Proxy(self._queue) # cola urgente
        self._workers = DLList() # cola de workers
        self._maxsize = maxsize if maxsize > 0 else 0
        self._loop    = io_loop or IOLoop.current()

    def empty(self):
        return not self._queue

    def full(self):
        return ((self._maxsize > 0) and (len(self._queue) >= self._maxsize))

    @property
    def maxsize(self):
        return self._maxsize

    def put(self, message, deadline=None, wait=True, preempt=False):
        """Put a message in the queue. Raise :exc:`toro.Queue.Full` if full.
        
        The function returns a :class:`tornado.gen.Future`. On completion,
        the value of the Future is the `worker` parameter given
        in the call to :meth:`get` or :meth:`get_nowait` by the consumer
        that got the message.
        
        If `timeout` is specified, the future is interrupted with a 
        :exc:`toro.Timeout` exception if no consumer acknowledges the
        message in time.

        :Parameters:
          - `message`: the message to push.
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta``
            for a deadline relative to the current time.
          - `wait`: Optional, ``True`` to wait for a worker to grab the
             or ``False`` to return inmediately.
          - `preempt`: if ``True``, the message is pushed to the front
            of the queue, and the function never fails with
            :exc:`toro.Queue.Full` exception.
        """
        entry = RTQueue.Entry(message)
        if self._workers:
            return self._workers.pop().dispatch(self._loop, entry)
        if not preempt and self.full():
            return entry.interrupt(Queue.Full())
        future = entry.schedule(
            self._loop,
            self._urgent if preempt else self._queue, 
            deadline
        )
        if not wait:
            future = entry.terminate(None)
        return future

    def get(self, worker=None, deadline=None):
        """Retrieve the first message from the queue.
        
        The function returns a :class:`tornado.gen.Future`. On completion,
        the value of the future is the `message` retrieved. The function
        will also automatically acknowledge that the message has been
        delivered by returning the `worker` ID to the producer that called
        :meth:`put`.

        If a `deadline` is specified, the future is interrupted when the
        timeout expires by raising a :exc:`toro.Timeout` Exception.

        :Parameters:
          - `worker`: Optional consumer ID to hand over to the producer.
          - `deadline`: Optional timeout, either an absolute timestamp
             (as returned by ``io_loop.time()``) or a ``datetime.timedelta``
             for a deadline relative to the current time.
        """
        entry = RTQueue.Entry(worker)
        if self._queue:
            return self._queue.pop().dispatch(self._loop, entry)
        return entry.schedule(self._loop, self._workers, deadline)

    def __len__(self):
        return len(self._queue)


if __name__ == '__main__':

    import fcntl
    import os
    import sys

    #from itertools import chain
    from datetime import timedelta
    from tornado.iostream import PipeIOStream
    from tornado.gen import Task, coroutine
    from toro import Queue, Timeout

    @coroutine
    def dummy_worker(queue, worker):
        """Example dummy worker"""
        while True:
            print ("Spawning worker %s" % str(worker))
            try:
                delta   = timedelta(seconds=2*(worker+1))
                message = yield queue.get(worker=worker, deadline=delta)
            except Timeout:
                print("No input for %d seconds!" % (2*(worker+1)))
            else:
                print("Mensaje '%s' recibido por worker %s" % (
                    str(message), str(worker)
                ))
                yield Task(IOLoop.current().call_later, 10)

    @coroutine
    def dummy_sender(queue):
        """Example dummy sender"""
        fd = sys.stdin.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        print ("Spawning sender")
        pipe = PipeIOStream(fd)
        while True:
            print ("Waiting for input")
            message = yield pipe.read_bytes(80, partial=True)
            message = str(message)
            preempt = True if "URG" in message else False
            if preempt:
                print("MENSAJE %s es URGENTE!" % message)
            print ("Mensaje detectado: %s" % message)
            try:
                delta  = timedelta(seconds=1)
                worker = yield queue.put(
                        message,
                        deadline=delta,
                        preempt=preempt,
                        wait=False)
            except Queue.Full:
                print("**COLA LLENA!  ERROR %s" % message)
            except Timeout:
                print("**TODOS LOS HILOS OCUPADOS! ERROR %s" % message)
            else:
                print ("Mensaje '%s' asignado a worker %s" % (
                    str(message), str(worker)
                ))
                if worker is None:
                    print("Tamaño de cola: %s" % str(len(queue)))

    def main(workers=5, loop=None):
        """Spawns several workers and the sender"""
        loop = loop or IOLoop.current()
        mq   = RTQueue(workers, io_loop=loop)
        #wks  = (dummy_worker(mq, i) for i in range(workers))
        #yield list(chain((dummy_sender(mq),), wks))
        producer  = dummy_sender(mq)
        consumers = tuple(dummy_worker(mq, i) for i in range(workers))
        loop.start()
        
    #IOLoop.current().run_sync(main)
    main(5, IOLoop.current())
