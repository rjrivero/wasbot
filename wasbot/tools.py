#!/usr/bin/env python3 
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


from tornado.concurrent import Future
from tornado.ioloop import IOLoop


class DLList(object):

    """Double-Linked List implementation.
    
    Simple double-linked list aimed to be used as a deque. Two
    main conceptual differences with ``collections.deque``:
    
      - It is not thread-safe, as we intend to use it with coroutines.
      - It provides a :meth:`remove` method to remove items
        from anywhere in the queue, without iterating or searching.
    
    .. seealso:: `http://www.kunxi.org/blog/2014/05/double-linked-list-in-python/`
    """

    __slots__ = ('_root', '_num')

    def __init__(self):
        """Build an empty deque"""
        self.clear()

    def clear(self):
        """Remove all elements from the deque leaving it with length 0"""
        root       = []
        root[:]    = [root, root, None]
        self._root = root
        self._num  = 0

    def append(self, item):
        """Insert the item at the tail of the queue.

        Returns the list node, that can be used to remove the
        item anytime by passing it to :meth:`remove`

        :Parameters:
          - `item`: the item to push.
        """
        root = self._root
        last = root[0]
        node = [last, root, item]
        last[1] = root[0] = node
        self._num += 1
        return node

    def appendleft(self, item):
        """Insert the item at the head of the queue.

        Returns the list node, that can be used to remove the
        item anytime by passing it to :meth:`remove`

        :Parameters:
          - `item`: the item to push in the head.
        """
        root = self._root
        head = root[1]
        node = [root, head, item]
        head[0] = root[1] = node
        self._num += 1
        return node

    def popleft(self):
        """Remove the item at the head of the queue.
        
        Raises :exc:`IndexError` if empty.
        """
        if not self._num:
            raise IndexError('pop from an empty DLList')
        return self.remove(self._root[1])

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
        self._num -= 1
        return item

    def __iter__(self):
        """Iterate over the items in the deque"""
        root = self._root
        curr = root[1]
        while curr is not root:
            yield curr[2]
            curr = curr[1]

    def __len__(self):
        """Get size of the deque"""
        return self._num


class Serializer(object):

    """A serializer queue for task functions.

    "task functions" are functions that return a
    :class:`tornado.concurrent.Future` object. The queue holds the
    execution of the task function until the Future for the previous task
    has resolved.

    This class is needed because :class:`tornado.iostream.BaseIOStream`,
    orphans the :class:`tornado.concurrent.Future` object returned by
    the `write` method if the method is called again before it has resolved.
    The Serializer imposes an order on calls to the `write` method
    performed by different coroutines.
    """

    __slots__ = ('_loop', '_active', '_deque')

    def __init__(self, io_loop=None):
        """Build a Serializer attached to the given loop.

        :Parameters:
          - `io_loop`: the :class:`tornado.ioloop.IOLoop` object.
        """
        self._loop   = io_loop or IOLoop.current()
        self._active = None
        self._deque  = DLList()

    def append(self, task):
        """Append a task to the Serializer queue.

        The task is not started until all the previous ones have already
        completed. The function returns a :class:`tornado.concurrent.Future`
        object that is resolved when the task is completed (that is,
        the `Future` returned by the task is resolved too).

        :Parameters:
          - `task`: a callable (method or function) that returns a
            :class:`tornado.concurrent.Future` object.
        """
        if self._active is not None:
            # The previous task has not finished, delay the new one.
            pending = Future()
            self._deque.append((pending, task))
            return pending
        # Get the writing promise and flag the queue as busy
        active = self._active = task()
        # Make sure we dispatch the next task when the current one is done
        self._loop.add_future(active, self._dispatch)
        return active

    def _dispatch(self, active):
        """Helper funtion to serialize tasks.

        After a task is done, this function runs the next task in
        the queue.

        :Parameters:
          - `active`: the :class:`tornado.concurrent.Future` object returned
            by the task currently running.
        """
        if not self._deque:
            # We are done, no more tasks waiting
            self._active = None
            return
        # Get the promise and the task waiting to be scheduled
        pending, task = self._deque.popleft()
        active = self._active = task()
        # Fulfill the pending promise as soon as the active task completes
        chain_future(active, pending)
        self._loop.add_future(active, self._dispatch)

    def clear(self, exc):
        """Cancel all pending tasks.
        
        :Parameters:
          - `exc`: the Exception object to raise for all pending tasks that
            are cancelled. If None, pending tasks are not notified at all,
            i.e. their Futures are never resolved.
        """
        if exc is not None:
            for pending, task in self._deque:
                pending.set_exception(exc) 
        self._deque.clear()
