"""Enables aborting long-running queries in interactive shells.

By introducing a 'wait_callback'
(see http://initd.org/psycopg/docs/extras.html - Coroutine support),
the interpreter retains control and listens for KeyboardInterrupts.
If one occurs, the callback cancels the current transaction using the connction
after which control is returned to the shell.

DEPENDENCY: only works with psycopg 2.2.0 or newer.

Author: Gil Georges <gil.georges@lav.mavt.ethz.ch>
Date: November 23, 2016
"""
import psycopg2.extensions
from select import select

_WAIT_SELECT_TIMEOUT = 1


def wait_select(conn):
    """Monitor long-running queries and cancle on KeyboardInterrupt.

    Copied from http://initd.org/psycopg/articles/2014/07/20/cancelling-postgresql-statements-python/
    """
    while 1:
        try:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_READ:
                select([conn.fileno()], [], [], _WAIT_SELECT_TIMEOUT)
            elif state == psycopg2.extensions.POLL_WRITE:
                select([], [conn.fileno()], [], _WAIT_SELECT_TIMEOUT)
            else:
                raise conn.OperationalError(
                    "bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


def activate():
    """Register callback to activate interrupt support."""
    try:
        callback = psycopg2.extras.wait_select
    except AttributeError:
        callback = wait_select

    try:
        psycopg2.extensions.set_wait_callback(callback)
    except AttributeError:
        raise NotImplementedError("only works with psycopg 2.2.0 or newer")

def deactivate():
    """Deactivate green-mode."""
    try:
        psycopg2.extensions.set_wait_callback(None)
    except AttributeError:
        raise NotImplementedError("only works with psycopg 2.2.0 or newer")

