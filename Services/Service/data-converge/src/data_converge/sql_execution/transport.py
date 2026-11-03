"""Direct HTTP connections with bounded query lifetimes and TLS verification."""

from __future__ import annotations

import http.client
import socket
import ssl
import threading
import time
import urllib.request
from functools import partial


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward private authentication headers to a redirected endpoint.
        return None


class QueryDeadline:
    """Cancel socket I/O at an absolute deadline, including HTTP/TLS headers.

    A blocking platform getaddrinfo call cannot be forcibly cancelled. Once it
    returns, an expired deadline prevents any new connection or SQL submission.
    """

    def __init__(self, timeout, *, connect_timeout=None):
        self.expires_at = time.monotonic() + timeout
        self.connect_timeout = connect_timeout
        self._lock = threading.Lock()
        self._expired = threading.Event()
        self._socket = None
        self._watchdog = None

    @property
    def expired(self):
        return self._expired.is_set() or time.monotonic() >= self.expires_at

    def remaining(self):
        remaining = self.expires_at - time.monotonic()
        if self._expired.is_set() or remaining <= 0:
            raise TimeoutError("query deadline exceeded")
        return remaining

    @staticmethod
    def _close(sock, *, interrupt=False):
        if sock is None:
            return
        if interrupt:
            try:
                # close() alone does not interrupt sockets retained by makefile().
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        sock.close()

    def _expire(self):
        with self._lock:
            self._expired.set()
            sock = self._socket
        self._close(sock, interrupt=True)

    def __enter__(self):
        self._watchdog = threading.Timer(max(0, self.expires_at - time.monotonic()), self._expire)
        self._watchdog.name = "clickhouse-query-deadline"
        self._watchdog.daemon = True
        self._watchdog.start()
        return self

    def __exit__(self, *_):
        self._watchdog.cancel()
        self._watchdog.join()
        self._close(self._socket)
        self._socket = None

    def connect(self, address, timeout=None, source_address=None):
        # Retain the actual socket before connect so the watchdog can interrupt
        # connection attempts as well as reads after urllib relinquishes ownership.
        host, port = address
        addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
        last_error = None
        for family, kind, protocol, _, target in addresses:
            self.remaining()
            sock = socket.socket(family, kind, protocol)
            try:
                with self._lock:
                    self.remaining()
                    self._socket = sock
                limit = self.remaining()
                if self.connect_timeout is not None:
                    limit = min(limit, self.connect_timeout)
                sock.settimeout(limit)
                if source_address:
                    sock.bind(source_address)
                sock.connect(target)
                sock.settimeout(self.remaining())
                return sock
            except OSError as error:
                sock.close()
                self.remaining()
                last_error = error
        if last_error is not None:
            raise last_error
        raise OSError("no connection address")

    def wrap_tls(self, sock, context, hostname):
        # Avoid a gap where wrap_socket detaches the tracked raw socket while the
        # TLS handshake blocks. Register the TLS socket before starting handshake.
        with self._lock:
            self.remaining()
            wrapped = context.wrap_socket(sock, server_hostname=hostname, do_handshake_on_connect=False)
            self._socket = wrapped
            return wrapped


class _DeadlineConnection:
    def __init__(self, *args, deadline, **kwargs):
        super().__init__(*args, **kwargs)
        self._deadline = deadline
        self._create_connection = deadline.connect


class _DeadlineHTTPConnection(_DeadlineConnection, http.client.HTTPConnection):
    pass


class _DeadlineHTTPSConnection(_DeadlineConnection, http.client.HTTPSConnection):
    def connect(self):
        http.client.HTTPConnection.connect(self)
        self.sock = self._deadline.wrap_tls(self.sock, self._context, self.host)
        self.sock.do_handshake()


class _DeadlineHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, deadline):
        super().__init__()
        self._deadline = deadline

    def http_open(self, request):
        return self.do_open(partial(_DeadlineHTTPConnection, deadline=self._deadline), request)


class _DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, deadline, context):
        super().__init__(context=context)
        self._deadline = deadline

    def https_open(self, request):
        return self.do_open(partial(_DeadlineHTTPSConnection, deadline=self._deadline), request,
                            context=self._context)


def build_query_handlers(deadline, secure, verify):
    """Use direct transport with the query deadline on HTTP and HTTPS sockets."""
    handlers = [urllib.request.ProxyHandler({}), _NoRedirect(), _DeadlineHTTPHandler(deadline)]
    if secure:
        context = ssl.create_default_context()
        if not verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        handlers.append(_DeadlineHTTPSHandler(deadline, context))
    return handlers
