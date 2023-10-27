#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import socket
import threading

from .process_monitor import ProcessMonitorServer, ProcessMonitorClient


class TcpProcessMonitorServer(ProcessMonitorServer):
    """Implement process monitoring via TCP.

    The process to be monitored opens a TCP socket and accepts all
    client connections. Clients are notified about process termination
    via closure of their end of the socket.
    """
    def __init__(self):
        self._is_open = False
        self._listen_socket = None
        self._client_sockets = []
        self._accept_thread = None

    def _accept_thread_entry(self):
        """Entry point for the thread accepting client connections."""
        while True:
            # TODO: use select, read from all sockets, and close them
            # when the peer closes them.
            client, clientaddr = self._listen_socket.accept()

            # The main thread sets is_open to false and wakes us up if the
            # monitor is to be closed.
            if self._is_open:
                self._client_sockets.append(client)
            else:
                client.close()
                break

    def open(self):
        self._listen_socket = socket.socket(socket.AF_INET,
                                            socket.SOCK_STREAM, 0)
        try:
            self._listen_socket.bind(('127.0.0.1', 0))
            self._listen_socket.listen(5)
            self._is_open = True

            self._accept_thread = threading.Thread(target=self._accept_thread_entry)
            self._accept_thread.daemon = True
            self._accept_thread.start()
        except:
            # Reset state in case something does not work
            self._is_open = False
            self._listen_socket.close()
            self._listen_socket = None
            raise

        # Return the port number that we got bound to
        return self._listen_socket.getsockname()[1]

    def close(self):
        if self._listen_socket is not None:
            self._is_open = False

            # Terminate the accepting thread. We need to wake it up, so
            # initiate a connection to it.
            port = self._listen_socket.getsockname()[1]
            term_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            term_socket.connect(('127.0.0.1', port))

            # Now wait for the accept thread to terminate
            self._accept_thread.join()

            # Then close all other sockets
            term_socket.close()
            self._listen_socket.close()
            self._listen_socket = None

            for sock in self._client_sockets:
                sock.close()
            self._client_sockets = []


class TcpProcessMonitorClient(ProcessMonitorClient):
    """Implement process monitoring via TCP.

    The process to be monitored opens a TCP socket and accepts all
    client connections. Clients are notified about process termination
    via closure of their end of the socket.
    """
    def __init__(self):
        self._socket = None

    def open(self, pid, update_time):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        try:
            self._socket.connect(('127.0.0.1', pid))
        except:
            self._socket.close()
            return False
        else:
            return True

    def wait(self):
        while True:
            try:
                # Wait until the socket is closed by the remote peer.
                x = self._socket.recv(1024)
                if not x:
                    break
            except Exception: # TODO: catch something less generic, maybe RuntimeError?
                # some sort of broken pipe error also indicates
                # socket closure.
                break

    def close(self):
        if self._socket is not None:
            self._socket.close()
