import socket


class SocketSet:
    def reserve(self, host="", port=None):
        if port is None:
            port = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sockets.append(sock)
        sock_port = sock.getsockname()[1]
        return sock, sock_port

    def __enter__(self):
        self.sockets = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for sock in self.sockets:
            sock.close()
        del self.sockets
