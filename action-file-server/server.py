#!/usr/bin/env python3

import socket
import threading
import socketserver
import json
import time

#f = open('/log/log', 'a')

class ThreadedServer(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(20)
        while True:
            client, address = self.sock.accept()
            threading.Thread(target = self.handleClient, args = (client, address)).start()

    def handleClient(self, client, address):
        while True:
            try:
                data = client.recv(4096)
                if data:
                    try:
                        j = json.loads(data)
                        j['ts'] = time.time()
                        print(j, flush=True)
                    except ValueError:
                        print("Not JSON?", flush=True)
                        print(data)
            except:
                client.close()
                return False

if __name__ == "__main__":
    print("starting...", flush=True)
    ThreadedServer('', 8787).listen()
