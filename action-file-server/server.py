#!/usr/bin/env python3

import socketserver
import json
import time

f = open('/log/log', 'a')

class TCPHandler(socketserver.BaseRequestHandler):
	def handle(self):
		self.data = self.request.recv(4096).strip()
		try:
			j = json.loads(self.data)
			j['ts'] = time.time()
			print(j, flush=True)
			f.write(json.dumps(j) + "\n")
			f.flush()
		except ValueError:
			print("Not JSON?", flush=True)

if __name__ == "__main__":
	print("starting...", flush=True)
	with socketserver.TCPServer(('0.0.0.0', 8787), TCPHandler) as server:
		server.serve_forever()
