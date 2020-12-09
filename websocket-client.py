# import websocket
# try:
#     import thread
# except ImportError:
#     import _thread as thread
# import time


# def on_message(ws, message):
#     print('message')
#     print(message)


# def on_error(ws, error):
#     print('error')
#     print(error)


# def on_close(ws):
#     print("### closed ###")


# def on_open(ws):
#     print('open')
#     # def run(*args):
#     #     for i in range(3):
#     #         time.sleep(1)
#     #         ws.send("Hello %d" % i)
#     #     time.sleep(1)
#     #     ws.close()
#     #     print("thread terminating...")
#     # thread.start_new_thread(run, ())
#     pass


# if __name__ == "__main__":
#     print('main method')
#     websocket.enableTrace(True)
#     ws = websocket.WebSocketApp("ws://159.122.237.12/ws",
#                                 on_message=on_message,
#                                 on_error=on_error,
#                                 on_close=on_close)
#     ws.on_open = on_open
#     ws.run_forever(http_proxy_port=80)

# # import websocket
# # ws = websocket.WebSocket()
# # ws.connect("ws://159.122.237.12/ws", http_proxy_host="proxy_host_name", http_proxy_port=8080)




#!/usr/bin/env python

# WS client example

import asyncio
import websockets


from autobahn.asyncio.websocket import WebSocketClientProtocol

class MyClientProtocol(WebSocketClientProtocol):

   def onOpen(self):
      self.sendMessage(u"Hello, world!".encode('utf8'))

   def onMessage(self, payload, isBinary):
      if isBinary:
         print("Binary message received: {0} bytes".format(len(payload)))
      else:
         print("Text message received: {0}".format(payload.decode('utf8')))


if __name__ == '__main__':
   import asyncio

   from autobahn.asyncio.websocket import WebSocketClientFactory
   factory = WebSocketClientFactory()
   factory.protocol = MyClientProtocol

   loop = asyncio.get_event_loop()
   coro = loop.create_connection(factory, 'ws://159.122.237.12/ws', 80)
   loop.run_until_complete(coro)
   loop.run_forever()
   loop.close()
