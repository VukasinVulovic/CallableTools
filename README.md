# A Framework for ez creation of functions that can be called remotely
*Originally started as a component of my version of MCP*

## 2 component design

### "Server"
A AMQP/MQTT consumer that processes the requests and runs defined python functions

### Client
A AMQP (Soon MQTT support?) publisher that publishes run function requests

## Features
- Auto Discovery (Client sends discovery message, all "servers" respond with their offer)
- Easy framework for defining functions (via modules and decorators)
- Asynchronous (All requests are async, meaning the leas ammount of server resources used and high throughput)
- Failsafes (Auto reconnect, auto recovery)