# DTN-RPyC (planned) Technology Overview

## Modes
DTN-RPyC supports three transport modes: DTN via Rhizome and direct via MSP.

### Client
The client is able to call a remote procedure in either of the modes.

#### Explicitly Choosing a Mode
Calling a procedure in either mode will instruct the client to issue the call in the chosen mode and waiting for the result on the same channel. For example, if a  procedure is called in DTN mode, the client will only listen on the DTN channel for the result.

#### Transparently Choosing a Mode
The better option is to call a procedure transparently. The client will first try to call the procedure using the direct mode. Therefore, a lookup will be done to check, if the/a (see [Server Addressing](#server-addressing) for more information) desired server is available. If so, the call will be issued directly using MSP. Otherwise, or if the connection gets lost before the result arrives, the call will be issued again, using the DTN mode.

If called transparently, the client will wait for the result either on the direct channel or on both channels if the procedure has to be called again using the DTN mode.

### Server
The server can be also called in one of the two modes explicitly.

#### Explicit Mode Selection
If started in either mode, the server will only listen on the chosen mode for calls and return the result on the same mode. If this was the direct channel but the connection got lost, the server will not try to return the result using the DTN mode!

#### Transparent Mode Selection
Similar to the client, the server can be started in transparent mode. The server will try to send the result on the same channel as the call arrived. If this was on the direct channel but the connection got lost, the server will return the result using the DTN channel.

## Server Addressing
Servers can be addressed in three different ways.

### Implicit
For the implicit mode, the server address (i.e. the server SID) has to be known. The client will call the procedure on this server in chosen mode (see [Modes](#modes)) and handle results accordingly. If the server is not available, the procedure will not be called.

### Explicit
The explicit mode splits into two separate modes.

#### Any
In the any mode, the client will select any of the available servers (see [Publishing Procedures and Capabilities](#publishing-procedures-and-capabilities)). If the chosen server does not fulfill the required capabilities, the next available server will be determined. If there is no server available or neither of the servers fulfill all requirements, the procedure will not be called.

The transport mode can be any of the above described.

#### All
If a server is issued in the all mode, the call will be broadcasted. All servers, which receive the broadcast, will execute the procedure and return the result. The client will count beforehand how many servers offer this procedure and wait until either this amount of results arrive or the client is stopped by hand (this is required, because it cannot be guaranteed that all servers are still available or move away before returning the result).

Since MSP does not offer a broadcasting option, the direct transport channel is not available if called in all mode.

## Cascading Procedures
Both client and server are able to cascade procedure calls.

### Client
The client can specify multiple steps for a single all. Assume an example, where a client wants to extract all faces from a picture, grey-scale them and compress the to JPEG. This could be done either by calling each of the three steps individually, every time receiving the intermediate result which will be the payload for the next step, or defining a cascade of servers, where every server executes one step of the cascade and the result will be returned to the client from the last server.

### Server
Servers are also able to call procedures on other servers, if they are not able or not willing to execute a step of a multi-staged procedure. Assume again the example above. If the server, the client called, theoretically could execute all steps but has not enough resources because he is busy with other calls, he could reissue the call or steps of the call to other servers. The server, which executed the final step, will return the result to the client.

If the client already defined a cascade of servers, every server is still able to define own cascades.

For both server and client, there are some restrictions and specialities: all servers can be addressed either explicitly or any, but not in all mode. Furthermore, if a intermediate server starts the execution, he will instruct the server before to clear the intermediate result to not pollute the network.

## Publishing Procedures and Capabilities
All servers publish periodically all procedures they offer and their specific capabilities in a Rhizome bundle. Furthermore, in this bundle the server can define his own capabilities. This capabilities are completely free and are not predefined by any means. This bundle is essential for the client, which will decide weather or not to choose this server for a specific call. The client will first lookup if the server offers the desired procedure and if so, if the server can fulfill all requirements, if any specified by the client.

The bundle will be updated periodically by the server and redistributed, whereby the existing bundle will be updated.

## Packets
In DTN-RPyC, there are five packet types defined:

```
CALL
ACK
RESULT
ERROR
CLEANUP
```

The `CALL` packet contains the name of the procedure, all arguments, payload if needed and the clients SID. If it is not an all call, the servers SID will be set, as well and the payload will be encrypted by default.

The `ACK` packet will be returned to the client if the server starts executing the procedure. All fields are the same as in the `call` packet, only the packet type is different. Furthermore, since the server always knows the clients SID, all communication is encrypted from know on, even if it is an all call.

The `RESULT` packet is used for the result. Again all fields from the call have to be preserved for the result packet. Additionally, the result has to be sent back together with the potential payload.

If the binary returns with a non-zero return code, the `ERROR` packet will be sent. As for all other packets, all fields have to be preserved. Additionally, the error message will be sent to the client.

The last packet is the `CLEANUP` packet. This packet is used if the procedure is done. The client will broadcast a cleanup packet with the fields from the call packet. All involved servers will cleanup their database and remove all payloads from all packets involved in this particular call.
Furthermore, if the call is a cascaded call, servers use an encrypted `CLEANUP` call for the previous server, which can cleanup the intermediate packets.

Single files can be passed as the payload. If more than one file has to be sent, you have to make sure to pack them, e.g. with `tar`. The server has to unpack the data and repack all files for the results if needed.

### Binaries
The parameters of the call will be sent to the called binary as CLI options in the order as they arrive at the server. You have to treat them accordingly, e.g. enforce types, etc.

The binaries can be written in any language. You have just to make sure that they are executable (e.g. `x` bit has to be set, etc.).

The success code has always to be `0` and the result has to be written to `stdout`. Other return codes will be treated as errors and the client will be informed with the message in `stderr`.

If the result is a file, the procedure has to write the file path to the result file to `stdout`. The server will handle everything else.