# DTN-RPyC
Remote Procedure Calls in Delay-Tolerant Networks.

## Requirements
There are two requirements for DTN-RPyC, requests and Serval. Furthermore, DTN-RPyC was tested on Ubuntu 16.04. It should work on other Linux distros and macOS, even *BSD should work, but was not tested, yet. Thus, the following guide will assume Ubuntu 16.04.

### Requests
For installing requests, follow the [instructions on their website](http://docs.python-requests.org/en/master/).

### Serval
Installing Serval is typically not that straightforward. You have to install Serval on all servers and clients. First, install `libsodium`:

```shell
sudo apt install libsodium-dev
```

After that, follow the following instructions for Serval:

```shell
git clone https://github.com/servalproject/serval-dna.git
cd serval-dna
autoreconf -i -f -I m4
./configure [--prefix=/desired/serval/config/path]
make
```

After compiling is done, you will need a minimal `serval.conf` configuration file, which is located in the `$SERVALINSTANCE_PATH/etc/serval`. `$SERVALINSTANCE_PATH` is either the path you have set during the `./configure` step with the `--prefix=` option or you have to find out with `./servald config paths`, which will tell you the `$SERVAL_ETC_PATH`, where `serval.conf` has to go. Paste the following into the configuration file, but make sure to not override values you already have. Furthermore, replace `INTERFACE_PREFIX` with the prefix for your network interfaces (e.g. `eth`):

```
api.restful.newsince_timeout=5s
api.restful.users.RPC.password=SRPC
interfaces.0.match=<INTERFACE_PREFIX>*
interfaces.0.socket_type=dgram
interfaces.0.type=ethernet
mdp.enable_inet=on
```

The last step: run `./servald keyring add` to generate an ID for your Serval instance.

## Preparing DTN-RPyC
For DTN-RPyC you need some configurations. Examples for the configurations can be found in the `examples` folder.

### Main configuration
The first one is the main configuration file. Here, you define a bunch of variables:

```
host=<IP> # The IP where the Serval RESTful interface listens, typically localhost
port=<PORT> # The port where the Serval RESTful interface listens, typically 4110
user=<RESTful USERNAME> # Username for the Serval RESTful interface, i.e. RPC from the example above
passwd=<RESTful PASSWORD> # Password for the Serval RESTful interface, i.e. SRPC from the example above
rpcs=<PATH/TO/RPC/DEFINITIONS> # Path to the RPC definitions file (see for more information)
bins=<PATH/TO/RPC/BINARIES> # Path to the RPC binaries (see below for more information)
```

The configuration file can be where ever you want, but if it is not in `$PWD`, you have to provide an additional parameter (see [usage](#usage) for more information).

### RPC Definitions (server only)
Furthermore, you will need a `rpc.defs` file, where the definitions for the offered procedures per server have to be. The file expects the following format:

```
return_type name parameter1 [parameter2 ...]\n
```

The file can be located where ever you want. The path has to be provided with the `rpcs` option.

### RPC binaries (server only)
Finally, you need the procedures. They have to be in the path provided with the `bins` option. The name of the binary have to be the same as the `name` in the `rpc.defs` file.

#### Binary requirements
The parameters of the call will be sent to the called binary as CLI options in the order as they arrive at the server. You have to treat them accordingly, e.g. enforce types, etc.

##### Language
The binaries can be written in any language. You have just to make sure that they are executable (e.g. `x` bit has to be set, etc.).

##### Return code
The success code has always to be `0` and the result has to be written to `stdout`. Other return codes will be treated as errors and the client will be informed with the message in `stderr`.

If the result is a file, the procedure has to write the file path to the result file to `stdout`. The server will handle everything else.

## Usage
DTN-RPyC has four modes, `call`, `listen`, `cascade` and `cascadejob`.

### Client

```
usage: ./dtn_rpyc -c [-h] [-d] [-p] -s SERVER -n NAME
              [-a [ARGUMENTS [ARGUMENTS ...]]] [-f CONFIG]
```
Call a remote procedure in ...

### Client list servers by filters
```
usage: ./dtn_rpyc -fc [-h] -k FILTERS
              [-f CONFIG]
```
current example filters:
```    
cpu_cores=2
cpu_load=0.1
disk_space=5G
TODO power_state
TODO power_percentage=50
TODO graphics card available
```
### Client create jobfile from commandline

```
usage: ./dtn_rpyc -cc [-h] [-d] [-p] -s SERVERS -n NAMES
              [-a [ARGUMENTS [ARGUMENTS ...]] in quotes!] [-f CONFIG] [-t TIMEOUT] [-fc FILTER] [-nd]
```
### Client load a predefined jobfile

```
usage: ./dtn_rpyc -cj [-h] [-d] [-p] -j JOBFILE 
            [-f CONFIG] [-t TIMEOUT] [-nd]

optional arguments:
  -h, --help            show this help message and exit
  -d, --dtn             ... in DTN mode.
  -p, --peer            ... in direct peer mode. (NOT IMPLEMENTED YET)
  -s SERVER, --server SERVER
                        Address of the RPC server
  -n NAME, --name NAME  Name of the procedure to be called
  -a [ARGUMENTS [ARGUMENTS ...]], --arguments [ARGUMENTS [ARGUMENTS ...]]
                        List of parameters
  -f CONFIG, --config CONFIG
                        Configuration file
  -t TIMEOUT --timeout  Seconds how long the client waits for results

  -nd --delete          FLAG: if set, files are kept during cascading process.
  -fc FILTERS [FILTERS ...],
      --filters         Filter parameters for server finding
```
The arguments should be self explaining. First, decide if the call should be issued in DTN mode or peer mode (not implemented yet). If neither `-d` nor `-p` is given, the procedure will be issued transparently using the best option (not implemented yet) Then, specify a server where the procedure should be executed. It can be a SID for a implicit server, `any` for any server or `all` or `broadcast` for all servers. Then, you need the name of the procedure and all arguments. Finally, you can specify a path to the `rpc.conf` file.

**Note:** The client will not stop waiting for a result. You have to stop the client with `SIGTERM` if you think it takes to long.

**Note:** If you call a procedure in `all` mode, the client will not stop waiting, even if results arrive. You have to stop the client with `SIGTERM` if you think enough or all results arrived.

**Note:** If you need a file for the procedure, the first argument has to be `file` and the second has to be the path to the file. Furthermore, it is only possible to send one file per call. If you need more, they have to be packed (e.g. `tar`) and the server has to take care about unpacking. If the file is a file, it can also be only one per call. You have to unpack it if required.


### Jobfile layout
```
[client_sid=X]
# comments...
| FILTER [FILTER ...]
any|server_sid procedure arguments [arguments ...] | FILTER [FILTER ...]
any|server_sid procedure arguments <output of first procedure>
[...]
```
Note: keep in mind, that an inputfile is always the first argument


### Server

```
usage: ./dtn_rpyc -l [-h] [-d | -p | -f CONFIG] [-nd]

Start the server listening for RPCs ...

optional arguments:
  -h, --help            show this help message and exit
  -d, --dtn             ... in DTN mode.
  -p, --peer            ... in direct peer mode. (NOT IMPLEMENTED YET)
  -f CONFIG, --config CONFIG
                        Configuration file
  -nd --delete          FLAG: if set, files are kept during cascading process
```

Again, the arguments should be self explaining. You can again decide either DTN mode, peer mode (not implemented yet) or both, if neither `-p` nor `-d` is given. You have also to provide a path to the config file.

**Note:** See [RPC binaries (server only)](#rpc-binaries-server-only) section above!

**Note:** It is only possible to receive and return one file per call. If more files should be returned, you have to pack them (e.g. `tar`). Furthermore, the `return_type` of the procedure has to be `file` to instruct the server to return the path in the result as a file.

### Known Bugs

fast mode: crash after two cleanup actions after some jumps, due to an update manifest which doesnt belong to the server
Cleanup works only once for each server-server/client combination. Dunno why, but assume a uniqueness problem

