# DTN-RPyC
Remote Procedure Calls in Delay-Tolerant Networks.

## Requirements
There are some requirements for DTN-RPyC, `requests`, `pyserval`, `numpy` and `Serval`. Furthermore, DTN-RPyC was tested on Ubuntu 16.04. It should work on other Linux distros and macOS, even *BSD should work, but was not tested, yet. Thus, the following guide will assume Ubuntu 16.04.

This guide will not show how to install these dependencies. See the following instructions:
 * [Requests](http://docs.python-requests.org/en/master/)
 * [Pyserval](https://github.com/umr-ds/pyserval)
 * [Numpy](http://www.numpy.org)
 * [Serval](https://github.com/servalproject/serval-dna)

In order to be able to talk to the Serval REST API, you have to set some credentials in the config. Replace the `<USERNAME>` and `<PASSWORD>` in the option below.

```
api.restful.users.<USERNAME>.password=<PASSWORD>
```

## Preparing DTN-RPyC
For DTN-RPyC you need some configurations. Examples for the configurations can be found in the `examples` folder.

### Main configuration
The first one is the main configuration file. Here, you define a bunch of variables:

```bash
host=<IP> # The IP where the Serval REST API listens, typically localhost
port=<PORT> # The port where the Serval REST API listens, typically 4110
user=<RESTful USERNAME> # Username for the Serval REST API, i.e. <USERNAME> from the example above
passwd=<RESTful PASSWORD> # Password for the Serval REST API, i.e. <PASSWORD> from the example above
rpcs=<PATH/TO/RPC/DEFINITIONS> # Path to the RPC definitions file (see below for more information)
bins=<PATH/TO/RPC/BINARIES> # Path to the RPC binaries (see below for more information)
server=<MODE> # Server selection mode (see below for more information)
capabilites=<PATH/TO/CAPABILITIES> # The capabilities of the server (see below for more information)
location=<PATH/TO/LOCATION/FILE> # Path to the file where the location of the node is set (x,y)
```
The configuration file can be where ever you want, but if it is not in `$PWD`, you have to provide an additional parameter (see [usage](#usage) for more information).

### Server Capabilities (server only)
It is possible to provide four server capabilities:

```bash
disk_space=<VALUE> # Remaining disk space of the server in kb
cpu_load=<VALUE> # current CPU load
memory=<VALUE> # Available RAM in kb
gps_coord=<VALUE> # The location of the node in x,y
```

The `examples` folder contains an example script how to generate these values.

### Server Selection
DTN-RPyC offer four server selection modes.

#### First
The first server found in the Rhizome store will be chosen (`first`).

#### Random
A uniformly distributed random server, which is offering the procedure and is able to execute it will be chosen (`random`).

#### Best
The nearest server with the most available resources will be chosen (`best`).

#### Probabilistic
Available servers will be sorted based on the capabilities and a random server based on the gamma distribution is chosen (`probabilistic`). This prevents that always the same server is chosen, but only servers, which have the most free resources.

### RPC Definitions (server only)
Furthermore, you will need a `rpc.defs` file, where the definitions for the offered procedures per server have to be. The file expects the following format:

``` bash
name parameter1 [parameter2 ...]\n
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
DTN-RPyC has two modes, `client` and `server`.

```
usage: DTN-RPyC [-h] [-f CONFIG_PATH] (-c JOB_FILE_PATH | -s) [-q]

optional arguments:
  -h, --help            show this help message and exit
  -f CONFIG_PATH, --config CONFIG_PATH
                        Path to the DTN-RPyC config file. Default is
                        $PWD/rpc.conf.
  -c JOB_FILE_PATH, --client JOB_FILE_PATH
                        Call a procedure(s) specified in the job file given.
  -s, --server          Start the server listening.
  -q, --queue           The server should execute calls sequentially insteadof
                        parallel.
```

With `-f` you can specify the path to the main config file (not needed, if it is in the current directory).

### Client
With `-c` the client will be started. A `JOB_FILE_PATH` has to be given.

**Note:** The client will not stop waiting for a result. You have to stop the client with `SIGTERM` if you think it takes to long.

**Note:** If you need a file for the procedure, the first argument has to be `file` and the second has to be the path to the file. Furthermore, it is only possible to send one file per call. If you need more, they have to be packed (e.g. `tar`) and the server has to take care about unpacking. If the file is a file, it can also be only one per call. You have to unpack it if required.

#### Jobfile Specification

```
client_sid=<SID>
# comments...
| REQUIREMENT1:VALUE1 [REQUIREMENT2:VALUE2 ...]
any|SID procedure argument1 [argument2 ...] | REQUIREMENT1 [REQUIREMENT2 ...]
any|SID procedure argument1 [argument2|## ...] | REQUIREMENT1 [REQUIREMENT2 ...]
[...]
```

The first line of the job file has to be the client SID. To specify requirements which should be applied to all procedures, the line has to start with a `|` followed by space-seperated requirements, e.g. `disk_space:50000` will ensure that only servers with at least `50000 kb` available disk space will be chosen (see above for more information about available server capabilities). The procedures itself start with either `any` or a particular `SID` of the server followed by the name of the desired procedure and all required arguments. You can additionally specify requirements per procedure after `|`.

#### Cascading Procedures
If more than one procedures are given, all procedures are executed sequentially hop-by-hop. Therefore, the result of a procedure will be the argument for the next procedure. To specify which argument should be substituted (only one per procedure), you have to set `##` at the corresponding argument position.

### Server
The option `-s` starts the server. Incoming calls are handled automatically, you have just to make sure that all options are set in the config file. There is also the `-q` option for servers, which causes the server to block until a procedure is executed, without the execution will be done in background.

## Docker
You can use docker to run the example simple and fast:

```
docker run --rm --privileged -v <PATH/TO/DTN-RPyC>:/dtnrpc -v <PATH/TO/DTN-RPyC>/examples:/tmp/dtnrpc -v /lib/modules:/lib/modules -it --cap-add=NET_ADMIN -e DISPLAY=<YOURDISPLAYVAR>:0 umrds/serval_core_worker-gui
```

This will pull a docker container containing all dependencies and start a CORE GUI forwarded to your local X server.
