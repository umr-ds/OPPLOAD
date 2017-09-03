#!/usr/bin/env python3

import argparse
import sys

import utilities
import server
import client


class DTN_RPyC(object):

    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='DTN-RPyC',
            description='Remote Procedure Calls in Disruption Tolerant Networks. Reimplementation in of the original DTN-RPC in Python (see https://github.com/umr-ds/DTN-RPC for more information)',
            epilog='Please report bugs to the issue tracker on Github.')

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-l', '--listen', action='store_true', help='Start the server listening.')
        group.add_argument('-c', '--call', action='store_true', help='Call a procedure.')
        args = parser.parse_args(sys.argv[1:2])
        getattr(self, 'call')() if args.call else getattr(self, 'listen')()

    def listen(self):
        parser = argparse.ArgumentParser(
            description='Start the server listening for RPCs ...')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-d', '--dtn', action='store_true', help='... in DTN mode.')
        group.add_argument('-b', '--broadcast', action='store_true', help='... in broadast mode.')
        group.add_argument('-p', '--peer', action='store_true', help='... in direct peer mode.')
        args = parser.parse_args(sys.argv[2:])
        utilities.pinfo("Starting server in DTN mode.")
        server.server_listen_dtn()

    def call(self):
        parser = argparse.ArgumentParser(
            description='Call a remote procedure in ...')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-d', '--dtn', action='store_true', help='... in DTN mode.')
        group.add_argument('-b', '--broadcast', action='store_true', help='... in broadast mode.')
        group.add_argument('-p', '--peer', action='store_true', help='... in direct peer mode.')

        parser.add_argument('-s', '--server', help='Address of the RPC server', required=True)
        parser.add_argument('-n', '--name', help='Name of the procedure to be called', required=True)
        parser.add_argument('-a', '--arguments', help='List of parameters', nargs='*')

        args = parser.parse_args(sys.argv[2:])
        utilities.pinfo("Calling procedure \'%s\' in DTN mode." % args.name)
        client.client_call_dtn(args.server, args.name, args.arguments)


if __name__ == '__main__':
    utilities.read_config()
    DTN_RPyC()