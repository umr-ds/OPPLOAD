#!/usr/bin/env python3

# -*- coding: utf-8 -*-
'''Main module for DTN-RPyC

This module contains the argument parser and will call start the server
or call a procedure using the client code.

Example:
    For examples visit our Github page.
'''

import argparse
import sys
import signal

import utilities
import server
import client
import filter_servers
class DTNRPyC(object):
    '''Main DTN-RPyC class. Contains just a argument parser and calls server or cloent.
    '''
    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='DTN-RPyC',
            description='Remote Procedure Calls in Disruption Tolerant Networks.' \
                'Reimplementation in of the original DTN-RPC in Python' \
                '(see https://github.com/umr-ds/DTN-RPC for more information)',
            epilog='Please report bugs to the issue tracker on Github.'
        )

        group = parser.add_mutually_exclusive_group(required=True)

        group.add_argument(
            '-l',
            '--listen',
            action='store_true',
            help='Start the server listening.'
        )
        group.add_argument(
            '-c',
            '--call',
            action='store_true',
            help='Call a procedure.'
        )
        group.add_argument(
            '-fc',
            '--filter',
            action='store_true',
            help='Filter servers by capabilties'
        )
        group.add_argument(
            '-cc',
            '--cascade',
            action='store_true',
            help='Cascading jobs' #TODO better help
        )
        # Only parse the first argument to decide,
        # if it is a call, server or filter invocation.
        args = parser.parse_args(sys.argv[1:2])
        # Get the attribute called 'call' or 'listen', respectively.
        # With the braces the attribute will be called like a function.
        if args.call:
            getattr(self, 'call')()
        elif args.listen:
            getattr(self, 'listen')()
        elif args.cascade:
            getattr(self, 'cascade')()
        else:
            getattr(self, 'filter')()

    def filter(self):
        ''' The server subparser and invocation.
        Parses the remaining arguments and decides which filter mode should be chosen.
        '''

        parser = argparse.ArgumentParser(
            description='Start filtering server lists...'
        )
        group = parser.add_mutually_exclusive_group()

        parser.add_argument(
            '-k',
            '--filters',
            help='Filter parameter(s), examples:\n cpu_cores=2, \n power_state=[charging, fully-charged], \n power_percentage=50%, \n disk_space=5.0G',
            nargs='+',
            required=True)

        group.add_argument(
            '-f',
            '--config',
            help='Configuration file',
            default='rpc.conf')

        args = parser.parse_args(sys.argv[2:])

        if not utilities.read_config(args.config):
            sys.exit(1)
        pre_exec_checks(False)

        utilities.pinfo('Filtering servers by capabilities.')
        filter_servers.client_filter(args.filters)


    def listen(self):
        ''' The server subparser and invocation.
        Parses the remaining arguments and decides which server mode should be chosen.
        '''
        parser = argparse.ArgumentParser(
            description='Start the server listening for RPCs ...'
        )

        group = parser.add_mutually_exclusive_group()

        group.add_argument(
            '-d',
            '--dtn',
            action='store_true',
            help='... in DTN mode.'
        )
        group.add_argument(
            '-p',
            '--peer',
            action='store_true',
            help='... in direct peer mode.'
        )
        group.add_argument(
            '-f',
            '--config',
            help='Configuration file',
            default='rpc.conf')

        args = parser.parse_args(sys.argv[2:])

        # Before starting the server, check, if the config file can be parsed
        # and do some other checks.
        if not utilities.read_config(args.config):
            sys.exit(1)
        pre_exec_checks(True)

        utilities.pinfo('Starting server in DTN mode.')
        server.server_listen_dtn()

    def call(self):
        ''' The cleint subparser and invocation.
        Parses the remaining arguments and decides which client mode should be chosen.
        '''
        parser = argparse.ArgumentParser(
            description='Call a remote procedure in ...'
        )

        group = parser.add_mutually_exclusive_group()

        group.add_argument(
            '-d',
            '--dtn',
            action='store_true',
            help='... in DTN mode.'
        )
        group.add_argument(
            '-p',
            '--peer',
            action='store_true',
            help='... in direct peer mode.'
        )
        parser.add_argument(
            '-s',
            '--server',
            help='Address of the RPC server',
            required=True
        )
        parser.add_argument(
            '-n',
            '--name',
            help='Name of the procedure to be called',
            required=True
        )
        parser.add_argument(
            '-a',
            '--arguments',
            help='List of parameters',
            nargs='*'
        )
        group.add_argument(
            '-f',
            '--config',
            help='Configuration file',
            default='rpc.conf'
        )
        parser.add_argument(
            '-t',
            '--timeout',
            help='Seconds how long the client waits for results'
        )
        parser.add_argument(
            '-q',
            '--cascade',
            help='Cascading jobs' #TODO better help
        )
        args = parser.parse_args(sys.argv[2:])

        # Before calling the procedure, check, if the config file can be parsed
        # and do some other checks.
        if not utilities.read_config(args.config) or not utilities.is_server_address(args.server):
            sys.exit(1)
        pre_exec_checks(False)

        if args.timeout:
            signal.signal(signal.SIGINT, client.signal_handler)
            client.client_call_dtn(args.server, args.name, args.arguments, args.timeout)
        else:
            client.client_call_dtn(args.server, args.name, args.arguments)

    def cascade(self):
            ''' The client subparser and invocation.
            Parses the remaining arguments and decides which client mode should be chosen.
            '''
            parser = argparse.ArgumentParser(
                description='Call a remote procedure in cascading mode.'
            )

            group = parser.add_mutually_exclusive_group()

            group.add_argument(
                '-d',
                '--dtn',
                action='store_true',
                help='... in DTN mode.'
            )
            group.add_argument(
                '-p',
                '--peer',
                action='store_true',
                help='... in direct peer mode.'
            )
            parser.add_argument(
                '-s',
                '--server',
                help='Address of the RPC server',
                nargs='+',
                required=True
            )
            parser.add_argument(
                '-n',
                '--name',
                help='Name of the procedure to be called',
                nargs='+',
                required=True
            )
            parser.add_argument(
                '-a',
                '--arguments',
                help='List of parameters. Put them in quotes like "<parameter>"',
                nargs='*'
            )
            group.add_argument(
                '-f',
                '--config',
                help='Configuration file',
                default='rpc.conf'
            )
            parser.add_argument(
                '-t',
                '--timeout',
                help='Seconds how long the client waits for results'
            )
            args = parser.parse_args(sys.argv[2:])

            # Before calling the procedure, check, if the config file can be parsed
            # and do some other checks.
            if not utilities.read_config(args.config) or not utilities.is_server_address(args.server):
                sys.exit(1)
            pre_exec_checks(False)

            if args.timeout:
                signal.signal(signal.SIGINT, client.signal_handler)
                client.client_call_dtn(args.server, args.name, args.arguments, args.timeout)
            else:
                client.client_call_dtn(args.server, args.name, args.arguments)

def pre_exec_checks(server_checks):
    ''' Checks, if all files are present and if Serval is running

    Args:
        server (bool): If server checks should be done or not.
    '''
    if server_checks and not utilities.config_files_present():
        sys.exit(1)
    if not utilities.serval_running():
        sys.exit(1)

def signal_handler(_, __):
    ''' Just a simple CTRL-C handler.
    '''
    utilities.pwarn('Stopping DTN-RPyC.')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    DTNRPyC()
