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

        # Only parse the first argument to decide,
        # if it is a call or a server incovation.
        args = parser.parse_args(sys.argv[1:2])

        # Get the attribute called 'call' or 'listen', respectively.
        # With the braces the attribute will be called like a function.
        getattr(self, 'call')() if args.call else getattr(self, 'listen')()

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
            '-b',
            '--broadcast',
            action='store_true',
            help='... in broadast mode.'
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
            '-b',
            '--broadcast',
            action='store_true',
            help='... in broadast mode.'
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

        args = parser.parse_args(sys.argv[2:])

        # Before calling the procedure, check, if the config file can be parsed
        # and do some other checks.
        if not utilities.read_config(args.config) or not utilities.is_server_address(args.server):
            sys.exit(1)
        pre_exec_checks(False)

        utilities.pinfo('Calling procedure \'%s\' in DTN mode.' % args.name)
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
