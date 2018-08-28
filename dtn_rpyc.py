#!/usr/bin/env python3

# -*- coding: utf-8 -*-
'''Main module for DTN-RPyC

This module contains the argument parser and will call start the server
or call a procedure using the client code.

Example:
    For examples visit our Github page.
'''
import os
import sys
import signal

import argparse

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

        parser.add_argument(
            '-f',
            '--config',
            type=str,
            dest='config_path',
            default='rpc.conf',
            help='Path to the DTN-RPyC config file. Defailt is $PWD./rpc.conf.'
        )

        group = parser.add_mutually_exclusive_group(required=True)

        group.add_argument(
            '-s',
            '--server',
            action='store_true',
            help='Start the server listening.'
        )

        parser.add_argument(
            '-q',
            '--queue',
            action='store_true',
            help='The server should execute calls sequentially instead of parallel.'
        )

        group.add_argument(
            '-c',
            '--client',
            type=str,
            dest='job_file_path',
            default="",
            help='Call a procedure(s) specified in the job file given..'
        )

        args = parser.parse_args()

        # Before starting the server, check, if the config file can be parsed
        # and do some other checks (for server or client, respectively).
        if args.server:
            utilities.pre_exec_checks(args.config_path, server_checks=True)
            server.server_listen(args.queue)
        elif args.job_file_path:
            utilities.pre_exec_checks(args.config_path, client_jobfle=args.job_file_path)
            client.client_call(args.job_file_path)

def signal_handler(_, __):
    ''' Just a simple CTRL-C handler.
    '''
    utilities.pwarn('Stopping DTN-RPyC.')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    try:
        DTNRPyC()
    except (KeyboardInterrupt, SystemExit):
        raise
