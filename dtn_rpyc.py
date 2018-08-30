#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''CLI interface for DTN-RPyC
'''

import os
import sys
import signal

import argparse

import utilities
import server
import client


class DTNRPyC(object):
    '''Object representing the DTN-RPyC CLI interface

    Arguments:
        object {DTNRPyC} -- Contains only __init__ with mainly two options:
        -s for starting the server and -c for starting the client.
    '''

    def __init__(self):
        '''Init the Object. Does not have any arguments.
        '''

        parser = argparse.ArgumentParser(
            prog='DTN-RPyC',
            description='Remote Procedure Calls in Disruption Tolerant' \
            'Networks. Alternative approach the original DTN-RPC, now in' \
            'Python (see https://github.com/umr-ds/DTN-RPC for more ' \
            'information)',
            epilog='Please report bugs to the issue tracker on Github.')

        parser.add_argument(
            '-f',
            '--config',
            type=str,
            dest='config_path',
            default='rpc.conf',
            help='Path to the DTN-RPyC config file. Default is $PWD/rpc.conf.'
        )

        group = parser.add_mutually_exclusive_group(required=True)

        group.add_argument(
            '-c',
            '--client',
            type=str,
            dest='job_file_path',
            default='',
            help='Call a procedure(s) specified in the job file given..')

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
            help='The server should execute calls sequentially instead' \
            'of parallel.'
        )

        args = parser.parse_args()

        # Before starting, check, if the config file can be parsed
        # and do some other checks (for server or client, respectively).
        if args.server:
            utilities.pre_exec_checks(args.config_path, server_checks=True)
            server.server_listen(args.queue)
        elif args.job_file_path:
            utilities.pre_exec_checks(
                args.config_path, client_jobfle=args.job_file_path)
            client.client_call(args.job_file_path)


def signal_handler(_, __):
    '''Simple CTRL-C signal handler. Since we do not have any global
    state to be persisted, it does nothing than exiting.

    Arguments:
        _ -- Not used.
        __ -- Not used.
    '''

    utilities.pwarn('Stopping DTN-RPyC.')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    try:
        DTNRPyC()
    except (KeyboardInterrupt, SystemExit):
        raise
