'''Contains helper functions and variables.
'''
import os
import requests
import restful
import rhizome

# ANSI color codes.
RESET = '\033[0m'
FATAL = '\033[1m\033[31mFATAL: \033[0m\033[31m' # Red
INFO = '\033[1m\033[32mINFO: \033[0m\033[32m'   # Green
WARN = '\033[1m\033[33mWARN: \033[0m\033[33m'   # Yellow
DEBUG = '\033[1m\033[34mDEBUG: \033[0m\033[34m' # Blue

def pdebug(string_to_print):
    '''Prints string_to_print in blue to stdout.
    '''
    print(DEBUG + str(string_to_print) + RESET)

def pfatal(string_to_print):
    '''Prints string_to_print in red to stdout.
    '''
    print(FATAL + str(string_to_print) + RESET)

def pinfo(string_to_print):
    '''Prints string_to_print in green to stdout.
    '''
    print(INFO + str(string_to_print) + RESET)

def pwarn(string_to_print):
    '''Prints string_to_print in yellow to stdout.
    '''
    print(WARN + str(string_to_print) + RESET)

# Type definitions for DTN-RPC protocol header.
CALL = '0'
ACK = '1'
RESULT = '2'
ERROR = '3'
CLEANUP = '4'

# Hold the configuration read from config file.
CONFIGURATION = {}

def config_files_present():
    '''Checks if the binary path and the procedure definitions file exist.
    Returns True if both exists, False otherwise.

    Returns:
        bool: If the files exists or not.
    '''
    if not os.path.exists(CONFIGURATION['bins']):
        pfatal('RPC binary path does not exist. Aborting.')
        return False
    if not os.path.exists(CONFIGURATION['rpcs']):
        pfatal('RPC definition file does not exists. Aborting.')
        return False
    return True

def read_config(path):
    '''Reads the configuration file, parses it and stores the values in the CONFIGURATION dict.
    Returns:
        bool: True, if file exists and could be read/parsed, False otherwise.
    '''
    try:
        with open(path, 'r') as rpc_conf:
            for conf in rpc_conf:
                conf_list = conf.split('=')
                CONFIGURATION[conf_list[0].rstrip()] = conf_list[1].rstrip()
        return True

    except FileNotFoundError:
        pfatal('DTN-RPyC configuration file %s was not found. Aborting.' % path)
        return False

def make_bundle(manifest_props, rpc_service=True):
    '''Compiles a bundle of a list of tuples.
    Args:
        manifest_pros (tuple-list): The list of tuples to make the bundle from.
        rpc_service (bool):         An indicator if service=RPC should be part of the bundle.
    Returns:
        bundle: The compiled bundle.
    '''
    bundle = rhizome.Bundle()
    for prop in manifest_props:
        bundle.__dict__[prop[0]] = prop[1]

    if rpc_service:
        default_props = [('service', 'RPC')]
        for prop in default_props:
            bundle.__dict__[prop[0]] = prop[1]

    return bundle

def serval_running():
    '''Check is Serval is running.
    Returns:
        bool: True, if Serval is running, false otherwise.
    '''
    try:
        restful.RestfulConnection(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
        return True
    except requests.exceptions.ConnectionError:
        pfatal('Serval is not running or not listening on %s:%s. Aborting.' \
                  % (CONFIGURATION['host'], CONFIGURATION['port'])
              )
    except requests.exceptions.HTTPError as http_error:
        if http_error.response.status_code == 401:
            pfatal('Serval returned an Unauthorized exception.' \
                      'You should check the credentials: %s:%s' \
                      % (CONFIGURATION['user'], CONFIGURATION['passwd'])
                  )
        else:
            pfatal('An error occured. Aborting. : %s' % http_error)
    return False
