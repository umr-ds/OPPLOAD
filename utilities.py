#!/usr/bin/env python3

# -*- coding: utf-8 -*-

'''Contains helper functions and variables.
'''
import os
import sys
import string
import requests
from pyserval.client import Client
import zipfile
import errno
import job
import math
import time
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
CALL = 0
ACK = 1
RESULT = 2
ERROR = 3
CLEANUP = 4

OFFER = 'RPCOFFER'

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

def pre_exec_checks(config_path, server_checks=False, client_jobfle=None):
    ''' Checks, if all files are present and if Serval is running

    Args:
        config_path (str):    Path of the main RPC config file to be checked.
        server_checks (bool): If server checks should be done or not (default False).
        client_jobfle (str):  The client job file. If None, it will not be checked.
    '''

    if not read_config(config_path):
        sys.exit(1)
    #if not serval_running():
    #    sys.exit(1)
    if server_checks and not config_files_present():
        sys.exit(1)
    if client_jobfle and not os.path.exists(client_jobfle):
        pfatal("Jobfile %s not present! Please check arguments!" % client_jobfle)
        sys.exit(1)

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

def extract_zip(path, extract_path):
    zipf = zipfile.ZipFile(path, 'r')
    member_list = zipf.namelist()

    if not os.path.exists(extract_path):
        try:
            os.makedirs(extract_path)
        except OSError as e:
            if e.errno != errno.EXIST:
                raise

    zipf.extractall(extract_path)
    zipf.close()

    member_list = [extract_path + x for x in member_list]

    return member_list

def make_zip(arg_list, name='tmp_container', subpath_to_remove=""):
    ''' Creates a zip archive with all information a server needs to execute a job
    Args:
        arg_list (list of strings): The list of files to make a archive from.
        name (string): An optionally name for the zip archive.
    '''
    # writing the zipfile
    with zipfile.ZipFile(name + '.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for arg in arg_list:
            zipf.write(arg, arg.replace(subpath_to_remove, ""))
    return name + '.zip'

def insert_to_line(line, appendix):
    line = line.split('|')
    line[0] = line[0].strip('\n')
    line[0] = line[0] + ' ' + appendix

    if len(line) == 1:
        line[0] = line[0] + '\n'
        return line[0]

    ret_line = line[0] + '|' + line[1]
    return ret_line

def find_available_servers(rhizome, first_job, server_sid=None):
    # If there are no bundles, the are no servers offering anything. Abort.
    bundles = rhizome.get_bundlelist()
    if not bundles:
        return None

    server_list = []
    for bundle in bundles:
        if not bundle.manifest.service == OFFER:
            continue

        if server_sid is not None and bundle.manifest.sender == server_sid:
            continue

        # We found an offer bundle. Therefore download the content...
        offers = rhizome.get_payload(bundle).decode("utf-8").split('\n')

        # ... iterate over the lines and see if this is the procedure we searching for.
        for offer in offers:
            procedure = offer.split(' ')
            if procedure[0] == '' or 'capabilities' in offer:
                break
            if procedure[1] == first_job.procedure and len(procedure[2:]) == len(first_job.arguments) and bundle.manifest.name:
                if not first_job.filter_dict:
                    server_list.append(bundle.manifest.name)
                else:
                    fullfills = True
                    for requirement in first_job.filter_dict:
                        capability_line = [line for line in offers if requirement in line]
                        capability_line = capability_line[0].split('=')
                        capability_type = capability_line[0]
                        capability_value = capability_line[1].rstrip()
                        unpacked_requirement_value = first_job.filter_dict[requirement][0]
                        unpacked_requirement_op = first_job.filter_dict[requirement][1]
                        if not eval("{} {} {}".format(capability_value, unpacked_requirement_op, unpacked_requirement_value)):
                            fullfills = False
                            break

                    if fullfills:
                        server_list.append(bundle.manifest.name)

    return server_list

def replace_any_to_sid(job_file_path, linecounter, sid):
    with open(job_file_path, 'r+') as job_file:
        lines = job_file.readlines()
        lines[linecounter] = lines[linecounter].replace('any', sid)

        job_file.seek(0)
        for line in lines:
            job_file.write(line)
        job_file.close()

def serval_running():
    # TODO: needs revision!
    '''Check is Serval is running.
    Returns:
        bool: True, if Serval is running, false otherwise.
    '''
    try:
        Client(
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

def is_server_address(sid):
    '''Simple function to check if a SID is realy a SID.
    Args:
        sid (str):  The string to check.

    Returns:
        bool: If the SID is a SID or 64 hex chars or not.
    '''
    if sid == 'any' \
        or (all(hex_char in string.hexdigits for hex_char in sid) and len(sid) == 64) \
        or (all(hex_char in string.hexdigits for sids in sid for hex_char in sids) and all(len(sids) == 64 for sids in sid)) \
        or (type(sid) is list and (char == 'any' or all(char in string.hexdigits) for sids in sid for char in sids)):
        return True

    pfatal('%s is not a valid server address. Aborting.' % sid)
    return False

def parse_jobfile(job_file_path):
    # This are the available capabilities.
    filter_keywords = ['cpu_cores', 'cpu_load', 'disk_space', 'power_state', 'power_percentage']

    # parse the jobfile if its well formed
    job_file = open(job_file_path, 'r+')

    client_sid = None

    #check each line
    lines = job_file.readlines()

    # First, we see, if the client SID is on the first line. If not, we stop the
    # execution here.
    split_first_line = lines[0].split('=')
    try:
        if split_first_line[0] == 'client_sid':
            client_sid = split_first_line[1].strip('\n')
            if not (all(hex_char in string.hexdigits for hex_char in client_sid) and len(client_sid) == 64):
                # This is a SID sanity check... If it fails, we prepend the default SID.
                pfatal("Looks like the provided client SID in the job file is not a real SID.")
                job_file.close()
                return None
    except IndexError:
        pfatal("Could not find a client SID in the first line of the job file.")
        job_file.close()
        return None

    # Create an empty jobs object. This will be filled in further execution.
    jobs = job.Jobfile(client_sid)

    counter = 1
    for line in lines[1:]:
        # The first thing to check for comments. Comments in job files start
        # with a '#'. Only line comments are allowed.
        # At this point also empty lines are skipped.
        if line[0] == '#' or len(line) == 0 or line == '\n':
            counter = counter + 1
            continue

        # We have the ability to specify global filters for capabilites, which
        # will be applied to all jobs. They can be defined anywhere in the file
        # and start with a '|'.
        if line[0] == '|':
            global_filter = line[1:].strip('\n')
            global_filter = global_filter.split(' ')

            # Not sure why this is if needed...
            if '' in global_filter:
                global_filter = [cap_filter for cap_filter in global_filter if cap_filter != '']

            # Split the filter at '=' and store the type and the corresponding filter value.
            for cap_filter in global_filter:
                filter_type = cap_filter.split(':')

                # Just a very basic sanity check.
                if filter_type == '':
                    continue
                elif filter_type[0] in filter_keywords:
                    jobs.add_filter(filter_type[0], (filter_type[1], filter_type[2]))

            counter = counter + 1
            # At this point we can ignore the remaining part of the line since
            # it is not allowed to do other stuff in the global filter line.
            continue

        # Here the job parsing starts. At the begining, we assume that all jobs
        # jobs are open.
        status = 'OPEN'
        line = line.strip('\n')
        # We also can have filters per step, which will override global filters
        # for this particular step. We keep them for later.
        possible_filters = line.split('|')

        # Take the first part of the line (before the '|') and parse it.
        job_parts = possible_filters[0].split(' ')
        if '' in job_parts:
            job_parts = [arg for arg in job_parts if arg != '']

        # We assume a fixed syntax: server_sid, procedure name and finally all
        # arguments.
        possible_sid = job_parts[0]
        procedure_name = job_parts[1]
        procedure_args = job_parts[2:]
        if '' in procedure_args:
            procedure_args = [arg for arg in procedure_args if arg != '']

        # At this point we have to make some sanity check of the server SID.
        if not is_server_address(possible_sid):
            pfatal('SID {} might be malformed.'.format(possible_sid))
            job_file.close()
            return None

        # A job can have three states: OPEN, DONE or ERROR. We have to remember
        # the state for every job.
        if 'DONE' in procedure_args:
            status = 'DONE'
        elif 'ERROR' in procedure_args:
            status = 'ERROR'

        # Done. Now let's create a job.
        # Finally, add local filters, if available.
        if len(possible_filters) > 1:
            filter_dict = {}
            possible_filters = possible_filters[1].split(' ')
            if '' in possible_filters:
                possible_filters = [arg for arg in possible_filters if arg != '']

            for filter_arg in possible_filters:
                fil = filter_arg.split(':')

                # Again, simple sanity check.
                if fil == '':
                    continue

                elif fil[0] in filter_keywords:
                    filter_dict[fil[0]] = (fil[1], fil[2].rstrip())
                    continue
                else:
                    pwarn("Filter {} not found. Not applying this filter to job.".format(fil[0]))

            jobs.add(possible_sid, procedure_name, procedure_args, status, counter, filter_dict)
        else:
            jobs.add(possible_sid, procedure_name, procedure_args, status, counter)

        counter += 1

    job_file.close()
    return jobs
