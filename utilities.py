#!/usr/bin/env python3

# -*- coding: utf-8 -*-

'''Collection of utility functions
'''

import os
import sys
import string
import zipfile
import errno
import math
import time
import logging

import requests
from numpy import random
from pyserval.client import Client

from job import Jobfile, Job

# Type definitions for RPC bundle types.
CALL = 0
ACK = 1
RESULT = 2
ERROR = 3
CLEANUP = 4

# Rhizome service definitions
OFFER = 'RPCOFFER'
RPC = 'RPC'

# Server selection definitions
FIRST = 'first'
RANDOM = 'random'
BEST = 'best'
PROB = 'probabilistic'

# Hold the configuration read from config file.
CONFIGURATION = {}

# This are the available capabilities.
filter_keywords = ['gps_coord', 'cpu_load', 'memory', 'disk_space']

LOGGER = logging.getLogger("dtnrpc")
LOGGER.setLevel(logging.DEBUG)

def add_logfile(file_path, level=logging.DEBUG):
    log_handler = logging.FileHandler(file_path)
    log_handler.setLevel(level)
    log_handler.setFormatter(logging.Formatter('%(asctime)-23s | %(name)-6s | %(levelname)-8s | %(message)s'))
    LOGGER.addHandler(log_handler)

def seed(random_file_path="random.seed"):
    # creates predictable random numbers (for server selection)
    try:
        with open(random_file_path, "r") as random_file:
            seed = random_file.read()
            LOGGER.info(" | Successfully read seed from {}.".format(random_file_path))
        random.seed(int(seed))
    except Exception:
        LOGGER.info(" | Couldn't read seed from {}, using default.".format(random_file_path))
        random.seed(0)

class Server():
    '''Simple class for representing servers
    '''

    def __init__(self,
                 sid,
                 jobs=None,
                 gps_coord=None,
                 cpu_load=None,
                 memory=None,
                 disk_space=None):
        '''Server constructor

        Arguments:
            sid -- SID of the server

        Keyword Arguments:
            jobs -- All jobs offered (default: {None})
            gps_coord -- Postion (x,y) (default: {None})
            cpu_load -- CPU capability (default: {None})
            memory -- Available memory (default: {None})
            disk_space -- Available disk space (default: {None})
        '''

        self.sid = sid
        self.gps_coord = gps_coord
        self.cpu_load = float(cpu_load) if cpu_load else None
        self.memory = float(memory) if memory else None
        self.disk_space = float(disk_space) if disk_space else None
        self.jobs = jobs


def sort_servers(server_list):
    '''Sort server list to the following key:
    gps_coord: proximity (closer is better)
    cpu_load: lower is better
    memory: more is better
    disk_space: more is better

    Arguments:
        server_list -- List of servers

    Returns:
        Sorted server list
    '''

    return sorted(
        server_list,
        key=lambda x: (x.gps_coord, x.cpu_load, -(x.memory), -(x.disk_space)))


def select_first_server(server_list):
    '''Select the first server

    Arguments:
        server_list -- List of servers

    Returns:
        The first server from the list
    '''

    return server_list[0]


def select_random_server(server_list):
    '''Select a random server

    Arguments:
        server_list -- List of servers

    Returns:
        A random server from the list
    '''

    return random.choice(server_list)


def select_best_server(server_list):
    '''Select the best server

    Arguments:
        server_list -- List of servers

    Returns:
        The best server based on the sorted list returned from
        'sort_servers'
    '''

    return sort_servers(server_list)[0]


def select_probabilistic_server(server_list):
    '''Select one of the best servers.
    Servers are first sorted using 'sort_servers' and then a server
    is selected using the gamma distribution

    Arguments:
        server_list -- List of servers

    Returns:
        One of the best available servers.
    '''

    sorted_server_list = sort_servers(server_list)
    index = round(random.gamma(2.0))
    try:
        return sorted_server_list[index]
    except IndexError:
        return sorted_server_list[-1]


def select_server(server_list, selection_type=FIRST):
    '''Server selection API function

    Arguments:
        server_list -- List of servers

    Keyword Arguments:
        selection_type -- The method to be used (default: {FIRST})

    Returns:
        A server from the server_list based on the selection_type
    '''

    if selection_type == FIRST:
        return select_first_server(server_list)

    if selection_type == RANDOM:
        return select_random_server(server_list)

    if selection_type == BEST:
        return select_best_server(server_list)

    if selection_type == PROB:
        return select_probabilistic_server(server_list)


def config_files_present(server=True):
    '''Check, if all files from conf are present.

    Returns:
        True, if all files are available, False otherwise
    '''
    if server:
        if not os.path.exists(CONFIGURATION['bins']):
            LOGGER.critical(' | RPC binaries paht {} does not exist.'.format(
                CONFIGURATION['bins']))
            return False
        if not os.path.exists(CONFIGURATION['rpcs']):
            LOGGER.critical(' | RPC definition file {} does not exist.'.format(
                CONFIGURATION['rpcs']))
            return False
        if not os.path.exists(CONFIGURATION['capabilites']):
            LOGGER.critical(' | Capabilities file {} does not exist.'.format(
                CONFIGURATION['capabilites']))
            return False
    if not os.path.exists(CONFIGURATION['location']):
        LOGGER.critical(' | Location file {} does not exist.'.format(
            CONFIGURATION['location']))
        return False
    return True


def pre_exec_checks(config_path, server_checks=False, client_jobfle=None):
    '''Check if all config files are available, if serval is running and
    a job file is present

    Arguments:
        config_path -- Path to the main config file

    Keyword Arguments:
        server_checks -- If additional server checks should be done
        (default: {False})
        client_jobfle -- Path to the client job file (default: {None})
    '''

    if not read_config(config_path):
        sys.exit(1)
    if not serval_running():
        sys.exit(1)
    if server_checks and not config_files_present(server=True):
        sys.exit(1)
    if not config_files_present(server=False):
        sys.exit(1)
    if client_jobfle and not os.path.exists(client_jobfle):
        LOGGER.critical(' | Can not find job file.')
        sys.exit(1)


def read_config(path):
    '''Read and parse the main config file

    Arguments:
        path -- Path to the main config file

    Returns:
        True, if parsing was successful, False otherwise
    '''

    try:
        with open(path, 'r') as rpc_conf:
            for conf in rpc_conf:
                conf_list = conf.split('=')
                CONFIGURATION[conf_list[0].rstrip()] = conf_list[1].rstrip()
        return True

    except FileNotFoundError:
        LOGGER.critical(' | Main config file {} is not available.'.format(path))
        return False


def extract_zip(path, extract_path):
    '''Unzip 'path' to 'extract_path'

    Arguments:
        path -- Path of the ZIP file
        extract_path -- Path of the destination

    Returns:
        A list of filenames containing all extracted files
    '''

    zipf = zipfile.ZipFile(path, 'r')
    member_list = zipf.namelist()

    # If the result folder does not exist, create it.
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    zipf.extractall(extract_path)
    zipf.close()

    # Prepend the extract_path to all extracted file paths.
    member_list = [extract_path + x for x in member_list]

    return member_list


def make_zip(arg_list, name='tmp_container', subpath_to_remove=''):
    '''Make a ZIP file containing everything in arg_list

    Arguments:
        arg_list -- List of files to be ZIP'd

    Keyword Arguments:
        name -- Name of the resulting ZIP file (default: {'tmp_container'})
        subpath_to_remove -- Remove subpaths (default: {''})

    Returns:
        Name of the resulting ZIP file
    '''

    with zipfile.ZipFile(name + '.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for arg in arg_list:
            zipf.write(arg, arg.replace(subpath_to_remove, ''))
    return name + '.zip'


def insert_to_line(line, appendix):
    '''Inserts appendix to line

    Arguments:
        line -- The line to be changed
        appendix -- String to inerst into line

    Returns:
        The changed line
    '''

    # We assume, that '|' is part of the line.
    line = line.split('|')
    line[0] = line[0].strip('\n')
    line[0] = line[0] + ' ' + appendix

    # if '|' is not available, just return.
    if len(line) == 1:
        line[0] = line[0] + '\n'
        return line[0]

    ret_line = line[0] + '|' + line[1]
    return ret_line


def parse_available_servers(rhizome, own_sid, originator_sid=None):
    '''This function iterates through all RPC offers and parses them
    into servers

    Arguments:
        rhizome -- Pyserval Rhizome connection
        own_sid -- SID of the caller of this function
        originator_sid -- SID of the originator a particular call.

    Returns:
        A list containing all found servers excluding self and originator
    '''

    # Get all bundles and check if any available. If not, just return.
    bundles = rhizome.get_bundlelist()
    if not bundles:
        return None

    server_list = []
    for bundle in bundles:
        # We are only intereseted in RPC offers.
        if not bundle.manifest.service == OFFER:
            continue

        # Make sure, that we can not call ourself.
        if bundle.manifest.sender == own_sid:
            continue

        # Do not call a procedure on the node which wants it to be offloaded...
        if bundle.manifest.sender == originator_sid:
            continue

        jobs = []
        capabilities = {}

        # We found an offer from a remote server. Start parsing.
        offers = rhizome.get_payload(bundle).decode('utf-8').split('\n')
        for offer in offers:
            # There are two lines containing :, which introduce new
            # sections of the file. These can be skipped.
            if ':' in offer:
                continue

            # If = is not in the line, than we have a procedure to be parsed
            if '=' not in offer:
                jobname = offer.split(' ')[0]
                jobarguments = offer.split(' ')[1:]
                jobs.append(
                    Job(server=bundle.manifest.name,
                        procedure=jobname,
                        arguments=jobarguments))
            else:
                # If there is a =, then we have a capability.
                _type, _value = offer.split('=')
                if _type == 'gps_coord':
                    # Location is a special case. We need to compute our
                    # own distance to the server distance, which will be stored
                    x1, y1 = _value.split(',')
                    x2 = y2 = None
                    with open(CONFIGURATION['location']) as coord_file:
                        x2, y2 = coord_file.readline().split(' ')
                        _value = math.sqrt((float(x1) - float(x2))**2 +
                                           (float(y1) - float(y2))**2)

                capabilities[_type] = _value

        # After parsing, create a Server object and store it in the result list
        server_list.append(
            Server(bundle.manifest.name, jobs=jobs, **capabilities))

    return server_list


def find_available_servers(servers, job):
    '''Function for finding server, which is offers the procedure and
    is able to execute it

    Arguments:
        servers -- List of available servers
        job -- The job to be executed

    Returns:
        List of servers, which offer and are able to execute the procedure.
    '''

    server_list = []
    for server in servers:
        # If the job has no capabilities, just execute it.
        if not job.filter_dict:
            server_list.append(server)
            continue

        for offered_job in server.jobs:
            # This is not the procedure we are looking for, so skip this.
            if offered_job.procedure != job.procedure or len(
                    offered_job.arguments) != len(job.arguments):
                continue

            # If we found a server offering the procedure, check if it is
            # capable to execute it.
            fullfills = True
            for requirement in job.filter_dict:
                capability = getattr(server, requirement)
                # If the server has no restrictions regarding this particular
                # requirement, just add the server to the result list.
                if not capability:
                    server_list.append(server)
                    continue

                requirement_value = job.filter_dict[requirement]

                if requirement == 'cpu_load' and int(capability) > int(
                        requirement_value):
                    fullfills = False
                    break
                if requirement == 'disk_space' and int(capability) < int(
                        requirement_value):
                    fullfills = False
                    break
                if requirement == 'memory' and int(capability) < int(
                        requirement_value):
                    fullfills = False
                    break
                if requirement == 'gps_coord' and int(
                        capability) > requirement_value:
                    fullfills = False
                    break

            if fullfills:
                server_list.append(server)

    return server_list


def replace_any_to_sid(job_file_path, linecounter, sid):
    '''Replaces 'any' with a concrete SID

    Arguments:
        job_file_path -- Path to the job file to be changed
        linecounter -- Line of the job file to be changed
        sid -- SID to be set
    '''

    with open(job_file_path, 'r+') as job_file:
        lines = job_file.readlines()
        lines[linecounter] = lines[linecounter].replace('any', sid)

        # Changes are so far only in-memory, so write it to disk.
        job_file.seek(0)
        for line in lines:
            job_file.write(line)
        job_file.close()


def serval_running():
    '''Check if Serval is running

    Returns:
        True, if Serval is running, False otherwise
    '''

    try:
        Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        ).keyring.get_identities()
    except requests.exceptions.ConnectionError:
        LOGGER.critical(' | Serval is not running. Start with \'servald start\'')
        return False
    return True


def is_server_address(sid):
    '''Checks if a SID is a valid server SID.
    A server SID can be 64 char hex or 'any'

    Arguments:
        sid -- SID to be checked

    Returns:
        True, if is server SID, False otherwise
    '''

    if sid == 'any' or (all(hex_char in string.hexdigits
                            for hex_char in sid) and len(sid) == 64):
        return True

    return False


def parse_jobfile(job_file_path):
    '''Parser for the job file

    Arguments:
        job_file_path -- Path to the job file

    Returns:
        Jobfile object containing all jobs from the file
    '''

    # Open the job file and read all lines
    job_file = open(job_file_path, 'r+')
    lines = job_file.readlines()

    # First, we see, if the client SID is on the first line. If not,
    # we stop the execution here.
    client_sid = None
    split_first_line = lines[0].split('=')
    try:
        if split_first_line[0] == 'client_sid':
            client_sid = split_first_line[1].strip('\n')
            # It seems, that we have a valid first line, but is it also a
            # valid SID?
            if not (all(hex_char in string.hexdigits
                        for hex_char in client_sid) and len(client_sid) == 64):
                job_file.close()
                return None
    except IndexError:
        job_file.close()
        return None

    # Create an empty Jobfile object. This will be filled in further execution.
    jobs = Jobfile(client_sid)

    counter = 1
    for line in lines[1:]:
        # The first thing to check for comments. Comments in job files start
        # with a '#'. Only line comments are allowed.
        # Empty lines are also skipped.
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
                global_filter = [
                    cap_filter for cap_filter in global_filter
                    if cap_filter != ''
                ]

            # Split the filter at '=' and store the type and the
            # corresponding filter value.
            for cap_filter in global_filter:
                filter_type = cap_filter.split(':')

                # Just a very basic sanity check.
                if filter_type == '':
                    continue
                elif filter_type[0] in filter_keywords:
                    # We only store some defined filters.
                    jobs.add_filter(filter_type[0], filter_type[1])

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
                possible_filters = [
                    arg for arg in possible_filters if arg != ''
                ]

            for filter_arg in possible_filters:
                fil = filter_arg.split(':')

                # Again, simple sanity check.
                if fil == '':
                    continue

                elif fil[0] in filter_keywords:
                    filter_dict[fil[0]] = fil[1]
                    continue

            jobs.add(possible_sid, procedure_name, procedure_args, status,
                     counter, filter_dict)
        else:
            jobs.add(possible_sid, procedure_name, procedure_args, status,
                     counter)

        counter += 1

    job_file.close()
    return jobs
