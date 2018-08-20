'''Contains helper functions and variables.
'''
import os
import string
import requests
import restful
import rhizome
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
        print(CONFIGURATION['bins'])
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
    #bundle = rhizome.Bundle(None, None)
    bundle = rhizome.Bundle()
    for prop in manifest_props:
        bundle.__dict__[prop[0]] = prop[1]

    if rpc_service:
        default_props = [('service', 'RPC')]
        for prop in default_props:
            bundle.__dict__[prop[0]] = prop[1]

    return bundle

def extract_zip(path, bundle_id, my_sid):
    zipf = zipfile.ZipFile(path, 'r')
    member_list = zipf.namelist()
    # prepare path
    if not os.path.exists('/tmp/' + bundle_id):
        try:
            os.makedirs('/tmp/' + bundle_id)
        except OSError as e:
            if e.errno != errno.EXIST:
                raise
    npath = '/tmp/' + bundle_id + '/'
    zipf.extractall(npath +'/')
    zipf.close()
    member_list = [npath + x for x in member_list]
    return member_list

def is_zipfile(file):
    return zipfile.is_zipfile(file)

def make_zip(arg_list, name='tmp_container'):
    ''' Creates a zip archive with all information a server needs to execute a job
    Args:
        arg_list (list of strings): The list of files to make a archive from.
        name (string): An optionally name for the zip archive.
    '''
    # check if all files exist
    current_time = str(math.floor(time.time()))
    # adding a timestamp file for uniqueness
    e = open('time.txt', 'w')
    e.write(current_time)
    e.close()
    arg_list.append('time.txt')

    for arg in arg_list:
        if not os.path.isfile(arg):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), arg)

    # writing the zipfile
    zipf = zipfile.ZipFile(name+'.zip', 'w', zipfile.ZIP_DEFLATED)
    pdebug('Creating zipfile from: %s' % arg_list)
    for arg in arg_list:
        zipf.write(arg, os.path.basename(arg))
    zipf.close()
    return name + '.zip'

def split_join(line, appendix):
    line = line.split('|')
    line[0] = line[0].strip('\n')
    line[0] = line[0] + ' ' + appendix
    if len(line) == 1:
        line[0] = line[0] + '\n'
        return line[0]
    ret_line = line[0] + '|' + line[1]
    return ret_line

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

def is_server_address(sid):
    '''Simple function to check if a SID is realy a SID.
    Args:
        sid (str):  The string to check.

    Returns:
        bool: If the SID is a SID or 64 hex chars or not.
    '''
    if sid == 'any' \
        or sid == 'all' \
        or sid == 'broadcast' \
        or (all(hex_char in string.hexdigits for hex_char in sid) and len(sid) == 64) \
        or (all(hex_char in string.hexdigits for sids in sid for hex_char in sids) and all(len(sids) == 64 for sids in sid)) \
        or (type(sid) is list and (char == 'any' or all(char in string.hexdigits) for sids in sid for char in sids)):
        return True

    pfatal('%s is not a valid server address. Aborting.' % sid)
    return False

# TODO check for global filters
def parse_jobfile(jobfile):
    filter_keywords = ['cpu_cores', 'cpu_load', 'disk_space', 'power_state', 'power_percentage']
    # parse the jobfile if its well formed
    f = open(jobfile, 'r+')
    if f is not None:
        client_sid = None
        #check each line
        lines = f.readlines()
        if lines[0].split('=')[0] == 'client_sid':
            client_sid = lines[0].split('=')[1]
            client_sid = client_sid.strip('\n')
            pdebug("Client sid found: " + client_sid)
            if not (all(hex_char in string.hexdigits for hex_char in client_sid) and len(client_sid) == 64):
                # wrong sid
                pdebug('No sid found')
                client_sid = None
        jbfile = job.Jobfile(client_sid)
        counter = 1
        for x in range(1,len(lines)):
            # ignore comments
            line = lines[x]
            if line[0] == '#' or len(line) == 0 or line == '\n':
                pdebug('found a comment or an empty line')
                counter = counter + 1
                continue
            # check for global filters
            if line[0] == '|':
                gfilter = line.strip('\n')
                gfilter = gfilter.split('|')
                gfilter = gfilter[1].split(' ')
                if '' in gfilter:
                    gfilter = [arg for arg in gfilter if arg != '']
                for filter_arg in gfilter:
                    poss_fil = filter_arg.split('=')
                    if poss_fil == '':
                        continue
                    elif poss_fil[0] in filter_keywords:
                        jbfile.add_filter(poss_fil[0], poss_fil[1])
                        pdebug('Adding global filter: %s=%s' % (poss_fil[0], poss_fil[1]))
                counter = counter + 1
                continue
            # check if clients sid is present
            status = 'OPEN'
            line = line.strip('\n')
            possible_filters = line.split('|')
            line = possible_filters[0].split(' ')
            if '' in line:
                line = [arg for arg in line if arg != '']
            possible_sid = line[0]
            procedure_name = line[1]
            procedure_args = line[2:]
            if '' in procedure_args:
                procedure_args = [arg for arg in procedure_args if arg != '']
            if not is_server_address(possible_sid):
                # parse the rest of the line
                # skip the given process and arguments, but they have to be not None
                pfatal('SID might be malformed: ' + possible_sid)
                f.close()
                return None
            if 'DONE' in procedure_args:
                status = 'DONE'
            elif 'ERROR' in procedure_args:
                status = 'ERROR'
            #pdebug('Possible sid found. ' + possible_sid)
            jbfile.add(possible_sid, procedure_name, procedure_args, status, counter)
            counter += 1
            # adding global filters if they exist
            if bool(jbfile.filter):
                for fil in jbfile.filter:
                    jbfile.joblist[-1].filter_dict[fil] = jbfile.filter[fil]
                    pdebug('Setting [%s:%s]' %  (fil , jbfile.filter[fil]))

            # optional filters
            if len(possible_filters) > 1:
                # filters found
                possible_filters = possible_filters[1].split(' ')
                if '' in possible_filters:
                    possible_filters = [arg for arg in possible_filters if arg != '']
                #print('Possible filters: %s' %possible_filters)
                for filter_arg in possible_filters:
                    filter = filter_arg.split('=')
                    #print('Filter: %s' % filter)
                    if filter == '':
                        continue
                    elif filter[0] in filter_keywords:
                        # adding filter to the dict
                        pdebug('Setting filter: %s' % filter)
                        jbfile.joblist[-1].filter_dict[filter[0]] = filter[1].strip('\n')
                        continue
                    else:
                        pdebug("Filteroption: " + filter[0] + " not found. Disable filter function")
        f.close()
        return jbfile
    else:
        pfatal('Error: '+  jobfile + ' does not exists. Aborting')
        return False

def simple_json_parse(json_list):
    '''Simple function for parsing parentheses in json
    Args:
        json_list (list): the list to check.
    Returns:
        int: amount of parentheses which are missing at the end.
    '''
    parentheses_counter = 0
    brackets_counter = 0
    for element in json_list:
        for char in element:
            char = chr(char)
            if char == '{':
                parentheses_counter += 1
            elif char == '}':
                parentheses_counter -= 1
            elif char == '[':
                brackets_counter += 1
            elif char == ']':
                brackets_counter -= 1

    if parentheses_counter != 0:
        pfatal('PARENTHESES_ERROR')
    if brackets_counter != 0:
        pfatal('BRACKETS_ERROR')
    return (parentheses_counter, brackets_counter)
