# -*- coding: utf-8 -*-
'''DTN-RPyC client.

This module contains all functions needed by the DTN-RPyC client, especially
the call functions.
'''
# TODO refactor
# TODO global filter in jobfile
#

import os
import random
import time
import math
from pyserval.client import Client
import utilities
from utilities import pdebug, pfatal, pinfo, CALL, ACK, RESULT, ERROR, CLEANUP, CONFIGURATION
import threading
import sys
import filter_servers
from job import Status, Job, FileNotFound

my_sid = None
PROCEDURES = {}


def rpc_for_me(potential_result, name, args, sid):
    ''' Helper function to decide if the received RPC result is for the client.

    Args:
        potential_result (bundle):  The bundle which has to be checked.
        name (str):                 Name of the called procedure.
        args (str):                 Arguments of the called procedure.
        sid (str):                  The hex representation of the clients SID.

    Returns:
        bool: The return value. True for success, False otherwise.
    '''
    return potential_result.name == name \
        and potential_result.args == args \
        and potential_result.recipient == sid

def client_find_procedures(rhizome):
    bundles = rhizome.get_bundlelist()
    if not bundles:
        return None
    for bundle in bundles:
        if not bundle.service == utilities.OFFER:
            continue
        # We found an offer bundle. Therefore download the content...
        offers = rhizome.get_decrypted(bundle.id).split('\n')
        # ... iterate over the lines and see if this is the procedure we searching for.
        if bundle.name not in PROCEDURES:
            PROCEDURES[bundle.name] = []
            for offer in offers:
                procedure = offer.split(' ')
                if procedure[0] == '':
                    break
                # save procedures and argument types in a dict for later usage
                PROCEDURES[bundle.name].append(procedure[1:])

def client_find_server(rhizome, name, args, server=False):
    # If there are no bundles, the are no servers offering anything. Abort.
    bundles = rhizome.get_bundlelist()
    pdebug(bundles)
    if not bundles:
        return None
    server_list = []
    for bundle in bundles:
        if not bundle.manifest.service == utilities.OFFER:
            continue
        # We found an offer bundle. Therefore download the content...
        offers = rhizome.get_payload(bundle).split('\n')
        # ... iterate over the lines and see if this is the procedure we searching for.
        for offer in offers:
            procedure = offer.split(' ')
            if procedure[0] == '':
               break
            if procedure[1] == name and bundle.manifest.name:
            #if procedure[1] == name ''' and len(procedure[2:]) == len(args)''' and bundle.name:
                if server:
                    if bundle.manifest.name not in server_list:
                        server_list.append(bundle.manifest.name)
                else:
                    return bundle.manifest.name
            else:
                continue
        return server_list

def choose_server(server_list, distribution='R', joblist=None, my_sid=None):
    '''
    Returns a server from a list by a given distribution:
        R: Random server
        E: Even distributed ~ choose a server which hasnt executed a procedure yet
        F: First server
    '''

    if my_sid is not None and my_sid.sid in server_list:
        server_list.remove(my_sid.sid)

    if len(server_list) == 0:
        return []

    if distribution == 'R':
        pdebug('Choose random server')
        _random = random.randint(0, len(server_list)-1)
        return server_list[_random]
    elif distribution == 'E':
        pdebug('Choose a server even distributed')
        if joblist == None:
            return choose_server(server_list, distribution='R', my_sid=my_sid)
        # get all server which executed a procedure already
        p_set = set()
        s_set = set()
        for job in joblist:
            if job.status == Status.DONE:
                p_set.add(job.server)
        for server in server_list:
            s_set.add(server)
        # convert to list again
        serverset = list(s_set.difference(p_set))
        if len(serverset) > 0:
            return serverset[-1]
        # all servers executed a procedure -> start over but random
        if len(server_list) > 0 and len(p_set) > 0:
            return choose_server(server_list, distribution='R', my_sid=my_sid)
    else:
        pdebug('Choose the last server in the list')
        return server_list[-1]

def client_call_cc_dtn(server, name, args, file=None, filter=None, job=True):
    ''' Main calling function for cascading job distribution in DTN mode.

    Args:
        server (str):           Hex representation of the server(s).
        name (str):             Name of the desired procedure.
        args (list of strings): Arguments of the desired procedure.
    '''
    # Create a RESTful serval_client to Serval with the parameters from the config file
    # and get the Rhizome serval_client.
    serval_client = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = serval_client.rhizome

    # Get the first SID found in Keyring.
    # Recent versions of Serval do not have a SID by default, which has to be
    # handled. Therefore, check if we could get a SID.
    global my_sid
    my_sid = serval_client.first_identity
    if not my_sid:
        pfatal(
            'The server does not have a SID. Create a SID with' \
            '"servald keyring add" and restart Serval. Aborting.'
        )
     # If the server address is 'any', we have to find a server, which offers this procedure.
    if server == 'any':
        server = filter_servers.client_find_server(rhizome, filter, name) if (filter is not None) else client_find_server(rhizome, name, args)
        if server is None or len(server) == 0:
            pfatal('Could not find any server offering the procedure. Aborting.')
            return
    # write job task file if its in cascading mode
    elif type(server) is list and len(server) > 1 and file != None:
        if len(server) != len(name):
            pdebug("Not creating jobfile")
        else:
            pinfo('Creating cascading jobfile')
            timestamp = str(math.floor(time.time()))
            f = open('jobtask_' + my_sid.sid + '_' + timestamp + '.jb', 'w+')
            f.write("client_sid=" + my_sid.sid + '\n')
            for x in range(len(server)):
                if  x < len(args):
                    f.write(server[x] + ' ' + name[x] + ' ' + args[x] + '\n')
                else:
                    f.write(server[x] + ' ' + name[x] + '\n')
            f.close()

    # The server expects the arguments in a single string delimited with '|'.
    joined_args = '|'.join(args)

    call_bundle_fields = [
        ('type', CALL),
        ('name', name),
        ('args', joined_args),
        ('sender', my_sid.sid)
    ]

    # If this is an 'all' or 'broadcast' call, we must not provide sender and recipient.
    if not server == 'all' and not server == 'broadcast':
        if type(server) is list and len(server) > 1:
            # send jobfile to the first server
            if file != None:
                jobfile = ['file', file]
            else:
                jobfile = ['file', 'jobtask_' + my_sid.sid + '_' + timestamp + ".jb"]
                args = jobfile
            pdebug('prepared cascading jobfile')
            if not job:
                call_bundle_fields = [
                    ('type', CALL),
                    ('name', 'file'),
                    ('args', 'file'),
                    ('sender', my_sid.sid)
                ]
            else:
                joined_args = 'jobfile'
                call_bundle_fields = [
                    ('type', CALL),
                    ('name', 'file'),
                    ('args', 'jobfile'),
                    ('sender', my_sid.sid)
                ]
            call_bundle_fields.append(('recipient', server[0]))
            server_list = server[-1]
        else:
            call_bundle_fields.append(('recipient', server))
            server_list = [server]
    # Find all servers which can execute the given procedure
    else:
        server_list = filter_servers.client_find_server(rhizome, filter, name) if (filter is not None) else client_find_server(rhizome, name, args)
    # Prepare the call bundle
    # Now the callbundle can be build.
    call_bundle = utilities.make_bundle(call_bundle_fields)

    # ... and the payload. By convention, if the first argument is 'file' and
    # there are exactly two arguments, we assume that the second argument
    # is the path to the file to be sent. This file will be opened and passed
    # as the payload to the insert function.
    # Otherwise, the payload will be empty.
    payload = ''
    if args[0] == 'file' and len(args) == 2:
        pdebug("Payload is set to file")
        payload = open(args[1], 'rb')

    # Insert the call payload, i.e. call the remote procedure.
    ibundle = rhizome.insert(call_bundle, payload, my_sid.sid)
    ibundle = ibundle.split('\n')
    bundle_list = []
    for val in ibundle:
        val = val.split('=')
        if len(val) > 1:
            bundle_list.append((val[0], val[1]))
    #print('Bundle: -- ' + ibundle)
    # Immediatelly after the insert, get the token from the store,
    # to not parse the entire bundlelist.
    token = rhizome.get_bundlelist()[0].__dict__['.token']

    ack_received = False
    pdebug('Server is waiting for ACK.')
    while not ack_received:
        bundles = rhizome.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if not bundle.service == 'RPC':
                    continue

                # Before further checks, we have to download the manifest
                # to have all metadata available.
                potential_result = rhizome.get_manifest(bundle.id)
                if type(name) is list:
                    name = name[-1]

                if not rpc_for_me(potential_result, name, joined_args, my_sid.sid):
                    continue

                # At this point, we know that there is a RPC file in the store
                # and it is for us. Start parsing.
                if potential_result.type == ACK:
                    pinfo('Received ACK. Preparing cleanup.')
                    ack_received = True
                    break
    newbundle = utilities.make_bundle(bundle_list, False)
    return (newbundle, bundle.id)

def update_file(file, linecounter, sid):
    f = open(file, 'r+')
    if f is not None:
        lines = f.readlines()
        lines[linecounter] = lines[linecounter].replace('any', sid)
        f.seek(0)
        for line in lines:
            f.write(line)
        f.close()
        return True
    else:
        return False

def create_jobfile(server_list, name_list, arg_list, filter_list = None):
    '''Creates a jobfile with the following arguments:
        serverlist (list of strings): predefined servers.
        name_list (list of procedures): given procedures.
        arg_list (list of string): defined procedures.
        filter_list (dict of filters): optional global filters
        '''
        # TODO needs filter implementation
    if len(server_list) != len(name_list):
        pfatal("More or less jobs are found for each server. Aborting!")
        return None
    pinfo('Creating cascading jobfile')
    timestamp = str(math.floor(time.time()))
    filename = 'jobtask_' + my_sid.sid + '_' + timestamp + '.jb'
    f = open('jobtask_' + my_sid.sid + '_' + timestamp + '.jb', 'w+')
    f.write('client_sid=' + my_sid.sid + '\n')
    for x in range(len(server_list)):
        if  x < len(arg_list):
            f.write(server_list[x] + ' ' + name_list[x] + ' ' + arg_list[x] + '\n')
        else:
            f.write(server_list[x] + ' ' + name_list[x] + '\n')
    f.close()
    return filename

def client_call_dtn(server=None, name=None, args=None, timeout=None, filter=None, jobfile=None, delete=False):
    ''' Main calling function for DTN mode.

    Args:
        server (str):           Hex representation of the server.
        name (str):             Name of the desired procedure.
        args (list of strings): Arguments of the desired procedure.
    '''
    filename = None
    server_list = None
    if not ((server is None and name is None and args is None) ^ (jobfile is None)):
        pdebug("Nothing true")
        return
    # Create a RESTful serval_client to Serval with the parameters from the config file
    # and get the Rhizome serval_client.
    serval_client = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = serval_client.rhizome

    # Get the first SID found in Keyring.
    # Recent versions of Serval do not have a SID by default, which has to be
    # handled. Therefore, check if we could get a SID.
    global my_sid
    my_sid = serval_client.keyring.default_identity()

    # If the server address is 'any', we have to find a server, which offers this procedure.
    if server == 'any':
        if filter is not None:
            server = filter_servers.client_find_server(rhizome, filter, name)
        else:
            server = client_find_server(rhizome, name, args)
        pdebug(server)
        first_job = Job(server, name, args, 'OPEN', 0, filter)

        if filter is not None:
            server = filter_servers.parse_server_caps(server, filter)
        if not server:
            pfatal('Could not find any server offering the procedure.')
            return
    # write job task file if its in cascading mode
    elif type(server) is list:# and len(server) > 1:
        filename = create_jobfile(server, name, args, filter)
        job = utilities.parse_jobfile(filename)
        if filename is None:
            pfatal("An error occurred during jobfile creation.")
            return None
    elif jobfile is not None:
        # parse the jobfile
        job = utilities.parse_jobfile(jobfile)
        if job.client_sid is None:
            # write own sid into the file
            pdebug('writing csid into jobfile')
            f = open(jobfile, 'r+')
            if f is not None:
                #check each line
                lines = f.readlines()
                lines.insert(0, "client_sid=" + my_sid.sid + "\n")
                f.seek(0)
                for line in lines:
                    f.write(line)
                f.close()
                job=utilities.parse_jobfile(jobfile)

        if job is None:
            pfatal("An error occurred during jobfile parsing.")
            return None
        for j in job.joblist:
            j.job_print()
    # The server expects the arguments in a single string delimited with '|'.
    # FIXME args are not set
    if args is not None:
        joined_args = '|'.join(args)

    call_bundle_fields = [
        ('type', CALL)
    ]

    # change code below to accept the new data structure
    # If this is an 'all' or 'broadcast' call, we must not provide sender and recipient.
    recipient = None
    if jobfile is not None:
        filename = jobfile
        first_job = job.joblist[0]
    if not server == 'all' and not server == 'broadcast':
        if (type(server) is list and len(server) > 1) or jobfile is not None:
            # send jobfile to the first server
            jobfile = ['file', filename]
            #procedure_name = name
            args = jobfile
            name = 'file'
            joined_args = 'jobfile'
            pdebug('prepared cascading jobfile')
            #recipient = server[0]
            if first_job.filter_dict:
                filter = first_job.filter_dict
            recipient = first_job.server
            if recipient == 'any':
                pdebug('Finding a server')
                server = filter_servers.client_find_server(rhizome, filter, first_job.procedure) if (bool(filter)) else client_find_server(rhizome, first_job.procedure, args)
                if filter is not None:
                    server = list(filter_servers.parse_server_caps(server, filter))
                if server is None or len(server) == 0:
                    pfatal("No server(s) found")
                    return None
                recipient = server
                # obsolete
                if type(recipient) is list:
                    recipient = recipient[0]
                # update the file
                update_file(filename, first_job.line, recipient)
                # check if this is still necessary
                if type(server) is list:
                    server_list = server[-1]
                else:
                    server_list = server
        else:
            recipient = server
            server_list = [server]
    # Find all servers which can execute the given procedure
    else:
        server_list = filter_servers.client_find_server(rhizome, filter, name) if (bool(filter)) else client_find_server(rhizome, name, args)
        if filter is not None:
            server_list = filter_servers.parse_server_caps(server, filter)
    # Prepare the call bundle
    # Now the callbundle can be build.
    call_bundle_fields.append(('name', name))
    call_bundle_fields.append(('args', joined_args))
    call_bundle_fields.append(('sender', my_sid.sid))
    # TODO recipient is either a list or a set for some reason
    if recipient is not None:
        if type(recipient) is list:
            recipient = recipient[0]
        elif type(recipient) is set:
            for x in recipient:
                recipient = x
                break
    call_bundle_fields.append(('recipient', recipient))
    call_bundle = utilities.make_bundle(call_bundle_fields)

    # check if an argument is a file
    argument_type = []
    client_find_procedures(rhizome)
    for key in PROCEDURES:
        if key == recipient:
            for procedure in PROCEDURES[key]:
                if procedure[0] == first_job.procedure:
                    argument_type = procedure

    # ... and the payload. By convention, if the first argument is 'file' and
    # there are exactly two arguments, we assume that the second argument
    # is the path to the file to be sent. This file will be opened and passed
    # as the payload to the insert function.
    # Otherwise, the payload will be empty.
    payload = ''
    if args[0] == 'file' and len(args) == 2 or 'file' in argument_type:
        pdebug("Payload is set to file")
        # set the payload to file
        c = 1
        zip_list = []
        if filename is None:
            filename = args[0]
        #args.insert(0, 'file')
        if argument_type is not None and 'file' in argument_type and len(args[1:]) == 1: # TODO more arguments
            # FIXME might be obsolete
            if len(args) != len(argument_type):
                pfatal('Argc doesnt match')
                return
            # FIXME obsolte?
            for argc in range(len(argument_type)):
                if argument_type[argc] == 'file':
                    c = argc
                    if not os.path.isfile(first_job.arguments[0]):
                        pfatal("File '%s' not found. Aborting" % first_job.arguments[0])
                        return
                    zip_list = first_job.arguments
        # create zipfile
        zip_list.append(filename)
        zip_list = list(map(str.strip, zip_list))
        zip_file = utilities.make_zip(zip_list, my_sid.sid + '_' + str(math.floor(time.time())))

        payload = open(zip_file, 'rb')

    # Insert the call payload, i.e. call the remote procedure.
    mybundle = rhizome.insert(call_bundle, payload, my_sid.sid)

    # save bundle id for cleanup
    mybundle = mybundle.split('\n')
    bundle_list = []
    for val in mybundle:
        val = val.split('=')
        if len(val) > 1:
            bundle_list.append((val[0], val[1]))
    mybundle = utilities.make_bundle(bundle_list, False)
    mymanifest = rhizome.get_manifest(mybundle.id)

    # Immediatelly after the insert, get the token from the store,
    # to not parse the entire bundlelist.
    token = rhizome.get_bundlelist()[0].__dict__['.token']

    # delete zip if its exist
    if args[0] == 'file' and len(args) == 2 or 'file' in argument_type:
        if not delete and os.path.isfile(zip_file):
            pdebug('Deleting ' + zip_file)
            os.remove(zip_file)
    # Start the waiting loop, until the result arrives.
    if timeout:
        pinfo('Waiting for result for ' + timeout + ' seconds.')
    else:
        pinfo('Waiting for result.')
    result_received = False
    counter = 0

    global thread_expired
    thread_expired = False

    def waitThread():
        global thread_expired
        thread_expired = True
        pfatal(
            'Time expired'
        )
        sys.exit(1)
    if timeout:
        global t
        t = threading.Timer(int(timeout), waitThread)
        t.start()

    if server_list is None:
        server_list = ['any']

    while not (result_received or counter == len(server_list)):
        bundles = rhizome.get_bundlelist(token=token)

        if thread_expired:
            break

        if bundles:
            for bundle in bundles:
                if thread_expired:
                    break

                if result_received and server is not 'all' and server is not 'broadcast':
                    break
                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if not bundle.service == 'RPC':
                    continue

                # Before further checks, we have to download the manifest
                # to have all metadata available.
                potential_result = rhizome.get_manifest(bundle.id)

                if not rpc_for_me(potential_result, name, joined_args, my_sid.sid):
                    continue

                # At this point, we know that there is a RPC file in the store
                # and it is for us. Start parsing.
                if potential_result.type == ACK:
                    pinfo('Received ACK. Will wait for result. Preparing cleanup')
                    # TODO CLEANUP
                    clear_bundle = utilities.make_bundle([('type', CLEANUP)], True)
                    #rhizome.insert(clear_bundle, '', my_sid.sid, mymanifest.id)


                if potential_result.type == RESULT:
                    # It is possible, that the result is a file.
                    # Therefore, we have to check the result field in the bundle.
                    # If it is a file, download it and return the path to the
                    # downloaded file.
                    # Otherwise, just return the result.
                    result_str = ''
                    if potential_result.result == 'file':
                        path = '/tmp/%s_%s' % (name, potential_result.version)
                        rhizome.get_decrypted_to_file(potential_result.id, path)
                        result_str = path
                    else:
                        result_str = potential_result.result

                    # The final step is to clean up the store.
                    # Therefore, we create a new bundle with an
                    # empty payload and CLEANUP as the type.
                    # Since the BID is the same as in the call,
                    # the call bundle will be updated with an empty file.
                    # This type will instruct the server to clean up
                    # the files involved during this RPC.
                    #if server != 'all' or server != 'broadcast':
                    clear_bundle = utilities.make_bundle([
                        ('type', CLEANUP),
                        ('name', name),
                        ('args', args),
                        ('sender', my_sid.sid)
                    ])
                    # FIXME rework cleanup
                    #rhizome.insert(clear_bundle, '', my_sid.sid, call_bundle.id)

                    pinfo('Received result: %s' % result_str)
                    # If the call was broadcastet, we do not want to stop here.
                    if server == 'all' or server == 'broadcast':
                        counter = counter + 1
                        pinfo("Received result " + str(counter) + "/" + str(len(server_list)))
                        continue

                    result_received = True

                if potential_result.type == ERROR:
                    pfatal('Bundle id: %s' % bundle.id)
                    pfatal(
                        'Received error response with the following message: %s' \
                        % potential_result.result
                    )

                    # If the call was broadcastet, we do not want to stop here.
                    if server == 'all' or server == 'broadcast':
                        continue

                    result_received = True

        time.sleep(1)


def signal_handler(_, __):
    ''' Just a simple CTRL-C handler.
    '''
    global t
    t.cancel()
    utilities.pwarn('Stopping DTN-RPyC and the timeout thread.')
    sys.exit(0)
