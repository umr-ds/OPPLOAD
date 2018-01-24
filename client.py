# -*- coding: utf-8 -*-
'''DTN-RPyC client.

This module contains all functions needed by the DTN-RPyC client, especially
the call functions.
'''

import time
import math
import restful
import utilities
from utilities import pdebug, pfatal, pinfo, CALL, ACK, RESULT, ERROR, CLEANUP
import threading
import sys
import signal

my_sid = None

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

def client_find_server(rhiz, name, args, server = False):
    '''Searches a server, which offers the desired procedure.
    Args:
        rhiz (Rhizome):     Rhizome connection to Serval
        name (str):         The name of the desired procedure
        args (list(str)):   All arguments of the procedure in a list.

    Returns:
        str: SID of the server or None, if not found.
    '''
    global my_sid
    # If there are no bundles, the are no servers offering anything. Abort.
    bundles = rhiz.get_bundlelist()
    if not bundles:
        return None
    server_list = []
    for bundle in bundles:
        if not bundle.service == 'RPC_OFFER':
            continue
        # We found an offer bundle. Therefore download the content...
        offers = rhiz.get_decrypted(bundle.id).split('\n')

        # ... iterate over the lines and see if this is the procedure we searching for.
        for offer in offers:
            procedure = offer.split(' ')
            if procedure[0] == '':
               break
            if procedure[1] == name and len(procedure[2:]) == len(args) and bundle.name:
                if server:
                    if bundle.name not in server_list and bundle.id is not my_sid:
                        server_list.append(bundle.name)
                else:
                    return bundle.name
            else:
                continue
    if len(server_list) > 0:
        return server_list
    else:
        return None

def client_call_cc_dtn(server, name, args, file=None):
    ''' Main calling function for cascading job distribution in DTN mode.

    Args:
        server (str):           Hex representation of the server(s).
        name (str):             Name of the desired procedure.
        args (list of strings): Arguments of the desired procedure.
    '''
    # Create a RESTful connection to Serval with the parameters from the config file
    # and get the Rhizome connection.
    connection = restful.RestfulConnection(
        host=utilities.CONFIGURATION['host'],
        port=int(utilities.CONFIGURATION['port']),
        user=utilities.CONFIGURATION['user'],
        passwd=utilities.CONFIGURATION['passwd']
    )
    rhiz = connection.rhizome

    # Get the first SID found in Keyring.
    # Recent versions of Serval do not have a SID by default, which has to be
    # handled. Therefore, check if we could get a SID.
    global my_sid
    my_sid = connection.first_identity
    if not my_sid:
        pfatal(
            'The server does not have a SID. Create a SID with' \
            '"servald keyring add" and restart Serval. Aborting.'
        )
     # If the server address is 'any', we have to find a server, which offers this procedure.
    if server == 'any':
        server = client_find_server(rhiz, name, args)
        if not server:
            pfatal('Could not find any server offering the procedure. Aborting.')
            return
    # write job task file if its in cascading mode
    elif type(server) is list and len(server) > 1 and file != None:
        if len(server) != len(name):
            pdebug("Not creating cascade file")
        else:
            pinfo('Creating cascading jobfile')
            f = open('jobtask_' + my_sid.sid + '.jb', 'w+')
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
                jobfile = ['file', 'jobtask_' + my_sid.sid + ".jb"]
                args = jobfile
            pdebug('prepared cascading jobfile')
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
        server_list = client_find_server(rhiz, name, args, True)
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
    rhiz.insert(call_bundle, payload, my_sid.sid)

    # Immediatelly after the insert, get the token from the store,
    # to not parse the entire bundlelist.
    token = rhiz.get_bundlelist()[0].__dict__['.token']

    thread_expired = False
    ack_received = False
    counter = 0
    while not ack_received or counter != len(server_list):
        bundles = rhiz.get_bundlelist(token=token)

        if thread_expired:
            break

        if bundles:
            for bundle in bundles:
                if thread_expired:
                    break

                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if not bundle.service == 'RPC':
                    continue

                # Before further checks, we have to download the manifest
                # to have all metadata available.
                potential_result = rhiz.get_manifest(bundle.id)

                if type(name) is list:
                    name = name[-1]
                if not rpc_for_me(potential_result, name, joined_args, my_sid.sid):
                    continue

                # At this point, we know that there is a RPC file in the store
                # and it is for us. Start parsing.
                if potential_result.type == ACK:
                    pinfo('Received ACK. Not my business anymore.')
                    ack_received = True

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

def client_call_dtn(server, name, args, timeout = None):
    ''' Main calling function for DTN mode.

    Args:
        server (str):           Hex representation of the server.
        name (str):             Name of the desired procedure.
        args (list of strings): Arguments of the desired procedure.
    '''

    # Create a RESTful connection to Serval with the parameters from the config file
    # and get the Rhizome connection.
    connection = restful.RestfulConnection(
        host=utilities.CONFIGURATION['host'],
        port=int(utilities.CONFIGURATION['port']),
        user=utilities.CONFIGURATION['user'],
        passwd=utilities.CONFIGURATION['passwd']
    )
    rhiz = connection.rhizome

    # Get the first SID found in Keyring.
    # Recent versions of Serval do not have a SID by default, which has to be
    # handled. Therefore, check if we could get a SID.
    global my_sid
    my_sid = connection.first_identity
    if not my_sid:
        pfatal(
            'The server does not have a SID. Create a SID with' \
            '"servald keyring add" and restart Serval. Aborting.'
        )
        return

    # If the server address is 'any', we have to find a server, which offers this procedure.
    if server == 'any':
        server = client_find_server(rhiz, name, args)
        if not server:
            pfatal('Could not find any server offering the procedure. Aborting.')
            return
    # write job task file if its in cascading mode
    elif type(server) is list:# and len(server) > 1:
        if len(server) != len(name):
            return None
        pinfo('Creating cascading jobfile')
        timestamp = str(math.floor(time.time()))
        f = open('jobtask_' + my_sid.sid + '_' + timestamp + '.jb', 'w+')
        f.write('client_sid=' + my_sid.sid + '\n')
        for x in range(len(server)):
            if  x < len(args):
                f.write(server[x] + ' ' + name[x] + ' ' + args[x] + '\n')
            else:
                f.write(server[x] + ' ' + name[x] + '\n')
        f.close()

    # The server expects the arguments in a single string delimited with '|'.
    joined_args = '|'.join(args)

    call_bundle_fields = [
        ('type', CALL)
    ]

    # If this is an 'all' or 'broadcast' call, we must not provide sender and recipient.
    recipient = None
    if not server == 'all' and not server == 'broadcast':
        if type(server) is list and len(server) > 1:
            # send jobfile to the first server
            jobfile = ['file', 'jobtask_' + my_sid.sid + '_' + timestamp + '.jb']
            procedure_name = name
            procedure_args = args
            args = jobfile
            name = 'file'
            joined_args = 'jobfile'
            pdebug('prepared cascading jobfile')
            recipient = server[0]
            if recipient == 'any':
                recipient = client_find_server(rhiz, procedure_name[0], procedure_args[0].split(' '))
                if type(recipient) is list:
                    recipient = recipient[0]
                update_file('jobtask_' + my_sid.sid + '_' + timestamp + '.jb', 1, recipient)
            server_list = server[-1]
        else:
            recipient = server
            server_list = [server]
    # Find all servers which can execute the given procedure
    else:
        server_list = client_find_server(rhiz, name, args, True)
    # Prepare the call bundle
    # Now the callbundle can be build.
    call_bundle_fields.append(('name', name))
    call_bundle_fields.append(('args', joined_args))
    call_bundle_fields.append(('sender', my_sid.sid))
    if recipient is not None:
        call_bundle_fields.append(('recipient', recipient))

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
    rhiz.insert(call_bundle, payload, my_sid.sid)

    # Immediatelly after the insert, get the token from the store,
    # to not parse the entire bundlelist.
    token = rhiz.get_bundlelist()[0].__dict__['.token']
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

    while not result_received or counter != len(server_list):
        bundles = rhiz.get_bundlelist(token=token)

        if thread_expired:
            break

        if bundles:
            for bundle in bundles:
                if thread_expired:
                    break

                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if not bundle.service == 'RPC':
                    continue

                # Before further checks, we have to download the manifest
                # to have all metadata available.
                potential_result = rhiz.get_manifest(bundle.id)

                if not rpc_for_me(potential_result, name, joined_args, my_sid.sid):
                    continue

                # At this point, we know that there is a RPC file in the store
                # and it is for us. Start parsing.
                if potential_result.type == ACK:
                    pinfo('Received ACK. Will wait for result.')

                if potential_result.type == RESULT:
                    # It is possible, that the result is a file.
                    # Therefore, we have to check the result field in the bundle.
                    # If it is a file, download it and return the path to the
                    # downloaded file.
                    # Otherwise, just return the result.
                    result_str = ''
                    if potential_result.result == 'file':
                        path = '/tmp/%s_%s' % (name, potential_result.version)
                        rhiz.get_decrypted_to_file(potential_result.id, path)
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
                    rhiz.insert(clear_bundle, '', my_sid.sid, call_bundle.id)

                    pinfo('Received result: %s' % result_str)

                    # If the call was broadcastet, we do not want to stop here.
                    if server == 'all' or server == 'broadcast':
                        counter = counter + 1
                        pinfo("Received result " + str(counter) + "/" + str(len(server_list)))
                        continue

                    result_received = True

                if potential_result.type == ERROR:
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
