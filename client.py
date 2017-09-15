# -*- coding: utf-8 -*-
'''DTN-RPyC client.

This module contains all functions needed by the DTN-RPyC client, especially
the call functions.
'''

import time

import restful
import utilities
from utilities import pdebug, pfatal, pinfo, CALL, ACK, RESULT, ERROR, CLEANUP


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

def client_find_server(rhiz, name, args):
    '''Searches a server, which offers the desired procedure.
    Args:
        rhiz (Rhizome):     Rhizome connection to Serval
        name (str):         The name of the desired procedure
        args (list(str)):   All arguments of the procedure in a list.

    Returns:
        str: SID of the server or None, if not found.
    '''

    # If there are no bundles, the are no servers offering anything. Abort.
    bundles = rhiz.get_bundlelist()
    if not bundles:
        return None

    for bundle in bundles:
        if not bundle.service == 'RPC_OFFER':
            continue
        # We found an offer bundle. Therefore download the content...
        offers = rhiz.get_decrypted(bundle.id).split('\n')

        # ... iterate over the lines and see if this is the procedure we searching for.
        for offer in offers:
            procedure = offer.split(' ')
            if procedure[1] == name and len(procedure[2:]) == len(args):
                return bundle.name

    return None

def client_call_dtn(server, name, args):
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
    my_sid = connection.first_identity
    if not my_sid:
        pfatal(
            'The server does not have a SID. Create a SID with' \
            '"servald keyring add" and restart Serval. Aborting.'
        )
        return

    pinfo('Calling procedure \'%s\'.' % name)

    # If the server address is 'any', we have to find a server, which offers this procedure.
    if server == 'any':
        server = client_find_server(rhiz, name, args)
        if not server:
            pfatal('Could not find any server offering the procedure. Aborting.')
            return

    # The server expects the arguments in a single string delimited with '|'.
    joined_args = '|'.join(args)

    # Prepare the call bundle...
    call_bundle = utilities.make_bundle([
        ('type', CALL),
        ('name', name),
        ('args', joined_args),
        ('sender', my_sid.sid),
        ('recipient', server)])

    # ... and the payload. By convention, if the first argument is 'file' and
    # there are exactly two arguments, we assume that the second argument
    # is the path to the file to be sent. This file will be opened and passed
    # as the payload to the insert function.
    # Otherwise, the payload will be empty.
    payload = ''
    if args[0] == 'file' and len(args) == 2:
        payload = open(args[1], 'rb')

    # Insert the call payload, i.e. call the remote procedure.
    rhiz.insert(call_bundle, payload, my_sid.sid)

    # Immediatelly after the insert, get the token from the store,
    # to not parse the entire bundlelist.
    token = rhiz.get_bundlelist()[0].__dict__['.token']

    # Start the waiting loop, until the result arrives.
    pinfo('Waiting for result.')
    result_received = False
    while not result_received:
        bundles = rhiz.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if bundle.service == 'RPC':
                    potential_result = rhiz.get_manifest(bundle.id)

                    if rpc_for_me(potential_result, name, joined_args, my_sid.sid):

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
                            clear_bundle = utilities.make_bundle([
                                ('type', CLEANUP),
                                ('name', name),
                                ('args', args),
                                ('sender', my_sid.sid),
                                ('recipient', server)])
                            rhiz.insert(clear_bundle, '', my_sid.sid, call_bundle.id)

                            pinfo('Received result: %s' % result_str)
                            result_received = True

                        if potential_result.type == ERROR:
                            pfatal(
                                'Received error response with the following message: %s' \
                                % potential_result.result
                            )
                            result_received = True

        time.sleep(1)
