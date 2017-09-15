'''Main Server module.
'''
import time
import subprocess
import os
from _thread import start_new_thread

import restful

import utilities
from utilities import pinfo, pfatal, pwarn
from utilities import ACK, CALL, CLEANUP, ERROR, RESULT

# Status indicators for the server
RUNNING = True
STOPPED = False
SERVER_MODE = STOPPED

# List of offered procedures.
OFFERED_PROCEDURES = None

# A dict where all bundles are stored which have to be cleaned up after execution.
CLEANUP_BUNDLES = {}

class Procedure(object):
    '''A simple procedure class.
    Args:
        return_type (str):  The return type for a procedure
        name (str):         The name for the procedure
        args (list(str)):   All arguments for the procedure
    '''
    def __init__(self, return_type=None, name=None, args=None):
        self.return_type = return_type
        self.name = name
        self.args = args

def get_offered_procedures(rpc_defs):
    '''Parses the rpc definitions file and stores all of them in a list.
    Args:
        rpc_defs (str): The path to the definitions file

    Returns:
        list(Procedure): A list of parsed Procedures.
    '''
    offered_procedures = []
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            procedure_definition_list = procedure_definition.split(' ')
            return_type = procedure_definition_list[0]
            name = procedure_definition_list[1]
            args = procedure_definition_list[2:]
            offered_procedures.append(Procedure(return_type=return_type, name=name, args=args))

    return offered_procedures

def server_offering_procedure(procedure):
    '''Checks, if the given procedure is offered by the server.
    Args:
        procedure (Procedure): The procedure to check.

    Returns:
        bool: True, if the procedure is offered, false otherwise.
    '''
    for offered_procedure in OFFERED_PROCEDURES:
        if offered_procedure.name == procedure.name \
            and len(offered_procedure.args) == len(procedure.args):

            procedure.return_type = offered_procedure.return_type
            bin_path = "%s/%s" % (utilities.CONFIGURATION['bins'], procedure.name)
            if not os.path.exists(bin_path) or not os.access(bin_path, os.X_OK):
                pwarn('Server is offering procedure \'%s\', ' \
                        'but it seems the binary %s/%s is not present or it is not executable. ' \
                        'Will not try to execute.' \
                        % (procedure.name, utilities.CONFIGURATION['bins'], procedure.name)
                     )
                return False
            pinfo('Offering procedure \'%s\'.' % procedure.name)
            return True
    pwarn('Not offering procedure \'%s\'. Waiting for next call.' % procedure.name)
    return False

def server_parse_call(call):
    '''Parse the incomming call.
    Args:
        call (Bundle): The call to be parsed.

    Returns:
        Procedure: The parsed procedure.
    '''
    return Procedure(name=call.name, args=call.args.split('|'))

def server_execute_procedure(procedure):
    '''Main execution function.
    Args:
        procedure (Procedure): The procedure to be executed.

    Returns:
        (int, str): Returns a tuple containing (return code, stdout)
    '''
    pinfo('Starting execution of \'%s\'.' % procedure.name)

    bin_path = os.getcwd() + '/' + utilities.CONFIGURATION['bins'] + '/%s %s'
    procedure_process = subprocess.Popen(
        bin_path % (procedure.name, ' '.join(procedure.args)), shell=True, stdout=subprocess.PIPE
    )
    out, _ = procedure_process.communicate()

    if procedure_process.returncode != 0:
        pwarn('Execution of \'%s\' was not successfull. Will return error %s' \
            % (procedure.name, out))
        return (1, out)
    else:
        pinfo('Execution of \'%s\' was successfull with result %s' % (procedure.name, out))
        return (0, out.rstrip())

def server_handle_call(potential_call, rhiz, my_sid):
    '''Main handler function for an incoming call.
    Args:
        potential_call (Bundle):    The potential call, which has to be handles.
        rhiz (Rhizome):             The Rhizome connection
        my_sid (ServalIdentity):    ServalIdentity of the server
    '''
    pinfo('Received call. Will check if procedure is offered.')

    # First step, parse the potential call.
    procedure = server_parse_call(potential_call)

    if server_offering_procedure(procedure):
        # If the server offers the procedure,
        # we first have to download the file because it will be removed as soon we send the ack.
        if procedure.args[0] == 'file':
            path = '/tmp/%s_%s' % (procedure.name, potential_call.version)
            rhiz.get_decrypted_to_file(potential_call.id, path)
            procedure.args[1] = path

        # Compile and insert the ACK bundle.
        ack_bundle = utilities.make_bundle([
            ('type', ACK),
            ('name', potential_call.name),
            ('sender', potential_call.recipient),
            ('recipient', potential_call.sender),
            ('args', potential_call.args)])
        rhiz.insert(ack_bundle, '', my_sid.sid)
        pinfo('Ack is sent. Will execute procedure.')

        # After sending the ACK, start the execution.
        code, result = server_execute_procedure(procedure)

        # At this point the result handling starts.
        # Therefore, we make a bundle with common values and within the different cases,
        # and send the bundle and payload at the end.
        result_bundle_values = [
            ('name', potential_call.name),
            ('sender', potential_call.recipient),
            ('recipient', potential_call.sender),
            ('args', potential_call.args)
        ]
        payload = ''

        # If code is 1, an error occured.
        if code == 1:
            result_bundle_values = result_bundle_values + [('type', ERROR), ('result', result)]

        # If the return type is file, we have to open a file, assuming the result is a file path.
        elif procedure.return_type == 'file':
            result_bundle_values = result_bundle_values + [('type', RESULT), ('result', 'file')]
            payload = open(result.decode('utf-8'), 'rb')
            # This is the only case, where we have to remember the bundle id for cleanup later on.
            CLEANUP_BUNDLES[potential_call.id] = ack_bundle.id
            pinfo('Result was sent. Call successufull, waiting for next procedure.')

        # This is the most simple case. Just return the result.
        else:
            result_bundle_values = result_bundle_values + [('type', RESULT), ('result', result)]
            pinfo('Result was sent. Call successufull, waiting for next procedure.')

        # The final step. Compile and insert the result bundle.
        result_bundle = utilities.make_bundle(result_bundle_values)
        rhiz.insert(result_bundle, payload, my_sid.sid, ack_bundle.id)

def server_cleanup_store(bundle, sid, rhiz):
    '''Cleans up all bundles involved in a call
    Args:
        bundle (Bundle):    The bundle which triggers the cleanup
        sid (str):          Author SID for the bundle
        rhiz (Rhizome):     Rhizome connection
    '''
    # Try to lookup the BID for the Bundle to be cleaned,
    # make a new clear bundle based on the gathered BID and insert this bundle.
    # Finally, remove the id.
    # If it fails, just return.
    try:
        result_bundle_id = CLEANUP_BUNDLES[bundle.id]
        clear_bundle = utilities.make_bundle([('type', CLEANUP)], True)
        rhiz.insert(clear_bundle, '', sid, result_bundle_id)
        del CLEANUP_BUNDLES[bundle.id]
    except KeyError:
        return

def server_listen_dtn():
    '''Main listening function.
    '''
    global OFFERED_PROCEDURES, SERVER_MODE
    OFFERED_PROCEDURES = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    SERVER_MODE = RUNNING

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
        pfatal('The server does not have a SID. ' \
                'Create a SID with "servald keyring add" and restart Serval. Aborting.'
              )
        return

    pinfo('Server address: %s' % my_sid.sid)

    # See if there are files in the store.
    # If the store is empty, we will start listening on the empty store.
    # Otherwise, we will ignore files before token.
    token = None
    try:
        token = rhiz.get_bundlelist()[0].__dict__['.token']
    except IndexError:
        pwarn('Rhizome store is empty.')

    while SERVER_MODE:
        bundles = rhiz.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                # The first bundle is the most recent. Therefore, we have to save the new token.
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if bundle.service == 'RPC':
                    # At this point, we have an call and have to start handling it.
                    # Therefore, we download the manifest.
                    potential_call = rhiz.get_manifest(bundle.id)

                    # If the bundle is a call, we start a handler thread.
                    if potential_call.type == CALL:
                        start_new_thread(server_handle_call, (potential_call, rhiz, my_sid))
                    # If the bundle is a cleanup file, we start the cleanup routine.
                    elif potential_call.type == CLEANUP:
                        server_cleanup_store(potential_call, my_sid.sid, rhiz)

        time.sleep(1)
