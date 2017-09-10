import base64, json
import time
import subprocess
import os

import utilities
import restful
import rhizome
from utilities import pdebug, pfatal, pinfo, pwarn, CALL, ACK, RESULT

RUNNING = True
STOPPED = False
server_mode = STOPPED
OFFERED_PROCEDURES = None

class Procedure():
    def __init__(self, return_type=None, name=None, args=None):
        self.return_type = return_type
        self.name = name
        self.args = args

def get_offered_procedures(rpc_defs):
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
    global OFFERED_PROCEDURES
    for offered_procedure in OFFERED_PROCEDURES:
        if offered_procedure.name == procedure.name and len(offered_procedure.args) == len(procedure.args):
            procedure.return_type = offered_procedure.return_type
            pinfo('Offering procedure \'%s\'.' % procedure.name)
            return True
    pwarn('Not offering procedure \'%s\'. Waiting for next call.' % procedure.name)
    return False

def server_parse_call(call):
    return Procedure(name=call.name, args=call.args.split('|'))

def server_execute_procedure(procedure):
    pinfo('Starting execution of \'%s\'.' % procedure.name)
    bin_path = os.getcwd() + '/' + utilities.CONFIGURATION['bins'] + '/%s %s'
    procedure_process = subprocess.Popen(bin_path % (procedure.name, ' '.join(procedure.args)), shell=True, stdout=subprocess.PIPE)
    out, err = procedure_process.communicate()
    pinfo('Execution of \'%s\' was successfull with result %s' % (procedure.name, out))
    return out

def server_listen_dtn():
    global OFFERED_PROCEDURES

    OFFERED_PROCEDURES = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    server_mode = RUNNING

    connection = restful.RestfulConnection(host=utilities.CONFIGURATION['host'], port=int(utilities.CONFIGURATION['port']), user=utilities.CONFIGURATION['user'], passwd=utilities.CONFIGURATION['passwd'])
    rhiz = connection.rhizome

    my_sid = connection.first_identity
    if not my_sid:
        pfatal('The server does not have a SID. Create a SID with "servald keyring add" and restart Serval. Aborting.')
        return

    pinfo('Server address: %s' % my_sid.sid)

    token = None
    try:
        token = rhiz.get_bundlelist()[0].__dict__['.token']
    except IndexError:
        pwarn('Rhizome store is empty.')

    while server_mode:
        bundles = rhiz.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if bundle.service == 'RPC':
                    potential_call = rhiz.get_manifest(bundle.id)

                    if potential_call.type == CALL:
                        pinfo('Received call. Will check if procedure is offered.')
                        procedure = server_parse_call(potential_call)

                        if server_offering_procedure(procedure):
                            # We first have to download the file because it will be removed as soon we send the ack.
                            if procedure.args[0] == 'file':
                                path = '/tmp/%s_%s' % (procedure.name, potential_call.version)
                                rhiz.get_decrypted_to_file(potential_call.id, path)
                                procedure.args[1] = path
                            
                            ack_bundle = utilities.make_bundle([('service', 'RPC'), ('type', ACK), ('name', potential_call.name), ('sender', potential_call.recipient), ('recipient', potential_call.sender)])
                            rhiz.insert(ack_bundle, '', my_sid.sid, potential_call.id)
                            pinfo('Ack is sent. Will execute procedure.')

                            result = server_execute_procedure(procedure).rstrip()

                            if procedure.return_type == 'file':
                                result_bundle = utilities.make_bundle([('service', 'RPC'), ('type', RESULT), ('result', 'file'), ('name', potential_call.name), ('sender', potential_call.recipient), ('recipient', potential_call.sender)])
                                rhiz.insert(result_bundle, open(result.decode('utf-8'), 'rb'), my_sid.sid, potential_call.id)
                                pinfo('Result was sent. Call successufull, waiting for next procedure.')
                            else:
                                result_bundle = utilities.make_bundle([('service', 'RPC'), ('type', RESULT), ('result', result), ('name', potential_call.name), ('sender', potential_call.recipient), ('recipient', potential_call.sender)])
                                rhiz.insert(result_bundle, '', my_sid.sid, potential_call.id)
                                pinfo('Result was sent. Call successufull, waiting for next procedure.')

        time.sleep(1)
