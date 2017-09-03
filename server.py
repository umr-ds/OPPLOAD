import base64, json
import time
import struct
import subprocess
import os

import utilities
import client
import rhizome
from utilities import pdebug, pfatal, pinfo, pwarn

RUNNING = True
STOPPED = False
server_mode = STOPPED
OFFERED_PROCEDURES = None

def get_offered_procedures(rpc_defs):
    offered_procedures = []
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            procedure_definition_list = procedure_definition.split(' ')
            name = procedure_definition_list[0]
            params = tuple(procedure_definition_list[1:])
            offered_procedures.append([name, params])
    
    return offered_procedures

def server_offering_procedure(procedure):
    global OFFERED_PROCEDURES
    for proc in OFFERED_PROCEDURES:
        if proc[0] == procedure[0] and len(proc[1]) == len(procedure[1]):
            return True
    return False

def server_parse_call(call):
    call_list = call[6:].split('|')
    procedure_name = call_list[0]
    parameters = tuple(param.rstrip('\x00') for param in call_list[1:])
    return (procedure_name, parameters)

def server_execute_procedure(name, params):
    p = subprocess.Popen(os.getcwd() + '/' + utilities.CONFIGURATION['bins'] + '/' + name + ' ' + ' '.join(params), shell=True, stdout=subprocess.PIPE)
    out, err = p.communicate()
    return out

def server_listen_rhizome():
    global OFFERED_PROCEDURES

    OFFERED_PROCEDURES = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    server_mode = RUNNING

    connection = client.RestfulConnection(host=utilities.CONFIGURATION['host'], port=int(utilities.CONFIGURATION['port']), user=utilities.CONFIGURATION['user'], passwd=utilities.CONFIGURATION['passwd'])
    rhiz = rhizome.Rhizome(connection)

    my_sid = connection.first_identity
    if not my_sid:
        pfatal('The server does not have a SID. Create a SID with "servald keyring add" and restart Serval. Aborting.')
        return
    
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

                def service_is_rpc(srvc): return srvc == 'RPC'
                #def not_my_file(sid): return sid and sid != my_sid

                if service_is_rpc(bundle.service):# and not_my_file(sender):
                    bid = bundle.id
                    potential_call = rhiz.get_decrypted(bid)

                    if struct.unpack('B', potential_call[0].encode())[0] == 0:
                        procedure = server_parse_call(potential_call)
                        
                        if server_offering_procedure(procedure):
                            ack_bundle = utilities.make_bundle([('service', 'RPC'), ('name', procedure[0]), ('sender', bundle.recipient), ('recipient', bundle.sender)])
                            rhiz.insert(ack_bundle, struct.pack('B', 1), my_sid.sid)

                            result_bundle = utilities.make_bundle([('service', 'RPC'), ('name', procedure[0]), ('sender', bundle.recipient), ('recipient', bundle.sender)])
                            result = struct.pack('B', 2) + server_execute_procedure(procedure[0], procedure[1]).rstrip()
                            rhiz.insert(result_bundle, result, my_sid.sid)
                        
        time.sleep(1)