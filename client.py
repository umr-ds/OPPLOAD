import time

import restful
import rhizome
import utilities
from utilities import pdebug, pfatal, pinfo, pwarn, CALL, ACK, RESULT, ERROR, CLEANUP

def rpc_for_me(potential_result, name, args, sid):
    return potential_result.name == name \
        and potential_result.args == args \
        and potential_result.recipient == sid 

def client_call_dtn(server, name, params):
    connection = restful.RestfulConnection(host=utilities.CONFIGURATION['host'], port=int(utilities.CONFIGURATION['port']), user=utilities.CONFIGURATION['user'], passwd=utilities.CONFIGURATION['passwd'])
    rhiz = connection.rhizome

    my_sid = connection.first_identity
    if not my_sid:
        pfatal('The server does not have a SID. Create a SID with "servald keyring add" and restart Serval. Aborting.')
        return

    pinfo('Calling procedure \'%s\'.' % name)
    args = '|'.join(params)
    call_bundle = utilities.make_bundle([('type', CALL), ('name', name), ('args', args), ('sender', my_sid.sid), ('recipient', server)], True)
    payload = ''
    if len(params) == 2 and params[0] == 'file':
        payload = open(params[1], 'rb')
    rhiz.insert(call_bundle, payload, my_sid.sid)
    pinfo('Waiting for result.')

    token = rhiz.get_bundlelist()[0].__dict__['.token']

    result_received = False

    while not result_received:
        bundles = rhiz.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                if bundle.service == 'RPC':
                    potential_result = rhiz.get_manifest(bundle.id)

                    if rpc_for_me(potential_result, name, args, my_sid.sid):

                        if potential_result.type == ACK:
                            pinfo('Received ACK. Will wait for result.')
                        if potential_result.type == RESULT:
                            result_str = ''
                            if potential_result.result == 'file':
                                path = '/tmp/%s_%s' % (name, potential_result.version)
                                rhiz.get_decrypted_to_file(potential_result.id, path)
                                result_str = path
                            else:
                                result_str = potential_result.result

                            clear_bundle = utilities.make_bundle([('type', CLEANUP), ('name', name), ('args', args), ('sender', my_sid.sid), ('recipient', server)], True)
                            rhiz.insert(clear_bundle, '', my_sid.sid, call_bundle.id)
                            pinfo('Received result: %s' % result_str)
                            result_received = True

                        if potential_result.type == ERROR:
                            pfatal('Received error response with the following message: %s' % potential_result.result)
                            result_received = True

        time.sleep(1)
#