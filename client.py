import time

import restful
import rhizome
import utilities
from utilities import pdebug, pfatal, pinfo, pwarn, CALL, ACK, RESULT

def client_call_dtn(server, name, params):
    connection = restful.RestfulConnection(host=utilities.CONFIGURATION['host'], port=int(utilities.CONFIGURATION['port']), user=utilities.CONFIGURATION['user'], passwd=utilities.CONFIGURATION['passwd'])
    rhiz = rhizome.Rhizome(connection)

    my_sid = connection.first_identity
    if not my_sid:
        pfatal('The server does not have a SID. Create a SID with "servald keyring add" and restart Serval. Aborting.')
        return

    pinfo('Calling procedure \'%s\'.' % name)
    call_bundle = utilities.make_bundle([('service', 'RPC'), ('name', name), ('sender', my_sid.sid), ('recipient', server)])
    call_payload = CALL + name + '|' + '|'.join(params)
    rhiz.insert(call_bundle, call_payload, my_sid.sid)
    pinfo('Waiting for result.')

    token = rhiz.get_bundlelist()[0].__dict__['.token']

    result_received = False

    while not result_received:
        bundles = rhiz.get_bundlelist(token=token)
        if bundles:
            for bundle in bundles:
                token = bundle.__dict__['.token'] if bundle.__dict__['.token'] else token

                def service_is_rpc(srvc): return srvc == 'RPC'
                #def not_my_file(sid): return sid and sid != my_sid

                if service_is_rpc(bundle.service):# and not_my_file(sender):
                    potential_result = rhiz.get_decrypted(bundle.id)

                    if potential_result[0] == ACK and bundle.name == name:
                        pinfo('Received ACK. Will wait for result.')
                    if potential_result[0] == RESULT and bundle.name == name:
                        pinfo('Received result: %s' % potential_result[1:])
                        result_received = True

        time.sleep(1)
