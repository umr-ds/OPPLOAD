'''Main Server module.
'''
import time
import subprocess
import os
import threading
from _thread import start_new_thread
from pyserval.client import Client
import utilities
from utilities import pdebug, pinfo, pfatal, pwarn
from utilities import ACK, CALL, CLEANUP, ERROR, RESULT, CONFIGURATION
import client
import logging
from job import Status
import filter_servers
from job import ServerNotFoundError, MalformedJobfileError, ServerNotOfferingProcedure, ArgumentMissmatchError
import sys
import shutil
import math

# This is the global serval RESTful client object
SERVAL = None

# A threading lock for critical parts like updating offered procedures.
LOCK = threading.RLock()

# Status indicators for the server
RUNNING = True
STOPPED = False
SERVER_MODE = STOPPED

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

    def __str__(self):
        return '%s %s %s' % (self.return_type, self.name, ' '.join(self.args))

def server_publish_procedures():
    '''Publishes all offered procedures.
    '''

    pdebug("publishing...")

    offered_procedures = get_offered_procedures(utilities.CONFIGURATION['rpcs'])

    payload = ''
    # To not run this code multiple times at the same time, we use a simple LOCK.
    with LOCK:
        # First, see if we already publishing our procedures. If so, get the bundle_id
        offer_bundle_id = None
        bundles = SERVAL.rhizome.get_bundlelist()
        for bundle in bundles:
            if bundle.from_here == 1 and bundle.manifest.service == utilities.OFFER:
                offer_bundle_id = bundle.bundle_id
                break

        # Build the payload containing all offered procedures
        for procedure in offered_procedures:
            procedure_str = str(procedure)
            payload = payload + procedure_str

        # If we already publish procedures, just update.
        # Otherwise, insert a new bundle.
        if offer_bundle_id:
            procedures_bundle = SERVAL.rhizome.get_bundle(offer_bundle_id)
            procedures_bundle.update_payload(payload)
        else:
            procedures_bundle = SERVAL.rhizome.new_bundle(
                name=SERVAL.keyring.default_identity().sid,
                payload=payload,
                use_default_identity=True,
                service=utilities.OFFER
            )

def get_offered_procedures(rpc_defs):
    '''Parses the rpc definitions file and stores all of them in a list.
    Args:
        rpc_defs (str): The path to the definitions file

    Returns:
        list(Procedure): A list of parsed Procedures.
    '''

    offered_procedures = set()
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            procedure_definition_list = procedure_definition.split(' ')
            return_type = procedure_definition_list[0]
            name = procedure_definition_list[1]
            args = procedure_definition_list[2:]
            offered_procedures.add(Procedure(return_type=return_type, name=name, args=args))
    
    return offered_procedures

def server_offering_procedure(procedure):
    '''Checks, if the given procedure is offered by the server.
    Args:
        procedure (Procedure): The procedure to check.

    Returns:
        bool: True, if the procedure is offered, false otherwise.
    '''
    with LOCK:
        try:
            for offered_procedure in get_offered_procedures(utilities.CONFIGURATION['rpcs']):
                if offered_procedure.name == procedure.name:
                    if len(offered_procedure.args) != len(procedure.args):
                        raise ArgumentMissmatchError
                    procedure.return_type = offered_procedure.return_type
                    bin_path = '%s/%s' % (utilities.CONFIGURATION['bins'], procedure.name)

                    bin_path = '%s/%s' % (utilities.CONFIGURATION['bins'], procedure.name)

                    if not os.path.exists(bin_path) or not os.access(bin_path, os.X_OK):
                        pwarn('Server is offering procedure \'%s\', ' \
                                'but it seems the binary %s/%s is not present ' \
                                'or it is not executable. ' \
                                'Will not try to execute.' \
                                % (procedure.name, utilities.CONFIGURATION['bins'], procedure.name)
                            )
                        raise ServerNotOfferingProcedure
                    pinfo('Offering procedure \'%s\'.' % procedure.name)
                    return True
            raise ServerNotOfferingProcedure
        except ServerNotOfferingProcedure:
            pwarn('Not offering procedure \'%s\'. Waiting for next call.' % procedure.name)
            raise
        except ArgumentMissmatchError:
            pfatal('Procedure arguments dont match the demanded arguments! Too few or too many arguments.')
            raise

def getArgumentType(tprocedure):
    '''Returns the needed argument type(s) of a given procedure'''
    offered_procedure = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    if offered_procedure != None and len(offered_procedure) > 0:
        for procedure in offered_procedure:
            if procedure.name == tprocedure:
                return procedure.args
    return None


def getResultType(tprocedure):
    '''Returns the result type of a given procedure'''
    offered_procedure = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    if offered_procedure != None and len(offered_procedure) > 0:
        for procedure in offered_procedure:
            if procedure.name == tprocedure:
                return procedure.return_type
    return None

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
    bin_path = utilities.CONFIGURATION['bins'] + '/%s %s'
    procedure_process = subprocess.Popen(
        bin_path % (procedure.name, ' '.join(procedure.args)),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = procedure_process.communicate()

    if procedure_process.returncode != 0:
        pwarn('Execution of \'%s\' was not successfull. Will return error %s\n' \
            % (procedure.name, err))
        return (1, err.rstrip())
    else:
        pinfo('Execution of \'%s\' was successfull with result %s' % (procedure.name, out))
        return (0, out.rstrip())

def server_thread_handle_call(potential_call, delete):
    LOG_FILENAME = '/tmp/server_{}.log'.format(SERVAL.keyring.default_identity().sid)
    logging.basicConfig(filename=LOG_FILENAME, level=logging.ERROR)

    try:
        server_handle_call(potential_call, delete)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        pfatal('An error occured: {}'.format(str(sys.exc_info()[0])))
        # Error in handle thread -> send errorlog to client
        logging.exception('Got exception on thread handler')
        # check if serval is running
        if not utilities.serval_running():
            # start serval again
            pwarn('Serval crashed. Restarting it')
            os.system('start_serval')
        if 'global_client_sid' in globals():
            ziplist = []
            # check if error occurred during a cascading job
            if 'global_jobfile' in globals():
                # adding to the ziplist
                ziplist.append(global_jobfile)
            ziplist.append(LOG_FILENAME)
            # creating an error_zip
            ret_zip = utilities.make_zip(ziplist, name="error_"+SERVAL.keyring.default_identity().sid)
            procedure = server_parse_call(potential_call)
            error_bundle = utilities.make_bundle([
                ('name', procedure.name),
                ('sender', SERVAL.keyring.default_identity().sid),
                ('recipient', global_client_sid),
                ('args', procedure.args[-1]),
                ('type', ERROR),
                ('result', str(sys.exc_info()[0]))
            ])
            # sending files
            payload = open(ret_zip, 'rb')
            rhizome.insert(error_bundle, payload, SERVAL.keyring.default_identity().sid)
            if not delete:
                pdebug('Deleting the created error zip: ' + ret_zip)
                os.remove(ret_zip)

            if 'global_jobfile' in globals():
                del global_jobfile
            elif 'global_client_sid' in globals():
                del global_client_sid
        else:
            utilities.fatal('Client sid not found. Aborting.')
            raise

def server_handle_call(potential_call, delete):
    '''Main handler function for an incoming call.
    Args:
        potential_call (Bundle):    The potential call, which has to be handled.
        my_sid (ServalIdentity):    ServalIdentity of the server
    '''
    # TODO DEBUG
    distribution = 'E'
    pinfo('Received call. Will check if procedure is offered.')
    # First step, parse the potential call.
    procedure = server_parse_call(potential_call)
    client_sid = None

    if server_offering_procedure(procedure):
        client_sid = potential_call.sender
        # If the server offers the procedure,
        # we first have to download the file because it will be removed as soon we send the ack.
        # If in the next line might be obscolete, because only the text is sent to all servers not the file itself
        if procedure.args[0] == 'jobfile':
            path = '/tmp/%s_%s' % (procedure.name, potential_call.version)
            rhizome.get_decrypted_to_file(potential_call.id, path)
            # FIXME sometimes the file is not decrypted
            job = None
            # getting the zipfile
            if utilities.is_zipfile(path):
                pdebug('zipfile received.')
                file_list = utilities.extract_zip(path, potential_call.id, SERVAL.keyring.default_identity())
                # parse jobfile
                for _file in file_list:
                    if _file.endswith('.jb'):
                        pdebug('Parsing received jobfile')
                        job = utilities.parse_jobfile(_file)
                        global global_jobfile
                        global_jobfile = _file
            else:
                pdebug('Not a valid zipfile: ' + path)

            # find next job
            if job is None:
                # raise an error
                pdebug('Jobfile is malformed')
                raise MalformedJobfileError
            _file = global_jobfile
            # update the client_sid
            client_sid = job.client_sid
            pdebug('Received jobs:')
            for x in job.joblist:
                x.job_print()

            cwd  = os.getcwd()
            os.chdir('/tmp/' + potential_call.id)
            # iterate through jobs
            possible_job = None
            possible_next_job = None
            # get next task aswell
            for jb in range(len(job.joblist)):
                if job.joblist[jb].status == Status.OPEN and job.joblist[jb].server == SERVAL.keyring.default_identity().sid:
                    pdebug('job found: %s' % job.joblist[jb].procedure)
                    possible_job = job.joblist[jb]
                    if jb+1 < len(job.joblist):
                        possible_next_job = job.joblist[jb+1]
                    break

            # write result filename to file and write it back into the archive
            resulttype = getResultType(possible_job.procedure)
            n_procedure = Procedure(return_type=resulttype, name=possible_job.procedure, args=possible_job.arguments)
            # checks if procedure in jobfile is offered by this server
            if n_procedure is not None and server_offering_procedure(n_procedure):
                dt = str(math.floor(time.time()))
                ack_bundle = utilities.make_bundle([
                    ('type', ACK),
                    ('name', potential_call.name),
                    ('sender', SERVAL.keyring.default_identity().sid),
                    ('recipient', potential_call.sender),
                    ('args', potential_call.args),
                    ('time', dt)
                ])
                rhizome.insert(ack_bundle, '', SERVAL.keyring.default_identity().sid)
                pinfo('Ack is sent. Will execute procedure.')
                # After sending the ACK, start the execution.
                code, result = server_execute_procedure(n_procedure)
                # update jobfile
                    # TODO exit and send file back to the client
                if possible_next_job is not None:
                    # checks if the type of the result is a file
                    result_decoded = result.decode('utf-8')

                    # check if a server has to be found
                    f = open(global_jobfile, 'r+')
                    lines = f.readlines()

                    if code == 0:
                        # write DONE
                        lines[possible_job.line] =  utilities.split_join(lines[possible_job.line], 'DONE')
                    else:
                        # write ERROR and exit
                        lines[possible_job.line] =  utilities.split_join(lines[possible_job.line], 'ERROR')
                        # TODO raise an Error
                    if possible_next_job.server == 'any':
                        # fetch a server which offers the given procedure, and remove yourself from the set
                        pdebug('Finding a new server')
                        possible_server = filter_servers.client_find_server(possible_next_job.filter_dict, possible_next_job.procedure) if (bool(possible_next_job.filter_dict)) else client.client_find_server(possible_next_job.procedure, possible_next_job.arguments, True)
                        if len(possible_server) == 0:
                            pfatal('No server nearby who executes the following procedure: %s. Aborting' % possible_next_job.procedure)
                            raise ServerNotFoundError
                        if not bool(possible_next_job.filter_dict):
                            possible_server = client.choose_server(possible_server, distribution, job.joblist, SERVAL.keyring.default_identity().sid)
                        if bool(possible_next_job.filter_dict):
                            possible_server = list(filter_servers.parse_server_caps(possible_server, possible_next_job.filter_dict))
                        if len(possible_server) == 0:
                            pfatal('No server nearby. Aborting')
                            raise ServerNotFoundError
                        # FIXME leads to a crash if the procedure is not offered by the server who checks
                        # filter_servers returns a dict -> convert it to list
                            #raise ServerNotFoundError
                        if type(possible_server) is dict:
                            # creating a list of all servers
                            ret_list = []
                            for server in possible_server:
                                ret_list.append(server)
                            possible_server = ret_list

                        if possible_server != None  and len(possible_server) > 0:
                            if type(possible_server) is list:
                                possible_server = possible_server[0]
                            lines[possible_next_job.line] = lines[possible_next_job.line].replace('any', possible_server)
                            # Update jobfile
                            result_split_path = result_decoded.split('/')[-1]
                            lines[possible_next_job.line] =  utilities.split_join(lines[possible_next_job.line], result_split_path)
                            tsid = possible_server
                    else:
                        possible_server = possible_next_job.server
                        result_split_path = result_decoded.split('/')[-1]
                        lines[possible_next_job.line] =  utilities.split_join(lines[possible_next_job.line], result_split_path)
                    # update the jobfile
                    f.seek(0)
                    for line in lines:
                        f.write(line)
                    f.close()
                    if n_procedure.return_type == 'file':
                        # Send the resultfile to the next server, write the filehash to the jobfile and send it too
                        # make zip
                        payld = utilities.make_zip([result_decoded, _file], name=SERVAL.keyring.default_identity().sid+ '_' + str(math.floor(time.time())))
                    else:
                        payld = utilities.make_zip([_file], name=SERVAL.keyring.default_identity().sid + '_' + str(math.floor(time.time())))
                    if type(possible_server) is list and len(possible_server) > 0:
                        possible_server = possible_server[0]
                    pdebug('Sending new task to tsid: ' + possible_server)
                    # Send file to server if returntype was 'file'
                    #if n_procedure.return_type == 'file':
                        #client.client_call_cc_dtn([tsid, '*'], 'file', ['file', payld], payld)
                    # Send the jobfile to the next recipient
                    cleanup_bundle = client.client_call_cc_dtn([possible_server, '*'], 'file', ['file', payld], payld)
                    #CLEANUP_BUNDLES[potential_call.id] = ack_bundle.id
                    # FIXME test
                    cleanup_store(cleanup_bundle[0])

                    os.chdir(cwd)
                    # cleanup of the created directory in /tmp/
                    if not delete:
                        shutil.rmtree('/tmp/' + potential_call.id)
                        pdebug('Deleting directory /tmp/' + potential_call.id)
                        os.remove(path)
                        pdebug('Deleting ' + path)
                else:
                    # Send the result directly to the initial client
                    tsid = job.client_sid
                    pdebug("Return result back to the client")
                    # name and arguments have to be the orignial one
                    lt = str(time.time())

                    ret = utilities.make_zip([result.decode('utf-8')], name=SERVAL.keyring.default_identity().sid + '_' + str(math.floor(time.time())))

                    result_bundle_values = [
                        ('name', procedure.name),
                        ('sender', SERVAL.keyring.default_identity().sid),
                        ('recipient', tsid),
                        ('args', procedure.args[-1])
                        ]

                    payload = ''
                    # If code is 1, an error occured.
                    if code == 1:
                        pdebug('Error')
                        result_bundle_values = result_bundle_values + [('type', ERROR), ('result', result)]

                    # If the return type is file, we have to open a file, assuming the result is a file path.
                    elif n_procedure.return_type == 'file':
                        result_decoded = result.decode('utf-8')
                        result_bundle_values = result_bundle_values + [('type', RESULT), ('result', 'file')]
                        payload = open(ret, 'rb')
                        # This is the only case, where we have to remember the bundle id for cleanup later on.
                        #CLEANUP_BUNDLES[potential_call.id] = ack_bundle.id
                        pinfo('Result was sent. Call successfull, waiting for next procedure.\n')

                    # This is the most simple case. Just return the result.
                    else:
                        result_decoded = result.decode('utf-8')
                        result_bundle_values = result_bundle_values + [('type', RESULT), ('result', result)]
                        pinfo('Result was sent. Call successfull, waiting for next procedure.\n')

                    # The final step. Compile and insert the result bundle.
                    result_bundle = utilities.make_bundle(result_bundle_values)
                    #rhiz.insert(result_bundle, payload, my_sid.sid, ack_bundle.id)
                    rhizome.insert(result_bundle, payload, SERVAL.keyring.default_identity().sid)
                    os.chdir(cwd)
                    # deleting files
                    if not delete:
                        shutil.rmtree('/tmp/' + potential_call.id)
                        pdebug('Deleting directory /tmp/' + potential_call.id)
                        os.remove(path)
                        pdebug('Deleting ' + path)

            else:
                pdebug('An error occured and no procedure was found in the jobfile or the job is done already.')
                return
        else:
            # setting client_sid as a global variable
            global_client_sid = potential_call.sender

            # if the incoming call was a file transfer
            if procedure.args[0] == 'file':
                path = '/tmp/%s_%s' % (procedure.name, potential_call.version)
                rhizome.get_decrypted_to_file(potential_call.id, path)

            # Compile and insert the ACK bundle.
            # FIXME insert time variable
            dt = str(math.floor(time.time()))
            ack_bundle = utilities.make_bundle([
                ('type', ACK),
                ('name', potential_call.name),
                ('sender', SERVAL.keyring.default_identity().sid),
                ('recipient', potential_call.sender),
                ('args', potential_call.args),
                ('time', dt)
            ])
            rhizome.insert(ack_bundle, '', SERVAL.keyring.default_identity().sid)
            pinfo('Ack is sent. Will execute procedure.')

            # After sending the ACK, start the execution.
            # TODO update procedure args if a file was needed as input
            code, result = server_execute_procedure(procedure)

            # At this point the result handling starts.
            # Therefore, we make a bundle with common values and within the different cases,
            # and send the bundle and payload at the end.
            result_bundle_values = [
                ('name', potential_call.name),
                ('sender', SERVAL.keyring.default_identity().sid),
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
                pinfo('Result was sent. Call successfull, waiting for next procedure.\n')
	        # This is the most simple case. Just return the result.
            else:
                result_bundle_values = result_bundle_values + [('type', RESULT), ('result', result)]
                pinfo('Result was sent. Call successfull, waiting for next procedure.\n')

            # The final step. Compile and insert the result bundle.
            result_bundle = utilities.make_bundle(result_bundle_values)
            rhizome.insert(result_bundle, payload, SERVAL.keyring.default_identity().sid, ack_bundle.id)

    else:
        # In this case, the server does not offer the procedure.
        # Therefore, the client will be informed with an error.
        result_bundle_values = [
            ('name', potential_call.name),
            ('sender', SERVAL.keyring.default_identity().sid),
            ('recipient', potential_call.sender),
            ('args', potential_call.args),
            ('type', ERROR),
            ('result', 'Server does not offer procedure.')
        ]
        result_bundle = utilities.make_bundle(result_bundle_values)
        rhizome.insert(result_bundle, '', SERVAL.keyring.default_identity().sid)

def server_cleanup_store(bundle, sid):
    '''Cleans up all bundles involved in a call
    Args:
        bundle (Bundle):    The bundle which triggers the cleanup
        sid (str):          Author SID for the bundle
    '''
    # Try to lookup the BID for the Bundle to be cleaned,
    # make a new clear bundle based on the gathered BID and insert this bundle.
    # Finally, remove the id.
    # If it fails, just return.
    try:
        result_bundle_id = CLEANUP_BUNDLES[bundle.id]
        clear_bundle = utilities.make_bundle([('type', CLEANUP)], True)
        rhizome.insert(clear_bundle, '', sid, result_bundle_id)
        del CLEANUP_BUNDLES[bundle.id]
    except KeyError:
        return

def cleanup_store(bundle, sid):
    '''Cleans up all bundles involved in a call
    Args:
        bundle (Bundle):    The bundle which triggers the cleanup
        sid (str):          Author SID for the bundle
    '''
    # Try to lookup the BID for the Bundle to be cleaned,
    # make a new clear bundle based on the gathered BID and insert this bundle.
    # Finally, remove the id.
    # If it fails, just return.
    try:
        if bundle.id:
            pdebug('Clearing bundle: ' + str(bundle.id))
            clear_bundle = utilities.make_bundle([('type', CLEANUP)], True)
            rhizome.insert(clear_bundle, '', sid, bundle.id)
    except KeyError:
        return

def server_listen(delete=False):
    '''Main listening function.
    '''
    global SERVER_MODE
    global SERVAL
    SERVER_MODE = RUNNING

    # Create a RESTful serval_client to Serval with the parameters from the config file
    # and get the Rhizome serval_client.
    SERVAL = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = SERVAL.rhizome

    # At this point we can publish all offered procedures.
    # This procedure is executed every 30 seconds.
    update_published_thread = threading.Timer(10, server_publish_procedures)
    update_published_thread.daemon = True
    update_published_thread.start()

    time.sleep(60)

    # Immediatelly after publishing the offered procedures,
    # get the token from the store, to not parse the entire bundlelist.
    token = rhizome.get_bundlelist()[0].bundle_id
    while SERVER_MODE:
        bundles = rhizome.get_bundlelist()
        if not bundles:
            continue
        for bundle in bundles:
            if not bundle.manifest.service == 'RPC':
                continue

            # At this point, we have an call and have to start handling it.
            # Therefore, we download the manifest.
            potential_call = bundle.manifst
            if not(potential_call.recipient == 'any' \
                or potential_call.recipient == SERVAL.keyring.default_identity().sid):
                continue

            # If the bundle is a call, we start a handler thread.
            if potential_call.type == CALL:
                start_new_thread(server_thread_handle_call, (potential_call, delete))

            # If the bundle is a cleanup file, we start the cleanup routine.
            # TODO Cleanup doesnt work with cc
            elif potential_call.type == CLEANUP:
                pdebug("CLEANUP")
                #server_cleanup_store(potential_call, SERVAL.keyring.default_identity().sid)

        time.sleep(1)
