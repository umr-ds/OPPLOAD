#!/usr/bin/env python3

# -*- coding: utf-8 -*-

'''Main Server module.
'''
import time
import subprocess
import os
import threading
import zipfile
from _thread import start_new_thread
from pyserval.client import Client
from pyserval.exceptions import DuplicateBundleException
import utilities
from utilities import pdebug, pinfo, pfatal, pwarn
from utilities import ACK, CALL, CLEANUP, ERROR, RESULT, CONFIGURATION
import client
import logging
from job import Status
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

        # We need to remove the \n at the last argument.
        self.args[-1] = args[-1].rstrip()

    def __str__(self):
        return '%s %s %s' % (self.return_type, self.name, ' '.join(self.args))

def server_publish_procedures():
    '''Publishes all offered procedures.
    '''
    update_published_thread = threading.Timer(30, server_publish_procedures)
    update_published_thread.daemon = True
    update_published_thread.start()

    opc, offered_procedures = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    cc, capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'])

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
        payload = 'procedures: {}\n'.format(opc)
        for procedure in offered_procedures:
            procedure_str = str(procedure) + '\n'
            payload = payload + procedure_str

        payload = payload + 'capabilities: {}\n'.format(cc)
        for capability in capabilities:
            payload = payload + capability

        # If we already publish procedures, just update.
        # Otherwise, insert a new bundle.
        if offer_bundle_id:
            procedures_bundle = SERVAL.rhizome.get_bundle(offer_bundle_id)
            procedures_bundle.update_payload(payload)
        else:
            procedures_bundle = SERVAL.rhizome.new_bundle(
                name=SERVAL.keyring.default_identity().sid,
                payload=payload,
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
    count = 0
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            procedure_definition_list = procedure_definition.split(' ')
            return_type = procedure_definition_list[0]
            name = procedure_definition_list[1]
            args = procedure_definition_list[2:]
            offered_procedures.add(Procedure(return_type=return_type, name=name, args=args))
            count = count + 1

    return (count, offered_procedures)

def get_capabilities(rpc_caps):
    '''Parses the rpc definitions file and stores all of them in a list.
    Args:
        rpc_defs (str): The path to the definitions file

    Returns:
        list(Procedure): A list of parsed Procedures.
    '''

    capabilities = set()
    count = 0
    with open(rpc_caps, 'r') as conf_file:
        for capability in conf_file:
            capabilities.add(capability)
            count = count + 1

    return (count, capabilities)

def server_offering_procedure(procedure):
    '''Checks, if the given procedure is offered by the server.
    Args:
        procedure (Procedure): The procedure to check.

    Returns:
        bool: True, if the procedure is offered, false otherwise.
    '''
    with LOCK:
        opc, offered_procedures = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
        pdebug("Will check for procedures.")
        for offered_procedure in offered_procedures:
            pdebug("Offered Proc: {}".format(offered_procedure))
            if offered_procedure.name != procedure.name:
                continue

            if len(offered_procedure.args) != len(procedure.args):
                continue

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
                continue
            pinfo('Offering procedure \'%s\'.' % procedure.name)
            return True

        return False

def server_parse_call(call):
    '''Parse the incomming call.
    Args:
        call (Bundle): The call to be parsed.

    Returns:
        Procedure: The parsed procedure.
    '''
    return Procedure(name=call.manifest.name, args=call.manifest.args.split('|'))

def server_execute_procedure(procedure, env_path):
    '''Main execution function.
    Args:
        procedure (Procedure): The procedure to be executed.

    Returns:
        (int, str): Returns a tuple containing (return code, stdout)
    '''
    pinfo('Starting execution of \'%s\'.' % procedure.name)
    bin_path = utilities.CONFIGURATION['bins'] + '/%s %s'

    _, offered_procedures = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    offered_procedure = [_procedure for _procedure in offered_procedures if _procedure.name == procedure.name and len(_procedure.args) == len(procedure.args)]

    error = None
    if len(offered_procedure) > 1:
        error = 'There is more than one matching procedure! Can not execute.'
    elif len(offered_procedure) < 1:
        error = 'There is no such procedure. Can not execute'

    if error:
        pwarn(error)
        return (1, error)

    offered_procedure = offered_procedure[0]

    for i in range(len(procedure.args)):
        if offered_procedure.args[i] == 'file':
            procedure.args[i] = env_path + procedure.args[i]

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

def server_thread_handle_call(potential_call):
    '''Main handler function for an incoming call.
    Args:
        potential_call (Bundle):    The potential call, which has to be handled.
        my_sid (ServalIdentity):    ServalIdentity of the server
    '''
    pinfo('Received call. Will check if procedure is offered.')

    # First step, parse the potential call.
    procedure = server_parse_call(potential_call)

    if server_offering_procedure(procedure):
        pdebug("Offering procedure.")
        client_sid = None

        # If the server offers the procedure,
        # we first have to download the file because it will be removed as soon we send the ack.
        # If in the next line might be obscolete, because only the text is sent to all servers not the file itself
        path = '/tmp/%s_%s' % (procedure.name, potential_call.manifest.version)
        with open(path, 'wb') as zip_file:
            pdebug("Downloading the ZIP from Rhizome store.")
            zip_payload = SERVAL.rhizome.get_payload(potential_call)
            zip_file.write(zip_payload)

        jobs = None
        job_file_path = None

        extract_path = '/tmp/{}_{}/'.format(potential_call.bundle_id, SERVAL.keyring.default_identity().sid)

        # If we have a valid ZIP file, we extract it and parse the job file.
        if zipfile.is_zipfile(path):
            pdebug("Extracting ZIP.")
            file_list = utilities.extract_zip(path, extract_path)

            # Find the job file
            for _file in file_list:
                if _file.endswith('.jb'):
                    jobs = utilities.parse_jobfile(_file)
                    job_file_path = _file
        else:
            pfatal('{} is not a valid ZIP file.' + path)
            return

        if jobs is None:
            raise MalformedJobfileError

        # Now we have all parts from the jobfile. Let's remember the real client_sid.
        client_sid = jobs.client_sid

        # Further execution will happen on /tmp, so we remember the CWD for later.
        cwd  = os.getcwd()
        os.chdir(extract_path)

        possible_job = None
        possible_next_job = None

        # Now, we iterate through all jobs and try to find a job for us.
        # We remember our job and the job for the next hop, if possible.
        pdebug("Iterating through all jobs.")
        for job in jobs.joblist:
            if job.status == Status.OPEN and job.server == SERVAL.keyring.default_identity().sid:
                possible_job = job
                try:
                    current_position = jobs.joblist.index(possible_job)
                    possible_next_job = jobs.joblist[current_position + 1]
                except IndexError:
                    pass
                break

        # Since we have our procedure, we create an object of it.
        procedure_to_execute = Procedure(name=possible_job.procedure, args=possible_job.arguments)

        # Let's do a final check, if the procedure is offered.
        if procedure_to_execute is None or not server_offering_procedure(procedure_to_execute):
            pfatal("Server is not offering this procedure.")
            return

        # Since we are not confident about the job, we sent an ACK and start processing.
        try:
            SERVAL.rhizome.new_bundle(
                name=potential_call.manifest.name,
                payload="",
                service="RPC",
                recipient=client_sid,
                custom_manifest={"type": ACK, 'args': 'jobfile'}
            )
        except DuplicateBundleException as e:
            pass

        pinfo('Ack is sent. Will execute procedure.')

        # After sending the ACK, start the execution.
        code, result = server_execute_procedure(procedure_to_execute, extract_path)
        result_decoded = result.decode('utf-8')

        # Here we need to prepare the job for the next hop.
        if possible_next_job is not None:
            # After executing the job, we have to update the job_file.
            # Therefore, we read it.
            job_file = open(job_file_path, 'r+')
            lines = job_file.readlines()

            if code == 0:
                # The execution was successful, so we append DONE to the corresponding line.
                lines[possible_job.line] =  utilities.insert_to_line(lines[possible_job.line], 'DONE')
            else:
                # Something went wrong, we append ERROR to the line.
                lines[possible_job.line] =  utilities.insert_to_line(lines[possible_job.line], 'ERROR')

            # So, the file has been updated, so we can write the content and close it.
            job_file.seek(0)
            for line in lines:
                job_file.write(line)
            job_file.close()

            # If the next server is again any, we search a new one for the next job
            if possible_next_job.server == 'any':
                servers = utilities.find_available_servers(SERVAL.rhizome, possible_next_job)

                if not servers:
                    # TODO: Here we have to have some error handling!
                    pfatal("Could not find any suitable servers. Aborting.")
                    return

                possible_next_job.server = servers[0]
                utilities.replace_any_to_sid(job_file_path, possible_next_job.line, possible_next_job.server)

            # Done. Now we only have to make the payload...
            zip_name = SERVAL.keyring.default_identity().sid+ '_' + str(math.floor(time.time()))
            payload_path = utilities.make_zip([result_decoded, job_file_path], name=zip_name, subpath_to_remove=extract_path)

            payload = open(payload_path, 'rb')

            # ... and send the bundle.
            new_bundle = SERVAL.rhizome.new_bundle(
                name=possible_next_job.procedure,
                payload=payload.read(),
                service="RPC",
                recipient=possible_next_job.server,
                custom_manifest={"type": CALL, 'args': 'jobfile'}
            )

        else:
            zip_name = SERVAL.keyring.default_identity().sid + '_' + str(math.floor(time.time()))
            payload_path = utilities.make_zip([result_decoded, job_file_path], name=zip_name, subpath_to_remove=extract_path)

            payload = open(payload_path, 'rb')

            custom_manifest={'recipient': jobs.client_sid, 'sender': SERVAL.keyring.default_identity().sid, 'type': None}

            # If code is 1, an error occured.
            if code == 1:
                custom_manifest['type'] = ERROR
            else:
                custom_manifest['type'] = RESULT


            new_bundle = SERVAL.rhizome.new_bundle(
                name=procedure_to_execute.name,
                payload=payload.read(),
                service="RPC",
                custom_manifest=custom_manifest
            )

        payload.close()
        os.chdir(cwd)

    else:
        # In this case, the server does not offer the procedure.
        # Therefore, the client will be informed with an error.
        new_bundle = SERVAL.rhizome.new_bundle(
            name=potential_call.manifest.name,
            payload="",
            service="RPC",
            recipient=potential_call.sender,
            custom_manifest={"type": ERROR, 'args': potential_call.manifest.args, 'result': 'Server does not offer procedure.'}
        )

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
    server_default_sid = SERVAL.keyring.default_identity().sid

    pdebug('Everything cleaned up.')

    # At this point we can publish all offered procedures.
    # The publish function is executed once at startup and then every 30 seconds.
    server_publish_procedures()

    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].bundle_id

    # This is the main server loop.
    pdebug("Entering the loop...")
    while SERVER_MODE:
        bundles = rhizome.get_bundlelist()
        for bundle in bundles:
            if bundle.bundle_id == token:
                break

            if not bundle.manifest.service == 'RPC':
                continue

            # At this point, we have an call and have to start handling it.
            # Therefore, we download the manifest.
            potential_call = rhizome.get_bundle(bundle.bundle_id)
            if not potential_call.manifest.recipient == server_default_sid:
                continue

            # If the bundle is a call, we start a handler thread.
            if potential_call.manifest.type == CALL:
                pdebug("Found call. Starting thread.")
                start_new_thread(server_thread_handle_call, (potential_call, ))

            # If the bundle is a cleanup file, we start the cleanup routine.
            # TODO Cleanup doesnt work with cc
            elif potential_call.manifest.type == CLEANUP:
                pdebug("CLEANUP")
                #server_cleanup_store(potential_call, server_default_sid)

        token = bundles[0].bundle_id

        time.sleep(1)