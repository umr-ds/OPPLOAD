#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''The main server module, contains everything needed to receive and
handle procedures
'''

import time
import subprocess
import os
import threading
import zipfile
from _thread import start_new_thread
import client
import math

from pyserval.client import Client
from pyserval.exceptions import DuplicateBundleException, DecryptionError

import utilities
from utilities import pdebug, pinfo, pfatal, pwarn
from utilities import ACK, CALL, CLEANUP, ERROR, RESULT, CONFIGURATION
from utilities import RPC, OFFER
from job import Status, Job

# This is the global serval RESTful client object
SERVAL = None

# A threading lock for critical parts like updating offered procedures.
LOCK = threading.RLock()

# A dict where all bundles are stored to be cleaned up after execution.
CLEANUP_BUNDLES = {}

# The server's default SID to be used.
SERVER_DEFAULT_SID = None


def server_publish_procedures():
    '''This function publishes offered procedures and capabilities
    periodically
    '''

    global SERVAL
    global LOCK
    global SERVER_DEFAULT_SID

    # Start a thread containing this function and execute it every
    # periodically in background.
    update_published_thread = threading.Timer(30, server_publish_procedures)
    update_published_thread.daemon = True
    update_published_thread.start()

    # The the offered procedures and capabilities for publishing.
    opc, offered_procedures = get_offered_procedures(
        utilities.CONFIGURATION['rpcs'])
    cc, capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'],
                                        utilities.CONFIGURATION['location'])

    payload = ''
    # To not run this code multiple times at the same time, we use a LOCK.
    with LOCK:
        # First, see if we already publishing our procedures.
        # If so, get the bundle_id
        offer_bundle_id = None
        bundles = SERVAL.rhizome.get_bundlelist()
        for bundle in bundles:
            if bundle.from_here == 1 and bundle.manifest.service == OFFER:
                offer_bundle_id = bundle.bundle_id
                break

        # Put all offered procedures to the payload
        payload = 'procedures: {}\n'.format(opc)
        for procedure in offered_procedures:
            procedure_str = str(procedure) + '\n'
            payload = payload + procedure_str

        # Put all capabilities to the payload
        payload = payload + 'capabilities: {}\n'.format(cc)
        for capability in capabilities:
            payload = payload + capability

        # If we already publish procedures, just update.
        # Otherwise, insert a new bundle.
        if offer_bundle_id:
            procedures_bundle = SERVAL.rhizome.get_bundle(offer_bundle_id)
            procedures_bundle.update_payload(payload)
        else:
            SERVAL.rhizome.new_bundle(
                name=SERVER_DEFAULT_SID,
                payload=payload,
                service=OFFER)


def get_offered_procedures(rpc_defs):
    '''Get all offered procedures from the procedures file specified
    in the config file

    Arguments:
        rpc_defs -- The path to the rpc.defs file

    Returns:
        Number of offered procedures and offered procedures
    '''

    offered_procedures = set()
    count = 0
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            # RPC defs are stored as '<NAME> <ARG1> ...'
            procedure_definition_list = procedure_definition.split(' ')
            name = procedure_definition_list[0]
            args = procedure_definition_list[1:]
            offered_procedures.add(Job(procedure=name, arguments=args))
            count = count + 1

    return (count, offered_procedures)


def get_capabilities(rpc_caps, location):
    '''Get all capabilities from the capabilities and location file specified
    in the config file

    Arguments:
        rpc_caps -- The path to rpc.caps file
        location -- The path to the location file

    Returns:
        Number of capabilities and capabilities
    '''

    capabilities = set()
    count = 0
    with open(rpc_caps, 'r') as conf_file:
        for capability in conf_file:
            capabilities.add(capability)
            count = count + 1

    with open(location, 'r') as location_file:
        capabilities.add(location_file.readline())
        count = count + 1

    return (count, capabilities)


def server_offering_procedure(job):
    '''Function for checking if the server is offering the procedure.

    Arguments:
        job -- The job to be checked

    Returns:
        True, if the procedure is offered, False otherwise
    '''

    global LOCK

    pinfo('Checking if offering {}'.format(job.procedure))

    with LOCK:
        # The the offered procedures
        _, offered_jobs = get_offered_procedures(
            utilities.CONFIGURATION['rpcs'])

        for offered_job in offered_jobs:
            if offered_job.procedure != job.procedure:
                # The name does not match, so skip.
                continue

            if len(offered_job.arguments) != len(job.arguments):
                # The number of arguments to not match, so skip.
                continue

            # It is not enough to check if we theoretically offer the
            # procedure but also if the executable is available.
            bin_path = '%s/%s' % (utilities.CONFIGURATION['bins'],
                                  job.procedure)

            if not os.path.exists(bin_path) or not os.access(
                    bin_path, os.X_OK):
                # The executable does not exist or is not executable.
                continue
            return True

        return False


def server_execute_procedure(job, env_path):
    '''The main execution function, which executes the called procedure

    Arguments:
        job -- The job to be executed
        env_path -- The temporary path where the procedure will be executed

    Returns:
        The return code of the procedure and a string containing the result
    '''

    pinfo('Starting execution of {}'.format(job.procedure))

    # Path of the executable itself.
    bin_path = utilities.CONFIGURATION['bins'] + '/%s %s'

    # We have to rewrite some paths, so we need the offered jobs...
    _, offered_jobs = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    offered_job = [
        _job for _job in offered_jobs if _job.procedure == job.procedure and
        len(_job.arguments) == len(job.arguments)
    ]

    # In case the are either more than one or none such procedures,
    # there is something wrong and we need to abort.
    error = None
    if len(offered_job) > 1:
        error = 'There is more than one matching procedure! Can not execute.'
    elif len(offered_job) < 1:
        error = 'There is no such procedure. Can not execute'

    if error:
        return (1, error)

    # This is the job we are looking for.
    offered_job = offered_job[0]

    # If the argument is 'file', we have to prepend the env_path
    # to the path found in the job file, so that the binary operates
    # on the file downloaded from the Rhizome store.
    for i in range(len(job.arguments)):
        if offered_job.arguments[i] == 'file':
            job.arguments[i] = env_path + job.arguments[i]
            job.arguments[i] = job.arguments[i].replace('//', '/')

    # Execute the job!
    job_process = subprocess.Popen(
        bin_path % (job.procedure, ' '.join(job.arguments)),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    out, err = job_process.communicate()

    # Either it was successful or not, so handle respectively.
    if job_process.returncode != 0:
        return (1, err.rstrip())
    else:
        return (0, out.rstrip())


def is_capable(job):
    '''Function for checking if the server is capable to execute the procedure

    Arguments:
        job -- The job to be checked

    Returns:
        True, is the server is capable, False otherwise
    '''

    pinfo('Checking if capable to execute {}'.format(job.procedure))

    # If there are no requirements for job, just execute.
    if not job.filter_dict:
        return True

    # Get the server capabilities
    cc, capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'],
                                        utilities.CONFIGURATION['location'])

    for requirement in job.filter_dict:
        # Get the capability we are looking for
        capability_line = [
            line for line in capabilities if requirement in line
        ]

        capability_value = capability_line.split('=')[1].rstrip()
        requirement_value = job.filter_dict[requirement]

        # Now check every possible capability and return False if
        # server is not able to fullfil.
        if requirement == 'cpu_load' and int(capability_value) > int(
                requirement_value):
            return False
        if requirement == 'disk_space' and int(capability_value) < int(
                requirement_value):
            return False
        if requirement == 'memory' and int(capability_value) < int(
                requirement_value):
            return False
        if requirement == 'gps_coord' and int(capability_value) > int(
                requirement_value):
            return False

    return True


def return_error(call_bundle,
                 reason,
                 client_sid=None,
                 file_list=[],
                 zip_file_name=None):
    '''This is a generic error handling function. Whenever an errror is
    encountert, this function will be used to inform the client about
    the error.

    Arguments:
        call_bundle -- The bundle where the error happend.
        reason -- The reason of the error

    Keyword Arguments:
        client_sid -- The SID of the client. Since a server can be any hop,
        it is possible that the call_bundle sender is not the client.
        (default: {None})
        file_list -- Files, which should be sent to the client (default: {[]})
        zip_file_name -- The name of the ZIP file created (default: {None})
    '''

    payload_path = None
    payload = None

    # If files should be returned to the client, create a ZIP and open
    # the ZIP file.
    if file_list:
        payload_path = utilities.make_zip(
            file_list,
            name=zip_file_name + '_error.zip',
            subpath_to_remove=zip_file_name)

        payload = open(payload_path, 'r')

    # Simply insert the error bundle containing all relevant data
    SERVAL.rhizome.new_bundle(
        name=call_bundle.manifest.name,
        payload=payload.read() if payload else '',
        service=RPC,
        recipient=client_sid,
        custom_manifest={
            'type': ERROR,
            'reason': reason,
            'originator': call_bundle.manifest.originator
        })


def server_handle_call(potential_call):
    '''Main call handling function. At this point, we can certainly say
    that we received a call which should be handled.

    Arguments:
        potential_call -- The bundle containing the call
    '''

    global CLEANUP_BUNDLES
    global SERVAL
    global SERVER_DEFAULT_SID

    # All involved files in a call should be uniquely named.
    # Thus, we use the procedure name, the server SID and a timestamp.
    zip_file_base_path = '{}_{}_{}'.format(potential_call.manifest.name,
                                           potential_call.manifest.sender,
                                           potential_call.manifest.version)

    # Download the payload from the Rhizome store
    with open(zip_file_base_path + '_call.zip', 'wb') as zip_file:
        zip_file.write(potential_call.payload)

    jobs = None
    job_file_path = None
    file_list = None

    # If we have a valid ZIP file, we extract it and parse the job file.
    if zipfile.is_zipfile(zip_file_base_path + '_call.zip'):
        file_list = utilities.extract_zip(zip_file_base_path + '_call.zip',
                                          zip_file_base_path + '/')

        # Find the job file and parse it.
        for _file in file_list:
            if _file.endswith('.jb'):
                jobs = utilities.parse_jobfile(_file)
                job_file_path = _file
    else:
        # We have not found a valid ZIP file, so abort here and inform
        # the client.
        reason = '{} is not a valid ZIP file.'.format(zip_file_base_path)
        pfatal(reason)
        return_error(
            potential_call,
            reason,
            client_sid=potential_call.manifest.originator)
        return

    # We could not find any jobs in the ZIP, so abort and inform the client.
    if jobs is None:
        reason = 'Call has no job file.'
        pfatal(reason)
        return_error(
            potential_call,
            reason,
            client_sid=potential_call.manifest.originator,
            file_list=file_list,
            zip_file_name=zip_file_base_path)
        return

    possible_job = None
    possible_next_job = None

    # We iterate through all jobs and try to find a job for us.
    # We remember our job and the job for the next hop, if available.
    for job in jobs.joblist:
        if job.status == Status.OPEN and job.server == SERVER_DEFAULT_SID:
            possible_job = job
            try:
                current_position = jobs.joblist.index(possible_job)
                possible_next_job = jobs.joblist[current_position + 1]
            except IndexError:
                pass
            break

    # Do a check, if the procedure is offered and inform the client if not.
    if possible_job is None or not server_offering_procedure(possible_job):
        reason = 'Server is not offering this procedure.'
        pfatal(reason)
        return_error(
            potential_call,
            reason,
            client_sid=potential_call.manifest.originator,
            file_list=file_list,
            zip_file_name=zip_file_base_path)
        return

    # Check, if the server is capable to execute the procedure and inform
    # the client if not.
    if not is_capable(possible_job):
        reason = 'Server is not capable to execute the job.'
        pfatal(reason)
        return_error(
            potential_call,
            reason,
            client_sid=potential_call.manifest.originator,
            file_list=file_list,
            zip_file_name=zip_file_base_path)
        return

    # Since we are now confident about the job, we sent an ACK and start
    # processing.
    try:
        SERVAL.rhizome.new_bundle(
            name=potential_call.manifest.name,
            payload='',
            service=RPC,
            recipient=potential_call.manifest.sender,
            custom_manifest={
                'type': ACK,
                'originator': potential_call.manifest.originator
            })
    except DuplicateBundleException:
        pass

    # After sending the ACK, execute the procedure and store the result.
    code, result = server_execute_procedure(possible_job,
                                            zip_file_base_path + '/')
    result_decoded = result.decode('utf-8')

    # Here we need to prepare the job for the next hop.
    if possible_next_job is not None:
        pinfo("Preparing job for next hop.")
        # After executing the job, we have to update the job_file.
        # Therefore, we first read it.
        job_file = open(job_file_path, 'r+')
        lines = job_file.readlines()

        if code == 0:
            # The execution was successful, so we append DONE to the
            # corresponding line.
            lines[possible_job.line] = utilities.insert_to_line(
                lines[possible_job.line], 'DONE')
        else:
            # Something went wrong, we append ERROR to the line.
            lines[possible_job.line] = utilities.insert_to_line(
                lines[possible_job.line], 'ERROR')

        # The result of the job has to be provided as input for the next hop,
        # so we have to replace the placeholder ## with the result in the
        # job file.
        lines[possible_next_job.line] = lines[possible_next_job.line].replace(
            '##', result_decoded.replace(zip_file_base_path, ''))

        # The file has been updated, so we can write the content and close it.
        job_file.seek(0)
        for line in lines:
            job_file.write(line)
        job_file.close()

        # If the next server is again any, we search a new one for the next
        # job. This is about the same process as in the client.
        if possible_next_job.server == 'any':
            pinfo('Searching next server.')
            servers = utilities.parse_available_servers(
                SERVAL.rhizome, SERVER_DEFAULT_SID)
            if not servers:
                reason = 'Could not find any suitable servers. Aborting.'
                pfatal(reason)
                return_error(
                    potential_call,
                    reason,
                    client_sid=potential_call.manifest.originator,
                    file_list=[result_decoded, job_file_path],
                    zip_file_name=zip_file_base_path)
                return

            servers = utilities.find_available_servers(servers,
                                                       possible_next_job)
            if not servers:
                reason = 'Could not find any suitable servers. Aborting.'
                pfatal(reason)
                return_error(
                    potential_call,
                    reason,
                    client_sid=potential_call.manifest.originator,
                    file_list=[result_decoded, job_file_path],
                    zip_file_name=zip_file_base_path)
                return

            possible_next_job.server = utilities.select_server(
                servers, CONFIGURATION['server']).sid

            utilities.replace_any_to_sid(job_file_path, possible_next_job.line,
                                         possible_next_job.server)

            pinfo('Found next server: {}'.format(possible_next_job.server))

        # Done. Make the payload containing all required files, read the
        # payload ...
        payload_path = utilities.make_zip(
            [result_decoded, job_file_path],
            name=zip_file_base_path + '_result',
            subpath_to_remove=zip_file_base_path + '/')
        payload = open(payload_path, 'rb')

        # ... and send the bundle.
        next_hop_bundle = SERVAL.rhizome.new_bundle(
            name=possible_next_job.procedure,
            payload=payload.read(),
            service=RPC,
            recipient=possible_next_job.server,
            custom_manifest={
                'type': CALL,
                'originator': potential_call.manifest.originator
            })

        # We have to remember the bundle id for cleanup lateron.
        id_to_store = next_hop_bundle.bundle_id
        if potential_call.bundle_id in CLEANUP_BUNDLES:
            CLEANUP_BUNDLES[potential_call.bundle_id].append(id_to_store)
        else:
            CLEANUP_BUNDLES[potential_call.bundle_id] = [id_to_store]

    else:
        pinfo('Preparing result for client.')
        # There is no next hop, return the result to the client by
        # building and reading the payload...
        payload_path = utilities.make_zip(
            [result_decoded, job_file_path],
            name=zip_file_base_path + '_result',
            subpath_to_remove=zip_file_base_path + '/')
        payload = open(payload_path, 'rb')

        # ... constructing the custom manifest part ...
        custom_manifest = {
            'type': None,
            'originator': potential_call.manifest.originator
        }

        # ... (if code is 1, an error occured) ...
        if code == 1:
            custom_manifest['type'] = ERROR
        else:
            custom_manifest['type'] = RESULT

        # ... and sending the result.
        result_bundle = SERVAL.rhizome.new_bundle(
            name=possible_job.procedure,
            payload=payload.read(),
            service=RPC,
            recipient=jobs.client_sid,
            custom_manifest=custom_manifest)

        # We have to remember the bundle id for cleanup lateron.
        id_to_store = result_bundle.bundle_id
        if potential_call.bundle_id in CLEANUP_BUNDLES:
            CLEANUP_BUNDLES[potential_call.bundle_id].append(id_to_store)
        else:
            CLEANUP_BUNDLES[potential_call.bundle_id] = [id_to_store]

    # This seems a little dangling, but in every case a payload is created
    # which needs to be closed anyway, so just do it here.
    payload.close()


def server_cleanup_store(bundle):
    '''Simple cleanup function for cleaning up the rhizome store

    Arguments:
        bundle -- The bundle which needs to be cleaned
    '''

    global CLEANUP_BUNDLES
    global SERVAL

    pwarn('Cleaning up store for bundle {}'.format(bundle.bundle_id))

    # Get all IDs associated with this bundle id
    stored_bundle_ids = CLEANUP_BUNDLES[bundle.bundle_id]

    # Iterate over all bundles, set the CLEANUP flag. remove the payload
    # and update the bundle in the Rhizome store
    for stored_bundle_id in stored_bundle_ids:
        stored_bundle = SERVAL.rhizome.get_bundle(stored_bundle_id)
        stored_bundle.refresh()
        stored_bundle.manifest.type = CLEANUP
        stored_bundle.payload = ''
        stored_bundle.update()

    # Finally, clean up the remembered bundle list.
    CLEANUP_BUNDLES.pop(bundle.bundle_id, None)


def server_listen(queue):
    '''The main server listening function

    Arguments:
        queue -- If the procedure should be executed sequentially in a queue
        or not
    '''

    pinfo('Starting server')

    global SERVER_DEFAULT_SID
    global SERVAL

    # Create a RESTful serval_client to Serval with the parameters from
    # the config file and get the Rhizome serval_client.
    SERVAL = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = SERVAL.rhizome
    SERVER_DEFAULT_SID = SERVAL.keyring.default_identity().sid

    # At this point we can publish all offered procedures and capabilities.
    # The publish function is executed once at startup and then periodically.
    pinfo('Publishing procedures and capabilities.')
    server_publish_procedures()

    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].bundle_id

    # This is the main server loop.
    while True:
        bundles = rhizome.get_bundlelist()
        # Iterate over all bundles
        for bundle in bundles:
            # We hit the virtual bottom of the list, so start over again.
            if bundle.bundle_id == token:
                break

            # If it is not a RPC bundle, skip.
            if not bundle.manifest.service == RPC:
                continue

            # At this point, we have an call and have to start handling it.
            # Therefore, we download the manifest.
            try:
                potential_call = rhizome.get_bundle(bundle.bundle_id)
            except DecryptionError:
                continue

            # We could download the bundle, but it seems that we are not the
            # destination, so skip.
            if not potential_call.manifest.recipient == SERVER_DEFAULT_SID:
                continue

            # Yay, ACK received.
            if potential_call.manifest.type == ACK:
                pinfo('Received ACK for {} from {}'.format(
                    potential_call.manifest.name,
                    potential_call.manifest.sender))

            # All checks pass, start the execution (either in background
            # or blocking in a queue)
            if potential_call.manifest.type == CALL:
                pinfo('Received call. Starting handling.')
                if queue:
                    server_handle_call(potential_call)
                else:
                    start_new_thread(server_handle_call, (potential_call, ))

            # If the bundle is a cleanup file, we start the cleanup routine.
            elif potential_call.manifest.type == CLEANUP:
                server_cleanup_store(potential_call)

        # After the for loop, remember the recent bundle id.
        token = bundles[0].bundle_id
        time.sleep(1)
