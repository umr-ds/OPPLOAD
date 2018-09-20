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
from utilities import LOGGER
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


def server_publish_procedures_thread():
    # Start a thread containing this function and execute it every
    # periodically in background.
    update_published_thread = threading.Timer(
        30, server_publish_procedures_thread)
    update_published_thread.daemon = True
    update_published_thread.start()

    server_publish_procedures()


def server_publish_procedures():
    '''This function publishes offered procedures and capabilities
    periodically
    '''

    global SERVAL
    global LOCK
    global SERVER_DEFAULT_SID


    # The the offered procedures and capabilities for publishing.
    offered_procedures = get_offered_procedures(
        utilities.CONFIGURATION['rpcs'])
    capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'],
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
        payload = 'procedures: {}\n'.format(len(offered_procedures))
        for procedure in offered_procedures:
            procedure_str = str(procedure) + '\n'
            payload = payload + procedure_str

        # Put all capabilities to the payload
        payload = payload + 'capabilities: {}\n'.format(len(capabilities))
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
    with open(rpc_defs, 'r') as conf_file:
        for procedure_definition in conf_file:
            # RPC defs are stored as '<NAME> <ARG1> ...'
            procedure_definition_list = procedure_definition.split(' ')
            name = procedure_definition_list[0]
            args = procedure_definition_list[1:]
            offered_procedures.add(Job(procedure=name, arguments=args))

    return offered_procedures


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
    with open(rpc_caps, 'r') as conf_file:
        for capability in conf_file:
            capabilities.add(capability)

    with open(location, 'r') as location_file:
        coords = location_file.readline().split(' ')
        capabilities.add('gps_coord={},{}\n'.format(coords[0], coords[1]))

    return capabilities


def update_capability(capability, value):
    read_caps = None
    updated_caps = []
    with open(utilities.CONFIGURATION['capabilites'], 'r') as caps:
        read_caps = caps.readlines()
        for cap in read_caps:
            cap_key, cap_value = cap.split('=')
            if cap_key != capability:
                updated_caps.append(cap)
                continue
            updated_caps.append('{}={}\n'.format(cap_key, value))

    with open(utilities.CONFIGURATION['capabilites'], 'w') as f:
        for item in updated_caps:
            f.write(item)


def server_offering_procedure(job):
    '''Function for checking if the server is offering the procedure.

    Arguments:
        job -- The job to be checked

    Returns:
        True, if the procedure is offered, False otherwise
    '''

    global LOCK

    with LOCK:
        # The the offered procedures
        offered_jobs = get_offered_procedures(utilities.CONFIGURATION['rpcs'])

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

    # Path of the executable itself.
    bin_path = utilities.CONFIGURATION['bins'] + '/%s %s'

    # We have to rewrite some paths, so we need the offered jobs...
    offered_jobs = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
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

    # If there are no requirements for job, just execute.
    if not job.filter_dict:
        return True

    # Get the server capabilities
    capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'],
                                    utilities.CONFIGURATION['location'])

    for requirement, requirement_value in job.filter_dict.items():
        # Get the capability we are looking for
        capability_line = [
            line for line in capabilities if requirement in line
        ]

        capability_value = capability_line[0].split('=')[1].rstrip()

        if float(capability_value) < float(requirement_value):
            return False

    return True


def return_error(call_bundle,
                 reason,
                 file_list=None,
                 zip_file_name=None):
    '''This is a generic error handling function. Whenever an errror is
    encountert, this function will be used to inform the client about
    the error.

    Arguments:
        call_bundle -- The bundle where the error happend.
        reason -- The reason of the error

    Keyword Arguments:
        file_list -- Files, which should be sent to the client (default: {[]})
        zip_file_name -- The name of the ZIP file created (default: {None})
    '''

    LOGGER.debug('####### {}, {}, {}, {}'.format(call_bundle.bundle_id, reason, file_list, zip_file_name))

    # If files should be returned to the client, create a ZIP and open
    # the ZIP file.
    payload_path = utilities.make_zip(
        file_list,
        name=zip_file_name + '_error.zip',
        subpath_to_remove=zip_file_name)

    payload = open(payload_path, 'rb')

    LOGGER.debug('####### {}, {},'.format(payload_path, payload))

    # Simply insert the error bundle containing all relevant data
    error_bundle = SERVAL.rhizome.new_bundle(
        name=call_bundle.manifest.name,
        payload=payload.read(),
        service=RPC,
        recipient=call_bundle.manifest.originator,
        custom_manifest={
            'type': ERROR,
            'reason': reason,
            'originator': call_bundle.manifest.originator,
            'rpcid': call_bundle.manifest.rpcid
        })
    LOGGER.debug('Returned Error with {} to {}'.format(
        error_bundle.bundle_id, call_bundle.manifest.originator))

    id_to_store = error_bundle.bundle_id
    if call_bundle.bundle_id in CLEANUP_BUNDLES:
        CLEANUP_BUNDLES[call_bundle.bundle_id].append(id_to_store)
    else:
        CLEANUP_BUNDLES[call_bundle.bundle_id] = [id_to_store]


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
    exec_time = int(time.time() * 1000)

    job_id = potential_call.manifest.rpcid
    zip_file_base_path = '{}_{}'.format(job_id, exec_time)

    zip_file_step_path = '{}_step.zip'.format(zip_file_base_path)
    zip_file_result_step_path = '{}_result_step'.format(zip_file_base_path)
    zip_file_result_path = '{}_result'.format(zip_file_base_path)

    # Download the payload from the Rhizome store
    with open(zip_file_step_path, 'wb') as zip_file:
        zip_file.write(potential_call.payload)

    jobs = None
    job_file_path = None
    file_list = None

    # If we have a valid ZIP file, we extract it and parse the job file.
    if zipfile.is_zipfile(zip_file_step_path):
        file_list = utilities.extract_zip(zip_file_step_path,
                                          zip_file_base_path + '/')

        # Find the job file and parse it.
        for _file in file_list:
            if _file.endswith('.jb'):
                jobs = utilities.parse_jobfile(_file)
                job_file_path = _file
    else:
        # We have not found a valid ZIP file, so abort here and inform
        # the client.
        reason = '{} | {} is not a valid ZIP file.'.format(
            job_id, zip_file_step_path)
        LOGGER.critical(reason)
        return_error(
            potential_call,
            reason,
            file_list=[zip_file_step_path],
            zip_file_name=zip_file_step_path)
        return

    # We could not find any jobs in the ZIP, so abort and inform the client.
    if jobs is None:
        reason = '{} | Call has no job file.'.format(job_id)
        LOGGER.critical(reason)
        return_error(
            potential_call,
            reason,
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
    LOGGER.info('{} | Checking if offering {}'.format(job_id,
                                                      possible_job.procedure))
    if possible_job is None or not server_offering_procedure(possible_job):
        reason = 'Server is not offering this procedure.'
        LOGGER.critical(reason)
        return_error(
            potential_call,
            reason,
            file_list=file_list,
            zip_file_name=zip_file_base_path)
        return

    # Check, if the server is capable to execute the procedure and inform
    # the client if not.
    LOGGER.info('{} | Checking if capable to execute {}'.format(
        job_id, possible_job.procedure))
    if not is_capable(possible_job):
        reason = 'Server is not capable to execute the job.'
        LOGGER.critical('{} | {}'.format(job_id, reason))
        return_error(
            potential_call,
            reason,
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
                'originator': potential_call.manifest.originator,
                'rpcid': job_id
            })
    except DuplicateBundleException:
        pass

    # After sending the ACK, execute the procedure and store the result.
    LOGGER.info(
        '{} | -Execution- Starting execution of {}...'
        .format(job_id, job.procedure))
    code, result = server_execute_procedure(possible_job,
                                            zip_file_base_path + '/')
    result_decoded = result.decode('utf-8')

    capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'],
                                    utilities.CONFIGURATION['location'])

    capability_line = [
            line for line in capabilities if 'energy' in line
        ]

    capability_value = float(capability_line[0].split('=')[1].rstrip())

    update_capability(
        'energy', capability_value - float(possible_job.filter_dict['energy']))
    server_publish_procedures()

    # Here we need to prepare the job for the next hop.
    if possible_next_job is not None:
        LOGGER.info('{} | -Runtime- Preparing job {} for next hop.'.format(
            job_id, possible_job.procedure))
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
            LOGGER.info('{} | Searching next server.'.format(job_id))
            reason = utilities.lookup_server(
                SERVAL.rhizome, SERVER_DEFAULT_SID,
                potential_call.manifest.originator, possible_next_job, job_id,
                job_file_path)

            if reason:
                return_error(
                    potential_call,
                    reason,
                    file_list=file_list,
                    zip_file_name=zip_file_base_path)
                return

        # Done. Make the payload containing all required files, read the
        # payload ...
        payload_path = utilities.make_zip(
            zip_file_base_path,
            name=zip_file_result_step_path,
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
                'originator': potential_call.manifest.originator,
                'rpcid': job_id
            })

        LOGGER.info(
            '{} | -Transmission- Next step {} is called: bid is {}'.format(
                job_id, possible_next_job.procedure,
                next_hop_bundle.bundle_id))

        # We have to remember the bundle id for cleanup lateron.
        id_to_store = next_hop_bundle.bundle_id
        if potential_call.bundle_id in CLEANUP_BUNDLES:
            CLEANUP_BUNDLES[potential_call.bundle_id].append(id_to_store)
        else:
            CLEANUP_BUNDLES[potential_call.bundle_id] = [id_to_store]

    else:
        LOGGER.info('{} | -Runtime- Preparing result from {}.'.format(
            job_id, possible_job.procedure))
        # There is no next hop, return the result to the client by
        # building and reading the payload...
        payload_path = utilities.make_zip(
            zip_file_base_path,
            name=zip_file_result_path,
            subpath_to_remove=zip_file_base_path + '/')
        payload = open(payload_path, 'rb')

        # ... constructing the custom manifest part ...
        custom_manifest = {
            'type': None,
            'originator': potential_call.manifest.originator,
            'rpcid': job_id
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

        LOGGER.info('{} | -Transmission- Result is sent: bid is {}'.format(
            job_id, result_bundle.bundle_id))

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

    # check if there are bundles to be cleaned up
    if bundle.bundle_id not in CLEANUP_BUNDLES:
        return

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

    LOGGER.info(' | Starting server')

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
    LOGGER.info(' | Publishing procedures and capabilities.')
    server_publish_procedures_thread()

    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].token

    # This is the main server loop.
    while True:
        bundles = rhizome.get_bundlelist_newsince(token)
        if len(bundles) == 0:
            continue

        bundle = bundles[0]
        token = bundle.token

        # If it is not a RPC bundle, skip.
        if not bundle.manifest.service == RPC:
            continue

        # We could download the bundle, but it seems that we are not the
        # destination, so skip.
        if not bundle.manifest.recipient == SERVER_DEFAULT_SID:
            LOGGER.debug(
                " | Received RPC bundle for other client, skipping. (bid:{})"
                .format(bundle.manifest.id))
            continue

        # At this point, we have an call and have to start handling it.
        # Therefore, we download the manifest.
        try:
            potential_call = rhizome.get_bundle(bundle.bundle_id)
        except DecryptionError:
            LOGGER.error(
                " | Error decrypting received RPC bundle, skipping. (bid:{})"
                .format(bundle.manifest.id))
            continue

        # Yay, ACK received.
        if potential_call.manifest.type == ACK:
            LOGGER.info('{} | Received ACK for {} from {}'.format(
                potential_call.manifest.rpcid,
                potential_call.manifest.name,
                potential_call.manifest.sender))

        # All checks pass, start the execution (either in background
        # or blocking in a queue)
        elif potential_call.manifest.type == CALL:
            LOGGER.info(
                '{} | -Runtime- Received call, starting handling.'
                .format(potential_call.manifest.rpcid))
            if queue:
                server_handle_call(potential_call)
            else:
                start_new_thread(server_handle_call, (potential_call, ))

        # If the bundle is a cleanup file, we start the cleanup routine.
        elif potential_call.manifest.type == CLEANUP:
            LOGGER.info('{} | Cleaning up store for bundle {}'.format(
                potential_call.manifest.rpcid, bundle.bundle_id))
            server_cleanup_store(potential_call)

        elif potential_call.manifest.type == RESULT:
            LOGGER.debug('{} | Recieved RPC result, skipping.'.format(
                potential_call.manifest.rpcid))
        else:
            LOGGER.error(
                "{} | Received RPC bundle of unknown type ({}), skipping."
                .format(potential_call.manifest.rpcid,
                        potential_call.manifest.type))

