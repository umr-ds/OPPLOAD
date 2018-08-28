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
from job import Status, Job
from pyserval.exceptions import DecryptionError
import sys
import shutil
import math

# This is the global serval RESTful client object
SERVAL = None

# A threading lock for critical parts like updating offered procedures.
LOCK = threading.RLock()

# A dict where all bundles are stored which have to be cleaned up after execution.
CLEANUP_BUNDLES = {}

SERVER_DEFAULT_SID = None

def server_publish_procedures():
    '''Publishes all offered procedures.
    '''

    global SERVAL
    global LOCK
    global SERVER_DEFAULT_SID

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
            procedure_str = str(procedure)
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
            SERVAL.rhizome.new_bundle(
                name=SERVER_DEFAULT_SID,
                payload=payload,
                service=utilities.OFFER)

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
            name = procedure_definition_list[0]
            args = procedure_definition_list[1:]
            offered_procedures.add(Job(procedure=name, arguments=args))
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

def server_offering_procedure(job):
    '''Checks, if the given procedure is offered by the server.
    Args:
        procedure (Procedure): The procedure to check.

    Returns:
        bool: True, if the procedure is offered, false otherwise.
    '''
    global LOCK

    with LOCK:
        opc, offered_jobs = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
        for offered_job in offered_jobs:
            if offered_job.procedure != job.procedure:
                continue

            if len(offered_job.arguments) != len(job.arguments):
                continue

            bin_path = '%s/%s' % (utilities.CONFIGURATION['bins'],
                                  job.procedure)

            if not os.path.exists(bin_path) or not os.access(bin_path, os.X_OK):
                pwarn('Server is offering procedure \'%s\', ' \
                        'but it seems the binary %s/%s is not present ' \
                        'or it is not executable. ' \
                        'Will not try to execute.' \
                        % (job.procedure, utilities.CONFIGURATION['bins'], job.procedure)
                    )
                continue
            pinfo('Offering procedure \'%s\'.' % job.procedure)
            return True

        return False

def server_execute_procedure(job, env_path):
    '''Main execution function.
    Args:
        procedure (Procedure): The procedure to be executed.

    Returns:
        (int, str): Returns a tuple containing (return code, stdout)
    '''
    pinfo('Starting execution of \'%s\'.' % job.procedure)
    bin_path = utilities.CONFIGURATION['bins'] + '/%s %s'

    _, offered_jobs = get_offered_procedures(utilities.CONFIGURATION['rpcs'])
    offered_job = [
        _job for _job in offered_jobs if _job.procedure == job.procedure
        and len(_job.arguments) == len(job.arguments)
    ]

    error = None
    if len(offered_job) > 1:
        error = 'There is more than one matching procedure! Can not execute.'
    elif len(offered_job) < 1:
        error = 'There is no such procedure. Can not execute'

    if error:
        pwarn(error)
        return (1, error)

    offered_job = offered_job[0]

    for i in range(len(job.arguments)):
        if offered_job.arguments[i] == 'file':
            job.arguments[i] = env_path + job.arguments[i]

    job_process = subprocess.Popen(
        bin_path % (job.procedure, ' '.join(job.arguments)),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    out, err = job_process.communicate()

    if job_process.returncode != 0:
        pwarn('Execution of \'%s\' was not successfull. Will return error %s\n' \
            % (job.procedure, err))
        return (1, err.rstrip())
    else:
        pinfo('Execution of \'%s\' was successfull with result %s' %
              (job.procedure, out))
        return (0, out.rstrip())


def is_capable(job):
    if not job.filter_dict:
        return True

    cc, capabilities = get_capabilities(utilities.CONFIGURATION['capabilites'])

    for requirement in job.filter_dict:
        capability_line = [
            line for line in capabilities if requirement in line
        ]
        capability_line = capability_line[0].split('=')
        capability_type = capability_line[0]
        capability_value = capability_line[1].rstrip()
        unpacked_requirement_value = job.filter_dict[requirement][0]
        unpacked_requirement_op = job.filter_dict[requirement][1]
        if not eval("{} {} {}".format(
                capability_value, unpacked_requirement_op,
                unpacked_requirement_value)):
            return False

    return True


def server_handle_call(potential_call):
    '''Main handler function for an incoming call.
    Args:
        potential_call (Bundle):    The potential call, which has to be handled.
        my_sid (ServalIdentity):    ServalIdentity of the server
    '''
    pinfo('Received call. Will check if procedure is offered.')

    global CLEANUP_BUNDLES
    global SERVAL
    global SERVER_DEFAULT_SID

    client_sid = None

    # If the server offers the procedure,
    # we first have to download the file because it will be removed as soon we send the ack.
    # If in the next line might be obscolete, because only the text is sent to all servers not the file itself
    path = '/tmp/{}_{}_{}_call.zip'.format(potential_call.manifest.name, potential_call.manifest.sender, potential_call.manifest.version)
    with open(path, 'wb') as zip_file:
        zip_file.write(potential_call.payload)

    jobs = None
    job_file_path = None

    extract_path = '/tmp/{}_{}/'.format(potential_call.bundle_id, SERVER_DEFAULT_SID)

    # If we have a valid ZIP file, we extract it and parse the job file.
    if zipfile.is_zipfile(path):
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
    for job in jobs.joblist:
        if job.status == Status.OPEN and job.server == SERVER_DEFAULT_SID:
            possible_job = job
            try:
                current_position = jobs.joblist.index(possible_job)
                possible_next_job = jobs.joblist[current_position + 1]
            except IndexError:
                pass
            break

    # Let's do a final check, if the procedure is offered.
    if possible_job is None or not server_offering_procedure(possible_job):
        pfatal("Server is not offering this procedure.")
        # TODO Error handling!
        return

    if not is_capable(possible_job):
        pfatal("Server is not capable to execute the job.")
        # TODO Error handling!
        return

    # Since we are not confident about the job, we sent an ACK and start processing.
    try:
        SERVAL.rhizome.new_bundle(
            name=potential_call.manifest.name,
            payload="",
            service="RPC",
            recipient=client_sid,
            custom_manifest={"type": ACK}
        )
    except DuplicateBundleException as e:
        pass

    pinfo('Ack is sent. Will execute procedure.')

    # After sending the ACK, start the execution.
    code, result = server_execute_procedure(possible_job, extract_path)
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

        lines[possible_next_job.line] = lines[possible_next_job.line].replace('##', result_decoded.replace(extract_path, ''))

        # So, the file has been updated, so we can write the content and close it.
        job_file.seek(0)
        for line in lines:
            job_file.write(line)
        job_file.close()

        # If the next server is again any, we search a new one for the next job
        if possible_next_job.server == 'any':
            servers = utilities.find_available_servers(
                SERVAL.rhizome, possible_next_job, SERVER_DEFAULT_SID)

            if not servers:
                # TODO: Here we have to have some error handling!
                pfatal("Could not find any suitable servers. Aborting.")
                return

            possible_next_job.server = servers[0]
            utilities.replace_any_to_sid(job_file_path, possible_next_job.line, possible_next_job.server)

        # Done. Now we only have to make the payload...
        zip_name = SERVER_DEFAULT_SID + '_' + str(math.floor(time.time()))
        payload_path = utilities.make_zip([result_decoded, job_file_path], name=zip_name, subpath_to_remove=extract_path)

        payload = open(payload_path, 'rb')

        # ... and send the bundle.
        next_hop_bundle = SERVAL.rhizome.new_bundle(
            name=possible_next_job.procedure,
            payload=payload.read(),
            service="RPC",
            recipient=possible_next_job.server,
            custom_manifest={"type": CALL}
        )

        id_to_store = next_hop_bundle.bundle_id
        if potential_call.bundle_id in CLEANUP_BUNDLES:
            CLEANUP_BUNDLES[potential_call.bundle_id].append(id_to_store)
        else:
            CLEANUP_BUNDLES[potential_call.bundle_id] = [id_to_store]

    else:
        zip_name = SERVER_DEFAULT_SID + '_' + str(math.floor(time.time()))
        payload_path = utilities.make_zip([result_decoded, job_file_path], name=zip_name, subpath_to_remove=extract_path)

        payload = open(payload_path, 'rb')

        custom_manifest = {
            'recipient': jobs.client_sid,
            'sender': SERVER_DEFAULT_SID,
            'type': None
        }

        # If code is 1, an error occured.
        if code == 1:
            custom_manifest['type'] = ERROR
        else:
            custom_manifest['type'] = RESULT


        result_bundle = SERVAL.rhizome.new_bundle(
            name=possible_job.procedure,
            payload=payload.read(),
            service="RPC",
            custom_manifest=custom_manifest)

        id_to_store = result_bundle.bundle_id
        if potential_call.bundle_id in CLEANUP_BUNDLES:
            CLEANUP_BUNDLES[potential_call.bundle_id].append(id_to_store)
        else:
            CLEANUP_BUNDLES[potential_call.bundle_id] = [id_to_store]

    payload.close()
    os.chdir(cwd)

def server_cleanup_store(bundle):
    # Try to lookup the BID for the Bundle to be cleaned,
    # make a new clear bundle based on the gathered BID and insert this bundle.
    # Finally, remove the id.
    # If it fails, just return.
    global CLEANUP_BUNDLES
    global SERVAL

    stored_bundle_ids = CLEANUP_BUNDLES[bundle.bundle_id]

    for stored_bundle_id in stored_bundle_ids:
        stored_bundle = SERVAL.rhizome.get_bundle(stored_bundle_id)
        stored_bundle.refresh()
        stored_bundle.manifest.type = CLEANUP
        stored_bundle.payload = ""
        stored_bundle.update()

    CLEANUP_BUNDLES.pop(bundle.bundle_id, None)

def server_listen(queue):
    '''Main listening function.
    '''
    global SERVER_DEFAULT_SID
    global SERVAL


    # Create a RESTful serval_client to Serval with the parameters from the config file
    # and get the Rhizome serval_client.
    SERVAL = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = SERVAL.rhizome
    SERVER_DEFAULT_SID = SERVAL.keyring.default_identity().sid

    # At this point we can publish all offered procedures.
    # The publish function is executed once at startup and then every 30 seconds.
    server_publish_procedures()

    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].bundle_id

    # This is the main server loop.
    while True:
        bundles = rhizome.get_bundlelist()
        for bundle in bundles:
            if bundle.bundle_id == token:
                break

            if not bundle.manifest.service == 'RPC':
                continue

            # At this point, we have an call and have to start handling it.
            # Therefore, we download the manifest.
            try:
                potential_call = rhizome.get_bundle(bundle.bundle_id)
            except DecryptionError:
                continue

            if not potential_call.manifest.recipient == SERVER_DEFAULT_SID:
                continue

            # If the bundle is a call, we start a handler thread.
            if potential_call.manifest.type == CALL:
                if queue:
                    server_handle_call(potential_call)
                else:
                    start_new_thread(server_handle_call, (potential_call, ))

            # If the bundle is a cleanup file, we start the cleanup routine.
            elif potential_call.manifest.type == CLEANUP:
                server_cleanup_store(potential_call)

        token = bundles[0].bundle_id

        time.sleep(1)
