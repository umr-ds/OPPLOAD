#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''The main client module, contains everything needed to call a remote
procedure
'''

import os
import time
import math
import sys
import hashlib

from pyserval.exceptions import DecryptionError
from pyserval.client import Client

import utilities
from utilities import pdebug, pfatal, pinfo, pwarn
from utilities import CALL, ACK, RESULT, ERROR, CLEANUP, RPC
from utilities import CONFIGURATION
from job import Job


def client_call(job_file_path):
    '''Client main call function. Calls a remote procedure found in
    job_file_path.

    Arguments:
        job_file_path {str} -- Path to the job file
    '''

    # Create a RESTful serval_client to Serval with the parameters from
    # the config file and get the Rhizome serval_client.
    SERVAL = Client(
            host=CONFIGURATION['host'],
            port=int(CONFIGURATION['port']),
            user=CONFIGURATION['user'],
            passwd=CONFIGURATION['passwd']
        )
    rhizome = SERVAL.rhizome
    client_default_sid = SERVAL.keyring.default_identity().sid

    pinfo('Client SID: {}, job file: {}'.format(client_default_sid,
                                                job_file_path))

    # Parse the job file and store all jobs in jobs.
    jobs = utilities.parse_jobfile(job_file_path)
    if not jobs:
        pfatal('Job file {} does not contain jobs. Aborting.'.format(
            job_file_path))
        return

    # This is the first job to be called. We remember it here for
    # further processing.
    first_job = jobs.joblist[0]

    hash_base_string = '{}_{}_{}'.format(first_job.procedure,
                                         first_job.server,
                                         str(math.floor(time.time())))
    encoded_hash_base_string = hash_base_string.encode('utf-8')

    job_id = hashlib.sha256(encoded_hash_base_string).hexdigest()[:8]

    pinfo('({}) Job file parsed. First Job is {} with ID {}.'.format(
        job_id, first_job.procedure, job_id))

    # If the server address is 'any', we have to find a server, which
    # offers this procedure.
    if first_job.server == 'any':
        pinfo('({}) The address is any, searching for server.'.format(job_id))

        for i in range(10):
            # First, get all available offers from the Rhizome store.
            servers = utilities.parse_available_servers(rhizome,
                                                        client_default_sid)
            if not servers:
                pwarn(
                    '({}) Could not find any servers for the job in try {}/10'.
                    format(job_id, i))
                time.sleep(1)
                continue

            pinfo('({}) Found {} offers. Searching possible server.'.format(
                len(servers), job_id))

            # Secondly, get servers offering the desired procedure.
            servers = utilities.find_available_servers(servers, first_job)
            if not servers:
                pwarn(
                    '({}) Could not find any capable servers for the job in try {}/10'.
                    format(job_id, i))
                time.sleep(1)
                continue

            break

        if not servers:
            pfatal(
                '({}) Could not find any servers for the job'.format(job_id))
            return

        pinfo('({}) Found {} servers. Getting server based on {} algorithm.'.
              format(job_id, len(servers), CONFIGURATION['server']))

        # If we have a list of potential servers, get the server based on
        # the selection algorithm as in the configure script.
        first_job.server = utilities.select_server(servers,
                                                   CONFIGURATION['server']).sid

        # Now everything is done. Set the SID to the job file and continue
        # with processing.
        utilities.replace_any_to_sid(job_file_path, first_job.line,
                                     first_job.server)

        pinfo('({}) Using server {} for the fist job.'.format(
            job_id, first_job.server))

    # All involved files in a call should be uniquely named.
    # Thus, we use the job id, which is a hash of
    # procedure name, the server SID and a timestamp.
    zip_file_base_path = job_id

    # Iterate through all arguments and check if it is file.
    # If so, add it to the file list to be ZIP'd.
    zip_list = []
    for arg in first_job.arguments:
        if not os.path.isfile(arg):
            continue
        zip_list.append(arg)
    # Of course, we have to add the job file to the file list.
    # For sanity reasons we also strip away all whitespace characters.
    zip_list.append(job_file_path)
    zip_list = list(map(str.strip, zip_list))

    # Now we can crate the ZIP file...
    zip_file = utilities.make_zip(zip_list, zip_file_base_path + '_call')

    pinfo('({}) Prepared ZIP file {} for call.'.format(
        job_id, zip_file_base_path + '_call'))

    # ... open it ...
    payload = open(zip_file, 'rb')
    # ... and create a new Rhizome bundle containing all relevant information
    # for the call.
    call_bundle = rhizome.new_bundle(
        name=first_job.procedure,
        payload=payload.read(),
        service=RPC,
        recipient=first_job.server,
        custom_manifest={
            'type': CALL,
            'originator': client_default_sid,
            'rpcid': job_id
        })
    payload.close()
    pinfo('({}) Procedure {} is called: bid is {}'.format(
        job_id, first_job.procedure, call_bundle.bundle_id))

    # Now we wait for the result.
    token = call_bundle.bundle_id
    result_received = False
    total_wait_time = 0
    while not result_received:
        bundles = rhizome.get_bundlelist()

        for bundle in bundles:
            # Cool, we are done.
            if result_received:
                break

            # We hit the virtual bottom of the bundle list, so we need
            # to poll the recent bundles.
            if bundle.bundle_id == token:
                break

            # Don't bother, if it is not a RPC bundle.
            if not bundle.manifest.service == RPC:
                continue

            # Before further checks, we have to download the manifest
            # to have all metadata available.
            try:
                potential_result = rhizome.get_bundle(bundle.bundle_id)
            except DecryptionError:
                continue

            # Although it is a RPC bundle for this client, it is not
            # the right result for the procedure.
            if not potential_result.manifest.name == first_job.procedure:
                continue

            # Yay, ACK received.
            if potential_result.manifest.type == ACK and potential_result.manifest.rpcid == job_id:
                pinfo('({}) Received ACK from {}'.format(
                    potential_result.manifest.rpcid,
                    potential_result.manifest.sender))

            # Here we have the result.
            if potential_result.manifest.type == RESULT and potential_result.manifest.rpcid == job_id:
                pinfo(
                    '({}) Received result, preparing download.'.format(
                        potential_result.manifest.rpcid))
                # Use the same filename as for the call, except
                # we append result instead of call to the name.
                result_path = zip_file_base_path + '_result.zip'

                # Download the payload from the Rhizome store and
                # write it to the mentioned ZIP file
                with open(result_path, 'wb') as zip_file:
                    zip_file.write(potential_result.payload)
                pinfo(
                    '({}) Download is done. Cleaning up store.'.format(job_id))

                # The final step is to cleanup the store by updating
                # the call bundle by setting the CLEANUP flag to the
                # bundle and removing the payload.
                call_bundle.refresh()
                call_bundle.manifest.type = CLEANUP
                call_bundle.payload = ''
                call_bundle.update()
                result_received = True

                pinfo('({}) Received result: {}'.format(job_id, result_path))
                break

            # One of the servers had an error, so see what is going on.
            if potential_result.manifest.type == ERROR:
                call_bundle.refresh()
                call_bundle.manifest.type = CLEANUP
                call_bundle.payload = ''
                call_bundle.update()
                result_received = True
                pwarn(
                    '({}) Received error \'{}\' from {} for call {}. See {} for more information'.
                    format(job_id,
                           potential_result.manifest.reason,
                           potential_result.manifest.sender,
                           potential_result.manifest.name,
                           result_path))
                break

        # After the for loop, remember the recent bundle id.
        token = bundles[0].bundle_id
        total_wait_time = total_wait_time + 1
        time.sleep(1)
