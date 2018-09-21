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

from pyserval.client import Client
from pyserval.exceptions import DecryptionError
from pyserval.exceptions import InvalidTokenError, RhizomeHTTPStatusError
from requests.exceptions import ConnectionError

import utilities
from utilities import LOGGER
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

    LOGGER.info(
        ' | Client SID: {}, job file: {}'
        .format(client_default_sid, job_file_path))

    # Parse the job file and store all jobs in jobs.
    jobs = utilities.parse_jobfile(job_file_path)
    if not jobs:
        LOGGER.critical(
            ' | Job file {} does not contain jobs. Aborting.'
            .format(job_file_path))
        return

    # This is the first job to be called. We remember it here for
    # further processing.
    first_job = jobs.joblist[0]

    hash_base_string = '{}{}{:.9f}'.format(first_job.procedure,
                                           client_default_sid,
                                           time.time())
    encoded_hash_base_string = hash_base_string.encode('utf-8')

    job_id = hashlib.sha256(encoded_hash_base_string).hexdigest()[:8]

    LOGGER.info(
        '{} | -Runtime- Job file parsed. First Job is {} with ID {}.'
        .format(job_id, first_job.procedure, job_id))

    # If the server address is 'any', we have to find a server, which
    # offers this procedure.
    if first_job.server == 'any':
        LOGGER.info(
                '{} | The address is any, searching for server.'
                .format(job_id))

        reason = utilities.lookup_server(rhizome, client_default_sid,
                                         client_default_sid, first_job, job_id,
                                         job_file_path)

        if reason:
            return

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

    LOGGER.info('{} | Prepared ZIP file {} for call.'.format(job_id, zip_file))

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
    LOGGER.info('{} | -Transmission- Procedure {} is called: bid is {}'.format(
        job_id, first_job.procedure, call_bundle.bundle_id))

    # Now we wait for the result.
    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].token

    while True:
        try:
            bundles = rhizome.get_bundlelist_newsince(token)

        except ConnectionError:
            LOGGER.warn(
                " | ConnectionError while calling newsince, continuing...")
            time.sleep(1)
            continue

        except RhizomeHTTPStatusError as e:
            LOGGER.warn(
                " | RhizomeHTTPStatusError while calling newsince, hint: {}, continuing..."
                .format(e))
            time.sleep(1)
            continue

        except InvalidTokenError as e:
            LOGGER.warn(
                " | InvalidTokenError while calling newsince, hint: {}, continuing..."
                .format(e))
            time.sleep(1)
            continue

        except KeyError as e:
            LOGGER.warn(
                " | KeyError while calling newsince, hint: {}, continuing...".
                format(e))
            time.sleep(1)
            continue

        except JSONDecodeError as e:
            LOGGER.warn(
                " | JSONDecodeError while calling newsince, hint: {}, continuing..."
                .format(e))
            time.sleep(1)
            continue

        bundle = bundles[0]
        token = bundle.token

        # Don't bother, if it is not a RPC bundle.
        if not bundle.manifest.service == RPC:
            continue

        # Ignore bundles not sended to me
        if not bundle.manifest.recipient == client_default_sid:
            continue

        # Before further checks, we have to download the manifest
        # to have all metadata available.
        try:
            potential_result = rhizome.get_bundle(bundle.bundle_id)
        except DecryptionError:
            continue

        # Yay, ACK received.
        if (potential_result.manifest.type == ACK and
                potential_result.manifest.rpcid == job_id):
            LOGGER.info('{} | Received ACK from {}'.format(
                potential_result.manifest.rpcid,
                potential_result.manifest.sender))

        # Here we have the result.
        if (potential_result.manifest.type == RESULT and
                potential_result.manifest.rpcid == job_id):
            LOGGER.info(
                '{} | -Runtime- Received result.'.format(
                    potential_result.manifest.rpcid))
            # Use the same filename as for the call, except
            # we append result instead of call to the name.
            result_path = zip_file_base_path + '_result.zip'

            # Download the payload from the Rhizome store and
            # write it to the mentioned ZIP file
            with open(result_path, 'wb') as zip_file:
                zip_file.write(potential_result.payload)
            LOGGER.info(
                '{} | Download is done. Cleaning up store.'.format(job_id))

            # The final step is to cleanup the store by updating
            # the call bundle by setting the CLEANUP flag to the
            # bundle and removing the payload.
            call_bundle.refresh()
            call_bundle.manifest.type = CLEANUP
            call_bundle.payload = ''
            call_bundle.update()

            LOGGER.info(
                '{} | -End- Finished RPC, result: {}'
                .format(job_id, result_path))
            break

        # One of the servers had an error, so see what is going on.
        if (potential_result.manifest.type == ERROR
                and potential_result.manifest.rpcid == job_id):

            result_path = zip_file_base_path + '_error.zip'

            # Download the payload from the Rhizome store and
            # write it to the mentioned ZIP file
            with open(result_path, 'wb') as zip_file:
                zip_file.write(potential_result.payload)
            LOGGER.info(
                '{} | Download is done. Cleaning up store.'.format(job_id))

            call_bundle.refresh()
            call_bundle.manifest.type = CLEANUP
            call_bundle.payload = ''
            call_bundle.update()

            LOGGER.warn(
                u'{} | -End- Received error \'{}\' for job {}.'
                .format(
                    job_id,
                    potential_result.manifest.reason,
                    potential_result.manifest.name))
            break
