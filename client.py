#!/usr/bin/env python3

# -*- coding: utf-8 -*-
'''DTN-RPyC client.

This module contains all functions needed by the DTN-RPyC client, especially
the call functions.
'''

import os
import random
import time
import math
from pyserval.client import Client
import utilities
from utilities import pdebug, pfatal, pinfo, CALL, ACK, RESULT, ERROR, CLEANUP, CONFIGURATION
import threading
from pyserval.exceptions import DecryptionError
import sys
from job import Status, Job, FileNotFound

my_sid = None
PROCEDURES = {}

SERVAL = None

def client_call(job_file_path):
    # Create a RESTful serval_client to Serval with the parameters from the config file
    # and get the Rhizome serval_client.
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
    client_default_sid = SERVAL.keyring.default_identity().sid

    jobs = utilities.parse_jobfile(job_file_path)
    if not jobs:
        pfatal("Could not parse the job file!")
        return

    first_job = jobs.joblist[0]

    pdebug(first_job.line)

    # If the server address is 'any', we have to find a server, which offers this procedure.
    if first_job.server == 'any':
        servers = utilities.find_available_servers(rhizome, first_job)

        if not servers:
            pfatal("Could not find any suitable servers. Aborting.")
            return

        # TODO: Here we have to implement the server selection mechanism
        first_job.server = servers[0]
        utilities.replace_any_to_sid(job_file_path, first_job.line, first_job.server)

    # set the payload to file
    zip_list = []

    for arg in first_job.arguments:
        if not os.path.isfile(first_job.arguments[0]):
            continue
        zip_list.append(arg)
    # create zipfile
    zip_list.append(job_file_path)
    zip_list = list(map(str.strip, zip_list))
    zip_file = utilities.make_zip(zip_list, client_default_sid + '_' + str(math.floor(time.time())))

    payload = open(zip_file, 'rb')

    # create new bundle with default identity
    new_bundle = rhizome.new_bundle(
        name=first_job.procedure,
        payload=payload.read(),
        service="RPC",
        recipient=first_job.server,
        custom_manifest={"type": CALL, 'args': 'jobfile'}
    )

    payload.close()

    all_bundles = rhizome.get_bundlelist()
    token = all_bundles[0].bundle_id

    result_received = False
    while not result_received:
        bundles = rhizome.get_bundlelist()

        for bundle in bundles:

            if result_received:
                break

            if bundle.bundle_id == token:
                break

            if not bundle.manifest.service == 'RPC':
                continue

            # Before further checks, we have to download the manifest
            # to have all metadata available.
            try:
                potential_result = rhizome.get_bundle(bundle.bundle_id)
            except DecryptionError:
                continue

            if not potential_result.manifest.name == first_job.procedure:
                pdebug("Not the right procedure: {}:{}".format(potential_result.manifest.name, first_job.procedure))
                continue

            # At this point, we know that there is a RPC file in the store
            # and it is for us. Start parsing.
            if potential_result.manifest.type == ACK:
                pinfo('Received ACK. Will wait for result.')


            if potential_result.manifest.type == RESULT:
                # It is possible, that the result is a file.
                # Therefore, we have to check the result field in the bundle.
                # If it is a file, download it and return the path to the
                # downloaded file.
                # Otherwise, just return the result.
                result_str = ''
                path = '/tmp/%s_%s' % (potential_result.manifest.name, potential_result.manifest.version)
                with open(path, 'wb') as zip_file:
                    zip_payload = SERVAL.rhizome.get_payload(potential_result)
                    zip_file.write(zip_payload)
                    result_str = path

                # The final step is to clean up the store.
                # Therefore, we create a new bundle with an
                # empty payload and CLEANUP as the type.
                # Since the BID is the same as in the call,
                # the call bundle will be updated with an empty file.
                # This type will instruct the server to clean up
                # the files involved during this RPC.
                #if server != 'all' or server != 'broadcast':
                # clear_bundle = utilities.make_bundle([
                #     ('type', CLEANUP),
                #     ('name', name),
                #     ('args', args),
                #     ('sender', my_sid.sid)
                # ])
                # FIXME rework cleanup
                #rhizome.insert(clear_bundle, '', my_sid.sid, call_bundle.id)

                pinfo('Received result: %s' % result_str)
                result_received = True

            if potential_result.manifest.type == ERROR:
                pfatal(
                    'Received error response with the following message: %s' \
                    % potential_result.manifest.result
                )

                result_received = True

        token = bundles[0].bundle_id
        time.sleep(1)