# -*- coding: utf-8 -*-
'''DTN-RPyC filter module.

This module filters servers by their capabilities.
'''
import restful
import utilities
from utilities import pfatal, pinfo
import sys
import socket
import copy
parsed_arguments = {}
my_sid = ""

def client_find_server(rhiz, args):
    '''Searches for all servers, which have to pass different filters.
    Args:
        rhiz (Rhizome):     Rhizome connection to Serval
        args (list(str)):   All filter arguments in a list.

    Returns:
        str: SID(s) of the server(s) or None, if not found.
    '''
    global parsed_arguments
    global my_sid
    for arg in args:
        arg = arg.split("=")
        parsed_arguments[arg[0]] = arg[1]

    # If there are no bundles, the are no servers offering anything. Abort.
    bundles = rhiz.get_bundlelist()
    if not bundles:
        return None
    server_list = {}
    for bundle in bundles:
        name = str(bundle).split(':')[1]
        if not bundle.service == 'file' or name  == (str(my_sid) + ".info"): #file
            continue
        server_id = (name.split('.')[0])
        offers = rhiz.get_decrypted(bundle.id).split('\n')
        for offer in offers:
            if offer == '':
               continue
            offer_arg = offer.split("=")
            if offer_arg[0] in parsed_arguments:
                if not server_id in server_list:
                    server_list[server_id] = set()
                server_list[server_id].add(offer)
            else:
                continue
    return server_list

def client_filter(args):
    ''' Main filter  function.

    Args:
        args (list of strings): Filter arguments.
    '''
    # Create a RESTful connection to Serval with the parameters from the config file
    # and get the Rhizome connection.
    connection = restful.RestfulConnection(
        host=utilities.CONFIGURATION['host'],
        port=int(utilities.CONFIGURATION['port']),
        user=utilities.CONFIGURATION['user'],
        passwd=utilities.CONFIGURATION['passwd']
    )
    rhiz = connection.rhizome

    # Get the first SID found in Keyring.
    # Recent versions of Serval do not have a SID by default, which has to be
    # handled. Therefore, check if we could get a SID.
    global my_sid
    my_sid = connection.first_identity
    if not my_sid:
        pfatal(
            'The server does not have a SID. Create a SID with' \
            '"servald keyring add" and restart Serval. Aborting.'
        )
        return

    args = set(args)
    # Find all servers which passes at least one filter
    server_list = client_find_server(rhiz, args)
    #print(server_list)
    parse_server_caps(server_list, args)

def parse_server_caps(server_list, args):
    ''' Parsing capabilities function.

    Args:
        server_list (dictionary -> server_id : capabilities): Found server capabilities
        args (list of strings): Filter arguments.
    '''
    desired_args_dict = {}
    desired_args = {}
    real_args_in = {}
    for arg in args:
        arg_s = arg.split("=")
        if "," in arg_s[1]:
            arg_s[1] = arg_s[1].replace(",", ".")
        desired_args_dict[arg_s[0]] = arg_s[1]

    # Goes through server capabilities and check them against the desired capabilities
    for server in server_list:
        args_in = {}
        real_args_in[server] = set()
        desired_args[server] = copy.deepcopy(desired_args_dict)
        # parse server capabilities
        for arg in server_list[server]:
            arg = arg.split("=")
            if "," in arg[1]:
                arg[1] = arg[1].replace(",", ".")
            args_in[arg[0]] = arg[1]

        for arg in args_in:
            if arg in desired_args_dict:
                # check capabilities
                if arg == "disk_space":
                    # check greater
                    if args_in[arg][-1] <= desired_args_dict[arg][-1] and float(args_in[arg][:-1]) >= float(desired_args_dict[arg][:-1]):
                        real_args_in[server].add(str(arg) + "="+args_in[arg])
                        del desired_args[server][arg]
                elif arg == "cpu_cores":
                    # check if total cores are greater
                    if int(args_in[arg]) >= int(desired_args_dict[arg]):
                        real_args_in[server].add(str(arg) + "=" + args_in[arg])
                        del desired_args[server][arg]
                elif arg == "cpu_load":
                    # check if less than given value
                    if float(args_in[arg]) >= float(desired_args_dict[arg]):
                        real_args_in[server].add(str(arg) + "=" + args_in[arg])
                        del desired_args[server][arg]
                elif arg == "power_state":
                    # check state is better than the given state
                    if args_in[arg] == "charging" or args_in[arg] == "fully-charged":
                        real_args_in[server].add(str(arg) + "=" +args_in[arg])
                        del desired_args_dict[server][arg]
                elif arg == "power_percentage":
                    # check if battery_power is greater // TODO or charging
                    if float(args_in[arg][:-1]) >= float(desired_args_dict[arg][:-1]):
                        real_args_in[server].add(str(arg) + "=" + args_in[arg])
                        del desired_args_dict[server][arg]
    for args in real_args_in:
        print("\nServer:" + str(args))
        for arg in real_args_in[args]:
            pinfo(arg)
        for arg in desired_args[args]:
            pfatal(arg)

def signal_handler(_, __):
    ''' Just a simple CTRL-C handler.
    '''
    global t
    t.cancel()
    utilities.pwarn('Stopping DTN-RPyC.')
    sys.exit(0)
