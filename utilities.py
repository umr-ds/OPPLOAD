import base64, json
import rhizome

RESET = '\033[0m'
FATAL = '\033[1m\033[31mFATAL: \033[0m\033[31m' # Red
INFO = '\033[1m\033[32mINFO: \033[0m\033[32m'   # Green
WARN = '\033[1m\033[33mWARN: \033[0m\033[33m'   # Yellow
DEBUG = '\033[1m\033[34mDEBUG: \033[0m\033[34m' # Blue

def pdebug(string_to_print):
    print(DEBUG + str(string_to_print) + RESET)

def pfatal(string_to_print):
    print(FATAL + str(string_to_print) + RESET)

def pinfo(string_to_print):
    print(INFO + str(string_to_print) + RESET)

def pwarn(string_to_print):
    print(WARN + str(string_to_print) + RESET)


CONFIGURATION = {}

def read_config():
    global CONFIGURATION
    with open('rpc.conf', 'r') as rpc_conf:
        for conf in rpc_conf:
            conf_list = conf.split('=')
            CONFIGURATION[conf_list[0].rstrip()] = conf_list[1].rstrip()

def make_bundle(manifest_props):
    bundle = rhizome.Bundle()
    for prop in manifest_props:
        bundle.__dict__[prop[0]] = prop[1]

    return bundle
