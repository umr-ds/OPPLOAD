'''Module for maintaining a RESTful Connection.
'''
import time
import math
import sys
import subprocess
import requests
import keyring
import rhizome
import utilities

class RestfulConnection(object):
    '''Class for maintaining a RESTful connection to Serval.
    Args:
        host (str):     The IP to the Serval host (default: localhost)
        port (int):     The port to the Serval host (default: 4110)
        user (str):     Username for the RESTful Server
        passwd (str):   Password fpr the RESTful Server
    '''
    def __init__(self, host='localhost', port=4110, user='pyserval', passwd='pyserval'):
        self._auth = (user, passwd)
        self._base = 'http://%s:%s' % (host, port)

        self.keyring = keyring.Keyring(self)
        self.rhizome = rhizome.Rhizome(self)
        self.first_identity = self.keyring.get_first_identity()

    def get(self, path, **params):
        '''GET request.
        Args:
            path (str):     URL path for the GET.
            params (dict):  Dictionary with parameters for the Request.

        Returns:
            The Response object.
        '''
        # FIXME
        # FIXME
        # FIXME
        DEBUG_FAST = False
        # FIXME
        # FIXME
        # FIXME
        auth = self._auth[0]+":"+self._auth[1]
        #print(self._base + path)
        command = "stdbuf -o0 curl -N -H 'Expect:' --silent --basic --user " + auth + " "  + self._base + path
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, bufsize=-1)

        brackets = 0
        closure = 0
        block = ''
        out = b''
        BIN = True
        NEW = False
        HEADER = False
        header = ''
        # obsolete

        result = ''
        if path.endswith('.bin'):
            out = b''
            # read to end if bin
            output = process.stdout.read(-1)
            return output
        elif not DEBUG_FAST or not 'newsince' in path:
            out = b''
            output = process.stdout.read(-1)
            if 'json' in path:
                return output.decode('utf-8')
            return output
        else:
            out = b''
        if 'newsince' in path:
            NEW = True

        # experimental code below. Remove 'elif True' above

        while True:
            output = process.stdout.read(1)
            if NEW:
                sys.stdout.flush()

            if BIN and type(output) is str:
                BIN = False
                out = out.decode('utf8')
            sys.stdout.flush()
            out += output
            if not HEADER:
                if 'json' in path and 'header' in out.decode('utf-8') and 'rows":[' in out.decode('utf-8'):
                    header = out
                    result += header.decode('utf-8')
                    HEADER = True
            # check if bracket is closed
            if output == '' and process.poll() is not None:
                break
            if output == '}' or output == b'}' or output == b'': # and process.poll() is not None:
                # TODO check if process is still running
                output = process.stdout.read(1)
                if output != b'' and process.poll() is None:
                    out += output
                    continue
                break
            if output == b'{':
                closure = brackets + 1
            elif output == b'}':
                closure = brackets - 1
            elif output == b'[':
                # start a new block
                block = '['
                closure = brackets + 1
            elif output == b']':
                block += output.decode('utf-8')
                closure = brackets - 1
                # check block
                if "RPC_OFFER" not in block and "RPC" in block and 'newsince' in path:
                    out = header.decode('utf-8') + block
                    result += block
                    output = process.stdout.read(1)
                    if output == '' and process.poll() is not None:
                        break
                    result += output.decode('utf-8')
                    break;
            elif 'json' in path:
                block += output.decode('utf-8')
        # TODO check status
        if "json" in path and type(out) is bytes:
            out = out.decode('utf-8')
        return out

    def post(self, path, **params):
        '''POST request.
        Args:
            path (str):     URL path for the POST.
            params (dict):  Dictionary with parameters for the Request.

        Returns:
            The Response object.
        '''
        request = requests.post(self._base+path, auth=self._auth, **params)
        request.raise_for_status()
        return request

    def __repr__(self):
        return 'RestfulConnection(\'%s, %s\')' % (self._base, self._auth)
