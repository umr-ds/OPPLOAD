'''Module for maintaining a RESTful Connection.
'''
import requests
import keyring
import rhizome

class RestfulConnection(object):
    '''Class for maintaining a RESTful connection to Serval.
    Args:
        host (str):     The IP to the Serval host (default: localhost)
        port (int):     The port to the Serval host (default: 4110)
        user (str):     Username for the RESTful Server
        passwd (str):   Password fpr the RESTful Server
    '''
    def __init__(self, host="localhost", port=4110, user="pyserval", passwd="pyserval"):
        self._auth = (user, passwd)
        self._base = "http://{}:{}".format(host, port)

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
        request = requests.get(self._base+path, auth=self._auth, **params)
        request.raise_for_status()
        return request

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
        return "RestfulConnection(\"{}, {}\")".format(self._base, self._auth)
