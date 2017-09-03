import requests
import keyring, rhizome

class RestfulConnection:
    def __init__(self, host="localhost", port=4110, user="pyserval", passwd="pyserval"):
        self._AUTH = (user, passwd)
        self._BASE = "http://{}:{}".format(host, port)
        
        self.keyring = keyring.Keyring(self)
        self.rhizome = rhizome.Rhizome(self)
        self.first_identity = self.keyring.get_first_identity()

    def get(self, path, **params):
        request = requests.get(self._BASE+path, auth=self._AUTH, **params)
        request.raise_for_status()
        return request
    
    def post(self, path, **params):
        request = requests.post(self._BASE+path, auth=self._AUTH, **params)
        request.raise_for_status()
        return request
        
    def __repr__(self):
        return "RestfulConnection(\"{}, {}\")".format(self._BASE, self._AUTH)