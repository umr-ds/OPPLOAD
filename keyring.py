import restful

class ServalIdentity:
    def __init__(self, _keyring, sid, did="", name="", identity=""):
        self.sid = sid
        self.__dict__["did"] = did
        self.__dict__["name"] = name
        self.identity = identity
        self._keyring = _keyring
        
    def __repr__(self):
        return "ServalIdentity(sid={}, did=\"{}\", name=\"{}\")".format(self.sid, self.did, self.name)

    def __str__(self):
        if self.name: return self.name
        if self.did: return "did:"+str(self.did)
        else: return "{}*".format(self.sid[:16])

    def __setattr__(self, name, value):
            if name == "did":
                self.refresh()
                self.set(did=value)
                
            elif name == 'name':
                self.refresh()
                self.set(name=value)
                
            else:
                self.__dict__[name] = value

    def refresh(self):
        identities = self._keyring.get_identities()
        for ident in identities:
            if ident.sid == self.sid:
                self.__dict__.update(ident.__dict__)

    # GET /restful/keyring/SID/remove
    def remove(self):
        request_json = self._keyring._connection.get("/restful/keyring/{}/remove".format(self.sid)).json()
        return ServalIdentity(self._keyring, **request_json["identity"])

    # GET /restful/keyring/SID/set
    def set(self, did=None, name=None):
        # serval will remove already set names when updating did and vice-versa. So the name/did needs to be sent with the change request
        params = {"did":self.did, "name":self.name}
        if did: params["did"] = did
        if name: params["name"] = name
        request_json = self._keyring._connection.get("/restful/keyring/{}/set".format(self.sid), params=params).json()
        self.__dict__.update(request_json["identity"])
        return self
    
class Keyring:
    def __init__(self, connection):
        self._connection = connection
        
    # GET /restful/keyring/identities.json
    def get_identities(self):
        identities_json = self._connection.get("/restful/keyring/identities.json").json()
        identities = []
        for row in identities_json["rows"]:
            identities.append(ServalIdentity(self, **dict(zip(identities_json["header"], row))))
        return identities
        
    def get_first_identity(self):
        try:
            return self.get_identities()[0]
        except IndexError:
            return None

    # GET /restful/keyring/add
    def add(self):
        request_json = self._connection.get("/restful/keyring/add").json()
        return ServalIdentity(self, **request_json["identity"])