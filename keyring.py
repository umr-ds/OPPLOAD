''' Module for Keyring and Identity management in Serval
'''

class ServalIdentity(object):
    ''' Identity management class.

    Args:
        _keyring (Keyring): A Keyring connection.
        sid (str):          The SID to be set.
        did (str):          A optional DID.
        name (str):         A optional human-readabel name for the identity.
        identity (str):     Identity idetifyier.
    '''
    def __init__(self, _keyring, sid, did='', name='', identity=''):
        self.sid = sid
        self.__dict__['did'] = did
        self.__dict__['name'] = name
        self.identity = identity
        self._keyring = _keyring

    def __repr__(self):
        ''' Returns a print ready Identity string.
        '''
        return 'ServalIdentity(sid=%s, did=\'%s\', name=\'%s\')' % (
            self.sid, self.did, self.name
        )

    def __str__(self):
        ''' Returns an abbreviated Identity.
        If a name is set the name onny will be returned.
        If not but a did is set, the did will be returned.
        Otherwise the first 16 hex-chars of the SID will be returned.
        '''
        if self.name:
            return self.name

        if self.did:
            return 'did:%s' % str(self.did)

        else:
            return '%s*' % self.sid[:16]

    def refresh(self):
        ''' Refreshed the identities from Serval Keyring.
        '''
        identities = self._keyring.get_identities()
        for ident in identities:
            if ident.sid == self.sid:
                self.__dict__.update(ident.__dict__)

    # GET /restful/keyring/SID/remove
    def remove(self):
        '''Removes an Identity from the Keyring and the ServalIdentity instance.
        Returns a new ServalIdentity intance without the removed identity.
        '''
        request_json = self._keyring._connection.get(
            '/restful/keyring/%s/remove' % self.sid
        ).json()

        return ServalIdentity(self._keyring, **request_json['identity'])

    # GET /restful/keyring/SID/set
    def set(self, did=None, name=None):
        '''Sets DID and Name for a given SID.
        If a parameter is not given, the ServalIdentity
        will not be changed for the respective value.
        Serval will remove already set names when updating did and vice-versa.
        So the name/did needs to be sent with the change request

        Args:
            did (str):  The DID to be set.
            name (str): The name to be set.

        Returns:
            ServalIdentity: The ServalIdentity with the newly set DID and name.
        '''

        params = {'did':self.did, 'name':self.name}

        if did:
            params['did'] = did
        if name:
            params['name'] = name

        request_json = self._keyring._connection.get(
            '/restful/keyring/%s/set' % self.sid, params=params
        ).json()
        self.__dict__.update(request_json['identity'])

        return self

class Keyring(object):
    ''' Keyring RESTful connection.
    Args:
        connection (restful): The initialized RESTful connection to Serval.
    '''
    def __init__(self, connection):
        self._connection = connection

    # GET /restful/keyring/identities.json
    def get_identities(self):
        ''' Function for gathering all identities in the Keyring.
        Returns:
            ServalIdentitity: A list of all gathered Serval Identities from Keyring.
        '''
        identities_json = self._connection.get('/restful/keyring/identities.json').json()
        identities = []

        for row in identities_json['rows']:
            identities.append(ServalIdentity(self, **dict(zip(identities_json['header'], row))))

        return identities

    def get_first_identity(self):
        '''Function to get the first (main) identitie from the Keyring.
        Returns:
            A ServalIdentity, if any. Otherwise, None.
        '''
        try:
            return self.get_identities()[0]
        except IndexError:
            return None

    # GET /restful/keyring/add
    def add(self):
        '''Add a new identity to the Keyring.
        Returns:
            ServalIdentity: The newly to the Keyring added identity.
        '''
        request_json = self._connection.get('/restful/keyring/add').json()
        return ServalIdentity(self, **request_json['identity'])
