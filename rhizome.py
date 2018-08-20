'''Module for maintaining for Rhizome Bundles.
'''
import utilities as util
import json
import os
class Bundle(object):
    '''Clss for Rhizome Bundles.
    Args:
        entries (dict): A dictionary for the attributes.
    '''
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def update_with_manifest(self, manifest):
        '''Update the current bundle with the fields from manifest
        Args:
            manifest (str): The manifest which fields for update.
        '''
        pure_manifest = manifest.split('\0')[0]
        manifest_parsed = [row.split('=') for row in pure_manifest.split('\n')][:-1]
        self.__dict__.update(manifest_parsed)

    def __repr__(self):
        return 'Bundle(id=%s, name=\'%s\')' % (self.id, self.name)

    def __str__(self):
        if hasattr(self, 'service'):
            prefix = self.service
        else:
            prefix = ''
        if hasattr(self, 'name') and self.name:
            return '%s:%s' % (prefix, self.name)

        return '%s:%s*' % (prefix, self.id[:16])

class Rhizome(object):
    '''The Rhizome connection to Serval.
    Args:
        _connection (Restful): RESTful connection to Serval.
    '''
    def __init__(self, _connection):
        self._connection = _connection

    def fix_closures(self, bundlestring):
        brackets = 0
        block = 0
        for x in bundlestring:
            if x == "{":
                brackets += 1
            elif x == "}":
                brackets -= 1
            if x == "[":
                block += 1
            if x == "]":
                block -= 1
        while block > 0:
            bundlestring += "]"
            block -= 1
        while brackets > 0:
            bundlestring += "}"
            brackets -= 1

        return bundlestring


    # GET /restful/rhizome/bundlelist.json
    def get_bundlelist(self, token=None):
        '''Get the bundle list from Rhizome store
        Args:
            token (str): The token for newsince request.
        Returns:
            Bundle list: A list of Bundles from the Rhizome store.
        '''
        bundlelist = None
        if token:
            # FIXME loads forever
            bundlelist = self._connection.get(
                '/restful/rhizome/newsince/%s/bundlelist.json' % token
            )
            bundlelist = self.fix_closures(bundlelist)
            if len(bundlelist) == 0:
                util.pfatal('Serval crashed')
            bundlelist = json.loads(bundlelist)
        else:
            bundlelist = self._connection.get('/restful/rhizome/bundlelist.json')
            bundlelist = self.fix_closures(bundlelist)
            bundlelist = json.loads(bundlelist)
        bundlelist_dict = [dict(list(zip(bundlelist['header'], interest))) \
            for interest in bundlelist['rows']]
        return [Bundle(**bundle) for bundle in bundlelist_dict]

    # GET /restful/rhizome/BID.rhm
    def get_manifest(self, bid):
        '''Get a bundle from Rhizome store for a given bid
        Args:
            bid (str): The bid for the manifest
        Returns:
            Bundle: The downloaded bundle.
        '''
        manifest = self._connection.get('/restful/rhizome/%s.rhm' % bid)
        true_manifest = ""
        for char in manifest:
            if char == 0:
                break
            true_manifest += chr(char)

        #print(true_manifest)
        #manifest_list = manifest.split('\x00')
        manifest_parts = true_manifest.split('\n')
        #manifest_parts = manifest_list#list(filter(None, manifest_list[0].split('\n')))
        #print(manifest_parts)
        if '' in manifest_parts:
            manifest_parts.remove('')
        manifest_dict = dict([(part.split('=')[0], part.split('=')[1]) for part in manifest_parts])
        return Bundle(**manifest_dict)

    # GET /restful/rhizome/BID/decrypted.bin
    def get_decrypted(self, bid):
        '''Get the decrypted payload from Rhizome store for a given bid
        Args:
            bid (str): The bid for the payload
        Returns:
            Bundle: The downloaded payload.
        '''
        #decrypted = self._connection.get('/restful/rhizome/%s/decrypted.bin' % bid).text
        decrypted = self._connection.get('/restful/rhizome/%s/decrypted.bin' % bid).decode('utf-8')
        return decrypted

    def get_decrypted_to_file(self, bid, path):
        '''Get the decrypted payload from Rhizome store for a given bid and write it to a file
        Args:
            bid (str):  The bid for the payload
            path (str): The path where the content will be written to.
        '''
        decrypted = self._connection.get(
            '/restful/rhizome/%s/decrypted.bin' % bid, stream=True
        )

        with open(path, 'wb') as handle:
            handle.write(decrypted)
            #for block in decrypted.iter_content(1024 * 1024):
                #handle.write(block)

    # POST /restful/rhizome/insert
    def insert(self, bundle, payload, sid=None, bid=None):
        '''Insert a new bundle to the Rhizome store.
        Args:
            bundle (Bundle):    The bundle which will be inserted.
            payload (str):      Payload of the file to be inserted. Can be a string or a file path.
            sid (str):          Author SID. If not given, the first SID will be chosen.
            bid (str):          A BID if the bundle should be updated.

        Returns:
            Raw string of the inserted Bundle.
        '''
        if not sid:
            sid = self._connection.first_identity.sid

        manifest_file = '\n'.join(['%s=%s' % (x[0], x[1]) \
            for x in bundle.__dict__.items()]) + '\n'

        multipart = [('bundle-author', sid)]

        if bid:
            multipart.append(('bundle-id', bid))
        multipart.append(('manifest', ('manifest1', manifest_file, \
            'rhizome/manifest;format="text+binarysig"')))
        multipart.append(('payload', ('file1', payload)))
        #print(multipart)
        manifest_request = self._connection.post('/restful/rhizome/insert', files=multipart)
        bundle.update_with_manifest(manifest_request.text)
        return manifest_request.text
