import restful

class Bundle:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    @classmethod
    def from_manifest(cls, manifest):
        bundle.update_with_manifest(manifest)
        return bundle

    def update_with_manifest(self, manifest):
        pure_manifest = manifest.split("\0")[0]
        manifest_parsed = [row.split("=") for row in pure_manifest.split("\n")][:-1]
        self.__dict__.update(manifest_parsed)

    def __repr__(self):
        return "Bundle(id={}, name=\"{}\")".format(self.id, self.name)

    def __str__(self):
        if hasattr(self, "service"): prefix = self.service
        else: prefix = ""
        if hasattr(self, 'name') and self.name:
            return "{}:{}".format(prefix, self.name)

        return "{}:{}*".format(prefix, self.id[:16])


class Rhizome:
    def __init__(self, _connection):
        self._connection = _connection

    # GET /restful/rhizome/bundlelist.json
    def get_bundlelist(self, token=None):
        bundlelist = None
        if token:
            bundlelist = self._connection.get("/restful/rhizome/newsince/{}/bundlelist.json".format(token)).json()
        else:
            bundlelist = self._connection.get("/restful/rhizome/bundlelist.json").json()
        bundlelist_dict = [dict(list(zip(bundlelist["header"], interest))) for interest in bundlelist["rows"]]
        return [Bundle(**bundle) for bundle in bundlelist_dict]

    # GET /restful/rhizome/BID.rhm
    def get_manifest(self, bid):
        manifest = self._connection.get("/restful/rhizome/{}.rhm".format(bid)).text
        manifest_list = manifest.split('\0')
        manifest_parts = list(filter(None, manifest_list[0].split('\n')))
        manifest_dict = dict([(part.split('=')[0], part.split('=')[1])for part in manifest_parts])
        return Bundle(**manifest_dict)

    # GET /restful/rhizome/BID/raw.bin
    def get_raw(self, bid):
        raw = self._connection.get("/restful/rhizome/{}/raw.bin".format(bid)).text
        return raw

    # GET /restful/rhizome/BID/decrypted.bin
    def get_decrypted(self, bid):
        decrypted = self._connection.get("/restful/rhizome/{}/decrypted.bin".format(bid)).text
        return decrypted

    def get_decrypted_to_file(self, bid, path):
        decrypted = self._connection.get("/restful/rhizome/{}/decrypted.bin".format(bid), stream=True)
        
        with open(path, 'wb') as handle:
            for block in decrypted.iter_content(1024 * 1024):
                handle.write(block)

    # POST /restful/rhizome/insert
    def insert(self, bundle, payload, sid=None, bid=None):
        if not sid: sid = self._connection.first_identity.sid

        manifest_file = "\n".join(["{}={}".format(x[0],x[1]) for x in bundle.__dict__.items()])+"\n"

        multipart = [("bundle-author", sid)]
        if bid: multipart.append(("bundle-id", bid))
        multipart.append(("manifest", ("manifest1", manifest_file, "rhizome/manifest;format=\"text+binarysig\"")))
        multipart.append(("payload", ("file1", payload)))

        manifest_request = self._connection.post("/restful/rhizome/insert", files=multipart)
        bundle.update_with_manifest(manifest_request.text)
        return manifest_request.text