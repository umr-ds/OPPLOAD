import urllib2, base64, json

url_bundlelist = 'http://localhost:4110/restful/rhizome/bundlelist.json'
credentials = base64.b64encode('RPC:SRPC')

def server_get_bundlelist():
    bundlelist_request = urllib2.Request(url_bundlelist)
    bundlelist_request.add_header("Authorization", "Basic %s" % credentials)
    bundlelist = urllib2.urlopen(bundlelist_request)
    
    return json.loads(bundlelist.read())

def server_get_service(bundlelist_rows):
    return [row[-1] for row in bundlelist_rows]

def server_listen_rhizome():
    print server_get_service(server_get_bundlelist()['rows'])