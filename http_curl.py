import subprocess
import sys


def parse_stuff(token):
    base = "http://localhost:4110"
    #path = "/restful/rhizome/bundlelist.json"
    #path = "/restful/rhizome/newsince/NoonXxIdRqmZhIvSJkpjLhkAAAAAAAAA/bundlelist.json"
    path = "http://localhost:4110/restful/rhizome/newsince/" + token + "/bundlelist.json"
    auth = "pum:pum123"
    command = "curl -H 'Expect:' --silent --basic --user " + auth + " " + path
    print(command)

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    brackets = 0
    closure = 0
    while True:
        output = process.stdout.read(1)
        # check if bracket is closed
        if output == '' and process.poll() is not None:
            break
        if output == '}' and process.poll() is not None:
            sys.stdout.write(output)
            sys.stdout.flush()
            print('Parsed until the EOF')
            break
        if output == ']':
            # check if there is more
            # maybe start a timer
            """
            while closure > 0:
                print('Add closing ]')
                sys.stdout.write(']')
                closure = closure -1
            """
            while brackets > 0:
                print('Add closing brackets')
                sys.stdout.write('}')
                sys.stdout.flush()
                brackets = brackets -1
        sys.stdout.write(output)
        sys.stdout.flush()
        if output == '{':
            closure = brackets + 1
        if output == '}':
            closure = brackets - 1
        if output == '[':
            closure = brackets + 1
        if output == ']':
            closure = brackets - 1
    output = process.communicate()
    #process.terminate()
    print(brackets)
    rc = process.poll()

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) == 1:
        print('Insert Token!')
        sys.exit()
    elif len(sys.argv) > 2:
        print('Inserted too many Tokens!')
        sys.exit()
    print(sys.argv[1])
    parse_stuff(sys.argv[1])
