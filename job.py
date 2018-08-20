from enum import Enum

class Jobfile:
    ''' Jobfile representation
    A Jobfile is a list of jobs.
    Contains the client sid
    And a filter dictionary
    '''

    def __init__(self, client_sid=None, filter=None):
        self.client_sid = client_sid
        self.joblist = []
        if filter is None:
            self.filter = {}
        else:
            self.filter = filter

    def add_filter(self, key, value):
        self.filter[key] = value

    def add(self, server, procedure, args, status, counter):
        self.joblist.append(Job(server, procedure, args, status, counter))

    def list_print(self):
        print('Client_sid: %s\n' % (self.client_sid))

class Job:
    '''
    Jobs which have to be executed
    '''
    RESET = '\033[0m'
    DONE = '\033[1m\033[32m\033[0m\033[32m'   # Green
    OPEN = '\033[1m\033[33m\033[0m\033[33m'   # Yellow
    ERROR = '\033[1m\033[31\033[0m\033[31m' # Red

    def __init__(self, server, procedure, arguments, status, line, filter_dict=None):
        self.server = server
        self.procedure = procedure
        self.arguments = arguments
        self.line = line
        if status == 'OPEN':
            self.status = Status.OPEN
        elif status == 'DONE':
            self.status = Status.DONE
        else:
            status = Status.Error
        if filter_dict is None:
            self.filter_dict = {}
        else:
            self.filter_dict = filter_dict

    def get_filters(self):
        return self.filter_dict

    def add_filter(self, key, value):
        self.filter_dict[key] = value

    def job_print(self):
        print('Server: %s\nProcedure: %s\nArguments: %s' % (self.server, self.procedure, self.arguments))
        if self.status == Status.DONE:
            print('Status: ' + self.DONE + str(self.status) + self.RESET)
        elif self.status == Status.OPEN:
            print('Status: ' + self.OPEN + str(self.status) + self.RESET)
        else:
            print('Status: ' + self.ERROR + str(self.status) + self.RESET)
        print('Line: %s\nFilter: %s\n' % (self.line, self.filter_dict))

class Status(Enum):
    OPEN = 0
    DONE = 1
    ERROR = 2

class MalformedJobfileError(Exception):
    pass

class ServerNotFoundError(Exception):
    pass

class ServerNotOfferingProcedure(Exception):
    pass

class ArgumentMissmatchError(Exception):
    pass

class FileNotFound(Exception):
    pass
