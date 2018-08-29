#!/usr/bin/env python3

# -*- coding: utf-8 -*-

from enum import Enum

class Jobfile:
    ''' Jobfile representation
    A Jobfile is a list of jobs.
    Contains the client sid
    And a filter dictionary
    '''

    def __init__(self, client_sid=None, filter={}):
        self.client_sid = client_sid
        self.joblist = []
        self.filter = filter

    def add_filter(self, key, value):
        self.filter[key] = value

    def add(self, server, procedure, args, status, counter, filter_dict={}):
        self.joblist.append(Job(server, procedure, args, status, counter))

    def list_print(self):
        print('Client_sid: %s\n' % (self.client_sid))

class Job:
    '''
    Jobs which have to be executed
    '''
    def __init__(self,
                 server=None,
                 procedure=None,
                 arguments=None,
                 status=None,
                 line=None,
                 filter_dict={}):
        self.server = server
        self.procedure = procedure
        self.arguments = arguments
        self.line = line
        self.filter_dict = filter_dict
        if status == 'OPEN':
            self.status = Status.OPEN
        elif status == 'DONE':
            self.status = Status.DONE
        else:
            status = Status.ERROR

        self.arguments = list(map(lambda x: x.strip(), self.arguments))

    def get_filters(self):
        return self.filter_dict

    def add_filter(self, key, value):
        self.filter_dict[key] = value

    def __str__(self):
        return '%s %s' % (self.procedure, ' '.join(self.arguments))


class Status(Enum):
    OPEN = 0
    DONE = 1
    ERROR = 2
