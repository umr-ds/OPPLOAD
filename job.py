#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This module contains all required objects to represent jobs.
'''

from enum import Enum


class Jobfile:
    '''Object representing a job file.
    '''

    def __init__(self, client_sid=None, filter={}):
        '''Init the job file object

        Keyword Arguments:
            client_sid -- SID of the client, i.e. the originator
            of the call. (default: {None})
            filter -- Global filters to check all servers (default: {{}})
        '''

        self.client_sid = client_sid
        self.joblist = []
        self.filter = filter

    def add_filter(self, key, value):
        '''Add a filter to the global filters

        Arguments:
            key -- Key of the filter (e.g. disk_space)
            value -- value if the filter
        '''

        self.filter[key] = value

    def add(self, server, procedure, args, status, counter, filter_dict={}):
        '''Add a Job to the joblist of the job file

        Arguments:
            server -- Server address (SID)
            procedure -- Name of the job
            args -- Arguments of the job
            status -- State of the job (e.g. DONE)
            counter -- Line counter of the job

        Keyword Arguments:
            filter_dict -- Optional filters for this job (default: {{}})
        '''

        self.joblist.append(
            Job(server, procedure, args, status, counter, filter_dict))


class Job:
    '''Class representing a job
    '''

    def __init__(self,
                 server=None,
                 procedure=None,
                 arguments=None,
                 status=None,
                 line=None,
                 filter_dict={}):
        '''Init the job object

        Keyword Arguments:
            server -- The server address (default: {None})
            procedure -- Name of the job (default: {None})
            arguments -- Arguments of a job (default: {None})
            status -- State of the job (e.g. DONE) (default: {None})
            line -- The line of the job in the file (default: {None})
            filter_dict -- Filters for this job (default: {{}})
        '''

        self.server = server
        self.procedure = procedure
        self.arguments = list(map(lambda x: x.strip(), arguments))
        self.line = line
        self.filter_dict = filter_dict
        if status == 'OPEN':
            self.status = Status.OPEN
        elif status == 'DONE':
            self.status = Status.DONE
        else:
            status = Status.ERROR

    def __str__(self):
        return '{} {}'.format(self.procedure, ' '.join(self.arguments))


class Status(Enum):
    '''Simple Enum representing the state of a job
    '''

    OPEN = 0
    DONE = 1
    ERROR = 2
