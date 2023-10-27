#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.


class Range(object):
    """
    This represents the range() specification in BQL.
    """
    def __init__(self, start, end):
        # When constructed by the user user, "start" and "end" have yet to
        # be validated.
        self.start = start
        self.end = end
