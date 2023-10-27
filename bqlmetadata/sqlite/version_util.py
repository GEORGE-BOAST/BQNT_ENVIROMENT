#!/usr/bin/env python

# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.


def encode_version(major, minor, micro, final):
    """Encode a version triplet in a single number.

    This number can be used for ordering purposes.
    """
    assert final >= 0 and final <= 1
    assert micro >= 0 and micro <= 0xfff
    assert minor >= 0 and minor <= 0xfff
    assert major >= 0 and major <= 0x7f
    return (major << 25) + (minor << 13) + (micro << 1) + final


def decode_version(v):
    """Decode a version encoded with encode_version."""
    return (v >> 25), ((v >> 13) & 0xfff), ((v >> 1) & 0xfff), (v & 0x1)
