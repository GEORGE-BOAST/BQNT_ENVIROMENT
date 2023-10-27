# Copyright 2021 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import re
from .response_error import ResponseError, UnexpectedResponseError

_VERSION_REGEX = re.compile(r'^(\d+)\.(\d+)(\.\d+)*$')

def _check_response(response, expected_major_version, execution_context):
    __check_exceptions(response, execution_context)
    __check_version(response, expected_major_version)


def __check_version(response, expected_major_version):
    version_info = response.get('versionInfo', {})
    response_schema_version = version_info.get('responseSchemaVersion')
    if response_schema_version is None:
        raise UnexpectedResponseError('No version information in response')
    major_version, _ = __parse_schema_version(response_schema_version)
    if major_version != expected_major_version:
        raise UnexpectedResponseError(
            'Expected response from schema with major version '
            f'{expected_major_version}, got major version '
            f'{response_schema_version} instead')


def __check_exceptions(response, execution_context):
    response_exceptions = response.get('responseExceptions')

    
    if response_exceptions:
        raise ResponseError(response_exceptions, execution_context)


def __parse_schema_version(version_str):
    """Parse a schema version of the form "1.3".

    Return a tuple (major, minor).
    """
    match = _VERSION_REGEX.match(version_str)
    if not match:
        raise ValueError(
            f'"{version_str}" is not a valid schema version string'
        )
    return int(match.group(1)), int(match.group(2))
