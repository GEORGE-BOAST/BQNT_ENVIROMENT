# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

import collections
import re


VersionInfo = collections.namedtuple(
    'VersionInfo', ['version', 'schema_version', 'metadata_build_version']
)


def build_version_info_from_response(version_info_response):
    """Constructs and returns a VersionInfo namedtuple from a metadata version
    response from bqldsvc/bqhopper.
    """
    if version_info_response is None:
        return None

    version = parse_version(version_info_response['version'])
    schema_version = parse_version(
        version_info_response['responseSchemaVersion'])
    metadata_build_version = version_info_response.get('metadataBuildVersion')
    user_ftam_hash_version = version_info_response.get('userFtamHashVersion')

    if user_ftam_hash_version is not None:
        metadata_build_version = (f'{metadata_build_version}-'
                                  f'{user_ftam_hash_version}')
    return VersionInfo(version=version,
                       schema_version=schema_version,
                       metadata_build_version=metadata_build_version)


def parse_version(version_str):
    """Parse a version of the form "1.8-SNAPSHOT".

    Return a (major, minor, micro, final) tuple. If SNAPSHOT is present in the
    version, then `final` is 0, otherwise it is 1.
    """
    match = re.match(r'([0-9]+)\.([0-9]+)(\.([0-9]+))?(\.[0-9]+)*(-SNAPSHOT)?',
                     version_str)
    if not match:
        raise ValueError(f'"{version_str}" is not a valid version string')

    major = int(match.group(1))
    minor = int(match.group(2))

    if match.group(4):
        micro = int(match.group(4))
    else:
        micro = 0

    if match.group(6):
        final = 0
    else:
        final = 1

    return major, minor, micro, final


def parse_schema_version(version_str):
    """Parse a schema version of the form "1.3".

    Return a tuple (major, minor).
    """
    match = re.match(r'([0-9+])\.([0-9]+)(\.[0-9]+)*', version_str)
    if not match:
        raise ValueError(
            f'"{version_str}" is not a valid schema version string')
    return int(match.group(1)), int(match.group(2))


def is_version_more_recent(version1, version2):
    """Decide if a given metadata version is more recent than another version.

    Basically, return the result of the expression `version1 > version2`, with
    the following modification:

    If the (major, minor, micro) component of `version1` is different from
    `version2`, simply return the ordering as above. If they are equal,
    return ``True`` if the `final` flag for `version2` is not set.

    If one of the two versions is ``None``, return ``True``, effectively
    treating ``None`` as "unknown" and taking the conservative approach of
    always preferring a metadata update.

    This makes sure that as long as the versions are not final they keep getting
    updated even when the version number is the same.
    """
    if version1 is None or version2 is None:
        return True

    major1, minor1, micro1, final1 = version1
    major2, minor2, micro2, final2 = version2
    if (major1, minor1, micro1, final1) != (major2, minor2, micro2, final2):
        return ((major1, minor1, micro1, final1) >
                (major2, minor2, micro2, final2))

    # For now, consider all versions final. The reason is that the BQL metadata
    # service always reports non-final versions, even in production, and,
    # therefore, this prevents us from pulling new metadata every time.
    return False
