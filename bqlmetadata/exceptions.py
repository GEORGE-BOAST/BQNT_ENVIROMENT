# Copyright 2020 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.


class MetadataError(Exception):
    pass


class ServiceError(Exception):
    pass


class StaleMetadataError(Exception):
    pass
