# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from ..service import Service


__service = None


def _get_service(service):
    # Provides a default service to all convenience functions.

    # This caches a single service between calls to convenience functions.
    global __service
    
    if service is not None:
        return service

    if __service is None:
        __service = Service()
    return __service
