# Copyright 2017 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

def deprecation_warning(logger, message, tag, **kwargs):
    context = {
        "event_type": "deprecation_warning",
        "source": logger.name,
        "tag": tag,
    }
    for key in context:
        assert key not in kwargs
    context.update(kwargs)
    logger.warning("DEPRECATION WARNING: %s", message, extra=context)
