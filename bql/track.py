# Copyright 2018 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.
from __future__ import absolute_import
import logging
from ._version import __version__

_LOGGER = logging.getLogger(__name__)


def _log():
    try:
        import bqevents, bqapi
    except ImportError:
        _LOGGER.info("Failed to import bqevents, bqapi", exc_info=True)
    else:
        import os, sys, json
        try:
            s = bqapi.get_session_singleton()
            payload = {
                "event_type": "imptrk.pybql",
                "file": os.path.realpath(__file__),
                "bqevents_file": os.path.realpath(bqevents.__file__),
                "executable": sys.executable,
                "current_working_dir": os.getcwd(),
                "version": __version__
            }

            bqevent_raw = bqevents.make_event(globals(), payload)

            data = [
                {"name": k, "value": v}
                for k,v in bqevent_raw['payload'].items()
            ]

            event = {
                "digest": bqevent_raw["digest"],
                "salt": bqevent_raw["salt"],
                "data": data
            }

            event_request = {"eventRequest": {"events": [event]}}

            request_data = {
                "requestData": {
                    "genericServiceRequest": {
                        "service": "bqtrksvc",
                        "version": {"major": 1, "minor": 0},
                        "request": json.dumps(event_request),
                    }
                }
            }

            s.send_request("//blp/bqgwsvc",
                           "GwRequest",
                           request_data,
                           format=None)

        except Exception as ex:
            _LOGGER.info("Failed to send", exc_info=True)


def _usage():
    import os, sys
    if os.environ.get('BIPYDIR') is None:
        msg = (
            "BQuant libraries are not currently supported outside of the "
            "BQuant environment. Please contact your Bloomberg BQuant "
            "representative for more information."
        )
        _LOGGER.critical(msg)
        raise ImportError(msg)

# this order matters
_log()
_usage()
