# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import os
import json

import six

from .data_frame import data_frame

default_settings = {
    'format': data_frame,
    'addresses': [('localhost', 8194)],
    'numStartAttempts': 1,
    # default input and output format for datetime objects, if not explicitly set
    'time_zone': 'UTC',

    # One of None, 'user', 'application', 'directory_service', 'user_and_application'
    'auth_type': None,
    # If auth_type is set to 'application' or 'user_and_application', specify the application name,
    # or if auth_Type is 'directory_service', specifies the directory name
    'auth_name': None,
    'authorization': None,
    'tls_options': None,
}


class Settings:

    """Session settings.

    This class represents the configurable settings for a :class:`Session`.
    It is constructed with a dictionary with settings options, and default
    values are used for options that are not specified in the dictionary.

    Note that this class itself is rarely needed, since the settings
    dictionary can be passed directly to the :class:`Session` constructor.

    The following settings can be set in the dictionary and queried as
    attributes:

    Attributes
    ----------
        addresses : list (default: ``[('localhost', 8194)``)
            List of hostname/port tuples to connect to.
        numStartAttempts : int (default: ``1``)
            Number of session start attempts.
        auth_type : str (default: ``None``)
            The authentication type, one of  ``'user'``, ``'application'``,
            ``'directory_service'``, ``'user_and_applicatation'``, or `None` for
            no authentication.
        auth_name : str (default: ``None``)
            Specifies the application name if `auth_type` is set to either
            ``'application'`` or ``'user_and_application'``.
        format : callable (default: `dictionary`)
            Specifies in which data structure to return requested data. See
            :ref:`output-formatters` for more information on output formatters.
        time_zone : str, :class:`datetime.tzinfo` (default: ``'UTC'``)
            The time zone in which to interpret all times and datetimes that
            are not timezone-aware, and in which to return all time and datetime
            data.
        authorization : dict (default: ``None``)
            Authorization request for default identity to issue upon session startup.
        tls_options : dict or blpapi.sessionoptions.TlsOptions (default: ``None``)
            Specifies parameters necessary to create `TlsOptions`.

              * If a dict is passed, it must contain three keys:
                - `client_cred_path`: path to DER encoded client credentials in
                                      PKCS#12 format
                - `client_cred_pass`: str password to access client credentials
                - `trusted_certs_path`: path to DER encoded trust material in
                                        PKCS#7 format

              * If a blpapi.sessionoptions.TlsOptions is passed, it will
              simply be set into the blpapi Session object.

    .. rubric:: Methods
    .. automethod:: Settings.__init__
    """

    def __init__(self, **settings):
        """Initializes the object from key-value pairs.

        Parameters
        ----------
        settings : dict
            See the class description for what options are available.
        """
        if 'BQAPI_CONFIG_DIR' in os.environ:
            config_file = os.path.join(
                os.environ['BQAPI_CONFIG_DIR'], '.bqapirc')
        else:
            config_file = os.path.expanduser('~/.bqapirc')
        if os.path.isfile(config_file) and os.path.getsize(config_file):
            with open(config_file) as f:
                user_settings = json.load(f)
        else:
            user_settings = {}
        self._settings = dict(default_settings, **user_settings)
        self._settings.update(**settings)

        # backwards compatibility:
        if 'hostname' in self._settings:
            self._settings['addresses'] = [(self._settings.pop('hostname'),
                                            self._settings.pop('port', 8194))]

    def __getattr__(self, key):
        try:
            return self._settings[key]
        except KeyError:
            raise AttributeError(key)

    def __getstate__(self):
        return (self._settings,)

    def __setstate__(self, state):
        settings_dict, = state
        self._settings = settings_dict

    def __repr__(self):
        return self._settings.__repr__()

    def _get_auth_string(self):
        auth_type = self.auth_type
        if auth_type is None:
            return None

        auth_dict = {}
        if auth_type == 'user':
            auth_dict['AuthenticationType'] = 'OS_LOGON'
        elif auth_type == 'directory_service':
            auth_dict['AuthenticationType'] = 'DIRECTORY_SERVICE'
            auth_dict['DirSvcPropertyName'] = self.auth_name
        elif auth_type == 'application':
            auth_dict['AuthenticationMode'] = 'APPLICATION_ONLY'
            auth_dict['ApplicationAuthenticationType'] = 'APPNAME_AND_KEY'
            auth_dict['ApplicationName'] = self.auth_name
        elif auth_type == 'user_and_application':
            auth_dict['AuthenticationMode'] = 'USER_AND_APPLICATION'
            auth_dict['AuthenticationType'] = 'OS_LOGON'
            auth_dict['ApplicationAuthenticationType'] = 'APPNAME_AND_KEY'
            auth_dict['ApplicationName'] = self.auth_name
        else:
            raise ValueError(
                '"{}" is not a valid authentication type'.format(auth_type))

        return ';'.join(['='.join((key, value)) for key, value in six.iteritems(auth_dict)])
