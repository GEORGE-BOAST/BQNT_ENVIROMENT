# Copyright 2016 Bloomberg Finance L.P.
# All Rights Reserved.
# This software is proprietary to Bloomberg Finance L.P. and is
# provided solely under the terms of the BFLP license agreement.

from __future__ import absolute_import

import datetime
import dateutil
import dateutil.tz

from .error import RequestError


# This table maps the TZDF timezone index to the name as it appears in TZDF,
# the entry in the Olson database, and the Windows timezone ID and name.
# Windows timezones taken from
#   https://msdn.microsoft.com/en-us/library/ms912391(v=winembedded.11).aspx
#   http://unicode.org/repos/cldr/trunk/common/supplemental/windowsZones.xml
_TZDF_INDEX = {
    11: ('Obsolete Indiana Time',                 'US/East-Indiana',                (40, 'US Eastern Standard Time')),
    1: ('Dateline Daylight Time',                'Pacific/Kwajalein',              (0, 'Dateline Standard Time')),
    2: ('Samoa Daylight Time',                   'Pacific/Midway',                 (1, 'Samoa Standard Time')),
    3: ('Hawaiian Daylight Time',                'Pacific/Honolulu',               (2, 'Hawaiian Standard Time')),
    4: ('Alaskan Daylight Time',                 'America/Anchorage',              (3, 'Alaskan Standard Time')),
    5: ('Pacific Daylight Time',                 'America/Los_Angeles',            (4, 'Pacific Standard Time')),
    6: ('Mountain Daylight Time',                'America/Denver',                 (10, 'Mountain Standard Time')),
    7: ('US Mountain Daylight Time',             'America/Phoenix',                (15, 'US Mountain Standard Time')),
    8: ('Mexico Daylight Time',                  'America/Mexico_City',            (30, 'Central Standard Time (Mexico)')),
    9: ('Central Daylight Time',                 'America/Chicago',                (20, 'Central Standard Time')),
    10: ('Canadian Central Daylight Time',        'America/Regina',                 (25, 'Canada Central Standard Time')),
    56: ('Central Standard Time',                 'America/Guatemala',              (33, 'Central America Standard Time')),
    12: ('South American Pacific Daylight Time',  'America/Bogota',                 (45, 'SA Pacific Standard Time')),
    13: ('Eastern Daylight Time',                 'America/New_York',               (35, 'Eastern Standard Time')),
    55: ('Eastern Standard Time',                 'America/Cayman',                 (45, 'SA Pacific Standard Time')),
    15: ('South American Western Daylight Time',  'America/Caracas',                (-1, 'Venezuela Standard Time')),  # ??
    14: ('Chile Daylight Time',                   'America/Santiago',               (56, 'Pacific SA Standard Time')),
    16: ('Atlantic Daylight Time',                'America/Halifax',                (50, 'Atlantic Standard Time')),
    52: ('Atlantic Standard Time',                'America/Puerto_Rico',            (55, 'SA Western Standard Time')),
    59: ('Guyana Time',                           'America/Guyana',                 (55, 'SA Western Standard Time')),
    69: ('Paraguay Daylight Time',                'America/Asuncion',               (-1, 'Paraguay Standard Time')),  # ??
    17: ('Newfoundland Daylight Time',            'America/St_Johns',               (60, 'Newfoundland Standard Time')),
    18: ('South American Eastern Daylight Time',  'America/Argentina/Buenos_Aires', (-1, 'Argentina Standard Time')),  # ??
    19: ('E. South America Daylight Time',        'America/Sao_Paulo',              (65, 'E. South America Standard Time')),
    53: ('Uruguay Time',                          'America/Montevideo',             (-1, 'Montevideo Standard Time')),  # ??
    20: ('Mid-Atlantic Daylight Time',            'America/Noronha',                (75, 'Mid-Atlantic Standard Time')),
    21: ('Azores Daylight Time',                  'Atlantic/Azores',                (80, 'Azores Standard Time')),
    22: ('British Time',                          'Europe/London',                  (85, 'GMT Standard Time')),
    23: ('Greenwich Mean Time',                   'Etc/GMT',                        (-1, 'UTC')),
    60: ('Morocco Time',                          'Africa/Casablanca',              (-1, 'Morocco Standard Time')),
    24: ('West and Central Europe Daylight Time', 'Europe/Berlin',                  (110, 'W. Europe Standard Time')),
    54: ('West Africa Time',                      'Africa/Lagos',                   (113, 'W. Central Africa Standard Time')),
    57: ('Central European Time',                 'Africa/Tunis',                   (113, 'W. Central Africa Standard Time')),
    25: ('GFT Daylight Time',                     'Europe/Athens',                  (130, 'GTB Standard Time')),
    26: ('East Europe Daylight Time',             'Europe/Kiev',                    (125, 'FLE Standard Time')),
    27: ('South Africa Daylight Time',            'Africa/Johannesburg',            (140, 'South Africa Standard Time')),
    28: ('Israel Daylight Time',                  'Asia/Jerusalem',                 (135, 'Israel Standard Time')),
    29: ('Egypt Daylight Time',                   'Africa/Cairo',                   (120, 'Egypt Standard Time')),
    58: ('Eastern European Time',                 'Africa/Tripoli',                 (-1, 'Libya Standard Time')),
    30: ('Saudi Arabia Daylight Time',            'Asia/Riyadh',                    (150, 'Arab Standard Time')),
    32: ('Iran Daylight Time',                    'Asia/Tehran',                    (160, 'Iran Standard Time')),
    31: ('Russian Daylight Time',                 'Europe/Moscow',                  (145, 'Russian Standard Time')),
    33: ('Arabian Daylight Time',                 'Asia/Dubai',                     (165, 'Arabian Standard Time')),
    66: ('Azerbaijan Daylight Time',              'Asia/Baku',                      (-1, 'Azerbaijan Standard Time')),
    34: ('Afghanistan Daylight Time',             'Asia/Kabul',                     (175, 'Afghanistan Standard Time')),
    35: ('West Asia Daylight Time',               'Asia/Karachi',                   (-1, 'Pakistan Standard Time')),
    65: ('Uzbekistan Time',                       'Asia/Tashkent',                  (185, 'West Asia Standard Time')),
    36: ('India Daylight Time',                   'Asia/Calcutta',                  (190, 'India Standard Time')),
    68: ('Nepal Time',                            'Asia/Kathmandu',                 (193, 'Nepal Standard Time')),
    37: ('Central Asia Daylight Time',            'Asia/Dhaka',                     (-1, 'Bangladesh Standard Time')),
    51: ('Almaty Daylight Time',                  'Asia/Almaty',                    (201, 'Central Asia Standard Time')),
    64: ('Yekaterinburg Time',                    'Asia/Yekaterinburg',             (180, 'Ekaterinburg Standard Time')),
    67: ('Myanmar Time',                          'Asia/Rangoon',                   (203, 'Myanmar Standard Time')),
    38: ('Bangkok Daylight Time',                 'Asia/Bangkok',                   (205, 'SE Asia Standard Time')),
    39: ('China Daylight Time',                   'Asia/Shanghai',                  (210, 'China Standard Time')),
    40: ('Taipei Daylight Time',                  'Asia/Hong_Kong',                 (210, 'China Standard Time')),
    61: ('Ulaanbaatar Time',                      'Asia/Ulaanbaatar',               (-1, 'Ulaanbaatar Standard Time')),
    62: ('AUS Western Standard Time',             'Australia/Perth',                (225, 'W. Australia Standard Time')),
    41: ('Japan Standard Time',                   'Asia/Tokyo',                     (235, 'Tokyo Standard Time')),
    42: ('Central Australia Daylight Time',       'Australia/Adelaide',             (250, 'Cen. Australia Standard Time')),
    43: ('Australia Central Standard Time',       'Australia/Darwin',               (245, 'AUS Central Standard Time')),
    44: ('Sydney Daylight Time',                  'Australia/Melbourne',            (255, 'AUS Eastern Standard Time')),
    45: ('West Pacific Daylight Time',            'Australia/Brisbane',             (260, 'E. Australia Standard Time')),
    46: ('Tasmania Daylight Time',                'Australia/Hobart',               (265, 'Tasmania Standard Time')),
    47: ('Central Pacific Daylight Time',         'Asia/Vladivostok',               (270, 'Vladivostok Standard Time')),
    48: ('Magadan Time',                          'Asia/Magadan',                   (-1, 'Magadan Standard Time')),
    49: ('New Zealand Daylight Time',             'Pacific/Auckland',               (290, 'New Zealand Standard Time')),
    50: ('Fiji Daylight Time',                    'Pacific/Fiji',                   (285, 'Fiji Standard Time')),
    63: ('Samoa Time',                            'Pacific/Apia',                   (300, 'Samoa Standard Time'))
}


class _TZDFFormat:

    def __init__(self):
        self._time_diff_ny = None
        self._time_zone_override = None

    def update(self, message):
        if 'responseError' in message:
            error_message = message['responseError']['message']
            raise RequestError(
                'Failed to process reply: {}'.format(error_message))
        self._time_diff_ny = message['securityData'][0]['fieldData']['TIME_DIFF_NY']
        self._time_zone_override = message['securityData'][0]['fieldData']['TIME_ZONE_OVERRIDE']

    def finalize(self):
        if self._time_diff_ny is None or self._time_zone_override is None:
            raise RuntimeError(
                'Server did not send TIME_DIFF_NY or TIME_ZONE_OVERRIDE')
        return self._time_diff_ny, self._time_zone_override


def get_tzdf(session):
    """Return TZDF timezone.

    Return a promise that will resolve into a
    :class:`datetime.tzinfo` object representing the time the
    user has set in "TZDF<Go>" on the terminal.
    """

    def make_tzdf_info(time_diff_and_override):
        time_diff_ny, time_zone_override = time_diff_and_override

        if time_zone_override not in _TZDF_INDEX:
            raise RuntimeError(
                'Server sent unknown time zone override value {}'.format(time_zone_override))

        name, olson, win = _TZDF_INDEX[time_zone_override]
        tzinfo = dateutil.tz.gettz(olson)
        if tzinfo is None:
            raise RuntimeError('Timezone for "{}" (timezone override value {}) not found'.format(
                olson, time_zone_override))

        # cross-check with the TIME_DIFF_NY value
        new_york_timezone = 'America/New_York'
        tzinfo_ny = dateutil.tz.gettz(new_york_timezone)
        if tzinfo_ny is None:
            raise RuntimeError(
                'Timezone for "{}" not found'.format(new_york_timezone))

        utcnow = datetime.datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
        nytime = utcnow.astimezone(tzinfo_ny).replace(tzinfo=None)
        tzdftime = utcnow.astimezone(tzinfo).replace(tzinfo=None)

        if tzdftime - nytime != datetime.timedelta(seconds=time_diff_ny):
            raise RuntimeError(
                'Timezone inconsistency check failed: dateutil says timezone'
                ' "{}" is {} minutes behind New York, but Bloomberg says {}'.format(olson,
                                                                                    (tzdftime - nytime).total_seconds() / 60,
                                                                                    time_diff_ny / 60))

        return tzinfo

    #  Note that the security for which we request this field is irrelevant.
    return session.send_request('//blp/refdata', 'ReferenceDataRequest',
                                {'securities': ['IBM US Equity'], 'fields': [
                                    'TIME_ZONE_OVERRIDE', 'TIME_DIFF_NY']},
                                format=_TZDFFormat(), callback=make_tzdf_info)
