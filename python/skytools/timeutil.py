
"""Module for parsing ISO 8601 format timestamps.

Only fixed offset timezones are supported.

http://en.wikipedia.org/wiki/ISO_8601
"""

import re

from datetime import datetime, timedelta, tzinfo

"""
TODO:
- support more combinations from ISO 8601 (only reasonable ones)
- cache TZ objects
- make it faster?
"""

__all__ = ['parse_iso_timestamp', 'FixedOffsetTimezone']

class FixedOffsetTimezone(tzinfo):
    """Fixed offset in minutes east from UTC."""
    __slots__ = ('__offset', '__name')

    def __init__(self, offset):
        self.__offset = timedelta(minutes = offset)

        # numeric tz name
        h, m = divmod(abs(offset), 60)
        if offset < 0:
            h = -h
        if m:
            self.__name = "%+03d:%02d" % (h,m)
        else:
            self.__name = "%+03d" % h

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO

ZERO = timedelta(0)

_iso_regex = r"""
    \s*
    (?P<year> \d\d\d\d) [-] (?P<month> \d\d) [-] (?P<day> \d\d) [ T]
    (?P<hour> \d\d) [:] (?P<min> \d\d)
    (?: [:] (?P<sec> \d\d ) (?: [.,] (?P<ss> \d+))? )?
    (?: \s*  (?P<tzsign> [-+]) (?P<tzhr> \d\d) (?: [:]? (?P<tzmin> \d\d))? )?
    \s* $
    """
_iso_rc = None

def parse_iso_timestamp(s, default_tz = None):
    """Parse ISO timestamp to datetime object.
    
    YYYY-MM-DD[ T]HH:MM[:SS[.ss]][-+HH[:MM]]

    Assumes that second fractions are zero-trimmed from the end,
    so '.15' means 150000 microseconds.

    If the timezone offset is not present, use default_tz as tzinfo.
    By default its None, meaning the datetime object will be without tz.

    >>> str(parse_iso_timestamp('2005-06-01 15:00'))
    '2005-06-01 15:00:00'
    >>> str(parse_iso_timestamp(' 2005-06-01T15:00 +02 '))
    '2005-06-01 15:00:00+02:00'
    >>> str(parse_iso_timestamp('2005-06-01 15:00:33+02:00'))
    '2005-06-01 15:00:33+02:00'
    >>> d = parse_iso_timestamp('2005-06-01 15:00:59.33 +02')
    >>> d.strftime("%z %Z")
    '+0200 +02'
    >>> str(parse_iso_timestamp(str(d)))
    '2005-06-01 15:00:59.330000+02:00'
    >>> parse_iso_timestamp('2005-06-01 15:00-0530').strftime('%Y-%m-%d %H:%M %z %Z')
    '2005-06-01 15:00 -0530 -05:30'
    """

    global _iso_rc
    if _iso_rc is None:
        _iso_rc = re.compile(_iso_regex, re.X)

    m = _iso_rc.match(s)
    if not m:
        raise ValueError('Date not in ISO format: %s' % repr(s))

    tz = default_tz
    if m.group('tzsign'):
        tzofs = int(m.group('tzhr')) * 60
        if m.group('tzmin'):
            tzofs += int(m.group('tzmin'))
        if m.group('tzsign') == '-':
            tzofs = -tzofs
        tz = FixedOffsetTimezone(tzofs)

    return datetime(int(m.group('year')),
                int(m.group('month')),
                int(m.group('day')),
                int(m.group('hour')),
                int(m.group('min')),
                m.group('sec') and int(m.group('sec')) or 0,
                m.group('ss') and int(m.group('ss').ljust(6, '0')) or 0,
                tz)

if __name__ == '__main__':
    import doctest
    doctest.testmod()

