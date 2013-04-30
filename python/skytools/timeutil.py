
"""Fill gaps in Python time API-s.

parse_iso_timestamp:
    Parse reasonable subset of ISO_8601 timestamp formats.
    [ http://en.wikipedia.org/wiki/ISO_8601 ]

datetime_to_timestamp:
    Get POSIX timestamp from datetime() object.

"""

import re
import time
from datetime import datetime, timedelta, tzinfo

__all__ = ['parse_iso_timestamp', 'FixedOffsetTimezone', 'datetime_to_timestamp']

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

#
# Parse ISO_8601 timestamps.
#

"""
TODO:
- support more combinations from ISO 8601 (only reasonable ones)
- cache TZ objects
- make it faster?
"""

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

    Only fixed offset timezones are supported.

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

#
# POSIX timestamp from datetime()
#

UTC = FixedOffsetTimezone(0)
TZ_EPOCH = datetime.fromtimestamp(0, UTC)
UTC_NOTZ_EPOCH = datetime.utcfromtimestamp(0)

def datetime_to_timestamp(dt, local_time=True):
    """Get posix timestamp from datetime() object.

    if dt is without timezone, then local_time specifies
    whether it's UTC or local time.

    Returns seconds since epoch as float.

    >>> datetime_to_timestamp(parse_iso_timestamp("2005-06-01 15:00:59.5 +02"))
    1117630859.5
    >>> datetime_to_timestamp(datetime.fromtimestamp(1117630859.5, UTC))
    1117630859.5
    >>> datetime_to_timestamp(datetime.fromtimestamp(1117630859.5))
    1117630859.5
    >>> now = datetime.utcnow()
    >>> now2 = datetime.utcfromtimestamp(datetime_to_timestamp(now, False))
    >>> abs(now2.microsecond - now.microsecond) < 100
    True
    >>> now2 = now2.replace(microsecond = now.microsecond)
    >>> now == now2
    True
    >>> now = datetime.now()
    >>> now2 = datetime.fromtimestamp(datetime_to_timestamp(now))
    >>> abs(now2.microsecond - now.microsecond) < 100
    True
    >>> now2 = now2.replace(microsecond = now.microsecond)
    >>> now == now2
    True
    """
    if dt.tzinfo:
        delta = dt - TZ_EPOCH
        return delta.total_seconds()
    elif local_time:
        s = time.mktime(dt.timetuple())
        return s + (dt.microsecond / 1000000.0)
    else:
        delta = dt - UTC_NOTZ_EPOCH
        return delta.total_seconds()

if __name__ == '__main__':
    import doctest
    doctest.testmod()

