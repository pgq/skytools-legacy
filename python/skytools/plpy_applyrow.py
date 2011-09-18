
"""
PLPY helper module for applying row events from pgq.logutriga().
"""


import plpy

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

## TODO: automatic fkey detection
# find FK columns
FK_SQL = """
SELECT (SELECT array_agg( (SELECT attname::text FROM pg_attribute
                            WHERE attrelid = conrelid AND attnum = conkey[i]))
          FROM generate_series(1, array_upper(conkey, 1)) i) AS kcols,
       (SELECT array_agg( (SELECT attname::text FROM pg_attribute
                            WHERE attrelid = confrelid AND attnum = confkey[i]))
          FROM generate_series(1, array_upper(confkey, 1)) i) AS fcols,
       confrelid::regclass::text AS ftable
  FROM pg_constraint
 WHERE conrelid = {tbl}::regclass AND contype='f'
"""

class DataError(Exception):
    "Invalid data"

def colfilter_full(rnew, rold):
    return rnew

def colfilter_changed(rnew, rold):
    res = {}
    for k, v in rnew:
        if rnew[k] != rold[k]:
            res[k] = rnew[k]
    return res

def canapply_dummy(rnew, rold):
    return True

def canapply_tstamp_helper(rnew, rold, tscol):
    tnew = rnew[tscol]
    told = rold[tscol]
    if not tnew[0].isdigit():
        raise DataError('invalid timestamp')
    if not told[0].isdigit():
        raise DataError('invalid timestamp')
    return tnew > told

def applyrow(tblname, ev_type, new_row,
             backup_row = None,
             alt_pkey_cols = None,
             fkey_cols = None,
             fkey_ref_table = None,
             fkey_ref_cols = None,
             fn_canapply = canapply_dummy,
             fn_colfilter = colfilter_full):
    """Core logic.  Actual decisions will be done in callback functions.

    - [IUD]: If row referenced by fkey does not exist, event is not applied
    - If pkey does not exist but alt_pkey does, row is not applied.
    
    @param tblname: table name, schema-qualified
    @param ev_type: [IUD]:pkey1,pkey2
    @param alt_pkey_cols: list of alternatice columns to consuder
    @param fkey_cols: columns in this table that refer to other table
    @param fkey_ref_table: other table referenced here
    @param fkey_ref_cols: column in other table that must match
    @param fn_canapply: callback function, gets new and old row, returns whether the row should be applied
    @param fn_colfilter: callback function, gets new and old row, returns dict of final columns to be applied
    """

    gd = None

    # parse ev_type
    tmp = ev_type.split(':', 1)
    if len(tmp) != 2 or tmp[0] not in ('I', 'U', 'D'):
        raise DataError('Unsupported ev_type: '+repr(ev_type))
    if not tmp[1]:
        raise DataError('No pkey in event')

    cmd = tmp[0]
    pkey_cols = tmp[1].split(',')
    qtblname = skytools.quote_fqident(tblname)

    # parse ev_data
    fields = skytools.db_urldecode(new_row)

    if ev_type.find('}') >= 0:
        raise DataError('Really suspicious activity')
    if ",".join(fields.keys()).find('}') >= 0:
        raise DataError('Really suspicious activity 2')

    # generate pkey expressions
    tmp = ["%s = {%s}" % (skytools.quote_ident(k), k) for k in pkey_cols]
    pkey_expr = " and ".join(tmp)
    alt_pkey_expr = None
    if alt_pkey_cols:
        tmp = ["%s = {%s}" % (skytools.quote_ident(k), k) for k in alt_pkey_cols]
        alt_pkey_expr = " and ".join(tmp)

    log = "data ok"

    #
    # Row data seems fine, now apply it
    #

    if fkey_ref_table:
        tmp = []
        for k, rk in zip(fkey_cols, fkey_ref_cols):
            tmp.append("%s = {%s}" % (skytools.quote_ident(rk), k))
        fkey_expr = " and ".join(tmp)
        q = "select 1 from only %s where %s" % (
                skytools.quote_fqident(fkey_ref_table),
                fkey_expr)
        res = skytools.plpy_exec(gd, q, fields)
        if not res:
            return "IGN: parent row does not exist"
        log += ", fkey ok"

    # fetch old row
    if alt_pkey_expr:
        q = "select * from only %s where %s for update" % (qtblname, alt_pkey_expr)
        res = skytools.plpy_exec(gd, q, fields)
        if res:
            oldrow = res[0]
            # if altpk matches, but pk not, then delete
            need_del = 0
            for k in pkey_cols:
                # fixme: proper type cmp?
                if fields[k] != str(oldrow[k]):
                    need_del = 1
                    break
            if need_del:
                log += ", altpk del"
                q = "delete from only %s where %s" % (qtblname, alt_pkey_expr)
                skytools.plpy_exec(gd, q, fields)
                res = None
            else:
                log += ", altpk ok"
    else:
        # no altpk
        q = "select * from only %s where %s for update" % (qtblname, pkey_expr)
        res = skytools.plpy_exec(None, q, fields)

    # got old row, with same pk and altpk
    if res:
        oldrow = res[0]
        log += ", old row"
        ok = fn_canapply(fields, oldrow)
        if ok:
            log += ", new row better"
        if not ok:
            # ignore the update
            return "IGN:" + log + ", current row more up-to-date"
    else:
        log += ", no old row"
        oldrow = None

    if res:
        if cmd == 'I':
            cmd = 'U'
    else:
        if cmd == 'U':
            cmd = 'I'

    # allow column changes
    if oldrow:
        fields2 = fn_colfilter(fields, oldrow)
        for k in pkey_cols:
            if k not in fields2:
                fields2[k] = fields[k]
        fields = fields2

    # apply change
    if cmd == 'I':
        q = skytools.mk_insert_sql(fields, tblname, pkey_cols)
    elif cmd == 'U':
        q = skytools.mk_update_sql(fields, tblname, pkey_cols)
    elif cmd == 'D':
        q = skytools.mk_delete_sql(fields, tblname, pkey_cols)
    else:
        plpy.error('Huh')

    plpy.execute(q)

    return log


def ts_conflict_handler(gd, args):
    """Conflict handling based on timestamp column."""

    conf = skytools.db_urldecode(args[0])
    timefield = conf['timefield']
    ev_type = args[1]
    ev_data = args[2]
    ev_extra1 = args[3]
    ev_extra2 = args[4]
    ev_extra3 = args[5]
    ev_extra4 = args[6]
    altpk = None
    if 'altpk' in conf:
        altpk = conf['altpk'].split(',')

    def ts_canapply(rnew, rold):
        return canapply_tstamp_helper(rnew, rold, timefield)

    return applyrow(ev_extra1, ev_type, ev_data,
                    backup_row = ev_extra2,
                    alt_pkey_cols = altpk,
                    fkey_ref_table = conf.get('fkey_ref_table'),
                    fkey_ref_cols = conf.get('fkey_ref_cols'),
                    fkey_cols = conf.get('fkey_cols'),
                    fn_canapply = ts_canapply)

