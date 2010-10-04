"""Find table structure and allow CREATE/DROP elements from it.
"""

import sys, re

from skytools.sqltools import fq_name_parts, get_table_oid
from skytools.quoting import quote_ident, quote_fqident

__all__ = ['TableStruct',
    'T_TABLE', 'T_CONSTRAINT', 'T_INDEX', 'T_TRIGGER',
    'T_RULE', 'T_GRANT', 'T_OWNER', 'T_PARENT', 'T_PKEY', 'T_ALL']

T_TABLE       = 1 << 0
T_CONSTRAINT  = 1 << 1
T_INDEX       = 1 << 2
T_TRIGGER     = 1 << 3
T_RULE        = 1 << 4
T_GRANT       = 1 << 5
T_OWNER       = 1 << 6
T_PARENT      = 1 << 7
T_PKEY        = 1 << 20 # special, one of constraints
T_ALL = (  T_TABLE | T_CONSTRAINT | T_INDEX
         | T_TRIGGER | T_RULE | T_GRANT | T_OWNER )

#
# Utility functions
#

def find_new_name(curs, name):
    """Create new object name for case the old exists.

    Needed when creating a new table besides old one.
    """
    # cut off previous numbers
    m = re.search('_[0-9]+$', name)
    if m:
        name = name[:m.start()]

    # now loop
    for i in range(1, 1000):
        tname = "%s_%d" % (name, i)
        q = "select count(1) from pg_class where relname = %s"
        curs.execute(q, [tname])
        if curs.fetchone()[0] == 0:
            return tname

    # failed
    raise Exception('find_new_name failed')

def rx_replace(rx, sql, new_part):
    """Find a regex match and replace that part with new_part."""
    m = re.search(rx, sql, re.I)
    if not m:
        raise Exception('rx_replace failed')
    p1 = sql[:m.start()]
    p2 = sql[m.end():]
    return p1 + new_part + p2

#
# Schema objects
#

class TElem(object):
    """Keeps info about one metadata object."""
    SQL = ""
    type = 0
    def get_create_sql(self, curs):
        """Return SQL statement for creating or None if not supported."""
        return None
    def get_drop_sql(self, curs):
        """Return SQL statement for dropping or None of not supported."""
        return None
    def get_load_sql(cls, pg_vers):
        """Return SQL statement for finding objects."""
        return cls.SQL
    get_load_sql = classmethod(get_load_sql)

class TConstraint(TElem):
    """Info about constraint."""
    type = T_CONSTRAINT
    SQL = """
        SELECT c.conname as name, pg_get_constraintdef(c.oid) as def, c.contype,
               i.indisclustered as is_clustered
          FROM pg_constraint c LEFT JOIN pg_index i ON
            c.conrelid = i.indrelid AND
            c.conname = (SELECT r.relname FROM pg_class r WHERE r.oid = i.indexrelid)
          WHERE c.conrelid = %(oid)s AND c.contype != 'f'
    """
    def __init__(self, table_name, row):
        self.table_name = table_name
        self.name = row['name']
        self.defn = row['def']
        self.contype = row['contype']
        self.is_clustered = row['is_clustered']

        # tag pkeys
        if self.contype == 'p':
            self.type += T_PKEY

    def get_create_sql(self, curs, new_table_name=None):
        # no ONLY here as table with childs (only case that matters)
        # cannot have contraints that childs do not have
        fmt = "ALTER TABLE %s ADD CONSTRAINT %s %s;"
        if new_table_name:
            name = self.name
            if self.contype in ('p', 'u'):
                name = find_new_name(curs, self.name)
            qtbl = quote_fqident(new_table_name)
            qname = quote_ident(name)
        else:
            qtbl = quote_fqident(self.table_name)
            qname = quote_ident(self.name)
        sql = fmt % (qtbl, qname, self.defn)
        if self.is_clustered:
            sql +=' ALTER TABLE ONLY %s CLUSTER ON %s;' % (qtbl, qname)
        return sql

    def get_drop_sql(self, curs):
        fmt = "ALTER TABLE ONLY %s DROP CONSTRAINT %s;"
        sql = fmt % (quote_fqident(self.table_name), quote_ident(self.name))
        return sql

class TIndex(TElem):
    """Info about index."""
    type = T_INDEX
    SQL = """
        SELECT n.nspname || '.' || c.relname as name,
               pg_get_indexdef(i.indexrelid) as defn,
               c.relname                     as local_name,
               i.indisclustered              as is_clustered
         FROM pg_index i, pg_class c, pg_namespace n
        WHERE c.oid = i.indexrelid AND i.indrelid = %(oid)s
          AND n.oid = c.relnamespace
          AND NOT EXISTS
            (select objid from pg_depend
              where classid = %(pg_class_oid)s
                and objid = c.oid
                and deptype = 'i')
    """
    def __init__(self, table_name, row):
        self.name = row['name']
        self.defn = row['defn'] + ';'
        self.is_clustered = row['is_clustered']
        self.table_name = table_name
        self.local_name = row['local_name']

    def get_create_sql(self, curs, new_table_name = None):
        if new_table_name:
            # fixme: seems broken
            iname = find_new_name(curs, self.name)
            tname = new_table_name
            pnew = "INDEX %s ON %s " % (quote_ident(iname), quote_fqident(tname))
            rx = r"\bINDEX[ ][a-z0-9._]+[ ]ON[ ][a-z0-9._]+[ ]"
            sql = rx_replace(rx, self.defn, pnew)
        else:
            sql = self.defn
            iname = self.local_name
            tname = self.table_name
        if self.is_clustered:
            sql += ' ALTER TABLE ONLY %s CLUSTER ON %s;' % (
                quote_fqident(tname), quote_ident(iname))
        return sql

    def get_drop_sql(self, curs):
        return 'DROP INDEX %s;' % quote_fqident(self.name)

class TRule(TElem):
    """Info about rule."""
    type = T_RULE
    SQL = """
        SELECT rulename as name, pg_get_ruledef(oid) as def
          FROM pg_rewrite
         WHERE ev_class = %(oid)s AND rulename <> '_RETURN'::name
    """
    def __init__(self, table_name, row, new_name = None):
        self.table_name = table_name
        self.name = row['name']
        self.defn = row['def']

    def get_create_sql(self, curs, new_table_name = None):
        if not new_table_name:
            return self.defn
        # fixme: broken
        rx = r"\bTO[ ][a-z0-9._]+[ ]DO[ ]"
        pnew = "TO %s DO " % new_table_name
        return rx_replace(rx, self.defn, pnew)

    def get_drop_sql(self, curs):
        return 'DROP RULE %s ON %s' % (quote_ident(self.name), quote_fqident(self.table_name))

class TTrigger(TElem):
    """Info about trigger."""
    type = T_TRIGGER

    def get_load_sql(cls, pg_vers):
        """Return SQL statement for finding objects."""

        sql = "SELECT tgname as name, pg_get_triggerdef(oid) as def "\
              "  FROM  pg_trigger "\
              "  WHERE tgrelid = %(oid)s AND "
        if pg_vers >= 90000:
            sql += "NOT tgisinternal"
        else:
            sql += "NOT tgisconstraint"
        return sql
    get_load_sql = classmethod(get_load_sql)

    def __init__(self, table_name, row):
        self.table_name = table_name
        self.name = row['name']
        self.defn = row['def'] + ';'

    def get_create_sql(self, curs, new_table_name = None):
        if not new_table_name:
            return self.defn
        # fixme: broken
        rx = r"\bON[ ][a-z0-9._]+[ ]"
        pnew = "ON %s " % new_table_name
        return rx_replace(rx, self.defn, pnew)

    def get_drop_sql(self, curs):
        return 'DROP TRIGGER %s ON %s' % (quote_ident(self.name), quote_fqident(self.table_name))

class TParent(TElem):
    """Info about trigger."""
    type = T_PARENT
    SQL = """
        SELECT n.nspname||'.'||c.relname AS name
          FROM pg_inherits i
          JOIN pg_class c ON i.inhparent = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
         WHERE i.inhrelid = %(oid)s
    """
    def __init__(self, table_name, row):
        self.name = table_name
        self.parent_name = row['name']

    def get_create_sql(self, curs, new_table_name = None):
        return 'ALTER TABLE ONLY %s INHERIT %s' % (quote_fqident(self.name), quote_fqident(self.parent_name))

    def get_drop_sql(self, curs):
        return 'ALTER TABLE ONLY %s NO INHERIT %s' % (quote_fqident(self.name), quote_fqident(self.parent_name))


class TOwner(TElem):
    """Info about table owner."""
    type = T_OWNER
    SQL = """
        SELECT pg_get_userbyid(relowner) as owner FROM pg_class
         WHERE oid = %(oid)s
    """
    def __init__(self, table_name, row, new_name = None):
        self.table_name = table_name
        self.name = 'Owner'
        self.owner = row['owner']

    def get_create_sql(self, curs, new_name = None):
        if not new_name:
            new_name = self.table_name
        return 'ALTER TABLE %s OWNER TO %s;' % (quote_fqident(new_name), quote_ident(self.owner))

class TGrant(TElem):
    """Info about permissions."""
    type = T_GRANT
    SQL = "SELECT relacl FROM pg_class where oid = %(oid)s"
    acl_map = {
        'r': 'SELECT', 'w': 'UPDATE',     'a': 'INSERT',  'd': 'DELETE',
        'R': 'RULE',   'x': 'REFERENCES', 't': 'TRIGGER', 'X': 'EXECUTE',
        'U': 'USAGE',  'C': 'CREATE',     'T': 'TEMPORARY'
    }
    def acl_to_grants(self, acl):
        if acl == "arwdRxt":   # ALL for tables
            return "ALL"
        return ", ".join([ self.acl_map[c] for c in acl ])

    def parse_relacl(self, relacl):
        if relacl is None:
            return []
        if len(relacl) > 0 and relacl[0] == '{' and relacl[-1] == '}':
            relacl = relacl[1:-1]
        list = []
        for f in relacl.split(','):
            user, tmp = f.strip('"').split('=')
            acl, who = tmp.split('/')
            list.append((user, acl, who))
        return list

    def __init__(self, table_name, row, new_name = None):
        self.name = table_name
        self.acl_list = self.parse_relacl(row['relacl'])

    def get_create_sql(self, curs, new_name = None):
        if not new_name:
            new_name = self.name

        list = []
        for user, acl, who in self.acl_list:
            astr = self.acl_to_grants(acl)
            sql = "GRANT %s ON %s TO %s;" % (astr, quote_fqident(new_name), quote_ident(user))
            list.append(sql)
        return "\n".join(list)

    def get_drop_sql(self, curs):
        list = []
        for user, acl, who in self.acl_list:
            sql = "REVOKE ALL FROM %s ON %s;" % (quote_ident(user), quote_fqident(self.name))
            list.append(sql)
        return "\n".join(list)

class TColumn(TElem):
    """Info about table column."""
    SQL = """
        select a.attname as name,
            a.attname || ' '
                || format_type(a.atttypid, a.atttypmod)
                || case when a.attnotnull then ' not null' else '' end
                || case when a.atthasdef then ' default ' || d.adsrc else '' end
            as def
          from pg_attribute a left join pg_attrdef d
            on (d.adrelid = a.attrelid and d.adnum = a.attnum)
         where a.attrelid = %(oid)s
           and not a.attisdropped
           and a.attnum > 0
         order by a.attnum;
    """
    def __init__(self, table_name, row):
        self.name = row['name']
        self.column_def = row['def']

class TTable(TElem):
    """Info about table only (columns)."""
    type = T_TABLE
    def __init__(self, table_name, col_list):
        self.name = table_name
        self.col_list = col_list

    def get_create_sql(self, curs, new_name = None):
        if not new_name:
            new_name = self.name
        sql = "create table %s (" % quote_fqident(new_name)
        sep = "\n\t"
        for c in self.col_list:
            sql += sep + c.column_def
            sep = ",\n\t"
        sql += "\n);"
        return sql
    
    def get_drop_sql(self, curs):
        return "DROP TABLE %s;" % quote_fqident(self.name)

#
# Main table object, loads all the others
#

class TableStruct(object):
    """Collects and manages all info about table.

    Allow to issue CREATE/DROP statements about any
    group of elements.
    """
    def __init__(self, curs, table_name):
        """Initializes class by loading info about table_name from database."""

        self.table_name = table_name

        # fill args
        schema, name = fq_name_parts(table_name)
        args = {
            'schema': schema,
            'table': name,
            'oid': get_table_oid(curs, table_name),
            'pg_class_oid': get_table_oid(curs, 'pg_catalog.pg_class'),
        }
        
        # load table struct
        self.col_list = self._load_elem(curs, args, TColumn)
        self.object_list = [ TTable(table_name, self.col_list) ]

        # load additional objects
        to_load = [TConstraint, TIndex, TTrigger, TRule, TGrant, TOwner, TParent]
        for eclass in to_load:
            self.object_list += self._load_elem(curs, args, eclass)

    def _load_elem(self, curs, args, eclass):
        list = []
        sql = eclass.get_load_sql(curs.connection.server_version)
        curs.execute(sql % args)
        for row in curs.dictfetchall():
            list.append(eclass(self.table_name, row))
        return list

    def create(self, curs, objs, new_table_name = None, log = None):
        """Issues CREATE statements for requested set of objects.
        
        If new_table_name is giver, creates table under that name
        and also tries to rename all indexes/constraints that conflict
        with existing table.
        """

        for o in self.object_list:
            if o.type & objs:
                sql = o.get_create_sql(curs, new_table_name)
                if not sql:
                    continue
                if log:
                    log.info('Creating %s' % o.name)
                    log.debug(sql)
                curs.execute(sql)

    def drop(self, curs, objs, log = None):
        """Issues DROP statements for requested set of objects."""
        # make sure the creating & dropping happen in reverse order
        olist = self.object_list[:]
        olist.reverse()
        for o in olist:
            if o.type & objs:
                sql = o.get_drop_sql(curs)
                if not sql:
                    continue
                if log:
                    log.info('Dropping %s' % o.name)
                    log.debug(sql)
                curs.execute(sql)

    def get_column_list(self):
        """Returns list of column names the table has."""

        res = []
        for c in self.col_list:
            res.append(c.name)
        return res

def test():
    from skytools import connect_database
    db = connect_database("dbname=fooz")
    curs = db.cursor()
    
    s = TableStruct(curs, "public.data1")

    s.drop(curs, T_ALL)
    s.create(curs, T_ALL)
    s.create(curs, T_ALL, "data1_new")
    s.create(curs, T_PKEY)

if __name__ == '__main__':
    test()

