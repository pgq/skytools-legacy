"""Find table structure and allow CREATE/DROP elements from it.
"""

import re

import skytools

from skytools import quote_ident, quote_fqident

__all__ = ['TableStruct', 'SeqStruct',
    'T_TABLE', 'T_CONSTRAINT', 'T_INDEX', 'T_TRIGGER',
    'T_RULE', 'T_GRANT', 'T_OWNER', 'T_PKEY', 'T_ALL',
    'T_SEQUENCE', 'T_PARENT', 'T_DEFAULT']

T_TABLE       = 1 << 0
T_CONSTRAINT  = 1 << 1
T_INDEX       = 1 << 2
T_TRIGGER     = 1 << 3
T_RULE        = 1 << 4
T_GRANT       = 1 << 5
T_OWNER       = 1 << 6
T_SEQUENCE    = 1 << 7
T_PARENT      = 1 << 8
T_DEFAULT     = 1 << 9
T_PKEY        = 1 << 20 # special, one of constraints
T_ALL = (  T_TABLE | T_CONSTRAINT | T_INDEX | T_SEQUENCE
         | T_TRIGGER | T_RULE | T_GRANT | T_OWNER | T_DEFAULT )

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
        raise Exception('rx_replace failed: rx=%r sql=%r new=%r' % (rx, sql, new_part))
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
    def get_create_sql(self, curs, new_name = None):
        """Return SQL statement for creating or None if not supported."""
        return None
    def get_drop_sql(self, curs):
        """Return SQL statement for dropping or None of not supported."""
        return None

    @classmethod
    def get_load_sql(cls, pgver):
        """Return SQL statement for finding objects."""
        return cls.SQL

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
        """Init constraint."""
        self.table_name = table_name
        self.name = row['name']
        self.defn = row['def']
        self.contype = row['contype']
        self.is_clustered = row['is_clustered']

        # tag pkeys
        if self.contype == 'p':
            self.type += T_PKEY

    def get_create_sql(self, curs, new_table_name=None):
        """Generate creation SQL."""
        # no ONLY here as table with childs (only case that matters)
        # cannot have contraints that childs do not have
        fmt = "ALTER TABLE %s ADD CONSTRAINT %s\n  %s;"
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
            sql +=' ALTER TABLE ONLY %s\n  CLUSTER ON %s;' % (qtbl, qname)
        return sql

    def get_drop_sql(self, curs):
        """Generate removal sql."""
        fmt = "ALTER TABLE ONLY %s\n  DROP CONSTRAINT %s;"
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
        self.defn = row['defn'].replace(' USING ', '\n  USING ', 1) + ';'
        self.is_clustered = row['is_clustered']
        self.table_name = table_name
        self.local_name = row['local_name']

    def get_create_sql(self, curs, new_table_name = None):
        """Generate creation SQL."""
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
            sql += ' ALTER TABLE ONLY %s\n  CLUSTER ON %s;' % (
                quote_fqident(tname), quote_ident(iname))
        return sql

    def get_drop_sql(self, curs):
        return 'DROP INDEX %s;' % quote_fqident(self.name)

class TRule(TElem):
    """Info about rule."""
    type = T_RULE
    SQL = """SELECT rw.*, pg_get_ruledef(rw.oid) as def
              FROM pg_rewrite rw
             WHERE rw.ev_class = %(oid)s AND rw.rulename <> '_RETURN'::name
    """
    def __init__(self, table_name, row, new_name = None):
        self.table_name = table_name
        self.name = row['rulename']
        self.defn = row['def']
        self.enabled = row.get('ev_enabled', 'O')

    def get_create_sql(self, curs, new_table_name = None):
        """Generate creation SQL."""
        if not new_table_name:
            sql = self.defn
            table = self.table_name
        else:
            idrx = r'''([a-z0-9._]+|"([^"]+|"")+")+'''
            # fixme: broken / quoting
            rx = r"\bTO[ ]" + idrx
            rc = re.compile(rx, re.X)
            m = rc.search(self.defn)
            if not m:
                raise Exception('Cannot find table name in rule')
            old_tbl = m.group(1)
            new_tbl = quote_fqident(new_table_name)
            sql = self.defn.replace(old_tbl, new_tbl)
            table = new_table_name
        if self.enabled != 'O':
            # O - rule fires in origin and local modes
            # D - rule is disabled
            # R - rule fires in replica mode
            # A - rule fires always
            action = {'R': 'ENABLE REPLICA',
                      'A': 'ENABLE ALWAYS',
                      'D': 'DISABLE'} [self.enabled]
            sql += ('\nALTER TABLE %s %s RULE %s;' % (table, action, self.name))
        return sql

    def get_drop_sql(self, curs):
        return 'DROP RULE %s ON %s' % (quote_ident(self.name), quote_fqident(self.table_name))


class TTrigger(TElem):
    """Info about trigger."""
    type = T_TRIGGER

    def __init__(self, table_name, row):
        self.table_name = table_name
        self.name = row['name']
        self.defn = row['def'] + ';'
        self.defn = self.defn.replace('FOR EACH', '\n  FOR EACH', 1)

    def get_create_sql(self, curs, new_table_name = None):
        """Generate creation SQL."""
        if not new_table_name:
            return self.defn

        # fixme: broken / quoting
        rx = r"\bON[ ][a-z0-9._]+[ ]"
        pnew = "ON %s " % new_table_name
        return rx_replace(rx, self.defn, pnew)

    def get_drop_sql(self, curs):
        return 'DROP TRIGGER %s ON %s' % (quote_ident(self.name), quote_fqident(self.table_name))

    @classmethod
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
        return 'ALTER TABLE ONLY %s\n  INHERIT %s' % (quote_fqident(self.name), quote_fqident(self.parent_name))

    def get_drop_sql(self, curs):
        return 'ALTER TABLE ONLY %s\n  NO INHERIT %s' % (quote_fqident(self.name), quote_fqident(self.parent_name))


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
        """Generate creation SQL."""
        if not new_name:
            new_name = self.table_name
        return 'ALTER TABLE %s\n  OWNER TO %s;' % (quote_fqident(new_name), quote_ident(self.owner))

class TGrant(TElem):
    """Info about permissions."""
    type = T_GRANT
    SQL = "SELECT relacl FROM pg_class where oid = %(oid)s"

    # Sync with: src/include/utils/acl.h
    acl_map = {
        'a': 'INSERT',
        'r': 'SELECT',
        'w': 'UPDATE',
        'd': 'DELETE',
        'D': 'TRUNCATE',
        'x': 'REFERENCES',
        't': 'TRIGGER',
        'X': 'EXECUTE',
        'U': 'USAGE',
        'C': 'CREATE',
        'T': 'TEMPORARY',
        'c': 'CONNECT',
        # old
        'R': 'RULE',
    }

    def acl_to_grants(self, acl):
        if acl == "arwdRxt":   # ALL for tables
            return "ALL"
        i = 0
        lst1 = []
        lst2 = []
        while i < len(acl):
            a = self.acl_map[acl[i]]
            if i+1 < len(acl) and acl[i+1] == '*':
                lst2.append(a)
                i += 2
            else:
                lst1.append(a)
                i += 1
        return ", ".join(lst1), ", ".join(lst2)

    def parse_relacl(self, relacl):
        """Parse ACL to tuple of (user, acl, who)"""
        if relacl is None:
            return []
        tup_list = []
        for sacl in skytools.parse_pgarray(relacl):
            acl = skytools.parse_acl(sacl)
            if not acl:
                continue
            tup_list.append(acl)
        return tup_list

    def __init__(self, table_name, row, new_name = None):
        self.name = table_name
        self.acl_list = self.parse_relacl(row['relacl'])

    def get_create_sql(self, curs, new_name = None):
        """Generate creation SQL."""
        if not new_name:
            new_name = self.name

        qtarget = quote_fqident(new_name)

        sql_list = []
        for role, acl, who in self.acl_list:
            qrole = quote_ident(role)
            astr1, astr2 = self.acl_to_grants(acl)
            if astr1:
                sql = "GRANT %s ON %s\n  TO %s;" % (astr1, qtarget, qrole)
                sql_list.append(sql)
            if astr2:
                sql = "GRANT %s ON %s\n  TO %s WITH GRANT OPTION;" % (astr2, qtarget, qrole)
                sql_list.append(sql)
        return "\n".join(sql_list)

    def get_drop_sql(self, curs):
        sql_list = []
        for user, acl, who in self.acl_list:
            sql = "REVOKE ALL FROM %s ON %s;" % (quote_ident(user), quote_fqident(self.name))
            sql_list.append(sql)
        return "\n".join(sql_list)

class TColumnDefault(TElem):
    """Info about table column default value."""
    type = T_DEFAULT
    SQL = """
        select a.attname as name, pg_get_expr(d.adbin, d.adrelid) as expr
          from pg_attribute a left join pg_attrdef d
            on (d.adrelid = a.attrelid and d.adnum = a.attnum)
         where a.attrelid = %(oid)s
           and not a.attisdropped
           and a.atthasdef
           and a.attnum > 0
         order by a.attnum;
    """
    def __init__(self, table_name, row):
        self.table_name = table_name
        self.name = row['name']
        self.expr = row['expr']

    def get_create_sql(self, curs, new_name = None):
        """Generate creation SQL."""
        tbl = new_name or self.table_name
        sql = "ALTER TABLE ONLY %s ALTER COLUMN %s\n  SET DEFAULT %s;" % (
                quote_fqident(tbl), quote_ident(self.name), self.expr)
        return sql

    def get_drop_sql(self, curs):
        return "ALTER TABLE %s ALTER COLUMN %s\n  DROP DEFAULT;" % (
                quote_fqident(self.table_name), quote_ident(self.name))

class TColumn(TElem):
    """Info about table column."""
    SQL = """
        select a.attname as name,
               quote_ident(a.attname) as qname,
               format_type(a.atttypid, a.atttypmod) as dtype,
               a.attnotnull,
               (select max(char_length(aa.attname))
                  from pg_attribute aa where aa.attrelid = %(oid)s) as maxcol,
               pg_get_serial_sequence(%(fq2name)s, a.attname) as seqname
          from pg_attribute a left join pg_attrdef d
            on (d.adrelid = a.attrelid and d.adnum = a.attnum)
         where a.attrelid = %(oid)s
           and not a.attisdropped
           and a.attnum > 0
         order by a.attnum;
    """
    seqname = None
    def __init__(self, table_name, row):
        self.name = row['name']

        fname = row['qname'].ljust(row['maxcol'] + 3)
        self.column_def = fname + ' ' + row['dtype']
        if row['attnotnull']:
            self.column_def += ' not null'

        self.sequence = None
        if row['seqname']:
            self.seqname = skytools.unquote_fqident(row['seqname'])


class TGPDistKey(TElem):
    """Info about GreenPlum table distribution keys"""
    SQL = """
        select a.attname as name
          from pg_attribute a, gp_distribution_policy p
        where p.localoid = %(oid)s
          and a.attrelid = %(oid)s
          and a.attnum = any(p.attrnums)
        order by a.attnum;
        """
    def __init__(self, table_name, row):
        self.name = row['name']


class TTable(TElem):
    """Info about table only (columns)."""
    type = T_TABLE
    def __init__(self, table_name, col_list, dist_key_list = None):
        self.name = table_name
        self.col_list = col_list
        self.dist_key_list = dist_key_list

    def get_create_sql(self, curs, new_name = None):
        """Generate creation SQL."""
        if not new_name:
            new_name = self.name
        sql = "CREATE TABLE %s (" % quote_fqident(new_name)
        sep = "\n    "
        for c in self.col_list:
            sql += sep + c.column_def
            sep = ",\n    "
        sql += "\n)"
        if self.dist_key_list is not None:
            if self.dist_key_list != []:
                sql += "\ndistributed by(%s)" % ','.join(c.name for c
                                                         in self.dist_key_list)
            else:
                sql += '\ndistributed randomly'

        sql += ";"
        return sql

    def get_drop_sql(self, curs):
        return "DROP TABLE %s;" % quote_fqident(self.name)


class TSeq(TElem):
    """Info about sequence."""
    type = T_SEQUENCE
    SQL = """SELECT *, %(owner)s as "owner" from %(fqname)s """
    def __init__(self, seq_name, row):
        self.name = seq_name
        defn = ''
        self.owner = row['owner']
        if row['increment_by'] != 1:
            defn += ' INCREMENT BY %d' % row['increment_by']
        if row['min_value'] != 1:
            defn += ' MINVALUE %d' % row['min_value']
        if row['max_value'] != 9223372036854775807:
            defn += ' MAXVALUE %d' % row['max_value']
        last_value = row['last_value']
        if row['is_called']:
            last_value += row['increment_by']
            if last_value >= row['max_value']:
                raise Exception('duh, seq passed max_value')
        if last_value != 1:
            defn += ' START %d' % last_value
        if row['cache_value'] != 1:
            defn += ' CACHE %d' % row['cache_value']
        if row['is_cycled']:
            defn += ' CYCLE '
        if self.owner:
            defn += ' OWNED BY %s' % self.owner
        self.defn = defn

    def get_create_sql(self, curs, new_seq_name = None):
        """Generate creation SQL."""

        # we are in table def, forget full def
        if self.owner:
            sql = "ALTER SEQUENCE %s\n  OWNED BY %s;" % (
                    quote_fqident(self.name), self.owner )
            return sql

        name = self.name
        if new_seq_name:
            name = new_seq_name
        sql = 'CREATE SEQUENCE %s %s;' % (quote_fqident(name), self.defn)
        return sql

    def get_drop_sql(self, curs):
        if self.owner:
            return ''
        return 'DROP SEQUENCE %s;' % quote_fqident(self.name)

#
# Main table object, loads all the others
#

class BaseStruct(object):
    """Collects and manages all info about a higher-level db object.

    Allow to issue CREATE/DROP statements about any
    group of elements.
    """
    object_list = []
    def __init__(self, curs, name):
        """Initializes class by loading info about table_name from database."""

        self.name = name
        self.fqname = quote_fqident(name)

    def _load_elem(self, curs, name, args, eclass):
        """Fetch element(s) from db."""
        elem_list = []
        #print "Loading %s, name=%s, args=%s" % (repr(eclass), repr(name), repr(args))
        sql = eclass.get_load_sql(curs.connection.server_version)
        curs.execute(sql % args)
        for row in curs.fetchall():
            elem_list.append(eclass(name, row))
        return elem_list

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

    def get_create_sql(self, objs):
        res = []
        for o in self.object_list:
            if o.type & objs:
                sql = o.get_create_sql(None, None)
                if sql:
                    res.append(sql)
        return "".join(res)

class TableStruct(BaseStruct):
    """Collects and manages all info about table.

    Allow to issue CREATE/DROP statements about any
    group of elements.
    """
    def __init__(self, curs, table_name):
        """Initializes class by loading info about table_name from database."""

        BaseStruct.__init__(self, curs, table_name)

        self.table_name = table_name

        # fill args
        schema, name = skytools.fq_name_parts(table_name)
        args = {
            'schema': schema,
            'table': name,
            'fqname': self.fqname,
            'fq2name': skytools.quote_literal(self.fqname),
            'oid': skytools.get_table_oid(curs, table_name),
            'pg_class_oid': skytools.get_table_oid(curs, 'pg_catalog.pg_class'),
        }

        # load table struct
        self.col_list = self._load_elem(curs, self.name, args, TColumn)
        # if db is GP then read also table distribution keys
        if skytools.exists_table(curs, "pg_catalog.gp_distribution_policy"):
            self.dist_key_list = self._load_elem(curs, self.name, args,
                                                 TGPDistKey)
        else:
            self.dist_key_list = None
        self.object_list = [ TTable(table_name, self.col_list,
                                    self.dist_key_list) ]
        self.seq_list = []

        # load seqs
        for col in self.col_list:
            if col.seqname:
                fqname = quote_fqident(col.seqname)
                owner = self.fqname + '.' + quote_ident(col.name)
                seq_args = { 'fqname': fqname, 'owner': skytools.quote_literal(owner) }
                self.seq_list += self._load_elem(curs, col.seqname, seq_args, TSeq)
        self.object_list += self.seq_list

        # load additional objects
        to_load = [TColumnDefault, TConstraint, TIndex, TTrigger,
                   TRule, TGrant, TOwner, TParent]
        for eclass in to_load:
            self.object_list += self._load_elem(curs, self.name, args, eclass)

    def get_column_list(self):
        """Returns list of column names the table has."""

        res = []
        for c in self.col_list:
            res.append(c.name)
        return res

class SeqStruct(BaseStruct):
    """Collects and manages all info about sequence.

    Allow to issue CREATE/DROP statements about any
    group of elements.
    """
    def __init__(self, curs, seq_name):
        """Initializes class by loading info about table_name from database."""

        BaseStruct.__init__(self, curs, seq_name)

        # fill args
        args = { 'fqname': self.fqname, 'owner': 'null' }

        # load table struct
        self.object_list = self._load_elem(curs, seq_name, args, TSeq)

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

