
"""Tools for Python database scripts."""

_symbols = {
    # skytools.timeutil
    'FixedOffsetTimezone': 'skytools.timeutil:FixedOffsetTimezone',
    'parse_iso_timestamp': 'skytools.timeutil:parse_iso_timestamp',
    # skytools.gzlog
    'gzip_append': 'skytools.gzlog:gzip_append',
    # skytools.config
    'Config': 'skytools.config:Config',
    # skytools.quoting
    'quote_bytea_copy': 'skytools.quoting:quote_bytea_copy',
    'quote_bytea_literal': 'skytools.quoting:quote_bytea_literal',
    'quote_bytea_raw': 'skytools.quoting:quote_bytea_raw',
    'quote_copy': 'skytools.quoting:quote_copy',
    'quote_fqident': 'skytools.quoting:quote_fqident',
    'quote_ident': 'skytools.quoting:quote_ident',
    'quote_json': 'skytools.quoting:quote_json',
    'quote_literal': 'skytools.quoting:quote_literal',
    'quote_statement': 'skytools.quoting:quote_statement',
    'db_urldecode': 'skytools.quoting:db_urldecode',
    'db_urlencode': 'skytools.quoting:db_urlencode',
    'unescape': 'skytools.quoting:unescape',
    'unescape_copy': 'skytools.quoting:unescape_copy',
    'unquote_fqident': 'skytools.quoting:unquote_fqident',
    'unquote_ident': 'skytools.quoting:unquote_ident',
    'unquote_literal': 'skytools.quoting:unquote_literal',
    'json_decode': 'skytools.quoting:json_decode',
    'json_encode': 'skytools.quoting:json_encode',
    # skytools.sqltools
    'dbdict': 'skytools.sqltools:dbdict',
    'CopyPipe': 'skytools.sqltools:CopyPipe',
    'DBObject': 'skytools.sqltools:DBObject',
    'DBFunction': 'skytools.sqltools:DBFunction',
    'DBLanguage': 'skytools.sqltools:DBLanguage',
    'DBSchema': 'skytools.sqltools:DBSchema',
    'DBTable': 'skytools.sqltools:DBTable',
    'Snapshot': 'skytools.sqltools:Snapshot',
    'db_install': 'skytools.sqltools:db_install',
    'exists_function': 'skytools.sqltools:exists_function',
    'exists_language': 'skytools.sqltools:exists_language',
    'exists_schema': 'skytools.sqltools:exists_schema',
    'exists_sequence': 'skytools.sqltools:exists_sequence',
    'exists_table': 'skytools.sqltools:exists_table',
    'exists_temp_table': 'skytools.sqltools:exists_temp_table',
    'exists_type': 'skytools.sqltools:exists_type',
    'fq_name': 'skytools.sqltools:fq_name',
    'fq_name_parts': 'skytools.sqltools:fq_name_parts',
    'full_copy': 'skytools.sqltools:full_copy',
    'get_table_columns': 'skytools.sqltools:get_table_columns',
    'get_table_oid': 'skytools.sqltools:get_table_oid',
    'get_table_pkeys': 'skytools.sqltools:get_table_pkeys',
    'installer_apply_file': 'skytools.sqltools:installer_apply_file',
    'installer_find_file': 'skytools.sqltools:installer_find_file',
    'magic_insert': 'skytools.sqltools:magic_insert',
    'mk_delete_sql': 'skytools.sqltools:mk_delete_sql',
    'mk_insert_sql': 'skytools.sqltools:mk_insert_sql',
    'mk_update_sql': 'skytools.sqltools:mk_update_sql',
    # skytools.psycopgwrapper
    'connect_database': 'skytools.psycopgwrapper:connect_database',
    'DBError': 'skytools.psycopgwrapper:DBError',
    # skytools.sockutil
    'set_tcp_keepalive': 'skytools.sockutil:set_tcp_keepalive',
    'set_cloexec': 'skytools.sockutil:set_cloexec',
    'set_nonblocking': 'skytools.sockutil:set_nonblocking',
    # skytools.scripting
    'BaseScript': 'skytools.scripting:BaseScript',
    'daemonize': 'skytools.scripting:daemonize',
    'DBScript': 'skytools.scripting:DBScript',
    'UsageError': 'skytools.scripting:UsageError',
    'signal_pidfile': 'skytools.scripting:signal_pidfile',
    'I_AUTOCOMMIT': 'skytools.scripting:I_AUTOCOMMIT',
    'I_READ_COMMITTED': 'skytools.scripting:I_READ_COMMITTED',
    'I_SERIALIZABLE': 'skytools.scripting:I_SERIALIZABLE',
    # skytools.adminscript
    'AdminScript': 'skytools.adminscript:AdminScript',
    # skytools.parsing
    'parse_acl': 'skytools.parsing:parse_acl',
    'parse_logtriga_sql': 'skytools.parsing:parse_logtriga_sql',
    'parse_pgarray': 'skytools.parsing:parse_pgarray',
    'parse_sqltriga_sql': 'skytools.parsing:parse_sqltriga_sql',
    'parse_statements': 'skytools.parsing:parse_statements',
    'parse_tabbed_table': 'skytools.parsing:parse_tabbed_table',
    'sql_tokenizer': 'skytools.parsing:sql_tokenizer',
    'dedent': 'skytools.parsing:dedent',
    # skytools.dbstruct
    'SeqStruct': 'skytools.dbstruct:SeqStruct',
    'TableStruct': 'skytools.dbstruct:TableStruct',
    'T_ALL': 'skytools.dbstruct:T_ALL',
    'T_CONSTRAINT': 'skytools.dbstruct:T_CONSTRAINT',
    'T_DEFAULT': 'skytools.dbstruct:T_DEFAULT',
    'T_GRANT': 'skytools.dbstruct:T_GRANT',
    'T_INDEX': 'skytools.dbstruct:T_INDEX',
    'T_OWNER': 'skytools.dbstruct:T_OWNER',
    'T_PARENT': 'skytools.dbstruct:T_PARENT',
    'T_PKEY': 'skytools.dbstruct:T_PKEY',
    'T_RULE': 'skytools.dbstruct:T_RULE',
    'T_SEQUENCE': 'skytools.dbstruct:T_SEQUENCE',
    'T_TABLE': 'skytools.dbstruct:T_TABLE',
    'T_TRIGGER': 'skytools.dbstruct:T_TRIGGER',
    # skytools.querybuilder
    'PLPyQuery': 'skytools.querybuilder:PLPyQuery',
    'QueryBuilder': 'skytools.querybuilder:QueryBuilder',
    'PLPyQueryBuilder': 'skytools.querybuilder:PLPyQueryBuilder',
    'plpy_exec': 'skytools.querybuilder:plpy_exec',
    'run_exists': 'skytools.querybuilder:run_exists',
    'run_lookup': 'skytools.querybuilder:run_lookup',
    'run_query': 'skytools.querybuilder:run_query',
    'run_query_row': 'skytools.querybuilder:run_query_row',
    # skytools.utf8
    'safe_utf8_decode': 'skytools.utf8:safe_utf8_decode',
    # skytools.skylog
    'getLogger': 'skytools.skylog:getLogger',
}

__all__ = _symbols.keys()
_symbols['__version__'] = 'skytools.installer_config:package_version'

if 1:
    import skytools.apipkg as _apipkg
    _apipkg.initpkg(__name__, _symbols, {'apipkg': _apipkg})
else:
    from skytools.timeutil import *
    from skytools.gzlog import *
    from skytools.config import *
    from skytools.quoting import *
    from skytools.parsing import *
    from skytools.sqltools import *
    from skytools.psycopgwrapper import *
    from skytools.dbstruct import *
    from skytools.scripting import *
    from skytools.adminscript import *
    from skytools.querybuilder import *
