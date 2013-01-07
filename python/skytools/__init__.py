
"""Tools for Python database scripts."""

_symbols = {
    # skytools.adminscript
    'AdminScript': 'skytools.adminscript:AdminScript',
    # skytools.config
    'Config': 'skytools.config:Config',
    # skytools.dbservice
    'DBService': 'skytools.dbservice:DBService',
    'ServiceContext': 'skytools.dbservice:ServiceContext',
    'TableAPI': 'skytools.dbservice:TableAPI',
    'get_record': 'skytools.dbservice:get_record',
    'get_record_list': 'skytools.dbservice:get_record_list',
    'make_record': 'skytools.dbservice:make_record',
    'make_record_array': 'skytools.dbservice:make_record_array',
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
    # skytools.fileutil
    'signal_pidfile': 'skytools.fileutil:signal_pidfile',
    'write_atomic': 'skytools.fileutil:write_atomic',
    # skytools.gzlog
    'gzip_append': 'skytools.gzlog:gzip_append',
    # skytools.hashtext
    'hashtext_old': 'skytools.hashtext:hashtext_old',
    'hashtext_new': 'skytools.hashtext:hashtext_new',
    # skytools.natsort
    'natsort': 'skytools.natsort:natsort',
    'natsort_icase': 'skytools.natsort:natsort_icase',
    'natsorted': 'skytools.natsort:natsorted',
    'natsorted_icase': 'skytools.natsort:natsorted_icase',
    'natsort_key': 'skytools.natsort:natsort_key',
    'natsort_key_icase': 'skytools.natsort:natsort_key_icase',
    # skytools.parsing
    'dedent': 'skytools.parsing:dedent',
    'hsize_to_bytes': 'skytools.parsing:hsize_to_bytes',
    'parse_acl': 'skytools.parsing:parse_acl',
    'parse_logtriga_sql': 'skytools.parsing:parse_logtriga_sql',
    'parse_pgarray': 'skytools.parsing:parse_pgarray',
    'parse_sqltriga_sql': 'skytools.parsing:parse_sqltriga_sql',
    'parse_statements': 'skytools.parsing:parse_statements',
    'parse_tabbed_table': 'skytools.parsing:parse_tabbed_table',
    'sql_tokenizer': 'skytools.parsing:sql_tokenizer',
    # skytools.psycopgwrapper
    'connect_database': 'skytools.psycopgwrapper:connect_database',
    'DBError': 'skytools.psycopgwrapper:DBError',
    'I_AUTOCOMMIT': 'skytools.psycopgwrapper:I_AUTOCOMMIT',
    'I_READ_COMMITTED': 'skytools.psycopgwrapper:I_READ_COMMITTED',
    'I_REPEATABLE_READ': 'skytools.psycopgwrapper:I_REPEATABLE_READ',
    'I_SERIALIZABLE': 'skytools.psycopgwrapper:I_SERIALIZABLE',
    # skytools.querybuilder
    'PLPyQuery': 'skytools.querybuilder:PLPyQuery',
    'PLPyQueryBuilder': 'skytools.querybuilder:PLPyQueryBuilder',
    'QueryBuilder': 'skytools.querybuilder:QueryBuilder',
    'plpy_exec': 'skytools.querybuilder:plpy_exec',
    'run_exists': 'skytools.querybuilder:run_exists',
    'run_lookup': 'skytools.querybuilder:run_lookup',
    'run_query': 'skytools.querybuilder:run_query',
    'run_query_row': 'skytools.querybuilder:run_query_row',
    # skytools.quoting
    'db_urldecode': 'skytools.quoting:db_urldecode',
    'db_urlencode': 'skytools.quoting:db_urlencode',
    'json_decode': 'skytools.quoting:json_decode',
    'json_encode': 'skytools.quoting:json_encode',
    'make_pgarray': 'skytools.quoting:make_pgarray',
    'quote_bytea_copy': 'skytools.quoting:quote_bytea_copy',
    'quote_bytea_literal': 'skytools.quoting:quote_bytea_literal',
    'quote_bytea_raw': 'skytools.quoting:quote_bytea_raw',
    'quote_copy': 'skytools.quoting:quote_copy',
    'quote_fqident': 'skytools.quoting:quote_fqident',
    'quote_ident': 'skytools.quoting:quote_ident',
    'quote_json': 'skytools.quoting:quote_json',
    'quote_literal': 'skytools.quoting:quote_literal',
    'quote_statement': 'skytools.quoting:quote_statement',
    'unescape': 'skytools.quoting:unescape',
    'unescape_copy': 'skytools.quoting:unescape_copy',
    'unquote_fqident': 'skytools.quoting:unquote_fqident',
    'unquote_ident': 'skytools.quoting:unquote_ident',
    'unquote_literal': 'skytools.quoting:unquote_literal',
    # skytools.scripting
    'BaseScript': 'skytools.scripting:BaseScript',
    'daemonize': 'skytools.scripting:daemonize',
    'DBScript': 'skytools.scripting:DBScript',
    'UsageError': 'skytools.scripting:UsageError',
    # skytools.skylog
    'getLogger': 'skytools.skylog:getLogger',
    # skytools.sockutil
    'set_cloexec': 'skytools.sockutil:set_cloexec',
    'set_nonblocking': 'skytools.sockutil:set_nonblocking',
    'set_tcp_keepalive': 'skytools.sockutil:set_tcp_keepalive',
    # skytools.sqltools
    'dbdict': 'skytools.sqltools:dbdict',
    'CopyPipe': 'skytools.sqltools:CopyPipe',
    'DBFunction': 'skytools.sqltools:DBFunction',
    'DBLanguage': 'skytools.sqltools:DBLanguage',
    'DBObject': 'skytools.sqltools:DBObject',
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
    'exists_view': 'skytools.sqltools:exists_view',
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
    # skytools.timeutil
    'FixedOffsetTimezone': 'skytools.timeutil:FixedOffsetTimezone',
    'parse_iso_timestamp': 'skytools.timeutil:parse_iso_timestamp',
    # skytools.utf8
    'safe_utf8_decode': 'skytools.utf8:safe_utf8_decode',
}

__all__ = _symbols.keys()
_symbols['__version__'] = 'skytools.installer_config:package_version'

if 1:
    # lazy-import exported vars
    import skytools.apipkg as _apipkg
    _apipkg.initpkg(__name__, _symbols, {'apipkg': _apipkg})
elif 1:
    # import everything immediately
    from skytools.quoting import *
    from skytools.sqltools import *
    from skytools.scripting import *

    from skytools.adminscript import *
    from skytools.config import *
    from skytools.dbservice import *
    from skytools.dbstruct import *
    from skytools.fileutil import *
    from skytools.gzlog import *
    from skytools.hashtext import *
    from skytools.natsort import *
    from skytools.parsing import *
    from skytools.psycopgwrapper import *
    from skytools.querybuilder import *
    from skytools.skylog import *
    from skytools.sockutil import *
    from skytools.timeutil import *
    from skytools.utf8 import *
else:
    from skytools.quoting import *
    from skytools.sqltools import *
    from skytools.scripting import *

    # compare apipkg list to submodule exports
    xall = []
    import skytools.adminscript
    import skytools.config
    import skytools.dbservice
    import skytools.dbstruct
    import skytools.fileutil
    import skytools.gzlog
    import skytools.hashtext
    import skytools.natsort
    import skytools.parsing
    import skytools.psycopgwrapper
    import skytools.querybuilder
    import skytools.quoting
    import skytools.scripting
    import skytools.skylog
    import skytools.sockutil
    import skytools.sqltools
    import skytools.timeutil
    import skytools.utf8
    xall = (  skytools.adminscript.__all__
            + skytools.config.__all__
            + skytools.dbservice.__all__
            + skytools.dbstruct.__all__
            + skytools.fileutil.__all__
            + skytools.gzlog.__all__
            + skytools.hashtext.__all__
            + skytools.natsort.__all__
            + skytools.parsing.__all__
            + skytools.psycopgwrapper.__all__
            + skytools.querybuilder.__all__
            + skytools.quoting.__all__
            + skytools.scripting.__all__
            + skytools.skylog.__all__
            + skytools.sockutil.__all__
            + skytools.sqltools.__all__
            + skytools.timeutil.__all__
            + skytools.utf8.__all__ )
    for k in __all__:
        if k not in xall:
            print '%s missing from __all__?' % k
    for k in xall:
        if k not in __all__:
            print '%s missing from top-level?' % k
