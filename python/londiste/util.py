
import skytools
import londiste.handler

__all__ = ['handler_allows_copy', 'find_copy_source']

def handler_allows_copy(table_attrs):
    """Decide if table is copyable based on attrs."""
    if not table_attrs:
        return True
    attrs = skytools.db_urldecode(table_attrs)
    hstr = attrs.get('handler', '')
    p = londiste.handler.build_handler('unused.string', hstr, None)
    return p.needs_table()

def find_copy_source(script, queue_name, copy_table_name, node_name, node_location):
    """Find source node for table.

    @param script: DbScript
    @param queue_name: name of the cascaded queue
    @param copy_table_name: name of the table
    @param node_name: target node name
    @param node_location: target node location
    @returns (node_name, node_location, downstream_worker_name) of source node
    """

    # None means no steps upwards were taken, so local consumer is worker
    worker_name = None

    while 1:
        src_db = script.get_database('_source_db', connstr = node_location, autocommit = 1)
        src_curs = src_db.cursor()

        q = "select * from pgq_node.get_node_info(%s)"
        src_curs.execute(q, [queue_name])
        info = src_curs.fetchone()
        if info['ret_code'] >= 400:
            raise skytools.UsageError("Node does not exists")

        script.log.info("Checking if %s can be used for copy", info['node_name'])

        q = "select table_name, local, table_attrs from londiste.get_table_list(%s) where table_name = %s"
        src_curs.execute(q, [queue_name, copy_table_name])
        got = False
        for row in src_curs.fetchall():
            tbl = row['table_name']
            if tbl != copy_table_name:
                continue
            if not row['local']:
                script.log.debug("Problem: %s is not local", tbl)
                continue
            if not handler_allows_copy(row['table_attrs']):
                script.log.debug("Problem: %s handler does not store data [%s]", tbl, row['table_attrs'])
                continue
            script.log.debug("Good: %s is usable", tbl)
            got = True
            break

        script.close_database('_source_db')

        if got:
            script.log.info("Node %s seems good source, using it", info['node_name'])
            return node_name, node_location, worker_name

        if info['node_type'] == 'root':
            raise skytools.UsageError("Found root and no source found")

        # walk upwards
        node_name = info['provider_node']
        node_location = info['provider_location']
        worker_name = info['worker_name']

