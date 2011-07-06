
"""PgQ cooperative consumer for Python.
"""

from pgq.consumer import Consumer

__all__ = ['CoopConsumer']

class CoopConsumer(Consumer):
    """Cooperative Consumer base class.

    There will be one dbscript process per subconsumer.

    Config params::
        ## pgq.CoopConsumer

        # name for subconsumer
        subconsumer_name =

        # pgsql interval when to consider parallel subconsumers dead,
        # and take over their unfinished batch
        #subconsumer_timeout = 1 hour
    """

    def __init__(self, service_name, db_name, args):
        """Initialize new subconsumer.

        @param service_name: service_name for DBScript
        @param db_name: name of database for get_database()
        @param args: cmdline args for DBScript
        """

        Consumer.__init__(self, service_name, db_name, args)

        self.subconsumer_name = self.cf.get("subconsumer_name")
        self.subconsumer_timeout = self.cf.get("subconsumer_timeout", "")

    def register_consumer(self):
        """Registration for subconsumer."""

        self.log.info("Registering consumer on source queue")
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq_coop.register_subconsumer(%s, %s, %s)",
                [self.queue_name, self.consumer_name, self.subconsumer_name])
        res = cx.fetchone()[0]
        db.commit()

        return res

    def unregister_consumer(self):
        """Unregistration for subconsumer."""

        self.log.info("Unregistering consumer from source queue")
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq_coop.unregister_subconsumer(%s, %s, %s, 0)",
                    [self.queue_name, self.consumer_name, self.subconsumer_name])
        db.commit()


    def _load_next_batch(self, curs):
        """Allocate next batch. (internal)"""

        if self.subconsumer_timeout:
            q = "select pgq_coop.next_batch(%s, %s, %s, %s)"
            curs.execute(q, [self.queue_name, self.consumer_name, self.subconsumer_name, self.subconsumer_timeout])
        else:
            q = "select pgq_coop.next_batch(%s, %s, %s)"
            curs.execute(q, [self.queue_name, self.consumer_name, self.subconsumer_name])
        return curs.fetchone()[0]

    def _finish_batch(self, curs, batch_id, list):
        """Finish batch. (internal)"""

        self._flush_retry(curs, batch_id, list)
        curs.execute("select pgq_coop.finish_batch(%s)", [batch_id])

