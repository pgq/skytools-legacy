
/*
 * Per-event temporary data.
 */
struct PgqTriggerEvent {
	const char *table_name;
	const char *queue_name;
	const char *ignore_list;
	const char *pkey_list;

	const char *attkind;
	int attkind_len;

	char op_type;
	bool skip;
	bool backup;

	struct PgqTableInfo *info;

	StringInfo ev_type;
	StringInfo ev_data;
	StringInfo ev_extra1;
	StringInfo ev_extra2;
};
typedef struct PgqTriggerEvent PgqTriggerEvent;

/*
 * Per-table cached info.
 *
 * Can be shared between triggers on same table,
 * so nothing trigger-specific should be stored.
 */
struct PgqTableInfo {
	Oid oid;		/* must be first, used by htab */
	int n_pkeys;		/* number of pkeys */
	const char *pkey_list;	/* pk column name list */
	int *pkey_attno;	/* pk column positions */
	char *table_name;	/* schema-quelified table name */
	int invalid;		/* set if the info was invalidated */
};

/* common.c */
struct PgqTableInfo *pgq_find_table_info(Relation rel);
void pgq_prepare_event(struct PgqTriggerEvent *ev, TriggerData *tg, bool newstyle);
char *pgq_find_table_name(Relation rel);
void pgq_simple_insert(const char *queue_name, Datum ev_type, Datum ev_data, Datum ev_extra1, Datum ev_extra2);
bool pgqtriga_skip_col(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx);
bool pgqtriga_is_pkey(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx);
void pgq_insert_tg_event(PgqTriggerEvent *ev);

bool pgq_is_logging_disabled(void);

/* makesql.c */
int pgqtriga_make_sql(PgqTriggerEvent *ev, TriggerData *tg, StringInfo sql);

/* logutriga.c */
void pgq_urlenc_row(PgqTriggerEvent *ev, TriggerData *tg, HeapTuple row, StringInfo buf);

