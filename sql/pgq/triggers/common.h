struct PgqTriggerEvent {
	const char *table_name;
	const char *queue_name;
	const char *ignore_list;
	const char *pkey_list;
	char op_type;
	bool skip;

	const char *attkind;
	int attkind_len;

	struct PgqTableInfo *info;

	StringInfo ev_type;
	StringInfo ev_data;
	StringInfo ev_extra1;
	StringInfo ev_extra2;
};
typedef struct PgqTriggerEvent PgqTriggerEvent;

void pgq_prepare_event(struct PgqTriggerEvent *ev, TriggerData *tg, bool newstyle);


char *pgq_find_table_name(Relation rel);
void pgq_simple_insert(const char *queue_name, Datum ev_type, Datum ev_data, Datum ev_extra1);

struct PgqColumnInfo {
	int col_no;
	char *col_name;
};

struct PgqTableInfo {
	Oid oid;
	char *table_name;
	const char *pkey_list;
	int n_pkeys;
	int *pkey_attno;
};

struct PgqTableInfo *
pgq_find_table_info(Relation rel);


int pgqtriga_make_sql(PgqTriggerEvent *ev, TriggerData *tg, StringInfo sql);

bool pgqtriga_skip_col(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx);
bool pgqtriga_is_pkey(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx);




