#include <libpq-fe.h>
#include <ruby.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

/* Code Map
 *
 * struct AltPg_Db .... DBI::DBD::AltPg::Database < DBI::BaseDatabase
 * struct AltPg_St .... DBI::DBD::AltPg::Statement < DBI::BaseStatement
 *
 * AltPg_Db_s_foo ..... DBI::DBD::AltPg::Database#foo (singleton method)
 * AltPg_Db_bar ....... DBI::DBD::AltPg::Database#bar
 *
 * altpg_db_* ......... Internal helper operating on `struct AltPg_Db'
 * altpg_st_* ......... ""                           `struct AltPg_St'
 *
 *
 */

#ifndef RSTRING_PTR
#define RSTRING_PTR(string) RSTRING(string)->ptr
#endif
#ifndef RSTRING_LEN
#define RSTRING_LEN(string) RSTRING(string)->len
#endif

static VALUE rbx_mAltPg;  /* module DBI::DBD::AltPg           */
static VALUE rbx_cDb;     /* class DBI::DBD::AltPg::Database  */
static VALUE rbx_cSt;     /* class DBI::DBD::AltPg::Statement */

static ID id_translate_parameters;
static VALUE sym_type_name;
static VALUE sym_dbi_type;

struct altpg_params {
	int nparams;
	Oid *param_types;
	char **param_values;
	int *param_lengths;
	int *param_formats;
};

struct AltPg_Db {
	PGconn *conn;
	unsigned long serial;  /* pstmt name suffix; may wrap */
};

struct AltPg_St {
	PGconn *conn;              /* NULL if finished                       */
	PGresult *res;             /* non-NULL if executed and not cancelled */
	int prepared;              /* non-zero if prepared                   */
	struct altpg_params params;
	unsigned int nfields;
	unsigned int ntuples;
	unsigned int row_number;
};

/* ==== Helper functions ================================================== */

static VALUE
raise_dbi_internal_error(const char *msg)
{
	VALUE err = rb_str_new2(msg);

	rb_exc_raise(rb_class_new_instance(1, &err,
	                                   rb_path2class("DBI::InternalError")));
}

static VALUE
raise_dbi_database_error(const char *msg, const char *sqlstate)
{
	VALUE args[3];

	args[0] = rb_str_new2(msg);       /* message */
	args[1] = Qnil;                   /* err     */
	args[2] = rb_str_new2(sqlstate);  /* state   */

	rb_exc_raise(rb_class_new_instance(3, args,
	                                   rb_path2class("DBI::DatabaseError")));
}

static int
altpg_thread_select(int nfd, fd_set *rfds, fd_set *wfds, struct timeval *tv)
{
	int r = rb_thread_select(nfd, rfds, wfds, NULL, tv);
	if (r > 0) return r;

	if (r < 0) raise_dbi_internal_error("Internal select() error");
	if (NULL == tv)
		raise_dbi_internal_error("Internal select() impossibly timed out");

	return r;
}

static int
fd_await_readable(int fd, struct timeval *tv)
{
	fd_set fds;

	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	return altpg_thread_select(fd + 1, &fds, NULL, tv);
}

static int
fd_await_writeable(int fd, struct timeval *tv)
{
	fd_set fds;

	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	return altpg_thread_select(fd + 1, NULL, &fds, tv);
}

static void
altpg_params_initialize(struct altpg_params *ap, int nparams)
{
	/* FIXME - we can skip setting param_lengths if text protocol */
	ap->nparams = nparams;
	ap->param_types = ALLOC_N(Oid, nparams);
	MEMZERO(ap->param_types, char *, nparams);
	ap->param_values = ALLOC_N(char *, nparams);
	MEMZERO(ap->param_values, char *, nparams);
	ap->param_lengths = ALLOC_N(int, nparams);
	MEMZERO(ap->param_lengths, int, nparams);
	ap->param_formats = ALLOC_N(int, nparams);
	MEMZERO(ap->param_formats, int, nparams);
}

/* Map an array of strings (or nils) to a struct altpg_params,
 * allocating the struct if necessary.
 *
 * CAUTION:  no ruby type checking!
 * FIXME:    This is DBI's register_conversion, sort of.
 */
static struct altpg_params *
altpg_params_from_ary(struct altpg_params *ap, VALUE ary)
{
  int i;

  for (i = 0; i < ap->nparams; ++i) {
    VALUE elt = rb_ary_entry(ary, i);
		/* elt := [ value, oid, fmt ] */

    if (Qnil == elt) {
			/* Hmm, a param was not bound.  Treat this as an implicit NULL */
			/* XXX - should throw internal error ? */
      ap->param_types[i]   = (Oid)0;
      ap->param_values[i]  = NULL;
      ap->param_lengths[i] = 0;
			ap->param_formats[i] = 0;
    } else {
			VALUE val = rb_ary_entry(elt, 0);

      ap->param_types[i]   = NUM2INT(rb_ary_entry(elt, 1));
			if (Qnil == val) {
				ap->param_values[i]  = NULL;
				ap->param_lengths[i] = 0;
			} else {
				SafeStringValue(val);
				ap->param_values[i]  = RSTRING_PTR(val);
				ap->param_lengths[i] = RSTRING_LEN(val);
			}
			ap->param_formats[i] = NUM2INT(rb_ary_entry(elt, 2));
    }
	}

  return ap;
}

static void
altpg_params_clear(struct altpg_params *ap)
{
	if (NULL == ap) return;
	if (ap->param_values) {
		xfree(ap->param_types);
		xfree(ap->param_values);
		xfree(ap->param_lengths);
		xfree(ap->param_formats);
	}
	MEMZERO(ap, struct altpg_params, 1);
}

PGresult *
async_PQgetResult(PGconn *conn)
{
	PGresult *tmp = NULL;
	PGresult *res = NULL;
	int fd;

	fd = PQsocket(conn);

	/* Assumption:  we are called immediately after a PQsend(),
	 *              so we're expecting new, un-PQconsume'd data to
	 *              arrive
	 */
	fd_await_readable(fd, NULL);

	/* ruby-pg-0.8.0 pgconn_block() */
	PQconsumeInput(conn);
	while (PQisBusy(conn)) {
		fd_await_readable(fd, NULL);
		PQconsumeInput(conn);
	}

	/* ruby-pg-0.8.0 pgconn_get_last_result(), except we PQclear as needed */
	while (tmp = PQgetResult(conn)) {
		if (res) PQclear(res);
		res = tmp;
	}

	/* ruby-pg-0.8.0 pgresult_check() */
	switch (PQresultStatus(res)) {
	case PGRES_TUPLES_OK:
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
	case PGRES_EMPTY_QUERY:
	case PGRES_COMMAND_OK:
		break;
	case PGRES_BAD_RESPONSE:
	case PGRES_FATAL_ERROR:
	case PGRES_NONFATAL_ERROR:
		{
			VALUE args[3];

			args[0] = rb_str_new2(PQresultErrorMessage(res));
			args[1] = Qnil;
			args[2] = rb_str_new2(PQresultErrorField(res, PG_DIAG_SQLSTATE));

			PQclear(res);

			rb_exc_raise(rb_class_new_instance(3,
			                                   args,
			                                   rb_path2class("DBI::DatabaseError")));
			break; /* Not reached */
		}
  default:
		raise_dbi_internal_error("Unknown/unexpected PQresultStatus");
	}

	return res;
}

static void
raise_PQsend_error(PGconn *conn)
{
	rb_raise(rb_path2class("DBI::DatabaseError"), PQerrorMessage(conn));
}

static int
altpg_db_in_transaction(struct AltPg_Db *db)
{
	int in_trans = 0;
	switch (PQtransactionStatus(db->conn)) {
	case PQTRANS_INTRANS:
	case PQTRANS_INERROR:
		in_trans = 1;
		break;
	case PQTRANS_IDLE:
		in_trans = 0;
		break;
	case PQTRANS_ACTIVE:
		/* Should Not Occur. (tm)
		 * We were called between a PQsend* and (async) PQgetResult, which
		 * is contrary to our design assumptions.
		 */
		rb_raise(rb_path2class("DBI::InternalError"),
		         "PQTRANS_ACTIVE unexpectedly encountered");
		break; /* Not reached */
	case PQTRANS_UNKNOWN:
		/* XXX force SQLSTATE to 08000 ? */
		rb_raise(rb_path2class("DBI::DatabaseError"),
		         "Bad connection");
		break; /* Not reached */
	default:
		rb_raise(rb_path2class("DBI::InternalError"),
		         "Fell through PQtransactionStatus code switch!");
	}

	return in_trans;
}

static struct AltPg_St *
altpg_st_get_unfinished(VALUE self)
{
	struct AltPg_St *st;

	Data_Get_Struct(self, struct AltPg_St, st);
	if (NULL == st->conn) {
		rb_raise(rb_path2class("DBI::ProgrammingError"), "method called on finished statement handle");
	}
	return st;
}

/* Clear any in-progress query, noop if redundant.  (internal) */
static void
altpg_st_cancel(struct AltPg_St *st)
{
	if (st->res) {
		PQclear(st->res);            /* Undo any execute()   */
		st->res = NULL;
		st->ntuples = 0;

		st->row_number = 0;          /* Erase any #fetch     */
	}

	if (st->params.nparams > 0) {  /* Undo any #bind_param */
		MEMZERO(st->params.param_values, char *, st->params.nparams);
		MEMZERO(st->params.param_lengths, int, st->params.nparams);
	}
}

/* ==== Class methods ===================================================== */

static void
AltPg_Db_s_free(struct AltPg_Db *db)
{
	if (NULL != db && NULL != db->conn) {
		PQfinish(db->conn);
		db->conn = NULL;
	}
}

static VALUE
AltPg_Db_s_alloc(VALUE klass)
{
	struct AltPg_Db *db = ALLOC(struct AltPg_Db);
	memset(db, '\0', sizeof(struct AltPg_Db));
	return Data_Wrap_Struct(klass, 0, AltPg_Db_s_free, db);
}

/* ==== Instance methods ================================================== */

static void
altpg_db_pq_connect_start(struct AltPg_Db *db, const char *conninfo)
{
	if (db->conn)
		raise_dbi_internal_error("Attempt to re-connect already-connected AltPg::Database object");

	db->conn = PQconnectStart(conninfo);
	if (NULL == db->conn) {
		raise_dbi_internal_error("PQconnectionStart: unable to allocate libPQ structures");
	} else if (PQstatus(db->conn) == CONNECTION_BAD) {
		raise_dbi_database_error("PQconnectionStart: connection failed", "08000");
	}
}

static void
altpg_db_pq_connect_poll(struct AltPg_Db *db)
{
	PostgresPollingStatusType pollstat;
	int fd;

	fd = PQsocket(db->conn);

	for (pollstat  = PGRES_POLLING_WRITING;
	     pollstat != PGRES_POLLING_OK;
			 pollstat  = PQconnectPoll(db->conn)) {
		switch (pollstat) {
		case PGRES_POLLING_OK:
			break; /* all done */
		case PGRES_POLLING_FAILED:
			raise_dbi_database_error("PQconnectPoll: bad connection", "08000");
			break; /* not reached */
		case PGRES_POLLING_READING:
			fd_await_readable(fd, NULL);
			break;
		case PGRES_POLLING_WRITING:
			fd_await_writeable(fd, NULL);
			break;
		default:
			raise_dbi_internal_error("PQconnectPoll: non-sensical PGRES_POLLING status encountered");
		}
	}
}

/* call-seq:
 *   db.pq_connect_db(conninfo) -> db
 *
 * Connect to the PostgreSQL server specified by +conninfo+.
 */
static VALUE
AltPg_Db_pq_connect_db(VALUE self, VALUE conninfo)
{
	/* We don't PQfinish() on error - object finalization will take
	 * care of that. */
	struct AltPg_Db *db;

	Check_SafeStr(conninfo);
	Data_Get_Struct(self, struct AltPg_Db, db);

  altpg_db_pq_connect_start(db, RSTRING_PTR(conninfo));
	altpg_db_pq_connect_poll(db);

	switch (PQprotocolVersion(db->conn)) {
	case 3:
		break;
	case 2:
		raise_dbi_database_error("DBD::AltPg requires protocol version >= 3",
					                   "08P01");
		break; /* Not reached */
	default:
		raise_dbi_internal_error("Unexpected protocol version");
		break; /* Not reached */
	}

	return self;
}

static VALUE
AltPg_Db_disconnect(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	if (db->conn) {
		PQfinish(db->conn);
		db->conn = NULL;
	}

	return Qnil;
}
/* Unprepared query execution.  (internal) */
static void
altpg_db_simple_exec(struct AltPg_Db *db, const char *query)
{
	PGresult *res;

	if (!PQsendQueryParams(db->conn, query, 0, NULL, NULL, NULL, NULL, 1))
		raise_PQsend_error(db->conn);
	res = async_PQgetResult(db->conn);
	PQclear(res);
}

/* call-seq:
 *   dbh.rollback -> nil
 *
 * If AutoCommit is false, this method commits the current transaction and
 * implicitly begins a new one.  If AutoCommit is true, this method does
 * nothing.
 */
static VALUE
AltPg_Db_commit(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	if (altpg_db_in_transaction(db)) { /* Implies self['AutoCommit'] := false */
		altpg_db_simple_exec(db, "COMMIT");
		altpg_db_simple_exec(db, "BEGIN");
	}

	return Qnil;
}

/* call-seq:
 *   dbh.rollback -> nil
 *
 * If AutoCommit is false, this method rolls back the current transaction and
 * implicitly begins a new one.  If AutoCommit is true, this method does
 * nothing.
 */
static VALUE
AltPg_Db_rollback(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	if (altpg_db_in_transaction(db)) { /* Implies self['AutoCommit'] := false */
		altpg_db_simple_exec(db, "ROLLBACK");
		altpg_db_simple_exec(db, "BEGIN");
	}

	return Qnil;
}

static VALUE
AltPg_Db_dbname(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);

	/* FIXME - internalerror if NULL == db->conn */
	return rb_tainted_str_new2(PQdb(db->conn));
}

static VALUE
AltPg_Db_in_transaction_p(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	return altpg_db_in_transaction(db) ? Qtrue : Qfalse;
}

static VALUE
AltPg_Db_pq_socket(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	return INT2FIX(PQsocket(db->conn));
}

/* call-seq:
 *  db.pq_notifies(timeout) -> [notify, pid] or nil
 *  db.pq_notifies(timeout) { |notify, pid| block }
 *
 *  Fetch the next pending NOTIFY or, if a block is given,
 *  all pending NOTIFYs, waiting up to +timeout+ for the
 *  first NOTIFY to arrive.  +timeout+ is interpreted as
 *  for Kernel.select().
 */
static VALUE
AltPg_Db_pq_notifies(VALUE self, VALUE timeout)
{
	extern struct timeval rb_time_interval(VALUE);

	struct AltPg_Db *db;
	struct pgNotify *notification;
	VALUE ary;

	Data_Get_Struct(self, struct AltPg_Db, db);

	PQconsumeInput(db->conn);
	notification = PQnotifies(db->conn);
	if (! notification) {
		struct timeval patience, *tv = NULL;

		if (! NIL_P(timeout)) {
			patience = rb_time_interval(timeout);
			tv = &patience;
		}

		fd_await_readable(PQsocket(db->conn), tv);
		PQconsumeInput(db->conn);
		notification = PQnotifies(db->conn);
	}
	if (! notification) return Qnil;

	ary = rb_ary_new2(2);
	rb_ary_store(ary, 0, rb_str_new2(notification->relname));
	rb_ary_store(ary, 1, INT2FIX(notification->be_pid));

	if (!rb_block_given_p()) return ary;

	do { rb_yield(ary); } while (notification = PQnotifies(db->conn));

	return Qnil;
}

/* ---------- DBI::DBD::Pq::Statement ------------------------------------- */

static void
AltPg_St_s_free(struct AltPg_St *st)
{
	if (NULL == st) return;
	if (st->res) PQclear(st->res);
	altpg_params_clear(&st->params);
	xfree(st);
}

static VALUE
AltPg_St_s_alloc(VALUE klass)
{
	struct AltPg_St *st = ALLOC(struct AltPg_St);
	MEMZERO(st, struct AltPg_St, 1);
	return Data_Wrap_Struct(klass, 0, AltPg_St_s_free, st);
}

/* FIXME:  paramTypes ? */
static VALUE
AltPg_St_initialize(VALUE self, VALUE parent, VALUE query, VALUE param_count, VALUE preparable)
{
	struct AltPg_Db *db;
	struct AltPg_St *st;
	VALUE plan;
	int nparams;

	Data_Get_Struct(self, struct AltPg_St, st); /* later, our own data_get */

	if (!rb_obj_is_instance_of(parent, rbx_cDb)) {
		rb_raise(rb_eTypeError,
		         "Expected argument of type DBI::DBD::AltPg::Database");
	}
	Data_Get_Struct(parent, struct AltPg_Db, db);
	if (!db || !db->conn) {
		rb_raise(rb_path2class("DBI::InternalError"),
		         "Attempt to create AltPg::Statement from invalid AltPg::Database (db %p, db->conn %p)", db, db ? db->conn : NULL);
	}
	st->conn = db->conn;

	SafeStringValue(query);
	rb_iv_set(self, "@query", query);

	nparams = NUM2INT(param_count);
	if (nparams > 0) {
		altpg_params_initialize(&st->params, nparams);
	}

	/* FIXME - we ignore "preparability" for now ... */
	plan = rb_str_new2("ruby-dbi:altpg:");
	rb_str_append(plan, rb_obj_as_string(ULONG2NUM(db->serial++)));
	rb_iv_set(self, "@plan", plan);

	rb_iv_set(self, "@type_map", rb_iv_get(parent, "@type_map"));
	rb_iv_set(self, "@params", rb_ary_new());

	return self;
}

static VALUE
AltPg_St_cancel(VALUE self)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);
	altpg_st_cancel(st);
	rb_ary_clear(rb_iv_get(self, "@params"));

	return Qnil;
}

static VALUE
AltPg_St_execute(VALUE self)
{
	struct AltPg_St *st;
	VALUE iv_params;
	VALUE iv_plan;
	int send_ok;

	st = altpg_st_get_unfinished(self);
	altpg_st_cancel(st);

	iv_params = rb_iv_get(self, "@params");
	iv_plan   = rb_iv_get(self, "@plan");

	if (RARRAY_LEN(iv_params) != st->params.nparams) {
		int nsupplied = (int)RARRAY_LEN(iv_params);
		/* Let's be charitable and give the user an opportunity to recover.
		 * Presently in the DBI, there's no way to clear bound parameters.
		 */
		rb_ary_clear(iv_params);
		rb_raise(rb_path2class("DBI::ProgrammingError"),
		                       "%d parameters supplied, but prepared statement \"%s\" requires %d",
		                       nsupplied,
		                       RSTRING_PTR(iv_plan),
		                       st->params.nparams);
	}

	altpg_params_from_ary(&st->params, iv_params);

	if (! st->prepared) {
		PGresult *res;

		if (!PQsendPrepare(st->conn,
					RSTRING_PTR(iv_plan),
					RSTRING_PTR(rb_iv_get(self, "@query")),
					st->params.nparams,
					st->params.param_types)) {
			raise_PQsend_error(st->conn);
		}
		res = async_PQgetResult(st->conn);
		PQclear(res);
		st->prepared = 1;
	}

	send_ok = PQsendQueryPrepared(st->conn,
	                              RSTRING_PTR(iv_plan),
	                              st->params.nparams,
	                              st->params.param_values,
	                              st->params.param_lengths,
	                              st->params.param_formats,
	                              1);

	if (!send_ok) {
			raise_PQsend_error(st->conn);
	}
	st->res = async_PQgetResult(st->conn);
	st->nfields = PQnfields(st->res);
	st->ntuples = PQntuples(st->res);

	return Qnil;
}

static VALUE
AltPg_St_finish(VALUE self)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);

	altpg_st_cancel(st);

	if (st->conn && st->prepared) {
		VALUE plan = rb_iv_get(self, "@plan");
		VALUE deallocate_fmt = rb_str_new2("DEALLOCATE \"%s\"");
		PGresult *res;

		if (!PQsendQuery(st->conn, STR2CSTR(rb_str_format(1, &plan, deallocate_fmt)))) {
			raise_PQsend_error(st->conn);
		}
		res = async_PQgetResult(st->conn);
		PQclear(res);
	}

	return Qnil;
}

static VALUE
AltPg_St_fetch(VALUE self)
{
	struct AltPg_St *st;
	VALUE ret;
	/* XXX - store @row rather than making anew each time? */
	int i;

	st = altpg_st_get_unfinished(self);

	if (!st->res || st->row_number >= st->ntuples) {
		return Qnil;
	}

	ret = rb_ary_new2(st->nfields);
	for (i = 0; i < st->nfields; ++i) {
		VALUE val = PQgetisnull(st->res, st->row_number, i)
		            ? Qnil
								: rb_str_new(PQgetvalue(st->res, st->row_number, i),
		                         PQgetlength(st->res, st->row_number, i));
		//printf("\trow %d, col %d. bytes of len %d%s\n",
		//       st->row_number, i, PQgetlength(st->res, st->row_number, i),
		//			 PQgetisnull(st->res, st->row_number, i) ? "(NULL)" : "");
		rb_ary_store(ret, i, val);
	}

	st->row_number++;
	return ret;
}

static VALUE
AltPg_St_rows(VALUE self)
{
	struct AltPg_St *st;
	char *rows;

	st = altpg_st_get_unfinished(self);
	rows = PQcmdTuples(st->res);

	return rows[0] ? rb_Integer(rb_str_new2(rows)) : Qnil;
}

/*
 * * name:: the name of the column.
 * * type:: This is not a field name in itself. You have two options:
 *   * type_name:: The name of the type as returned by the database
 *   * dbi_type:: A DBI::Type-conforming class that can be used to convert to a native type.
 * * precision:: the precision (generally length) of the column
 * * scale:: the scale (generally a secondary attribute to precision
 *   that helps indicate length) of the column
 *
 */
static VALUE
AltPg_St_column_info(VALUE self)
{
	struct AltPg_St *st;

	// XXX - @column_info ||= (...).freeze
	// XXX - module-level strings for keys?
	VALUE ret;
	VALUE iv_type_map;
	int i;

	st = altpg_st_get_unfinished(self);
	ret = rb_ary_new2(st->nfields);
	iv_type_map = rb_iv_get(self, "@type_map");

	for (i = 0; i < st->nfields; ++i) {
		VALUE col = rb_hash_new();
		VALUE type_map_entry;
		int typmod, typlen;
		VALUE precision = Qnil;
		VALUE scale = Qnil;
		Oid type_oid;

		rb_hash_aset(col, rb_str_new2("name"),
		                  rb_str_new2(PQfname(st->res, i)));

		type_oid = PQftype(st->res, i);
		type_map_entry = rb_hash_aref(iv_type_map, INT2FIX(type_oid));

		// col['type_name'] = @type_map[16][:type_name]
		rb_hash_aset(col, rb_str_new2("type_name"),
		                  rb_hash_aref(type_map_entry, sym_type_name));
		// col['dbi_type'] = @type_map[16][:dbi_type]
		rb_hash_aset(col, rb_str_new2("dbi_type"),
		                  rb_hash_aref(type_map_entry, sym_dbi_type));
		/*
		printf("\tcolumn \"%s\", Oid %lu, type %s\n",
					 PQfname(st->res, i),
		       type_oid,
		       RSTRING_PTR(rb_hash_aref(type_map_entry, sym_type_name)));
		*/

		typmod = PQfmod(st->res, i);
		typlen = PQfsize(st->res, i);

		if (typlen > 0) {
			precision = INT2FIX(typlen);
		} else if (typmod > 0xffff) {
			precision = INT2FIX(typmod >> 16);
			scale     = INT2FIX((typmod & 0xffff) - 4);
		} else if (typmod > 4) {
			precision = INT2FIX(typmod - 4);
		}

		rb_hash_aset(col, rb_str_new2("precision"), precision);
		rb_hash_aset(col, rb_str_new2("scale"), scale);

		rb_ary_store(ret, i, col);
	}

	return ret;
}

void
Init_pq()
{
	rbx_mAltPg = rb_path2class("DBI::DBD::AltPg");
	rbx_cDb    = rb_define_class_under(rbx_mAltPg, "Database", rb_path2class("DBI::BaseDatabase"));
	rbx_cSt    = rb_define_class_under(rbx_mAltPg, "Statement",
	                                   rb_path2class("DBI::BaseStatement"));

	rb_define_alloc_func(rbx_cDb, AltPg_Db_s_alloc);
	rb_define_private_method(rbx_cDb, "pq_connect_db", AltPg_Db_pq_connect_db, 1);
	rb_define_private_method(rbx_cDb, "pq_socket", AltPg_Db_pq_socket, 0);
	rb_define_private_method(rbx_cDb, "pq_notifies", AltPg_Db_pq_notifies, 1);
	rb_define_method(rbx_cDb, "in_transaction?", AltPg_Db_in_transaction_p, 0);
	rb_define_method(rbx_cDb, "database_name", AltPg_Db_dbname, 0);
	rb_define_method(rbx_cDb, "disconnect", AltPg_Db_disconnect, 0);
	rb_define_method(rbx_cDb, "commit", AltPg_Db_commit, 0);
	rb_define_method(rbx_cDb, "rollback", AltPg_Db_rollback, 0);

	rb_define_alloc_func(rbx_cSt, AltPg_St_s_alloc);
	rb_define_method(rbx_cSt, "initialize", AltPg_St_initialize, 4);
	rb_define_method(rbx_cSt, "cancel", AltPg_St_cancel, 0);
	rb_define_method(rbx_cSt, "finish", AltPg_St_finish, 0);
	rb_define_method(rbx_cSt, "execute", AltPg_St_execute, 0);
	rb_define_method(rbx_cSt, "fetch", AltPg_St_fetch, 0);
	rb_define_method(rbx_cSt, "rows", AltPg_St_rows, 0);
	rb_define_method(rbx_cSt, "column_info", AltPg_St_column_info, 0);

	id_translate_parameters = rb_intern("translate_parameters");
	sym_type_name    = ID2SYM(rb_intern("type_name"));
	sym_dbi_type     = ID2SYM(rb_intern("dbi_type"));
}
