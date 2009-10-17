#include <libpq-fe.h>
#include <ruby.h>
//#include <rubyio.h>
//#include <st.h>

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

static ID id_to_s;
static ID id_to_i;
static ID id_new;
static ID id_inspect;
static ID id_translate_parameters;
static VALUE sym_type_name;
static VALUE sym_text;
static VALUE sym_binary;

struct altpg_params {
	int nparams;
	char **param_values;
	int *param_lengths;
};

struct AltPg_Db {
	PGconn *conn;
	unsigned long serial;  /* pstmt name suffix; may wrap */
};

struct AltPg_St {
	PGconn *conn;              /* NULL if finished                       */
	PGresult *res;             /* non-NULL if executed and not cancelled */
	struct altpg_params params;
	unsigned int nfields;
	unsigned int ntuples;
	unsigned int row_number;
	int result_format;
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

static void
altpg_thread_select(int nfd, fd_set *rfds, fd_set *wfds)
{
	int r = rb_thread_select(nfd, rfds, wfds, NULL, NULL);
	if (r > 0) return;

	if (r < 0) raise_dbi_internal_error("Internal select() error");
	raise_dbi_internal_error("Internal select() impossibly timed out");
}

static void
fd_await_readable(fd)
{
	fd_set fds;

	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	altpg_thread_select(fd + 1, &fds, NULL);
}

static void
fd_await_writeable(fd)
{
	fd_set fds;

	FD_ZERO(&fds);
	FD_SET(fd, &fds);
	altpg_thread_select(fd + 1, NULL, &fds);
}

static void
altpg_params_initialize(struct altpg_params *ap, int nparams)
{
	/* FIXME - we can skip setting param_lengths if text protocol */
	ap->nparams = nparams;
	ap->param_values = ALLOC_N(char *, nparams);
	MEMZERO(ap->param_values, char *, nparams);
	ap->param_lengths = ALLOC_N(int, nparams);
	MEMZERO(ap->param_lengths, int, nparams);
}

/* Map an array of strings (or nils) to a struct altpg_params,
 * allocating the struct if necessary.
 *
 * CAUTION:  no ruby type checking!
 */
static struct altpg_params *
altpg_params_from_ary(struct altpg_params *ap, VALUE ary)
{
  int i;

  for (i = 0; i < ap->nparams; ++i) {
    VALUE elt = rb_ary_entry(ary, i);
    if (Qnil == elt) {
      ap->param_values[i]  = NULL;
      ap->param_lengths[i] = 0;
    } else {
      ap->param_values[i]  = RSTRING_PTR(elt);
      ap->param_lengths[i] = RSTRING_LEN(elt);
    }
  }

  return ap;
}

static void
altpg_params_clear(struct altpg_params *ap)
{
	if (NULL == ap) return;
	if (ap->param_values) xfree(ap->param_values);
	ap->param_values = NULL;
	if (ap->param_lengths) xfree(ap->param_lengths);
	ap->param_lengths = NULL;
	ap->nparams = 0;
}

static VALUE
convert_PQcmdTuples(PGresult *res)
{
	VALUE ret = Qnil;
	if (res) {
		char *rows = PQcmdTuples(res);
		if (rows[0] != '\0') {
			ret = rb_Integer(rb_str_new2(rows));
		}
	}
	return ret;
}

PGresult *
async_PQgetResult(PGconn *conn)
{
	PGresult *tmp = NULL;
	PGresult *res = NULL;
	int fd;

	fd = PQsocket(conn);
	fd_await_readable(fd);

	/* ruby-pg-0.8.0 pgconn_block() */
	PQconsumeInput(conn);
	while (PQisBusy(conn)) {
		fd_await_readable(fd);
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

/* Unprepared query execution.  (internal) */
static void
altpg_db_direct_exec(struct AltPg_Db *db, VALUE *out, const char *query, VALUE ary)
{
	struct altpg_params params = { 0, NULL, NULL };
	int send_ok;
	PGresult *res;

	if (ary != Qnil && RARRAY_LEN(ary) > 0) {
		altpg_params_initialize(&params, RARRAY_LEN(ary));
		altpg_params_from_ary(&params, ary);
	}
	send_ok = PQsendQueryParams(db->conn,
	                            query,
	                            params.nparams,
															NULL,
	                            params.param_values,
															params.param_lengths,
															NULL,
															1);
	altpg_params_clear(&params);

	if (!send_ok) raise_PQsend_error(db->conn);

	res = async_PQgetResult(db->conn);
	if (out) *out = convert_PQcmdTuples(res);

	PQclear(res);
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
			fd_await_readable(fd);
			break;
		case PGRES_POLLING_WRITING:
			fd_await_writeable(fd);
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

static VALUE
AltPg_Db_do(int argc, VALUE *argv, VALUE self)
{
	struct AltPg_Db *db;
	VALUE query, params, rowcount;

	rb_scan_args(argc, argv, "1*", &query, &params);

	SafeStringValue(query);
	query = rb_funcall(rbx_mAltPg, id_translate_parameters, 1, query);

	Data_Get_Struct(self, struct AltPg_Db, db);
	altpg_db_direct_exec(db, &rowcount, RSTRING_PTR(query), params);

	return rowcount;
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
		altpg_db_direct_exec(db, NULL, "COMMIT", Qnil);
		altpg_db_direct_exec(db, NULL, "BEGIN", Qnil);
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
		altpg_db_direct_exec(db, NULL, "ROLLBACK", Qnil);
		altpg_db_direct_exec(db, NULL, "BEGIN", Qnil);
	}

	return Qnil;
}

static VALUE
AltPg_Db_dbname(VALUE self)
{
	struct AltPg_Db *db;
	VALUE ret;

	Data_Get_Struct(self, struct AltPg_Db, db);

	/* FIXME - internalerror if NULL == db->conn */
	ret = rb_str_new2(PQdb(db->conn));
	OBJ_TAINT(ret);

	return ret;
}

static VALUE
AltPg_Db_in_transaction_p(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	return altpg_db_in_transaction(db) ? Qtrue : Qfalse;
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
AltPg_St_initialize(VALUE self, VALUE parent, VALUE query, VALUE default_resfmt)
{
	struct AltPg_Db *db;
	struct AltPg_St *st;
	PGresult *tmp_result;
	VALUE plan;

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
	st->result_format = FIX2INT(default_resfmt);

	SafeStringValue(query);
	query = rb_funcall(rbx_mAltPg, id_translate_parameters, 1, query);

	rb_iv_set(self, "@type_map", rb_iv_get(parent, "@type_map")); // ??? store @parent instead?
	plan = rb_str_new2("ruby-dbi:altpg:");
	rb_str_append(plan, rb_funcall(ULONG2NUM(db->serial++), id_to_s, 0));
	rb_iv_set(self, "@plan", plan);

	if (!PQsendPrepare(st->conn,
	    RSTRING_PTR(plan),
	    RSTRING_PTR(query),
	    0,
	    NULL)) {
		raise_PQsend_error(st->conn);
	}
	tmp_result = async_PQgetResult(st->conn);
	PQclear(tmp_result);

	rb_iv_set(self, "@params", rb_ary_new());

	return self;
}

static VALUE
AltPg_St_bind_param(VALUE self, VALUE index, VALUE val, VALUE ignored)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);

	/* ??? No bounds checking on index yet.  If you want to balloon
	 *     @params, go ahead.
	 */
	rb_ary_store(rb_iv_get(self, "@params"),
	             FIX2INT(index) - 1,
	             rb_funcall(val, id_to_s, 0));
	//printf("bind_param -> @params[%s] = '%s'\n",
	//       STR2CSTR(rb_funcall(index, id_inspect, 0)),
	//       STR2CSTR(rb_funcall(val, id_inspect, 0)));
	//printf("bind_param -> @params[%d] = '%s'\n",
	//       FIX2INT(index) - 1,
	//       STR2CSTR(rb_funcall(val, id_to_s, 0)));

	return Qnil;
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
	int send_ok;

	st = altpg_st_get_unfinished(self);
	altpg_st_cancel(st);
	iv_params = rb_iv_get(self, "@params");

	if (RARRAY_LEN(iv_params) != st->params.nparams) {
		altpg_params_clear(&st->params);
		altpg_params_initialize(&st->params, RARRAY_LEN(iv_params));
	}
	altpg_params_from_ary(&st->params, iv_params);
	send_ok = PQsendQueryPrepared(st->conn,
	                              RSTRING_PTR(rb_iv_get(self, "@plan")),
	                              st->params.nparams,
	                              st->params.param_values,
	                              st->params.param_lengths,
	                              NULL,
	                              st->result_format);
	if (st->params.nparams > 0) rb_ary_clear(iv_params);

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

	if (st->conn) {
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

	//printf("fetch index %d (of %d)\n", st->row_number, st->ntuples);
	ret = rb_ary_new2(st->nfields);
	for (i = 0; i < st->nfields; ++i) {
		VALUE val = PQgetisnull(st->res, st->row_number, i)
		            ? Qnil
								: rb_str_new(PQgetvalue(st->res, st->row_number, i),
		                         PQgetlength(st->res, st->row_number, i));
		rb_ary_store(ret, i, val);
	}

	st->row_number++;
	return ret;
}

static VALUE
AltPg_St_rows(VALUE self)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);

	return convert_PQcmdTuples(st->res);
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
	VALUE conversion_key; /* :text_conversion or :binary_conversion */
	VALUE iv_type_map;
	int i;

	st = altpg_st_get_unfinished(self);
	ret = rb_ary_new2(st->nfields);
	iv_type_map = rb_iv_get(self, "@type_map");

	conversion_key = PQfformat(st->res, 0) == 1 ? sym_binary : sym_text;

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
		                  rb_hash_aref(type_map_entry, conversion_key));

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

static VALUE
AltPg_St_result_format(VALUE self)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);

	return INT2FIX(st->result_format);
}


static VALUE
AltPg_St_result_format_set(VALUE self, VALUE fmtcode)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);
	st->result_format = FIX2INT(fmtcode);

	return Qnil;
}

void
Init_pq()
{
	rbx_mAltPg = rb_path2class("DBI::DBD::AltPg");
	rbx_cDb    = rb_define_class_under(rbx_mAltPg, "Database", rb_path2class("DBI::BaseDatabase"));
	rbx_cSt    = rb_define_class_under(rbx_mAltPg, "Statement",
	                                   rb_path2class("DBI::BaseStatement"));

	rb_define_alloc_func(rbx_cDb, AltPg_Db_s_alloc);
	rb_define_method(rbx_cDb, "pq_connect_db", AltPg_Db_pq_connect_db, 1);
	rb_define_method(rbx_cDb, "in_transaction?", AltPg_Db_in_transaction_p, 0);
	rb_define_method(rbx_cDb, "database_name", AltPg_Db_dbname, 0);
	rb_define_method(rbx_cDb, "disconnect", AltPg_Db_disconnect, 0);
	rb_define_method(rbx_cDb, "commit", AltPg_Db_commit, 0);
	rb_define_method(rbx_cDb, "rollback", AltPg_Db_rollback, 0);
	rb_define_method(rbx_cDb, "do", AltPg_Db_do, -1);

	rb_define_alloc_func(rbx_cSt, AltPg_St_s_alloc);
	rb_define_method(rbx_cSt, "initialize", AltPg_St_initialize, 3);
	rb_define_method(rbx_cSt, "bind_param", AltPg_St_bind_param, 3);
	rb_define_method(rbx_cSt, "cancel", AltPg_St_cancel, 0);
	rb_define_method(rbx_cSt, "finish", AltPg_St_finish, 0);
	rb_define_method(rbx_cSt, "execute", AltPg_St_execute, 0);
	rb_define_method(rbx_cSt, "fetch", AltPg_St_fetch, 0);
	rb_define_method(rbx_cSt, "rows", AltPg_St_rows, 0);
	rb_define_method(rbx_cSt, "column_info", AltPg_St_column_info, 0);
	rb_define_method(rbx_cSt, "result_format",  AltPg_St_result_format, 0);
	rb_define_method(rbx_cSt, "result_format=", AltPg_St_result_format_set, 1);

	id_to_s                 = rb_intern("to_s");
	id_to_i                 = rb_intern("to_i");
	id_new                  = rb_intern("new");
	id_inspect              = rb_intern("inspect");
	id_translate_parameters = rb_intern("translate_parameters");
	sym_type_name    = ID2SYM(rb_intern("type_name"));
	sym_text         = ID2SYM(rb_intern("text"));
	sym_binary       = ID2SYM(rb_intern("binary"));
}
