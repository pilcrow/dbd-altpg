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
static VALUE sym_type_name;
static VALUE sym_dbi_type;

struct AltPg_Db {
	PGconn *conn;
	unsigned long serial;  /* pstmt name suffix; may wrap */
};

struct AltPg_St {
	PGconn *conn;              /* NULL if finished                       */
	PGresult *res;             /* non-NULL if executed and not cancelled */
	int nparams;
	char **param_values;
	int *param_lengths;
	unsigned int nfields;
	unsigned int ntuples;
	unsigned int row_number;
	int resultFormat;
};

/* ==== Helper functions ================================================== */

static VALUE
new_dbi_database_error(const char *msg,
                       VALUE err,
                       const char *sqlstate)
{
	return rb_funcall(rb_path2class("DBI::DatabaseError"),
	                  id_new,
	                  3,
	                  msg ? rb_str_new2(msg) : Qnil,
	                  err,
	                  sqlstate ? rb_str_new2(sqlstate) : Qnil);
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
	int fd, r;
	fd_set rfds;
	PGresult *tmp = NULL;
	PGresult *res = NULL;

	fd = PQsocket(conn);

	FD_ZERO(&rfds);
	FD_SET(fd, &rfds);
	r = rb_thread_select(fd + 1, &rfds, NULL, NULL, NULL);

	/* ruby-pg-0.8.0 pgconn_block() */
	PQconsumeInput(conn);
	while (PQisBusy(conn)) {
		FD_ZERO(&rfds);
		FD_SET(fd, &rfds);
		r = rb_thread_select(fd + 1, &rfds, NULL, NULL, NULL);
		if (r > 0) {
			PQconsumeInput(conn);
			continue;
		}
		if (r < 0) rb_sys_fail("async_PQgetResult select failed");
		rb_raise(rb_path2class("DBI::InternalError"),
		         "async_PQgetResult select() indicated impossible timeout!");
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
			VALUE e = new_dbi_database_error(
			            PQresultErrorMessage(res),
									Qnil,
			            PQresultErrorField(res, PG_DIAG_SQLSTATE));
			PQclear(res);
			rb_exc_raise(e);
			break; /* not reached */
		}
  default:
	  rb_raise(rb_path2class("DBI::InternalError"),
		         "Unknown/unexpected PQresultStatus");
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
altpg_db_direct_exec(struct AltPg_Db *db, VALUE *out, const char *query)
{
	PGresult *res;

	if (! PQsendQuery(db->conn, query)) {
		raise_PQsend_error(db->conn);
	}
	res = async_PQgetResult(db->conn);
	if (out) {
		*out = convert_PQcmdTuples(res);
	}

	PQclear(res);
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
		PQclear(st->res);        /* Undo any execute()   */
		st->res = NULL;
		st->ntuples = 0;

		st->row_number = 0;      /* Erase any #fetch     */
	}

	if (st->nparams) {         /* Undo any #bind_param */
		MEMZERO(st->param_values, char *, st->nparams);
		MEMZERO(st->param_lengths, int, st->nparams);
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

static VALUE
AltPg_Db_connectdb(VALUE self, VALUE conninfo)
{
	struct AltPg_Db *db;
	int fd, r;
	fd_set fds;
	PostgresPollingStatusType pollstat;

	Check_SafeStr(conninfo);
	Data_Get_Struct(self, struct AltPg_Db, db);

	db->conn = PQconnectStart(RSTRING_PTR(conninfo));
	if (NULL == db->conn) {
		rb_raise(rb_eNoMemError, "Unable to allocate libPQ structures");
	} else if (PQstatus(db->conn) == CONNECTION_BAD) {
		goto ConnError;
	}
	
	fd = PQsocket(db->conn);

	for (pollstat = PGRES_POLLING_WRITING;
	     pollstat != PGRES_POLLING_OK;
			 pollstat = PQconnectPoll(db->conn)) {
		FD_ZERO(&fds);
		FD_SET(fd, &fds);

		switch (pollstat) {
		case PGRES_POLLING_OK:
			break; /* all done */
		case PGRES_POLLING_FAILED:
			goto ConnError;
			break; /* not reached */
		case PGRES_POLLING_READING:
			r = rb_thread_select(fd + 1, &fds, NULL, NULL, NULL);
			if (r <= 0) goto SelectError;
			break;
		case PGRES_POLLING_WRITING:
			r = rb_thread_select(fd + 1, NULL, &fds, NULL, NULL);
			if (r <= 0) goto SelectError;
			break;
		default:
			rb_raise(rb_path2class("DBI::InternalError"),
			         "Non-sensical PGRES_POLLING status encountered");
		}
	}

	/*
	{
		int i, ntuples;
		VALUE map = rb_hash_new();
		PGresult *r = PQexecParams(db->conn, "SELECT oid, typname FROM pg_type", 0, NULL, NULL, NULL, NULL, 0);
		maybe_raise_dbi_error(NULL, r);

		ntuples = PQntuples(r);
		for (i = 0; i < ntuples; ++i) {
			VALUE oid;
			VALUE typname;

			oid = rb_str_new(PQgetvalue(r, i, 0), PQgetlength(r, i, 0));
			oid = rb_funcall(oid, id_to_i, 0);
			typname = rb_str_new(PQgetvalue(r, i, 1), PQgetlength(r, i, 1));
			rb_hash_aset(map, oid, typname);
		}
		//rb_iv_set(self, "@type_map", map);
		PQclear(r);
	}
	*/

	return self;

ConnError:
	{
		VALUE e = new_dbi_database_error(PQerrorMessage(db->conn),
		                                 Qnil,
		                                 "08000");
		PQfinish(db->conn);
		rb_exc_raise(e);
	}
SelectError:
	{
		if (r < 0) rb_sys_fail("connectdb select() failed");
		rb_raise(rb_path2class("DBI::InternalError"),
		         "connectdb select() indicated impossible timeout!");
	}
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
AltPg_Db_do(VALUE self, VALUE query)
{
	struct AltPg_Db *db;
	VALUE rows;

	Data_Get_Struct(self, struct AltPg_Db, db);
	altpg_db_direct_exec(db, &rows, STR2CSTR(query));

	return rows;
}

static VALUE
AltPg_Db_commit(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	altpg_db_direct_exec(db, NULL, "COMMIT");

	return Qnil;
}

static VALUE
AltPg_Db_rollback(VALUE self)
{
	struct AltPg_Db *db;

	Data_Get_Struct(self, struct AltPg_Db, db);
	altpg_db_direct_exec(db, NULL, "ROLLBACK");

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

/* ---------- DBI::DBD::Pq::Statement ------------------------------------- */

static void
AltPg_St_s_free(struct AltPg_St *st)
{
	if (NULL == st) return;
	if (st->res) PQclear(st->res);
	if (st->param_lengths) xfree(st->param_lengths);
	if (st->param_values) xfree(st->param_values);
	xfree(st);
}

static VALUE
AltPg_St_s_alloc(VALUE klass)
{
	struct AltPg_St *st = ALLOC(struct AltPg_St);
	memset((void *)st, '\0', sizeof(struct AltPg_St));
	return Data_Wrap_Struct(klass, 0, AltPg_St_s_free, st);
}

/* FIXME:  paramTypes ? */
static VALUE
AltPg_St_initialize(VALUE self, VALUE parent, VALUE query, VALUE nParams) /* PQprepare */
{
	struct AltPg_Db *db;
	struct AltPg_St *st;
	PGresult *tmp_result;
	VALUE plan;

	if (!rb_obj_is_instance_of(parent, rbx_cDb)) {
		rb_raise(rb_eTypeError,
		         "Expected argument of type DBI::DBD::AltPg::Database");
	}

	Data_Get_Struct(self, struct AltPg_St, st); /* later, our own data_get */
	Data_Get_Struct(parent, struct AltPg_Db, db);
	Check_SafeStr(query);

	if (!db || !db->conn) {
		rb_raise(rb_path2class("DBI::InternalError"),
		         "Attempt to create AltPg::Statement from invalid AltPg::Database (db %p, db->conn %p)", db, db ? db->conn : NULL);
	}
	st->conn = db->conn;
	st->nparams = FIX2INT(nParams);
	if (st->nparams < 0) {
		rb_raise(rb_eTypeError, "Number of parameters must be >= 0");
	}
	st->param_values = ALLOC_N(char *, st->nparams);
	MEMZERO(st->param_values, char *, st->nparams);
	st->param_lengths = ALLOC_N(int, st->nparams);
	MEMZERO(st->param_lengths, int, st->nparams);

	rb_iv_set(self, "@type_map", rb_iv_get(parent, "@type_map")); // ??? store @parent instead?
	plan = rb_str_new2("ruby-dbi:altpg:");
	rb_str_append(plan, rb_funcall(ULONG2NUM(db->serial++), id_to_s, 0));
	rb_iv_set(self, "@plan", plan);

	if (!PQsendPrepare(st->conn, RSTRING_PTR(plan), RSTRING_PTR(query), 0, NULL)) {
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
	int i, send_ok;

	st = altpg_st_get_unfinished(self);
	altpg_st_cancel(st);

	iv_params = rb_iv_get(self, "@params");

	for (i = 0; i < st->nparams; ++i) {
		VALUE elt = rb_ary_entry(iv_params, i);
		//printf("@params[%d] => %s\n", i, STR2CSTR(elt));
		if (Qnil == elt) {
			st->param_values[i] = NULL;
			st->param_lengths[i] = 0;
		} else {
			st->param_values[i] = STR2CSTR(elt);
			st->param_lengths[i] = RSTRING_LEN(elt);
		}
	}

	send_ok = PQsendQueryPrepared(st->conn,
	                              STR2CSTR(rb_iv_get(self, "@plan")),
	                              st->nparams,
	                              st->param_values,
	                              st->param_lengths,
	                              NULL,
	                              0);   /* text format */
	if (!send_ok) {
			raise_PQsend_error(st->conn);
	}
	st->res = async_PQgetResult(st->conn);
	if (st->nparams) rb_ary_clear(iv_params);
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

	return INT2FIX(st->resultFormat);
}


static VALUE
AltPg_St_result_format_set(VALUE self, VALUE fmtcode)
{
	struct AltPg_St *st;

	st = altpg_st_get_unfinished(self);
	st->resultFormat = FIX2INT(fmtcode);

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
	rb_define_method(rbx_cDb, "connectdb", AltPg_Db_connectdb, 1);
	rb_define_method(rbx_cDb, "database_name", AltPg_Db_dbname, 0);
	rb_define_method(rbx_cDb, "disconnect", AltPg_Db_disconnect, 0);
	rb_define_method(rbx_cDb, "commit", AltPg_Db_commit, 0);
	rb_define_method(rbx_cDb, "rollback", AltPg_Db_rollback, 0);
	rb_define_method(rbx_cDb, "do", AltPg_Db_do, 1);

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

	id_to_s       = rb_intern("to_s");
	id_to_i       = rb_intern("to_i");
	id_new        = rb_intern("new");
	id_inspect    = rb_intern("inspect");
	sym_type_name = ID2SYM(rb_intern("type_name"));
	sym_dbi_type  = ID2SYM(rb_intern("dbi_type"));
}
