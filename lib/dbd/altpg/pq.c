#include <libpq-fe.h>

#include <ruby.h>
//#include <rubyio.h>
//#include <st.h>

#ifndef RSTRING_PTR
#define RSTRING_PTR(string) RSTRING(string)->ptr
#endif
#ifndef RSTRING_LEN
#define RSTRING_LEN(string) RSTRING(string)->len
#endif

static VALUE rbx_mAltPg;
static VALUE rbx_cPq;
static VALUE rbx_cSt;

static ID id_object_id;
static ID id_to_s;
static ID id_to_i;
static ID id_new;
static VALUE sym_type_name;
static VALUE sym_dbi_type;

struct rbpq_struct { // FIXME - this could just be the pointer...
	PGconn *conn;
	int finished;
	unsigned long serial;  // pstmt name suffix; may wrap
};

struct rbst_struct {
	PGconn *conn;
	PGresult *res;
	char **param_values;
	int *param_lengths;
	unsigned int nfields;
	unsigned int ntuples;
	unsigned int row_number;
	int resultFormat;
};

/* ==== Helper functions ================================================== */

#define GetPqStruct(obj, out) \
	do { out = (struct rbpq_struct *)DATA_PTR(obj); } while (0)

#define GetStmt(obj, out) \
	do { out = (struct rbst_struct *)DATA_PTR(obj); } while (0)

#define SetStmt(obj, st) \
	do { ((struct rbst_struct *)DATA_PTR(obj)) = st; } while (0);

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

/* ==== Class methods ===================================================== */

static void
rbpq_s_free(struct rbpq_struct *pq)
{
	if (NULL != pq && !pq->finished && NULL != pq->conn) {
		PQfinish(pq->conn);
		pq->conn = NULL;
		pq->finished = 1;
	}
}

static VALUE
rbpq_s_alloc(VALUE klass)
{
	struct rbpq_struct *pq = ALLOC(struct rbpq_struct);
	memset(pq, '\0', sizeof(struct rbpq_struct));
	return Data_Wrap_Struct(klass, 0, rbpq_s_free, pq);
}

static void
raise_dbi_error(VALUE klass, const char *msg, const char *sqlstate)
{
	rb_raise(klass, msg);
}

static void
maybe_raise_dbi_error(struct rbst_struct *st, PGresult *res)
{
	ExecStatusType status = PQresultStatus(res);
	switch (status) {
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
			VALUE e = rb_funcall(rb_path2class("DBI::DatabaseError"),
			                     id_new,
			                     3,
			                     rb_str_new2(PQresultErrorMessage(res)),
													 Qnil,
			                     rb_str_new2(PQresultErrorField(res, PG_DIAG_SQLSTATE)));
			PQclear(res);
			if (st) st->res = NULL;
			rb_exc_raise(e);
			break; /* not reached */
		}
  default:
	  raise_dbi_error(rb_path2class("DBI::InternalError"),
		                "Unknown/unexpected PQresultStatus '%s'", PQresStatus(status));
	}
}

static void
raise_dbi_database_error(const char *msg, const char *sqlstate)
{
	// FIXME - ignoring other args for now
	raise_dbi_error(rb_path2class("DBI::OperationalError"), msg, sqlstate);
}

/* ==== Instance methods ================================================== */

static VALUE
rbpq_connectdb(VALUE self, VALUE conninfo) /* PQconnectdb */
{
	struct rbpq_struct *pq;

	Check_SafeStr(conninfo);
	GetPqStruct(self, pq);

	pq->conn = PQconnectdb(RSTRING_PTR(conninfo)); /* FIXME : connectionstart ... */
	if (NULL == pq->conn) {
		rb_raise(rb_eNoMemError, "Unable to allocate libPQ structures");
	} else if (PQstatus(pq->conn) != CONNECTION_OK) {
		char *e = strdup(PQerrorMessage(pq->conn));
		PQfinish(pq->conn);
		raise_dbi_database_error(e, "08000");
	}
	/*
	{
		int i, ntuples;
		VALUE map = rb_hash_new();
		PGresult *r = PQexecParams(pq->conn, "SELECT oid, typname FROM pg_type", 0, NULL, NULL, NULL, NULL, 0);
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

	pq->finished = 0;

	return self;
}

static VALUE
rbpq_disconnect(VALUE self)
{
	struct rbpq_struct *pq;

	GetPqStruct(self, pq);
	if (pq->conn) {
		PQfinish(pq->conn);
		pq->conn = NULL;
	}
	pq->finished = 1;

	return Qnil;
}

static VALUE
rbpq_do(VALUE self, VALUE query)
{
	struct rbpq_struct *pq;
	PGresult *res;
	VALUE rows;

	GetPqStruct(self, pq);
	res = PQexec(pq->conn, STR2CSTR(query));
	maybe_raise_dbi_error(NULL, res);
	rows = convert_PQcmdTuples(res);
	PQclear(res);

	return rows;
}

static VALUE
rbpq_dbname(VALUE self)
{
	struct rbpq_struct *pq;
	VALUE ret;

	GetPqStruct(self, pq); // FIXME - internal error if already finished

	ret = rb_str_new2(PQdb(pq->conn));
	OBJ_TAINT(ret);

	return ret;
}

/* ---------- DBI::DBD::Pq::Statement ------------------------------------- */

static void
rbst_s_free(struct rbst_struct *st)
{
	if (NULL != st && NULL != st->conn) {
		// FIXME - cancel in progress?
		if (st->res) PQclear(st->res);
	}
}

static VALUE
rbst_s_alloc(VALUE klass)
{
	struct rbst_struct *st = ALLOC(struct rbst_struct);
	memset((void *)st, '\0', sizeof(struct rbst_struct));
	//st->nfields = st->ntuples = st->row_number = 0;
	st->resultFormat = 0; /* text */
	return Data_Wrap_Struct(klass, 0, rbst_s_free, st);
}

static VALUE
rbst_initialize(VALUE self, VALUE parent, VALUE query, VALUE nParams, VALUE paramTypes) /* PQprepare */
{
	struct rbpq_struct *pq;
	struct rbst_struct *st;
	VALUE plan;

	if (!rb_obj_is_instance_of(parent, rbx_cPq)) {
		rb_raise(rb_eTypeError, "Expected argument of type DBI::DBD::AltPg::Database");
	}

	Check_SafeStr(query);
	GetStmt(self, st);
	GetPqStruct(parent, pq);

	st->conn = pq->conn;

	rb_iv_set(self, "@type_map", rb_iv_get(parent, "@type_map")); // ??? store @parent instead?
	plan = rb_str_new2("ruby-dbi:altpg:");
	rb_str_append(plan, rb_funcall(rb_funcall(self, id_object_id, 0), id_to_s, 0));
	rb_str_buf_cat(plan, "-", 1);
	rb_str_append(plan, rb_funcall(ULONG2NUM(pq->serial++), id_to_s, 0));
	rb_iv_set(self, "@plan", plan);

	st->res = PQprepare(st->conn, RSTRING_PTR(plan), RSTRING_PTR(query), 0, NULL);
	maybe_raise_dbi_error(st, st->res);
	PQclear(st->res);
	st->res = NULL;
	rb_iv_set(self, "@params", rb_ary_new());

	return self;
}

static VALUE
rbst_bind_param(VALUE self, VALUE index, VALUE val, VALUE ignored)
{
	struct rbst_struct *st;

	GetStmt(self, st);

	rb_ary_store(rb_iv_get(self, "@params"),
	             NUM2INT(index) - 1,
	             rb_funcall(val, id_to_s, 0));

	return Qnil;
}

static VALUE
rbst_execute(VALUE self)
{
	struct rbst_struct *st;
	VALUE iv_params;
	int nparams;

	GetStmt(self, st);

	if (st->res) {
		PQclear(st->res); // FIXME - other cancellation?
	}

	iv_params = rb_iv_get(self, "@params");
	nparams = RARRAY_LEN(iv_params);
	//printf("[execute] nparams is %d\n", nparams);
	if (nparams > 0) {
		int i;

		if (NULL == st->param_values) {
			st->param_values  = ALLOC_N(char *, nparams);
			memset(st->param_values, '\0', sizeof(char *) * nparams);
			st->param_lengths = ALLOC_N(int, nparams);
			memset(st->param_lengths, '\0', sizeof(int) * nparams);
		}

		for (i = 0; i < nparams; ++i) {
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
	}

	st->res = PQexecPrepared(st->conn, STR2CSTR(rb_iv_get(self, "@plan")),
	                         nparams, st->param_values,
	                         st->param_lengths, NULL, st->resultFormat);
	st->row_number = 0;
	maybe_raise_dbi_error(st, st->res);
	if (nparams) rb_ary_clear(iv_params);
	st->nfields = PQnfields(st->res);
	st->ntuples = PQntuples(st->res);

	return Qnil;
}

static VALUE
rbst_finish(VALUE self)
{
	struct rbst_struct *st;

	GetStmt(self, st);

	if (st->res) {
		/* FIXME - cancellation? */
		PQclear(st->res);
		st->res = NULL;
	}

	if (st->conn) {
		VALUE plan = rb_iv_get(self, "@plan");
		//printf("@plan is %s\n", STR2CSTR(plan));
		VALUE deallocate_fmt = rb_str_new2("DEALLOCATE \"%s\"");
		PGresult *res = PQexec(st->conn, STR2CSTR(rb_str_format(1, &plan, deallocate_fmt)));
		maybe_raise_dbi_error(st, res);
		PQclear(res);
	}

	return Qnil;
}

static VALUE
rbst_fetch(VALUE self)
{
	struct rbst_struct *st;
	VALUE ret;
	/* XXX - store @row rather than making anew each time? */
	int i;

	GetStmt(self, st);

	if (st->row_number >= st->ntuples)
		return Qnil;

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
rbst_rows(VALUE self)
{
	struct rbst_struct *st;

	GetStmt(self, st);
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
rbst_column_info(VALUE self)
{
	struct rbst_struct *st;

	// XXX - @column_info ||= (...).freeze
	// XXX - module-level strings for keys?
	VALUE ret;
	VALUE iv_type_map;
	int i;

	GetStmt(self, st);
	ret = rb_ary_new2(st->nfields);
	iv_type_map = rb_iv_get(self, "@type_map");

	for (i = 0; i < st->nfields; ++i) {
		VALUE col = rb_hash_new();
		VALUE type_map_entry;
		//int fmod;
		Oid ftype;

		//fmod  = PQfmod(st->res, i);  // XXX - T.E. compute ftypes in advance?

		rb_hash_aset(col, rb_str_new2("name"),
		                  rb_str_new2(PQfname(st->res, i)));

		ftype = PQftype(st->res, i);
		type_map_entry = rb_hash_aref(iv_type_map, INT2FIX(ftype));

		// col['type_name'] = @type_map[16][:type_name]
		rb_hash_aset(col, rb_str_new2("type_name"),
		                  rb_hash_aref(type_map_entry, sym_type_name));
		// col['dbi_type'] = @type_map[16][:dbi_type]
		rb_hash_aset(col, rb_str_new2("dbi_type"),
		                  rb_hash_aref(type_map_entry, sym_dbi_type));

		// FIXME - need to look up in case new type added!
		// 
		// Look at perl-DBD-Pg's computation of precision/scale...
		//
		//rb_hash_aset(col, rb_str_new2("precision"), INT2FIX(64));
		//rb_hash_aset(col, rb_str_new2("scale"), Qnil);

		rb_ary_store(ret, i, col);
	}

	return ret;
}

static VALUE
rbst_result_format(VALUE self, VALUE fmtcode)
{
	struct rbst_struct *st;

	GetStmt(self, st);

	st->resultFormat = FIX2INT(fmtcode);

	return Qnil;
}

void
Init_pq()
{
	rbx_mAltPg = rb_path2class("DBI::DBD::AltPg");
//rbx_cPq    = rb_define_class_under(rbx_mAltPg, "Pq", rb_cObject);
	rbx_cPq    = rb_define_class_under(rbx_mAltPg, "Database", rb_path2class("DBI::BaseDatabase"));
	rbx_cSt    = rb_define_class_under(rbx_mAltPg, "Statement",
	                                   rb_path2class("DBI::BaseStatement"));

	rb_define_alloc_func(rbx_cPq, rbpq_s_alloc);
	rb_define_method(rbx_cPq, "connectdb", rbpq_connectdb, 1); /* PQconnectdb */
	rb_define_method(rbx_cPq, "disconnect", rbpq_disconnect, 0); /* PQfinish */
	rb_define_method(rbx_cPq, "database_name", rbpq_dbname, 0); /* PQdb */
	rb_define_method(rbx_cPq, "do", rbpq_do, 1); /* PQdb */

	rb_define_alloc_func(rbx_cSt, rbst_s_alloc);
	rb_define_method(rbx_cSt, "initialize", rbst_initialize, 4);
	rb_define_method(rbx_cSt, "finish", rbst_finish, 0);
	rb_define_method(rbx_cSt, "execute", rbst_execute, 0);
	rb_define_method(rbx_cSt, "fetch", rbst_fetch, 0);
	rb_define_method(rbx_cSt, "rows", rbst_rows, 0);
	rb_define_method(rbx_cSt, "column_info", rbst_column_info, 0);
	rb_define_method(rbx_cSt, "bind_param", rbst_bind_param, 3);
	rb_define_method(rbx_cSt, "result_format=", rbst_result_format, 1);

	id_object_id = rb_intern("object_id");
	id_to_s      = rb_intern("to_s");
	id_to_i      = rb_intern("to_i");
	id_new       = rb_intern("new");
	sym_type_name = ID2SYM(rb_intern("type_name"));
	sym_dbi_type  = ID2SYM(rb_intern("dbi_type"));
}
