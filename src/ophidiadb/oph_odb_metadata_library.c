/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "oph_odb_metadata_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <mysql.h>
#include "debug.h"

#include "oph_pid_library.h"

extern int msglevel;

#ifdef OPH_ODB_MNG
void _print_bson(const bson_t * b)
{
	char *str;
	str = bson_as_json(b, NULL);
	pmesg(LOG_INFO, __FILE__, __LINE__, "%s\n", str);
	bson_free(str);
}
#endif

int oph_odb_meta_find_metadatakey_list(ophidiadb * oDB, int vocabulary_id, MYSQL_RES ** information_list)
{
	(*information_list) = NULL;

	if (!oDB || !information_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;

	n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_METADATAKEY_LIST, vocabulary_id);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Execute query
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	*information_list = mysql_store_result(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_insert_into_metadatainstance_manage_tables(ophidiadb * oDB, const int id_datacube, const int id_metadatakey, const char *new_metadatakey, const char *new_metadatakey_variable,
							    const int id_metadatatype, const int id_user, const char *value, int *last_insertd_id)
{
	if (!oDB || !id_datacube || !id_metadatakey || !id_metadatatype || !id_user || !value || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter %d %d %d %d %d %d %d\n", oDB, id_datacube, id_metadatakey, id_metadatatype, id_user, value, last_insertd_id);
		return OPH_ODB_NULL_PARAM;
	}
	if (!new_metadatakey) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Label for new metadata key not present.\n");
		return OPH_ODB_NULL_PARAM;
	}
	*last_insertd_id = 0;

#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_oid_t oid;
	bson_oid_init(&oid, NULL);
	int id_metadatainstance = bson_oid_hash(&oid);
	if (!id_metadatainstance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get anew identifier.\n");
		return OPH_ODB_MONGODB_ERROR;
	} else if (id_metadatainstance < 0)
		id_metadatainstance = -id_metadatainstance;

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

	BSON_APPEND_OID(doc, "_id", &oid);
	BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);
	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
	BSON_APPEND_INT32(doc, "idtype", id_metadatatype);
	BSON_APPEND_UTF8(doc, "value", value);
	BSON_APPEND_UTF8(doc, "label", new_metadatakey);
	BSON_APPEND_DATE_TIME(doc, "lastupdate", millisecondsSinceEpoch);
	if (id_metadatakey > 0)
		BSON_APPEND_INT32(doc, "idkey", id_metadatakey);
	if (new_metadatakey_variable)
		BSON_APPEND_UTF8(doc, "variable", new_metadatakey_variable);

	bson_error_t error;
	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}
	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new document into MongoDB: %s\n", error.message);
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_destroy(doc);
	mongoc_collection_destroy(collection);

	*last_insertd_id = id_metadatainstance;

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//escape value
	int n = strlen(value);
	char *escaped_value = (char *) malloc(2 * n + 1);
	if (!escaped_value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for escaped value\n");
		return OPH_ODB_MEMORY_ERROR;
	}
	mysql_real_escape_string(oDB->conn, escaped_value, value, n);

	int query_buflen = QUERY_BUFLEN;

	if (id_metadatakey == -1) {
		if (new_metadatakey_variable)
			query_buflen =
			    1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, new_metadatakey, new_metadatakey_variable);
		else
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, new_metadatakey);
	} else {
		if (new_metadatakey_variable)
			query_buflen =
			    1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey,
					 new_metadatakey_variable);
		else
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey);
	}

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		free(escaped_value);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Update metadatainstance table
	char insertQuery[query_buflen];
	if (id_metadatakey == -1) {
		if (new_metadatakey_variable)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, new_metadatakey,
				     new_metadatakey_variable);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, new_metadatakey);
	} else {
		if (new_metadatakey_variable)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey,
				     new_metadatakey_variable);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey);
	}

	free(escaped_value);

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	int metadatainstance_id = 0;
	if (!(metadatainstance_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted metadatainstance id\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	*last_insertd_id = metadatainstance_id;

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_retrieve_vocabulary_id(ophidiadb * oDB, char *vocabulary_name, int *id_vocabulary)
{
	if (!oDB || !vocabulary_name || !id_vocabulary) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_vocabulary = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID, vocabulary_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	*id_vocabulary = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_retrieve_vocabulary_id_from_container(ophidiadb * oDB, int id_container, int *id_vocabulary)
{
	if (!oDB || !id_vocabulary || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_vocabulary = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;
	n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID_FROM_CONTAINER, id_container);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Vocabulary doesn't exist\n");	// Vocabulary may not exist
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	*id_vocabulary = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_check_metadatainstance_existance(ophidiadb * oDB, int metadatainstance_id, int id_datacube, int *exists)
{
	if (!oDB || !metadatainstance_id || !id_datacube || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*exists = 0;

#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	BSON_APPEND_INT32(doc, "idmetadatainstance", metadatainstance_id);
	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	bson_error_t error;
	const bson_t *target;
	if (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target))
		*exists = 1;

	mongoc_collection_destroy(collection);

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve the instance: %s.\n", error.message);
		mongoc_cursor_destroy(cursor);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_METADATAINSTANCE, metadatainstance_id, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL)
		*exists = 1;

	mysql_free_result(res);

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_retrieve_metadatakey_id(ophidiadb * oDB, char *key_label, char *key_variable, int id_container, int *id_metadatakey)
{
	if (!oDB || !key_label || !id_container || !id_metadatakey) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_metadatakey = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;

	if (key_variable)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_KEY_ID, id_container, key_label, key_variable);
	else
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_KEY_ID2, id_container, key_label);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	unsigned int row_number = mysql_num_rows(res);

	if (!row_number) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (row_number > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL)
		*id_metadatakey = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_retrieve_metadatatype_id(ophidiadb * oDB, char *metadatatype_name, int *id_metadatatype)
{
	if (!oDB || !metadatatype_name || !id_metadatatype) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_metadatatype = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_METADATATYPE_ID, metadatatype_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}


	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL)
		*id_metadatatype = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_insert_into_metadatainstance_table(ophidiadb * oDB, int id_datacube, int id_metadatakey, int id_metadatatype, char *metadata_key, char *metadata_variable, char *metadata_value,
						    const int id_user, int *last_insertd_id)
{
	if (!oDB || !id_datacube || !id_metadatatype || !metadata_value || !id_user || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*last_insertd_id = 0;
	if (!id_metadatakey && !metadata_key) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_ODB_MNG
	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_oid_t oid;
	bson_oid_init(&oid, NULL);
	int id_metadatainstance = bson_oid_hash(&oid);
	if (!id_metadatainstance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get anew identifier.\n");
		return OPH_ODB_MONGODB_ERROR;
	} else if (id_metadatainstance < 0)
		id_metadatainstance = -id_metadatainstance;

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

	BSON_APPEND_OID(doc, "_id", &oid);
	BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);
	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
	BSON_APPEND_INT32(doc, "idtype", id_metadatatype);
	BSON_APPEND_UTF8(doc, "value", metadata_value);
	BSON_APPEND_UTF8(doc, "label", metadata_key);
	BSON_APPEND_DATE_TIME(doc, "lastupdate", millisecondsSinceEpoch);
	if (id_metadatakey)
		BSON_APPEND_INT32(doc, "idkey", id_metadatakey);
	if (metadata_variable)
		BSON_APPEND_UTF8(doc, "variable", metadata_variable);

	bson_error_t error;
	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}
	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new document into MongoDB: %s\n", error.message);
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_destroy(doc);
	mongoc_collection_destroy(collection);

	*last_insertd_id = id_metadatainstance;
#else
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//escape value
	int n = strlen(metadata_value);
	char *escaped_value = (char *) malloc(2 * n + 1);
	if (!escaped_value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for escaped value\n");
		return OPH_ODB_MEMORY_ERROR;
	}
	mysql_real_escape_string(oDB->conn, escaped_value, metadata_value, n);

	int query_buflen = QUERY_BUFLEN;

	if (id_metadatakey) {
		if (metadata_variable)
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key,
						    metadata_variable);
		else
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key);
	} else {
		if (metadata_variable)
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, metadata_key, metadata_variable);
		else
			query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, metadata_key);
	}

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		free(escaped_value);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char insertQuery[query_buflen];
	if (id_metadatakey) {
		if (metadata_variable)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key,
				     metadata_variable);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key);
	} else {
		if (metadata_variable)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, metadata_key, metadata_variable);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, metadata_key);
	}

	free(escaped_value);

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_find_complete_metadata_list(ophidiadb * oDB, int id_datacube, const char **metadata_keys, int metadata_keys_num, char *id_metadatainstance, char *metadata_variable,
					     char *metadata_type, char *metadata_value, char ***metadata_list, int *num_rows)
{
	if (!oDB || !id_datacube || !metadata_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*metadata_list = NULL;
	*num_rows = 0;

	char query[MYSQL_BUFLEN];
	int n = 0;
	int num_fields = 7;

#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}
	// Query to evaluate number of element to be retrieved

	bson_t *doc = bson_new(), doc_and, doc_item;
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	if (id_metadatainstance) {

		BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
		BSON_APPEND_UTF8(doc, "idmetadatainstance", id_metadatainstance);

	} else {

		char key_filter[MYSQL_BUFLEN];

		BSON_APPEND_ARRAY_BEGIN(doc, "$and", &doc_and);

		BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "iddatacube", &doc_item);
		BSON_APPEND_INT32(&doc_item, "iddatacube", id_datacube);
		bson_append_document_end(&doc_and, &doc_item);

		if (metadata_variable) {
			snprintf(key_filter, MYSQL_BUFLEN, "/%s/", metadata_variable);	// Check regex
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "variable", &doc_item);
			BSON_APPEND_UTF8(&doc_item, "variable", key_filter);
			bson_append_document_end(&doc_and, &doc_item);
		}

		if (metadata_value && strcmp(metadata_value, "%")) {
			snprintf(key_filter, MYSQL_BUFLEN, "/%s/", metadata_value);	// Check regex
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "value", &doc_item);
			BSON_APPEND_UTF8(&doc_item, "value", key_filter);
			bson_append_document_end(&doc_and, &doc_item);
		}

		if (metadata_type && strcmp(metadata_type, "%")) {

			n = snprintf(query, MYSQL_BUFLEN, MONGODB_QUERY_META_GET_TYPE, metadata_type);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				bson_destroy(doc);
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				bson_destroy(doc);
				return OPH_ODB_MYSQL_ERROR;
			}

			MYSQL_RES *res;
			MYSQL_ROW row;
			res = mysql_store_result(oDB->conn);
			if (!mysql_num_rows(res)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "No row found by query\n");
				bson_destroy(doc);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if (mysql_field_count(oDB->conn) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
				bson_destroy(doc);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}

			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "idtype", &doc_item);
			int i = 0;
			bson_t doc_or, doc_or_item;
			char buf[16];
			const char *key;
			size_t keylen;
			BSON_APPEND_ARRAY_BEGIN(&doc_item, "$or", &doc_or);
			while ((row = mysql_fetch_row(res)) && row[0]) {
				keylen = bson_uint32_to_string(i++, &key, buf, sizeof buf);
				bson_append_document_begin(&doc_or, key, (int) keylen, &doc_or_item);
				BSON_APPEND_INT32(&doc_or_item, "idtype", (int) strtol(row[0], NULL, 10));
				bson_append_document_end(&doc_or, &doc_or_item);
			}
			bson_append_array_end(&doc_item, &doc_or);
			bson_append_document_end(&doc_and, &doc_item);

			mysql_free_result(res);
		}

		if (metadata_keys) {
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "label", &doc_item);
			int i;
			bson_t doc_or, doc_or_item;
			char buf[16];
			const char *key;
			size_t keylen;
			BSON_APPEND_ARRAY_BEGIN(&doc_item, "$or", &doc_or);
			for (i = 0; i < metadata_keys_num; i++) {
				keylen = bson_uint32_to_string(i, &key, buf, sizeof buf);
				bson_append_document_begin(&doc_or, key, (int) keylen, &doc_or_item);
				BSON_APPEND_UTF8(&doc_or_item, "label", metadata_keys[i]);
				bson_append_document_end(&doc_or, &doc_or_item);
			}
			bson_append_array_end(&doc_item, &doc_or);
			bson_append_document_end(&doc_and, &doc_item);
		}

		bson_append_array_end(doc, &doc_and);
	}

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_error_t error;
	*num_rows = (int) mongoc_collection_count(collection, MONGOC_QUERY_NONE, doc, 0, 0, NULL, &error);
	if (*num_rows < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document: %s.\n", error.message);
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	if (!*num_rows) {
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_SUCCESS;
	}

	*metadata_list = (char **) calloc(*num_rows * num_fields, sizeof(char *));
	if (!*metadata_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load data.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MEMORY_ERROR;
	}
	// Actual query to retrieve metadata
	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	bson_iter_t iter;
	const bson_t *target;
	int j = 0, k;
	char buf[64];
	const char *key;
	MYSQL_RES *res;
	MYSQL_ROW row;
	while (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {
		k = num_fields * j;
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idmetadatainstance") && BSON_ITER_HOLDS_INT32(&iter)) {
			bson_uint32_to_string(bson_iter_int32(&iter), &key, buf, sizeof buf);
			(*metadata_list)[k] = key ? strdup(key) : NULL;
		}
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "variable") && BSON_ITER_HOLDS_UTF8(&iter))
			(*metadata_list)[k] = bson_iter_utf8(&iter, NULL) ? strdup(bson_iter_utf8(&iter, NULL)) : NULL;
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "label") && BSON_ITER_HOLDS_UTF8(&iter))
			(*metadata_list)[k] = bson_iter_utf8(&iter, NULL) ? strdup(bson_iter_utf8(&iter, NULL)) : NULL;
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idtype") && BSON_ITER_HOLDS_INT32(&iter)) {
			n = snprintf(query, MYSQL_BUFLEN, MONGODB_QUERY_META_GET_TYPE_BY_ID, bson_iter_int32(&iter));
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				return OPH_ODB_MYSQL_ERROR;
			}
			res = mysql_store_result(oDB->conn);
			if (mysql_num_rows(res) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "No row found by query\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if (mysql_field_count(oDB->conn) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if ((row = mysql_fetch_row(res)))
				(*metadata_list)[k] = row[0] ? strdup(row[0]) : NULL;
			mysql_free_result(res);
		}
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "value") && BSON_ITER_HOLDS_UTF8(&iter))
			(*metadata_list)[k] = bson_iter_utf8(&iter, NULL) ? strdup(bson_iter_utf8(&iter, NULL)) : NULL;
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "lastupdate") && BSON_ITER_HOLDS_DATE_TIME(&iter)) {
			time_t timestep = bson_iter_date_time(&iter) / 1000;
			struct tm timeinfo;
			*buf = 0;
			if (localtime_r(&timestep, &timeinfo))
				strftime(buf, 64, "%Y-%m-%d %H:%M:%S", &timeinfo);
			(*metadata_list)[k] = *buf ? strdup(buf) : NULL;
		}
		k++;		// Go to the next field
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idkey") && BSON_ITER_HOLDS_INT32(&iter)) {
			n = snprintf(query, MYSQL_BUFLEN, MONGODB_QUERY_META_GET_VOCABULARY_BY_IDKEY, bson_iter_int32(&iter));
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				return OPH_ODB_MYSQL_ERROR;
			}
			res = mysql_store_result(oDB->conn);
			if (mysql_num_rows(res) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "No row found by query\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if (mysql_field_count(oDB->conn) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
				mongoc_cursor_destroy(cursor);
				mongoc_collection_destroy(collection);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if ((row = mysql_fetch_row(res)))
				(*metadata_list)[k] = row[0] ? strdup(row[0]) : NULL;
			mysql_free_result(res);
		}
		j++;		// Go to the next row
	}

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find elements in management table: %s.\n", error.message);
		mongoc_cursor_destroy(cursor);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (id_metadatainstance)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCES, id_datacube, id_metadatainstance, "", "%", "%", "%", "");
	else {
		char key_filter[MYSQL_BUFLEN];
		*key_filter = 0;
		if (metadata_keys) {
			int m, i;
			char *ptr = key_filter;
			int len = MYSQL_BUFLEN;
			m = snprintf(ptr, len, "AND (");
			for (i = 0; i < metadata_keys_num; i++) {
				ptr += m;
				len -= m;
				m = snprintf(ptr, len, "%smetadatainstance.label='%s'", i ? " OR " : "", metadata_keys[i]);
			}
			ptr += m;
			len -= m;
			snprintf(ptr, len, ")");
		}
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCES, id_datacube, "%", metadata_variable ? "" : "metadatainstance.variable IS NULL OR",
			     metadata_variable ? metadata_variable : "%", metadata_type ? metadata_type : "%", metadata_value ? metadata_value : "%", key_filter);
	}

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res = mysql_store_result(oDB->conn);
	if (!(*num_rows = mysql_num_rows(res))) {
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}

	if (mysql_field_count(oDB->conn) != num_fields) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	*metadata_list = (char **) calloc(*num_rows * num_fields, sizeof(char *));
	if (!*metadata_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load data\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i, j = 0, k;
	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res))) {
		k = num_fields * j++;
		for (i = 0; i < num_fields; i++, k++)
			(*metadata_list)[k] = row[i] ? strdup(row[i]) : NULL;
	}

	mysql_free_result(res);

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_update_metadatainstance_table(ophidiadb * oDB, int id_metadatainstance, int id_datacube, char *metadata_value, int force, const int id_user)
{
	if (!oDB || !id_metadatainstance || !id_datacube || !metadata_value || !id_user) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;

#ifdef OPH_ODB_MNG

	const bson_t *target;

	if (!force) {

		// Select the id_metadatakey associated with input filter
		if (oph_odb_check_connection_to_mongodb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}

		bson_t *doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}

		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);

		mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
		if (!collection) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			bson_destroy(doc);
			return OPH_ODB_MONGODB_ERROR;
		}

		mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
		if (!cursor) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
			bson_destroy(doc);
			mongoc_collection_destroy(collection);
			return OPH_ODB_MONGODB_ERROR;
		}
		bson_destroy(doc);

		char key_filter[MYSQL_BUFLEN];
		*key_filter = 0;
		char *ptr = key_filter;
		int i = 0, m, len = MYSQL_BUFLEN;
		bson_iter_t iter;
		bson_error_t error;
		while ((len > 0) && !mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {
			if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idkey") && BSON_ITER_HOLDS_INT32(&iter)) {
				m = snprintf(ptr, len, "%s%d", i++ ? "," : "", bson_iter_int32(&iter));
				ptr += m;
				len -= m;
			}
		}
		mongoc_collection_destroy(collection);

		if (mongoc_cursor_error(cursor, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve keys: %s.\n", error.message);
			mongoc_cursor_destroy(cursor);
			return OPH_ODB_MONGODB_ERROR;
		}
		mongoc_cursor_destroy(cursor);

		if (i) {
			n = snprintf(insertQuery, MYSQL_BUFLEN, MONGODB_QUERY_META_CHECK_VOCABULARY, key_filter);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}

			if (mysql_query(oDB->conn, insertQuery)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			MYSQL_RES *res = mysql_store_result(oDB->conn);
			if (mysql_num_rows(res)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "The metadata is associated to a vocabulary. Force operation\n");
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			mysql_free_result(res);
		}
	}

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}
	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
	BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);

	bson_t *update = bson_new(), doc_item;
	if (!update) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &doc_item);
	BSON_APPEND_UTF8(&doc_item, "value", metadata_value);
	BSON_APPEND_DATE_TIME(&doc_item, "lastupdate", millisecondsSinceEpoch);
	bson_append_document_end(update, &doc_item);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		bson_destroy(update);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_error_t error;
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_NONE, doc, update, NULL, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update a document: %s.\n", error.message);
		bson_destroy(doc);
		bson_destroy(update);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_destroy(doc);
	bson_destroy(update);
	mongoc_collection_destroy(collection);

#else

	if (!force) {

		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_CHECK_VOCABULARY, id_metadatainstance);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, insertQuery)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		MYSQL_RES *res = mysql_store_result(oDB->conn);
		if (mysql_num_rows(res)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The metadata is associated to a vocabulary. Force operation\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		mysql_free_result(res);
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//escape value
	n = strlen(metadata_value);
	char *escaped_value = (char *) malloc(2 * n + 1);
	if (!escaped_value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for escaped value\n");
		return OPH_ODB_MEMORY_ERROR;
	}
	mysql_real_escape_string(oDB->conn, escaped_value, metadata_value, n);

	int query_buflen = 1 + snprintf(NULL, 0, MYSQL_QUERY_META_UPDATE_INSTANCE, escaped_value, id_metadatainstance, id_datacube);

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		free(escaped_value);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//update metadata
	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_INSTANCE, escaped_value, id_metadatainstance, id_datacube);
	free(escaped_value);

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_delete_from_metadatainstance_table(ophidiadb * oDB, int id_datacube, const char **metadata_keys, int metadata_keys_num, int id_metadatainstance, char *metadata_variable,
						    char *metadata_type, char *metadata_value, int force)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];
	int n = 0;
	char key_filter[MYSQL_BUFLEN];
	*key_filter = 0;

#ifdef OPH_ODB_MNG

	const bson_t *target;

	if (!force) {

		// Select the id_metadatakey associated with input filter
		if (oph_odb_check_connection_to_mongodb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}

		bson_t *doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}

		if (id_metadatainstance)
			BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);
		else {
			bson_t doc_and, doc_item;
			BSON_APPEND_ARRAY_BEGIN(doc, "$and", &doc_and);

			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "iddatacube", &doc_item);
			BSON_APPEND_INT32(&doc_item, "iddatacube", id_datacube);
			bson_append_document_end(&doc_and, &doc_item);

			if (metadata_keys) {
				BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "label", &doc_item);
				int i;
				bson_t doc_or, doc_or_item;
				char buf[16];
				const char *key;
				size_t keylen;
				BSON_APPEND_ARRAY_BEGIN(&doc_item, "$or", &doc_or);
				for (i = 0; i < metadata_keys_num; i++) {
					keylen = bson_uint32_to_string(i, &key, buf, sizeof buf);
					bson_append_document_begin(&doc_or, key, (int) keylen, &doc_or_item);
					BSON_APPEND_UTF8(&doc_or_item, "label", metadata_keys[i]);
					bson_append_document_end(&doc_or, &doc_or_item);
				}
				bson_append_array_end(&doc_item, &doc_or);
				bson_append_document_end(&doc_and, &doc_item);
			}

			bson_append_array_end(doc, &doc_and);
		}

		mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
		if (!collection) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			bson_destroy(doc);
			return OPH_ODB_MONGODB_ERROR;
		}

		mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
		if (!cursor) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
			bson_destroy(doc);
			mongoc_collection_destroy(collection);
			return OPH_ODB_MONGODB_ERROR;
		}
		bson_destroy(doc);

		char *ptr = key_filter;
		int i = 0, m, len = MYSQL_BUFLEN;
		bson_iter_t iter;
		bson_error_t error;
		while ((len > 0) && !mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {
			if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idkey") && BSON_ITER_HOLDS_INT32(&iter)) {
				m = snprintf(ptr, len, "%s%d", i++ ? "," : "", bson_iter_int32(&iter));
				ptr += m;
				len -= m;
			}
		}
		mongoc_collection_destroy(collection);

		if (mongoc_cursor_error(cursor, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve keys: %s.\n", error.message);
			mongoc_cursor_destroy(cursor);
			return OPH_ODB_MONGODB_ERROR;
		}
		mongoc_cursor_destroy(cursor);

		if (i) {
			n = snprintf(query, MYSQL_BUFLEN, MONGODB_QUERY_META_CHECK_VOCABULARY, key_filter);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}

			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			MYSQL_RES *res = mysql_store_result(oDB->conn);
			if (mysql_num_rows(res)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "The metadata is associated to a vocabulary. Force operation\n");
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			mysql_free_result(res);
		}
	}

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	if (id_metadatainstance) {

		BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);

	} else {

		bson_t doc_and, doc_item;
		BSON_APPEND_ARRAY_BEGIN(doc, "$and", &doc_and);

		BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "iddatacube", &doc_item);
		BSON_APPEND_INT32(&doc_item, "iddatacube", id_datacube);
		bson_append_document_end(&doc_and, &doc_item);

		if (metadata_variable) {
			snprintf(key_filter, MYSQL_BUFLEN, "/%s/", metadata_variable);	// Check regex
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "variable", &doc_item);
			BSON_APPEND_UTF8(&doc_item, "variable", key_filter);
			bson_append_document_end(&doc_and, &doc_item);
		}

		if (metadata_value && strcmp(metadata_value, "%")) {
			snprintf(key_filter, MYSQL_BUFLEN, "/%s/", metadata_value);	// Check regex
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "value", &doc_item);
			BSON_APPEND_UTF8(&doc_item, "value", key_filter);
			bson_append_document_end(&doc_and, &doc_item);
		}

		if (metadata_type && strcmp(metadata_type, "%")) {

			n = snprintf(query, MYSQL_BUFLEN, MONGODB_QUERY_META_GET_TYPE, metadata_type);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				bson_destroy(doc);
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				bson_destroy(doc);
				return OPH_ODB_MYSQL_ERROR;
			}

			MYSQL_RES *res;
			MYSQL_ROW row;
			res = mysql_store_result(oDB->conn);
			if (!mysql_num_rows(res)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "No row found by query\n");
				bson_destroy(doc);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if (mysql_field_count(oDB->conn) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
				bson_destroy(doc);
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}

			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "idtype", &doc_item);
			int i = 0;
			bson_t doc_or, doc_or_item;
			char buf[16];
			const char *key;
			size_t keylen;
			BSON_APPEND_ARRAY_BEGIN(&doc_item, "$or", &doc_or);
			while ((row = mysql_fetch_row(res)) && row[0]) {
				keylen = bson_uint32_to_string(i++, &key, buf, sizeof buf);
				bson_append_document_begin(&doc_or, key, (int) keylen, &doc_or_item);
				BSON_APPEND_INT32(&doc_or_item, "idtype", (int) strtol(row[0], NULL, 10));
				bson_append_document_end(&doc_or, &doc_or_item);
			}
			bson_append_array_end(&doc_item, &doc_or);
			bson_append_document_end(&doc_and, &doc_item);

			mysql_free_result(res);
		}

		if (metadata_keys) {
			BSON_APPEND_DOCUMENT_BEGIN(&doc_and, "label", &doc_item);
			int i;
			bson_t doc_or, doc_or_item;
			char buf[16];
			const char *key;
			size_t keylen;
			BSON_APPEND_ARRAY_BEGIN(&doc_item, "$or", &doc_or);
			for (i = 0; i < metadata_keys_num; i++) {
				keylen = bson_uint32_to_string(i, &key, buf, sizeof buf);
				bson_append_document_begin(&doc_or, key, (int) keylen, &doc_or_item);
				BSON_APPEND_UTF8(&doc_or_item, "label", metadata_keys[i]);
				bson_append_document_end(&doc_or, &doc_or_item);
			}
			bson_append_array_end(&doc_item, &doc_or);
			bson_append_document_end(&doc_and, &doc_item);
		}

		bson_append_array_end(doc, &doc_and);
	}

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_error_t error;

	if (!mongoc_collection_remove(collection, MONGOC_REMOVE_NONE, doc, NULL, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to remove a document: %s.\n", error.message);
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_destroy(doc);
	mongoc_collection_destroy(collection);

#else

	if (!id_metadatainstance && metadata_keys) {
		int m, i;
		char *ptr = key_filter;
		int len = MYSQL_BUFLEN;
		m = snprintf(ptr, len, "AND (");
		for (i = 0; (len > 0) && (i < metadata_keys_num); i++) {
			ptr += m;
			len -= m;
			m = snprintf(ptr, len, "%smetadatainstance.label='%s'", i ? " OR " : "", metadata_keys[i]);
		}
		ptr += m;
		len -= m;
		snprintf(ptr, len, ")");
	}

	if (!force) {
		if (oph_odb_check_connection_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
			return OPH_ODB_MYSQL_ERROR;
		}

		if (id_metadatainstance)
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_CHECK_VOCABULARY, id_metadatainstance);
		else
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_CHECK_VOCABULARIES, id_datacube, key_filter);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		MYSQL_RES *res = mysql_store_result(oDB->conn);
		if (mysql_num_rows(res)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The metadata is associated to a vocabulary. Force operation\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		mysql_free_result(res);
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (id_metadatainstance)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_DELETE_INSTANCE, id_metadatainstance, id_datacube);
	else
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_DELETE_INSTANCES, id_datacube, metadata_variable ? "" : "variable IS NULL OR",
			     metadata_variable ? metadata_variable : "%", metadata_type ? metadata_type : "%", metadata_value ? metadata_value : "%", key_filter);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_copy_from_cube_to_cube(ophidiadb * oDB, int id_datacube_input, int id_datacube_output, const int id_user)
{
	if (!oDB || !id_user || !id_datacube_input || !id_datacube_output) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	BSON_APPEND_INT32(doc, "iddatacube", id_datacube_input);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

	char error_flag = 0;
	bson_oid_t oid;
	bson_iter_t iter;
	int id_metadatainstance;

	bson_error_t error;
	const bson_t *target;
	while (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {

		bson_oid_init(&oid, NULL);
		id_metadatainstance = bson_oid_hash(&oid);
		if (!id_metadatainstance) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get anew identifier.\n");
			error_flag = 1;
			break;
		} else if (id_metadatainstance < 0)
			id_metadatainstance = -id_metadatainstance;

		doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			error_flag = 1;
			break;
		}
		BSON_APPEND_OID(doc, "_id", &oid);
		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);
		BSON_APPEND_INT32(doc, "iddatacube", id_datacube_output);
		BSON_APPEND_DATE_TIME(doc, "lastupdate", millisecondsSinceEpoch);
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idtype") && BSON_ITER_HOLDS_INT32(&iter))
			BSON_APPEND_INT32(doc, "idtype", bson_iter_int32(&iter));
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "label") && BSON_ITER_HOLDS_UTF8(&iter))
			BSON_APPEND_UTF8(doc, "label", bson_iter_utf8(&iter, NULL));
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "value") && BSON_ITER_HOLDS_UTF8(&iter))
			BSON_APPEND_UTF8(doc, "value", bson_iter_utf8(&iter, NULL));
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idkey") && BSON_ITER_HOLDS_INT32(&iter))
			BSON_APPEND_INT32(doc, "idkey", bson_iter_int32(&iter));
		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "variable") && BSON_ITER_HOLDS_UTF8(&iter))
			BSON_APPEND_UTF8(doc, "variable", bson_iter_utf8(&iter, NULL));

		if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new document into MongoDB: %s\n", error.message);
			bson_destroy(doc);
			break;
		}
		bson_destroy(doc);
	}

	mongoc_collection_destroy(collection);

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve the instance: %s.\n", error.message);
		mongoc_cursor_destroy(cursor);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

	if (error_flag)
		return OPH_ODB_MONGODB_ERROR;

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_COPY_INSTANCES, id_datacube_input);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_INSERT_INSTANCES, id_datacube_output);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (oph_odb_query_ophidiadb(oDB, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_get(ophidiadb * oDB, int id_datacube, const char *variable, const char *template, int *id_metadata_instance, char **value)
{
	if (!oDB || !id_datacube || !template || (!id_metadata_instance && !value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (id_metadata_instance)
		*id_metadata_instance = 0;
	if (value)
		*value = NULL;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;

#ifdef OPH_ODB_MNG

	int id_metadatakey;

	if (variable)
		n = snprintf(selectQuery, MYSQL_BUFLEN, MONGODB_QUERY_META_GET1, template, variable);
	else
		n = snprintf(selectQuery, MYSQL_BUFLEN, MONGODB_QUERY_META_GET2, template);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	if (!mysql_num_rows(res)) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res))) {
		if (row[0])
			id_metadatakey = (int) strtol(row[0], NULL, 10);
	}

	mysql_free_result(res);

	if (!id_metadatakey) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		return OPH_ODB_SUCCESS;
	}

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
	BSON_APPEND_INT32(doc, "idkey", id_metadatakey);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_iter_t iter;
	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	bson_error_t error;
	const bson_t *target;
	while (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {

		if (id_metadata_instance && bson_iter_init(&iter, target) && bson_iter_find(&iter, "idmetadatainstance") && BSON_ITER_HOLDS_INT32(&iter))
			*id_metadata_instance = bson_iter_int32(&iter);
		if (value && bson_iter_init(&iter, target) && bson_iter_find(&iter, "value") && BSON_ITER_HOLDS_UTF8(&iter))
			*value = bson_iter_utf8(&iter, NULL) ? strdup(bson_iter_utf8(&iter, NULL)) : NULL;
	}

	mongoc_collection_destroy(collection);

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve the instance: %s.\n", error.message);
		mongoc_cursor_destroy(cursor);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

#else

	if (variable)
		n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_GET1, id_datacube, template, variable);
	else
		n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_GET2, id_datacube, template);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	if (!mysql_num_rows(res)) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res))) {
		if (row[0] && id_metadata_instance)
			*id_metadata_instance = (int) strtol(row[0], NULL, 10);
		if (row[1] && value)
			*value = strdup(row[1]);
	}

	mysql_free_result(res);

#endif
	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_put(ophidiadb * oDB, int id_datacube, const char *variable, const char *template, int id_metadata_instance, const char *value, const int id_user)
{
	if (!oDB || !id_datacube || (!template && !id_metadata_instance) || !value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (!id_metadata_instance) {
		int error;
		if ((error = oph_odb_meta_get(oDB, id_datacube, variable, template, &id_metadata_instance, NULL)))
			return error;
	}
#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
#endif

	if (id_metadata_instance)	// update the metadata
	{

#ifdef OPH_ODB_MNG

		bson_t *doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}
		BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadata_instance);

		bson_t *update = bson_new(), doc_item;
		if (!update) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			bson_destroy(doc);
			return OPH_ODB_MONGODB_ERROR;
		}

		BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &doc_item);
		BSON_APPEND_UTF8(&doc_item, "value", value);
		BSON_APPEND_DATE_TIME(&doc_item, "lastupdate", millisecondsSinceEpoch);
		bson_append_document_end(update, &doc_item);

		mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
		if (!collection) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			bson_destroy(doc);
			bson_destroy(update);
			return OPH_ODB_MONGODB_ERROR;
		}

		bson_error_t error;
		if (!mongoc_collection_update(collection, MONGOC_UPDATE_NONE, doc, update, NULL, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update a document: %s.\n", error.message);
			bson_destroy(doc);
			bson_destroy(update);
			mongoc_collection_destroy(collection);
			return OPH_ODB_MONGODB_ERROR;
		}

		bson_destroy(doc);
		bson_destroy(update);
		mongoc_collection_destroy(collection);

#else

		char query[MYSQL_BUFLEN];
		int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_INSTANCE, value, id_metadata_instance, id_datacube);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s in \n%s\n", mysql_error(oDB->conn), query);
			return OPH_ODB_MYSQL_ERROR;
		}
#endif

	} else			// insert a new value for the metadata
	{

#ifdef OPH_ODB_MNG

		if (oph_odb_check_connection_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
			return OPH_ODB_MYSQL_ERROR;
		}
#endif

		int id_metadatatype = 0;
		MYSQL_RES *res;
		MYSQL_ROW row;

		char *metadatakey = NULL;
		if (!(metadatakey = strchr(template, OPH_ODB_META_TEMPLATE_SEPARATOR)))	// Template format is standardVariableName:standardKeyName, so it will extract the standardKeyName
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Template '%s' is not correct.\n", template);
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		metadatakey++;

		char query[MYSQL_BUFLEN];
		int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_METADATATYPE_ID, "text");
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		res = mysql_store_result(oDB->conn);
		if (mysql_num_rows(res) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		if (mysql_field_count(oDB->conn) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		if ((row = mysql_fetch_row(res)))
			id_metadatatype = (int) strtol(row[0], NULL, 10);
		mysql_free_result(res);

#ifdef OPH_ODB_MNG

		bson_oid_t oid;
		bson_oid_init(&oid, NULL);
		int id_metadatainstance = bson_oid_hash(&oid);
		if (!id_metadatainstance) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get anew identifier.\n");
			return OPH_ODB_MONGODB_ERROR;
		} else if (id_metadatainstance < 0)
			id_metadatainstance = -id_metadatainstance;

		bson_t *doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			return OPH_ODB_MONGODB_ERROR;
		}

		BSON_APPEND_OID(doc, "_id", &oid);
		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadatainstance);
		BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
		BSON_APPEND_INT32(doc, "idtype", id_metadatatype);
		BSON_APPEND_UTF8(doc, "value", value);
		BSON_APPEND_UTF8(doc, "label", metadatakey);
		BSON_APPEND_DATE_TIME(doc, "lastupdate", millisecondsSinceEpoch);
		if (variable)
			BSON_APPEND_UTF8(doc, "variable", variable);

		bson_error_t error;
		mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
		if (!collection) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
			bson_destroy(doc);
			return OPH_ODB_MONGODB_ERROR;
		}
		if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new document into MongoDB: %s\n", error.message);
			bson_destroy(doc);
			mongoc_collection_destroy(collection);
			return OPH_ODB_MONGODB_ERROR;
		}

		bson_destroy(doc);
		mongoc_collection_destroy(collection);

#else

		if (variable)
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, value, metadatakey, variable);
		else
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, value, metadatakey);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
#endif

	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_check_for_time_dimension(ophidiadb * oDB, int id_datacube, const char *dimension_name, int *count)
{
	if (!oDB || !id_datacube || !count) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*count = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;

#ifdef OPH_ODB_MNG

	n = snprintf(selectQuery, MYSQL_BUFLEN, MONGODB_QUERY_META_TIME_DIMENSION_CHECK, dimension_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#else

	n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_TIME_DIMENSION_CHECK, id_datacube, dimension_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#endif

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	int _count = 0, id_vocabulary = 0;

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	if (!(n = mysql_num_rows(res))) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
#ifdef OPH_ODB_MNG

	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i = 0, id_metadatakey, _id_metadatakey[n], _id_vocabulary[n];

	while ((row = mysql_fetch_row(res))) {
		_id_metadatakey[i] = row[0] ? (int) strtol(row[0], NULL, 10) : 0;
		_id_vocabulary[i] = row[1] ? (int) strtol(row[1], NULL, 10) : 0;
		i++;
	}
	mysql_free_result(res);

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_iter_t iter;
	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	bson_error_t error;
	const bson_t *target;
	while (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {

		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idkey") && BSON_ITER_HOLDS_INT32(&iter)) {
			id_metadatakey = bson_iter_int32(&iter);
			if (id_metadatakey)
				for (i = 0; i < n; ++i)
					if (id_metadatakey == _id_metadatakey[i]) {
						if (!id_vocabulary) {
							id_vocabulary = _id_vocabulary[i];
							_count = 1;
						} else if (id_vocabulary == _id_vocabulary[i])	// Let us assume that a metadatakey is related to only one vocabulary
							_count++;
						break;
					}
		}
	}

	mongoc_collection_destroy(collection);

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve the instance: %s.\n", error.message);
		mongoc_cursor_destroy(cursor);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

#else

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res))) {
		if (row[0])
			_count = (int) strtol(row[0], NULL, 10);
		if (row[1])
			id_vocabulary = (int) strtol(row[1], NULL, 10);
	}
	mysql_free_result(res);

#endif

	if (!id_vocabulary) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Vocabulary not found\n");
		return OPH_ODB_SUCCESS;
	}

	n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_TIME_DIMENSION_CHECK2, id_vocabulary);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	res = mysql_store_result(oDB->conn);
	if (!mysql_num_rows(res)) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) && row[0] && (_count >= (int) strtol(row[0], NULL, 10)))
		*count = _count;

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_update_metadatakeys(ophidiadb * oDB, int id_datacube, const char *old_variable, const char *new_variable)
{
	if (!oDB || !id_datacube || !old_variable || !new_variable) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	int *id_metadata_instance = NULL, nrows = 0, i = 0;
	int ret = OPH_ODB_SUCCESS;

#ifdef OPH_ODB_MNG

	if (oph_odb_check_connection_to_mongodb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_t *doc = bson_new();
	if (!doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
		return OPH_ODB_MONGODB_ERROR;
	}

	BSON_APPEND_INT32(doc, "iddatacube", id_datacube);
	BSON_APPEND_UTF8(doc, "variable", old_variable);

	mongoc_collection_t *collection = mongoc_client_get_collection(oDB->mng_conn, oDB->mng_name, OPH_ODB_MNGDB_COLL_METADATAINSTANCE);
	if (!collection) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to MongoDB.\n");
		bson_destroy(doc);
		return OPH_ODB_MONGODB_ERROR;
	}

	bson_error_t error;
	nrows = (int) mongoc_collection_count(collection, MONGOC_QUERY_NONE, doc, 0, 0, NULL, &error);
	if (nrows < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document: %s.\n", error.message);
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}

	if (!nrows) {
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_SUCCESS;
	}

	id_metadata_instance = (int *) calloc(nrows, sizeof(int));
	if (!id_metadata_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for metadata instance ids\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MEMORY_ERROR;
	}

	bson_iter_t iter;
	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, doc, NULL, NULL);
	if (!cursor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find any document.\n");
		bson_destroy(doc);
		mongoc_collection_destroy(collection);
		return OPH_ODB_MONGODB_ERROR;
	}
	bson_destroy(doc);

	const bson_t *target;
	while (!mongoc_cursor_error(cursor, &error) && mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &target)) {

		if (bson_iter_init(&iter, target) && bson_iter_find(&iter, "idmetadatainstance") && BSON_ITER_HOLDS_INT32(&iter))
			id_metadata_instance[i++] = bson_iter_int32(&iter);
	}

	if (mongoc_cursor_error(cursor, &error)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve the instance: %s.\n", error.message);
		mongoc_collection_destroy(collection);
		mongoc_cursor_destroy(cursor);
		if (id_metadata_instance)
			free(id_metadata_instance);
		return OPH_ODB_MONGODB_ERROR;
	}
	mongoc_cursor_destroy(cursor);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long) (tv.tv_sec) * 1000 + (unsigned long long) (tv.tv_usec) / 1000;

	bson_t *update = NULL, doc_item;

	for (i = 0; i < nrows; ++i) {

		doc = bson_new();
		if (!doc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			break;
		}

		BSON_APPEND_INT32(doc, "idmetadatainstance", id_metadata_instance[i]);

		update = bson_new();
		if (!update) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a query for MongoDB.\n");
			bson_destroy(doc);
			break;
		}

		BSON_APPEND_DOCUMENT_BEGIN(update, "$set", &doc_item);
		BSON_APPEND_UTF8(&doc_item, "variable", new_variable);
		BSON_APPEND_DATE_TIME(&doc_item, "lastupdate", millisecondsSinceEpoch);
		bson_append_document_end(update, &doc_item);

		if (!mongoc_collection_update(collection, MONGOC_UPDATE_NONE, doc, update, NULL, &error)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update a document: %s.\n", error.message);
			bson_destroy(doc);
			bson_destroy(update);
			break;
		}

		bson_destroy(doc);
		bson_destroy(update);
	}

	mongoc_collection_destroy(collection);

	if (i < nrows)
		ret = OPH_ODB_MONGODB_ERROR;

#else

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_KEY_OF_INSTANCE, id_datacube, old_variable);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res = mysql_store_result(oDB->conn);

	nrows = mysql_num_rows(res);
	if (!nrows) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	id_metadata_instance = (int *) calloc(nrows, sizeof(int));
	if (!id_metadata_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for metadata instance ids\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res))) {
		if (!row[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Found an empty row\n");
			ret = OPH_ODB_MYSQL_ERROR;
			break;
		}
		id_metadata_instance[i++] = (int) strtol(row[0], NULL, 10);
	}

	mysql_free_result(res);

	if (ret == OPH_ODB_SUCCESS) {

		for (i = 0; i < nrows; ++i) {

			n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_KEY_OF_INSTANCE, new_variable, id_metadata_instance[i]);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				ret = OPH_ODB_STR_BUFF_OVERFLOW;
				break;
			}
			if (mysql_query(oDB->conn, selectQuery)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				ret = OPH_ODB_MYSQL_ERROR;
				break;
			}
		}
	}
#endif

	if (id_metadata_instance)
		free(id_metadata_instance);

	return ret;
}
