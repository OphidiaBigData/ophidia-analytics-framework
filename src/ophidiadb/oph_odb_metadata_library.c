/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#include "oph_odb_cube_library.h"
#include "oph_pid_library.h"

extern int msglevel;

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
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, new_metadatakey,
				     new_metadatakey_variable);
		else
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, new_metadatakey);
	} else {
		if (new_metadatakey_variable)
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey,
				     new_metadatakey_variable);
		else
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, new_metadatakey);
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

#ifdef OPH_DB_SUPPORT

	//Insert manage relation
	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_MANAGE, id_user, metadatainstance_id);
	if (n >= MYSQL_BUFLEN) {
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

	if ((row = mysql_fetch_row(res)))
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

	if ((row = mysql_fetch_row(res)))
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
						    int *last_insertd_id)
{
	if (!oDB || !id_datacube || !id_metadatatype || !metadata_value || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (!id_metadatakey && !metadata_key) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

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
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key,
				     metadata_variable);
		else
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4, id_datacube, id_metadatakey, id_metadatatype, escaped_value, metadata_key);
	} else {
		if (metadata_variable)
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_metadatatype, escaped_value, metadata_key, metadata_variable);
		else
			n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_metadatatype, escaped_value, metadata_key);
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

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_insert_into_manage_table(ophidiadb * oDB, int id_metadatainstance, int id_user)
{
	if (!oDB || !id_user || !id_metadatainstance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_DB_SUPPORT

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_MANAGE, id_user, id_metadatainstance);
	if (n >= MYSQL_BUFLEN) {
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

int oph_odb_meta_find_complete_metadata_list(ophidiadb * oDB, int id_datacube, const char **metadata_keys, int metadata_keys_num, char *id_metadatainstance, char *metadata_variable,
					     char *metadata_type, char *metadata_value, MYSQL_RES ** metadata_list)
{
	(*metadata_list) = NULL;

	if (!oDB || !id_datacube || !metadata_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = 0;

	if (id_metadatainstance)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCES, id_datacube, id_metadatainstance, "", "%", "%", "%", "");
	else {
		if (!metadata_keys) {
			//read all keys
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCES, id_datacube, "%", metadata_variable ? "" : "metadatainstance.variable IS NULL OR",
				     metadata_variable ? metadata_variable : "%", metadata_type ? metadata_type : "%", metadata_value ? metadata_value : "%", "");
		} else {
			//read a group of keys
			char key_filter[MYSQL_BUFLEN];
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

			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCES, id_datacube, "%", metadata_variable ? "" : "metadatainstance.variable IS NULL OR",
				     metadata_variable ? metadata_variable : "%", metadata_type ? metadata_type : "%", metadata_value ? metadata_value : "%", key_filter);
		}
	}

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	*metadata_list = mysql_store_result(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_update_metadatainstance_table(ophidiadb * oDB, int id_metadatainstance, int id_datacube, char *metadata_value, int force)
{
	if (!oDB || !id_metadatainstance || !id_datacube || !metadata_value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	int n;

	if (!force) {

		char insertQuery[MYSQL_BUFLEN];
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
	char insertQuery[query_buflen];
	n = snprintf(insertQuery, query_buflen, MYSQL_QUERY_META_UPDATE_INSTANCE, escaped_value, id_metadatainstance, id_datacube);

	free(escaped_value);

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

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
	if (!id_metadatainstance && metadata_keys) {
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

	if (!force) {
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

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_copy_from_cube_to_cube(ophidiadb * oDB, int id_datacube_input, int id_datacube_output, int id_user)
{
	if (!oDB || !id_user || !id_datacube_input || !id_datacube_output) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

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
#ifdef OPH_DB_SUPPORT

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_COPY_MANAGE, id_user, id_datacube_output);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
#endif

	int idmissingvalue = 0;
	char measure_name[OPH_COMMON_BUFFER_LEN];
	if (oph_odb_cube_retrieve_missingvalue(oDB, id_datacube_input, &idmissingvalue, measure_name))
		return OPH_ODB_MYSQL_ERROR;

	if (idmissingvalue) {
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_ID_BY_NAME, OPH_COMMON_FILLVALUE, measure_name, id_datacube_output);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}

		if (mysql_query(oDB->conn, insertQuery)) {
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

		if ((row = mysql_fetch_row(res)))
			idmissingvalue = strtol(row[0], NULL, 10);

		mysql_free_result(res);

		if (oph_odb_cube_update_missingvalue(oDB, id_datacube_output, idmissingvalue))
			return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_retrieve_single_metadata_instance(ophidiadb * oDB, int id_metadata_instance, char **type, char **value)
{
	if (!oDB || !id_metadata_instance || !type || !value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*type = NULL;
	*value = NULL;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_READ_INSTANCE, id_metadata_instance);
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
	if (mysql_field_count(oDB->conn) != 5) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res))) {
		if (row[3])
			*type = strdup(row[3]);
		if (row[4])
			*value = strdup(row[4]);
	}

	mysql_free_result(res);
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
	return OPH_ODB_SUCCESS;
}

int oph_odb_meta_put(ophidiadb * oDB, int id_datacube, const char *variable, const char *template, int id_metadata_instance, const char *value)
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

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = 0;
	if (id_metadata_instance)	// update the metadata
	{
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_INSTANCE, value, id_metadata_instance, id_datacube);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s in \n%s\n", mysql_error(oDB->conn), query);
			return OPH_ODB_MYSQL_ERROR;
		}
	} else			// insert a new value for the metadata
	{
		int id_type = 0;
		MYSQL_RES *res;
		MYSQL_ROW row;

		char *label;
		if (!(label = strchr(template, OPH_ODB_META_TEMPLATE_SEPARATOR)))	// Template format is standardVariableName:standardKeyName, so it will extract the standardKeyName
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Template '%s' is not correct.\n", template);
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		label++;

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
			id_type = (int) strtol(row[0], NULL, 10);
		mysql_free_result(res);

		if (variable)
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1, id_datacube, id_type, value, label, variable);
		else
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2, id_datacube, id_type, value, label);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
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
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_TIME_DIMENSION_CHECK, id_datacube, dimension_name);
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

	int _count = 0, id_vocabulary = 0;
	if ((row = mysql_fetch_row(res))) {
		if (row[0])
			_count = (int) strtol(row[0], NULL, 10);
		if (row[1])
			id_vocabulary = (int) strtol(row[1], NULL, 10);
	}
	mysql_free_result(res);

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

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;
	n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_RETRIEVE_KEY_OF_INSTANCE, id_datacube, old_variable);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res = mysql_store_result(oDB->conn);

	int nrows = mysql_num_rows(res);
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

	int *id_metadata_instance = (int *) calloc(nrows, sizeof(int));
	if (!id_metadata_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for metadata instance ids\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i = 0, ret = OPH_ODB_SUCCESS;
	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res))) {
		if (!row[0] || !row[1]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Found an empty row\n");
			ret = OPH_ODB_MYSQL_ERROR;
			break;
		}
		id_metadata_instance[i] = (int) strtol(row[0], NULL, 10);
		i++;
	}

	mysql_free_result(res);

	if (ret == OPH_ODB_SUCCESS) {

		int j;
		for (j = 0; j < nrows; ++j) {

			n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_META_UPDATE_KEY_OF_INSTANCE, new_variable, id_metadata_instance[j]);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				ret = OPH_ODB_STR_BUFF_OVERFLOW;
			} else if (mysql_query(oDB->conn, selectQuery)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				ret = OPH_ODB_MYSQL_ERROR;
			}
		}
	}

	if (id_metadata_instance)
		free(id_metadata_instance);

	return ret;
}
