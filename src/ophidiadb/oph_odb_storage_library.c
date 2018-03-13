/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#include "oph_odb_storage_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mysql.h>
#include "debug.h"

#include "oph_idstring_library.h"

extern int msglevel;

int oph_odb_stge_init_fragment_list(oph_odb_fragment_list * frag)
{
	if (!frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	frag->value = NULL;
	frag->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_init_fragment_list2(oph_odb_fragment_list2 * frag)
{
	if (!frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	frag->value = NULL;
	frag->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_free_fragment_list(oph_odb_fragment_list * frag)
{
	if (!frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (frag->value) {
		free(frag->value);
		frag->value = NULL;
	}
	frag->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_free_fragment_list2(oph_odb_fragment_list2 * frag)
{
	if (!frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (frag->value) {
		free(frag->value);
		frag->value = NULL;
	}
	frag->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_init_db_list(oph_odb_db_instance_list * db)
{
	if (!db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	db->value = NULL;
	db->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_free_db_list(oph_odb_db_instance_list * db)
{
	if (!db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (db->value) {
		free(db->value);
		db->value = NULL;
	}
	db->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_init_dbms_list(oph_odb_dbms_instance_list * dbms)
{
	if (!dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	dbms->value = NULL;
	dbms->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_free_dbms_list(oph_odb_dbms_instance_list * dbms)
{
	if (!dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (dbms->value) {
		free(dbms->value);
		dbms->value = NULL;
	}
	dbms->size = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_dbmsinstance(ophidiadb * oDB, int id_dbms, oph_odb_dbms_instance * dbms)
{
	if (!oDB || !dbms || !id_dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DBMS, id_dbms);
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
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 7) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		dbms->id_dbms = (int) strtol(row[0], NULL, 10);
		memset(&(dbms->hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
		strncpy(dbms->hostname, row[1], OPH_ODB_STGE_HOST_NAME_SIZE);
		memset(&(dbms->login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
		strncpy(dbms->login, row[2], OPH_ODB_STGE_LOGIN_SIZE);
		memset(&(dbms->pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
		strncpy(dbms->pwd, row[3], OPH_ODB_STGE_PWD_SIZE);
		dbms->port = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		dbms->fs_type = (row[5] ? (int) strtol(row[5], NULL, 10) : 0);
		memset(&(dbms->io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
		strncpy(dbms->io_server_type, row[6], OPH_ODB_STGE_SERVER_NAME_SIZE);
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_first_dbmsinstance(ophidiadb * oDB, oph_odb_dbms_instance * dbms)
{
	if (!oDB || !dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FIRST_DBMS);
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
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int id_dbms = 0;
	if ((row = mysql_fetch_row(res)) != NULL) {
		id_dbms = (int) strtol(row[0], NULL, 10);
	}
	mysql_free_result(res);

	return oph_odb_stge_retrieve_dbmsinstance(oDB, id_dbms, dbms);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_fetch_db_connection_string(ophidiadb * oDB, int datacube_id, int start_position, int row_number, oph_odb_db_instance_list * dbs, oph_odb_dbms_instance_list * dbmss)
{
	if (!oDB || !dbs || !dbmss || !datacube_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_db_list(dbs);
	oph_odb_stge_init_dbms_list(dbmss);

	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DB_CONNECTION, datacube_id, start_position, row_number);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 8) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i_db = 0;
	int i_dbms = 0;

	if (!((dbs->value) = (oph_odb_db_instance *) calloc(num_rows, sizeof(oph_odb_db_instance)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	dbs->size = num_rows;

	int curr_dbms_id = 0;
	oph_odb_dbms_instance *curr_dbms = NULL;

	//Fill connection strings structures
	while ((row = mysql_fetch_row(res))) {
		//Found new DB
		dbs->value[i_db].id_db = (int) strtol(row[6], NULL, 10);
		dbs->value[i_db].id_dbms = (int) strtol(row[0], NULL, 10);
		memset(&(dbs->value[i_db].db_name), 0, OPH_ODB_STGE_DB_NAME_SIZE + 1);
		strncpy(dbs->value[i_db].db_name, row[7], OPH_ODB_STGE_DB_NAME_SIZE);

		//Found new DBMS
		if (curr_dbms_id != (int) strtol(row[0], NULL, 10)) {
			if (!(curr_dbms = (oph_odb_dbms_instance *) realloc((dbmss->value), (i_dbms + 1) * sizeof(oph_odb_dbms_instance)))) {
				oph_odb_stge_free_db_list(dbs);
				oph_odb_stge_free_dbms_list(dbmss);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				mysql_free_result(res);
				return OPH_ODB_MEMORY_ERROR;
			}
			dbmss->value = curr_dbms;
			dbmss->size++;
			dbmss->value[i_dbms].id_dbms = (int) strtol(row[0], NULL, 10);
			memset(&(dbmss->value[i_dbms].login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
			strncpy(dbmss->value[i_dbms].login, row[1], OPH_ODB_STGE_LOGIN_SIZE);
			memset(&(dbmss->value[i_dbms].pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
			strncpy(dbmss->value[i_dbms].pwd, row[2], OPH_ODB_STGE_PWD_SIZE);
			dbmss->value[i_dbms].port = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
			memset(&(dbmss->value[i_dbms].hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
			strncpy(dbmss->value[i_dbms].hostname, row[4], OPH_ODB_STGE_HOST_NAME_SIZE);
			memset(&(dbmss->value[i_dbms].io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
			strncpy(dbmss->value[i_dbms].io_server_type, row[5], OPH_ODB_STGE_SERVER_NAME_SIZE);
			//Set current dbms
			curr_dbms_id = dbmss->value[i_dbms].id_dbms;
			curr_dbms = &(dbmss->value[i_dbms]);
			i_dbms++;
		}
		i_db++;
	}

	mysql_free_result(res);

	//Link DBs to DBMSs
	int i, j;
	for (i = 0; i < dbmss->size; i++) {
		for (j = 0; j < dbs->size; j++) {
			if (dbs->value[j].id_dbms == dbmss->value[i].id_dbms)
				dbs->value[j].dbms_instance = &(dbmss->value[i]);
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_fetch_dbms_connection_string(ophidiadb * oDB, int id_datacube, int start_position, int row_number, oph_odb_dbms_instance_list * dbmss)
{
	if (!oDB || !dbmss || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_dbms_list(dbmss);

	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DBMS_CONNECTION, id_datacube, start_position, row_number);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 6) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i_dbms = 0;

	if (!((dbmss->value) = (oph_odb_dbms_instance *) calloc(num_rows, sizeof(oph_odb_dbms_instance)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	dbmss->size = num_rows;

	//Fill connection strings structures
	while ((row = mysql_fetch_row(res))) {
		dbmss->value[i_dbms].id_dbms = (int) strtol(row[0], NULL, 10);
		memset(&(dbmss->value[i_dbms].login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
		strncpy(dbmss->value[i_dbms].login, row[1], OPH_ODB_STGE_LOGIN_SIZE);
		memset(&(dbmss->value[i_dbms].pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
		strncpy(dbmss->value[i_dbms].pwd, row[2], OPH_ODB_STGE_PWD_SIZE);
		dbmss->value[i_dbms].port = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		memset(&(dbmss->value[i_dbms].hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
		strncpy(dbmss->value[i_dbms].hostname, row[4], OPH_ODB_STGE_HOST_NAME_SIZE);
		memset(&(dbmss->value[i_dbms].io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
		strncpy(dbmss->value[i_dbms].io_server_type, row[5], OPH_ODB_STGE_SERVER_NAME_SIZE);
		i_dbms++;
	}

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_fetch_fragment_connection_string(ophidiadb * oDB, int id_datacube, char *fragrelindexset, oph_odb_fragment_list * frags, oph_odb_db_instance_list * dbs,
						  oph_odb_dbms_instance_list * dbmss)
{
	if (!oDB || !frags || !dbs || !dbmss || !id_datacube || !fragrelindexset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	frags->size = 0;
	dbs->size = 0;
	dbmss->size = 0;

	char query[MYSQL_BUFLEN];
	char condition[MYSQL_BUFLEN];
	memset(condition, 0, MYSQL_BUFLEN);

	//Prepare query statement
	char *start, *end, *hyphen, *cond_ptr;
	char buffer[10];
	int a, b, len, total_len = 0;

	start = fragrelindexset;
	cond_ptr = condition;
	do {
		len = 0;
		//Fetch till ; or fetch all string
		end = strchr(start, OPH_IDS_SEMICOLON_CHAR);
		if (end != NULL) {
			strncpy(buffer, start, (end - start));
			buffer[(end - start)] = '\0';
		} else {
			strncpy(buffer, start, (strlen(start) + 1));
		}

		//Check if single value or range
		hyphen = strchr(buffer, OPH_IDS_HYPHEN_CHAR);
		if (hyphen == NULL) {
			a = (int) strtol(buffer, NULL, 10);
			if (a == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error converting ASCII to INT\n");
				return OPH_ODB_ERROR;
			}
			//Do k = a
			len = snprintf(NULL, 0, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1, a);
			total_len += len;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, len + 1, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1, a);
		} else {
			*hyphen = '\0';
			a = (int) strtol(buffer, NULL, 10);
			b = (int) strtol(hyphen + 1, NULL, 10);
			if (a == 0 || b == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error converting ASCII to INT\n");
				return OPH_ODB_ERROR;
			}
			//Do k >= a AND k<= b
			len = snprintf(NULL, 0, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2, a, b);
			total_len += len;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, len + 1, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2, a, b);
		}
		start = end + 1;
		cond_ptr += len;
		if (end) {
			//Add OR
			total_len += 4;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, 5, " OR ");
			cond_ptr += 4;
		}
	} while (end);

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_fragment_list(frags);
	oph_odb_stge_init_db_list(dbs);
	oph_odb_stge_init_dbms_list(dbmss);

	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG_CONNECTION, condition, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 14) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i_frag = 0;
	int i_db = 0;
	int i_dbms = 0;

	if (!((frags->value) = (oph_odb_fragment *) calloc(num_rows, sizeof(oph_odb_fragment)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	frags->size = num_rows;

	int curr_db_id = 0;
	int curr_dbms_id = 0;
	oph_odb_db_instance *curr_db = NULL;
	oph_odb_dbms_instance *curr_dbms = NULL;

	//Fill connection strings structures
	while ((row = mysql_fetch_row(res))) {
		// Fragment
		frags->value[i_frag].frag_relative_index = (int) strtol(row[2], NULL, 10);
		frags->value[i_frag].id_datacube = (int) strtol(row[1], NULL, 10);
		frags->value[i_frag].id_db = (int) strtol(row[12], NULL, 10);
		memset(&(frags->value[i_frag].fragment_name), 0, OPH_ODB_STGE_FRAG_NAME_SIZE + 1);
		strncpy(frags->value[i_frag].fragment_name, row[0], OPH_ODB_STGE_FRAG_NAME_SIZE);
		frags->value[i_frag].id_fragment = (int) strtol(row[3], NULL, 10);
		frags->value[i_frag].key_start = (int) strtol(row[4], NULL, 10);
		frags->value[i_frag].key_end = (int) strtol(row[5], NULL, 10);

		//Found new DB
		if (curr_db_id != (row[12] ? (int) strtol(row[12], NULL, 10) : 0)) {
			if (!(curr_db = (oph_odb_db_instance *) realloc((dbs->value), (i_db + 1) * sizeof(oph_odb_db_instance)))) {
				oph_odb_stge_free_fragment_list(frags);
				oph_odb_stge_free_db_list(dbs);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				mysql_free_result(res);
				return OPH_ODB_MEMORY_ERROR;
			}
			dbs->value = curr_db;
			dbs->size++;
			dbs->value[i_db].id_db = (int) strtol(row[12], NULL, 10);
			dbs->value[i_db].id_dbms = (int) strtol(row[6], NULL, 10);
			memset(&(dbs->value[i_db].db_name), 0, OPH_ODB_STGE_DB_NAME_SIZE + 1);
			strncpy(dbs->value[i_db].db_name, row[13], OPH_ODB_STGE_DB_NAME_SIZE);

			//Found new DBMS
			if (curr_dbms_id != (row[6] ? (int) strtol(row[6], NULL, 10) : 0)) {
				if (!(curr_dbms = (oph_odb_dbms_instance *) realloc((dbmss->value), (i_dbms + 1) * sizeof(oph_odb_dbms_instance)))) {
					oph_odb_stge_free_fragment_list(frags);
					oph_odb_stge_free_db_list(dbs);
					oph_odb_stge_free_dbms_list(dbmss);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					mysql_free_result(res);
					return OPH_ODB_MEMORY_ERROR;
				}
				dbmss->value = curr_dbms;
				dbmss->size++;
				dbmss->value[i_dbms].id_dbms = (int) strtol(row[6], NULL, 10);
				memset(&(dbmss->value[i_dbms].login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
				strncpy(dbmss->value[i_dbms].login, row[7], OPH_ODB_STGE_LOGIN_SIZE);
				memset(&(dbmss->value[i_dbms].pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
				strncpy(dbmss->value[i_dbms].pwd, row[8], OPH_ODB_STGE_PWD_SIZE);
				dbmss->value[i_dbms].port = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
				memset(&(dbmss->value[i_dbms].hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
				strncpy(dbmss->value[i_dbms].hostname, row[10], OPH_ODB_STGE_HOST_NAME_SIZE);
				memset(&(dbmss->value[i_dbms].io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
				strncpy(dbmss->value[i_dbms].io_server_type, row[11], OPH_ODB_STGE_SERVER_NAME_SIZE);
				//Set current dbms
				curr_dbms_id = dbmss->value[i_dbms].id_dbms;
				curr_dbms = &(dbmss->value[i_dbms]);
				i_dbms++;
			}
			//Set current db
			curr_db_id = dbs->value[i_db].id_db;
			curr_db = &(dbs->value[i_db]);
			i_db++;
		}
		i_frag++;
	}
	mysql_free_result(res);

	//Link DBs to DBMSs
	int i, j, k;
	for (i = 0; i < dbmss->size; i++) {
		for (j = 0; j < dbs->size; j++) {
			if (dbs->value[j].id_dbms == dbmss->value[i].id_dbms)
				dbs->value[j].dbms_instance = &(dbmss->value[i]);
			for (k = 0; k < frags->size; k++) {
				if (dbs->value[j].id_db == frags->value[k].id_db)
					frags->value[k].db_instance = &(dbs->value[j]);
			}
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_fetch_fragment_connection_string_for_deletion(ophidiadb * oDB, int id_datacube, char *fragrelindexset, oph_odb_fragment_list * frags, oph_odb_db_instance_list * dbs,
							       oph_odb_dbms_instance_list * dbmss)
{
	if (!oDB || !frags || !dbs || !dbmss || !id_datacube || !fragrelindexset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	frags->size = 0;
	dbs->size = 0;
	dbmss->size = 0;

	char query[MYSQL_BUFLEN];
	char condition[MYSQL_BUFLEN];
	memset(condition, 0, MYSQL_BUFLEN);

	//Prepare query statement
	char *start, *end, *hyphen, *cond_ptr;
	char buffer[10];
	int a, b, len, total_len = 0;

	start = fragrelindexset;
	cond_ptr = condition;
	do {
		len = 0;
		//Fetch till ; or fetch all string
		end = strchr(start, OPH_IDS_SEMICOLON_CHAR);
		if (end != NULL) {
			strncpy(buffer, start, (end - start));
			buffer[(end - start)] = '\0';
		} else {
			strncpy(buffer, start, (strlen(start) + 1));
		}

		//Check if single value or range
		hyphen = strchr(buffer, OPH_IDS_HYPHEN_CHAR);
		if (hyphen == NULL) {
			a = (int) strtol(buffer, NULL, 10);
			if (a == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error converting ASCII to INT\n");
				return OPH_ODB_ERROR;
			}
			//Do k = a
			len = snprintf(NULL, 0, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1, a);
			total_len += len;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, len + 1, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1, a);
		} else {
			*hyphen = '\0';
			a = (int) strtol(buffer, NULL, 10);
			b = (int) strtol(hyphen + 1, NULL, 10);
			if (a == 0 || b == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error converting ASCII to INT\n");
				return OPH_ODB_ERROR;
			}
			//Do k >= a AND k<= b
			len = snprintf(NULL, 0, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2, a, b);
			total_len += len;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, len + 1, MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2, a, b);
		}
		start = end + 1;
		cond_ptr += len;
		if (end) {
			//Add OR
			total_len += 4;
			if (total_len >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Length of substring exceed limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			snprintf(cond_ptr, 5, " OR ");
			cond_ptr += 4;
		}
	} while (end);

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_fragment_list(frags);
	oph_odb_stge_init_db_list(dbs);
	oph_odb_stge_init_dbms_list(dbmss);

	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG_CONNECTION, condition, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}

	if (mysql_field_count(oDB->conn) != 14) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i_frag = 0;
	int i_db = 0;
	int i_dbms = 0;

	if (!((frags->value) = (oph_odb_fragment *) calloc(num_rows, sizeof(oph_odb_fragment)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	frags->size = num_rows;

	int curr_db_id = 0;
	int curr_dbms_id = 0;
	oph_odb_db_instance *curr_db = NULL;
	oph_odb_dbms_instance *curr_dbms = NULL;

	//Fill connection strings structures
	while ((row = mysql_fetch_row(res))) {
		// Fragment
		frags->value[i_frag].frag_relative_index = (int) strtol(row[2], NULL, 10);
		frags->value[i_frag].id_datacube = (int) strtol(row[1], NULL, 10);
		frags->value[i_frag].id_db = (int) strtol(row[12], NULL, 10);
		memset(&(frags->value[i_frag].fragment_name), 0, OPH_ODB_STGE_FRAG_NAME_SIZE + 1);
		strncpy(frags->value[i_frag].fragment_name, row[0], OPH_ODB_STGE_FRAG_NAME_SIZE);
		frags->value[i_frag].id_fragment = (int) strtol(row[3], NULL, 10);
		frags->value[i_frag].key_start = (int) strtol(row[4], NULL, 10);
		frags->value[i_frag].key_end = (int) strtol(row[5], NULL, 10);

		//Found new DB
		if (curr_db_id != (row[12] ? (int) strtol(row[12], NULL, 10) : 0)) {
			if (!(curr_db = (oph_odb_db_instance *) realloc((dbs->value), (i_db + 1) * sizeof(oph_odb_db_instance)))) {
				oph_odb_stge_free_fragment_list(frags);
				oph_odb_stge_free_db_list(dbs);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				mysql_free_result(res);
				return OPH_ODB_MEMORY_ERROR;
			}
			dbs->value = curr_db;
			dbs->size++;
			dbs->value[i_db].id_db = (int) strtol(row[12], NULL, 10);
			dbs->value[i_db].id_dbms = (int) strtol(row[6], NULL, 10);
			memset(&(dbs->value[i_db].db_name), 0, OPH_ODB_STGE_DB_NAME_SIZE + 1);
			strncpy(dbs->value[i_db].db_name, row[13], OPH_ODB_STGE_DB_NAME_SIZE);

			//Found new DBMS
			if (curr_dbms_id != (row[6] ? (int) strtol(row[6], NULL, 10) : 0)) {
				if (!(curr_dbms = (oph_odb_dbms_instance *) realloc((dbmss->value), (i_dbms + 1) * sizeof(oph_odb_dbms_instance)))) {
					oph_odb_stge_free_fragment_list(frags);
					oph_odb_stge_free_db_list(dbs);
					oph_odb_stge_free_dbms_list(dbmss);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					mysql_free_result(res);
					return OPH_ODB_MEMORY_ERROR;
				}
				dbmss->value = curr_dbms;
				dbmss->size++;
				dbmss->value[i_dbms].id_dbms = (int) strtol(row[6], NULL, 10);
				memset(&(dbmss->value[i_dbms].login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
				strncpy(dbmss->value[i_dbms].login, row[7], OPH_ODB_STGE_LOGIN_SIZE);
				memset(&(dbmss->value[i_dbms].pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
				strncpy(dbmss->value[i_dbms].pwd, row[8], OPH_ODB_STGE_PWD_SIZE);
				dbmss->value[i_dbms].port = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
				memset(&(dbmss->value[i_dbms].hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
				strncpy(dbmss->value[i_dbms].hostname, row[10], OPH_ODB_STGE_HOST_NAME_SIZE);
				memset(&(dbmss->value[i_dbms].io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
				strncpy(dbmss->value[i_dbms].io_server_type, row[11], OPH_ODB_STGE_SERVER_NAME_SIZE);
				//Set current dbms
				curr_dbms_id = dbmss->value[i_dbms].id_dbms;
				curr_dbms = &(dbmss->value[i_dbms]);
				i_dbms++;
			}
			//Set current db
			curr_db_id = dbs->value[i_db].id_db;
			curr_db = &(dbs->value[i_db]);
			i_db++;
		}
		i_frag++;
	}
	mysql_free_result(res);

	//Link DBs to DBMSs
	int i, j, k;
	for (i = 0; i < dbmss->size; i++) {
		for (j = 0; j < dbs->size; j++) {
			if (dbs->value[j].id_dbms == dbmss->value[i].id_dbms)
				dbs->value[j].dbms_instance = &(dbmss->value[i]);
			for (k = 0; k < frags->size; k++) {
				if (dbs->value[j].id_db == frags->value[k].id_db)
					frags->value[k].db_instance = &(dbs->value[j]);
			}
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_count_number_of_host_dbms(ophidiadb * oDB, int fs_type, char *ioserver_type, char *host_partition, int *host_number, int *dbmsxhost_number)
{
	if (!oDB || !host_number || !dbmsxhost_number || !host_partition || fs_type < 0 || fs_type > 2 || !ioserver_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_COUNT_HOST_DBMS, host_partition, fs_type, ioserver_type);
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
	int num = 0;
	if ((num = mysql_num_rows(res)) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res))) {
		*host_number = (int) strtol((row[0] ? row[0] : "0"), NULL, 10);
		*dbmsxhost_number = (int) strtol((row[1] ? row[1] : "0"), NULL, 10);
	}

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_get_default_host_partition_fs(ophidiadb * oDB, int *fs_type, char *ioserver_type, char **host_partition, int host_number, int dbmsxhost_number, int *exist)
{
	if (!oDB || !host_number || !dbmsxhost_number || !host_partition || !exist || !fs_type || !ioserver_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*exist = 0;

	//Check if arguments are admissible
	short int partition_default = (!strncmp(*host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(*host_partition))
				       && !strncmp(*host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(OPH_COMMON_HOSTPARTITION_DEFAULT)) ? 1 : 0);
	short int fs_default = (*fs_type == OPH_COMMON_IO_FS_DEFAULT_TYPE ? 1 : 0);

	//At least one default value and Fs type should be between -1 and 2
	if ((!partition_default && !fs_default) || (*fs_type < -1 || *fs_type > 2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Default arguments not valid.\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];

	char tmp_fs_type[2] = { '\0' };
	if (*fs_type == OPH_COMMON_IO_FS_DEFAULT_TYPE) {
		snprintf(tmp_fs_type, 2, "%s", "%");
	} else {
		snprintf(tmp_fs_type, 2, "%d", *fs_type);
	}

	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_HOSTPARTITION_FS, (partition_default ? "%" : *host_partition), tmp_fs_type, ioserver_type, dbmsxhost_number, host_number);
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
	int num = 0;
	if ((num = mysql_num_rows(res)) != 1) {
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	*exist = 1;

	row = mysql_fetch_row(res);
	free(*host_partition);
	*host_partition = (char *) strndup(row[0], strlen(row[0]));
	*fs_type = (int) strtol(row[1], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_check_number_of_host_dbms(ophidiadb * oDB, int fs_type, char *ioserver_type, char *host_partition, int host_number, int dbmsxhost_number, int *exist)
{
	if (!oDB || !host_number || !dbmsxhost_number || !host_partition || !exist || fs_type < 0 || fs_type > 2 || !ioserver_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*exist = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_HOST_DBMS_NUMBER, host_partition, fs_type, ioserver_type);
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
	int num = 0;
	if ((num = mysql_num_rows(res)) < 1) {
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (num < host_number)
		return OPH_ODB_SUCCESS;

	int i = 0;
	while ((row = mysql_fetch_row(res))) {
		if ((int) strtol(row[0], NULL, 10) >= dbmsxhost_number)
			i++;
	}
	if (i >= host_number)
		*exist = 1;

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_fragment_list(ophidiadb * oDB, int id_datacube, oph_odb_fragment_list * frags)
{
	if (!oDB || !frags || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_fragment_list(frags);

	//Execute query
	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 4) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (!((frags->value) = (oph_odb_fragment *) calloc(num_rows, sizeof(oph_odb_fragment)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	frags->size = num_rows;

	//Fill connection strings structures
	int i_frag = 0;
	while ((row = mysql_fetch_row(res))) {
		// Fragment
		frags->value[i_frag].id_fragment = (int) strtol(row[0], NULL, 10);
		frags->value[i_frag].frag_relative_index = (int) strtol(row[1], NULL, 10);
		frags->value[i_frag].key_start = (int) strtol(row[2], NULL, 10);
		frags->value[i_frag].key_end = (int) strtol(row[3], NULL, 10);
		i_frag++;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_fragment_list2(ophidiadb * oDB, int id_datacube, oph_odb_fragment_list2 * frags)
{
	if (!oDB || !frags || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Initialize structures
	oph_odb_stge_init_fragment_list2(frags);

	//Execute query
	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS2, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if (!(num_rows = mysql_num_rows(res))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 7) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (!((frags->value) = (oph_odb_fragment2 *) calloc(num_rows, sizeof(oph_odb_fragment2)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	frags->size = num_rows;

	//Fill connection strings structures
	int i_frag = 0;
	while ((row = mysql_fetch_row(res))) {
		// Fragment
		frags->value[i_frag].id_fragment = (int) strtol(row[0], NULL, 10);
		frags->value[i_frag].frag_relative_index = (int) strtol(row[1], NULL, 10);
		frags->value[i_frag].key_start = (int) strtol(row[2], NULL, 10);
		frags->value[i_frag].key_end = (int) strtol(row[3], NULL, 10);
		frags->value[i_frag].id_host = (int) strtol(row[4], NULL, 10);
		frags->value[i_frag].id_dbms = (int) strtol(row[5], NULL, 10);
		frags->value[i_frag].id_db = (int) strtol(row[6], NULL, 10);
		i_frag++;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_find_datacube_fragmentation_list(ophidiadb * oDB, int level, int id_datacube, char *hostname, char *db_name, int id_dbms, MYSQL_RES ** information_list)
{
	(*information_list) = NULL;

	if (!oDB || !information_list || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (level > OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB_FRAG) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Level not allowed\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];
	int n;
	char where_clause[MYSQL_BUFLEN];

	//Set values to correct number
	char *p3 = hostname;
	int p4 = id_dbms;
	char *p5 = db_name;

	switch (level) {
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB_FRAG:
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB:
			p5 = NULL;
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS:
			p4 = 0;
			p5 = NULL;
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST:
			p3 = NULL;
			p4 = 0;
			p5 = NULL;
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE:
			p3 = NULL;
			p4 = 0;
			p5 = NULL;
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad filter parameters\n");
			return OPH_ODB_NULL_PARAM;
	}

	n = 0;
	where_clause[0] = 0;

	if (p3 || p4 || p5)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (p3)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "host.hostname='%s' ", p3);
	if (p4 && (p3))
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (p4)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "dbmsinstance.iddbmsinstance=%d ", p4);
	if (p5 && (p3 || p4))
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (p5)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "dbinstance.dbname='%s' ", p5);

	switch (level) {
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_2, id_datacube);
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_3, id_datacube, where_clause);
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_4, id_datacube, where_clause);
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_5, id_datacube, where_clause);
			break;
		case OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB_FRAG:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_6, id_datacube, where_clause);
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad filter parameters\n");
			return OPH_ODB_NULL_PARAM;
	}
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

int oph_odb_stge_find_instances_information(ophidiadb * oDB, int level, char *hostname, char *partition_name, int fs_type, char *ioserver_type, char *host_status, char *dbms_status,
					    MYSQL_RES ** information_list, int id_user)
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
	char where_clause[MYSQL_BUFLEN];

	n = 0;
	where_clause[0] = 0;
	char *p1 = hostname;
	int p2 = fs_type;
	char *p3 = partition_name;
	char *p4 = host_status;
	char *p5 = dbms_status;
	char *p6 = ioserver_type;
	if (p2 > 2 || p2 < 0)
		p2 = 2;
	//If level is one than only the host status filter can be used
	if (level == 1) {
		p1 = p3 = p5 = p6 = NULL;
		p2 = 2;
	}
	//If level is two than host status, dbms status, hostname and ioservertype filters can be used
	else if (level == 2) {
		p3 = NULL;
	}
	//If level is three than only partition_name and host_status filters can be used
	else {
		p1 = p5 = p6 = NULL;
		p2 = 2;
	}

	if (p1 || p2 != 2 || p3 || p4 || p5 || p6)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (level == 1 && p4)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "host.status ='%s' ", p4);
	if (level == 2) {
		if (p1)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "hostname='%s' ", p1);
		if (p2 != 2 && p1)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
		if (p2 != 2)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "fstype=%d ", p2);
		if (p4 && (p1 || p2 != 2))
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
		if (p4)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "host.status ='%s' ", p4);
		if (p5 && (p1 || p2 != 2 || p4))
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
		if (p5)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "dbmsinstance.status ='%s' ", p5);
		if (p6 && (p1 || p2 != 2 || p4 || p5))
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
		if (p6)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "ioservertype ='%s' ", p6);
	}
	if (level == 3 && p3) {
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "partitionname = '%s' ", p3);
		if (p4)
			n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND host.status ='%s' ", p4);
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND hidden = 0 ");
	}

	switch (level) {
		case 1:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_1, where_clause);
			break;
		case 2:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_2, where_clause);
			break;
		case 3:
			if (p3)
				n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3, id_user, where_clause);
			else
				n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3_PART, id_user);
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad filter parameters\n");
			return OPH_ODB_NULL_PARAM;
	}
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

int oph_odb_stge_find_fragment_name_list(ophidiadb * oDB, int id_datacube, int id_dbms, int start_position, int row_number, MYSQL_RES ** fragment_list)
{
	(*fragment_list) = NULL;

	if (!oDB || !fragment_list || !id_datacube || !id_dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	//Execute query
	int n;
	if (start_position == 0 && row_number == 0)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST, id_dbms, id_datacube);
	else
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST_LIMIT, id_dbms, id_datacube, start_position, row_number);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	*fragment_list = mysql_store_result(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_fragment(ophidiadb * oDB, char *frag_name, oph_odb_fragment * frag)
{
	if (!oDB || !frag || !frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG, frag_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if ((num_rows = mysql_num_rows(res)) != 1) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 7) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	//Fill connection strings structures
	if ((row = mysql_fetch_row(res))) {
		frag->id_fragment = (int) strtol(row[0], NULL, 10);
		frag->id_datacube = (int) strtol(row[1], NULL, 10);
		frag->id_db = (int) strtol(row[2], NULL, 10);
		frag->frag_relative_index = (int) strtol(row[3], NULL, 10);
		memset(&(frag->fragment_name), 0, OPH_ODB_STGE_FRAG_NAME_SIZE + 1);
		strncpy(frag->fragment_name, row[4], OPH_ODB_STGE_FRAG_NAME_SIZE);
		frag->key_start = (int) strtol(row[5], NULL, 10);
		frag->key_end = (int) strtol(row[6], NULL, 10);
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_dbinstance(ophidiadb * oDB, int id_dbinstance, oph_odb_db_instance * db)
{
	if (!oDB || !db || !id_dbinstance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DB, id_dbinstance);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if ((num_rows = mysql_num_rows(res)) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 8) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	//Fill connection strings structures
	if ((row = mysql_fetch_row(res))) {
		db->id_db = (int) strtol(row[6], NULL, 10);
		db->id_dbms = (int) strtol(row[0], NULL, 10);
		memset(&(db->db_name), 0, OPH_ODB_STGE_DB_NAME_SIZE + 1);
		strncpy(db->db_name, row[7], OPH_ODB_STGE_DB_NAME_SIZE);
		if (!(db->dbms_instance = (oph_odb_dbms_instance *) malloc(1 * sizeof(oph_odb_dbms_instance)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			mysql_free_result(res);
			return OPH_ODB_MEMORY_ERROR;
		}
		db->dbms_instance->id_dbms = (int) strtol(row[0], NULL, 10);
		memset(&(db->dbms_instance->login), 0, OPH_ODB_STGE_LOGIN_SIZE + 1);
		strncpy(db->dbms_instance->login, row[1], OPH_ODB_STGE_LOGIN_SIZE);
		memset(&(db->dbms_instance->pwd), 0, OPH_ODB_STGE_PWD_SIZE + 1);
		strncpy(db->dbms_instance->pwd, row[2], OPH_ODB_STGE_PWD_SIZE);
		db->dbms_instance->port = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		memset(&(db->dbms_instance->hostname), 0, OPH_ODB_STGE_HOST_NAME_SIZE + 1);
		strncpy(db->dbms_instance->hostname, row[4], OPH_ODB_STGE_HOST_NAME_SIZE);
		memset(&(db->dbms_instance->io_server_type), 0, OPH_ODB_STGE_SERVER_NAME_SIZE + 1);
		strncpy(db->dbms_instance->io_server_type, row[5], OPH_ODB_STGE_SERVER_NAME_SIZE);
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_delete_from_dbinstance_table(ophidiadb * oDB, int id_db)
{
	if (!oDB || !id_db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];

	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_DELETE_OPHIDIADB_DB, id_db);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, deleteQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_insert_into_fragment_table(ophidiadb * oDB, oph_odb_fragment * fragment)
{
	if (!oDB || !fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_FRAG, fragment->id_db, fragment->id_datacube, fragment->frag_relative_index, fragment->fragment_name,
			 fragment->key_start, fragment->key_end);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_container_id_from_fragment_name(ophidiadb * oDB, char *frag_name, int *id_container)
{
	if (!oDB || !frag_name || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_CONTAINER_FROM_FRAGMENT, frag_name);
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

	row = mysql_fetch_row(res);
	*id_container = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_fragment_id(ophidiadb * oDB, char *frag_name, int *id_fragment)
{
	if (!oDB || !frag_name || !id_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_FRAG_ID, frag_name);
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

	row = mysql_fetch_row(res);
	*id_fragment = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_dbinstance_id(ophidiadb * oDB, char *db_name, int *id_db)
{
	if (!oDB || !db_name || !id_db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DB_ID, db_name);
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

	row = mysql_fetch_row(res);
	*id_db = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_dbinstance_id_list_from_datacube(ophidiadb * oDB, int id_datacube, int **id_dbs, int *size)
{
	if (!oDB || !id_datacube || !id_dbs || !size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_PARTITIONED_DB, id_datacube);
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
	int rows;
	rows = mysql_num_rows(res);

	if (!(*id_dbs = (int *) malloc(rows * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i = 0;
	*size = rows;
	while ((row = mysql_fetch_row(res))) {
		(*id_dbs)[i] = (int) strtol(row[0], NULL, 10);
		i++;
	}

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_retrieve_dbmsinstance_id_list(ophidiadb * oDB, int fs_type, char *ioserver_type, char *host_partition, int host_number, int dbmsxhost_number, int **id_dbmss, int *size)
{
	if (!oDB || !host_number || !dbmsxhost_number || !id_dbmss || !size || fs_type < 0 || fs_type > 2 || !ioserver_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	//Select all up host that have at least dbmsxhost_number
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DBMS_LIST, host_partition, fs_type, ioserver_type, host_partition, fs_type, ioserver_type, dbmsxhost_number);
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
	if (mysql_num_rows(res) < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_MYSQL_ERROR;
	}
	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Too many fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	*size = host_number * dbmsxhost_number;
	if (!(*id_dbmss = (int *) malloc((*size) * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	memset(*id_dbmss, 0, (*size) * sizeof(int));

	int counter = 0;
	int old_id = 0;
	int i = 0;

	//Get dbmsxhost_number of dbms for host_number times
	while ((row = mysql_fetch_row(res))) {
		if ((int) strtol(row[0], NULL, 10) != old_id)
			counter = 0;
		old_id = (int) strtol(row[0], NULL, 10);
		if (counter < dbmsxhost_number) {
			(*id_dbmss)[i] = (int) strtol(row[1], NULL, 10);
			i++;
			counter++;
		}
		if (i == *size)
			break;
	}
	mysql_free_result(res);

	if (i < *size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough hosts for the operation\n");
		free(*id_dbmss);
		*id_dbmss = NULL;
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_get_number_of_datacube_for_db(ophidiadb * oDB, int id_db, int *datacubexdb_number)
{
	if (!oDB || !id_db || !datacubexdb_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DATACUBEXDB_NUMBER, id_db);
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

	if (mysql_num_rows(res) == 1) {
		if (mysql_field_count(oDB->conn) == 1) {
			row = mysql_fetch_row(res);
			*datacubexdb_number = (int) strtol(row[0], NULL, 10);
			mysql_free_result(res);
		} else
			*datacubexdb_number = 0;
	} else
		*datacubexdb_number = 0;

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_insert_into_dbinstance_partitioned_tables(ophidiadb * oDB, oph_odb_db_instance * db, int id_datacube)
{
	if (!oDB || !db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_DB, db->id_dbms, db->db_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_RETRIEVE_DB_ID, db->db_name);
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

	row = mysql_fetch_row(res);
	db->id_db = (int) strtol(row[0], NULL, 10);
	mysql_free_result(res);

	//If database_instance update partition table
	if (id_datacube) {
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_PART, db->id_db, id_datacube);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}

		if (mysql_query(oDB->conn, insertQuery)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_add_hostpartition(ophidiadb * oDB, const char *name, int id_user, char reserved, int *id_hostpartition)
{
	if (!oDB || !name || !id_user || !id_hostpartition) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_hostpartition = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_CREATE_PARTITION, name, id_user, reserved);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	*id_hostpartition = (int) mysql_insert_id(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_add_all_hosts_to_partition(ophidiadb * oDB, int id_hostpartition, char reserved)
{
	if (!oDB || !id_hostpartition) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_ADD_ALL_HOSTS_TO_PARTITION, id_hostpartition);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (reserved) {

		if (mysql_query(oDB->conn, MYSQL_QUERY_STGE_CHECK_ALL_HOSTS)) {
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

		row = mysql_fetch_row(res);
		if (strtol(row[0], NULL, 10) > 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Host is already reserved\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}

		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_add_host_to_partition(ophidiadb * oDB, int id_hostpartition, int id_host, char reserved)
{
	if (!oDB || !id_hostpartition || !id_host) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_ADD_HOST_TO_PARTITION, id_hostpartition, id_host);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (reserved) {

		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_CHECK_HOST, id_host);
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

		row = mysql_fetch_row(res);
		if (strtol(row[0], NULL, 10) > 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Host is already reserved\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}

		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_delete_hostpartition(ophidiadb * oDB, const char *name, int id_user, int *num_rows)
{
	if (!oDB || !name || !id_user) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (num_rows)
		*num_rows = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_DELETE_PARTITION, name, id_user);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (num_rows)
		*num_rows = (int) mysql_affected_rows(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_stge_delete_hostpartition_by_id(ophidiadb * oDB, int id_hostpartition)
{
	if (!oDB || !id_hostpartition) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_STGE_DELETE_PARTITION2, id_hostpartition);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}
