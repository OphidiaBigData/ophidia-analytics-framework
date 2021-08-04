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

#include "oph_odb_job_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <mysql.h>
#include "debug.h"

#include "oph_odb_cube_library.h"

extern int msglevel;

const char *oph_odb_job_convert_status_to_str(oph_odb_job_status status)
{
	switch (status) {
		case OPH_ODB_JOB_STATUS_PENDING:
			return OPH_ODB_JOB_STATUS_PENDING_STR;
		case OPH_ODB_JOB_STATUS_WAIT:
			return OPH_ODB_JOB_STATUS_WAIT_STR;
		case OPH_ODB_JOB_STATUS_RUNNING:
			return OPH_ODB_JOB_STATUS_RUNNING_STR;
		case OPH_ODB_JOB_STATUS_START:
			return OPH_ODB_JOB_STATUS_START_STR;
		case OPH_ODB_JOB_STATUS_SET_ENV:
			return OPH_ODB_JOB_STATUS_SET_ENV_STR;
		case OPH_ODB_JOB_STATUS_INIT:
			return OPH_ODB_JOB_STATUS_INIT_STR;
		case OPH_ODB_JOB_STATUS_DISTRIBUTE:
			return OPH_ODB_JOB_STATUS_DISTRIBUTE_STR;
		case OPH_ODB_JOB_STATUS_EXECUTE:
			return OPH_ODB_JOB_STATUS_EXECUTE_STR;
		case OPH_ODB_JOB_STATUS_REDUCE:
			return OPH_ODB_JOB_STATUS_REDUCE_STR;
		case OPH_ODB_JOB_STATUS_DESTROY:
			return OPH_ODB_JOB_STATUS_DESTROY_STR;
		case OPH_ODB_JOB_STATUS_UNSET_ENV:
			return OPH_ODB_JOB_STATUS_UNSET_ENV_STR;
		case OPH_ODB_JOB_STATUS_COMPLETED:
			return OPH_ODB_JOB_STATUS_COMPLETED_STR;
		case OPH_ODB_JOB_STATUS_ERROR:
			return OPH_ODB_JOB_STATUS_ERROR_STR;
		case OPH_ODB_JOB_STATUS_PENDING_ERROR:
			return OPH_ODB_JOB_STATUS_PENDING_ERROR_STR;
		case OPH_ODB_JOB_STATUS_RUNNING_ERROR:
			return OPH_ODB_JOB_STATUS_RUNNING_ERROR_STR;
		case OPH_ODB_JOB_STATUS_START_ERROR:
			return OPH_ODB_JOB_STATUS_START_ERROR_STR;
		case OPH_ODB_JOB_STATUS_SET_ENV_ERROR:
			return OPH_ODB_JOB_STATUS_SET_ENV_ERROR_STR;
		case OPH_ODB_JOB_STATUS_INIT_ERROR:
			return OPH_ODB_JOB_STATUS_INIT_ERROR_STR;
		case OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR:
			return OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR_STR;
		case OPH_ODB_JOB_STATUS_EXECUTE_ERROR:
			return OPH_ODB_JOB_STATUS_EXECUTE_ERROR_STR;
		case OPH_ODB_JOB_STATUS_REDUCE_ERROR:
			return OPH_ODB_JOB_STATUS_REDUCE_ERROR_STR;
		case OPH_ODB_JOB_STATUS_DESTROY_ERROR:
			return OPH_ODB_JOB_STATUS_DESTROY_ERROR_STR;
		case OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR:
			return OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR_STR;
		case OPH_ODB_JOB_STATUS_SKIPPED:
			return OPH_ODB_JOB_STATUS_SKIPPED_STR;
		case OPH_ODB_JOB_STATUS_ABORTED:
			return OPH_ODB_JOB_STATUS_ABORTED_STR;
		case OPH_ODB_JOB_STATUS_UNSELECTED:
			return OPH_ODB_JOB_STATUS_UNSELECTED_STR;
		case OPH_ODB_JOB_STATUS_EXPIRED:
			return OPH_ODB_JOB_STATUS_EXPIRED_STR;
		case OPH_ODB_JOB_STATUS_CLOSED:
			return OPH_ODB_JOB_STATUS_CLOSED_STR;
		default:
			return OPH_ODB_JOB_STATUS_UNKNOWN_STR;
	}
	return OPH_ODB_JOB_STATUS_UNKNOWN_STR;
}

int oph_odb_job_set_job_status(ophidiadb * oDB, int id_job, oph_odb_job_status status)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_DB_SUPPORT

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;

	switch (status) {
		case OPH_ODB_JOB_STATUS_START:
		case OPH_ODB_JOB_STATUS_SET_ENV:
		case OPH_ODB_JOB_STATUS_INIT:
		case OPH_ODB_JOB_STATUS_DISTRIBUTE:
		case OPH_ODB_JOB_STATUS_EXECUTE:
		case OPH_ODB_JOB_STATUS_REDUCE:
		case OPH_ODB_JOB_STATUS_DESTROY:
		case OPH_ODB_JOB_STATUS_UNSET_ENV:
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_1, oph_odb_job_convert_status_to_str(status), id_job);
			break;
		case OPH_ODB_JOB_STATUS_RUNNING:
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_2, oph_odb_job_convert_status_to_str(status), id_job);
			break;
		default:
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_3, oph_odb_job_convert_status_to_str(status), id_job);
	}
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Job status changed into '%s' using: %s\n", oph_odb_job_convert_status_to_str(status), insertQuery);

#else

	UNUSED(id_job);
	UNUSED(status);

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_job_retrieve_session_id(ophidiadb * oDB, char *sessionid, int *id_session)
{
	if (!oDB || !sessionid || !id_session) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_JOB_RETRIEVE_SESSION_ID, sessionid);
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

	if (mysql_num_rows(res) < 1) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_NO_ROW_FOUND;
	}

	if (mysql_num_rows(res) > 1) {
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
		*id_session = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_job_retrieve_job_id(ophidiadb * oDB, char *sessionid, char *markerid, int *id_job)
{
	if (!oDB || !sessionid || !markerid || !id_job) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_DB_SUPPORT

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_JOB_RETRIEVE_JOB_ID, sessionid, markerid);
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

	if (mysql_num_rows(res) < 1) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_NO_ROW_FOUND;
	}

	if (mysql_num_rows(res) > 1) {
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
		*id_job = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_update_folder_table(ophidiadb * oDB, char *folder_name, int *id_folder)
{
	if (!oDB || !folder_name || !id_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_UPDATE_OPHIDIADB_SESSION_FOLDER, folder_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*id_folder = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted folder id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_get_session_code(const char *sessionid, char *session_code)
{

	if (!sessionid || !session_code)
		return OPH_ODB_NULL_PARAM;

	size_t length = strlen(sessionid);
	if (!length)
		return OPH_ODB_STR_BUFF_OVERFLOW;

	char *tmp = strdup(sessionid), *pointer = tmp + length;
	if (!tmp)
		return OPH_ODB_NULL_PARAM;

	char counter = 2;
	while (pointer && (pointer >= tmp)) {
		if (*pointer == '/') {
			*pointer = 0;
			counter--;
			if (!counter)
				break;
		}
		pointer--;
	}

	int result = OPH_ODB_SUCCESS;
	if (pointer && (pointer >= tmp))
		strcpy(session_code, 1 + pointer);
	else
		result = OPH_ODB_STR_BUFF_OVERFLOW;

	free(tmp);

	return result;
}

int oph_odb_job_update_session_table(ophidiadb * oDB, char *sessionid, int id_user, int id_folder, int *id_session)
{
	if (!oDB || !sessionid || !id_user || !id_session) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!id_folder) {

		char session_code[MYSQL_BUFLEN];
		if (oph_get_session_code(sessionid, session_code)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract session code.\n");
			return OPH_ODB_NULL_PARAM;
		}

		if (oph_odb_update_folder_table(oDB, session_code, &id_folder) || !id_folder) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a new folder.\n");
			return OPH_ODB_MYSQL_ERROR;
		}
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_SESSION, id_user, id_folder, sessionid);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*id_session = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_job_update_job_table(ophidiadb * oDB, char *markerid, char *task_string, char *status, int id_user, int id_session, int *id_job, char *parentid)
{
	if (!oDB || !markerid || !task_string || !status || !id_job) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
#ifdef OPH_DB_SUPPORT

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n, i, j;

	//Escape ' char with \'
	char new_query[OPH_ODB_CUBE_OPERATION_QUERY_SIZE];
	j = 0;
	for (i = 0; i < OPH_ODB_CUBE_OPERATION_QUERY_SIZE; i++) {
		if (task_string[i] == '\'')
			new_query[j++] = '\\';
		new_query[j] = task_string[i];
		j++;
		if (j >= OPH_ODB_CUBE_OPERATION_QUERY_SIZE - 1)
			break;
	}
	new_query[OPH_ODB_CUBE_OPERATION_QUERY_SIZE - 1] = 0;

	if (parentid)
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_JOB2, id_session, markerid, status, new_query, parentid, id_user);
	else
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_JOB_UPDATE_JOB, id_session, markerid, status, new_query, id_user);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*id_job = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
#else

	UNUSED(id_user);
	UNUSED(id_session);
	UNUSED(parentid);

#endif

	return OPH_ODB_SUCCESS;
}

int oph_odb_job_retrieve_folder_id(ophidiadb * oDB, char *sessionid, int *id_folder)
{
	if (!oDB || !sessionid || !id_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_JOB_RETRIEVE_FOLDER_ID, sessionid);
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

	if (mysql_num_rows(res) < 1) {

		mysql_free_result(res);

		char session_code[MYSQL_BUFLEN];
		if (oph_get_session_code(sessionid, session_code)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract session code.\n");
			return OPH_ODB_NULL_PARAM;
		}

		if (oph_odb_update_folder_table(oDB, session_code, id_folder) || !*id_folder) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a new folder.\n");
			return OPH_ODB_MYSQL_ERROR;
		}

		return OPH_ODB_SUCCESS;
	}

	if (mysql_num_rows(res) > 1) {
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
		*id_folder = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}
