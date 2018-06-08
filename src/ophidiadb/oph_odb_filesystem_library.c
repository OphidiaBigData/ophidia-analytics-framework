/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

#include "oph_odb_filesystem_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <mysql.h>
#include "debug.h"

extern int msglevel;

static char oph_odb_fs_chars[OPH_ODB_FS_CHARS_NUM] = { '\'', '\"', '?', '|', '\\', '/', ':', ';', ',', '<', '>', '*', '=', '!' };

int oph_odb_fs_path_parsing(char *inpath, char *cwd, int *folder_id, char **output_path, ophidiadb * oDB)
{

	if (!inpath || !cwd) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oDB && oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (cwd[0] != OPH_ODB_FS_FOLDER_SLASH_CHAR) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "CWD must start with / (root)\n");
		return OPH_ODB_ERROR;
	}

	char buffer[MYSQL_BUFLEN];
	char buffer2[MYSQL_BUFLEN];
	int list_size = 0;
	int i, j;

	if (inpath[0] == OPH_ODB_FS_FOLDER_SLASH_CHAR)
		snprintf(buffer, MYSQL_BUFLEN, "%s", inpath);
	else if (!cwd || !cwd[0] || !strcmp(cwd, OPH_ODB_FS_FOLDER_SLASH))
		snprintf(buffer, MYSQL_BUFLEN, "/%s", inpath);	// Root folder is an exception
	else
		snprintf(buffer, MYSQL_BUFLEN, "%s/%s", cwd, inpath);

	strcpy(buffer2, buffer);
	char *savepointer = NULL, *ptr2 = strtok_r(buffer2, OPH_ODB_FS_FOLDER_SLASH, &savepointer);
	if (ptr2) {
		list_size++;
		while ((ptr2 = strtok_r(NULL, OPH_ODB_FS_FOLDER_SLASH, &savepointer)) != NULL)
			list_size++;
	}

	char **list = list_size ? (char **) malloc(sizeof(char *) * list_size) : NULL;
	if (!list && list_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for path\n");
		return OPH_ODB_MEMORY_ERROR;
	}
	for (i = 0; i < list_size; i++) {
		list[i] = (char *) malloc(MYSQL_BUFLEN);
		if (!list[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for path\n");
			for (j = 0; j < i; j++) {
				free(list[j]);
			}
			free(list);
			return OPH_ODB_MEMORY_ERROR;
		}
	}

	i = 0;
	char *ptr = strtok_r(buffer, OPH_ODB_FS_FOLDER_SLASH, &savepointer);
	if (ptr) {
		if (!strcmp(ptr, OPH_ODB_FS_CURRENT_DIR)) {
			if (!output_path) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
				free(list);
				return OPH_ODB_ERROR;
			}
		} else if (!strcmp(ptr, OPH_ODB_FS_PARENT_DIR)) {
			if (!output_path) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
				free(list);
				return OPH_ODB_ERROR;
			}
			i--;
			if (i < 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
				for (j = 0; j < list_size; j++) {
					free(list[j]);
				}
				free(list);
				return OPH_ODB_ERROR;
			}
		} else {
			snprintf(list[i], MYSQL_BUFLEN, "%s", ptr);
			i++;
		}

		while ((ptr = strtok_r(NULL, OPH_ODB_FS_FOLDER_SLASH, &savepointer)) != NULL) {
			if (!strcmp(ptr, OPH_ODB_FS_CURRENT_DIR)) {
				if (!output_path) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
					if (list)
						free(list);
					return OPH_ODB_ERROR;
				}
			} else if (!strcmp(ptr, OPH_ODB_FS_PARENT_DIR)) {
				if (!output_path) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
					if (list)
						free(list);
					return OPH_ODB_ERROR;
				}
				i--;
				if (i < 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid path\n");
					for (j = 0; j < list_size; j++) {
						free(list[j]);
					}
					if (list)
						free(list);
					return OPH_ODB_ERROR;
				}
			} else {
				snprintf(list[i], MYSQL_BUFLEN, "%s", ptr);
				i++;
			}
		}
	}

	int n = 0;
	if (output_path) {
		*output_path = (char *) malloc(MYSQL_BUFLEN);
		if (!*output_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for path\n");
			for (j = 0; j < list_size; j++) {
				free(list[j]);
			}
			if (list)
				free(list);
			return OPH_ODB_MEMORY_ERROR;
		}

		n += snprintf((*output_path) + n, MYSQL_BUFLEN, OPH_ODB_FS_FOLDER_SLASH);
		for (j = 0; j < i; j++) {
			n += snprintf((*output_path) + n, MYSQL_BUFLEN, "%s/", list[j]);
		}
	}

	if (oDB && folder_id) {

		char query[MYSQL_BUFLEN];
		MYSQL_RES *res;
		MYSQL_ROW row;
		int num_rows;

		// retrieve root id
		snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_ROOT_ID);
		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			for (j = 0; j < list_size; j++) {
				free(list[j]);
			}
			if (list)
				free(list);
			if (output_path) {
				free(*output_path);
				*output_path = NULL;
			}
			return OPH_ODB_MYSQL_ERROR;
		}
		res = mysql_store_result(oDB->conn);
		num_rows = mysql_num_rows(res);
		if (num_rows != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find id of root folder\n");
			mysql_free_result(res);
			for (j = 0; j < list_size; j++) {
				free(list[j]);
			}
			if (list)
				free(list);
			if (output_path) {
				free(*output_path);
				*output_path = NULL;
			}
			return OPH_ODB_ERROR;
		}
		row = mysql_fetch_row(res);
		*folder_id = (int) strtol(row[0], NULL, 10);
		mysql_free_result(res);

		// retrieve folder id
		int k;
		for (k = 0; k < i; k++) {
			snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_PATH_PARSING_ID, *folder_id, list[k]);
			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				for (j = 0; j < list_size; j++) {
					free(list[j]);
				}
				if (list)
					free(list);
				if (output_path) {
					free(*output_path);
					*output_path = NULL;
				}
				return OPH_ODB_MYSQL_ERROR;
			}
			res = mysql_store_result(oDB->conn);
			num_rows = mysql_num_rows(res);
			if (num_rows != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find id of folder %s\n", list[k]);
				mysql_free_result(res);
				for (j = 0; j < list_size; j++) {
					free(list[j]);
				}
				if (list)
					free(list);
				if (output_path) {
					free(*output_path);
					*output_path = NULL;
				}
				return OPH_ODB_ERROR;
			}
			row = mysql_fetch_row(res);
			*folder_id = (int) strtol(row[0], NULL, 10);
			mysql_free_result(res);
		}
	}
	// cleanup
	for (j = 0; j < list_size; j++) {
		free(list[j]);
	}
	if (list)
		free(list);

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_check_folder_session(int folder_id, char *sessionid, ophidiadb * oDB, int *status)
{
	if (!oDB || !folder_id || !sessionid || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*status = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	int session_folder_id;

	//Retrive session home folder id
	if (oph_odb_fs_get_session_home_id(sessionid, oDB, &session_folder_id)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get session home id.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//If session home is the folder specified than end
	if (session_folder_id == folder_id) {
		*status = 1;
		return OPH_ODB_SUCCESS;
	}
	//Retrive all input parent of this folder
	char query[MYSQL_BUFLEN];
	int n;
	MYSQL_RES *res;
	MYSQL_ROW row;

	int root_flag = 0;
	int internal_folder_id = folder_id;
	int tmp_folder_id = 0;
	while (!root_flag) {
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_PARENT_FOLDER_ID, internal_folder_id);
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

		if ((row = mysql_fetch_row(res)) != NULL) {
			if (row[0]) {
				tmp_folder_id = (int) strtol(row[0], NULL, 10);
				if (tmp_folder_id == session_folder_id) {
					*status = 1;
					mysql_free_result(res);
					break;
				}
				internal_folder_id = tmp_folder_id;
			} else {
				root_flag = 1;
			}
		}
		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}


int oph_odb_fs_get_session_home_id(char *sessionid, ophidiadb * oDB, int *folder_id)
{
	if (!oDB || !folder_id || !sessionid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}
	//Retrive session home folder id
	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_SESSION_FOLDER_ID, sessionid);
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

	if ((row = mysql_fetch_row(res)) != NULL)
		*folder_id = (int) strtol(row[0], NULL, 10);
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_build_path(int folder_id, ophidiadb * oDB, char (*out_path)[MYSQL_BUFLEN])
{
	if (!oDB || !folder_id || !out_path) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	int n;
	char query[MYSQL_BUFLEN];

	MYSQL_RES *res;
	MYSQL_ROW row;

	//Retrive all input parent of this folder
	int root_flag = 0;
	int internal_folder_id = folder_id;
	int tmp_folder_id = 0;
	(*out_path)[0] = 0;
	char tmp_out_path[MYSQL_BUFLEN];
	tmp_out_path[0] = 0;
	while (!root_flag) {
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_PARENT_FOLDER, internal_folder_id);
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

		if (mysql_field_count(oDB->conn) != 2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}

		if ((row = mysql_fetch_row(res)) != NULL) {
			if (row[0]) {
				snprintf(*out_path, MYSQL_BUFLEN, "%s/%s", row[1], tmp_out_path);
				snprintf(tmp_out_path, MYSQL_BUFLEN, "%s", *out_path);
				tmp_folder_id = (int) strtol(row[0], NULL, 10);
				internal_folder_id = tmp_folder_id;
			} else {
				root_flag = 1;
			}
		}
		mysql_free_result(res);
	}
	snprintf(*out_path, MYSQL_BUFLEN, OPH_ODB_FS_ROOT "%s", tmp_out_path);

	return OPH_ODB_SUCCESS;
}

//It also checks if the container is not hidden
int oph_odb_fs_retrive_container_folder_id(ophidiadb * oDB, int container_id, int non_hidden, int *folder_id)
{
	if (!oDB || !folder_id || !container_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;
	if (non_hidden)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_CONTAINER_FOLDER_ID, container_id);
	else
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_CONTAINER_FOLDER_ID2, container_id);
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
		if (!non_hidden)
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Container doesn't exists\n");
		else
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Container is hidden or it doesn't exists\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	*folder_id = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_str_last_token(const char *input, char **first_part, char **last_token)
{
	if (!first_part || !input || !last_token)
		return OPH_ODB_NULL_PARAM;

	*first_part = (char *) malloc(MYSQL_BUFLEN);
	if (!*first_part) {
		return OPH_ODB_MEMORY_ERROR;
	}
	*last_token = (char *) malloc(MYSQL_BUFLEN);
	if (!*last_token) {
		free(*first_part);
		*first_part = NULL;
		return OPH_ODB_MEMORY_ERROR;
	}

	char buffer[MYSQL_BUFLEN];
	char buffer2[MYSQL_BUFLEN];
	snprintf(buffer, MYSQL_BUFLEN, "%s", input);
	snprintf(buffer2, MYSQL_BUFLEN, "%s", input);

	int token_num = 0;
	char *savepointer = NULL, *ptr2 = strtok_r(buffer2, OPH_ODB_FS_FOLDER_SLASH, &savepointer);
	if (ptr2) {
		token_num++;
	} else {
		free(*first_part);
		free(*last_token);
		*first_part = NULL;
		*last_token = NULL;
		return OPH_ODB_ERROR;
	}

	while ((ptr2 = strtok_r(NULL, OPH_ODB_FS_FOLDER_SLASH, &savepointer)) != NULL)
		token_num++;

	char *ptr = strtok_r(buffer, OPH_ODB_FS_FOLDER_SLASH, &savepointer);

	if (token_num == 1) {
		if (input[0] == OPH_ODB_FS_FOLDER_SLASH_CHAR) {
			snprintf(*first_part, MYSQL_BUFLEN, "%s", OPH_ODB_FS_FOLDER_SLASH);
		} else {
			snprintf(*first_part, MYSQL_BUFLEN, "%s", "");
		}
		snprintf(*last_token, MYSQL_BUFLEN, "%s", ptr);
		return OPH_ODB_SUCCESS;
	}
	int i;
	int n = 0;
	if (input[0] == OPH_ODB_FS_FOLDER_SLASH_CHAR) {
		n += snprintf((*first_part) + n, MYSQL_BUFLEN, "%s", OPH_ODB_FS_FOLDER_SLASH);
	}
	for (i = 0; i < token_num; i++) {
		if (i != (token_num - 1)) {
			n += snprintf((*first_part) + n, MYSQL_BUFLEN, "%s/", ptr);
			ptr = strtok_r(NULL, OPH_ODB_FS_FOLDER_SLASH, &savepointer);
		} else {
			snprintf(*last_token, MYSQL_BUFLEN, "%s", ptr);
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_is_visible_container(int folder_id, char *name, ophidiadb * oDB, int *answer)
{
	if (!oDB || !name || !folder_id || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	MYSQL_RES *res;
	int num_rows;

	snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_IS_VISIBLE_CONTAINER, folder_id, name);
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	num_rows = mysql_num_rows(res);
	if (num_rows == 0) {
		*answer = 0;
		mysql_free_result(res);
	} else if (num_rows == 1) {
		*answer = 1;
		mysql_free_result(res);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Too much rows found\n");
		mysql_free_result(res);
		return OPH_ODB_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_is_hidden_container(int folder_id, char *name, ophidiadb * oDB, int *answer)
{
	if (!oDB || !name || !folder_id || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	MYSQL_RES *res;
	int num_rows;

	snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_IS_HIDDEN_CONTAINER, folder_id, name);
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	num_rows = mysql_num_rows(res);
	if (num_rows == 0) {
		*answer = 0;
		mysql_free_result(res);
	} else if (num_rows == 1) {
		*answer = 1;
		mysql_free_result(res);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Too much rows found\n");
		mysql_free_result(res);
		return OPH_ODB_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_is_unique(int folder_id, char *name, ophidiadb * oDB, int *answer)
{
	if (!oDB || !name || !folder_id || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	MYSQL_RES *res;
	int num_rows;

	snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_UNIQUENESS, folder_id, name, folder_id, name);
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	num_rows = mysql_num_rows(res);
	if (num_rows == 0) {
		*answer = 1;
		mysql_free_result(res);
	} else {
		*answer = 0;
		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_is_unique_hidden(int folder_id, char *name, ophidiadb * oDB, int *answer)
{
	if (!oDB || !name || !folder_id || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	MYSQL_RES *res;
	int num_rows;

	snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_UNIQUENESS_HIDDEN, folder_id, name);
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	num_rows = mysql_num_rows(res);
	if (num_rows == 0) {
		*answer = 1;
		mysql_free_result(res);
	} else {
		*answer = 0;
		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_is_empty_folder(int folder_id, ophidiadb * oDB, int *answer)
{
	if (!oDB || !folder_id || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	MYSQL_RES *res;
	int num_rows;

	snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_EMPTINESS, folder_id, folder_id);
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	num_rows = mysql_num_rows(res);
	if (num_rows == 0) {
		*answer = 1;
		mysql_free_result(res);
	} else {
		*answer = 0;
		mysql_free_result(res);
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_set_container_hidden_status(int container_id, int hidden, ophidiadb * oDB)
{
	if (!oDB || !container_id || hidden < 0 || hidden > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_CONTAINER_STATUS, hidden, container_id);
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


int oph_odb_fs_update_container_path_name(ophidiadb * oDB, int in_container_id, int out_folder_id, char *out_container_name)
{
	if (!oDB || !in_container_id || !out_folder_id || !out_container_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_MV, out_folder_id, out_container_name, in_container_id);
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

int oph_odb_fs_find_fs_objects(ophidiadb * oDB, int level, int id_folder, int hidden, char *container_name, MYSQL_RES ** information_list)
{
	if (!oDB || !id_folder || !information_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	(*information_list) = NULL;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (level > 3) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Level not allowed\n");
		return OPH_ODB_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];
	int n;
	if (level < 1)
		container_name = NULL;

	switch (level) {
		case 0:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_0, id_folder);
			break;
		case 1:
			if (!hidden) {
				if (container_name)
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_1_WC, id_folder, container_name);
				else
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_1, id_folder, id_folder);
			} else {
				if (container_name)
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_1_H_WC, id_folder, container_name, id_folder, container_name);
				else
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_1_H, id_folder, id_folder, id_folder);
			}
			break;
		case 2:
			if (!hidden) {
				if (container_name)
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_WC, id_folder, container_name);
				else
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2, id_folder, id_folder);
			} else {
				if (container_name)
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_H_WC, id_folder, container_name, id_folder, container_name);
				else
					n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_H, id_folder, id_folder, id_folder);
			}
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

int oph_odb_fs_find_fs_filtered_objects(ophidiadb * oDB, int id_folder, int hidden, char *container_name, char *measure, int oper_level, char *src, MYSQL_RES ** information_list)
{
	if (!oDB || !id_folder || !information_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	(*information_list) = NULL;


	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = 0;
	char where_clause[MYSQL_BUFLEN] = { '\0' };

	if (measure)
		n = snprintf(where_clause + n, MYSQL_BUFLEN, "AND measure LIKE '%s'", measure);
	if (oper_level >= 0)
		n = snprintf(where_clause + n, MYSQL_BUFLEN, "AND level = %d", oper_level);
	if (src)
		n = snprintf(where_clause + n, MYSQL_BUFLEN, "AND uri LIKE '%s'", src);


	if (!hidden) {
		if (container_name)
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_WC_FILTER, OPH_ODB_FS_TASK_MULTIPLE_INPUT, id_folder, container_name, where_clause);
		else
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_FILTER, id_folder, OPH_ODB_FS_TASK_MULTIPLE_INPUT, id_folder, where_clause);
	} else {
		if (container_name)
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_H_WC_FILTER, OPH_ODB_FS_TASK_MULTIPLE_INPUT, id_folder, container_name, where_clause, id_folder, container_name,
				     where_clause);
		else
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_LIST_2_H_FILTER, id_folder, OPH_ODB_FS_TASK_MULTIPLE_INPUT, id_folder, where_clause, OPH_ODB_FS_TASK_MULTIPLE_INPUT, id_folder,
				     where_clause);
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

int oph_odb_fs_insert_into_folder_table(ophidiadb * oDB, int parent_folder_id, char *child_folder, int *last_insertd_id)
{
	if (!oDB || !child_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_MKDIR, parent_folder_id, child_folder);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_delete_from_folder_table(ophidiadb * oDB, int folderid)
{
	if (!oDB || !folderid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];
	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_RMDIR, folderid);
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

int oph_odb_fs_update_folder_table(ophidiadb * oDB, int old_folder_id, int new_parent_folder_id, char *new_child_folder)
{
	if (!oDB || !old_folder_id || !new_parent_folder_id || !new_child_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_MVDIR, new_parent_folder_id, new_child_folder, old_folder_id);

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

int oph_odb_fs_delete_from_container_table(ophidiadb * oDB, int id_container)
{
	if (!oDB || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];

	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_DELETE_OPHIDIADB_CONTAINER, id_container);
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

int oph_odb_fs_check_if_container_empty(ophidiadb * oDB, int id_container)
{
	if (!oDB || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_EMPTY_CONTAINER, id_container);
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
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Found too many rows in query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	if (strtol(row[0], NULL, 10) > 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Found too many rows in query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_insert_into_container_table(ophidiadb * oDB, oph_odb_container * cont, int *last_insertd_id)
{
	if (!oDB || !cont || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;
	if (!strlen(cont->description)) {
		if (cont->id_vocabulary) {
			if (cont->id_parent)
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_3, cont->id_folder, cont->id_parent, cont->container_name, cont->operation,
					     cont->id_vocabulary);
			else
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_4, cont->id_folder, cont->container_name, cont->operation, cont->id_vocabulary);
		} else {
			if (cont->id_parent)
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER, cont->id_folder, cont->id_parent, cont->container_name, cont->operation);
			else
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_2, cont->id_folder, cont->container_name, cont->operation);
		}
	} else {
		if (cont->id_vocabulary) {
			if (cont->id_parent)
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_3, cont->id_folder, cont->id_parent, cont->container_name, cont->operation,
					     cont->id_vocabulary, cont->description);
			else
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_4, cont->id_folder, cont->container_name, cont->operation, cont->id_vocabulary,
					     cont->description);
		} else {
			if (cont->id_parent)
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D, cont->id_folder, cont->id_parent, cont->container_name, cont->operation,
					     cont->description);
			else
				n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_2, cont->id_folder, cont->container_name, cont->operation, cont->description);
		}
	}
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted container id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_retrieve_container_id_from_container_name(ophidiadb * oDB, int folder_id, char *container_name, int hidden, int *id_container)
{
	if (!oDB || !container_name || !id_container || !folder_id || hidden > 1 || hidden < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;
	n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_CONTAINER_ID, container_name, folder_id, hidden);
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
	*id_container = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_retrieve_container_from_container_name(ophidiadb * oDB, int folder_id, char *container_name, int *id_container, char **description, char **vocabulary)
{
	if (!oDB || !container_name || !folder_id || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_container = 0;
	if (description)
		*description = 0;
	if (vocabulary)
		*vocabulary = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;
	n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_CONTAINER, container_name, folder_id);
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

	if (mysql_field_count(oDB->conn) != 3) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	*id_container = (int) strtol(row[0], NULL, 10);
	if (description && row[1])
		*description = strdup(row[1]);
	if (vocabulary && row[2])
		*vocabulary = strdup(row[2]);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_check_if_container_not_present(ophidiadb * oDB, char *container_name, int folder_id, int *result)
{
	*result = 0;
	if (!oDB || !container_name || !folder_id || !result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_CONTAINER_ID2, container_name, folder_id);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	res = mysql_store_result(oDB->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 0)
		*result = 1;
	else
		*result = 0;
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}



int oph_odb_fs_is_allowed_name(char *name)
{
	if (!name)
		return 0;
	if (strlen(name) == 0)
		return 0;
	if (!isalnum((int) name[0]) && name[0] != '_')
		return 0;
	unsigned int i, j;
	for (i = 1; i < strlen(name); i++) {
		for (j = 0; j < OPH_ODB_FS_CHARS_NUM; j++) {
			if (name[i] == oph_odb_fs_chars[j])
				return 0;
		}
	}
	return 1;
}

int oph_odb_fs_has_ascendant_equal_to_folder(ophidiadb * oDB, int folderid, int new_parent_folderid, int *answer)
{
	if (!oDB || !folderid || !new_parent_folderid || !answer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (folderid == new_parent_folderid) {
		*answer = 1;
		return OPH_ODB_SUCCESS;
	}

	int n;
	char query[MYSQL_BUFLEN];

	MYSQL_RES *res;
	MYSQL_ROW row;

	//Retrive all input parent of this folder
	int root_flag = 0;
	int internal_folder_id = new_parent_folderid;
	while (!root_flag) {
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_FS_RETRIEVE_PARENT_FOLDER, internal_folder_id);
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
		if (mysql_field_count(oDB->conn) != 2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		if ((row = mysql_fetch_row(res)) != NULL) {
			if (row[0]) {
				internal_folder_id = (int) strtol(row[0], NULL, 10);
				if (folderid == internal_folder_id) {
					*answer = 1;
					mysql_free_result(res);
					return OPH_ODB_SUCCESS;
				}
			} else {
				root_flag = 1;
			}
		}
		mysql_free_result(res);
	}
	*answer = 0;
	return OPH_ODB_SUCCESS;
}

int oph_odb_fs_add_suffix_to_container_name(ophidiadb * oDB, int containerid)
{
	if (!oDB || !containerid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char updateQuery[MYSQL_BUFLEN];
	int n = snprintf(updateQuery, MYSQL_BUFLEN, MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_NAME, containerid);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, updateQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}
