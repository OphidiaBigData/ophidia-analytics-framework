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

#define _GNU_SOURCE
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "drivers/OPH_SEARCH_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_pid_library.h"

#define OPH_SEARCH_AND_SEPARATOR	","

#ifdef OPH_ODB_MNG
void _print_bson(const bson_t * b)
{
	char *str;
	str = bson_as_json(b, NULL);
	pmesg(LOG_INFO, __FILE__, __LINE__, "%s\n", str);
	bson_free(str);
}
#endif

int _oph_search_recursive_search(const char *folder_abs_path, int folderid, const char *filters, ophidiadb * oDB, int *max_lengths, int max_lengths_size, char *query, char *path, int is_start,
				 oph_json * oper_json, int is_objkey_printable, int recursive_search)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_FIELD *fields;
	MYSQL_ROW row;
	int i, j, num_fields;
	char *buffer;
	char *buffer2;

	if (is_start) {
		buffer = malloc(MYSQL_BUFLEN);
		if (!buffer) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for query\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		buffer2 = malloc(MYSQL_BUFLEN);
		if (!buffer2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for path\n");
			free(buffer);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		buffer = query;
		buffer2 = path;
	}

	snprintf(buffer, MYSQL_BUFLEN, MYSQL_QUERY_OPH_SEARCH_READ_INSTANCES, folderid, filters);
	if (mysql_query(oDB->conn, buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		free(buffer);
		free(buffer2);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	fields = mysql_fetch_fields(res);
	num_fields = mysql_num_fields(res) - 1;

	if (is_start) {
		//print header
		printf("+");
		for (i = 0; i < max_lengths_size; i++) {
			printf("-");
			for (j = 0; j < max_lengths[i]; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");
		printf("|");
		for (i = 0; i < max_lengths_size; i++) {
			printf(" ");
			printf("%-*s", max_lengths[i], fields[i].name);
			printf(" |");
		}
		printf("\n");
		printf("+");
		for (i = 0; i < max_lengths_size; i++) {
			printf("-");
			for (j = 0; j < max_lengths[i]; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");

		if (is_objkey_printable) {
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int iii, jjj;

			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "keys");
				mysql_free_result(res);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; ++jjj) {
				jsonkeys[jjj] = strdup(fields[jjj + 1].name ? fields[jjj + 1].name : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					mysql_free_result(res);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				mysql_free_result(res);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; ++jjj) {
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					mysql_free_result(res);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_add_grid(oper_json, OPH_JSON_OBJKEY_SEARCH, "Searching results", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				mysql_free_result(res);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (iii = 0; iii < num_fields; iii++)
				if (fieldtypes[iii])
					free(fieldtypes[iii]);
			if (fieldtypes)
				free(fieldtypes);
		}
	}
	//print each ROW
	while ((row = mysql_fetch_row(res))) {
		printf("|");
		for (i = 0; i < max_lengths_size; i++) {
			printf(" ");
			if (!strcasecmp(fields[i].name, "Container")) {
				if (folder_abs_path[strlen(folder_abs_path) - 1] == '/') {
					snprintf(buffer2, MYSQL_BUFLEN, "%s%s", folder_abs_path, row[i]);
				} else {
					snprintf(buffer2, MYSQL_BUFLEN, "%s/%s", folder_abs_path, row[i]);
				}
				printf("%-*s", max_lengths[i], buffer2);
			} else {
				printf("%-*s", max_lengths[i], row[i]);
			}
			printf(" |");
		}
		printf("\n");

		if (is_objkey_printable) {
			char **jsonvalues = NULL;
			int iii, jjj;
			jsonvalues = (char **) calloc(num_fields, sizeof(char *));
			if (!jsonvalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "values");
				mysql_free_result(res);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; ++jjj) {
				if (jjj)
					jsonvalues[jjj] = strdup(row[1 + jjj] ? row[1 + jjj] : "");
				else {
					char *tmp_uri = NULL, *pid = NULL;
					if (oph_pid_get_uri(&tmp_uri)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "PID");
						mysql_free_result(res);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (oph_pid_create_pid(tmp_uri, (int) strtol(row[0], NULL, 10), (int) strtol(row[1], NULL, 10), &pid)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "PID");
						mysql_free_result(res);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (tmp_uri)
						free(tmp_uri);
					jsonvalues[jjj] = strdup(pid ? pid : "");
				}
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					mysql_free_result(res);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_SEARCH, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				mysql_free_result(res);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
		}
	}
	mysql_free_result(res);

	//recursive step
	snprintf(buffer, MYSQL_BUFLEN, MYSQL_QUERY_OPH_SEARCH_READ_SUBFOLDERS, folderid);
	if (mysql_query(oDB->conn, buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		free(buffer);
		free(buffer2);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	while (recursive_search && (row = mysql_fetch_row(res))) {
		snprintf(buffer2, MYSQL_BUFLEN, "%s%s%s/", folder_abs_path, folder_abs_path[strlen(folder_abs_path) - 1] == '/' ? "" : "/", row[1]);
		char *subfolder_path = strdup(buffer2);
		if (!subfolder_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for subfolder path\n");
			free(buffer);
			free(buffer2);
			mysql_free_result(res);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (_oph_search_recursive_search(subfolder_path, (int) strtol(row[0], NULL, 10), filters, oDB, max_lengths, max_lengths_size, buffer, buffer2, 0, oper_json, is_objkey_printable, 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Recursive step error\n");
			free(buffer);
			free(buffer2);
			free(subfolder_path);
			mysql_free_result(res);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		free(subfolder_path);
	}
	mysql_free_result(res);

	if (is_start) {
		//print footer
		printf("+");
		for (i = 0; i < max_lengths_size; i++) {
			printf("-");
			for (j = 0; j < max_lengths[i]; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");

		if (buffer) {
			free(buffer);
			buffer = NULL;
		}
		if (buffer2) {
			free(buffer2);
			buffer2 = NULL;
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int recursive_get_max_lengths(int folder_abs_path_len, int folderid, const char *filters, ophidiadb * oDB, int **max_lengths, int *max_lengths_size, char *query, int is_start)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_FIELD *fields;
	MYSQL_ROW row;

	int i, tmp;
	char *buffer;

	if (is_start) {
		buffer = malloc(MYSQL_BUFLEN);
		if (!buffer) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for query\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		buffer = query;
	}

#ifdef OPH_ODB_MNG

#else

	snprintf(buffer, MYSQL_BUFLEN, MYSQL_QUERY_OPH_SEARCH_READ_INSTANCES, folderid, filters);
	if (mysql_query(oDB->conn, buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		free(buffer);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	fields = mysql_fetch_fields(res);
	if (is_start) {
		*max_lengths_size = mysql_num_fields(res);
		*max_lengths = (int *) malloc((*max_lengths_size) * sizeof(int));
		if (!(*max_lengths)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(buffer);
			mysql_free_result(res);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < *max_lengths_size; i++) {
			(*max_lengths)[i] = 0;
		}
	}
	for (i = 0; i < *max_lengths_size; i++) {
		if (!strcasecmp(fields[i].name, "Container")) {
			tmp = ((*max_lengths)[i] > (int) (fields[i].max_length + folder_abs_path_len)) ? (*max_lengths)[i] : (int) (fields[i].max_length + folder_abs_path_len);
		} else {
			tmp = ((*max_lengths)[i] > (int) fields[i].max_length) ? (*max_lengths)[i] : (int) fields[i].max_length;
		}
		(*max_lengths)[i] = (tmp > (int) fields[i].name_length) ? tmp : (int) fields[i].name_length;
	}
	mysql_free_result(res);

#endif

	snprintf(buffer, MYSQL_BUFLEN, MYSQL_QUERY_OPH_SEARCH_READ_SUBFOLDERS, folderid);
	if (mysql_query(oDB->conn, buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		free(buffer);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	res = mysql_store_result(oDB->conn);
	while ((row = mysql_fetch_row(res))) {
		if (recursive_get_max_lengths(folder_abs_path_len + strlen(row[1]) + 1, (int) strtol(row[0], NULL, 10), filters, oDB, max_lengths, max_lengths_size, buffer, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Recursive step error\n");
			free(buffer);
			mysql_free_result(res);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	mysql_free_result(res);

	if (is_start) {
		if (buffer) {
			free(buffer);
			buffer = NULL;
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int get_filters_string(char **container_filter, int container_filter_num, char **metadata_key_filter, int metadata_key_filter_num, char **metadata_value_filter, int metadata_value_filter_num,
		       char **filters_string)
{
	int n = 0, m = 0;
	int i, j = 0, k, t;

	*filters_string = (char *) malloc(MYSQL_BUFLEN);
	if (!(*filters_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for filters\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	(*filters_string)[n] = 0;

	char tmp[MYSQL_BUFLEN], *_tmp, *pch, *sp;

#ifdef OPH_ODB_MNG

	// Filter on containers is not provided

	if (strcasecmp(metadata_key_filter[0], OPH_COMMON_ALL_FILTER)) {
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "{ $or : [");
		for (i = k = t = 0; i < metadata_key_filter_num; i++, t = 0) {
			strcpy(tmp, metadata_key_filter[i]);
			for (_tmp = tmp, sp = NULL; (pch = strtok_r(_tmp, OPH_SEARCH_AND_SEPARATOR, &sp)); _tmp = NULL, k++, t++) {
				n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "%s{ label : \"/%s/\" }", k ? ", " : "", pch);
				if (strcasecmp(metadata_value_filter[0], OPH_COMMON_ALL_FILTER) && (j < metadata_value_filter_num)) {
					n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, ", { value : \"/%s/\" }", metadata_value_filter[j]);
					j++;
				}
			}
		}
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "] }");
	}

	if (strcasecmp(metadata_value_filter[0], OPH_COMMON_ALL_FILTER) && (j < metadata_value_filter_num)) {
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "{ $or : [");
		for (i = j, k = 0; i < metadata_value_filter_num; i++, k++)
			n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "%s{ value : \"/%s/\" }", k ? ", " : "", metadata_value_filter[i]);
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "] }");
	}
#else

	char query[MYSQL_BUFLEN];
	*query = 0;

	if (strcasecmp(container_filter[0], OPH_COMMON_ALL_FILTER)) {
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "AND (");
		m += snprintf(query + m, MYSQL_BUFLEN - m, m ? "AND (" : "(");
		for (i = 0; i < container_filter_num; i++) {
			n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "%scontainername LIKE '%s'", i ? " OR " : "", container_filter[i]);
			m += snprintf(query + m, MYSQL_BUFLEN - m, "%scontainer=%s", i ? " OR " : "", container_filter[i]);
		}
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, ") ");
		m += snprintf(query + m, MYSQL_BUFLEN - m, ") ");
	}

	if (strcasecmp(metadata_key_filter[0], OPH_COMMON_ALL_FILTER)) {
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "AND (");
		m += snprintf(query + m, MYSQL_BUFLEN - m, m ? "AND (" : "(");
		for (i = k = t = 0; i < metadata_key_filter_num; i++, t = 0) {
			strcpy(tmp, metadata_key_filter[i]);
			for (_tmp = tmp, sp = NULL; (pch = strtok_r(_tmp, OPH_SEARCH_AND_SEPARATOR, &sp)); _tmp = NULL, k++, t++) {
				n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "%s(label LIKE '%s'", k ? (t ? " AND " : " OR ") : "", pch);
				m += snprintf(query + m, MYSQL_BUFLEN - m, "%s(key=%s", k ? (t ? " AND " : " OR ") : "", pch);
				if (strcasecmp(metadata_value_filter[0], OPH_COMMON_ALL_FILTER) && (j < metadata_value_filter_num)) {
					n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, " AND CONVERT(value USING latin1) LIKE '%%%s%%')", metadata_value_filter[j]);
					m += snprintf(query + m, MYSQL_BUFLEN - m, " AND value=%s)", metadata_value_filter[j]);
					j++;
				} else {
					n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, ")");
					m += snprintf(query + m, MYSQL_BUFLEN - m, ")");
				}
			}
		}
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, ") ");
		m += snprintf(query + m, MYSQL_BUFLEN - m, ") ");
	}

	if (strcasecmp(metadata_value_filter[0], OPH_COMMON_ALL_FILTER) && (j < metadata_value_filter_num)) {
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "AND (");
		m += snprintf(query + m, MYSQL_BUFLEN - m, m ? "AND (" : "(");
		for (i = j, k = 0; i < metadata_value_filter_num; i++, k++) {
			n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, "%sCONVERT(value USING latin1) LIKE '%%%s%%'", k ? " OR " : "", metadata_value_filter[i]);
			m += snprintf(query + m, MYSQL_BUFLEN - m, "%svalue=%s", k ? " OR " : "", metadata_value_filter[i]);
		}
		n += snprintf((*filters_string) + n, MYSQL_BUFLEN - n, ") ");
		m += snprintf(query + m, MYSQL_BUFLEN - m, ") ");
	}

	query[m] = 0;
	pmesg(LOG_INFO, __FILE__, __LINE__, "Search query: %s\n", query);

#endif

	(*filters_string)[n] = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_SEARCH_operator_handle *) calloc(1, sizeof(OPH_SEARCH_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter_num = 0;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter_num = 0;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num = 0;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->path = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->recursive_search = 0;

	ophidiadb *oDB = &((OPH_SEARCH_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//3 - Fill struct with the correct data

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SEARCH_operator_handle *) handle->operator_handle)->path = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, OPH_IN_PARAM_PATH);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_KEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_KEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_KEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_VALUE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_VALUE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_VALUE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter, &((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	int i;
	for (i = 0; i < ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num; ++i)
		if (strstr(((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter[i], OPH_SEARCH_AND_SEPARATOR)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Separator '%s' cannot be used in filter for values: use it in filter for keys as 'AND'\n", OPH_SEARCH_AND_SEPARATOR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Separator '%s' cannot be used in filter for values: use it in filter for keys as 'AND'\n",
				OPH_SEARCH_AND_SEPARATOR);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SEARCH_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_RECURSIVE_SEARCH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_RECURSIVE_SEARCH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_RECURSIVE_SEARCH);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_SEARCH_operator_handle *) handle->operator_handle)->recursive_search = strcmp(value, OPH_COMMON_NO_VALUE);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int folderid;
	char *abs_path = NULL;
	char *filters = NULL;
	int *max_lengths = NULL;
	int max_lengths_size;
	int permission = 0;
	ophidiadb *oDB = &((OPH_SEARCH_operator_handle *) handle->operator_handle)->oDB;

	if (!strcasecmp(((OPH_SEARCH_operator_handle *) handle->operator_handle)->path, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
		if (oph_odb_fs_path_parsing(".", ((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd, &folderid, &abs_path, oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_PATH_PARSING_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	} else {
		if (oph_odb_fs_path_parsing(((OPH_SEARCH_operator_handle *) handle->operator_handle)->path, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd, &folderid, &abs_path, oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path '%s/%s'\n", ((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd,
			      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->path);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_PATH_PARSING_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	if ((oph_odb_fs_check_folder_session(folderid, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_PATH_NOT_ALLOWED_ERROR, abs_path);
		if (abs_path) {
			free(abs_path);
			abs_path = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	// Change folder_to_be_printed in user format
	if (abs_path) {
		char old_abs_path[1 + strlen(abs_path)];
		strcpy(old_abs_path, abs_path);
		free(abs_path);
		abs_path = NULL;
		if (oph_pid_drop_session_prefix(old_abs_path, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid, &abs_path)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Folder conversion error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Folder conversion error\n");
			if (abs_path) {
				free(abs_path);
				abs_path = NULL;
			}
			return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
		}
	}

	if (get_filters_string
	    (((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter_num,
	     ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter_num,
	     ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num, &filters)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse filters\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_FILTERS_PARSING_ERROR);
		if (abs_path) {
			free(abs_path);
			abs_path = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (recursive_get_max_lengths(strlen(abs_path), folderid, filters, oDB, &max_lengths, &max_lengths_size, NULL, 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Search error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_SEARCH_ERROR);
		if (abs_path) {
			free(abs_path);
			abs_path = NULL;
		}
		if (filters) {
			free(filters);
			filters = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (_oph_search_recursive_search
	    (abs_path, folderid, filters, oDB, max_lengths, max_lengths_size, NULL, NULL, 1, handle->operator_json,
	     oph_json_is_objkey_printable(((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys_num,
					  OPH_JSON_OBJKEY_SEARCH), ((OPH_SEARCH_operator_handle *) handle->operator_handle)->recursive_search)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Search error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_SEARCH_ERROR);
		if (abs_path) {
			free(abs_path);
			abs_path = NULL;
		}
		if (filters) {
			free(filters);
			filters = NULL;
		}
		if (max_lengths) {
			free(max_lengths);
			max_lengths = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (abs_path) {
		free(abs_path);
		abs_path = NULL;
	}
	if (filters) {
		free(filters);
		filters = NULL;
	}
	if (max_lengths) {
		free(max_lengths);
		max_lengths = NULL;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SEARCH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_SEARCH_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->path) {
		free((char *) ((OPH_SEARCH_operator_handle *) handle->operator_handle)->path);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->path = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_SEARCH_operator_handle *) handle->operator_handle)->user);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter) {
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter_num);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->container_filter = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter) {
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter_num);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_key_filter = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter) {
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter,
						      ((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter_num);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->metadata_value_filter = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_SEARCH_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_SEARCH_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
