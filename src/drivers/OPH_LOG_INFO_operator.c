/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2022 CMCC Foundation

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

#include "drivers/OPH_LOG_INFO_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <dirent.h>
#include <strings.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_framework_paths.h"

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_LOG_INFO_operator_handle *) calloc(1, sizeof(OPH_LOG_INFO_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->log_type = 0;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id = -1;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type = NULL;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->nlines = OPH_LOG_INFO_DEFAULT_NLINES;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);

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
	if (oph_tp_parse_multiple_value_param(value, &((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys, &((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LOGGING_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LOGGING_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LOGGING_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (!strcasecmp(value, OPH_LOG_INFO_SERVER)) {
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->log_type = OPH_LOG_INFO_SERVER_LOG_REQUEST;
	} else if (!strcasecmp(value, OPH_LOG_INFO_CONTAINER)) {
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->log_type = OPH_LOG_INFO_CONTAINER_LOG_REQUEST;
	} else if (!strcasecmp(value, OPH_LOG_INFO_IOSERVER)) {
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->log_type = OPH_LOG_INFO_IOSERVER_LOG_REQUEST;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_BAD_LOG_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IOSERVER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "server_type");
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_CONTAINER_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id = (int) strtol(value, NULL, 10);
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Container_id must be >=0\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_BAD_CONTAINER_ID, OPH_IN_PARAM_CONTAINER_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LOG_LINES_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LOG_LINES_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LOG_LINES_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->nlines = (int) strtol(value, NULL, 10);
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->nlines <= 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "nlines must be >0\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_BAD_LINES_NUMBER, OPH_IN_PARAM_LOG_LINES_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "username");
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_INFO_START);

	int is_objkey_printable =
	    oph_json_is_objkey_printable(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys_num,
					 OPH_JSON_OBJKEY_LOG_INFO);
	if (is_objkey_printable) {
		int num_fields = 3, iii, jjj = 0;

		// Header
		char **jsonkeys = NULL;
		char **fieldtypes = NULL;
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "keys");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jsonkeys[jjj] = strdup("TIMESTAMP");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("TYPE");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("MESSAGE");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "fieldtypes");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LOG_INFO, "Log Data", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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

	int log_type = ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->log_type;
	int container_id = ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id;
	char *server_type = ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type;
	int nlines = ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->nlines;

	int folder_id = 0;
	int permission = 0;
	char *user = ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user;
	ophidiadb *oDB = &((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->oDB;

	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id != 0) {
		if ((oph_odb_fs_retrive_container_folder_id(oDB, ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->container_id, &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Container with this id doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_CONTAINER_FOLDER_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (strncmp(user, OPH_COMMON_ADMIN_USERNAME, strlen(OPH_COMMON_ADMIN_USERNAME))
		    && ((oph_odb_fs_check_folder_session(folder_id, ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission)) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to read this log\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_PERMISSION_ERROR, user);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}
	char *lines;
	if (nlines > 0) {
		lines = (char *) malloc(nlines * OPH_COMMON_BUFFER_LEN + 1);
		if (!lines) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid lines_number value\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_INVALID_LINES_NUMBER, OPH_IN_PARAM_LOG_LINES_NUMBER);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	FILE *file;
	char name[OPH_COMMON_BUFFER_LEN];
	switch (log_type) {
		case OPH_LOG_INFO_SERVER_LOG_REQUEST:
			snprintf(name, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_SERVER_LOG_PATH, OPH_ANALYTICS_LOCATION);
			file = fopen(name, "r");
			if (file == NULL) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "File %s could not be opened\n", name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_FILE_NOT_OPENED, name);
				free(lines);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			break;
		case OPH_LOG_INFO_CONTAINER_LOG_REQUEST:
			if (container_id < 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "container_id must be >=0\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_BAD_CONTAINER_ID, OPH_IN_PARAM_CONTAINER_ID_FILTER);
				free(lines);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			snprintf(name, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_CONTAINER_LOG_PATH, OPH_ANALYTICS_LOCATION, container_id);
			file = fopen(name, "r");
			if (file == NULL) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "File %s could not be opened\n", name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_FILE_NOT_OPENED, name);
				free(lines);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			break;
		case OPH_LOG_INFO_IOSERVER_LOG_REQUEST:
			snprintf(name, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_IOSERVER_LOG_PATH2, OPH_ANALYTICS_LOCATION, server_type);
			file = fopen(name, "r");
			if (file == NULL) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "File %s could not be opened\n", name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_FILE_NOT_OPENED, name);
				free(lines);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Request type not recognized\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_REQUEST_NOT_RECOGNIZED);
			free(lines);
			return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	fseek(file, 0, SEEK_END);
	if (!ftell(file)) {
		printf("File %s is empty\n", name);
		pmesg(LOG_WARNING, __FILE__, __LINE__, "File %s is empty\n", name);
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_FILE_EMPTY, name);
		logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_INFO_END);
		fclose(file);
		free(lines);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	memset(lines, 0, nlines * OPH_COMMON_BUFFER_LEN + 1);

	fseek(file, -1, SEEK_END);
	char c;
	int i = nlines * OPH_COMMON_BUFFER_LEN - 1;
	int count = 0;
	int flag = 0;
	do {
		c = getc(file);
		if (c == '\n' && i != (nlines * OPH_COMMON_BUFFER_LEN - 1)) {
			count++;
			if (count == nlines) {
				flag = 1;
				break;
			}
		}
		lines[i] = c;
		i--;
	} while ((fseek(file, -2, SEEK_CUR)) == 0);
	fclose(file);

	if (flag == 0) {
		count++;
	}

	printf("Last %d lines (max %d) from %s\n\n", count, nlines, name);
	char *ptr = 0;
	for (i = 0; i < nlines * OPH_COMMON_BUFFER_LEN + 1; i++) {
		if (lines[i] != '\0') {
			ptr = lines + i;
			break;
		}
	}
	printf("%s\n", ptr ? ptr : "");

	if (ptr && is_objkey_printable) {
		int num_fields = 3, iii, jjj = 0, kkk = 0, print_data, k;
		char *jsontmp[num_fields];
		char **jsonvalues = NULL;
		char *my_ptr = ptr;

		while (my_ptr) {
			for (k = 0; k < num_fields; ++k)
				jsontmp[k] = 0;
			k = 0;
			while (my_ptr && (*my_ptr != '\n') && (*my_ptr != '\0')) {
				if (*my_ptr == '[') {
					if (k < num_fields)
						jsontmp[k++] = my_ptr + 1;
				} else if (*my_ptr == ']') {
					if (!jsontmp[1] || !jsontmp[2])
						*my_ptr = '\0';
					else if (*(jsontmp[2]) != '\t')
						jsontmp[2] = my_ptr + 1;
				}
				my_ptr++;
			}

			if (!my_ptr || (*my_ptr == '\0'))
				break;

			*my_ptr = '\0';
			my_ptr++;

			if (jsontmp[2] && (*(jsontmp[2]) == '\t'))
				(jsontmp[2])++;

			print_data = 1;
			for (k = 0; k < num_fields; ++k)
				if (!jsontmp[k]) {
					print_data = 0;
					break;
				}
			if (print_data) {
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "values");
					free(lines);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (jjj = 0; jjj < num_fields; jjj++) {
					jsonvalues[jjj] = strdup(jsontmp[jjj]);
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						free(lines);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LOG_INFO, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					free(lines);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}
			kkk++;
		}
	}

	free(lines);

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_INFO_END);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOG_INFO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user);
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type) {
		free((char *) ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type);
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->server_type = NULL;
	}
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_LOG_INFO_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_LOG_INFO_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
