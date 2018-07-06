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

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "drivers/OPH_SCRIPT_operator.h"

#include <errno.h>
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_framework_paths.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"
#include "oph_soap.h"

#define MAX_OUT_LEN 500*1024
#define OPH_SCRIPT_MARKER '\''
#define OPH_SCRIPT_MARKER2 '\\'
#define OPH_SCRIPT_MARKER3 ' '

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_SCRIPT_operator_handle *) calloc(1, sizeof(OPH_SCRIPT_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args_num = -1;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id = NULL;
	((OPH_SCRIPT_operator_handle *) handle->operator_handle)->list = 0;

	//3 - Fill struct with the correct data
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, &((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCRIPT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCRIPT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SCRIPT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "script");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ARGS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ARGS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_ARGS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args, &((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDOUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDOUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDOUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "stdout");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDERR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDERR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDERR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "stderr");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LIST);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LIST);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LIST);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->list = 1;
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_code = (char *) strdup(session_code))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session code");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char buff[OPH_COMMON_BUFFER_LEN];
	snprintf(buff, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code);
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path = (char *) strdup(buff))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	snprintf(buff, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH, oph_pid_uri(), session_code);
	if (!(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url = (char *) strdup(buff))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session url");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID);
	if (value && !(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_WORKFLOWID);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_MARKERID);
	if (value && !(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_MARKERID);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (value && !(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->list
	    && oph_json_is_objkey_printable(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num,
					    OPH_JSON_OBJKEY_SCRIPT)) {
		char config[OPH_COMMON_BUFFER_LEN];
		snprintf(config, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_SCRIPT_CONF_FILE_PATH, OPH_ANALYTICS_LOCATION);
		FILE *file = fopen(config, "r");
		if (!file) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "%s: no such file\n", config);
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "%s: no such file\n", config);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char **jsonkeys = NULL, **fieldtypes = NULL;
		int num_fields = 1, iii, jjj = 0;
#if defined(OPH_DEBUG_LEVEL_1) || defined(OPH_DEBUG_LEVEL_2)
		num_fields = 2;
#endif
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "keys");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jsonkeys[jjj] = strdup("SCRIPT");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
#if defined(OPH_DEBUG_LEVEL_1) || defined(OPH_DEBUG_LEVEL_2)
		jjj++;
		jsonkeys[jjj] = strdup("COMMAND");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
#endif
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "fieldtypes");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
#if defined(OPH_DEBUG_LEVEL_1) || defined(OPH_DEBUG_LEVEL_2)
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
#endif
		jjj++;
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_SCRIPT, "Available scripts", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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

		char script[OPH_COMMON_BUFFER_LEN], *pch, *save_pointer, *key, *value, **jsonvalues;
		while (fgets(script, OPH_COMMON_BUFFER_LEN, file)) {
			save_pointer = NULL;
			key = NULL;
			value = NULL;
			for (pch = strtok_r(script, OPH_SCRIPT_NOP, &save_pointer); pch; pch = strtok_r(NULL, OPH_SCRIPT_NOP, &save_pointer)) {
				if (!key)
					key = pch;
				else if (!value)
					value = pch;
				else
					break;
			}
			if (!key || !strlen(key) || !value || !strlen(value))
				continue;
			if (value[strlen(value) - 1] == '\n')
				value[strlen(value) - 1] = 0;
			if (!strlen(value))
				continue;

			jjj = 0;
			jsonvalues = (char **) calloc(num_fields, sizeof(char *));
			if (!jsonvalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "values");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonvalues[jjj] = strdup(key);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
#if defined(OPH_DEBUG_LEVEL_1) || defined(OPH_DEBUG_LEVEL_2)
			jjj++;
			jsonvalues[jjj] = strdup(value);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
#endif
			jjj++;
			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_SCRIPT, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
		}
		fclose(file);

		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Create dir if not exist
	struct stat st;
	char user_space;
	if (oph_pid_get_user_space(&user_space)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read user_space\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read user_space\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (user_space) {
		free(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path = NULL;
		free(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url = NULL;
	} else if (stat(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path, &st)) {
		if (oph_dir_r_mkdir(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to create dir %s\n", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path);
			logging(LOG_WARNING, __FILE__, __LINE__, 0, OPH_LOG_GENERIC_DIR_CREATION_ERROR, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path);
			free(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path);
			((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path = NULL;
			free(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url);
			((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url = NULL;
		}
	}

	if (strcmp(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script, OPH_SCRIPT_NOP)) {
		// Check for registered scripts
		char config[OPH_COMMON_BUFFER_LEN];
		snprintf(config, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_SCRIPT_CONF_FILE_PATH, OPH_ANALYTICS_LOCATION);
		FILE *file = fopen(config, "r");
		if (!file) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "%s: no such file\n", config);
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "%s: no such file\n", config);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		int found = 0;
		char script[OPH_COMMON_BUFFER_LEN], *pch, *save_pointer, *key, *value;
		while (fgets(script, OPH_COMMON_BUFFER_LEN, file)) {
			save_pointer = NULL;
			key = NULL;
			value = NULL;
			for (pch = strtok_r(script, OPH_SCRIPT_NOP, &save_pointer); pch; pch = strtok_r(NULL, OPH_SCRIPT_NOP, &save_pointer)) {
				if (!key)
					key = pch;
				else if (!value)
					value = pch;
				else
					break;
			}
			if (!key || !strlen(key) || !value || !strlen(value))
				continue;
			if (value[strlen(value) - 1] == '\n')
				value[strlen(value) - 1] = 0;
			if (!strlen(value))
				continue;
			if (!strcmp(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script, key)) {
				pch = value;
				while (pch && (*pch == ' '))
					pch++;
				if (!pch || !*pch)
					continue;
				else if (*pch != '/') {
					snprintf(config, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_SCRIPT_FOLDER_PATH "/%s", OPH_ANALYTICS_LOCATION, value);
					value = config;
				}

				free(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
				((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script = strdup(value);
				found = 1;
				break;
			}
		}
		fclose(file);

		if (!found) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "%s: no such script\n", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "%s: no such script\n", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//If script is not a regular file then error
		if (stat(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script, &st)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "%s: no such file\n", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "%s: no such file\n", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char *ptr = realpath(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script, NULL);
		if (!ptr) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to resolve path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "Unable to resolve path\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (!strncmp(ptr, "/bin/", 5) || !strncmp(ptr, "/sbin/", 6) || !strncmp(ptr, "/usr/bin/", 9) || !strncmp(ptr, "/usr/sbin/", 10)) {
			free(ptr);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Permission denied\n");
			logging(LOG_ERROR, __FILE__, __LINE__, 0, "Permission denied\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		free(ptr);
	}

	char *base_src_path = NULL;
	if (oph_pid_get_base_src_path(&base_src_path)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Unable to read OphidiaDB configuration\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int error = 0, n = 0, i;
	size_t j, k;
	char command[OPH_COMMON_BUFFER_LEN];
	memset(command, 0, OPH_COMMON_BUFFER_LEN);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_DATA_PATH='%s' ", base_src_path ? base_src_path : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_SESSION_PATH='%s' ",
		      ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path ? ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_SESSION_URL='%s' ",
		      ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url ? ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_SESSION_CODE='%s' ", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_code);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_WORKFLOW_ID=%s ",
		      ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id ? ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id : 0);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_MARKER_ID=%s ",
		      ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id ? ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id : 0);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_SERVER_HOST='%s' ", ((oph_soap_data *) handle->soap_data)->host ? ((oph_soap_data *) handle->soap_data)->host : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_SERVER_PORT=%s ", ((oph_soap_data *) handle->soap_data)->port ? ((oph_soap_data *) handle->soap_data)->port : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_SCRIPT_USER='%s' ",
		      ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user ? ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "%s ", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);

	char *new_arg = NULL, *arg;
	for (i = 0; i < ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args_num; i++) {
		arg = ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args[i];
		if (arg) {
			new_arg = (char *) calloc(4 * strlen(arg), sizeof(char));
			for (j = k = 0; j < strlen(arg); ++j, ++k) {
				if (arg[j] == OPH_SCRIPT_MARKER3)
					new_arg[k++] = OPH_SCRIPT_MARKER;
				new_arg[k] = arg[j];
				if (arg[j] == OPH_SCRIPT_MARKER) {
					new_arg[++k] = OPH_SCRIPT_MARKER2;
					new_arg[++k] = OPH_SCRIPT_MARKER;
					new_arg[++k] = OPH_SCRIPT_MARKER;
				} else if (arg[j] == OPH_SCRIPT_MARKER3)
					new_arg[++k] = OPH_SCRIPT_MARKER;
			}
			new_arg[k] = 0;
			n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "'%s' ", new_arg);
		}
	}

	if (strcmp(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir, "stdout")) {
		n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "1>>%s ", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir);
	}
	if (strcmp(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir, "stderr")) {
		n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "2>>%s", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir);
	}
	// Dynamic creation of the folders
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path) {
		char dirname[OPH_COMMON_BUFFER_LEN];
		snprintf(dirname, OPH_COMMON_BUFFER_LEN, "%s/%s", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path,
			 ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id);
		struct stat ss;
		if (stat(dirname, &ss) && (errno == ENOENT)) {
			int i;
			for (i = 0; dirname[i]; ++i) {
				if (dirname[i] == '/') {
					dirname[i] = 0;
					mkdir(dirname, 0755);
					dirname[i] = '/';
				}
			}
			mkdir(dirname, 0755);
		}
	}

	char system_output[MAX_OUT_LEN];
	memset(system_output, 0, MAX_OUT_LEN);
	char line[OPH_COMMON_BUFFER_LEN];

	int s = 0;
	FILE *fp = popen(command, "r");
	if (!fp)
		error = -1;
	else {
		while (fgets(line, OPH_COMMON_BUFFER_LEN, fp))
			if ((s = MAX_OUT_LEN - strlen(system_output)) > 1)
				strncat(system_output, line, s);
		error = pclose(fp);
	}

	// Print command and output in text log
	printf("Command:\n%s\n\nScript output:\n%s\n", command, system_output);

	// ADD COMMAND TO JSON AS TEXT
	s = oph_json_is_objkey_printable(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num,
					 OPH_JSON_OBJKEY_SCRIPT);
	if (s && oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_SCRIPT, "Script output", system_output)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
	}

	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path
	    && oph_json_is_objkey_printable(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num,
					    OPH_JSON_OBJKEY_SCRIPT_URL)) {
		if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id)
			snprintf(system_output, MAX_OUT_LEN, "%s/%s", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url,
				 ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id);
		else
			snprintf(system_output, MAX_OUT_LEN, "%s", ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url);
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_SCRIPT_URL, "Output URL", system_output)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
		}
	}
	// ADD OUTPUT PID TO NOTIFICATION STRING
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url) {
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_LINK, s ? system_output : ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url);
		if (handle->output_string) {
			strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
			free(handle->output_string);
		}
		handle->output_string = strdup(tmp_string);
	}

	char return_code[MAX_OUT_LEN];
	if (error == -1)
		snprintf(return_code, MAX_OUT_LEN, "System command failed");
	else if (WEXITSTATUS(error) == 127)
		snprintf(return_code, MAX_OUT_LEN, "System command cannot be executed (127)");
	else if (WEXITSTATUS(error) != 0)
		snprintf(return_code, MAX_OUT_LEN, "Script failed with code %d", WEXITSTATUS(error));
	else
		snprintf(return_code, MAX_OUT_LEN, "Script executed correctly (0)");
	if (oph_json_is_objkey_printable
	    (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_SCRIPT_RETURNCODE)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_SCRIPT_RETURNCODE, "Return code", return_code)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
		}
	}
	if (error)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "%s\n", return_code);
	if (error == -1)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_COMMAND_FAILED);
	else if (WEXITSTATUS(error) == 127)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_COMMAND_NOT_EXECUTED);
	else if (WEXITSTATUS(error) != 0)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, return_code);

	return error ? OPH_ANALYTICS_OPERATOR_COMMAND_ERROR : OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args) {
		oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args_num);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->args = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->err_redir = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->out_redir = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->script = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_path = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_url = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_code) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_code);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->session_code = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->workflow_id = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->marker_id = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_SCRIPT_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_SCRIPT_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
