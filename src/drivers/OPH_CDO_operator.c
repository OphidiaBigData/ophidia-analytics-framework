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

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "drivers/OPH_CDO_operator.h"

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
#define OPH_CDO_MARKER '\''
#define OPH_CDO_MARKER2 '\\'
#define OPH_CDO_MARKER3 ' '

#define OPH_CDO_DEFAULT_OUTPUT_PATH "default"

#define OPH_CDO_BEGIN_PARAMETER "%"

int _is_ended(char pointer)
{
	switch (pointer) {
		case ' ':
		case '[':
		case ']':
		case '%':
			return 1;
	}
	return 0;
}

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

	if (!(handle->operator_handle = (OPH_CDO_operator_handle *) calloc(1, sizeof(OPH_CDO_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CDO_operator_handle *) handle->operator_handle)->command = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->inputs = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num = 0;
	((OPH_CDO_operator_handle *) handle->operator_handle)->args = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->args_num = -1;
	((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CDO_operator_handle *) handle->operator_handle)->session_path = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->session_url = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->space = 0;
	((OPH_CDO_operator_handle *) handle->operator_handle)->output_path = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->output_name = NULL;
	((OPH_CDO_operator_handle *) handle->operator_handle)->force = 0;
	((OPH_CDO_operator_handle *) handle->operator_handle)->nthread = 0;

	//3 - Fill struct with the correct data
	char tmp[OPH_COMMON_BUFFER_LEN];
	char *value, *value2 = NULL;
	size_t size;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMMAND);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMMAND);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_COMMAND);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->command = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "script");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CDO_operator_handle *) handle->operator_handle)->inputs, &((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ARGS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ARGS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_ARGS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->inputs && (((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num > 0)) {
		int i;
		char *base_src_path = NULL;
		if (oph_pid_get_base_src_path(&base_src_path)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base user_path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base user path\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (base_src_path) {
			value = value2 = strdup(value);
			for (i = 0; i < ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num; i++) {
				if (((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i] && strlen(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i])) {
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", base_src_path ? base_src_path : "",
						 *((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i] != '/' ? "/" : "", ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i]);
					free(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i]);
					((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i] = strdup(tmp);
				}
			}
			free(base_src_path);
		}
		if (!strstr(((OPH_CDO_operator_handle *) handle->operator_handle)->command, OPH_CDO_BEGIN_PARAMETER)) {
			size_t old_size;
			value = value2 = strdup(value);
			for (i = 0; i < ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num; i++) {
				if (((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i] && strlen(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i])) {
					size = strlen(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i]);
					old_size = strlen(value);
					if (old_size) {
						value2 = (char *) malloc((2 + old_size + size) * sizeof(char));
						sprintf(value2, "%s|%s", value, ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i]);
					} else
						value2 = strdup(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[i]);
					free(value);
					value = value2;
				}
			}
		}
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CDO_operator_handle *) handle->operator_handle)->args, &((OPH_CDO_operator_handle *) handle->operator_handle)->args_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		if (value2)
			free(value2);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (value2)
		free(value2);	// Do not put it before previous parsing function

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDOUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDOUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDOUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "stdout");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDERR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDERR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDERR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "stderr");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SPACE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SPACE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SPACE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0)
		((OPH_CDO_operator_handle *) handle->operator_handle)->space = 1;
	else if (strncmp(value, OPH_COMMON_NO_VALUE, OPH_TP_TASKLEN)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", OPH_IN_PARAM_SPACE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SPACE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->session_code = (char *) strdup(session_code))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "session code");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char buff[OPH_COMMON_BUFFER_LEN];
	snprintf(buff, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code);
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->session_path = (char *) strdup(buff))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "session path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	snprintf(buff, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH, oph_pid_uri(), session_code);
	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->session_url = (char *) strdup(buff))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "session url");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID);
	if (value && !(((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, OPH_ARG_WORKFLOWID);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_MARKERID);
	if (value && !(((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, OPH_ARG_MARKERID);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (value && !(((OPH_CDO_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	char user_space, user_space_default = 0;
	if (oph_pid_get_user_space(&user_space)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read user_space\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read user_space\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char *output_path = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_PATH);
	char *output_name = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_NAME);
	char *output = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT);
	char home[2];
	home[0] = '/';
	home[1] = 0;
	if (output && ((size = strlen(output)))) {
		char *pointer = output + size;
		while ((pointer >= output) && (*pointer != '/'))
			pointer--;
		if (pointer < output) {
			output_name = output;
			if ((output[size - 3] == '.') && (output[size - 2] == 'n') && (output[size - 1] == 'c'))
				output[size - 3] = 0;
		} else {
			if (pointer == output)
				output_path = home;
			else
				output_path = output;
			*pointer = 0;
			pointer++;
			if (pointer && *pointer) {
				output_name = pointer;
				if ((output[size - 3] == '.') && (output[size - 2] == 'n') && (output[size - 1] == 'c'))
					output[size - 3] = 0;
			}
		}
	}

	value = output_path;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char *cdd = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
	if (!cdd) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_GENERIC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (*cdd != '/') {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if ((strlen(cdd) > 1) && strstr(cdd, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (!strcmp(value, OPH_CDO_DEFAULT_OUTPUT_PATH))
		value = user_space ? &user_space_default : cdd;

	if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->output_path = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "output path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (strstr(((OPH_CDO_operator_handle *) handle->operator_handle)->output_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *pointer = ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path;
	while (pointer && (*pointer == ' '))
		pointer++;
	if (pointer) {
		if ((*pointer != '/') && (strlen(cdd) > 1)) {
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", cdd + 1, pointer);
			((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user = strdup(tmp);
			free(((OPH_CDO_operator_handle *) handle->operator_handle)->output_path);
			((OPH_CDO_operator_handle *) handle->operator_handle)->output_path = strdup(tmp);
			pointer = ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path;
		}
		if (!((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user)
			((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user = strdup(pointer);
		if (oph_pid_get_base_src_path(&value)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base user_path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base user path\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
		free(((OPH_CDO_operator_handle *) handle->operator_handle)->output_path);
		((OPH_CDO_operator_handle *) handle->operator_handle)->output_path = strdup(tmp);
		if (value)
			free(value);
	}

	value = output_name;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_CDO_DEFAULT_OUTPUT_PATH)) {
		if (!(((OPH_CDO_operator_handle *) handle->operator_handle)->output_name = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "output name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	size_t s;
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->output_name) {
		for (s = 0; s < strlen(((OPH_CDO_operator_handle *) handle->operator_handle)->output_name); s++) {
			if ((((OPH_CDO_operator_handle *) handle->operator_handle)->output_name[s] == '/')
			    || (((OPH_CDO_operator_handle *) handle->operator_handle)->output_name[s] == ':')) {
				((OPH_CDO_operator_handle *) handle->operator_handle)->output_name[s] = '_';
			}
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FORCE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FORCE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_FORCE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_CDO_operator_handle *) handle->operator_handle)->force = 1;

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MISSING_INPUT_PARAMETER, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CDO_operator_handle *) handle->operator_handle)->nthread = (int) strtol(value, NULL, 10);

	char *path = ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path;
	if (!path) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_MEMORY_ERROR_INPUT, "output_path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	size = strlen(path);
	if (size && (path[size - 1] == '/'))
		path[--size] = 0;

	//Create dir if not exist
	struct stat st;
	if (stat(path, &st)) {
		if (errno == EACCES) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CDO_PERMISSION_ERROR, path);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_PERMISSION_ERROR, path);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		} else if ((errno != ENOENT) || oph_dir_r_mkdir(path)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_CDO_DIR_CREATION_ERROR, path);
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_DIR_CREATION_ERROR, path);
		}
	}
	//Create a random name
	if (!((OPH_CDO_operator_handle *) handle->operator_handle)->output_name) {
		char tmp[20];
		srand(time(NULL));
		snprintf(tmp, 20, "%d", rand());
		((OPH_CDO_operator_handle *) handle->operator_handle)->output_name = strdup(tmp);
	}
	//Check if file exists
	char file_name[OPH_COMMON_BUFFER_LEN] = { '\0' };
	snprintf(file_name, OPH_COMMON_BUFFER_LEN, OPH_CDO_OUTPUT_PATH_SINGLE_FILE, path, ((OPH_CDO_operator_handle *) handle->operator_handle)->output_name);
	if (stat(file_name, &st)) {
		if (errno == EACCES) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CDO_PERMISSION_ERROR, file_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_PERMISSION_ERROR, file_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		} else if (errno != ENOENT) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CDO_FILE_STAT_ERROR, file_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_FILE_STAT_ERROR, file_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	//File exists
	else {
		//If it is not a regular file
		if (!S_ISREG(st.st_mode)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CDO_OVERWRITE_FOLDER_ERROR, file_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_OVERWRITE_FOLDER_ERROR, file_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (!((OPH_CDO_operator_handle *) handle->operator_handle)->force) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_CDO_FILE_EXISTS, file_name);
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_FILE_EXISTS, file_name);
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handler\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Create dir if not exist
	struct stat st;
	char user_space = 0;
	if (oph_pid_get_user_space(&user_space)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read user_space\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read user_space\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (user_space) {
		free(((OPH_CDO_operator_handle *) handle->operator_handle)->session_path);
		((OPH_CDO_operator_handle *) handle->operator_handle)->session_path = NULL;
		free(((OPH_CDO_operator_handle *) handle->operator_handle)->session_url);
		((OPH_CDO_operator_handle *) handle->operator_handle)->session_url = NULL;
	} else if (stat(((OPH_CDO_operator_handle *) handle->operator_handle)->session_path, &st)) {
		if (oph_dir_r_mkdir(((OPH_CDO_operator_handle *) handle->operator_handle)->session_path)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to create dir %s\n", ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path);
			logging(LOG_WARNING, __FILE__, __LINE__, 0, OPH_LOG_GENERIC_DIR_CREATION_ERROR, ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path);
			free(((OPH_CDO_operator_handle *) handle->operator_handle)->session_path);
			((OPH_CDO_operator_handle *) handle->operator_handle)->session_path = NULL;
			free(((OPH_CDO_operator_handle *) handle->operator_handle)->session_url);
			((OPH_CDO_operator_handle *) handle->operator_handle)->session_url = NULL;
		}
	}

	if (strstr(((OPH_CDO_operator_handle *) handle->operator_handle)->command, OPH_CDO_BEGIN_PARAMETER)) {
		int n = 0, index;
		char command_c[OPH_COMMON_BUFFER_LEN], *start_pointer = ((OPH_CDO_operator_handle *) handle->operator_handle)->command, *current_pointer, *number;
		while ((current_pointer = strstr(start_pointer, OPH_CDO_BEGIN_PARAMETER))) {
			for (number = 1 + current_pointer; number && *number && !_is_ended(*number); ++number);
			if (!number)
				break;
			char end_char = *number;
			*number = 0;
			index = strtol(1 + current_pointer, NULL, 10) - 1;	// Non 'C'-like indexing
			if ((index < 0) || (index >= ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Wrong parameter '%s'\n", current_pointer);
				break;
			}
			*current_pointer = 0;
			pmesg(LOG_INFO, __FILE__, __LINE__, "Change parameter %%%d with '%s'\n", index + 1, ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[index]);
			n += snprintf(command_c + n, OPH_COMMON_BUFFER_LEN - n, "%s%s", start_pointer, ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs[index]);
			*number = end_char;
			start_pointer = number;
		}
		snprintf(command_c + n, OPH_COMMON_BUFFER_LEN - n, "%s", start_pointer);
		free(((OPH_CDO_operator_handle *) handle->operator_handle)->command);
		((OPH_CDO_operator_handle *) handle->operator_handle)->command = strdup(command_c);
	}

	char *cdo_path = NULL;
	if (oph_pid_get_cdo_path(&cdo_path)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Unable to read configuration\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char *base_src_path = NULL;
	if (oph_pid_get_base_src_path(&base_src_path)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Unable to read configuration\n");
		if (cdo_path)
			free(cdo_path);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int error = 0, n = 0, i;
	size_t j, k;
	char command[OPH_COMMON_BUFFER_LEN];
	memset(command, 0, OPH_COMMON_BUFFER_LEN);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_DATA_PATH='%s' ", base_src_path ? base_src_path : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_SESSION_PATH='%s' ",
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path ? ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_SESSION_URL='%s' ",
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->session_url ? ((OPH_CDO_operator_handle *) handle->operator_handle)->session_url : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_SESSION_CODE='%s' ", ((OPH_CDO_operator_handle *) handle->operator_handle)->session_code);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_WORKFLOW_ID=%s ",
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id ? ((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id : 0);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_MARKER_ID=%s ",
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id ? ((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id : 0);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_SERVER_HOST='%s' ", ((oph_soap_data *) handle->soap_data)->host ? ((oph_soap_data *) handle->soap_data)->host : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_SERVER_PORT=%s ", ((oph_soap_data *) handle->soap_data)->port ? ((oph_soap_data *) handle->soap_data)->port : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "OPH_CDO_USER='%s' ",
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->user ? ((OPH_CDO_operator_handle *) handle->operator_handle)->user : "");
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "%s/cdo -P %d %s ", cdo_path, ((OPH_CDO_operator_handle *) handle->operator_handle)->nthread,
		      ((OPH_CDO_operator_handle *) handle->operator_handle)->command);

	if (cdo_path)
		free(cdo_path);
	if (base_src_path)
		free(base_src_path);

	char *new_arg = NULL, *arg;
	for (i = 0; i < ((OPH_CDO_operator_handle *) handle->operator_handle)->args_num; i++) {
		arg = ((OPH_CDO_operator_handle *) handle->operator_handle)->args[i];
		if (arg) {
			new_arg = (char *) calloc(4 * strlen(arg), sizeof(char));
			if (new_arg) {
				for (j = k = 0; j < strlen(arg); ++j, ++k) {
					if (!((OPH_CDO_operator_handle *) handle->operator_handle)->space && (arg[j] == OPH_CDO_MARKER3))
						new_arg[k++] = OPH_CDO_MARKER;
					new_arg[k] = arg[j];
					if (arg[j] == OPH_CDO_MARKER) {
						new_arg[++k] = OPH_CDO_MARKER2;
						new_arg[++k] = OPH_CDO_MARKER;
						new_arg[++k] = OPH_CDO_MARKER;
					} else if (!((OPH_CDO_operator_handle *) handle->operator_handle)->space && (arg[j] == OPH_CDO_MARKER3))
						new_arg[++k] = OPH_CDO_MARKER;
				}
				new_arg[k] = 0;
				n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "'%s' ", new_arg);
				free(new_arg);
			}
		}
	}

	// Output file
	char file_name[OPH_COMMON_BUFFER_LEN] = { '\0' };
	snprintf(file_name, OPH_COMMON_BUFFER_LEN, OPH_CDO_OUTPUT_PATH_SINGLE_FILE, ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path,
		 ((OPH_CDO_operator_handle *) handle->operator_handle)->output_name);
	n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "'%s' ", file_name);

	if (strcmp(((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir, "stdout")) {
		n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "1>>%s ", ((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir);
	}
	if (strcmp(((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir, "stderr")) {
		n += snprintf(command + n, OPH_COMMON_BUFFER_LEN - n, "2>>%s", ((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir);
	}
	// Dynamic creation of the folders
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->session_path) {
		char dirname[OPH_COMMON_BUFFER_LEN];
		snprintf(dirname, OPH_COMMON_BUFFER_LEN, "%s/%s", ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path,
			 ((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id);
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
	s = oph_json_is_objkey_printable(((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num,
					 OPH_JSON_OBJKEY_CDO_OUTPUT);
	if (s && oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CDO_OUTPUT, "Script output", system_output)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
	}

	char jsonbuf[OPH_COMMON_BUFFER_LEN];
	*jsonbuf = 0;
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user) {
		char *output_path_file = ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user;
		size_t size = strlen(output_path_file);
		if (size && (output_path_file[size - 1] == '/'))
			output_path_file[--size] = 0;
		if (oph_json_is_objkey_printable
		    (((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_CDO_OUTPUT)) {
			snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, "%s" OPH_CDO_OUTPUT_PATH_SINGLE_FILE, size
				 && *output_path_file != '/' ? "/" : "", output_path_file, ((OPH_CDO_operator_handle *) handle->operator_handle)->output_name);
		}
	}
	// ADD OUTPUT TO NOTIFICATION STRING
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->session_url) {
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;%s=%s;", OPH_IN_PARAM_LINK, ((OPH_CDO_operator_handle *) handle->operator_handle)->session_url, OPH_IN_PARAM_FILE, jsonbuf);
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
	    (((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_CDO_RETURNCODE)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CDO_RETURNCODE, "Return code", return_code)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
		}
	}
	if (error)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "%s\n", return_code);
	if (error == -1)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_COMMAND_FAILED);
	else if (WEXITSTATUS(error) == 127)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_COMMAND_NOT_EXECUTED);
	else if (WEXITSTATUS(error) != 0)
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "%s\n", return_code);

	return error ? OPH_ANALYTICS_OPERATOR_COMMAND_ERROR : OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handler\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CDO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_CDO_operator_handle *) handle->operator_handle)->output_path) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path);
		((OPH_CDO_operator_handle *) handle->operator_handle)->output_path = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user);
		((OPH_CDO_operator_handle *) handle->operator_handle)->output_path_user = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->output_name) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->output_name);
		((OPH_CDO_operator_handle *) handle->operator_handle)->output_name = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->inputs) {
		oph_tp_free_multiple_value_param_list(((OPH_CDO_operator_handle *) handle->operator_handle)->inputs, ((OPH_CDO_operator_handle *) handle->operator_handle)->inputs_num);
		((OPH_CDO_operator_handle *) handle->operator_handle)->inputs = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->args) {
		oph_tp_free_multiple_value_param_list(((OPH_CDO_operator_handle *) handle->operator_handle)->args, ((OPH_CDO_operator_handle *) handle->operator_handle)->args_num);
		((OPH_CDO_operator_handle *) handle->operator_handle)->args = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir);
		((OPH_CDO_operator_handle *) handle->operator_handle)->err_redir = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir);
		((OPH_CDO_operator_handle *) handle->operator_handle)->out_redir = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->command) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->command);
		((OPH_CDO_operator_handle *) handle->operator_handle)->command = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->session_path) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->session_path);
		((OPH_CDO_operator_handle *) handle->operator_handle)->session_path = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->session_url) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->session_url);
		((OPH_CDO_operator_handle *) handle->operator_handle)->session_url = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->session_code) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->session_code);
		((OPH_CDO_operator_handle *) handle->operator_handle)->session_code = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id);
		((OPH_CDO_operator_handle *) handle->operator_handle)->workflow_id = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id);
		((OPH_CDO_operator_handle *) handle->operator_handle)->marker_id = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_CDO_operator_handle *) handle->operator_handle)->user);
		((OPH_CDO_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CDO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_CDO_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
