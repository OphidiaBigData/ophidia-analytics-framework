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

#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "drivers/OPH_MAN_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_render_output_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#include "oph_json_library.h"

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_MAN_operator_handle *) calloc(1, sizeof(OPH_MAN_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_MAN_operator_handle *) handle->operator_handle)->function_name = NULL;
	((OPH_MAN_operator_handle *) handle->operator_handle)->function_version = NULL;
	((OPH_MAN_operator_handle *) handle->operator_handle)->function_type_code = 0;
	((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys_num = -1;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;


	//3 - Fill struct with the correct data
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys, &((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FUNCTION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FUNCTION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_FUNCTION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MAN_operator_handle *) handle->operator_handle)->function_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MEMORY_ERROR_INPUT, "function_name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}


	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FUNCTION_VERSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FUNCTION_VERSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_FUNCTION_VERSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_LATEST_VERSION) != 0) {
		if (!(((OPH_MAN_operator_handle *) handle->operator_handle)->function_version = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MEMORY_ERROR_INPUT, "function_version");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FUNCTION_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FUNCTION_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_FUNCTION_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncasecmp(value, OPH_TP_XML_OPERATOR_TYPE, OPH_TP_TASKLEN)) {
		((OPH_MAN_operator_handle *) handle->operator_handle)->function_type_code = OPH_TP_XML_OPERATOR_TYPE_CODE;
	} else if (!strncasecmp(value, OPH_TP_XML_PRIMITIVE_TYPE, OPH_TP_TASKLEN)) {
		((OPH_MAN_operator_handle *) handle->operator_handle)->function_type_code = OPH_TP_XML_PRIMITIVE_TYPE_CODE;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_BAD_PARAMETER, "function_type_code", value);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_INFO_START);

	char *function_name = ((OPH_MAN_operator_handle *) handle->operator_handle)->function_name;
	char *function_version = ((OPH_MAN_operator_handle *) handle->operator_handle)->function_version;
	char **objkeys = ((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys;
	int objkeys_num = ((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys_num;
	short int function_type_code = ((OPH_MAN_operator_handle *) handle->operator_handle)->function_type_code;

	char xml[OPH_TP_BUFLEN];
	char filename[2 * OPH_TP_BUFLEN];

	if (oph_tp_retrieve_function_xml_file((const char *) function_name, (const char *) function_version, function_type_code, &xml)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find xml\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_XML_NOT_FOUND, function_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
		snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_PRIMITIVE_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, xml);
	else
		snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_OPERATOR_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, xml);

	int res;
	res = renderXML(filename, function_type_code, handle->operator_json, objkeys, objkeys_num);	//render XML
	if (res == OPH_RENDER_OUTPUT_RENDER_ERROR) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error parsing XML\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_XML_PARSE_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else if (res == OPH_RENDER_OUTPUT_RENDER_INVALID_XML) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "XML not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_INVALID_XML);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else {
		logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_INFO_END);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MAN_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_MAN_operator_handle *) handle->operator_handle)->function_name) {
		free((char *) ((OPH_MAN_operator_handle *) handle->operator_handle)->function_name);
		((OPH_MAN_operator_handle *) handle->operator_handle)->function_name = NULL;
	}
	if (((OPH_MAN_operator_handle *) handle->operator_handle)->function_version) {
		free((char *) ((OPH_MAN_operator_handle *) handle->operator_handle)->function_version);
		((OPH_MAN_operator_handle *) handle->operator_handle)->function_version = NULL;
	}
	if (((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_MAN_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_MAN_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
