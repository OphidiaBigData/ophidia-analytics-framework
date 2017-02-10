/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_RESTORECONTAINER_operator.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

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

	if (!(handle->operator_handle = (OPH_RESTORECONTAINER_operator_handle *) calloc(1, sizeof(OPH_RESTORECONTAINER_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num = 0;
	((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	char *container_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys, &((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_OPHIDIADB_CONFIGURATION_FILE,
			((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_OPHIDIADB_CONNECTION_ERROR,
			((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *container_name = ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output;
	int id_container_out = 0, container_exists = 0;
	ophidiadb *oDB = &((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	char *cwd = ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd;
	char *user = ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user;

	int permission = 0;
	int folder_id = 0;
	//Check if input path exists
	if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_CWD_ERROR, container_name, cwd);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_DATACUBE_PERMISSION_ERROR, container_name, user);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}


	if ((oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, 1, &id_container_out))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Output container doesn't exist or it isn't hidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RESTORECONTAINER_OUTPUT_CONTAINER_EXIST_ERROR_NO_CONTAINER, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (id_container_out) {

		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->id_output_container = id_container_out;
		//Check if non-hidden container exists in folder
		if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RESTORECONTAINER_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (container_exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Visible container '%s' already exists in this folder\n", container_name);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RESTORECONTAINER_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//If container exists then restore
		if (oph_odb_fs_set_container_hidden_status(id_container_out, 0, oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RESTORECONTAINER_UPDATE_CONTAINER_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_RESTORECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_RESTORECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output) {
		free((char *) ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	}

	if (((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd);
		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user);
		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	}

	if (((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_RESTORECONTAINER_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
