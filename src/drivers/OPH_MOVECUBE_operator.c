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

#include "drivers/OPH_MOVECUBE_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
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

	if (!(handle->operator_handle = (OPH_MOVECUBE_operator_handle *) calloc(1, sizeof(OPH_MOVECUBE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output = NULL;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->userrole = OPH_ROLE_NONE;

	ophidiadb *oDB = &((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *value;

	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *datacube_in = value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_OPHIDIADB_CONNECTION_ERROR);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For error checking
	int id_datacube_in[3] = { 0, 0, 0 };

	//Check if datacube exists (by ID path and datacube)
	int exists = 0;
	int status = 0;
	char *uri = NULL;
	int folder_id = 0;
	int permission = 0;
	if (oph_pid_parse_pid(datacube_in, &id_datacube_in[1], &id_datacube_in[0], &uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_PID_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NO_INPUT_DATACUBE, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_DATACUBE_AVAILABILITY_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_DATACUBE_FOLDER_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_DATACUBE_PERMISSION_ERROR, username);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	}
	if (uri)
		free(uri);
	uri = NULL;

	id_datacube_in[2] = id_datacube_in[1];

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, "path output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, "input path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERROLE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERROLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MISSING_INPUT_PARAMETER, OPH_ARG_USERROLE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->userrole = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->oDB;
	char *sessionid = ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid;
	char *path = ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output;
	char *cwd = ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd;

	int target_folder_id = 0;
	char *abs_path = NULL;
	int permission = 0;

	if (!IS_OPH_ROLE_PRESENT(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->userrole, OPH_ROLE_WRITE_POS)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "At least '%s' permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (!strcasecmp(path, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {

		if (oph_odb_fs_get_session_home_id(sessionid, oDB, &target_folder_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_PATH_PARSING_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

	} else {

		if (oph_odb_fs_path_parsing(path, cwd, &target_folder_id, &abs_path, oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_PATH_PARSING_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if ((oph_odb_fs_check_folder_session(target_folder_id, sessionid, oDB, &permission)) || !permission) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path '%s' is not allowed\n", path);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_PATH_NOT_ALLOWED_ERROR, path);
			if (abs_path) {
				free(abs_path);
				abs_path = NULL;
			}
			return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
		}
		if (abs_path) {
			free(abs_path);
			abs_path = NULL;
		}
	}

	if (!target_folder_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path '%s' is not allowed\n", path);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_PATH_NOT_ALLOWED_ERROR, path);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (oph_odb_cube_set_folder(oDB, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_datacube, target_folder_id)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_MOVECUBE_MOVING_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECUBE_MOVING_ERROR);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MOVECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MOVECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->oDB);

	if (((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output) {
		free((char *) ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output);
		((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->path_output = NULL;
	}

	if (((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd);
		((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_MOVECUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_MOVECUBE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
