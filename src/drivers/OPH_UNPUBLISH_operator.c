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

#include "drivers/OPH_UNPUBLISH_operator.h"

#include <errmsg.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

#include "oph_task_parser_library.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"
#include "oph_json_library.h"

#include <errno.h>
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

	if (!(handle->operator_handle = (OPH_UNPUBLISH_operator_handle *) calloc(1, sizeof(OPH_UNPUBLISH_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path = NULL;
	((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid = NULL;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	char *datacube_name;
	int id_container = 0;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys, &((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	char *tmp_username = NULL;
	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(tmp_username = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_OPHIDIADB_CONFIGURATION_FILE);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_UNPUBLISH_OPHIDIADB_CONNECTION_ERROR);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	int id_datacube = 0;

	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	char *uri = NULL;
	int folder_id = 0;
	int permission = 0;
	if (oph_pid_parse_pid(value, &id_container, &id_datacube, &uri)) {
		if (uri)
			free(uri);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_UNPUBLISH_PID_ERROR, datacube_name);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_container, id_datacube, &exists)) || !exists) {
		free(uri);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_UNPUBLISH_NO_INPUT_DATACUBE, datacube_name);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if ((oph_odb_fs_retrive_container_folder_id(oDB, id_container, 1, &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_UNPUBLISH_DATACUBE_FOLDER_ERROR, datacube_name);
		free(uri);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", tmp_username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_UNPUBLISH_DATACUBE_PERMISSION_ERROR, tmp_username);
		free(uri);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	free(tmp_username);
	free(uri);
	((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container = id_container;

	char session_code[OPH_COMMON_BUFFER_LEN];
	oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);
	int n = snprintf(NULL, 0, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
			 ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, id_datacube) + 1;
	if (n >= OPH_TP_TASKLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_STRING_BUFFER_OVERFLOW, "path",
			OPH_FRAMEWORK_HTML_FILES_PATH);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path = (char *) malloc(n * sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_MEMORY_ERROR_INPUT, "datacube path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	snprintf(((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path, n, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
		 ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, id_datacube);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int res;
	char *datacube_path = ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path;
	struct stat st;

	//Check if datacube folder exists and delete it
	if (stat(datacube_path, &st)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Cuboid hasn't been published %s\n", datacube_path);
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_DATACUBE_NOT_PUBLISHED, datacube_path);
	} else {
		if ((res = nftw(datacube_path, oph_dir_unlink_path, 2, (FTW_DEPTH | FTW_PHYS)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting files. ERRNO: %d\n", (res == -1 ? errno : res));
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_DELETING_FILES_ERROR,
				(res == -1 ? errno : res));
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_UNPUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path) {
		free((char *) ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path);
		((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->datacube_path = NULL;
	}
	if (((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_UNPUBLISH_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_UNPUBLISH_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
