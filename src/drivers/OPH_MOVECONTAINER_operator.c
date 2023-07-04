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

#include "drivers/OPH_MOVECONTAINER_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

int env_set(HASHTBL *task_tbl, oph_operator_struct *handle)
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

	if (!(handle->operator_handle = (OPH_MOVECONTAINER_operator_handle *) calloc(1, sizeof(OPH_MOVECONTAINER_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//3 - Fill struct with the correct data
	char *container_input, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys, &((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char **containers = NULL;
	int containers_num = 0;
	container_input = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MISSING_INPUT_PARAMETER, container_input, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &containers, &containers_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "There must be exactly 2 container names\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_CONTAINERS_ERROR);
		oph_tp_free_multiple_value_param_list(containers, containers_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (containers_num != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "There must be exactly 2 container names\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_CONTAINERS_ERROR);
		oph_tp_free_multiple_value_param_list(containers, containers_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(containers[0], OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_input, "container input name");
		oph_tp_free_multiple_value_param_list(containers, containers_num);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output = (char *) strndup(containers[1], OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_input, "container output name");
		oph_tp_free_multiple_value_param_list(containers, containers_num);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	oph_tp_free_multiple_value_param_list(containers, containers_num);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MISSING_INPUT_PARAMETER, container_input, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_input, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MISSING_INPUT_PARAMETER, container_input, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_input, "username");
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_OPHIDIADB_CONFIGURATION_FILE, container_input);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_OPHIDIADB_CONNECTION_ERROR, container_input);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	int id_container_in = 0;
	char *value2 = strrchr(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input, '/'), *container_name = NULL;
	if (value2)
		id_container_in = (int) strtol(1 + value2, NULL, 10);
	if (id_container_in && !oph_odb_fs_retrieve_container_name_from_container(oDB, id_container_in, &container_name, NULL)) {
		free(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input = container_name;
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->id_input_container = id_container_in;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int oph_movecontainer_free_all(char *ap1, char *ap2, char *fp1, char *fp2, char *lt1, char *lt2)
{
	if (fp1) {
		free(fp1);
		fp1 = NULL;
	}
	if (lt1) {
		free(lt1);
		lt1 = NULL;
	}
	if (ap1) {
		free(ap1);
		ap1 = NULL;
	}
	if (fp2) {
		free(fp2);
		fp2 = NULL;
	}
	if (lt2) {
		free(lt2);
		lt2 = NULL;
	}
	if (ap2) {
		free(ap2);
		ap2 = NULL;
	}
	return 0;
}

int task_execute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	char *container_input = ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input;
	char *container_output = ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output;
	char *cwd = ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd;
	char *user = ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user;

	//Check if user can operate on container and if container exists
	int permission = 0;
	int answer = 0;
	int folder_id1 = 0, folder_id2 = 0;
	char *abs_path1 = NULL, *abs_path2 = NULL, *first_part = NULL, *first_part2 = NULL, *last_token = NULL, *last_token2 = NULL;
	int id_container = 0;

	//Check if input path exists
	if (oph_odb_fs_str_last_token(container_input, &first_part, &last_token)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Input container path isn't correct\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_PATH_PARSING_ERROR, container_input);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_odb_fs_path_parsing(first_part, cwd, &folder_id1, &abs_path1, oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_CWD_ERROR, container_input, cwd);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id1, ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MOVECONTAINER_DATACUBE_PERMISSION_ERROR, user, container_input);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id1, last_token, &id_container)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_NO_INPUT_CONTAINER, container_input);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->id_input_container = id_container;

	//Check output container
	//second path processing start
	permission = 0;
	if (oph_odb_fs_str_last_token(container_output, &first_part2, &last_token2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Output container path isn't correct\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_PATH_PARSING_ERROR, container_output);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_odb_fs_path_parsing(first_part2, cwd, &folder_id2, &abs_path2, oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_CWD_ERROR, container_output, cwd);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id2, ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_DATACUBE_PERMISSION_ERROR, user, container_output);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (!oph_odb_fs_is_allowed_name(last_token2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", last_token2);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_NAME_NOT_ALLOWED_ERROR, last_token2);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	if (oph_odb_fs_is_unique(folder_id2, last_token2, &((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->oDB, &answer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check uniqueness\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_UNIQUE_CHECK_ERROR, container_output);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (answer == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "There is already a folder or a visible container with the same name\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_NOT_UNIQUE_ERROR, container_output);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	if (oph_odb_fs_update_container_path_name(oDB, id_container, folder_id2, last_token2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update  container %s\n", last_token);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MOVECONTAINER_UPDATE_CONTAINER_ERROR, last_token);
		oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	oph_movecontainer_free_all(abs_path1, abs_path2, first_part, first_part2, last_token, last_token2);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MOVECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MOVECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_input = NULL;
	}
	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output) {
		free((char *) ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	}

	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	}

	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_MOVECONTAINER_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
