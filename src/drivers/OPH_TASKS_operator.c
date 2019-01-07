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

#include "drivers/OPH_TASKS_operator.h"

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
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

	if (!(handle->operator_handle = (OPH_TASKS_operator_handle *) calloc(1, sizeof(OPH_TASKS_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->operator = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->path = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_TASKS_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//3 - Fill struct with the correct data
	char *datacube_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys, &((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_NAME_FILTER);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (strcmp(datacube_name, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name = (char *) strndup(datacube_name, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "datacube name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OPERATOR_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OPERATOR_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OPERATOR_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->operator =(char *)strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "operator");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_CONTAINER_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "container name");

			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_PATH);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_FRAMEWORK_FS_DEFAULT_PATH) != 0) {
		if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->path = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_TASKS_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "NO-CONTAINER", "username");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_task_recursive_task_search(ophidiadb * oDB, int folder_id, int datacube_id, char *operator, char *container, char *tmp_uri, oph_json * oper_json, int is_objkey_printable)
{
	if (!oDB || !folder_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	MYSQL_RES *task_list = NULL;
	int num_rows = 0;

	char output_cube[OPH_PID_SIZE] = { '\0' };
	char *pid = NULL;
	char *pid2 = NULL;
	MYSQL_RES *tmp_task_list = NULL;

	//If container name is not set, then search recursively
	if (!container) {
		//Find all child folders
		if (oph_odb_fs_find_fs_objects(oDB, 0, folder_id, NULL, &tmp_task_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_READ_LIST_INFO_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(tmp_task_list))) {
			mysql_free_result(tmp_task_list);
			tmp_task_list = NULL;
		}
	}
	//retrieve TASK information
	if (oph_odb_cube_find_task_list(oDB, folder_id, datacube_id, operator, container, &task_list)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive task list\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_READ_TASK_LIST);
		if (tmp_task_list)
			mysql_free_result(tmp_task_list);
		mysql_free_result(task_list);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if ((num_rows = mysql_num_rows(task_list))) {
		MYSQL_ROW row;
		//For each ROW
		while ((row = mysql_fetch_row(task_list))) {
			if (tmp_uri && row[1] && row[2]) {
				if (oph_pid_create_pid(tmp_uri, (int) strtol(row[1], NULL, 10), (int) strtol(row[2], NULL, 10), &pid)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to create PID string\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_PID_CREATE_ERROR);
					if (pid)
						free(pid);
					pid = NULL;
				}
			}
			if (tmp_uri && row[3] && row[4]) {
				if (oph_pid_create_pid(tmp_uri, (int) strtol(row[3], NULL, 10), (int) strtol(row[4], NULL, 10), &pid2)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to create PID string\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_PID_CREATE_ERROR);
					if (pid2)
						free(pid2);
					pid2 = NULL;
				}
			}
			//Empty set
			if (!(num_rows = mysql_num_rows(task_list))) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NO_ROWS_FOUND);
				if (tmp_task_list)
					mysql_free_result(tmp_task_list);
				mysql_free_result(task_list);
				if (pid)
					free(pid);
				if (pid2)
					free(pid2);
				return OPH_ANALYTICS_OPERATOR_SUCCESS;
			}

			if (strcmp(output_cube, row[4]) == 0)
				printf("|   | %-20s | %-40s | %-40s |\n", (row[0] ? row[0] : "-"), (pid ? pid : "-"), (pid2 ? pid2 : "-"));
			else {
				printf("| * | %-20s | %-40s | %-40s |\n", (row[0] ? row[0] : "-"), (pid ? pid : "-"), (pid2 ? pid2 : "-"));
				strncpy(output_cube, row[4], OPH_PID_SIZE);
			}

			// ADD ROW TO JSON GRID
			if (is_objkey_printable) {
				char **jsonvalues = NULL;
				int num_fields = 3, iii, jjj;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "values");
					if (tmp_task_list)
						mysql_free_result(tmp_task_list);
					mysql_free_result(task_list);
					if (pid)
						free(pid);
					pid = NULL;
					if (pid2)
						free(pid2);
					pid2 = NULL;
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				jsonvalues[jjj] = strdup(row[0] ? row[0] : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (tmp_task_list)
						mysql_free_result(tmp_task_list);
					mysql_free_result(task_list);
					if (pid)
						free(pid);
					pid = NULL;
					if (pid2)
						free(pid2);
					pid2 = NULL;
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(pid ? pid : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (tmp_task_list)
						mysql_free_result(tmp_task_list);
					mysql_free_result(task_list);
					if (pid)
						free(pid);
					pid = NULL;
					if (pid2)
						free(pid2);
					pid2 = NULL;
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(pid2 ? pid2 : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (tmp_task_list)
						mysql_free_result(tmp_task_list);
					mysql_free_result(task_list);
					if (pid)
						free(pid);
					pid = NULL;
					if (pid2)
						free(pid2);
					pid2 = NULL;
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_TASKS, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (tmp_task_list)
						mysql_free_result(tmp_task_list);
					mysql_free_result(task_list);
					if (pid)
						free(pid);
					pid = NULL;
					if (pid2)
						free(pid2);
					pid2 = NULL;
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}

			if (pid)
				free(pid);
			pid = NULL;
			if (pid2)
				free(pid2);
			pid2 = NULL;
		}
	}
	mysql_free_result(task_list);

	if (tmp_task_list) {
		MYSQL_ROW tmp_row;
		//For each ROW
		while ((tmp_row = mysql_fetch_row(tmp_task_list))) {
			if (_oph_task_recursive_task_search(oDB, (int) strtol(tmp_row[1], NULL, 10), datacube_id, operator, container, tmp_uri, oper_json, is_objkey_printable))
				break;
		}
		mysql_free_result(tmp_task_list);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_TASKS_operator_handle *) handle->operator_handle)->oDB;
	char *datacube_name = ((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name;
	char *operator =((OPH_TASKS_operator_handle *) handle->operator_handle)->operator;
	char *user = ((OPH_TASKS_operator_handle *) handle->operator_handle)->user;
	char *container_name = ((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name;
	char *cwd = ((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd;
	char *path = ((OPH_TASKS_operator_handle *) handle->operator_handle)->path;


	int permission = 0;
	int folder_id = 0;

	if (path || cwd) {
		//Check if user can operate on container and if container exists
		if (oph_odb_fs_path_parsing((path ? path : ""), cwd, &folder_id, NULL, oDB)) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", path);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_CWD_ERROR, path);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_DATACUBE_PERMISSION_ERROR, user);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

	} else {
		if (oph_odb_fs_get_session_home_id(((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid, oDB, &folder_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get session home id\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_USER_HOME_ERROR, user);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}


	int id_container = 0, id_datacube = 0;
	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	if (datacube_name) {
		char *uri = NULL;
		if (oph_pid_parse_pid(datacube_name, &id_container, &id_datacube, &uri)) {
			if (uri)
				free(uri);
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_TASKS_PID_ERROR, datacube_name);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_container, id_datacube, &exists)) || !exists) {
			free(uri);
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unknown input datacube\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_DATACUBE_READ_ERROR, datacube_name);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}
		free(uri);
	}

	printf("+---+----------------------+------------------------------------------+------------------------------------------+\n");
	printf("|   | %-20s | %-40s | %-40s |\n", "OPERATOR", "INPUT DATACUBE", "OUTPUT DATACUBE");
	printf("+---+----------------------+------------------------------------------+------------------------------------------+\n");

// SET TABLE COLUMN FOR JSON
	int is_objkey_printable =
	    oph_json_is_objkey_printable(((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_TASKS);
	if (is_objkey_printable) {
		char **jsonkeys = NULL;
		char **fieldtypes = NULL;
		int num_fields = 3, iii, jjj = 0;
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "keys");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jsonkeys[jjj] = strdup("OPERATOR");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("INPUT DATACUBE");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("OUTPUT DATACUBE");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "fieldtypes");
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "fieldtype");
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
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "fieldtype");
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
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_MEMORY_ERROR_INPUT, "fieldtype");
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
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_TASKS, "Task List", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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

	char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_PID_URI_ERROR);
	}
	//retrieve TASK information
	if (_oph_task_recursive_task_search(oDB, folder_id, id_datacube, operator, container_name, tmp_uri, handle->operator_json, is_objkey_printable)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive task list\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_READ_TASK_LIST);
		if (tmp_uri)
			free(tmp_uri);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if (tmp_uri)
		free(tmp_uri);
	printf("+---+----------------------+------------------------------------------+------------------------------------------+\n");

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_TASKS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_TASKS_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_TASKS_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->container_name = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->path) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->path);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->path = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->user);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->operator) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->operator);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->operator = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_TASKS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_TASKS_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
