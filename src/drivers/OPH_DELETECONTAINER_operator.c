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

#include "drivers/OPH_DELETECONTAINER_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "oph_analytics_operator_library.h"

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

	if (!(handle->operator_handle = (OPH_DELETECONTAINER_operator_handle *) calloc(1, sizeof(OPH_DELETECONTAINER_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->hidden = 0;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->delete_type = 0;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//3 - Fill struct with the correct data
	char *container_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys, &((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DELETE_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DELETE_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DELETE_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_DELETE_PHYSIC_TYPE)) {
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->delete_type = OPH_DELETE_PHYSIC_CODE;
	} else {
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->delete_type = OPH_DELETE_LOGIC_CODE;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIDDEN);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIDDEN);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HIDDEN);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_NO_VALUE)) {
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->hidden = 0;
	} else {
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->hidden = 1;
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_OPHIDIADB_CONFIGURATION_FILE, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_OPHIDIADB_CONNECTION_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	int id_container = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container;
	int hidden = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->hidden;
	int delete_type = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->delete_type;
	char *container_name = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input;
	char *cwd = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd;
	char *user = ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user;

	if (delete_type == OPH_DELETE_LOGIC_CODE)
		hidden = 0;

	//Check if user can operate on container and if container exists
	int permission = 0;
	int folder_id = 0;
	int container_exists = 0;
	//Check if input path exists
	if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_CWD_ERROR, container_name, cwd);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETECONTAINER_DATACUBE_PERMISSION_ERROR, container_name, user);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, hidden, &id_container)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input %s container\n", (hidden ? "hidden" : "visible"));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETECONTAINER_NO_INPUT_CONTAINER, (hidden ? "hidden" : "visible"), container_name);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container = id_container;

	if (delete_type == OPH_DELETE_PHYSIC_CODE) {
		if (oph_odb_fs_check_if_container_empty(oDB, id_container)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Input container isn't empty\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETECONTAINER_CONTAINER_NOT_EMPTY, container_name);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Remove also grid related to container dimensions
		if (oph_odb_dim_delete_from_grid_table(oDB, id_container)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting grid related to container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_GRID_DELETE_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Delete container and related dimensions/ dimension instances
		if (oph_odb_fs_delete_from_container_table(oDB, id_container)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container,
				OPH_LOG_OPH_DELETECONTAINER_CONTAINER_DELETE_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container);

		if (oph_dim_delete_table(db, index_dimension_table_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container,
				OPH_LOG_OPH_DELETECONTAINER_DIM_TABLE_DELETE_ERROR);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_delete_table(db, label_dimension_table_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container,
				OPH_LOG_OPH_DELETECONTAINER_DIM_TABLE_DELETE_ERROR);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
	} else {
		//Check if hidden container exists in folder
		if (oph_odb_fs_is_hidden_container(folder_id, container_name, oDB, &container_exists)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETECONTAINER_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (container_exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Hidden container '%s' already exists in this folder\n", container_name);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETECONTAINER_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//If container exists then DELETE
		if (oph_odb_fs_set_container_hidden_status(id_container, 1, oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete  logically  container %s\n", container_name);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETECONTAINER_UPDATE_CONTAINER_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input);
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->container_input = NULL;
	}

	if (((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd);
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user);
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_DELETECONTAINER_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
