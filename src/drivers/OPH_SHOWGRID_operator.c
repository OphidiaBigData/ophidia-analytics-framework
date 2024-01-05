/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

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

#include "drivers/OPH_SHOWGRID_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_pid_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
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

	if (!(handle->operator_handle = (OPH_SHOWGRID_operator_handle *) calloc(1, sizeof(OPH_SHOWGRID_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names = NULL;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names_num = 0;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index = 0;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_OPHIDIADB_CONNECTION_ERROR);
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
	if (oph_tp_parse_multiple_value_param(value, &((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, &((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int folder_id = 0;
	char *container_name = NULL;
	int permission = 0;
	int container_exists = 0;

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "username");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "cwd");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	container_name = value;

	if ((oph_odb_fs_path_parsing("", ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd, &folder_id, NULL, oDB))) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_CWD_ERROR, container_name, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on container
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_CONTAINER_PERMISSION_ERROR, container_name,
			((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check if container exists
	if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!container_exists) {
		//If it doesn't exists then return an error
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Else retreive container ID
	if ((oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_INDEX);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SHOW_INDEX);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) != 0) {
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index = 0;
	} else {
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		char **dim_names;
		int number_of_dimensions_names = 0;
		if (oph_tp_parse_multiple_value_param(value, &dim_names, &number_of_dimensions_names)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names = dim_names;
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names_num = number_of_dimensions_names;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->oDB;
	char *grid_name = ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name;
	char **dimension_names = ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names;
	int id_container = ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container;

	oph_odb_dimension *dims = NULL;
	oph_odb_dimension_instance *dim_insts = NULL;
	int number_of_dimensions = 0, is_objkey_printable;

	if (!grid_name) {

		//Show list of grids
		printf("+--------------------------------+\n");
		printf("| %-30s |\n", "GRID NAME");
		printf("+--------------------------------+\n");

		is_objkey_printable =
		    oph_json_is_objkey_printable(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_SHOWGRID_DIMINFO);
		if (is_objkey_printable) {
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 1, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "keys");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonkeys[jjj] = strdup("GRID NAME");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtypes");
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_LIST, "Grid List", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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

		MYSQL_RES *info_list = NULL;
		int num_rows = 0;
		//retrieve information list
		if (oph_odb_dim_find_container_grid_list(oDB, id_container, &info_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive grid list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_READ_LIST_INFO_ERROR);
			if (info_list)
				mysql_free_result(info_list);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(info_list))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_NO_ROWS_FOUND);
			mysql_free_result(info_list);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}

		if (mysql_field_count(oDB->conn) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MISSING_FIELDS);
			mysql_free_result(info_list);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		MYSQL_ROW row;

		//For each ROW
		while ((row = mysql_fetch_row(info_list))) {
			printf("| %-30s |\n", (row[0] ? row[0] : "-"));

			if (is_objkey_printable) {
				// Data
				int num_fields = 1, iii, jjj = 0;
				char **jsonvalues = NULL;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "values");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				for (jjj = 0; jjj < num_fields; jjj++) {
					jsonvalues[jjj] = strdup(row[0] ? row[0] : "");
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_LIST, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}

		}
		mysql_free_result(info_list);

		printf("+--------------------------------+\n");

		printf(OPH_SHOWGRID_HELP_MESSAGE);

		if (oph_json_is_objkey_printable
		    (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_SHOWGRID_TIP)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_TIP, "Help Message", OPH_SHOWGRID_HELP_MESSAGE)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

	} else {
		if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container(oDB, grid_name, id_container, &dims, &dim_insts, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIMENSION_READ_ERROR);
			if (dims) {
				free(dims);
			}
			if (dim_insts) {
				free(dim_insts);
			}
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		oph_odb_hierarchy hier;

		char filename[2 * OPH_TP_BUFLEN];
		char *concept_level = 0;

		printf("+----------------------------------------------------------------------------------------------------+\n");
		printf("| %-39s%-s%-38s |\n", "", "DIMENSION INFORMATION", "");
		printf("+----------------------+----------------+----------------+----------------------+--------------------+\n");
		printf("| %-20s | %-14s | %-14s | %-20s | %-18s |\n", "NAME", "TYPE", "SIZE", "HIERACHY", "CONCEPT LEVEL");
		printf("+----------------------+----------------+----------------+----------------------+--------------------+\n");

		is_objkey_printable =
		    oph_json_is_objkey_printable(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_SHOWGRID_DIMINFO);
		if (is_objkey_printable) {
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 5, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "keys");
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonkeys[jjj] = strdup("NAME");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("TYPE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("SIZE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("HIERARCHY");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("CONCEPT LEVEL");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_DIMINFO, "Dimension Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				if (dims)
					free(dims);
				if (dim_insts)
					free(dim_insts);
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
		//Read dimension list
		int l;
		for (l = 0; l < number_of_dimensions; l++) {
			if (oph_odb_dim_retrieve_hierarchy(oDB, dims[l].id_hierarchy, &hier)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve hierarchy info.\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_HIERARCHY_INFO_ERROR);
				continue;
			}

			if (dim_insts[l].concept_level != OPH_COMMON_CONCEPT_LEVEL_UNKNOWN) {
				snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
				if (oph_hier_get_concept_level_long(filename, dim_insts[l].concept_level, &concept_level)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve long format of concept level\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_ADDITIONAL_INFO_ERROR);
					if (concept_level)
						free(concept_level);
					concept_level = (char *) malloc(2 * sizeof(char));
					concept_level[0] = dim_insts[l].concept_level;
					concept_level[1] = 0;
				}
			} else {
				concept_level = (char *) malloc(2 * sizeof(char));
				concept_level[0] = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
				concept_level[1] = 0;
			}

			if (dim_insts[l].size)
				printf("| %-20s | %-14s | %-14d | %-20s | %-18s |\n", (dims[l].dimension_name ? dims[l].dimension_name : "-"), (dims[l].dimension_type ? dims[l].dimension_type : "-"),
				       dim_insts[l].size, (hier.hierarchy_name ? hier.hierarchy_name : "-"), concept_level ? concept_level : "-");
			else
				printf("| %-20s | %-14s | %-14s | %-20s | %-18s |\n", (dims[l].dimension_name ? dims[l].dimension_name : "-"), (dims[l].dimension_type ? dims[l].dimension_type : "-"),
				       OPH_COMMON_FULL_REDUCED_DIM, (hier.hierarchy_name ? hier.hierarchy_name : "-"), concept_level ? concept_level : "-");

			if (is_objkey_printable) {
				char jsontmp[OPH_COMMON_BUFFER_LEN];
				// Data
				int num_fields = 5, iii, jjj = 0;
				char **jsonvalues = NULL;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "values");
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				jsonvalues[jjj] = strdup(dims[l].dimension_name ? dims[l].dimension_name : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(dims[l].dimension_type ? dims[l].dimension_type : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				if (dim_insts[l].size) {
					snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", dim_insts[l].size);
					jsonvalues[jjj] = strdup(jsontmp);
				} else
					jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(hier.hierarchy_name ? hier.hierarchy_name : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(concept_level ? concept_level : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_DIMINFO, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					if (dims)
						free(dims);
					if (dim_insts)
						free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}

			if (concept_level)
				free(concept_level);
		}
		printf("+----------------------+----------------+----------------+----------------------+--------------------+\n");


		if (number_of_dimensions) {
			int i = 0;

			oph_odb_db_instance db_;
			oph_odb_db_instance *db_dimension = &db_;
			if (oph_dim_load_dim_dbinstance(db_dimension)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container);
			int exist_flag = 0;

			if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if (!exist_flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if (!exist_flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_insts);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			char *dim_row = NULL;
			char *dim_row_index = NULL;

			printf("+----------------------------------------------------------------------------------------------------+\n");

			int len = 0;
			int first_row = 1;
			int j;
			char *tmp_row = NULL, *tmp_row2 = NULL;
			char tmp_row_buffer[OPH_SHOWGRID_TABLE_SIZE + 1];
			int buff_size = 0;

			double *dim_d = NULL;
			float *dim_f = NULL;
			int *dim_i = NULL;
			long long *dim_l = NULL;
			short *dim_s = NULL;
			char *dim_b = NULL;
			long long index = 0;

			char tmp_type[OPH_COMMON_BUFFER_LEN];
			*tmp_type = 0;
			char tmp_value[OPH_COMMON_BUFFER_LEN];
			*tmp_value = 0;
			char tmp_index[OPH_COMMON_BUFFER_LEN];
			*tmp_index = 0;

			is_objkey_printable =
			    oph_json_is_objkey_printable(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num,
							 OPH_JSON_OBJKEY_SHOWGRID_DIMVALUES);

			for (i = 0; i < number_of_dimensions; i++) {

				if (dimension_names) {
					for (j = 0; j < ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names_num; j++) {
						if (strncmp(dims[i].dimension_name, dimension_names[j], OPH_ODB_DIM_DIMENSION_SIZE) == 0)
							break;
					}
					if (j == ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names_num)
						continue;
				}
				if (dim_insts[i].size == 0) {
					continue;
				}

				dim_row_index = NULL;
				dim_row = NULL;

				if (dim_insts[i].fk_id_dimension_label) {
					if (oph_dim_read_dimension_data(db_dimension, label_dimension_table_name, dim_insts[i].fk_id_dimension_label, MYSQL_DIMENSION, 0, &dim_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dims);
						free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
					if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_insts[i].fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_row_index)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dims);
						free(dim_insts);
						if (!dim_row)
							free(dim_row);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
				} else {
					strncpy(dims[i].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					dims[i].dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
					if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_insts[i].fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dims);
						free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}

				}
				if (!dim_row) {
					if (dim_insts[i].size) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension data are corrupted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dims);
						free(dim_insts);
						if (dim_row_index)
							free(dim_row_index);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					} else
						continue;
				}

				if (!first_row)
					printf("+----------------------------------------------------------------------------------------------------+\n");

				printf("| %-*s%-s [index] %-*s |\n", OPH_SHOWGRID_TABLE_SIZE / 2 - (int) strlen(dims[i].dimension_name) / 2 - 4, "", dims[i].dimension_name,
				       OPH_SHOWGRID_TABLE_SIZE - (OPH_SHOWGRID_TABLE_SIZE / 2 - (int) strlen(dims[i].dimension_name) / 2 - 4) - (int) strlen(dims[i].dimension_name) - 9, "");
				printf("+----------------------------------------------------------------------------------------------------+\n");

				if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_INT_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_INT);
				} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_LONG_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_LONG);
				} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_SHORT_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_SHORT);
				} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_LONG_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_LONG);
				} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_FLOAT_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_FLOAT);
				} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					buff_size = OPH_COMMON_MAX_DOUBLE_LENGHT + 2;
					strcpy(tmp_type, OPH_JSON_DOUBLE);
				} else {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Type not supported.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_TYPE_NOT_SUPPORTED, dims[i].dimension_type);
					free(dim_row);
					if (dim_row_index)
						free(dim_row_index);
					continue;
				}

				if (is_objkey_printable) {
					// Header
					char **jsonkeys = NULL;
					char **fieldtypes = NULL;
					int num_fields = 1 + (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index ? 1 : 0), iii, jjj = 0;
					jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
					if (!jsonkeys) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "keys");
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						if (dims)
							free(dims);
						if (dim_insts)
							free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jsonkeys[jjj] = strdup("VALUE");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						if (dims)
							free(dims);
						if (dim_insts)
							free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index) {
						jjj++;
						jsonkeys[jjj] = strdup("INDEX");
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							free(dim_row);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							if (dim_row_index)
								free(dim_row_index);
							if (dims)
								free(dims);
							if (dim_insts)
								free(dim_insts);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
					}
					jjj = 0;
					fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
					if (!fieldtypes) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtypes");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						if (dims)
							free(dims);
						if (dim_insts)
							free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					fieldtypes[jjj] = strdup(tmp_type);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						if (dims)
							free(dims);
						if (dim_insts)
							free(dim_insts);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index) {
						jjj++;
						fieldtypes[jjj] = strdup(OPH_JSON_INT);	// or LONG
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "fieldtype");
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
							free(dim_row);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							if (dim_row_index)
								free(dim_row_index);
							if (dims)
								free(dims);
							if (dim_insts)
								free(dim_insts);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
					}
					if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_DIMVALUES, dims[i].dimension_name, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						if (dims)
							free(dims);
						if (dim_insts)
							free(dim_insts);
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

				if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index) {
					buff_size += 3 + OPH_COMMON_MAX_INT_LENGHT;
				}
				tmp_row = (char *) malloc(dim_insts[i].size * buff_size * sizeof(char));
				if (!tmp_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocationg memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "dimension row");
					free(dim_row);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					if (dim_row_index)
						free(dim_row_index);
					free(dims);
					free(dim_insts);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}

				len = 0;
				index = 0;
				for (j = 0; j < dim_insts[i].size; j++) {
					if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_i = (int *) (dim_row + index * sizeof(int));
						} else {
							dim_i = (int *) (dim_row + j * sizeof(int));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-d [%d]", *dim_i, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-d", *dim_i);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
					} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_l = (long long *) (dim_row + index * sizeof(long long));
						} else {
							dim_l = (long long *) (dim_row + j * sizeof(long long));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-lld [%d]", *dim_l, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-lld", *dim_l);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
					} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_s = (short *) (dim_row + index * sizeof(short));
						} else {
							dim_s = (short *) (dim_row + j * sizeof(short));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-d [%d]", *dim_s, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-d", *dim_s);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
					} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_b = (char *) (dim_row + index * sizeof(char));
						} else {
							dim_b = (char *) (dim_row + j * sizeof(char));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-d [%d]", *dim_b, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-d", *dim_b);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
					} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_f = (float *) (dim_row + index * sizeof(float));
						} else {
							dim_f = (float *) (dim_row + j * sizeof(float));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-f [%d]", *dim_f, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-f", *dim_f);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
					} else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
						if (dim_row_index) {
							index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
							dim_d = (double *) (dim_row + index * sizeof(double));
						} else {
							dim_d = (double *) (dim_row + j * sizeof(double));
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index)
							len += snprintf(tmp_row + len, buff_size, "%-f [%d]", *dim_d, j + 1);
						else
							len += snprintf(tmp_row + len, buff_size, "%-f", *dim_d);
						if (is_objkey_printable)
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
					} else {
						pmesg(LOG_WARNING, __FILE__, __LINE__, "Type not supported.\n");
						logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_SHOWGRID_TYPE_NOT_SUPPORTED, dims[i].dimension_type);
						free(dim_row);
						if (dim_row_index)
							free(dim_row_index);
						continue;
					}

					if (j != (dim_insts[i].size - 1))
						len += snprintf(tmp_row + len, 3, ",  ");
					len--;

					if (is_objkey_printable) {
						// Data
						int num_fields = 1 + (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index ? 1 : 0), iii, jjj = 0;
						char **jsonvalues = NULL;
						jsonvalues = (char **) calloc(num_fields, sizeof(char *));
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "values");
							free(tmp_row);
							free(dim_row);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							if (dim_row_index)
								free(dim_row_index);
							if (dims)
								free(dims);
							if (dim_insts)
								free(dim_insts);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj = 0;
						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							free(tmp_row);
							free(dim_row);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							if (dim_row_index)
								free(dim_row_index);
							if (dims)
								free(dims);
							if (dim_insts)
								free(dim_insts);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->show_index) {
							jjj++;
							snprintf(tmp_index, OPH_COMMON_BUFFER_LEN, "%d", j + 1);
							jsonvalues[jjj] = strdup(tmp_index);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SHOWGRID_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								free(tmp_row);
								free(dim_row);
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								if (dim_row_index)
									free(dim_row_index);
								if (dims)
									free(dims);
								if (dim_insts)
									free(dim_insts);
								return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							}
						}
						jjj++;
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_SHOWGRID_DIMVALUES, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							free(tmp_row);
							free(dim_row);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							if (dim_row_index)
								free(dim_row_index);
							if (dims)
								free(dims);
							if (dim_insts)
								free(dim_insts);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
					}


				}

				len = strlen(tmp_row);
				tmp_row2 = tmp_row;
				for (j = 0; j < len; j += OPH_SHOWGRID_TABLE_SIZE) {
					strncpy(tmp_row_buffer, tmp_row2, OPH_SHOWGRID_TABLE_SIZE);
					tmp_row_buffer[OPH_SHOWGRID_TABLE_SIZE] = 0;
					tmp_row2 += OPH_SHOWGRID_TABLE_SIZE;
					printf("| %-*s |\n", OPH_SHOWGRID_TABLE_SIZE, tmp_row_buffer);
				}

				if (first_row)
					first_row = 0;
				free(tmp_row);
				free(dim_row);
				if (dim_row_index)
					free(dim_row_index);
				tmp_row = NULL;

			}
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			printf("+----------------------------------------------------------------------------------------------------+\n\n");

		}

		free(dims);
		free(dim_insts);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SHOWGRID_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names) {
		oph_tp_free_multiple_value_param_list(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names,
						      ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names_num);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->dimension_names = NULL;
	}
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_SHOWGRID_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_SHOWGRID_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
