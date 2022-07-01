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

#include "drivers/OPH_CONTAINERSCHEMA_operator.h"
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

	if (!(handle->operator_handle = (OPH_CONTAINERSCHEMA_operator_handle *) calloc(1, sizeof(OPH_CONTAINERSCHEMA_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->oDB;
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
	    (value, &((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "container output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "username");
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_OPHIDIADB_CONFIGURATION_FILE, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_OPHIDIADB_CONNECTION_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->oDB;
	int id_container = ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->id_input_container;
	char *container_name = ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input;
	char *cwd = ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd;
	char *user = ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user;

	//Check if user can operate on container and if container exists
	int permission = 0;
	int folder_id = 0;
	//Check if input path exists
	if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_CWD_ERROR, container_name, cwd);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONTAINERSCHEMA_DATACUBE_PERMISSION_ERROR, container_name, user);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->id_input_container = id_container;

	if (oph_json_is_objkey_printable
	    (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
	     OPH_JSON_OBJKEY_CONTAINERSCHEMA_INFO)) {

		char *description = NULL, *vocabulary = NULL;
		if (oph_odb_fs_retrieve_container_from_container_name(oDB, folder_id, container_name, &id_container, &description, &vocabulary)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_NO_INPUT_CONTAINER, container_name);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		// Header
		char **jsonkeys = NULL;
		char **fieldtypes = NULL;
		int num_fields = 3, iii, jjj = 0;
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "keys");
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jsonkeys[jjj] = strdup("NAME");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("DESCRIPTION");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("VOCABULARY");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtypes");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CONTAINERSCHEMA_INFO, "Container Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, "ADD GRID error\n");
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
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
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

		// Data
		char **jsonvalues = NULL;
		jsonvalues = (char **) calloc(num_fields, sizeof(char *));
		if (!jsonvalues) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "values");
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj = 0;
		jsonvalues[jjj] = strdup(container_name ? container_name : "");
		if (!jsonvalues[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
			for (iii = 0; iii < jjj; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonvalues[jjj] = strdup(description ? description : "");
		if (!jsonvalues[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
			for (iii = 0; iii < jjj; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			if (description)
				free(description);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (description)
			free(description);
		jjj++;
		jsonvalues[jjj] = strdup(vocabulary ? vocabulary : "");
		if (!jsonvalues[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
			for (iii = 0; iii < jjj; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			if (vocabulary)
				free(vocabulary);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (vocabulary)
			free(vocabulary);
		if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CONTAINERSCHEMA_INFO, jsonvalues)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, "ADD GRID ROW error\n");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		for (iii = 0; iii < num_fields; iii++)
			if (jsonvalues[iii])
				free(jsonvalues[iii]);
		if (jsonvalues)
			free(jsonvalues);
	}

	if (oph_json_is_objkey_printable
	    (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
	     OPH_JSON_OBJKEY_CONTAINERSCHEMA_DIMINFO)) {

		int dim_num;
		oph_odb_dimension *dims = NULL;
		if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container, &dims, &dim_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_DIMENSION_READ_ERROR);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		// Header
		char **jsonkeys = NULL;
		char **fieldtypes = NULL;
		int num_fields = 9, iii, jjj = 0, i, j, n;
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "keys");
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jsonkeys[jjj] = strdup("NAME");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("TYPE");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("HIERARCHY");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("BASE TIME");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("CALENDAR");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("UNITS");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("LEAP MONTH");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("LEAP YEAR");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		jsonkeys[jjj] = strdup("MONTH LENGTHS");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtypes");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_INT);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_INT);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj++;
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "fieldtype");
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
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CONTAINERSCHEMA_DIMINFO, "Dimension Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, "ADD GRID error\n");
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

		// Data
		char jsontmp[OPH_COMMON_BUFFER_LEN];
		char **jsonvalues = NULL;
		oph_odb_hierarchy hierarchy;
		jsonvalues = (char **) calloc(num_fields, sizeof(char *));
		if (!jsonvalues) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "values");
			if (dims)
				free(dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < dim_num; ++i) {

			if (oph_odb_dim_retrieve_hierarchy(oDB, dims[i].id_hierarchy, &hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			jjj = 0;
			jsonvalues[jjj] = strdup(dims[i].dimension_name ? dims[i].dimension_name : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(dims[i].dimension_type ? dims[i].dimension_type : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(hierarchy.hierarchy_name ? hierarchy.hierarchy_name : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(dims[i].base_time ? dims[i].base_time : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(dims[i].calendar ? dims[i].calendar : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(dims[i].units ? dims[i].units : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", dims[i].leap_month);
			jsonvalues[jjj] = strdup(dims[i].calendar && strlen(dims[i].calendar) ? jsontmp : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", dims[i].leap_year);
			jsonvalues[jjj] = strdup(dims[i].calendar && strlen(dims[i].calendar) ? jsontmp : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			for (j = n = 0; j < OPH_ODB_DIM_MONTH_NUMBER; ++j)
				n += snprintf(jsontmp + n, OPH_COMMON_BUFFER_LEN, "%s%d", j ? "," : "", dims[i].month_lengths[j]);
			jsonvalues[jjj] = strdup(dims[i].calendar && strlen(dims[i].calendar) ? jsontmp : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CONTAINERSCHEMA_MEMORY_ERROR_INPUT, container_name, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CONTAINERSCHEMA_DIMINFO, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				if (dims)
					free(dims);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
		}
		if (jsonvalues)
			free(jsonvalues);
		if (dims)
			free(dims);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONTAINERSCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONTAINERSCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input);
		((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->container_input = NULL;
	}

	if (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd);
		((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user);
		((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_CONTAINERSCHEMA_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
