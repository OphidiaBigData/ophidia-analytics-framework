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

#include "drivers/OPH_PRIMITIVES_LIST_operator.h"
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"

#include "oph_json_library.h"

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_PRIMITIVES_LIST_operator_handle *) calloc(1, sizeof(OPH_PRIMITIVES_LIST_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->level = 0;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret = NULL;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type = NULL;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter = NULL;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->limit = 0;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->id_dbms = 0;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server = NULL;
	ophidiadb *oDB = &((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys_num = -1;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	//3 - Fill struct with the correct data
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys, &((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PRIMITIVE_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PRIMITIVE_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_PRIMITIVE_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MEMORY_ERROR_INPUT, "name_filter");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LIMIT_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LIMIT_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LIMIT_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->limit = (int) strtol(value, NULL, 10);
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->limit < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Limit must be positive\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LIMIT, OPH_IN_PARAM_LIMIT_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->level = (int) strtol(value, NULL, 10);
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->level < 1 || ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->level > 5) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Level unrecognized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DBMS_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DBMS_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DBMS_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE) == 0) {
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->id_dbms = 0;
	} else {
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->id_dbms = (int) strtol(value, NULL, 10);
		if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->id_dbms < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "id of dbms must be positive\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LIMIT, OPH_IN_PARAM_DBMS_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PRIMITIVE_RETURN_TYPE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PRIMITIVE_RETURN_TYPE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_PRIMITIVE_RETURN_TYPE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MEMORY_ERROR_INPUT, "func_ret");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PRIMITIVE_TYPE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PRIMITIVE_TYPE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_PRIMITIVE_TYPE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_MEMORY_ERROR_INPUT, "func_type");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_INFO_START);

	int level = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->level;
	char *func_ret = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret;
	char *func_type = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type;
	char *name_filter = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter;
	int limit = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->limit;
	int id_dbms = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->id_dbms;
	ophidiadb *oDB = &((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB;
	char **objkeys = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys;
	int objkeys_num = ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys_num;

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		oph_odb_disconnect_from_ophidiadb(&((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_dbms_instance dbms;
	if (id_dbms == 0) {
		if (oph_odb_stge_retrieve_first_dbmsinstance(oDB, &dbms)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS info\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_RETRIEVE_DBMS_ERROR, id_dbms);
			oph_odb_disconnect_from_ophidiadb(&((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	} else {
		if (oph_odb_stge_retrieve_dbmsinstance(oDB, id_dbms, &dbms)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS info\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_RETRIEVE_DBMS_ERROR, id_dbms);
			oph_odb_disconnect_from_ophidiadb(&((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}
	oph_odb_disconnect_from_ophidiadb(&((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB);

	int num_fields;
	switch (level) {
		case 5:
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+---------+\n");
			printf("| %-35s | %-12s | %-45s | %-15s | %-7s |\n", "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY", "PRIMITIVE TYPE", "DBMS ID");
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+---------+\n");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
				num_fields = 5;
				char *keys[5] = { "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY", "PRIMITIVE TYPE", "DBMS ID" };
				char *fieldtypes[5] = { "string", "string", "string", "string", "int" };
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, "Primitives List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			break;
		case 4:
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+\n");
			printf("| %-35s | %-12s | %-45s | %-15s |\n", "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY", "PRIMITIVE TYPE");
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+\n");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
				num_fields = 4;
				char *keys[4] = { "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY", "PRIMITIVE TYPE" };
				char *fieldtypes[4] = { "string", "string", "string", "string" };
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, "Primitives List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			break;
		case 3:
			printf("+-------------------------------------+--------------+-----------------------------------------------+\n");
			printf("| %-35s | %-12s | %-45s |\n", "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY");
			printf("+-------------------------------------+--------------+-----------------------------------------------+\n");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
				num_fields = 3;
				char *keys[3] = { "PRIMITIVE NAME", "RETURN TYPE", "DYNAMIC LIBRARY" };
				char *fieldtypes[3] = { "string", "string", "string" };
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, "Primitives List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			break;
		case 2:
			printf("+-------------------------------------+--------------+\n");
			printf("| %-35s | %-12s |\n", "PRIMITIVE NAME", "RETURN TYPE");
			printf("+-------------------------------------+--------------+\n");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
				num_fields = 2;
				char *keys[2] = { "PRIMITIVE NAME", "RETURN TYPE" };
				char *fieldtypes[2] = { "string", "string" };
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, "Primitives List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			break;
		case 1:
			printf("+-------------------------------------+\n");
			printf("| %-35s |\n", "PRIMITIVE NAME");
			printf("+-------------------------------------+\n");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
				num_fields = 1;
				char *keys[1] = { "PRIMITIVE NAME" };
				char *fieldtypes[1] = { "string" };
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, "Primitives List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LEVEL);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (oph_dc_setup_dbms(&(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server), dbms.io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_IOPLUGIN_SETUP_ERROR, dbms.io_server_type);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (oph_dc_connect_to_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_CONNECT_DBMS_ERROR, dbms.id_dbms);
		oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
		oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_ioserver_result *primitives_list = NULL;

	//retrieve primitives list
	if (oph_dc_get_primitives(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms, name_filter, &primitives_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve primitives list\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_RETRIEVE_LIST_ERROR, dbms.id_dbms);
		oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
		oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int num_rows = 0;

	//Empty set
	if (!(num_rows = primitives_list->num_rows)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NO_ROWS_FOUND);
		oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
		oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
		oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if (primitives_list->num_fields != 4) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NO_ROWS_FOUND);
		oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
		oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
		oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	oph_ioserver_row *curr_row = NULL;

	if (oph_ioserver_fetch_row(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list, &curr_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_ROW_ERROR);
		oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
		oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
		oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
		return OPH_DC_SERVER_ERROR;
	}
	//For each ROW
	char tmp_ret[OPH_COMMON_BUFFER_LEN] = { '\0' }, tmp_type[OPH_COMMON_BUFFER_LEN] = {
	'\0'};
	int n;
	int count_limit = 0;
	while (curr_row->row) {
		if (limit > 0 && count_limit >= limit)
			break;
		switch (level) {
			case 5:
				if (func_ret || func_type) {
					if (func_ret) {
						n = snprintf(tmp_ret, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[1][0] == '0') ? "array" : "number");
						if (n <= 0)
							break;
						if (strncmp(tmp_ret, func_ret, OPH_COMMON_BUFFER_LEN) != 0)
							break;
					}
					if (func_type) {
						n = snprintf(tmp_type, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[3][0] == 'f') ? "simple" : "aggregate");
						if (n <= 0)
							break;
						if (strncmp(tmp_type, func_type, OPH_COMMON_BUFFER_LEN) != 0)
							break;
					}
					printf("| %-35s | %-12s | %-45s | %-15s | %7d |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2],
					       (curr_row->row[3][0] == 'f') ? "simple" : "aggregate", dbms.id_dbms);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char tmpbuf[20];
						snprintf(tmpbuf, 20, "%d", dbms.id_dbms);
						char *my_row[5] =
						    { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2], (curr_row->row[3][0] == 'f') ? "simple" : "aggregate",
							tmpbuf
						};
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				} else {
					printf("| %-35s | %-12s | %-45s | %-15s | %7d |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2],
					       (curr_row->row[3][0] == 'f') ? "simple" : "aggregate", dbms.id_dbms);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char tmpbuf[20];
						snprintf(tmpbuf, 20, "%d", dbms.id_dbms);
						char *my_row[5] =
						    { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2], (curr_row->row[3][0] == 'f') ? "simple" : "aggregate",
							tmpbuf
						};
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				}
				count_limit++;
				break;
			case 4:
				if (func_ret || func_type) {
					if (func_ret) {
						n = snprintf(tmp_ret, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[1][0] == '0') ? "array" : "number");
						if (n <= 0)
							break;
						if (strncmp(tmp_ret, func_ret, OPH_COMMON_BUFFER_LEN) != 0)
							break;
					}
					if (func_type) {
						n = snprintf(tmp_type, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[3][0] == 'f') ? "simple" : "aggregate");
						if (n <= 0)
							break;
						if (strncmp(tmp_type, func_type, OPH_COMMON_BUFFER_LEN) != 0)
							break;
					}
					printf("| %-35s | %-12s | %-45s | %-15s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2],
					       (curr_row->row[3][0] == 'f') ? "simple" : "aggregate");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[4] =
						    { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2], (curr_row->row[3][0] == 'f') ? "simple" : "aggregate" };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				} else {
					printf("| %-35s | %-12s | %-45s | %-15s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2],
					       (curr_row->row[3][0] == 'f') ? "simple" : "aggregate");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[4] =
						    { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2], (curr_row->row[3][0] == 'f') ? "simple" : "aggregate" };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				}
				count_limit++;
				break;
			case 3:
				if (func_ret) {
					n = snprintf(tmp_ret, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[1][0] == '0') ? "array" : "number");
					if (n <= 0)
						break;
					if (strncmp(tmp_ret, func_ret, OPH_COMMON_BUFFER_LEN) != 0)
						break;
					printf("| %-35s | %-12s | %-45s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2]);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[3] = { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2] };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				} else {
					printf("| %-35s | %-12s | %-45s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2]);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[3] = { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number", curr_row->row[2] };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				}
				count_limit++;
				break;
			case 2:
				if (func_ret) {
					n = snprintf(tmp_ret, OPH_COMMON_BUFFER_LEN, "%s", (curr_row->row[1][0] == '0') ? "array" : "number");
					if (n <= 0)
						break;
					if (strncmp(tmp_ret, func_ret, OPH_COMMON_BUFFER_LEN) != 0)
						break;
					printf("| %-35s | %-12s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[2] = { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number" };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				} else {
					printf("| %-35s | %-12s |\n", curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
						char *my_row[2] = { curr_row->row[0], (curr_row->row[1][0] == '0') ? "array" : "number" };
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
							oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
							oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
				}
				count_limit++;
				break;
			case 1:
				printf("| %-35s |\n", curr_row->row[0]);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST)) {
					char *my_row[1] = { curr_row->row[0] };
					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_LIST, my_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
						oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
						oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}
				count_limit++;
				break;
			default:
				pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LEVEL);
				oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
				oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
				oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (oph_ioserver_fetch_row(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list, &curr_row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_ROW_ERROR);
			oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
			oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
			oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);
			return OPH_DC_SERVER_ERROR;
		}

	}

	oph_ioserver_free_result(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, primitives_list);
	oph_dc_disconnect_from_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server, &dbms);
	oph_dc_cleanup_dbms(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->server);

	switch (level) {
		case 5:
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+---------+\n");
			break;
		case 4:
			printf("+-------------------------------------+--------------+-----------------------------------------------+-----------------+\n");
			break;
		case 3:
			printf("+-------------------------------------+--------------+-----------------------------------------------+\n");
			break;
		case 2:
			printf("+-------------------------------------+--------------+\n");
			break;
		case 1:
			printf("+-------------------------------------+\n");
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_BAD_LEVEL);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	printf(OPH_PRIMITIVES_LIST_HELP_MESSAGE);
	if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_PRIMITIVES_LIST_TIP)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_PRIMITIVES_LIST_TIP, "Useful Tip", OPH_PRIMITIVES_LIST_HELP_MESSAGE)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_INFO_END);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PRIMITIVES_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret) {
		free((char *) ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret);
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_ret = NULL;
	}
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type) {
		free((char *) ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type);
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->func_type = NULL;
	}
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter) {
		free((char *) ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter);
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->name_filter = NULL;
	}
	if (((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_PRIMITIVES_LIST_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
