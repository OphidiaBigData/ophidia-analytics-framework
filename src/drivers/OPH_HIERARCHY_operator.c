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

#include "drivers/OPH_HIERARCHY_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"
#include "oph_render_output_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_HIERARCHY_operator_handle *) calloc(1, sizeof(OPH_HIERARCHY_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name = NULL;
	((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version = NULL;
	((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num = -1;

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
	if (oph_tp_parse_multiple_value_param(value, &((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, &((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIERARCHY_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIERARCHY_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_HIERARCHY_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "hierarchy_name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIERARCHY_VERSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIERARCHY_VERSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_HIERARCHY_VERSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_LATEST_VERSION) != 0) {
		if (!(((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "hierarchy_version");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_INFO_START);

	char *hierarchy_name = ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name;
	char *hierarchy_version = ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version;

	char xml[OPH_TP_BUFLEN];
	char filename[2 * OPH_TP_BUFLEN];

	if (hierarchy_name) {
		//Show hierarchy description
		if (oph_tp_retrieve_function_xml_file((const char *) hierarchy_name, (const char *) hierarchy_version, OPH_TP_XML_HIERARCHY_TYPE_CODE, &xml)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find xml\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_XML_NOT_FOUND, hierarchy_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, xml);

		int res;
		res = renderXML(filename, OPH_TP_XML_HIERARCHY_TYPE_CODE, handle->operator_json, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num);	//render XML
		if (res == OPH_RENDER_OUTPUT_RENDER_ERROR) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error parsing XML\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_XML_PARSE_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		} else if (res == OPH_RENDER_OUTPUT_RENDER_INVALID_XML) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "XML not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_INVALID_XML);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		} else {
			logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_INFO_END);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}
	} else {
		//Show list of hierarchies
		printf("+--------------------------------+\n");
		printf("| %-30s |\n", "HIERARCHY NAME");
		printf("+--------------------------------+\n");

		// SET TABLE COLUMN FOR JSON
		int objkey_printable =
		    oph_json_is_objkey_printable(((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_HIERARCHY_LIST);
		if (objkey_printable) {
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 1, iii;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "keys");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonkeys[0] = strdup("HIERARCHY NAME");
			if (!jsonkeys[0]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "key");
				free(jsonkeys);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[0] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[0]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(fieldtypes);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_HIERARCHY_LIST, "Hierarchy List", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
		ophidiadb oDB;
		oph_odb_init_ophidiadb(&oDB);

		if (oph_odb_read_ophidiadb_config_file(&oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
			oph_odb_free_ophidiadb(&oDB);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(&oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
			oph_odb_free_ophidiadb(&oDB);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//retrieve information list
		if (oph_odb_dim_find_hierarchy_list(&oDB, &info_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive hierarchy list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_READ_LIST_INFO_ERROR);
			if (info_list)
				mysql_free_result(info_list);
			oph_odb_free_ophidiadb(&oDB);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(info_list))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NO_ROWS_FOUND);
			mysql_free_result(info_list);
			oph_odb_free_ophidiadb(&oDB);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}

		if (mysql_field_count(oDB.conn) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MISSING_FIELDS);
			mysql_free_result(info_list);
			oph_odb_free_ophidiadb(&oDB);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		MYSQL_ROW row;

		//For each ROW
		while ((row = mysql_fetch_row(info_list))) {
			printf("| %-30s |\n", (row[0] ? row[0] : "-"));

			// ADD ROW TO JSON GRID
			if (objkey_printable) {
				char **jsonvalues = NULL;
				int num_fields = 1, iii;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "values");
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jsonvalues[0] = strdup(row[0] ? row[0] : "");
				if (!jsonvalues[0]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_MEMORY_ERROR_INPUT, "value");
					free(jsonvalues);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_HIERARCHY_LIST, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
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

		}
		mysql_free_result(info_list);

		printf("+--------------------------------+\n");
		oph_odb_free_ophidiadb(&oDB);

		printf(OPH_HIERARCHY_HELP_MESSAGE);
		// ADD OUTPUT TIP TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_TIP)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_HIERARCHY_TIP, "Useful Tip", OPH_HIERARCHY_HELP_MESSAGE)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_HIERARCHY_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name) {
		free((char *) ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name);
		((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_name = NULL;
	}
	if (((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version) {
		free((char *) ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version);
		((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->hierarchy_version = NULL;
	}
	if (((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys, ((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_HIERARCHY_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_HIERARCHY_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
