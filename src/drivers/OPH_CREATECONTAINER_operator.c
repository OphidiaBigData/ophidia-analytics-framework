/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <math.h>

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_CREATECONTAINER_operator.h"

#include "oph_idstring_library.h"
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

	if (!(handle->operator_handle = (OPH_CREATECONTAINER_operator_handle *) calloc(1, sizeof(OPH_CREATECONTAINER_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	int i;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_vocabulary = 0;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_year = 0;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_month = 2;
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description = NULL;

	ophidiadb *oDB = &((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *container_name, *value;

	container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char **dim_names;
	char **dim_types;
	char **dim_hierarchy;
	int number_of_dimensions_names = 0;
	int number_of_dimensions_types = 0;
	int number_of_dimensions_hierarchy = 0;
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &dim_names, &number_of_dimensions_names)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DIMENSION_TYPE);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_TYPE, OPH_TP_TASKLEN)) {
		if (oph_tp_parse_multiple_value_param(value, &dim_types, &number_of_dimensions_types)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (number_of_dimensions_names != number_of_dimensions_types) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	} else {
		number_of_dimensions_types = number_of_dimensions_names;
		if (!(dim_types = (char **) malloc(number_of_dimensions_types * sizeof(char *)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < number_of_dimensions_types; i++)
			dim_types[i] = strdup(OPH_COMMON_DEFAULT_TYPE);
	}

	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->number_of_dimensions = number_of_dimensions_names;

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_OPHIDIADB_CONFIGURATION_FILE,
			((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_OPHIDIADB_CONNECTION_ERROR,
			((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIERARCHY_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIERARCHY_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HIERARCHY_NAME);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_HIERARCHY, OPH_TP_TASKLEN)) {
		if (oph_tp_parse_multiple_value_param(value, &dim_hierarchy, &number_of_dimensions_hierarchy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (number_of_dimensions_names != number_of_dimensions_hierarchy) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(number_of_dimensions_hierarchy * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, number_of_dimensions_hierarchy * sizeof(int));

		int id_hierarchy = 0, found_oph_time = 0;
		for (i = 0; i < number_of_dimensions_hierarchy; i++) {
			//Retrieve hierarchy ID
			if (oph_odb_dim_retrieve_hierarchy_id(oDB, dim_hierarchy[i], &id_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, dim_hierarchy[i]);
				oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
				oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] = id_hierarchy;
			if (!strcasecmp(dim_hierarchy[i], OPH_COMMON_TIME_HIERARCHY)) {
				if (found_oph_time) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Only one time dimension can be used\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name,
						dim_hierarchy[i]);
					oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
					oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
					oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] *= -1;
				found_oph_time = 1;
			}
		}
		oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
	}
	//Default hierarchy
	else {
		if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(number_of_dimensions_names * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, number_of_dimensions_names * sizeof(int));

		//Retrieve hierarchy ID
		int id_hierarchy = 0;
		if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_DEFAULT_HIERARCHY, &id_hierarchy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, OPH_COMMON_DEFAULT_HIERARCHY);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		for (i = 0; i < number_of_dimensions_names; i++)
			((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] = id_hierarchy;
	}

	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name = (char **) malloc(number_of_dimensions_names * sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension_name");
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name, 0, number_of_dimensions_names * sizeof(char *));

	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type = (char **) malloc(number_of_dimensions_names * sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension_type");
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type, 0, number_of_dimensions_names * sizeof(char *));

	int size;
	for (i = 0; i < number_of_dimensions_names; i++) {
		if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name[i] = (char *) strndup(dim_names[i], OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, dim_names[i]);

			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (oph_dim_check_data_type(dim_types[i], &size)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimension type %s\n", dim_types[i]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_DIM_TYPE_ERROR, container_name, dim_types[i]);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type[i] = (char *) strndup(dim_types[i], OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, dim_types[i]);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
	oph_tp_free_multiple_value_param_list(dim_types, number_of_dimensions_types);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMPRESSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMPRESSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_COMPRESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->compressed = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VOCABULARY);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VOCABULARY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_VOCABULARY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if ((oph_odb_meta_retrieve_vocabulary_id(oDB, value, &((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_vocabulary))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input vocabulary\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_NO_VOCABULARY_NO_CONTAINER, container_name, value);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BASE_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BASE_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_UNITS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_UNITS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CALENDAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CALENDAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MONTH_LENGTHS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MONTH_LENGTHS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_YEAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_YEAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_YEAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_year = (int) strtol(value, NULL, 10);
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_MONTH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_MONTH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_MONTH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_month = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_DESCRIPTION);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *container_name = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output;
	int i, id_container_out = 0, container_unique = 0, num_of_input_dim = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->number_of_dimensions, last_insertd_id = 0;
	ophidiadb *oDB = &((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->oDB;
	char *cwd = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd;
	char *user = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user;
	int id_vocabulary = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_vocabulary;

	int permission = 0;
	int folder_id = 0;
	//Check if input path exists
	if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_CWD_ERROR, container_name, cwd);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_DATACUBE_PERMISSION_ERROR, container_name, user);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (!oph_odb_fs_is_allowed_name(container_name)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_NAME_NOT_ALLOWED_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	//Check if container exists in folder
	if ((oph_odb_fs_is_unique(folder_id, container_name, oDB, &container_unique))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (!container_unique) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Container '%s' already exists in this path or a folder has the same name\n", container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	oph_odb_db_instance db_;
	oph_odb_db_instance *db = &db_;
	if (oph_dim_load_dim_dbinstance(db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DIM_LOAD);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DIM_CONNECT);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	if (oph_dim_create_db(db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension db\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DB_CREATION);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//If it doesn't then create new container and get last id
	oph_odb_container cont;
	snprintf(cont.container_name, OPH_ODB_FS_CONTAINER_SIZE, "%s", container_name);
	cont.id_parent = 0;
	cont.id_folder = folder_id;
	cont.operation[0] = 0;
	cont.id_vocabulary = id_vocabulary;
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description)
		snprintf(cont.description, OPH_ODB_FS_DESCRIPTION_SIZE, "%s", ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description);
	else
		*cont.description = 0;

	if (oph_odb_fs_insert_into_container_table(oDB, &cont, &id_container_out)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CREATECONTAINER_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container = id_container_out;

	//Create new dimensions
	oph_odb_dimension dim;
	dim.id_container = id_container_out;
	strncpy(dim.base_time, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
	dim.base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
	dim.leap_year = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_year;
	dim.leap_month = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->leap_month;
	for (i = 0; i < num_of_input_dim; i++) {
		//Change the dimension names
		strncpy(dim.dimension_name, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name[i], OPH_ODB_DIM_DIMENSION_SIZE);
		dim.dimension_name[OPH_ODB_DIM_DIMENSION_SIZE] = 0;
		strncpy(dim.dimension_type, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type[i], OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
		dim.dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
		dim.id_hierarchy = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i];
		if (dim.id_hierarchy >= 0)
			dim.units[0] = dim.calendar[0] = 0;
		else {
			int j = 0;
			dim.id_hierarchy *= -1;
			strncpy(dim.units, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
			dim.units[OPH_ODB_DIM_TIME_SIZE] = 0;
			strncpy(dim.calendar, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
			dim.calendar[OPH_ODB_DIM_TIME_SIZE] = 0;
			char *tmp = NULL, *save_pointer = NULL, month_lengths[1 + OPH_ODB_DIM_TIME_SIZE];
			strncpy(month_lengths, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
			month_lengths[OPH_ODB_DIM_TIME_SIZE] = 0;
			while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
				dim.month_lengths[j++] = (int) strtol(tmp, NULL, 10);
			while (j < OPH_ODB_DIM_MONTH_NUMBER)
				dim.month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
		}
		if (oph_odb_dim_insert_into_dimension_table(oDB, &(dim), &last_insertd_id, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert dimension.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_CREATECONTAINER_INSERT_DIMENSION_ERROR);
			oph_odb_fs_delete_from_container_table(oDB, id_container_out);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	char dimension_table_name[OPH_COMMON_BUFFER_LEN];
	snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container);
	if (oph_dim_create_empty_table(db, dimension_table_name)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DIM_TABLE_CREATION_ERROR);
		oph_odb_fs_delete_from_container_table(oDB, id_container_out);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container);
	if (oph_dim_create_empty_table(db, dimension_table_name)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_DIM_TABLE_CREATION_ERROR);
		oph_odb_fs_delete_from_container_table(oDB, id_container_out);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_dim_disconnect_from_dbms(db->dbms_instance);
	oph_dim_unload_dim_dbinstance(db);

	//Master process print output container PID
	char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_PID_URI_ERROR);
		oph_odb_fs_delete_from_container_table(oDB, id_container_out);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_pid_show_pid(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, 0, tmp_uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_PID_SHOW_ERROR);
		free(tmp_uri);
		oph_odb_fs_delete_from_container_table(oDB, id_container_out);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char jsonbuf[OPH_COMMON_BUFFER_LEN];
	memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
	snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT2, tmp_uri, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container);

	// ADD OUTPUT PID TO JSON AS TEXT
	if (oph_json_is_objkey_printable
	    (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num,
	     OPH_JSON_OBJKEY_CREATECONTAINER)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CREATECONTAINER, "Output Container", jsonbuf)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			free(tmp_uri);
			oph_odb_fs_delete_from_container_table(oDB, id_container_out);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	// ADD OUTPUT PID TO NOTIFICATION STRING
	char tmp_string[OPH_COMMON_BUFFER_LEN];
	snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_CONTAINER_PID, jsonbuf);
	if (handle->output_string) {
		strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
		free(handle->output_string);
	}
	handle->output_string = strdup(tmp_string);

	free(tmp_uri);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_output_container, OPH_LOG_OPH_CREATECONTAINER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->container_output = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->user = NULL;
	}

	int i, dim_num = ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->number_of_dimensions;

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name) {
		for (i = 0; i < dim_num; i++) {
			if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name[i]) {
				free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name[i]);
				((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name[i] = NULL;
			}
		}
		free((char **) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type) {
		for (i = 0; i < dim_num; i++) {
			if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type[i]) {
				free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type[i]);
				((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type[i] = NULL;
			}
		}
		free((char **) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->dimension_type = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy) {
		free((int *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}

	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->base_time = NULL;
	}
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->units = NULL;
	}
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->calendar = NULL;
	}
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	}
	if (((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description);
		((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle)->description = NULL;
	}

	free((OPH_CREATECONTAINER_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
