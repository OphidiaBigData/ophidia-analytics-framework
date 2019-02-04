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

#include "drivers/OPH_RANDCUBE_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <math.h>
#include <strings.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"
#include "oph_driver_procedure_library.h"

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

	if (!(handle->operator_handle = (OPH_RANDCUBE_operator_handle *) calloc(1, sizeof(OPH_RANDCUBE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	int i;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run = 1;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo = NULL;

	//3 - Fill struct with the correct data
	char *container_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input path");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HOST_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HOST_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HOST_NUMBER);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number = (int) strtol(value, NULL, 10);
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number == 0)
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number = -1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FRAGMENENT_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FRAGMENENT_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_FRAGMENENT_NUMBER);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TUPLE_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_TUPLE_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_TUPLE_NUMBER);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MEASURE_NAME);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "measure name");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MEASURE_TYPE);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_dc_check_data_type(value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid measure type %s\n", value);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEASURE_TYPE_ERROR, container_name, value);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "measure type");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions = (int) strtol(value, NULL, 10);

	char **dim_names;
	char **dim_levels;
	char **dim_sizes;
	int number_of_dimensions_names = 0;
	int number_of_dimensions_levels = 0;
	int number_of_dimensions_sizes = 0;
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &dim_names, &number_of_dimensions_names)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_SIZE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_SIZE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DIMENSION_SIZE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &dim_sizes, &number_of_dimensions_sizes)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_dimensions_names != number_of_dimensions_sizes) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_imp_dimensions =
	    number_of_dimensions_names - ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions;
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_imp_dimensions < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_BAD_PARAMETER, OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER,
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_DIMENSION_LEVEL);
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(OPH_COMMON_DEFAULT_CONCEPT_LEVEL))) {
		if (oph_tp_parse_multiple_value_param(value, &dim_levels, &number_of_dimensions_levels)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_levels, number_of_dimensions_levels);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (number_of_dimensions_names != number_of_dimensions_levels) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_levels, number_of_dimensions_levels);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level = (char *) malloc(number_of_dimensions_levels * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension levels");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_levels, number_of_dimensions_levels);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level, 0, number_of_dimensions_levels);

		for (i = 0; i < number_of_dimensions_levels; i++) {
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i] = dim_levels[i][0];
			if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i] == OPH_COMMON_ALL_CONCEPT_LEVEL) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_BAD2_PARAMETER, "dimension level",
					((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i]);
				oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
				oph_tp_free_multiple_value_param_list(dim_levels, number_of_dimensions_levels);
				oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
		oph_tp_free_multiple_value_param_list(dim_levels, number_of_dimensions_levels);
	}
	//Default levels
	else {
		if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level = (char *) malloc(number_of_dimensions_names * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension levels");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level, 0, number_of_dimensions_names);

		for (i = 0; i < number_of_dimensions_names; i++) {
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;
		}
	}

	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name = (char **) malloc(number_of_dimensions_names * sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension_name");
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name, 0, number_of_dimensions_names * sizeof(char *));

	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size = (long long *) malloc(number_of_dimensions_names * sizeof(long long)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "dimension_size");
		oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
		oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size, 0, number_of_dimensions_names * sizeof(long long));

	for (i = 0; i < number_of_dimensions_names; i++) {
		if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i] = (char *) strndup(dim_names[i], OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, dim_names[i]);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i] = (int) strtol(dim_sizes[i], NULL, 10);
		if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value: %s cannot be %d\n", OPH_IN_PARAM_DIMENSION_SIZE,
			      ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_BAD_PARAMETER, OPH_IN_PARAM_DIMENSION_SIZE,
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i]);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}
	oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
	oph_tp_free_multiple_value_param_list(dim_sizes, number_of_dimensions_sizes);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMPRESSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMPRESSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_COMPRESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->compressed = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SIMULATE_RUN);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SIMULATE_RUN);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SIMULATE_RUN);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_NO_VALUE) == 0) {
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run = 0;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PARTITION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PARTITION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_PARTITION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input partition");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IOSERVER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "I/O server type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}


	long long imp_size_prod = 1;
	for (i = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions;
	     i < ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions + ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_imp_dimensions; i++)
		imp_size_prod *= ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i];
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->array_length = imp_size_prod;


	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);
#ifdef OPH_ODB_MNG
		oph_odb_init_mongodb(oDB);
#endif
		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_OPHIDIADB_CONFIGURATION_FILE,
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_OPHIDIADB_CONNECTION_ERROR,
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
#ifdef OPH_ODB_MNG
		if (oph_odb_connect_to_mongodb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_OPHIDIADB_CONNECTION_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
#endif
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo = (char *) strndup(value, OPH_TP_TASKLEN))) {
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MEMORY_ERROR_INPUT_NO_CONTAINER, OPH_IN_PARAM_ALGORITHM);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int id_datacube[4] = { 0, 0, 0, 0 }, flush = 1, id_datacube_out = 0;
	char *container_name = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input;
	ophidiadb *oDB = &((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB;

	if (handle->proc_rank == 0) {

		int i, j;
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

		long long kk;
		long long *index_array = NULL;
		int container_exists = 0;
		int id_container_out = 0;
		int last_insertd_id = 0;
		int num_of_input_dim =
		    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions + ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_imp_dimensions;

		//Retrieve user id
		char *user = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user;
		int id_user = 0;
		if (oph_odb_user_retrieve_user_id(oDB, user, &id_user)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive user id\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_USER_ID_ERROR);
			goto __OPH_EXIT_1;
		}

	  /********************************
	   *INPUT PARAMETERS CHECK - BEGIN*
	   ********************************/

		int id_host_partition = 0;
		char hidden = 0, *host_partition = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input;

		//If default values are used: select fylesystem and partition
		if (!strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(host_partition))
		    && !strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(OPH_COMMON_HOSTPARTITION_DEFAULT))) {
			if (oph_odb_stge_get_default_host_partition_fs
			    (oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type, &id_host_partition,
			     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number > 0 ? ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number : 1)
			    || !id_host_partition) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
					((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number, host_partition);
				goto __OPH_EXIT_1;
			}
		} else {
			if (oph_odb_stge_get_host_partition_by_name(oDB, host_partition, id_user, &id_host_partition, &hidden)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to load partition '%s'!\n", host_partition);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Failed to load partition '%s'!\n", host_partition);
				goto __OPH_EXIT_1;
			}
		}

		int exist_part = 0;
		int nhost = 0;
		if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number < 0) {
			//Check if are available DBMS and HOST number into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type, id_host_partition, &nhost) || !nhost) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or dbms or server type and partition are not available!\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name,
					((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input);
				goto __OPH_EXIT_1;
			}
			if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number < 0)
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number = nhost;
		}
		//Check if are available DBMS and HOST number into specified partition and of server type
		if ((oph_odb_stge_check_number_of_host_dbms
		     (oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type, id_host_partition, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number,
		      &exist_part)) || !exist_part) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input);
			goto __OPH_EXIT_1;
		}

		if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run) {
			int s;
			char jsonbuf[OPH_COMMON_BUFFER_LEN], jsonbuf_item[OPH_COMMON_BUFFER_LEN];
			memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);

			snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN, "Specified parameters are:\n");
			if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
				strncat(jsonbuf, jsonbuf_item, s);
			snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN, "\tNumber of hosts: %d\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number);
			if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
				strncat(jsonbuf, jsonbuf_item, s);
			snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN, "\tNumber of fragments per databases: %d\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number);
			if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
				strncat(jsonbuf, jsonbuf_item, s);
			snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN, "\tNumber of tuples per fragment: %d\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number);
			if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
				strncat(jsonbuf, jsonbuf_item, s);
			long long tot_tuple_num =
			    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number *
			    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number;
			long long exp_size_prod = 1;
			for (i = 0; i < ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions; i++)
				exp_size_prod *= ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i];
			if (tot_tuple_num != exp_size_prod) {
				snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN,
					 "Product of explicit dimension sizes doesn't match partitioning schema (host, fragxdb, tuplexfrag). It should be %lld, while it is %lld\n",
					 tot_tuple_num, exp_size_prod);
				if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
					strncat(jsonbuf, jsonbuf_item, s);
			} else {
				snprintf(jsonbuf_item, OPH_COMMON_BUFFER_LEN, "Parameters are correct\n");
				if ((s = OPH_COMMON_BUFFER_LEN - strlen(jsonbuf)) > 1)
					strncat(jsonbuf, jsonbuf_item, s);
			}
			if (oph_json_is_objkey_printable
			    (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_RANDCUBE)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_RANDCUBE, "Fragmentation parameters", jsonbuf)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			printf("%s", jsonbuf);
			goto __OPH_EXIT_1;
		} else {
			//Check if product of explicit dimension sizes is the same of total tuple number
			long long tot_tuple_num =
			    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number *
			    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number;
			long long exp_size_prod = 1;
			for (i = 0; i < ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions; i++)
				exp_size_prod *= ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i];
			if (tot_tuple_num != exp_size_prod) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Product of explicit dimension sizes doesn't match partitioning schema (host, fragxdb, tuplexfrag)\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_SIZES_PARTITION_PARAMS_MISMATCH, tot_tuple_num, exp_size_prod);
				goto __OPH_EXIT_1;
			}
		}


		char *cwd = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd;
		int permission = 0;
		int folder_id = 0;
		//Check if input path exists
		if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_CWD_ERROR, container_name, cwd);
			goto __OPH_EXIT_1;
		}
		if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_DATACUBE_PERMISSION_ERROR, container_name, user);
			goto __OPH_EXIT_1;
		}
		//Check if container exists
		if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		if (!container_exists) {
			//If it doesn't exists then return an error
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Else retreive container ID and check for dimension table 
		if ((oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}

	  /********************************
	   *  DATACUBE CREATION - BEGIN   *
	   ********************************/
		char *tmp = id_string;
		if (oph_ids_create_new_id_string
		    (&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1,
		     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment ids string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_CREATE_ID_STRING_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		cube.hostxdatacube = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number;
		cube.fragmentxdb = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number;
		cube.tuplexfragment = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		cube.id_container = id_container_out;
		strncpy(cube.measure, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure, OPH_ODB_CUBE_MEASURE_SIZE);
		cube.measure[OPH_ODB_CUBE_MEASURE_SIZE] = 0;
		strncpy(cube.measure_type, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
		cube.measure_type[OPH_ODB_CUBE_MEASURE_TYPE_SIZE] = 0;
		strncpy(cube.frag_relative_index_set, id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		cube.frag_relative_index_set[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE] = 0;
		cube.db_number = cube.hostxdatacube;
		cube.compressed = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->compressed;
		cube.id_db = NULL;
		//New fields
		cube.id_source = 0;
		cube.level = 0;
		if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &id_datacube_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DATACUBE_INSERT_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);
	  /********************************
	   *   DATACUBE CREATION - END    *
	   ********************************/

		oph_odb_dimension_instance *dim_inst = NULL;
		int dim_inst_num = 0;
		oph_odb_dimension *dims = NULL;
		int id_grid = 0;

		if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name
		    && !oph_odb_dim_retrieve_grid_id(oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &id_grid) && id_grid) {
			//Check if ophidiadb dimensions are the same of input dimensions

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
			    (oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &dims, &dim_inst, &dim_inst_num)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input grid name not usable! It is already used by another container.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_NO_GRID, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name);
				if (dims)
					free(dims);
				if (dim_inst)
					free(dim_inst);
				goto __OPH_EXIT_1;

			}

			if (dim_inst_num != num_of_input_dim) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension number doesn't match with specified grid dimension number\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_INPUT_GRID_DIMENSION_MISMATCH);
				free(dims);
				free(dim_inst);
				goto __OPH_EXIT_1;
			}

			int found_flag;
			for (i = 0; i < num_of_input_dim; i++) {
				//Check if container dimension name, size and concpet level matches input dimension params
				found_flag = 0;
				for (j = 0; j < dim_inst_num; j++) {
					if (!strncmp(dims[j].dimension_name, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i], OPH_ODB_DIM_DIMENSION_SIZE)) {
						if (dim_inst[j].size == ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i]
						    && dim_inst[j].concept_level == ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i]) {
							found_flag = 1;
							break;
						}
					}
				}

				if (!found_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n",
					      ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_INPUT_DIMENSION_MISMATCH,
						((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]);
					free(dims);
					free(dim_inst);
					goto __OPH_EXIT_1;
				}
			}
		}
		  /********************************
		   * INPUT PARAMETERS CHECK - END *
		   ********************************/
		else {
		 /****************************
		  * BEGIN - IMPORT DIMENSION *
		  ***************************/

			//Load dimension table database infos and connect
			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
			int exist_flag = 0;

			if (oph_dim_check_if_dimension_table_exists(db, index_dimension_table_name, &exist_flag)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}
			if (!exist_flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}
			if (oph_dim_check_if_dimension_table_exists(db, label_dimension_table_name, &exist_flag)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}
			if (!exist_flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}

			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;

			}

			int dimension_array_id = 0;
			int number_of_dimensions_c = 0;
			oph_odb_dimension *tot_dims = NULL;

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIMENSION_READ_ERROR);
				if (tot_dims)
					free(tot_dims);
				goto __OPH_EXIT_1;

			}

			char filename[2 * OPH_TP_BUFLEN];
			oph_odb_hierarchy hier;
			dims = (oph_odb_dimension *) malloc(num_of_input_dim * sizeof(oph_odb_dimension));
			dim_inst = (oph_odb_dimension_instance *) malloc(num_of_input_dim * sizeof(oph_odb_dimension_instance));
			dim_inst_num = num_of_input_dim;

			//For each input dimension check hierarchy
			for (i = 0; i < num_of_input_dim; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncmp(tot_dims[j].dimension_name, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i],
					      ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_CONT_ERROR,
						((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i],
						((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;

				}
				//Find dimension level into hierarchy file
				if (oph_odb_dim_retrieve_hierarchy(oDB, tot_dims[j].id_hierarchy, &hier)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}

				exist_flag = 0;
				snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
				if (oph_hier_check_concept_level_short(filename, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i], &exist_flag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (!exist_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_BAD2_PARAMETER,
						"dimension level", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i]);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}
			int id_grid = 0, grid_exist = 0;
			if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name) {
				oph_odb_dimension_grid new_grid;
				strncpy(new_grid.grid_name, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				new_grid.grid_name[OPH_ODB_DIM_GRID_SIZE] = 0;
				int last_inserted_grid_id = 0;

				if (oph_odb_dim_insert_into_grid_table(oDB, &new_grid, &last_inserted_grid_id, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert grid in OphidiaDB, or grid already exists.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_GRID_INSERT_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, "Grid already exists: dimensions will be not associated to a grid.\n");
				} else
					id_grid = last_inserted_grid_id;
			}
			//For each input dimension
			for (i = 0; i < num_of_input_dim; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncmp(tot_dims[j].dimension_name, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i],
					      ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_CONT_ERROR,
						((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i],
						((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				//Create entry in dims and dim_insts
				dims[i].id_dimension = tot_dims[j].id_dimension;
				dims[i].id_container = tot_dims[j].id_container;
				dims[i].id_hierarchy = tot_dims[j].id_hierarchy;
				snprintf(dims[i].dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "%s", tot_dims[j].dimension_name);
				snprintf(dims[i].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE, "%s", tot_dims[j].dimension_type);

				dim_inst[i].id_dimension = tot_dims[j].id_dimension;
				dim_inst[i].fk_id_dimension_index = 0;
				dim_inst[i].concept_level = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level[i];
				dim_inst[i].unlimited = 0;
				dim_inst[i].size = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size[i];
				dim_inst[i].fk_id_dimension_label = 0;
				dim_inst[i].id_grid = id_grid;
				dim_inst[i].id_dimensioninst = 0;
				if (oph_dim_insert_into_dimension_table_rand_data(db, label_dimension_table_name, tot_dims[j].dimension_type, dim_inst[i].size, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_ROW_ERROR, dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				dim_inst[i].fk_id_dimension_label = dimension_array_id;	// Real data

				index_array = (long long *) malloc(dim_inst[i].size * sizeof(long long));
				for (kk = 0; kk < dim_inst[i].size; ++kk)
					index_array[kk] = 1 + kk;	// Non 'C'-like indexing

				if (oph_dim_insert_into_dimension_table
				    (db, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, (long long) dim_inst[i].size, (char *) index_array, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIM_ROW_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					free(index_array);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				free(index_array);
				dim_inst[i].fk_id_dimension_index = dimension_array_id;	// Indexes

				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[i]), &dimension_array_id, id_datacube_out, NULL, NULL, id_user)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIMINST_INSERT_ERROR, dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				dim_inst[i].id_dimensioninst = dimension_array_id;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);

			if (id_grid && oph_odb_dim_enable_grid(oDB, id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to enable grid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to enable grid\n");
				free(dims);
				free(dim_inst);
				goto __OPH_EXIT_1;
			}

	 /****************************
      *  END - IMPORT DIMENSION  *
	  ***************************/
		}

	  /********************************
	   *  CUBEHASDIM CREATION - BEGIN *
	   ********************************/
		oph_odb_cubehasdim *cubedim = (oph_odb_cubehasdim *) malloc(num_of_input_dim * sizeof(oph_odb_cubehasdim));

		//Write new cube - dimension relation rows
		for (i = 0; i < num_of_input_dim; i++) {
			//Find input dimension with same name of container dimension
			for (j = 0; j < dim_inst_num; j++)
				if (!strncmp(dims[j].dimension_name, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
					break;
			if (j == dim_inst_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimension %s in OphidiaDB.\n", ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DIMENSION_ODB_ERROR,
					((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]);
				free(dims);
				free(dim_inst);
				free(cubedim);
				goto __OPH_EXIT_1;
			}

			cubedim[i].id_dimensioninst = dim_inst[j].id_dimensioninst;
			cubedim[i].id_datacube = id_datacube_out;
			if (j >= ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions) {
				cubedim[i].explicit_dim = 0;
				cubedim[i].level = i + 1 - ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions;
			} else {
				cubedim[i].explicit_dim = 1;
				cubedim[i].level = i + 1;
			}

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedim[i]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_CUBEHASDIM_INSERT_ERROR);
				free(dims);
				free(dim_inst);
				free(cubedim);
				goto __OPH_EXIT_1;
			}
		}
		free(dims);
		free(cubedim);
		free(dim_inst);
	  /********************************
	   *   CUBEHASDIM CREATION - END  *
	   ********************************/

	  /********************************
	   * DB INSTANCE CREATION - BEGIN *
	   ********************************/
		int dbmss_length, host_num;
		dbmss_length = host_num = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number;
		int *id_dbmss = NULL, *id_hosts = NULL;
		//Retreive ID dbms list 
		if (oph_odb_stge_retrieve_dbmsinstance_id_list
		    (oDB, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type, id_host_partition, hidden, host_num, id_datacube_out, &id_dbmss, &id_hosts, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DBMS_LIST_ERROR);
			if (id_dbmss)
				free(id_dbmss);
			if (id_hosts)
				free(id_hosts);
			goto __OPH_EXIT_1;
		}

		oph_odb_db_instance db;
		oph_odb_dbms_instance dbms;
		char db_name[OPH_ODB_STGE_DB_NAME_SIZE];

		for (j = 0; j < dbmss_length; j++) {
			db.id_dbms = id_dbmss[j];
			//Retreive DBMS params
			if (oph_odb_stge_retrieve_dbmsinstance(oDB, db.id_dbms, &dbms)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive DBMS\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DBMS_ERROR, db.id_dbms);
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}
			db.dbms_instance = &dbms;

			if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server) {
				if (oph_dc_setup_dbms(&(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server), dbms.io_server_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_IOPLUGIN_SETUP_ERROR, db.id_dbms);
					free(id_dbmss);
					free(id_hosts);
					goto __OPH_EXIT_1;
				}
			}

			if (oph_dc_connect_to_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DBMS_CONNECTION_ERROR, dbms.id_dbms);
				oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms));
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}

			if (oph_dc_generate_db_name(oDB->name, id_datacube_out, db.id_dbms, 0, i, &db_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of Db instance  name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_STRING_BUFFER_OVERFLOW, "DB instance name", db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			strcpy(db.db_name, db_name);
			if (oph_dc_create_db(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create new db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_NEW_DB_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			//Insert new database instance and partitions
			if (oph_odb_stge_insert_into_dbinstance_partitioned_tables(oDB, &db, id_datacube_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dbinstance table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_DB_INSERT_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbms));
		}
		free(id_dbmss);
		free(id_hosts);
	  /********************************
	   *  DB INSTANCE CREATION - END  *
	   ********************************/

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = id_datacube_out;
		new_task.id_job = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->compressed)
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_DC_SQ_INSERT_COMPRESSED_FRAG, "fact");
		else
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_DC_SQ_INSERT_FRAG, "fact");
		new_task.input_cube_number = 0;
		new_task.id_inputcube = NULL;
		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_RANDCUBE_TASK_INSERT_ERROR, new_task.operator);
			goto __OPH_EXIT_1;
		}

		id_datacube[0] = id_datacube_out;
		id_datacube[1] = id_container_out;
		id_datacube[2] = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number;
		id_datacube[3] = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number;

		flush = 0;
	}
      __OPH_EXIT_1:

	if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (!handle->proc_rank && flush)
		oph_odb_cube_delete_from_datacube_table(&((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB, id_datacube_out);

	//Broadcast to all other processes the result         
	MPI_Bcast(id_datacube, 4, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (!id_datacube[0] || !id_datacube[1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube[1], OPH_LOG_OPH_RANDCUBE_MASTER_TASK_INIT_FAILED_NO_CONTAINER, container_name);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube = id_datacube[0];
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube[1];
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number = id_datacube[2];
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number = id_datacube[3];

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int frag_total_number = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->host_number * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number;


	//All processes compute the fragment number to work on
	int div_result = (frag_total_number) / (handle->proc_number);
	int div_remainder = (frag_total_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	} else {
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id += div_result;
		}
		if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id >= frag_total_number)
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i, j, k;
	int id_datacube_out = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube;
	int compr_flag = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->compressed;

	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Compute DB list starting position and number of rows
	int start_position =
	    (int) floor((double) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id / ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number);
	int row_number =
	    (int) ceil((double) (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id + ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number) /
		       ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_OPHIDIADB_CONFIGURATION_FILE,
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_OPHIDIADB_CONNECTION_ERROR,
			((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube_out, start_position, row_number, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_fragment new_frag;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];

	int frag_to_insert = 0;
	int frag_count = 0;
	int result = OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (!((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server) {
		if (oph_dc_setup_dbms(&(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_IOPLUGIN_SETUP_ERROR,
				(dbmss.value[0]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}
	//For each DBMS
	for (i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {

		if (oph_dc_connect_to_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//Compute number of fragments to insert in DB
			frag_to_insert =
			    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number - (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id + frag_count -
													  start_position * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number);

			//For each fragment
			for (k = 0; (k < frag_to_insert) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {

				//Set new fragment
				new_frag.id_datacube = id_datacube_out;
				new_frag.id_db = dbs.value[j].id_db;
				new_frag.frag_relative_index = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id + frag_count + 1;
				new_frag.key_start =
				    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number + 1;
				new_frag.key_end =
				    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number;
				new_frag.db_instance = &(dbs.value[j]);

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &fragment_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_RANDCUBE_STRING_BUFFER_OVERFLOW, "fragment name", fragment_name);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				strcpy(new_frag.fragment_name, fragment_name);

				//Create Empty fragment
				if (oph_dc_create_empty_fragment(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_RANDCUBE_FRAGMENT_CREATION_ERROR, new_frag.fragment_name);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Populate fragment
				if (compr_flag) {
					if (oph_dc_populate_fragment_with_rand_data
					    (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &new_frag,
					     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->array_length,
					     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type, 1, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment with compressed data.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_RANDCUBE_FRAG_POPULATE_ERROR, new_frag.fragment_name, "compressed");
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
				} else {
					if (oph_dc_populate_fragment_with_rand_data
					    (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &new_frag,
					     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->tuplexfrag_number, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->array_length,
					     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type, 0, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment with uncompressed data.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_RANDCUBE_FRAG_POPULATE_ERROR, new_frag.fragment_name, "uncompressed");
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
				}

				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_RANDCUBE_FRAGMENT_INSERT_ERROR, new_frag.fragment_name);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				frag_count++;
				if (frag_count == ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number)
					break;
			}
			start_position++;
		}
		oph_dc_disconnect_from_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));

	}
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	if (result == OPH_ANALYTICS_OPERATOR_SUCCESS)
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->execute_error = 0;

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
			 ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_RANDCUBE)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_RANDCUBE, "Output Cube", jsonbuf)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				free(tmp_uri);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		// ADD OUTPUT PID TO NOTIFICATION STRING
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_DATACUBE_INPUT, jsonbuf);
		if (handle->output_string) {
			strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
			free(handle->output_string);
		}
		handle->output_string = strdup(tmp_string);

		free(tmp_uri);
	}

	if (global_error) {
		//For error checking
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
		memset(id_string, 0, sizeof(id_string));

		if (handle->proc_rank == 0) {
			ophidiadb *oDB = &((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB;
			oph_odb_datacube cube;
			oph_odb_cube_init_datacube(&cube);

			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, id_datacube, &cube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_DATACUBE_READ_ERROR);
			} else {
				//Copy fragment id relative index set 
				strncpy(id_string, cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			}
			oph_odb_cube_free_datacube(&cube);
		}
		//Broadcast to all other processes the fragment relative index        
		MPI_Bcast(id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

		//Check if sequential part has been completed
		if (id_string[0] == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_RANDCUBE_MASTER_TASK_INIT_FAILED);
		} else {
			if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id >= 0 || handle->proc_rank == 0) {
				//Partition fragment relative index string
				char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
				char *new_ptr = new_id_string;
				if (oph_ids_get_substring_from_string
				    (id_string, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id,
				     ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_RANDCUBE_ID_STRING_SPLIT_ERROR);
				} else {
					//Delete fragments
					int start_position =
					    (int) floor((double) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id /
							((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number);
					int row_number = (int)
					    ceil((double)
						 (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_first_id +
						  ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragment_number) /
						 ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

					if (oph_dproc_delete_data
					    (id_datacube, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, new_id_string, start_position, row_number, 1)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_DELETE_DB_READ_ERROR);
					}
				}
			}
		}

		if (handle->output_code)
			proc_error = (short int) handle->output_code;
		else
			proc_error = OPH_ODB_JOB_STATUS_DESTROY_ERROR;
		MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MIN, MPI_COMM_WORLD);
		handle->output_code = global_error;

		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_free_ophidiadb(&((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB);
#ifdef OPH_ODB_MNG
		oph_odb_free_mongodb(&((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->oDB);
#endif
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->container_input = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server)
		oph_dc_cleanup_dbms(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->server);

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}


	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->partition_input = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_level = NULL;
	}

	int dim_num = ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_exp_dimensions + ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->number_of_imp_dimensions;
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name) {
		for (i = 0; i < dim_num; i++) {
			if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]) {
				free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i]);
				((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name[i] = NULL;
			}
		}
		free((char **) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size) {
		free((int *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->dimension_size = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}

	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo) {
		free((char *) ((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo);
		((OPH_RANDCUBE_operator_handle *) handle->operator_handle)->rand_algo = NULL;
	}
	free((OPH_RANDCUBE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
