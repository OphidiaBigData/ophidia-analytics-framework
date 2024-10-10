/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#include "drivers/OPH_EXPLORECUBE_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"
#include "oph_utility_library.h"

void oph_free_vector(char **vect, int nn)
{
	if (!vect || !nn)
		return;
	int ii;
	for (ii = 0; ii < nn; ii++)
		if (vect[ii])
			free(vect[ii]);
	free(vect);
}

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

	if (!(handle->operator_handle = (OPH_EXPLORECUBE_operator_handle *) calloc(1, sizeof(OPH_EXPLORECUBE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->where_clause = NULL;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->apply_clause = NULL;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->number_of_dim = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->limit = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_id = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_index = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_time = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->level = 1;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->base64 = 0;
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->export_metadata = 0;

	int i, j;
	for (i = 0; i < OPH_SUBSET_LIB_MAX_DIM; ++i)
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->task[i] = ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->dim_task[i] = NULL;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data 
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int data_on_ids[2 + OPH_SUBSET_LIB_MAX_DIM];
	int *id_datacube_in = data_on_ids;
	int *id_dimension = data_on_ids + 2;
	for (i = 0; i < 2 + OPH_SUBSET_LIB_MAX_DIM; ++i)
		data_on_ids[i] = 0;


	char **sub_dims = 0;
	char **sub_filters = 0;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN) != 0) {
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN) != 0) {
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->subset_type = !strncmp(value, OPH_EXPLORECUBE_TYPE_COORD, OPH_TP_TASKLEN);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LIMIT_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LIMIT_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_LIMIT_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->limit = (int) strtol(value, NULL, 10);
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->level = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_ID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_ID);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SHOW_ID);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_id = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_INDEX);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SHOW_INDEX);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_index = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SHOW_TIME);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->show_time = 1;

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->number_of_dim = number_of_sub_dims;

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_PATH);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, "default")) {
		if (!(handle->output_path = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
				"output path");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (strstr(handle->output_path, "..")) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		char *pointer = handle->output_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			char tmp[OPH_COMMON_BUFFER_LEN];
			if (*pointer != '/') {
				value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (*value != '/') {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (strlen(value) > 1) {
					if (strstr(value, "..")) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", value + 1, pointer);
					free(handle->output_path);
					handle->output_path = strdup(tmp);
					pointer = handle->output_path;
				}
			}
			if (oph_pid_get_base_src_path(&value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
			free(handle->output_path);
			handle->output_path = strdup(tmp);
			free(value);
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_NAME);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, "default")) {
		if (!(handle->output_name = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
				"output name");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (strstr(handle->output_name, "..")) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		int s;
		for (s = 0; s < (int) strlen(handle->output_name); s++) {
			if ((handle->output_name[s] == '/') || (handle->output_name[s] == ':')) {
				handle->output_name[s] = '_';
			}
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BASE64);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BASE64);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_BASE64);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->base64 = 1;

	//Only master process has to initialize and open connection to management OphidiaDB
	ophidiadb *oDB = &((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->oDB;
	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_OPHIDIADB_CONFIGURATION_FILE);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_OPHIDIADB_CONNECTION_ERROR);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	int status = 0;
	char *uri = NULL;
	int folder_id = 0;
	int permission = 0;
	if (oph_pid_parse_pid(datacube_in, &id_datacube_in[1], &id_datacube_in[0], &uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_PID_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_NO_INPUT_DATACUBE, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_DATACUBE_AVAILABILITY_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_DATACUBE_FOLDER_ERROR, datacube_in);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_DATACUBE_PERMISSION_ERROR, username);
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	} else
		for (i = 0; i < ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->number_of_dim; ++i) {
			if (oph_odb_dim_retrieve_dimension_instance_id(oDB, id_datacube_in[0], sub_dims[i], &id_dimension[i])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension id.\n");
				id_dimension[i] = 0;
				break;
			} else {
				for (j = 0; j < i; ++j)
					if (id_dimension[j] == id_dimension[i]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "A dimension has been set more times.\n");
						id_dimension[i] = 0;
						break;
					}
				if (!id_dimension[i])
					break;
			}
		}
	if (uri)
		free(uri);
	uri = NULL;

	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];
	((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	for (i = 0; i < ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->number_of_dim; ++i) {
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_dimension[i] = id_dimension[i];
		if (!(((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->task[i] = (char *) strndup(sub_filters[i], OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "subset task list");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
	oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_EXPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->export_metadata = 1;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;


	OPH_EXPLORECUBE_operator_handle *oper_handle = (OPH_EXPLORECUBE_operator_handle *) handle->operator_handle;



	int pointer, stream_max_size = 6 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int) + OPH_ODB_CUBE_MEASURE_TYPE_SIZE + 2 * OPH_TP_TASKLEN;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;

	pointer = 0;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	pointer += 1 + sizeof(int);
	pointer += 1 + sizeof(int);
	pointer += 1 + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	pointer += 1 + OPH_TP_TASKLEN;

	ophidiadb *oDB = &(oper_handle)->oDB;
	oph_odb_datacube cube;
	oph_odb_cube_init_datacube(&cube);

	int datacube_id = oper_handle->id_input_datacube;

	//retrieve input datacube
	if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
		oph_odb_cube_free_datacube(&cube);
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DATACUBE_READ_ERROR);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (!handle->output_name) {
		handle->output_name = strdup(cube.measure);
		if (!handle->output_name) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "output name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	oph_odb_cubehasdim *cubedims = NULL;
	int number_of_dimensions = 0, l;

	//Read old cube - dimension relation rows
	if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_CUBEHASDIM_READ_ERROR);
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	unsigned long long dim_size[oper_handle->number_of_dim][number_of_dimensions], new_size[number_of_dimensions], block_size;
	int dim_number[oper_handle->number_of_dim], first_explicit, d, i, explicit_dim_number = 0, implicit_dim_number = 0;
	int subsetted_dim[number_of_dimensions];
	char size_string_vector[oper_handle->number_of_dim][OPH_COMMON_BUFFER_LEN], *size_string, *dim_row;
	oph_subset *subset_struct[oper_handle->number_of_dim];
	oph_odb_dimension dim[number_of_dimensions];
	oph_odb_dimension_instance dim_inst[number_of_dimensions];
	char subarray_param[OPH_TP_TASKLEN];
	int explicited[number_of_dimensions];

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	char temp[max_size];

	for (i = 0; i < number_of_dimensions; ++i)
		explicited[i] = 0;

	for (d = 0; d < oper_handle->number_of_dim; ++d)
		dim_number[d] = 0;
	for (l = 0; l < number_of_dimensions; l++) {
		new_size[l] = cubedims[l].size;
		subsetted_dim[l] = -1;
	}

	oph_odb_db_instance db_;
	oph_odb_db_instance *db = &db_;
	if (oph_dim_load_dim_dbinstance(db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_LOAD);
		oph_dim_unload_dim_dbinstance(db);
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_CONNECT);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[1 + OPH_COMMON_BUFFER_LEN], operation2[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
	snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, oper_handle->id_input_container);
	snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, oper_handle->id_input_container);
	int n, compressed = 0, non_empty_set = 1;

	for (d = 0; d < oper_handle->number_of_dim; ++d)	// Loop on dimensions set as input
	{
		first_explicit = 1;
		for (l = number_of_dimensions - 1; l >= 0; l--) {
			if (cubedims[l].explicit_dim && first_explicit)
				dim_number[d] = first_explicit = 0;	// The considered dimension is explicit
			if (cubedims[l].size)
				dim_size[d][dim_number[d]++] = cubedims[l].size;
			if (cubedims[l].id_dimensioninst == oper_handle->id_dimension[d]) {
				if (!cubedims[l].size) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "This dimension has been completely reduced.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIMENSION_REDUCED);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				} else
					break;
			}
		}
		if (l < 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension informations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIMENSION_INFO_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		subsetted_dim[l] = d;
		explicited[d] = cubedims[l].explicit_dim;
		if (explicited[d] && !dim_number[d]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension sizes.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIMENSION_SIZE_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (explicited[d])
			explicit_dim_number++;
		else
			implicit_dim_number++;

		block_size = 1;
		for (i = 0; i < dim_number[d] - 1; ++i)
			block_size *= dim_size[d][i];
		size_string = &(size_string_vector[d][0]);
		snprintf(size_string, OPH_COMMON_BUFFER_LEN, "%lld,%lld", block_size, dim_size[d][dim_number[d] - 1]);

		if (!oper_handle->subset_type) {

			// Parsing indexes
			subset_struct[d] = 0;
			if (oph_subset_init(&subset_struct[d])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate subset struct.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT, "subset");
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (oph_subset_parse(oper_handle->task[d], strlen(oper_handle->task[d]), subset_struct[d], dim_size[d][dim_number[d] - 1])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot parse subset string '%s'. Select values in [1,%lld].\n", oper_handle->task[d], dim_size[d][dim_number[d] - 1]);
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_PARSE_ERROR, oper_handle->task[d], dim_size[d][dim_number[d] - 1]);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (oph_subset_size(subset_struct[d], dim_size[d][dim_number[d] - 1], &new_size[l], 0, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot evaluate subset size.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_SIZE_ERROR);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		// Dimension extraction
		if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
			oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_odb_dim_retrieve_dimension2(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
			oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oper_handle->subset_type) {

			if (compressed) {
				n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "uncompress(%s)", MYSQL_DIMENSION);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			} else {
				strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);
				operation[OPH_COMMON_BUFFER_LEN] = 0;
			}
			dim_row = 0;
			if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, 0, &dim_row) || !dim_row) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
				if (dim_row)
					free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (dim_inst[l].fk_id_dimension_label) {
				if (oph_dim_read_dimension_filtered_data
				    (db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, operation, 0, &dim_row, dim[l].dimension_type, dim_inst[l].size)
				    || !dim_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			} else
				strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);	// A reduced dimension is handled by indexes

			// Pre-parsing for time dimensions
			if (oper_handle->time_filter && dim[l].calendar && strlen(dim[l].calendar)) {
				if ((n = oph_dim_parse_season_subset(oper_handle->task[d], &(dim[l]), temp, dim_row, dim_size[d][dim_number[d] - 1]))) {
					if (n != OPH_DIM_TIME_PARSING_ERROR) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_PARSE_ERROR, oper_handle->task[d]);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				} else {
					free(oper_handle->task[d]);
					oper_handle->task[d] = strndup(temp, OPH_TP_TASKLEN);
				}
				if ((n = oph_dim_parse_time_subset(oper_handle->task[d], &(dim[l]), temp))) {
					if (n != OPH_DIM_TIME_PARSING_ERROR) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_PARSE_ERROR, oper_handle->task[d]);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				} else {
					free(oper_handle->task[d]);
					oper_handle->task[d] = strndup(temp, OPH_TP_TASKLEN);
				}
			}
			// Real parsing
			subset_struct[d] = 0;
			if (oph_subset_value_to_index(oper_handle->task[d], dim_row, dim_size[d][dim_number[d] - 1], dim[l].dimension_type, 0, temp, &subset_struct[d])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot convert the subset '%s' into a subset expressed as indexes.\n", oper_handle->task[d]);
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_PARSE_ERROR, oper_handle->task[d]);
				if (dim_row)
					free(dim_row);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (dim_row)
				free(dim_row);

			if (non_empty_set && !strlen(temp))
				non_empty_set = 0;

			if (oper_handle->task[d]) {
				free((char *) oper_handle->task[d]);
				oper_handle->task[d] = NULL;
			}
			if (!(oper_handle->task[d] = (char *) strndup(temp, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_STRUCT, "task");
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (strlen(temp) && oph_subset_size(subset_struct[d], dim_size[d][dim_number[d] - 1], &new_size[l], 0, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot evaluate subset size.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_SIZE_ERROR);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

		}

		if (oper_handle->task[d] && strlen(oper_handle->task[d])) {
			snprintf(subarray_param, OPH_TP_TASKLEN, "'%s',1,%d", oper_handle->task[d], dim_inst[l].size);
			if (!(oper_handle->dim_task[d] = (char *) strndup(subarray_param, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_STRUCT, "task");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (explicited[d]) {
				// WHERE clause building
				char where_clause[OPH_TP_TASKLEN];
				*where_clause = '\0';
				for (i = 0; i < (int) subset_struct[d]->number; ++i)	// loop on subsets
				{
					if (i)
						strcat(where_clause, " OR ");
					snprintf(temp, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_ISINSUBSET_PLUGIN, MYSQL_FRAG_ID, size_string, subset_struct[d]->start[i], subset_struct[d]->stride[i],
						 subset_struct[d]->end[i]);
					strncat(where_clause, temp, OPH_TP_TASKLEN - strlen(where_clause) - 1);
				}
				if (oper_handle->task[d]) {
					free((char *) oper_handle->task[d]);
					oper_handle->task[d] = NULL;
				}
				if (!(oper_handle->task[d] = (char *) strndup(where_clause, OPH_TP_TASKLEN))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_STRUCT, "task");
					oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			} else {
				snprintf(subarray_param, OPH_TP_TASKLEN, "'%s',%s", oper_handle->task[d], size_string);
				if (oper_handle->task[d]) {
					free((char *) oper_handle->task[d]);
					oper_handle->task[d] = NULL;
				}
				if (!(oper_handle->task[d] = (char *) strndup(subarray_param, OPH_TP_TASKLEN))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_STRUCT, "task");
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
		}
	}

	int total_number_of_rows = 0, number_of_rows = 0;

	for (l = 0; l < number_of_dimensions; ++l) {
		if (subsetted_dim[l] < 0) {
			if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (oph_odb_dim_retrieve_dimension2(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		if (cubedims[l].explicit_dim && new_size[l]) {
			if (!total_number_of_rows)
				total_number_of_rows = 1;
			total_number_of_rows *= new_size[l];
		}
	}
	if (!total_number_of_rows)
		total_number_of_rows = 1;	// APEX

	//Copy fragment id relative index set
	char *fragment_ids;
	if (!(fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_RETREIVE_IDS_ERROR);
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int *keys = 0;
	int frags_size = 0;
	if (explicit_dim_number && non_empty_set) {
		// Fragment keys
		oph_odb_fragment_list2 frags;
		if (oph_odb_stge_retrieve_fragment_list2(oDB, datacube_id, &frags)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve fragment keys\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_KEY_ERROR);
			oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
			oph_odb_cube_free_datacube(&cube);
			free(fragment_ids);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (!frags.size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "OphidiaDB parameters are corrupted - check 'fragment' table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_BAD_PARAMETER);
			oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
			oph_odb_cube_free_datacube(&cube);
			free(fragment_ids);
			free(cubedims);
			oph_odb_stge_free_fragment_list2(&frags);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		frags_size = frags.size;
		keys = (int *) malloc(2 * frags.size * sizeof(int));

		int id = 1, is_in_subset, max = 0, offset;
		unsigned long long block_size[oper_handle->number_of_dim], min_block_size = 0;
		for (d = 0; d < oper_handle->number_of_dim; ++d) {
			block_size[d] = 1;
			if (explicited[d]) {
				for (i = 0; i < dim_number[d] - 1; i++)
					block_size[d] *= dim_size[d][i];
				if (!min_block_size || (min_block_size > block_size[d]))
					min_block_size = block_size[d];
			}
		}
		for (i = 0; i < frags.size; i++) {
			keys[i] = keys[i + frags.size] = 0;
			if (frags.value[i].frag_relative_index != i + 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Value of fragmentrelativeindex is not expected\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_RETREIVE_FRAGMENT_ID_ERROR);
				oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(fragment_ids);
				free(cubedims);
				oph_odb_stge_free_fragment_list2(&frags);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				if (keys)
					free(keys);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		for (i = 0; i < frags.size;) {
			if (id <= frags.value[i].key_end) {
				is_in_subset = 0;
				for (d = 0; d < oper_handle->number_of_dim; ++d)
					if (explicited[d]) {
						is_in_subset = oph_subset_index_is_in_subset(oph_subset_id_to_index(id, &(dim_size[d][0]), dim_number[d]), subset_struct[d]);
						if (!is_in_subset)
							break;
					}
				if (is_in_subset) {
					if (!keys[i]) {
						keys[i] = max;	// key_end
						keys[i + frags.size] = keys[i] + 1;	// key_start
					}
					max = keys[i] += (int) min_block_size;
					id += (int) min_block_size;
				} else
					id += (int) block_size[d];
				while (id > frags.value[i].key_end) {
					if (is_in_subset) {
						offset = id - frags.value[i].key_end - 1;
						max = keys[i] -= offset;
					}
					i++;
					if ((i < frags.size) && is_in_subset) {
						keys[i] = max;	// key_end
						keys[i + frags.size] = keys[i] + 1;	// key_start
						max = keys[i] += offset;
					} else
						break;
				}
			} else
				i++;
		}
		//Check if key_start and key_end are correct. If not set to zero
		for (i = 0; i < frags.size; i++) {
			if (keys[i] < keys[i + frags.size]) {
				keys[i] = keys[i + frags.size] = 0;
			}
		}

		// WHERE clause
		char where_clause[OPH_TP_TASKLEN];
		*where_clause = '\0';
		i = 0;
		for (d = 0; d < oper_handle->number_of_dim; ++d)	// loop on dimensions
		{
			if (explicited[d]) {
				if (i)
					strcat(where_clause, " AND ");
				else
					i = 1;
				strcat(where_clause, "(");
				strncat(where_clause, oper_handle->task[d], OPH_TP_TASKLEN - strlen(where_clause) - 1);
				strcat(where_clause, ")");
			}
		}
		if (!(oper_handle->where_clause = (char *) strndup(where_clause, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "WHERE clause");
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			free(fragment_ids);
			oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);
			oph_odb_stge_free_fragment_list2(&frags);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			if (keys)
				free(keys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		oph_odb_stge_free_fragment_list2(&frags);
	}
	oph_subset_vector_free(subset_struct, oper_handle->number_of_dim);

	if (implicit_dim_number && non_empty_set) {
		int first = 1;
		char buffer[OPH_TP_TASKLEN];
		*buffer = '\0';
		for (d = 0; d < oper_handle->number_of_dim; ++d)	// loop on dimensions
		{
			if (!explicited[d]) {
				if (first)
					first = 0;
				else
					strncat(buffer, ",", OPH_TP_TASKLEN - strlen(buffer) - 1);
				strncat(buffer, oper_handle->task[d], OPH_TP_TASKLEN - strlen(buffer) - 1);
			}
		}
		if (!(oper_handle->apply_clause = (char *) strndup(buffer, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "APPLY clause");
			oph_odb_cube_free_datacube(&cube);
			free(fragment_ids);
			free(cubedims);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			if (keys)
				free(keys);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	int dimension_index_set = 0;
	char dimension_index[OPH_COMMON_BUFFER_LEN];
	int j, first = 1;
	int explicit_dim_total_number = 0;
	char *dim_rows[number_of_dimensions];
	char *dim_rows_index[number_of_dimensions];
	char *sdim_rows_index[number_of_dimensions];
	memset(dim_rows, 0, sizeof(char *) * number_of_dimensions);
	memset(dim_rows_index, 0, sizeof(char *) * number_of_dimensions);
	memset(sdim_rows_index, 0, sizeof(char *) * number_of_dimensions);

	if (non_empty_set) {
		// Begin - Load dimension table management (only explicit dimensions are preloaded if level=1; dimension are read without considering any previous subsetting)
		n = 0;
		explicit_dim_number = 0;
		//Write ID to index dimension string
		for (l = 0; l < number_of_dimensions; l++) {
			if (!cubedims[l].explicit_dim)
				continue;
			explicit_dim_total_number++;
			if (!cubedims[l].size)
				continue;
			if (!first)
				n += sprintf(dimension_index + n, "|");
			n += sprintf(dimension_index + n, "oph_id_to_index(%s", MYSQL_FRAG_ID);
			first = 0;
			for (j = (number_of_dimensions - 1); j >= l; j--) {
				if (!cubedims[j].size)
					continue;
				if (cubedims[j].explicit_dim == 0)
					continue;
				n += sprintf(dimension_index + n, ",%d", cubedims[j].size);
			}
			n += sprintf(dimension_index + n, ")");
			dimension_index_set = 1;
			explicit_dim_number++;
		}

		int iiii = oper_handle->level > 1 ? number_of_dimensions : explicit_dim_total_number;
		for (l = 0; l < iiii; ++l) {
			if (!dim_inst[l].fk_id_dimension_index || !dim_inst[l].size)
				continue;
			if (dim_inst[l].fk_id_dimension_label)	// Dimension not (partially) reduced
			{
				if (oph_dim_read_dimension_data(db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, MYSQL_DIMENSION, 0, &dim_rows[l])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_rows_index[l])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			} else	// Dimension partially reduced
			{
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_rows_index[l])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
				dim[l].dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
				//Since index is always long we can assure that lenght is size*SIZEOF(long)
				dim_rows[l] = (char *) malloc(dim_inst[l].size * sizeof(long long));
				if (!dim_rows[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "dim_rows");
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				memcpy(dim_rows[l], dim_rows_index[l], dim_inst[l].size * sizeof(long long));
			}

			if ((d = subsetted_dim[l]) >= 0) {
				if (compressed)
					n = snprintf(operation2, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN_COMPR, OPH_DIM_INDEX_DATA_TYPE, OPH_DIM_INDEX_DATA_TYPE, MYSQL_DIMENSION,
						     oper_handle->dim_task[d]);
				else
					n = snprintf(operation2, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN, OPH_DIM_INDEX_DATA_TYPE, OPH_DIM_INDEX_DATA_TYPE, MYSQL_DIMENSION,
						     oper_handle->dim_task[d]);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation2, 0, &sdim_rows_index[l])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					free(fragment_ids);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					for (j = 0; j <= l; ++j) {
						if (dim_rows[j])
							free(dim_rows[j]);
						if (dim_rows_index[j])
							free(dim_rows_index[j]);
						if (sdim_rows_index[j])
							free(sdim_rows_index[j]);
					}
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			}
		}
		// End - Load dimension table
	}

	int ii, jj, k, kk = 0;
	int id_datacube_in = oper_handle->id_input_datacube;
	compressed = cube.compressed;
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	long long index_dim = 0;

	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(oDB, id_datacube_in, fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_CONNECTION_STRINGS_NOT_FOUND);
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		oph_odb_cube_free_datacube(&cube);
		free(fragment_ids);
		free(cubedims);
		for (l = 0; l < number_of_dimensions; ++l) {
			if (dim_rows[l])
				free(dim_rows[l]);
			if (dim_rows_index[l])
				free(dim_rows_index[l]);
			if (sdim_rows_index[l])
				free(sdim_rows_index[l]);
		}
		if (keys)
			free(keys);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(fragment_ids);

	int result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0, index = 0, empty = 1, first_frag = 1, num_rows = 0;

	oph_ioserver_result *frag_rows = NULL;
	oph_ioserver_row *curr_row = NULL;
	int sql_num_fields, num_fields = 0;

	long long total_length = 2;
	int id_container = oper_handle->id_input_container, additional_space = OPH_EXPLORECUBE_TABLE_SPACING;

	double *dim_d = NULL;
	float *dim_f = NULL;
	int *dim_i = NULL;
	long long *dim_l = NULL;
	short *dim_s = NULL;
	char *dim_b = NULL;

	char *table_row = 0;
	int no_limit = (oper_handle->base64 && !(oper_handle->limit) ? 1 : 0);
	int current_limit = oper_handle->limit;
	unsigned long long offset;

	int is_objkey_printable = 0, iii, jjj = 0;
	char jsontmp[OPH_COMMON_BUFFER_LEN];
	char **jsonkeys = NULL;
	char **fieldtypes = NULL;
	char **jsonvalues = NULL;
	char tmp_value[OPH_COMMON_BUFFER_LEN];
	*tmp_value = 0;

	if (non_empty_set) {

		if (oph_dc_setup_dbms(&(oper_handle->server), (dbmss.value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DBMS
		for (i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (no_limit || (current_limit > 0)); i++) {
			if (oph_dc_connect_to_dbms(oper_handle->server, &(dbmss.value[i]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//For each DB
			for (j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (no_limit || (current_limit > 0)); j++) {
				//Check DB - DBMS Association
				if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
					continue;

				if (oph_dc_use_db_of_dbms(oper_handle->server, &(dbmss.value[i]), &(dbs.value[j]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_DB_SELECTION_ERROR, (dbs.value[j]).db_name);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//For each fragment
				for (k = 0; (k < frags.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (no_limit || (current_limit > 0)); k++) {
					//Check Fragment - DB Association
					if (frags.value[k].db_instance != &(dbs.value[j]))
						continue;

					if (frags_size) {
						index = frags.value[k].frag_relative_index - 1;
						frags.value[k].key_start = keys[index + frags_size];
					}

					if (oper_handle->apply_clause && strlen(oper_handle->apply_clause)) {
						if (compressed)
							n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN_COMPR2, cube.measure_type, cube.measure_type, cube.measure_type,
								     MYSQL_FRAG_MEASURE, oper_handle->apply_clause, oper_handle->base64 ? OPH_EXPLORECUBE_BASE64 : OPH_EXPLORECUBE_DECIMAL);
						else
							n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN2, cube.measure_type, cube.measure_type, cube.measure_type,
								     MYSQL_FRAG_MEASURE, oper_handle->apply_clause, oper_handle->base64 ? OPH_EXPLORECUBE_BASE64 : OPH_EXPLORECUBE_DECIMAL);
					} else {
						if (compressed)
							n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN_COMPR3, cube.measure_type, MYSQL_FRAG_MEASURE,
								     oper_handle->base64 ? OPH_EXPLORECUBE_BASE64 : OPH_EXPLORECUBE_DECIMAL);
						else
							n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_EXPLORECUBE_PLUGIN3, cube.measure_type, MYSQL_FRAG_MEASURE,
								     oper_handle->base64 ? OPH_EXPLORECUBE_BASE64 : OPH_EXPLORECUBE_DECIMAL);
					}
					if (n >= OPH_COMMON_BUFFER_LEN) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container,
							OPH_LOG_OPH_EXPLORECUBE_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					//EXPLORECUBE fragment
					if (frags.value[k].key_start) {
						if (oph_dc_read_fragment_data
						    (oper_handle->server, &(frags.value[k]), cube.measure_type, compressed,
						     dimension_index_set ? dimension_index : 0, operation, oper_handle->where_clause, current_limit, 1, &frag_rows)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment %s.\n", frags.value[k].fragment_name);
							logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container,
								OPH_LOG_OPH_EXPLORECUBE_READ_FRAGMENT_ERROR, frags.value[k].fragment_name);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							oph_ioserver_free_result(oper_handle->server, frag_rows);
							break;
						}
						if ((num_rows = frag_rows->num_rows) < 1) {
							frag_count++;
							oph_ioserver_free_result(oper_handle->server, frag_rows);
							continue;
						} else
							empty = 0;

						if ((sql_num_fields = frag_rows->num_fields) != explicit_dim_number + 2) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
							logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_MISSING_FIELDS);
							result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							oph_ioserver_free_result(oper_handle->server, frag_rows);
							break;
						}
						//Read field maximum length and total row max length
						jj = 0;
						for (ii = 0; ii < sql_num_fields; ii++) {
							if (ii) {
								while (jj < number_of_dimensions) {
									if (!cubedims[jj].size) {
										jj++;
										continue;
									}
									if (oper_handle->show_index)
										additional_space = 2 * OPH_EXPLORECUBE_TABLE_SPACING;
									else
										additional_space = OPH_EXPLORECUBE_TABLE_SPACING;
									if (frag_rows->max_field_length[ii] < strlen(dim[jj].dimension_name) + additional_space)
										frag_rows->max_field_length[ii] = strlen(dim[jj].dimension_name) + additional_space;
									jj++;
									break;
								}
							} else if (frag_rows->max_field_length[ii] < strlen(OPH_EXPLORECUBE_ID))
								frag_rows->max_field_length[ii] = strlen(OPH_EXPLORECUBE_ID);
							if (first_frag)
								total_length += frag_rows->max_field_length[ii] + 3;
						}

						if (first_frag) {
							first_frag = 0;

							for (ii = 0; ii < number_of_dimensions; ii++)	// Full reduced explicit dimensions
							{
								if (!cubedims[ii].explicit_dim)
									break;
								if (cubedims[ii].size)
									continue;
								total_length += strlen(OPH_COMMON_FULL_REDUCED_DIM) + 3;
							}

							table_row = (char *) malloc((total_length + 1) * sizeof(char));
							n = 0;
							for (ii = 0; ii <= explicit_dim_total_number + 1; ii++) {
								if (!(oper_handle->show_id) && !ii)
									continue;
								n += snprintf(table_row + n, 2, "+");
								if (ii <= explicit_dim_number) {
									for (jj = 0; jj < (int) frag_rows->max_field_length[ii] + 2; jj++)
										n += snprintf(table_row + n, 2, "-");
								} else if (ii == explicit_dim_total_number + 1) {
									for (jj = 0; jj < (int) frag_rows->max_field_length[sql_num_fields - 1] + 2; jj++)
										n += snprintf(table_row + n, 2, "-");
								} else {
									for (jj = 0; jj < (int) strlen(OPH_COMMON_FULL_REDUCED_DIM) + 2; jj++)
										n += snprintf(table_row + n, 2, "-");
								}
							}
							n += snprintf(table_row + n, 3, "+\n");

							//Print column headers
							int ed = 0, fr_ed = 0;
							char *dimension_name[number_of_dimensions];
							char *dimension_type[number_of_dimensions];
							char dimension_time[number_of_dimensions];
							jj = 0;
							printf("%s", table_row);
							if (oper_handle->show_id)
								printf("| %-*s", (int) frag_rows->max_field_length[jj++] + 1, OPH_EXPLORECUBE_ID);
							else
								jj++;

							for (ii = 0; ii < number_of_dimensions; ii++) {
								dimension_name[ii] = dimension_type[ii] = NULL;
								dimension_time[ii] = 0;
								if (!cubedims[ii].explicit_dim)
									break;
								if (!cubedims[ii].size)
									continue;
								if (oper_handle->show_index)
									printf("| %s%-*s", "(index) ", (int) (frag_rows->max_field_length[jj++] + 1 - strlen("(index) ")), dim[ii].dimension_name);
								else
									printf("| %-*s", (int) frag_rows->max_field_length[jj++] + 1, dim[ii].dimension_name);
								dimension_name[ed] = dim[ii].dimension_name;
								dimension_type[ed] = dim[ii].dimension_type;
								dimension_time[ed] = oper_handle->show_time && dim[ii].calendar && strlen(dim[ii].calendar);
								ed++;
							}
							for (ii = 0; ii < number_of_dimensions; ii++) {
								if (!cubedims[ii].explicit_dim)
									break;
								if (cubedims[ii].size)
									continue;
								printf("| %-*s", (int) strlen(OPH_COMMON_FULL_REDUCED_DIM) + 1, dim[ii].dimension_name);
								dimension_name[ed + fr_ed] = dim[ii].dimension_name;
								dimension_type[ed + fr_ed] = dim[ii].dimension_type;
								fr_ed++;
							}
							printf("| %-*s |\n", (int) frag_rows->max_field_length[explicit_dim_number + 1], cube.measure);
							printf("%s", table_row);

							is_objkey_printable = oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_DATA);
							if (is_objkey_printable) {
								num_fields = (oper_handle->show_id ? 1 : 0) + (oper_handle->show_index ? 2 : 1) * ed + fr_ed + 1;

								// Header
								jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
								if (!jsonkeys) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "keys");
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (oper_handle->show_id) {
									jsonkeys[jjj] = strdup(OPH_EXPLORECUBE_ID);
									if (!jsonkeys[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
										for (iii = 0; iii < jjj; iii++)
											if (jsonkeys[iii])
												free(jsonkeys[iii]);
										if (jsonkeys)
											free(jsonkeys);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
								}
								for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
									jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
									if (!jsonkeys[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
										for (iii = 0; iii < jjj; iii++)
											if (jsonkeys[iii])
												free(jsonkeys[iii]);
										if (jsonkeys)
											free(jsonkeys);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
									if (oper_handle->show_index) {
										snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
										jsonkeys[jjj] = strdup(jsontmp);
										if (!jsonkeys[jjj]) {
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
												"key");
											for (iii = 0; iii < jjj; iii++)
												if (jsonkeys[iii])
													free(jsonkeys[iii]);
											if (jsonkeys)
												free(jsonkeys);
											result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
											break;
										}
										jjj++;
									}
								}
								if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
									break;
								for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
									jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
									if (!jsonkeys[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
										for (iii = 0; iii < jjj; iii++)
											if (jsonkeys[iii])
												free(jsonkeys[iii]);
										if (jsonkeys)
											free(jsonkeys);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
								}
								if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
									break;
								jsonkeys[jjj] = strdup(cube.measure ? cube.measure : "");
								if (!jsonkeys[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
									for (iii = 0; iii < jjj; iii++)
										if (jsonkeys[iii])
											free(jsonkeys[iii]);
									if (jsonkeys)
										free(jsonkeys);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}

								jjj = 0;
								fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
								if (!fieldtypes) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtypes");
									for (iii = 0; iii < num_fields; iii++)
										if (jsonkeys[iii])
											free(jsonkeys[iii]);
									if (jsonkeys)
										free(jsonkeys);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (oper_handle->show_id) {
									fieldtypes[jjj] = strdup(OPH_JSON_LONG);
									if (!fieldtypes[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
											"fieldtype");
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
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
								}
								for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
									if (dimension_time[iii])
										fieldtypes[jjj] = strdup(OPH_JSON_STRING);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_INT);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_LONG);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
									else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
										fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
									else
										fieldtypes[jjj] = 0;
									if (!fieldtypes[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
											"fieldtype");
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
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
									if (oper_handle->show_index) {
										fieldtypes[jjj] = strdup(OPH_JSON_LONG);
										if (!fieldtypes[jjj]) {
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
												"fieldtype");
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
											result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
											break;
										}
										jjj++;
									}
								}
								if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
									break;
								for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
									fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
									if (!fieldtypes[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
											"fieldtype");
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
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
								}
								if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
									break;
								if (!strncasecmp(cube.measure_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_INT);
								else if (!strncasecmp(cube.measure_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_LONG);
								else if (!strncasecmp(cube.measure_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
								else if (!strncasecmp(cube.measure_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
								else if (!strncasecmp(cube.measure_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
								else if (!strncasecmp(cube.measure_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
									fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
								else
									fieldtypes[jjj] = strdup(OPH_JSON_STRING);
								if (!fieldtypes[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}

								if (oph_json_add_grid
								    (handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DATA, cube.measure, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
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
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
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

						}

						dim_d = NULL;
						dim_f = NULL;
						dim_i = NULL;
						dim_l = NULL;
						dim_s = NULL;
						dim_b = NULL;

						if (oph_ioserver_fetch_row(oper_handle->server, frag_rows, &curr_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_IOPLUGIN_FETCH_ROW_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							break;
						}

						while ((curr_row->row) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS)) {
							if (is_objkey_printable) {
								jsonvalues = (char **) calloc(num_fields, sizeof(char *));
								if (!jsonvalues) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "values");
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj = 0;
							}

							jj = 0;
							for (ii = 0; (ii < explicit_dim_total_number + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); kk = ii++) {
								if (ii) {
									if (curr_row->row[jj])
										offset = strtoll(curr_row->row[jj], NULL, 10) - 1;
									else
										offset = 0;
									if (!dim_rows[kk] || !dim_inst[kk].size)
										continue;
									else {
										index_dim = *((long long *) (dim_rows_index[kk] + offset * sizeof(long long))) - 1;
										if (oper_handle->show_time && dim[kk].calendar && strlen(dim[kk].calendar)) {
											if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dim[kk]), jsontmp)) {
												pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
												logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
												result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
												break;
											}
											if (oper_handle->show_index)
												printf("| (%s) %-*s", curr_row->row[jj], (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), jsontmp);	// Traditional print does not work correctly!!!
											else
												printf("| %-*s", (int) frag_rows->max_field_length[jj] + 1, jsontmp);	// Traditional print does not work correctly!!!
											if (is_objkey_printable)
												snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
											if (oper_handle->show_index)
												printf("| (%s) %-*d", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_i);
											else
												printf("| %-*d", (int) frag_rows->max_field_length[jj] + 1, *dim_i);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_i, sizeof(int), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
											}
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
											if (oper_handle->show_index)
												printf("| (%s) %-*lld", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_l);
											else
												printf("| %-*lld", (int) frag_rows->max_field_length[jj] + 1, *dim_l);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_l, sizeof(long long), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
											}
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
											if (oper_handle->show_index)
												printf("| (%s) %-*d", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_s);
											else
												printf("| %-*d", (int) frag_rows->max_field_length[jj] + 1, *dim_s);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_s, sizeof(short), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
											}
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
											if (oper_handle->show_index)
												printf("| (%s) %-*d", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_b);
											else
												printf("| %-*d", (int) frag_rows->max_field_length[jj] + 1, *dim_b);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_b, sizeof(char), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
											}
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
											if (oper_handle->show_index)
												printf("| (%s) %-*f", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_f);
											else
												printf("| %-*f", (int) frag_rows->max_field_length[jj] + 1, *dim_f);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_f, sizeof(float), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
											}
										} else if (!strncasecmp(dim[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
											dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
											if (oper_handle->show_index)
												printf("| (%s) %-*f", curr_row->row[jj],
												       (int) (frag_rows->max_field_length[jj] + 1 - (strlen(curr_row->row[jj]) + 3)), *dim_d);
											else
												printf("| %-*f", (int) frag_rows->max_field_length[jj] + 1, *dim_d);
											if (is_objkey_printable) {
												if (oper_handle->base64) {
													memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
													oph_utl_base64encode(dim_d, sizeof(double), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
												} else
													snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
											}
										} else {
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_TYPE_NOT_SUPPORTED,
												dim[kk].dimension_type);
											result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
											break;
										}
									}

									if (is_objkey_printable) {
										jsonvalues[jjj] = strdup(tmp_value);
										if (!jsonvalues[jjj]) {
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
												"value");
											for (iii = 0; iii < jjj; iii++)
												if (jsonvalues[iii])
													free(jsonvalues[iii]);
											if (jsonvalues)
												free(jsonvalues);
											result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
											break;
										}
										jjj++;
										if (oper_handle->show_index) {
											jsonvalues[jjj] = strdup(curr_row->row[jj] ? curr_row->row[jj] : "");
											if (!jsonvalues[jjj]) {
												pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
												logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID,
													OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
												for (iii = 0; iii < jjj; iii++)
													if (jsonvalues[iii])
														free(jsonvalues[iii]);
												if (jsonvalues)
													free(jsonvalues);
												result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
												break;
											}
											jjj++;
										}
									}
								} else if (oper_handle->show_id) {
									printf("| %-*s", (int) frag_rows->max_field_length[jj] + 1, curr_row->row[jj]);
									if (is_objkey_printable) {
										jsonvalues[jjj] = strdup(curr_row->row[jj] ? curr_row->row[jj] : "");
										if (!jsonvalues[jjj]) {
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT,
												"value");
											for (iii = 0; iii < jjj; iii++)
												if (jsonvalues[iii])
													free(jsonvalues[iii]);
											if (jsonvalues)
												free(jsonvalues);
											result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
											break;
										}
										jjj++;
									}
								}
								jj++;
							}
							for (ii = 0; (ii < explicit_dim_total_number - explicit_dim_number) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
								printf("| %-*s", (int) strlen(OPH_COMMON_FULL_REDUCED_DIM) + 1, OPH_COMMON_FULL_REDUCED_DIM);
								if (is_objkey_printable) {
									jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
									if (!jsonvalues[jjj]) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
										for (iii = 0; iii < jjj; iii++)
											if (jsonvalues[iii])
												free(jsonvalues[iii]);
										if (jsonvalues)
											free(jsonvalues);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									jjj++;
								}
							}
							if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
								break;
							printf("| %-*s |\n", (int) frag_rows->max_field_length[explicit_dim_number + 1],
							       curr_row->row[explicit_dim_number + 1] ? curr_row->row[explicit_dim_number + 1] : "NULL");
							if (is_objkey_printable) {
								jsonvalues[jjj] = strdup(curr_row->row[explicit_dim_number + 1] ? curr_row->row[explicit_dim_number + 1] : "");
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DATA, jsonvalues)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
									for (iii = 0; iii < num_fields; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (iii = 0; iii < num_fields; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
							}
							number_of_rows++;

							if (oph_ioserver_fetch_row(oper_handle->server, frag_rows, &curr_row)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_IOPLUGIN_FETCH_ROW_ERROR);
								result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
								break;
							}
						}

						oph_ioserver_free_result(oper_handle->server, frag_rows);
						frag_count++;

						if (oper_handle->limit)
							current_limit -= num_rows;
					}
				}
			}
			oph_dc_disconnect_from_dbms(oper_handle->server, &(dbmss.value[i]));
		}

		if (oph_dc_cleanup_dbms(oper_handle->server)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_EXPLORECUBE_IOPLUGIN_CLEANUP_ERROR, (dbmss.value[0]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);

	oph_odb_cube_free_datacube(&cube);

	if (keys)
		free(keys);

	if (table_row && !result) {
		printf("%s", table_row);
		free(table_row);
		if (number_of_rows > total_number_of_rows)
			number_of_rows = total_number_of_rows;
		char message[OPH_COMMON_BUFFER_LEN];
		snprintf(message, OPH_COMMON_BUFFER_LEN, "Selected %d row%s out of %d", number_of_rows, number_of_rows == 1 ? "" : "s", total_number_of_rows);
		printf("%s\n", message);
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_SUMMARY)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_SUMMARY, "Summary", message)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}
	if (empty && !result) {
		char message[OPH_COMMON_BUFFER_LEN];
		snprintf(message, OPH_COMMON_BUFFER_LEN, "Empty set");
		printf("%s\n", message);
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_SUMMARY)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_SUMMARY, "Summary", message)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}

	if (!result && (oper_handle->level > 1)) {
		is_objkey_printable = oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_DIMVALUES);
		if (is_objkey_printable) {
			num_fields = oper_handle->show_index ? 2 : 1;
			int success = 1;
			char *dim_rows_, *dim_rows_index_;

			for (l = 0; l < number_of_dimensions; l++) {
				if (!dim_rows[l] || !dim_inst[l].size)
					continue;	// Don't show fully reduced dimensions

				dim_rows_index_ = subsetted_dim[l] >= 0 ? sdim_rows_index[l] : dim_rows_index[l];
				dim_rows_ = dim_inst[l].fk_id_dimension_label ? dim_rows[l] : dim_rows_index_;

				success = 0;
				while (!success) {
					// Header
					jjj = 0;
					jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
					if (!jsonkeys) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "keys");
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oper_handle->show_index) {
						jsonkeys[jjj] = strdup("INDEX");
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					jsonkeys[jjj] = strdup("VALUE");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}

					jjj = 0;
					fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
					if (!fieldtypes) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtypes");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oper_handle->show_index) {
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (oper_handle->show_time && dim[l].calendar && strlen(dim[l].calendar))
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_INT);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
					else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					else
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}

					if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DIMVALUES, dim[l].dimension_name, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
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
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
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

					success = 1;
				}
				if (!success)
					break;

				for (ii = 0; ii < (int) new_size[l]; ii++) {
					success = 0;
					while (!success) {
						jjj = 0;
						jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "values");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						if (oper_handle->show_index) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", ii + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}

						index_dim = *((long long *) (dim_rows_index_ + ii * sizeof(long long))) - 1;
						if (oper_handle->show_time && dim[l].calendar && strlen(dim[l].calendar)) {
							if (oph_dim_get_time_string_of(dim_rows_, index_dim, &(dim[l]), tmp_value)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_EXPLORECUBE_DIM_READ_ERROR);
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_i = (int *) (dim_rows_ + index_dim * sizeof(int));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_i, sizeof(int), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_l = (long long *) (dim_rows_ + index_dim * sizeof(long long));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_l, sizeof(long long), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);

						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_s = (short *) (dim_rows_ + index_dim * sizeof(short));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_s, sizeof(short), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);

						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_b = (char *) (dim_rows_ + index_dim * sizeof(char));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_b, sizeof(char), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);

						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_f = (float *) (dim_rows_ + index_dim * sizeof(float));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_f, sizeof(float), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);

						} else if (!strncasecmp(dim[l].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
							dim_d = (double *) (dim_rows_ + index_dim * sizeof(double));
							if (oper_handle->base64) {
								memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
								oph_utl_base64encode(dim_d, sizeof(double), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
							} else
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);

						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_TYPE_NOT_SUPPORTED, dim[l].dimension_type);
							result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							break;
						}
						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DIMVALUES, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);

						success = 1;
					}
					if (!success)
						break;
				}
				if (!success)
					break;
			}
		}
	}

	for (l = 0; l < number_of_dimensions; ++l) {
		if (dim_rows[l])
			free(dim_rows[l]);
		if (dim_rows_index[l])
			free(dim_rows_index[l]);
		if (sdim_rows_index[l])
			free(sdim_rows_index[l]);
	}

	oph_dim_disconnect_from_dbms(db->dbms_instance);
	oph_dim_unload_dim_dbinstance(db);

	if (!result && oper_handle->export_metadata) {
		is_objkey_printable = oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_DIMINFO);
		if (is_objkey_printable) {
			num_fields = 8;
			int success = 0;

			while (!success) {
				// Header
				jjj = 0;
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jsonkeys[jjj] = strdup("NAME");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("TYPE");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("SIZE");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("HIERARCHY");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("CONCEPT LEVEL");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("ARRAY");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("LEVEL");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("LATTICE NAME");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//jjj++;

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_INT);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_INT);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//jjj++;

				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DIMINFO, "Dimension Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
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

				success = 1;
			}

			if (success) {

				oph_odb_hierarchy hier;
				char filename[2 * OPH_TP_BUFLEN], *concept_level_long = NULL;
				for (l = 0; l < number_of_dimensions; l++) {

					if (concept_level_long)
						free(concept_level_long);
					concept_level_long = NULL;

					if (oph_odb_dim_retrieve_hierarchy(oDB, dim[l].id_hierarchy, &hier)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
					if (oph_hier_get_concept_level_long(filename, dim_inst[l].concept_level, &concept_level_long)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}

					oph_odb_dimension_grid dim_grid;
					if (dim_inst[l].id_grid && oph_odb_dim_retrieve_grid(oDB, dim_inst[l].id_grid, &dim_grid)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}

					success = 0;
					while (!success) {

						jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "values");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						jjj = 0;
						jsonvalues[jjj] = strdup(dim[l].dimension_name ? dim[l].dimension_name : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(dim[l].dimension_type ? dim[l].dimension_type : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", dim_inst[l].size);
						jsonvalues[jjj] = strdup(jsontmp);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(hier.hierarchy_name ? hier.hierarchy_name : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(concept_level_long ? concept_level_long : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(cubedims[l].explicit_dim ? OPH_COMMON_YES_VALUE : OPH_COMMON_NO_VALUE);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cubedims[l].level);
						jsonvalues[jjj] = strdup(jsontmp);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(dim_inst[l].id_grid && dim_grid.grid_name ? dim_grid.grid_name : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						//jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_DIMINFO, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);

						success = 1;
					}
					if (!success)
						break;
				}
				if (concept_level_long)
					free(concept_level_long);
			}
		}
	}

	free(cubedims);

	if (!result && oper_handle->export_metadata) {
		is_objkey_printable = oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_EXPLORECUBE_METADATA);
		if (is_objkey_printable) {
			num_fields = 4;
			int success = 0;

			while (!success) {
				// Header
				jjj = 0;
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jsonkeys[jjj] = strdup("Variable");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("Key");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("Type");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("Value");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//jjj++;

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//jjj++;

				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_METADATA, "Metadata List", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
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

				success = 1;
			}

			if (success) {

				char **mvariable = NULL, **mkey = NULL, **mtype = NULL, **mvalue = NULL;
				MYSQL_RES *read_result = NULL;
				MYSQL_ROW row;
				if (oph_odb_meta_find_complete_metadata_list(oDB, datacube_id, NULL, 0, NULL, NULL, NULL, NULL, &read_result)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load metadata.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_READ_METADATA_ERROR);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				int nn = mysql_num_rows(read_result);
				if (nn) {
					mvariable = (char **) malloc(nn * sizeof(char *));
					mkey = (char **) malloc(nn * sizeof(char *));
					mtype = (char **) malloc(nn * sizeof(char *));
					mvalue = (char **) malloc(nn * sizeof(char *));
					l = 0;
					while ((row = mysql_fetch_row(read_result))) {
						mvariable[l] = row[1] ? strdup(row[1]) : NULL;
						mkey[l] = row[2] ? strdup(row[2]) : NULL;
						mtype[l] = row[3] ? strdup(row[3]) : NULL;
						mvalue[l] = row[4] ? strdup(row[4]) : NULL;
						l++;
					}
				}
				mysql_free_result(read_result);

				for (l = 0; l < nn; l++) {

					success = 0;
					while (!success) {

						jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "values");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						jjj = 0;
						jsonvalues[jjj] = strdup(mvariable[l] ? mvariable[l] : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(mkey[l] ? mkey[l] : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(mtype[l] ? mtype[l] : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(mvalue[l] ? mvalue[l] : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						//jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORECUBE_METADATA, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);

						success = 1;
					}
					if (!success)
						break;
				}

				oph_free_vector(mvariable, nn);
				oph_free_vector(mkey, nn);
				oph_free_vector(mtype, nn);
				oph_free_vector(mvalue, nn);
			}
		}
	}

	return result;
}

int task_reduce(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPLORECUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_disconnect_from_ophidiadb(&((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->oDB);
	}
	int i;
	for (i = 0; i < OPH_SUBSET_LIB_MAX_DIM; ++i) {
		if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->task[i]) {
			free((char *) ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->task[i]);
			((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->task[i] = NULL;
		}
		if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->dim_task[i]) {
			free((char *) ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->dim_task[i]);
			((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->dim_task[i] = NULL;
		}
	}
	if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->where_clause) {
		free((int *) ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->where_clause);
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->where_clause = NULL;
	}
	if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->apply_clause) {
		free((int *) ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->apply_clause);
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->apply_clause = NULL;
	}
	if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_EXPLORECUBE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
