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
#include <mpi.h>
#include <glob.h>
#include <sys/stat.h>

#include <math.h>
#include <ctype.h>

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_IMPORTNCS_operator.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_driver_procedure_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#ifdef OPH_OPENMP
#include <omp.h>
#else
#include <pthread.h>
#endif

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

	if (!(handle->operator_handle = (OPH_IMPORTNCS_operator_handle *) calloc(1, sizeof(OPH_IMPORTNCS_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->create_container = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_grid = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_compliance = 0;
	NETCDF_var *nc_measure = &(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure);
	nc_measure->dims_name = NULL;
	nc_measure->dims_id = NULL;
	nc_measure->dims_unlim = NULL;
	nc_measure->dims_length = NULL;
	nc_measure->dims_type = NULL;
	nc_measure->dims_oph_level = NULL;
	nc_measure->dims_start_index = NULL;
	nc_measure->dims_end_index = NULL;
	nc_measure->dims_concept_level = NULL;
	nc_measure->order_src_path = NULL;
	nc_measure->dim_unlim_array = NULL;
	nc_measure->base_time = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run = 1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_year = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_month = 2;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->memory_size = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->policy = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids = NULL;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));

	//3 - Fill struct with the correct data

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_POLICY);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_POLICY_PORT))
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->policy = 1;
	else if (strcmp(value, OPH_COMMON_POLICY_RR)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BASE_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BASE_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_UNITS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_UNITS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CALENDAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CALENDAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MONTH_LENGTHS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MONTH_LENGTHS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_YEAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_YEAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_YEAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_year = (int) strtol(value, NULL, 10);
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_MONTH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_MONTH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_MONTH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_month = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *input = hashtbl_get(task_tbl, OPH_IN_PARAM_INPUT);
	if (input && strlen(input))
		value = input;
	else if (!strlen(value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The param '%s' is mandatory; at least the param '%s' needs to be set\n", OPH_IN_PARAM_SRC_FILE_PATH, OPH_IN_PARAM_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *tmp_base_path = NULL;
	if (oph_pid_get_base_src_path(&tmp_base_path)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int i, j, retval;
	char *cdd = NULL;
	for (j = 0; j < ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num; ++j) {
		if (strstr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], "..")) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
			free(tmp_base_path);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!strstr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], "http://")
		    && !strstr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], "https://")) {
			char *pointer = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j];
			while (pointer && (*pointer == ' '))
				pointer++;
			if (pointer) {
				char tmp[OPH_COMMON_BUFFER_LEN];
				if (*pointer != '/') {
					if (!cdd) {
						cdd = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
						if (!cdd) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
							free(tmp_base_path);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						if (*cdd != '/') {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", cdd);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", cdd);
							free(tmp_base_path);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						if (strlen(cdd) > 1) {
							if (strstr(cdd, "..")) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
								free(tmp_base_path);
								return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							}
						}
					}
					if (strlen(cdd) > 1) {
						snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", cdd + 1, pointer);
						free(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j] = strdup(tmp);
						pointer = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j];
					}
				}
				snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", tmp_base_path ? tmp_base_path : "", *pointer != '/' ? "/" : "", pointer);
				free(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j] = strdup(tmp);
			}
		}
	}
	free(tmp_base_path);

	// Check for glob values
	size_t ll;
	glob_t globbuf;
	char *buffer = NULL;
	for (j = 0; j < ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num; ++j) {
		value = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j];
		if ((retval = glob(value, GLOB_MARK | GLOB_NOSORT, NULL, &globbuf))) {
			if (retval != GLOB_NOMATCH) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse '%s'\n", value);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to parse '%s'\n", value);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
		for (ll = 0; ll < globbuf.gl_pathc; ++ll) {
			if (buffer) {
				value = buffer;
				retval = asprintf(&buffer, "%s%c%s", value, OPH_TP_MULTI_VALUE_SEPARATOR, globbuf.gl_pathv[ll]);
				free(value);
			} else
				buffer = strdup(globbuf.gl_pathv[ll]);
		}
		globfree(&globbuf);
	}
	if (!buffer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths) {
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths,
						      ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths = NULL;
	}
	if (oph_tp_parse_multiple_value_param
	    (buffer, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->create_container = 1;
		char *pointer = strrchr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[0], '/');
		while (pointer && !strlen(pointer)) {
			*pointer = 0;
			pointer = strrchr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[0], '/');
		}
		container_name = pointer ? pointer + 1 : ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[0];
	} else
		container_name = value;
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(container_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nthread = (int) strtol(value, NULL, 10);

	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids = (int *) calloc(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num, sizeof(int));

	for (j = 0; j < ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num; ++j) {
		if (!strstr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], "http://")
		    && !strstr(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], "https://")) {
			//Open netcdf file
			struct stat st;
			if (stat(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], &st) == 0) {
				ll = (size_t) 2 *st.st_blksize;
				if ((retval =
				     nc__open(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], NC_NOWRITE, &ll,
					      &(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[j])))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j],
					      nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_OPEN_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j],
				      strerror(errno));
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_OPEN_ERROR_NO_CONTAINER, container_name, strerror(errno));
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

		} else {
			//Open netcdf file from URL
			if ((retval =
			     nc_open(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j], NC_NOWRITE,
				     &(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[j])))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j],
				      nc_strerror(retval));
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_OPEN_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}

	if (oph_pid_get_memory_size(&(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->memory_size))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_OPHIDIADB_CONFIGURATION_FILE, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "cwd");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HOST_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HOST_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HOST_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number == 0)
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number = -1;	// All the host of the partition

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FRAGMENENT_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FRAGMENENT_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_FRAGMENENT_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number == 0)
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number = -1;	// The maximum number of fragments

	//Additional check (all distrib arguments must be bigger than 0 or at least -1 if default values are given)
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number == 0 || ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_FRAG_PARAMS_ERROR, container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_FRAG_PARAMS_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	NETCDF_var *measure = ((NETCDF_var *) & (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MEASURE_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	strncpy(measure->varname, value, NC_MAX_NAME);
	measure->varname[NC_MAX_NAME] = 0;

	char **exp_dim_names = NULL;
	char **imp_dim_names = NULL;
	char **exp_dim_clevels = NULL;
	char **imp_dim_clevels = NULL;
	int exp_number_of_dim_names = 0;
	int imp_number_of_dim_names = 0;
	int imp_number_of_dim_clevels = 0;
	int number_of_dim_clevels = 0;

	int ncid = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[0];
	//Extract measured variable information
	if ((retval = nc_inq_varid(ncid, measure->varname, &(measure->varid)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Get information from id
	if ((retval = nc_inq_vartype(ncid, measure->varid, &(measure->vartype)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Check ndims value
	int ndims;
	if ((retval = nc_inq_varndims(ncid, measure->varid, &ndims))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	// Check the other files
	int *ncids = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids;
	int varid, ndims_other;
	nc_type vartype;
	for (j = 1; j < ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num; ++j) {
		if ((retval = nc_inq_varid(ncids[j], measure->varname, &varid))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if ((retval = nc_inq_vartype(ncids[j], varid, &vartype))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (vartype != measure->vartype) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong type of input file '%s'\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong input file '%s'\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if ((retval = nc_inq_varndims(ncids[j], varid, &ndims_other))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (ndims_other != ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong dimensions of input file '%s'\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong input file '%s'\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[j]);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	char *tmp_concept_levels = NULL;

	if (ndims) {

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(OPH_IMPORTNCS_DIMENSION_DEFAULT))) {
			//If implicit is differen't from auto use standard approach
			if (oph_tp_parse_multiple_value_param(value, &imp_dim_names, &imp_number_of_dim_names)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			measure->nimp = imp_number_of_dim_names;

			if (measure->nimp > ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(OPH_IMPORTNCS_DIMENSION_DEFAULT))) {
				//Explicit is not auto, use standard approach
				if (oph_tp_parse_multiple_value_param(value, &exp_dim_names, &exp_number_of_dim_names)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				measure->nexp = exp_number_of_dim_names;
			} else {
				//Use optimized approach with drilldown
				measure->nexp = ndims - measure->nimp;
				exp_dim_names = NULL;
				exp_number_of_dim_names = 0;
			}

		} else {

			//Implicit dimension is auto, import as NetCDF file order
			pmesg(LOG_WARNING, __FILE__, __LINE__, "One dimension will be considered implicit\n");
			measure->nimp = 1;
			measure->nexp = ndims - 1;
			imp_dim_names = NULL;
			imp_number_of_dim_names = 0;

			value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTNCS_DIMENSION_DEFAULT, strlen(OPH_IMPORTNCS_DIMENSION_DEFAULT))) {
				//Explicit is not auto, use standard approach
				if (oph_tp_parse_multiple_value_param(value, &exp_dim_names, &exp_number_of_dim_names)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (measure->nexp != exp_number_of_dim_names) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of explicit dimensions: set %d dimensions\n", measure->nexp);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			} else {
				//Use optimized approach with drilldown
				exp_dim_names = NULL;
				exp_number_of_dim_names = 0;
			}
		}
		measure->ndims = measure->nexp + measure->nimp;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!(tmp_concept_levels = (char *) malloc(measure->ndims * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "Tmp concpet levels");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(tmp_concept_levels, 0, measure->ndims * sizeof(char));
		if (strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(OPH_COMMON_DEFAULT_CONCEPT_LEVEL))) {
			if (oph_tp_parse_multiple_value_param(value, &exp_dim_clevels, &number_of_dim_clevels)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (number_of_dim_clevels != measure->nexp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			for (i = 0; i < measure->nexp; i++) {
				if ((exp_dim_clevels[i][0] == OPH_HIER_MINUTE_SHORT_NAME[0]) || (exp_dim_clevels[i][0] == OPH_HIER_MONTH_SHORT_NAME[0])) {
					if (!strncmp(exp_dim_clevels[i], OPH_HIER_MINUTE_LONG_NAME, strlen(exp_dim_clevels[i])))
						tmp_concept_levels[i] = OPH_HIER_MINUTE_SHORT_NAME[0];
					else
						tmp_concept_levels[i] = OPH_HIER_MONTH_SHORT_NAME[0];
				} else
					tmp_concept_levels[i] = exp_dim_clevels[i][0];
				if (tmp_concept_levels[i] == OPH_COMMON_ALL_CONCEPT_LEVEL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", OPH_COMMON_ALL_CONCEPT_LEVEL);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_BAD2_PARAMETER, "dimension level", OPH_COMMON_ALL_CONCEPT_LEVEL);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					if (tmp_concept_levels)
						free(tmp_concept_levels);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
		}
		//Default levels
		else {
			for (i = 0; i < measure->nexp; i++)
				tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(OPH_COMMON_DEFAULT_CONCEPT_LEVEL))) {
			if (oph_tp_parse_multiple_value_param(value, &imp_dim_clevels, &imp_number_of_dim_clevels)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (imp_number_of_dim_clevels != measure->nimp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			for (i = measure->nexp; i < measure->ndims; i++) {
				if ((imp_dim_clevels[i - measure->nexp][0] == OPH_HIER_MINUTE_SHORT_NAME[0]) || (imp_dim_clevels[i - measure->nexp][0] == OPH_HIER_MONTH_SHORT_NAME[0])) {
					if (!strncmp(imp_dim_clevels[i - measure->nexp], OPH_HIER_MINUTE_LONG_NAME, strlen(imp_dim_clevels[i - measure->nexp])))
						tmp_concept_levels[i] = OPH_HIER_MINUTE_SHORT_NAME[0];
					else
						tmp_concept_levels[i] = OPH_HIER_MONTH_SHORT_NAME[0];
				} else
					tmp_concept_levels[i] = imp_dim_clevels[i - measure->nexp][0];
				if (tmp_concept_levels[i] == OPH_COMMON_ALL_CONCEPT_LEVEL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", OPH_COMMON_ALL_CONCEPT_LEVEL);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_BAD2_PARAMETER, "dimension level", OPH_COMMON_ALL_CONCEPT_LEVEL);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					if (tmp_concept_levels)
						free(tmp_concept_levels);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
		}
		//Default levels
		else {
			for (i = measure->nexp; i < measure->ndims; i++)
				tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;
		}


		if (ndims != measure->ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (!(measure->dims_name = (char **) malloc(measure->ndims * sizeof(char *)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_name");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(measure->dims_name, 0, measure->ndims * sizeof(char *));

		if (!(measure->dims_length = (size_t *) malloc(measure->ndims * sizeof(size_t)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_length");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_unlim = (char *) malloc(measure->ndims * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_unlim");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_type = (short int *) malloc(measure->ndims * sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_type");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_oph_level");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_concept_level = (char *) calloc(measure->ndims, sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_concept_level");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(measure->dims_concept_level, 0, measure->ndims * sizeof(char));

	} else {

		measure->ndims = ndims;

	}

	//Extract dimension ids following order in the nc file
	if (!(measure->dims_id = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_id");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		if (tmp_concept_levels)
			free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if ((retval = nc_inq_vardimid(ncid, measure->varid, measure->dims_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		if (tmp_concept_levels)
			free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	int unlimdimid;
	if ((retval = nc_inq_unlimdim(ncid, &unlimdimid))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		if (tmp_concept_levels)
			free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Extract dimensions information and check names provided by task string
	char *dimname = NULL;
	short int flag = 0;
	measure->dim_unlim = -1;
	for (i = 0; i < ndims; i++) {
		measure->dims_unlim[i] = measure->dims_id[i] == unlimdimid;
		if (measure->dims_unlim[i] && (measure->dim_unlim < 0))
			measure->dim_unlim = i;
		measure->dims_name[i] = (char *) malloc((NC_MAX_NAME + 1) * sizeof(char));
		if ((retval = nc_inq_dim(ncid, measure->dims_id[i], measure->dims_name[i], &measure->dims_length[i]))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	int level = 1;
	int m2u[measure->ndims ? measure->ndims : 1];
	m2u[0] = 0;
	if (exp_dim_names) {
		for (i = 0; i < measure->nexp; i++) {
			flag = 0;
			dimname = exp_dim_names[i];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					m2u[i] = j;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_oph_level[j] = level++;
			measure->dims_type[j] = 1;
			measure->dims_concept_level[j] = tmp_concept_levels[i];
		}
	} else {
		if (imp_dim_names) {
			int k = 0;
			for (i = 0; i < measure->ndims; i++) {
				flag = 1;
				for (j = 0; j < measure->nimp; j++) {
					dimname = imp_dim_names[j];
					if (!strcmp(dimname, measure->dims_name[i])) {
						//Found implicit dimension
						flag = 0;
						break;
					}
				}
				if (flag) {
					m2u[k] = i;
					measure->dims_oph_level[i] = level++;
					measure->dims_type[i] = 1;
					measure->dims_concept_level[i] = tmp_concept_levels[k];
					k++;
				}
			}
		} else {
			//Use order in nc file
			for (i = 0; i < measure->nexp; i++) {
				m2u[i] = i;

				measure->dims_oph_level[i] = level++;
				measure->dims_type[i] = 1;
				measure->dims_concept_level[i] = tmp_concept_levels[i];
			}
		}
	}

	level = 1;
	if (imp_dim_names) {
		for (i = measure->nexp; i < measure->ndims; i++) {
			flag = 0;
			dimname = imp_dim_names[i - measure->nexp];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					m2u[i] = j;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_concept_level[j] = tmp_concept_levels[i];
			measure->dims_type[j] = 0;
			measure->dims_oph_level[j] = level++;
		}
	} else if (exp_dim_names) {
		int k;
		j = 0;
		for (i = measure->nexp; i < measure->ndims; i++, j++) {
			for (; j < ndims; j++) {
				for (k = 0; k < exp_number_of_dim_names; ++k)
					if (!strcmp(exp_dim_names[k], measure->dims_name[j]))
						break;
				if (k >= exp_number_of_dim_names) {
					m2u[i] = j;
					break;
				}
			}
			if (j >= ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find next implicit dimension in nc file\n", measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_concept_level[j] = tmp_concept_levels[i];
			measure->dims_type[j] = 0;
			measure->dims_oph_level[j] = level++;
		}
	} else {
		//Use order in nc file
		for (i = measure->nexp; i < measure->ndims; i++) {
			m2u[i] = i;
			measure->dims_concept_level[i] = tmp_concept_levels[i];
			measure->dims_type[i] = 0;
			measure->dims_oph_level[i] = level++;
		}
	}

	oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
	oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
	if (tmp_concept_levels)
		free(tmp_concept_levels);

	if (handle->proc_rank == 0) {

		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_OPHIDIADB_CONFIGURATION_FILE, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_OPHIDIADB_CONNECTION_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_VOCABULARY);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VOCABULARY);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_VOCABULARY);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
			if ((oph_odb_meta_retrieve_vocabulary_id(oDB, value, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input vocabulary\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NO_VOCABULARY_NO_CONTAINER, container_name, value);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
	}
	// Find time dimension. Let us assume that OPH_IN_PARAM_CALENDAR is only for time dimensions
	int idp, td = -1;	// Id of time dimension using NetCDF indexing
	for (i = 0; i < measure->ndims; i++) {
		if (!nc_inq_varid(ncid, measure->dims_name[i], &idp) && !nc_inq_attid(ncid, idp, OPH_IN_PARAM_CALENDAR, NULL)) {
			td = i;
			break;
		}
	}

	// Load data regarding more files: master process
	oph_odb_dimension *time_dims = NULL;
	if (!handle->proc_rank && (measure->dim_unlim >= 0) && (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num > 1)) {

		if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "To import more files metadata need to be imported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "To import more files metadata need to be imported\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		measure->number_src_path = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num;
		measure->order_src_path = (int *) malloc(measure->number_src_path * sizeof(int));
		if (!measure->order_src_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure order_src_path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < measure->number_src_path; ++i)
			measure->order_src_path[i] = i;

		if (measure->dim_unlim == td) {
			time_dims = (oph_odb_dimension *) calloc(measure->number_src_path, sizeof(oph_odb_dimension));
			if (!time_dims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure order_src_path");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}
	}
	// Load data regarding more files
	int time_dim_id[((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num];
	for (i = 0; i < measure->number_src_path; ++i)
		time_dim_id[i] = NC_GLOBAL;
	if (time_dims) {
		ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;
		int id_vocabulary = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary;
		for (i = 0; i < measure->number_src_path; ++i) {
			if (oph_nc_update_dim_with_nc_metadata2(oDB, time_dims + i, id_vocabulary, OPH_GENERIC_CONTAINER_ID, ncids[i], time_dim_id + i) || (time_dim_id[i] < 0))
				break;
		}
		if (i < measure->number_src_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in detecting time dimension\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in detecting time dimension\n");
			free(time_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	double start_point[((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num];
	double base_time[((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num];
	char main_base_time[OPH_ODB_DIM_TIME_SIZE + 1];
	char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE];
	if (measure->order_src_path)
		for (i = 0; i < measure->number_src_path; ++i)
			base_time[i] = start_point[i] = 0;
	if (time_dims) {
		long long sp;
		for (i = 0; i < measure->number_src_path; ++i) {
			if (oph_dim_get_base_time(time_dims + i, &sp))
				break;
			base_time[i] = start_point[i] = (double) sp;
			// Convert from "seconds"
			switch (time_dims[i].units[0]) {
				case 'd':
					base_time[i] /= 4.0;
				case '6':
					base_time[i] /= 2.0;
				case '3':
					base_time[i] /= 3.0;
				case 'h':
					base_time[i] /= 60.0;
				case 'm':
					base_time[i] /= 60.0;
				case 's':
					break;
				default:
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unrecognized or unsupported units\n");
			}
		}
		if (i < measure->number_src_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in evaluating the base time\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in evaluating the base time\n");
			free(time_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}
	// Load the starting point of each input file
	size_t data_size = 0, tot_size = 0;
	if (measure->order_src_path) {
		char **dim_array = (char **) calloc(measure->number_src_path, sizeof(char *));
		if (!dim_array) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Memory error\n");
			if (time_dims)
				free(time_dims);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		nc_type vartype;
		if (nc_inq_vartype(ncids[0], time_dim_id[0], &vartype)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in detecting dimension type\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in detecting dimension type\n");
			if (time_dims)
				free(time_dims);
			free(dim_array);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (oph_nc_get_c_type(vartype, dim_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in converting dimension type\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in converting dimension type\n");
			if (time_dims)
				free(time_dims);
			free(dim_array);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		size_t size[measure->number_src_path];
		for (i = 0; i < measure->number_src_path; ++i) {
			if (oph_nc_get_dim_array_and_size(OPH_GENERIC_CONTAINER_ID, ncids[i], time_dim_id[i], dim_type, 0, dim_array + i, size + i))
				break;
			tot_size += size[i];
		}
		if (i < measure->number_src_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in loading the starting point of each input file\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in loading the starting point of each input file\n");
			if (time_dims)
				free(time_dims);
			for (i = 0; i < measure->number_src_path; ++i)
				if (dim_array[i])
					free(dim_array[i]);
			free(dim_array);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		for (i = 0; i < measure->number_src_path; ++i) {
			if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(char *) dim_array[i];
			else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(short *) dim_array[i];
			else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(int *) dim_array[i];
			else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(float *) dim_array[i];
			else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(double *) dim_array[i];
			else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				start_point[i] += *(long long *) dim_array[i];
			else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in loading the starting point of each input file\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in loading the starting point of each input file\n");
				if (time_dims)
					free(time_dims);
				for (i = 0; i < measure->number_src_path; ++i)
					if (dim_array[i])
						free(dim_array[i]);
				free(dim_array);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
		if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(char);
		else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(short);
		else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(int);
		else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(float);
		else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(double);
		else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			data_size = sizeof(long long);
		// Order the input files
		int tmp;
		for (i = 0; i < measure->number_src_path - 1; ++i)
			for (j = i + 1; j < measure->number_src_path; ++j)
				if (start_point[measure->order_src_path[i]] > start_point[measure->order_src_path[j]]) {
					tmp = measure->order_src_path[i];
					measure->order_src_path[i] = measure->order_src_path[j];
					measure->order_src_path[j] = tmp;
				}
		if (time_dims) {
			strcpy(main_base_time, time_dims[measure->order_src_path[0]].base_time);
			free(time_dims);
		}
		size_t k, offset2, offset3 = 0;
		double diff;
		measure->dim_unlim_array = (char *) malloc(tot_size);
		if (!measure->dim_unlim_array) {
			for (i = 0; i < measure->number_src_path; ++i)
				if (dim_array[i])
					free(dim_array[i]);
			free(dim_array);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < measure->number_src_path; ++i) {
			memcpy(measure->dim_unlim_array + offset3, dim_array[measure->order_src_path[i]], size[measure->order_src_path[i]]);
			if (i) {
				diff = base_time[measure->order_src_path[i]] - base_time[measure->order_src_path[0]];
				if (diff > 0.0) {
					size_t _size = size[measure->order_src_path[i]] / data_size;
					if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(char *) (measure->dim_unlim_array + offset2) += (char) diff;
					else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(short *) (measure->dim_unlim_array + offset2) += (short) diff;
					else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(int *) (measure->dim_unlim_array + offset2) += (int) diff;
					else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(float *) (measure->dim_unlim_array + offset2) += (float) diff;
					else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(double *) (measure->dim_unlim_array + offset2) += (double) diff;
					else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						for (k = 0, offset2 = offset3; k < _size; ++k, offset2 += data_size)
							*(long long *) (measure->dim_unlim_array + offset2) += (long long) diff;
				}
			}
			offset3 += size[measure->order_src_path[i]];
		}
		for (i = 0; i < measure->number_src_path; ++i)
			if (dim_array[i])
				free(dim_array[i]);
		free(dim_array);
	}
	// Other processes
	if (handle->proc_rank && (measure->dim_unlim >= 0) && (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num > 1)) {

		measure->number_src_path = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num;
		measure->order_src_path = (int *) malloc(measure->number_src_path * sizeof(int));
		if (!measure->order_src_path) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure order_src_path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	for (i = 1; i < ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num; ++i)
		if ((retval = nc_close(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[i])))
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %s\n", nc_strerror(retval));

	if (measure->order_src_path) {

		// TODO: some of the following Bcast could be skipped: check

		// Synchronize the file order
		MPI_Bcast(measure->order_src_path, measure->number_src_path, MPI_INT, 0, MPI_COMM_WORLD);

		// Synchronize the base time
		MPI_Bcast(main_base_time, OPH_ODB_DIM_TIME_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
		main_base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
		if (!handle->proc_rank)
			measure->base_time = strdup(main_base_time);

		// Synchronize the unlimited dimension size
		int sizes[2];
		if (!handle->proc_rank) {
			sizes[0] = data_size;
			sizes[1] = tot_size;
			MPI_Bcast(sizes, 2, MPI_INT, 0, MPI_COMM_WORLD);
		} else {
			MPI_Bcast(sizes, 2, MPI_INT, 0, MPI_COMM_WORLD);
			data_size = sizes[0];
			tot_size = sizes[1];
		}

		if (handle->proc_rank)
			measure->dim_unlim_array = (char *) malloc(tot_size);	// Due to MPI communications, memory error is not handled yet

		// Synchronize the unlimited dimension array
		MPI_Bcast(measure->dim_unlim_array, tot_size, MPI_CHAR, 0, MPI_COMM_WORLD);

		measure->dims_length[measure->dim_unlim] = tot_size / data_size;	// Real length

		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
			free(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig);
			size_t orig_length = 0;
			for (i = 0; i < measure->number_src_path; ++i)
				orig_length += 1 + strlen(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[i]);
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = (char *) malloc(orig_length + 1);
			*((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = 0;
			strcat(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig,
			       ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[measure->order_src_path[0]]);
			for (i = 1; i < measure->number_src_path; ++i) {
				strcat(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig, "|");
				strcat(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig,
				       ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[measure->order_src_path[i]]);
			}
		}
	} else {

		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
			free(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig);
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = strdup(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths[0]);
		}
	}

	//ADDED TO MANAGE SUBSETTED IMPORT

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = NULL;
	int s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	double *offset = NULL;
	if (s_offset_num > 0) {
		offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "offset");
			oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < s_offset_num; ++i)
			offset[i] = (double) strtod(s_offset[i], NULL);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
	}

	char **sub_dims = 0;
	char **sub_filters = 0;
	char **sub_types = NULL;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;
	int number_of_sub_types = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int issubset = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int isfilter = 0;

	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		isfilter = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check dimension names
	int sub_to_dims[number_of_sub_dims];
	for (i = 0; i < number_of_sub_dims; i++) {
		dimname = sub_dims[i];
		for (j = 0; j < ndims; j++)
			if (!strcmp(dimname, measure->dims_name[j]))
				break;
		if (j == ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension '%s' related to variable '%s' in in nc file\n", dimname, measure->varname);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		sub_to_dims[i] = j;
	}

	//Check the sub_filters strings
	int tf = -1;		// Id of time filter using user indexing (the input parameter for subsetting)
	for (i = 0; i < number_of_sub_dims; i++) {
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->time_filter) {
			if (sub_to_dims[i] == td) {
				tf = i;
				break;
			}
		}
		if ((tf != i) || !strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR[1])) {
			if (strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2) != strrchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided ranges are not supported\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
	}

	int id_container_out = OPH_GENERIC_CONTAINER_ID;

	// Load time metadata
	if (tf >= 0) {

		oph_odb_dimension dim;
		oph_odb_dimension *time_dim = &dim, *tot_dims = NULL;

		int permission = 0, folder_id = 0, container_exists;
		ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

		if (handle->proc_rank == 0) {

			char *cwd = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd;
			char *user = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user;

			//Check if input path exists
			if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_CWD_ERROR, container_name, cwd);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DATACUBE_PERMISSION_ERROR, container_name, user);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			//Check if container exists
			if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		int create_container = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->create_container;
		if (container_exists)
			create_container = 0;

		if (create_container) {
			if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata || !handle->proc_rank) {
				strncpy(time_dim->base_time, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
				time_dim->base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
				time_dim->leap_year = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_year;
				time_dim->leap_month = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_month;

				NETCDF_var tmp_var;
				tmp_var.dims_id = NULL;
				tmp_var.dims_length = NULL;
				if (oph_nc_get_nc_var(id_container_out, measure->dims_name[j], ncid, 1, &tmp_var)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
					if (tmp_var.dims_id)
						free(tmp_var.dims_id);
					if (tmp_var.dims_length)
						free(tmp_var.dims_length);
					oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
					oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
					if (offset)
						free(offset);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (tmp_var.dims_id)
					free(tmp_var.dims_id);
				if (tmp_var.dims_length)
					free(tmp_var.dims_length);

				if (oph_nc_get_c_type(tmp_var.vartype, time_dim->dimension_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, "type cannot be converted");
					oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
					oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
					if (offset)
						free(offset);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}

				j = 0;
				strncpy(time_dim->units, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
				time_dim->units[OPH_ODB_DIM_TIME_SIZE] = 0;
				strncpy(time_dim->calendar, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
				time_dim->calendar[OPH_ODB_DIM_TIME_SIZE] = 0;
				char *tmp = NULL, *save_pointer = NULL, month_lengths[1 + OPH_ODB_DIM_TIME_SIZE];
				strncpy(month_lengths, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
				month_lengths[OPH_ODB_DIM_TIME_SIZE] = 0;
				while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
					time_dim->month_lengths[j++] = (int) strtol(tmp, NULL, 10);
				while (j < OPH_ODB_DIM_MONTH_NUMBER)
					time_dim->month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
			}

			if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata) {
				size_t packet_size = sizeof(oph_odb_dimension);
				char buffer[packet_size];

				if (handle->proc_rank == 0) {
					ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

					if (oph_nc_update_dim_with_nc_metadata(oDB, time_dim, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary, id_container_out, ncid))
						time_dim->id_dimension = 0;
					else
						time_dim->id_dimension = -1;

					memcpy(buffer, time_dim, packet_size);
					MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
				} else {
					MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
					memcpy(time_dim, buffer, packet_size);
				}

				if (!time_dim->id_dimension) {
					oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
					oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
					if (offset)
						free(offset);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
		} else {
			size_t packet_size = sizeof(oph_odb_dimension);
			char buffer[packet_size];

			if (handle->proc_rank == 0) {
				int flag = 1, number_of_dimensions_c = 0;
				while (flag) {

					if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out) && !id_container_out) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
						break;
					}
					if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions .\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIMENSION_READ_ERROR);
						break;
					}

					for (i = 0; i < number_of_dimensions_c; ++i)
						if (!strncmp(tot_dims[i].dimension_name, measure->dims_name[j], OPH_ODB_DIM_DIMENSION_SIZE))
							break;
					if (i >= number_of_dimensions_c) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[j], container_name);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_CONT_ERROR, measure->dims_name[j], container_name);
						break;
					}

					time_dim = tot_dims + i;

					if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata) {
						// Load the vocabulary associated with the container
						if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary
						    && oph_odb_meta_retrieve_vocabulary_id_from_container(oDB, id_container_out,
													  &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown vocabulary\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_VOCABULARY_NO_CONTAINER, container_name, "");
							break;
						}

						if (oph_nc_update_dim_with_nc_metadata
						    (oDB, time_dim, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary, id_container_out, ncid))
							break;
					}

					flag = 0;
				}
				if (flag)
					time_dim->id_dimension = 0;

				memcpy(buffer, time_dim, packet_size);
				MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
			} else {
				MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
				memcpy(time_dim, buffer, packet_size);
			}

			if (!time_dim->id_dimension) {
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		if (measure->order_src_path && (measure->dim_unlim == sub_to_dims[tf]))
			strcpy(time_dim->base_time, main_base_time);

		long long max_size = QUERY_BUFLEN;
		oph_pid_get_buffer_size(&max_size);
		char temp[max_size];
		if (oph_dim_parse_time_subset(sub_filters[tf], time_dim, temp)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values '%s'\n", sub_filters[tf]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		free(sub_filters[tf]);
		sub_filters[tf] = strdup(temp);
	}
	//Alloc space for subsetting parameters
	if (!(measure->dims_start_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_start_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_end_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &sub_types, &number_of_sub_types)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (number_of_sub_dims && (number_of_sub_types > number_of_sub_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char is_index[1 + number_of_sub_dims];
	if (number_of_sub_dims) {
		for (i = 0; i < number_of_sub_types; ++i)
			is_index[i] = strncmp(sub_types[i], OPH_IMPORTNCS_SUBSET_COORD, OPH_TP_TASKLEN);
		for (; i < number_of_sub_dims; ++i)
			is_index[i] = number_of_sub_types == 1 ? is_index[0] : 1;
	}
	is_index[number_of_sub_dims] = 0;
	oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);

	char *curfilter = NULL;
	int ii;

	//For each dimension, set the dim_start_index and dim_end_index
	for (i = 0; i < ndims; i++) {
		curfilter = NULL;
		for (j = 0; j < number_of_sub_dims; j++) {
			dimname = sub_dims[j];
			if (!strcmp(dimname, measure->dims_name[i])) {
				curfilter = sub_filters[j];
				break;
			}
		}
		if (!curfilter) {
			//Dimension will not be subsetted
			measure->dims_start_index[i] = 0;
			measure->dims_end_index[i] = measure->dims_length[i] - 1;
		} else if ((ii = oph_nc_check_subset_string(curfilter, i, measure, is_index[j], ncid, j < s_offset_num ? offset[j] : 0.0, 0))) {
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return ii;
		} else if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
			   || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
	oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
	if (offset)
		free(offset);

	//Check explicit dimension oph levels (all values in interval [1, nexp] should be supplied)
	int curr_lev;
	int level_ok;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		level_ok = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				level_ok = 1;
				break;
			}
		}
		if (!level_ok) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Explicit dimension levels aren't correct\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_EXP_DIM_LEVEL_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	//Set variable size
	measure->varsize = 1;
	for (j = 0; j < measure->ndims; j++) {
		//Modified to allow subsetting
		if (measure->dims_end_index[j] == measure->dims_start_index[j])
			continue;
		measure->varsize *= (measure->dims_end_index[j] - measure->dims_start_index[j]) + 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMPRESSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMPRESSION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_COMPRESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->compressed = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SIMULATE_RUN);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SIMULATE_RUN);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SIMULATE_RUN);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NO_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run = 0;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_COMPLIANCE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_COMPLIANCE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CHECK_COMPLIANCE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_compliance = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PARTITION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PARTITION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_PARTITION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->partition_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input partition");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IOSERVER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "I/O server type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//Only Ophidia IO server can be used
	if (strcasecmp(value, OPH_IOSERVER_OPHIDIAIO_TYPE) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_PARAMETER, OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "[CONTAINER: %s] " OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_GRID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_GRID);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CHECK_GRID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN))
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_grid = 1;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIERARCHY_NAME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIERARCHY_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HIERARCHY_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_HIERARCHY, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_HIERARCHY, strlen(OPH_COMMON_DEFAULT_HIERARCHY))) {
			char **dim_hierarchy;
			int number_of_dimensions_hierarchy = 0;
			if (oph_tp_parse_multiple_value_param(value, &dim_hierarchy, &number_of_dimensions_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (measure->ndims != number_of_dimensions_hierarchy) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(number_of_dimensions_hierarchy * sizeof(int)))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			memset(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, number_of_dimensions_hierarchy * sizeof(int));

			int id_hierarchy = 0, found_oph_time = 0;
			for (i = 0; i < number_of_dimensions_hierarchy; i++) {
				//Retrieve hierarchy ID
				if (oph_odb_dim_retrieve_hierarchy_id(oDB, dim_hierarchy[i], &id_hierarchy)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, dim_hierarchy[i]);
					oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[m2u[i]] = id_hierarchy;
				if (!strcasecmp(dim_hierarchy[i], OPH_COMMON_TIME_HIERARCHY)) {
					if (found_oph_time) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Only one time dimension can be used\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, dim_hierarchy[i]);
						oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[m2u[i]] *= -1;
					found_oph_time = 1;
				}
			}
			oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
		}
		//Default hierarchy
		else {
			if (!(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(measure->ndims * sizeof(int)))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			memset(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, measure->ndims * sizeof(int));

			//Retrieve hierarchy ID
			int id_hierarchy = 0;
			if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_DEFAULT_HIERARCHY, &id_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, OPH_COMMON_DEFAULT_HIERARCHY);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (i = 0; i < measure->ndims; i++)
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] = id_hierarchy;
		}
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int id_datacube[7] = { 0, 0, 0, 0, 0, 0, 0 };

	int i, j, retval = 0, flush = 1, id_datacube_out = 0, id_container_out = OPH_GENERIC_CONTAINER_ID;

	NETCDF_var *measure = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure;

	//Find the first explicit dimension checking oph_value
	short int min_lev = 1;
	//Find most external dimension with size bigger than 1
	for (i = 0; i < measure->nexp; i++) {
		for (j = 0; j < measure->ndims; j++) {
			//Consider only explicit dimensions
			if (measure->dims_type[j] && measure->dims_oph_level[j] == (i + 1)) {
				break;
			}
		}

		if ((measure->dims_end_index[j] - measure->dims_start_index[j]) > 0) {
			min_lev = measure->dims_oph_level[j];
			break;
		}
	}

	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number = 1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->array_length = 1;
	for (i = 0; i < measure->ndims; i++) {
		if (measure->dims_type[i]) {
			//Consider only explicit dimensions
			if (measure->dims_oph_level[i] == min_lev) {
				//Compute total fragment as the number of values of the most external explicit dimensions excluding those with size 1
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			} else if (measure->dims_oph_level[i] > min_lev) {
				//Compute tuple per fragment as the number of values of most inernal explicit dimension (excluding the first one bigger than 1)
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
			}
		} else {
			//Consider only implicit dimensions
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->array_length *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
		}
	}

	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->number_unven_frag = 0;
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->int_dim_product = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number;

	int container_exists = 0;
	char *container_name = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input;
	int create_container = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->create_container;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

		int i, j;
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
		int last_insertd_id = 0;
		int *host_number = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number;
		int *fragxdb_number = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number;
		char *host_partition = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->partition_input;
		char *ioserver_type = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type;
		int run = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run;

		//Retrieve user id
		char *username = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user;
		int id_user = 0;
		if (oph_odb_user_retrieve_user_id(oDB, username, &id_user)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive user id\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_USER_ID_ERROR);
			goto __OPH_EXIT_1;
		}

	  /********************************
	   *INPUT PARAMETERS CHECK - BEGIN*
	   ********************************/
		int exist_part = 0;
		int nhost = 0;
		int frag_param_error = 0;
		int final_frag_number = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number;

		int admissible_frag_number = 0;
		int user_arg_prod = 0;
		int id_host_partition = 0;
		char hidden = 0;

		//If default values are used: select fylesystem and partition
		if (!strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(host_partition))
		    && !strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(OPH_COMMON_HOSTPARTITION_DEFAULT))) {
			if (oph_odb_stge_get_default_host_partition_fs(oDB, ioserver_type, id_user, &id_host_partition, *host_number > 0 ? *host_number : 1) || !id_host_partition) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number, host_partition);
					goto __OPH_EXIT_1;
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
					frag_param_error = 1;
				}
			}
		} else {
			if (oph_odb_stge_get_host_partition_by_name(oDB, host_partition, id_user, &id_host_partition, &hidden)) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to load partition '%s'!\n", host_partition);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Failed to load partition '%s'!\n", host_partition);
					goto __OPH_EXIT_1;
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to load partition '%s'!\n", host_partition);
					frag_param_error = 1;
				}
			}
			//Check if are available DBMS and HOST number into specified partition and of server type
			if (*host_number > 0) {
				if ((oph_odb_stge_check_number_of_host_dbms(oDB, ioserver_type, id_host_partition, *host_number, &exist_part)) || !exist_part) {
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
							((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number, host_partition);
						goto __OPH_EXIT_1;
					} else {
						//If simulated run then reset values
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
						frag_param_error = 1;
					}
				}
			}
		}

		if (*host_number <= 0) {
			//Check how many DBMS and HOST are available into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ioserver_type, id_host_partition, &nhost) || !nhost) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or server type and partition are not available!\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name, host_partition);
					goto __OPH_EXIT_1;
				} else {
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name, host_partition);
					frag_param_error = 1;
				}
			}
		}

		if (*host_number > 0 || *fragxdb_number > 0) {
			//At least one argument is specified
			if (*host_number <= 0) {
				if (*fragxdb_number <= 0) {
					user_arg_prod = (1 * 1);
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
								final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						admissible_frag_number = final_frag_number / user_arg_prod;
						if (admissible_frag_number <= nhost) {
							*host_number = admissible_frag_number;
							*fragxdb_number = 1;
						} else {
							//Get highest divisor for host_number
							int ii = 0;
							for (ii = nhost; ii > 0; ii--) {
								if (admissible_frag_number % ii == 0)
									break;
							}
							*host_number = ii;
							*fragxdb_number = (int) admissible_frag_number / (*host_number);
						}
					}
				} else
					*host_number = nhost;
			}
			if (*host_number > 0) {
				if (*fragxdb_number <= 0) {
					user_arg_prod = ((*host_number) * 1);
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
								final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						*fragxdb_number = (int) ceilf((float) final_frag_number / user_arg_prod);
					}
				} else {
					//User has set all parameters - in this case allow further fragmentation
					user_arg_prod = ((*host_number) * (*fragxdb_number));
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
								final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						//Since fragxdb is fixed recompute tuplexfrag
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number *= (int) ceilf((float) final_frag_number / user_arg_prod);
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->number_unven_frag = final_frag_number % (user_arg_prod);
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number = ((*host_number) * (*fragxdb_number));
					}
				}
			}
		} else {
			//Default case
			user_arg_prod = 1;
			admissible_frag_number = final_frag_number / user_arg_prod;
			if (admissible_frag_number <= nhost) {
				*host_number = admissible_frag_number;
				*fragxdb_number = 1;
			} else {
				*host_number = nhost;
				*fragxdb_number = (int) ceilf((float) admissible_frag_number / (*host_number));
				//if (((int) handle->proc_number / nhost) >= 1)
				//      *fragxdb_number = (((int) handle->proc_number / nhost) < *fragxdb_number ? ((int) handle->proc_number / nhost) : *fragxdb_number);
			}
		}

		if (frag_param_error) {
			//Check how many DBMS and HOST are available into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ioserver_type, id_host_partition, &nhost) || !nhost) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or server type and partition are not available!\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name, host_partition);
				goto __OPH_EXIT_1;
			}

			int ii = 0;
			for (ii = nhost; ii > 0; ii--) {
				if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number % ii == 0)
					break;
			}

			//Simulate simple arguments
			*host_number = ii;
			*fragxdb_number = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number / ii;
		}
		//Check that product of ncores and nthread is at most equal to total number of fragments        
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
		}

		if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run) {
			char message[OPH_COMMON_BUFFER_LEN] = { 0 };
			int len = 0;

			if (frag_param_error)
				printf("Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				printf("Specified parameters are:\n");
			printf("\tNumber of hosts: %d\n", *host_number);
			printf("\tNumber of fragments per database: %d\n", *fragxdb_number);
			printf("\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (frag_param_error)
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters are:\n");
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of hosts: %d\n", *host_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of fragments per database: %d\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (oph_json_is_objkey_printable
			    (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys_num,
			     OPH_JSON_OBJKEY_IMPORTNCS)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTNCS, "Fragmentation parameters", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, "ADD TEXT error\n");
				}
			}
			goto __OPH_EXIT_1;
		}
		//Check if are available DBMS and HOST number into specified partition and of server type
		if ((oph_odb_stge_check_number_of_host_dbms(oDB, ioserver_type, id_host_partition, *host_number, &exist_part)) || !exist_part) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number, host_partition);
			goto __OPH_EXIT_1;
		}

		int ncid = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[0];
		char *cwd = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd;
		char *user = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user;
		NETCDF_var tmp_var;

		int permission = 0;
		int folder_id = 0;
		//Check if input path exists
		if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_CWD_ERROR, container_name, cwd);
			goto __OPH_EXIT_1;
		}
		if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DATACUBE_PERMISSION_ERROR, container_name, user);
			goto __OPH_EXIT_1;
		}
		//Check if container exists
		if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		if (container_exists)
			create_container = 0;
		if (create_container) {

			if (!oph_odb_fs_is_allowed_name(container_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", container_name);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_GENERIC_NAME_NOT_ALLOWED_ERROR, container_name);
				goto __OPH_EXIT_1;
			}
			//Check if container exists in folder
			int container_unique = 0;
			if ((oph_odb_fs_is_unique(folder_id, container_name, oDB, &container_unique))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			if (!container_unique) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Container '%s' already exists in this path or a folder has the same name\n", container_name);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			//If it doesn't then create new container and get last id
			oph_odb_container cont;
			strncpy(cont.container_name, container_name, OPH_ODB_FS_CONTAINER_SIZE);
			cont.container_name[OPH_ODB_FS_CONTAINER_SIZE] = 0;
			cont.id_parent = 0;
			cont.id_folder = folder_id;
			cont.operation[0] = 0;
			cont.id_vocabulary = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary;
			cont.description[0] = 0;

			if (oph_odb_fs_insert_into_container_table(oDB, &cont, &id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container = id_container_out;

			if (container_exists && oph_odb_fs_add_suffix_to_container_name(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			//Create new dimensions
			oph_odb_dimension dim;
			dim.id_container = id_container_out;
			strncpy(dim.base_time, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
			dim.base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
			dim.leap_year = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_year;
			dim.leap_month = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->leap_month;

			for (i = 0; i < measure->ndims; i++) {
				tmp_var.dims_id = NULL;
				tmp_var.dims_length = NULL;

				if (oph_nc_get_nc_var(id_container_out, measure->dims_name[i], ncid, 1, &tmp_var)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
					if (tmp_var.dims_id)
						free(tmp_var.dims_id);
					if (tmp_var.dims_length)
						free(tmp_var.dims_length);
					goto __OPH_EXIT_1;
				}
				if (tmp_var.dims_id)
					free(tmp_var.dims_id);
				if (tmp_var.dims_length)
					free(tmp_var.dims_length);

				// Load dimension names and types
				strncpy(dim.dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE);
				dim.dimension_name[OPH_ODB_DIM_DIMENSION_SIZE] = 0;
				if (oph_nc_get_c_type(tmp_var.vartype, dim.dimension_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, "type cannot be converted");
					goto __OPH_EXIT_1;
				}
				dim.id_hierarchy = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i];
				if (dim.id_hierarchy >= 0)
					dim.units[0] = dim.calendar[0] = 0;
				else {
					int j = 0;
					dim.id_hierarchy *= -1;
					strncpy(dim.units, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
					dim.units[OPH_ODB_DIM_TIME_SIZE] = 0;
					strncpy(dim.calendar, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
					dim.calendar[OPH_ODB_DIM_TIME_SIZE] = 0;
					char *tmp = NULL, *save_pointer = NULL, month_lengths[1 + OPH_ODB_DIM_TIME_SIZE];
					strncpy(month_lengths, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
					month_lengths[OPH_ODB_DIM_TIME_SIZE] = 0;
					while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
						dim.month_lengths[j++] = (int) strtol(tmp, NULL, 10);
					while (j < OPH_ODB_DIM_MONTH_NUMBER)
						dim.month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
				}
				if (oph_odb_dim_insert_into_dimension_table(oDB, &(dim), &last_insertd_id, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert dimension.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_DIMENSION_ERROR);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_create_db(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DB_CREATION);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			char dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

		} else if (!container_exists) {
			//If it doesn't exist then return an error
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Else retreive container ID and check for dimension table
		if (!create_container && oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Check vocabulary
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata) {
			if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary
			    && oph_odb_meta_retrieve_vocabulary_id_from_container(oDB, id_container_out, &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown vocabulary\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_VOCABULARY_NO_CONTAINER, container_name, "");
				goto __OPH_EXIT_1;
			}
		}
		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
		int exist_flag = 0;

		if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		char not_exists = 0, base_time_conversion = 0;
		int id_grid = 0, time_dimension = -1;
		oph_odb_dimension_instance *dim_inst = NULL;
		int dim_inst_num = 0;
		oph_odb_dimension *dims = NULL;
		int *dimvar_ids = malloc(sizeof(int) * measure->ndims);
		if (!dimvar_ids) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating dimvar_ids\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Error allocating dimvar_ids\n");
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name
		    && !oph_odb_dim_retrieve_grid_id(oDB, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &id_grid) && id_grid) {
			//Check if ophidiadb dimensions are the same of input dimensions

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
			    (oDB, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &dims, &dim_inst, &dim_inst_num)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input grid name not usable! It is already used by another container.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NO_GRID, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				if (dims)
					free(dims);
				if (dim_inst)
					free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;

			}

			if (dim_inst_num != measure->ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension number doesn't match with specified grid dimension number\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_GRID_DIMENSION_MISMATCH, "number");
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}

			int match = 1;
			int found_flag;
			char *dim_array = NULL;
			int curdimlength = 1;
			for (i = 0; i < measure->ndims; i++) {
				//Check if container dimension name, size and concept level matches input dimension params -
				found_flag = 0;
				for (j = 0; j < dim_inst_num; j++) {
					if (!strncmp(dims[j].dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE)) {
						//Modified to allow subsetting
						if (measure->dims_start_index[i] == measure->dims_end_index[i])
							curdimlength = 1;
						else
							curdimlength = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
						if (dim_inst[j].size == curdimlength && dim_inst[j].concept_level == measure->dims_concept_level[i]) {
							//Retrieve dimension row from nc file
							if ((retval = nc_inq_varid(ncid, measure->dims_name[i], &(tmp_var.varid)))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							dimvar_ids[i] = tmp_var.varid;

							if (nc_inq_vartype(ncid, tmp_var.varid, &(tmp_var.vartype))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							if ((tmp_var.varid >= 0) && oph_nc_compare_nc_c_types(id_container_out, tmp_var.vartype, dims[j].dimension_type)) {
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension type in NC file doesn't correspond to the one stored in OphidiaDB\n");
								logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TYPE_MISMATCH_ERROR, measure->dims_name[i]);
							}
							//Modified to allow subsetting
							tmp_var.dims_start_index = &(measure->dims_start_index[i]);
							tmp_var.dims_end_index = &(measure->dims_end_index[i]);

							//Get information from id
							if ((retval = nc_inq_varndims(ncid, tmp_var.varid, &(tmp_var.ndims)))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}

							if (tmp_var.ndims != 1) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_NOT_ALLOWED);
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}

							tmp_var.dims_id = malloc(tmp_var.ndims * sizeof(int));
							if (!(tmp_var.dims_id)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT, "dimensions nc ids");
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							if ((retval = nc_inq_vardimid(ncid, tmp_var.varid, tmp_var.dims_id))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(tmp_var.dims_id);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}

							dim_array = NULL;

							if (measure->order_src_path && (measure->dim_unlim == i))
								dim_array = measure->dim_unlim_array;

							else if (oph_nc_get_dim_array2
								 (id_container_out, ncid, tmp_var.varid, dims[j].dimension_type, dim_inst[j].size, *(tmp_var.dims_start_index),
								  *(tmp_var.dims_end_index), &dim_array)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(tmp_var.dims_id);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							free(tmp_var.dims_id);

							if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_grid || (!oph_dim_compare_dimension
																	 (db_dimension, label_dimension_table_name,
																	  dims[j].dimension_type, dim_inst[j].size, dim_array,
																	  dim_inst[j].fk_id_dimension_label, &match)
																	 && !match)) {
								free(dim_array);
								found_flag = 1;
								break;
							}

							if (!measure->order_src_path || (measure->dim_unlim != i))
								free(dim_array);
						}
					}
				}

				if (!found_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n", measure->dims_name[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INPUT_DIMENSION_MISMATCH, measure->dims_name[i]);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dims);
					free(dim_inst);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
			}
		} else if (measure->ndims) {
		 /****************************
	      * BEGIN - IMPORT DIMENSION *
		  ***************************/
			int number_of_dimensions_c = 0, i, j;
			oph_odb_dimension *tot_dims = NULL;

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIMENSION_READ_ERROR);
				if (tot_dims)
					free(tot_dims);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}

			int dimension_array_id = 0;
			char *dim_array = NULL;
			int exists = 0;
			char filename[2 * OPH_TP_BUFLEN];
			oph_odb_hierarchy hier;
			dims = (oph_odb_dimension *) malloc(measure->ndims * sizeof(oph_odb_dimension));
			dim_inst = (oph_odb_dimension_instance *) malloc(measure->ndims * sizeof(oph_odb_dimension_instance));
			dim_inst_num = measure->ndims;

			long long kk;
			long long *index_array = NULL;

			//For each input dimension check hierarchy
			for (i = 0; i < measure->ndims; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncasecmp(tot_dims[j].dimension_name, measure->dims_name[i], strlen(tot_dims[j].dimension_name)))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[i],
					      ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				//Find dimension level into hierarchy file
				if (oph_odb_dim_retrieve_hierarchy(oDB, tot_dims[j].id_hierarchy, &hier)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				exist_flag = 0;
				snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
				if (oph_hier_check_concept_level_short(filename, measure->dims_concept_level[i], &exists)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if ((time_dimension < 0) && !strcmp(hier.hierarchy_name, OPH_COMMON_TIME_HIERARCHY)) {
					time_dimension = i;
					int idp;
					size_t xlen = 0;
					if (!nc_inq_varid(ncid, measure->dims_name[i], &idp) && !nc_inq_attlen(ncid, idp, OPH_IN_PARAM_UNITS, &xlen)) {
						char tmp[1 + xlen];
						if (!nc_get_att_text(ncid, measure->dims_id[i], OPH_IN_PARAM_UNITS, tmp)) {
							tmp[xlen] = 0;
							char *pch = strchr(tmp, ' ');
							if (pch && (pch = strchr(pch + 1, ' '))) {
								strcpy(tot_dims[j].base_time, pch + 1);
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Update base time to '%s'\n", tot_dims[j].base_time);
							}
						}
					}
				}
				if (!exists) {
					if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata && !strcmp(hier.hierarchy_name, OPH_COMMON_TIME_HIERARCHY))
						not_exists = 1;
					else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c': check container specifications\n", measure->dims_concept_level[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_BAD2_PARAMETER, measure->dims_concept_level[i]);
						free(tot_dims);
						free(dims);
						free(dim_inst);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				}
			}

			oph_odb_dimension_grid new_grid;
			int id_grid = 0, grid_exist = 0;
			if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name) {
				strncpy(new_grid.grid_name, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				new_grid.grid_name[OPH_ODB_DIM_GRID_SIZE] = 0;
				int last_inserted_grid_id = 0;
				if (oph_odb_dim_insert_into_grid_table(oDB, &new_grid, &last_inserted_grid_id, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert grid in OphidiaDB, or grid already exists.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_GRID_INSERT_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, "Grid already exists: dimensions will be not associated to a grid.\n");
				} else
					id_grid = last_inserted_grid_id;
			}
			//For each input dimension
			for (i = 0; i < measure->ndims; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncasecmp(tot_dims[j].dimension_name, measure->dims_name[i], strlen(tot_dims[j].dimension_name)))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[i],
					      ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				tmp_var.varid = -1;
				tmp_var.dims_id = NULL;
				tmp_var.dims_length = NULL;

				if ((retval = oph_nc_get_nc_var(id_container_out, measure->dims_name[i], ncid, 1, &tmp_var))) {
					if (create_container) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read information of dimension '%s': %s\n", measure->dims_name[i], nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
						free(tot_dims);
						free(dims);
						free(dim_inst);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (tmp_var.dims_id)
							free(tmp_var.dims_id);
						if (tmp_var.dims_length)
							free(tmp_var.dims_length);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					} else {
						tmp_var.varid = -1;
						pmesg(LOG_WARNING, __FILE__, __LINE__, "Fill dimension '%s' with integers\n", measure->dims_name[i]);
						logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, "Fill dimension '%s' with integers\n", measure->dims_name[i]);
					}
				}
				dimvar_ids[i] = tmp_var.varid;
				if (tmp_var.dims_id)
					free(tmp_var.dims_id);
				if (tmp_var.dims_length)
					free(tmp_var.dims_length);

				dim_array = NULL;

				//Create entry in dims and dim_insts
				dims[i].id_dimension = tot_dims[j].id_dimension;
				dims[i].id_container = tot_dims[j].id_container;
				dims[i].id_hierarchy = tot_dims[j].id_hierarchy;
				snprintf(dims[i].dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "%s", tot_dims[j].dimension_name);
				snprintf(dims[i].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE, "%s", tot_dims[j].dimension_type);

				dim_inst[i].id_dimension = tot_dims[j].id_dimension;
				dim_inst[i].fk_id_dimension_index = 0;
				dim_inst[i].fk_id_dimension_label = 0;
				dim_inst[i].id_grid = id_grid;
				dim_inst[i].id_dimensioninst = 0;
				//Modified to allow subsetting
				tmp_var.varsize = 1 + measure->dims_end_index[i] - measure->dims_start_index[i];
				dim_inst[i].size = tmp_var.varsize;
				dim_inst[i].concept_level = measure->dims_concept_level[i];
				dim_inst[i].unlimited = measure->dims_unlim[i] ? 1 : 0;

				if ((tmp_var.varid >= 0) && oph_nc_compare_nc_c_types(id_container_out, tmp_var.vartype, tot_dims[j].dimension_type)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension type in NC file doesn't correspond to the one stored in OphidiaDB\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_TYPE_MISMATCH_ERROR, measure->dims_name[i]);
				}
				//Modified to allow subsetting
				tmp_var.dims_start_index = &(measure->dims_start_index[i]);
				tmp_var.dims_end_index = &(measure->dims_end_index[i]);

				if (measure->order_src_path && (measure->dim_unlim == i))
					dim_array = measure->dim_unlim_array;
				else if (oph_nc_get_dim_array2
					 (id_container_out, ncid, tmp_var.varid, tot_dims[j].dimension_type, tmp_var.varsize, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index), &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_READ_ERROR, nc_strerror(retval));
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				if ((i == time_dimension) && strchr(tot_dims[j].base_time, OPH_DIM_DATA_FORMAT_CHECK)) {
					if (oph_dim_convert_data(tot_dims + j, tmp_var.varsize, dim_array)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to convert data for dimension %s\n", tot_dims[j].dimension_name);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to convert data for dimension %s\n", tot_dims[j].dimension_name);
						free(tot_dims);
						free(dims);
						free(dim_inst);
						free(dim_array);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
					base_time_conversion = 1;
				}

				if (oph_dim_insert_into_dimension_table(db_dimension, label_dimension_table_name, tot_dims[j].dimension_type, tmp_var.varsize, dim_array, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_ROW_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					free(dim_array);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (!measure->order_src_path || (measure->dim_unlim != i))
					free(dim_array);
				dim_inst[i].fk_id_dimension_label = dimension_array_id;	// Real dimension

				index_array = (long long *) malloc(tmp_var.varsize * sizeof(long long));
				for (kk = 0; kk < tmp_var.varsize; ++kk)
					index_array[kk] = 1 + kk;	// Non 'C'-like indexing
				if (oph_dim_insert_into_dimension_table(db_dimension, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, tmp_var.varsize, (char *) index_array, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIM_ROW_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					free(index_array);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				free(index_array);
				dim_inst[i].fk_id_dimension_index = dimension_array_id;	// Indexes

				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[i]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIMINST_INSERT_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				dim_inst[i].id_dimensioninst = dimension_array_id;
			}
			free(tot_dims);

			if (id_grid && oph_odb_dim_enable_grid(oDB, id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to enable grid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to enable grid\n");
				free(dims);
				free(dim_inst);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
		 /****************************
	      *  END - IMPORT DIMENSION  *
		  ***************************/
		}
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
	  /********************************
	   * INPUT PARAMETERS CHECK - END *
	   ********************************/

	  /********************************
	   *  DATACUBE CREATION - BEGIN   *
	   ********************************/
		//Set fragment string
		char *tmp = id_string;
		if (oph_ids_create_new_id_string(&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment ids string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_CREATE_ID_STRING_ERROR);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//Import source name
		oph_odb_source src;
		int id_src = 0;
		strncpy(src.uri, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig, OPH_ODB_CUBE_SOURCE_URI_SIZE);
		src.uri[OPH_ODB_CUBE_SOURCE_URI_SIZE] = 0;
		if (oph_odb_cube_insert_into_source_table(oDB, &src, &id_src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert source URI\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_SOURCE_URI_ERROR, src.uri);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		//Set datacube params
		cube.hostxdatacube = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number;
		cube.fragmentxdb = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number;
		cube.tuplexfragment = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		cube.id_container = id_container_out;
		strncpy(cube.measure, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure.varname, OPH_ODB_CUBE_MEASURE_SIZE);
		cube.measure[OPH_ODB_CUBE_MEASURE_SIZE] = 0;
		if (oph_nc_get_c_type(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure.vartype, cube.measure_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_VAR_TYPE_NOT_SUPPORTED, cube.measure_type);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		strncpy(cube.frag_relative_index_set, id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		cube.frag_relative_index_set[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE] = 0;
		cube.db_number = cube.hostxdatacube;
		cube.compressed = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->compressed;
		cube.id_db = NULL;
		//New fields
		cube.id_source = id_src;
		cube.level = 0;
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Check dimensions
		oph_odb_cubehasdim *cubedim = (oph_odb_cubehasdim *) malloc(measure->ndims * sizeof(oph_odb_cubehasdim));

		//Check if dimensions exists and insert
		for (i = 0; i < measure->ndims; i++) {
			//Find input dimension with same name of container dimension
			for (j = 0; j < dim_inst_num; j++) {
				if (!strncmp(dims[j].dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
					break;
			}
			if (j == dim_inst_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimension %s in OphidiaDB.\n", measure->dims_name[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DIMENSION_ODB_ERROR, measure->dims_name[i]);
				oph_odb_cube_free_datacube(&cube);
				free(dims);
				free(dim_inst);
				free(cubedim);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			cubedim[i].id_dimensioninst = dim_inst[j].id_dimensioninst;
			cubedim[i].explicit_dim = measure->dims_type[i];
			cubedim[i].level = measure->dims_oph_level[i];
		}

		//If everything was ok insert cubehasdim relations
		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &id_datacube_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DATACUBE_INSERT_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(cubedim);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		for (i = 0; i < measure->ndims; i++) {
			cubedim[i].id_datacube = id_datacube_out;
			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedim[i]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_CUBEHASDIM_INSERT_ERROR);
				free(dims);
				free(cubedim);
				free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
		}
		free(dims);
		free(cubedim);
		free(dim_inst);
	  /********************************
	   *   DATACUBE CREATION - END    *
	   ********************************/

	  /********************************
	   *   METADATA IMPORT - BEGIN    *
	   ********************************/
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->import_metadata) {
			//Check vocabulary and metadata key presence
			//NOTE: the type of the key values is fixed to text
			//Retrieve 'text' type id
			char key_type[OPH_COMMON_TYPE_SIZE];
			snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_METADATA_TYPE_TEXT);
			int id_key_type = 0, sid_key_type;
			if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &id_key_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_METADATATYPE_ID_ERROR);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			//Read all vocabulary keys
			MYSQL_RES *key_list = NULL;
			int num_rows = 0;
			if (oph_odb_meta_find_metadatakey_list(oDB, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_vocabulary, &key_list)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive key list\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_READ_KEY_LIST);
				mysql_free_result(key_list);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			num_rows = mysql_num_rows(key_list);

			//Load all keys into a hash table with variable:key form
			HASHTBL *key_tbl = NULL, *required_tbl = NULL;
			if (!(key_tbl = hashtbl_create(num_rows + 1, NULL))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT, "Key hash table");
				mysql_free_result(key_list);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			if (!(required_tbl = hashtbl_create(num_rows + 1, NULL))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT, "Required hash table");
				mysql_free_result(key_list);
				free(dimvar_ids);
				hashtbl_destroy(key_tbl);
				goto __OPH_EXIT_1;
			}

			char key_and_variable[OPH_COMMON_BUFFER_LEN];
			if (num_rows)	// The vocabulary is not empty
			{
				MYSQL_ROW row;
				//For each ROW - insert key into hashtable
				while ((row = mysql_fetch_row(key_list))) {
					memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
					snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", row[2] ? row[2] : "", row[1]);
					hashtbl_insert(key_tbl, key_and_variable, row[0]);
					hashtbl_insert(required_tbl, key_and_variable, row[3]);
				}
			}
			mysql_free_result(key_list);

			//Get global attributes from nc file
			char key[OPH_COMMON_BUFFER_LEN], value[OPH_COMMON_BUFFER_LEN], svalue[OPH_COMMON_BUFFER_LEN], *big_value = NULL;
			char *id_key, *keyptr, *keydup;
			int id_metadatainstance;
			keyptr = key;
			int natts = 0;
			size_t att_len = 0;
			if (nc_inq_varnatts(ncid, NC_GLOBAL, &natts)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of global attributes\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_NATTS_ERROR);
				hashtbl_destroy(key_tbl);
				hashtbl_destroy(required_tbl);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}

			nc_type xtype;

			//For each global attribute find the corresponding key
			for (i = 0; i < natts; i++) {
				memset(key, 0, OPH_COMMON_BUFFER_LEN);
				if (nc_inq_attname(ncid, NC_GLOBAL, i, keyptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a global attribute\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
#ifdef OPH_NC_SKIP_ATTRIBUTES
				if (!strcmp(key, OPH_NC_PROPERTIES))
					continue;
#endif
				// Check for attribute type
				if (nc_inq_atttype(ncid, NC_GLOBAL, keyptr, &xtype)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a global attribute\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (xtype != NC_CHAR) {
					switch (xtype) {
						case NC_BYTE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case NC_UBYTE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case NC_SHORT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case NC_USHORT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case NC_INT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case NC_UINT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case NC_UINT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case NC_INT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case NC_FLOAT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
							break;
						case NC_DOUBLE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
							break;
					}
					if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_METADATATYPE_ID_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				} else
					sid_key_type = id_key_type;

				memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
				snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, ":%s", key);
				memset(value, 0, OPH_COMMON_BUFFER_LEN);
				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

				//Check if the :key is inside the hashtble
				id_key = hashtbl_get(key_tbl, key_and_variable);

				//Insert key value into OphidiaDB
				keydup = strdup(key);

				if (xtype == NC_CHAR || xtype == NC_STRING) {
					//Check attribute length
					if (nc_inq_attlen(ncid, NC_GLOBAL, (const char *) key, &att_len)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute length from file\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute length from file\n");
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						if (keydup)
							free(keydup);
						goto __OPH_EXIT_1;
					}

					if (att_len >= OPH_COMMON_BUFFER_LEN) {

						if (!(big_value = (char *) malloc((att_len + 1) * sizeof(char)))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name,
								"Tmp big attribute buffer");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							if (keydup)
								free(keydup);
							if (big_value)
								free(big_value);
							goto __OPH_EXIT_1;
						}

						if (!keydup || nc_get_att(ncid, NC_GLOBAL, (const char *) key, (void *) big_value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							free(big_value);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}

					} else {

						if (!keydup || nc_get_att(ncid, NC_GLOBAL, (const char *) key, (void *) &value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}
						value[att_len] = 0;
					}
				} else {

					if (!keydup || nc_get_att(ncid, NC_GLOBAL, (const char *) key, (void *) &value)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						if (keydup)
							free(keydup);
						goto __OPH_EXIT_1;
					}
				}
				strcpy(key, keydup);
				free(keydup);

				switch (xtype) {
					case NC_BYTE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((char *) value));
						break;
					case NC_UBYTE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned char *) value));
						break;
					case NC_SHORT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((short *) value));
						break;
					case NC_USHORT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned short *) value));
						break;
					case NC_INT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((int *) value));
						break;
					case NC_UINT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned int *) value));
						break;
					case NC_UINT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((unsigned long long *) value));
						break;
					case NC_INT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((long long *) value));
						break;
					case NC_FLOAT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((float *) value));
						break;
					case NC_DOUBLE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((double *) value));
						break;
					default:
						strcpy(svalue, value);
				}

				//Insert metadata instance (also manage relation)
				if (oph_odb_meta_insert_into_metadatainstance_manage_tables
				    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, NULL, sid_key_type, id_user, big_value ? big_value : svalue, &id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_METADATAINSTANCE_ERROR, key, big_value ? big_value : svalue);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					if (big_value)
						free(big_value);
					goto __OPH_EXIT_1;
				}

				if (big_value)
					free(big_value);
				big_value = NULL;

				// Drop the metadata out of the hashtable
				if (id_key)
					hashtbl_remove(key_tbl, key_and_variable);
			}

			//Get local attributes from nc file for each dimension variable
			int ii;
			for (ii = 0; ii < measure->ndims; ii++) {
				if (dimvar_ids[ii] < 0)
					continue;
				natts = 0;
				if (nc_inq_varnatts(ncid, dimvar_ids[ii], &natts)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of local attributes\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_NATTS_LOCAL_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				//For each local attribute find the corresponding key
				for (i = 0; i < natts; i++) {
					memset(key, 0, OPH_COMMON_BUFFER_LEN);
					if (nc_inq_attname(ncid, dimvar_ids[ii], i, keyptr)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a local attribute\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_LOCAL_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
#ifdef OPH_NC_SKIP_ATTRIBUTES
					if (!strcmp(key, OPH_NC_BOUNDS))
						continue;
#endif
					// Check for attribute type
					if (nc_inq_atttype(ncid, dimvar_ids[ii], keyptr, &xtype)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a global attribute\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
					if (xtype != NC_CHAR) {
						switch (xtype) {
							case NC_BYTE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
								break;
							case NC_UBYTE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
								break;
							case NC_SHORT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
								break;
							case NC_USHORT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
								break;
							case NC_INT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
								break;
							case NC_UINT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
								break;
							case NC_UINT64:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
								break;
							case NC_INT64:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
								break;
							case NC_FLOAT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
								break;
							case NC_DOUBLE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
								break;
						}
						if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_METADATATYPE_ID_ERROR);
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							goto __OPH_EXIT_1;
						}
					} else
						sid_key_type = id_key_type;

					memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
					snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", measure->dims_name[ii], key);
					memset(value, 0, OPH_COMMON_BUFFER_LEN);
					memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

					//Check if the variable:key is inside the hashtble
					id_key = hashtbl_get(key_tbl, key_and_variable);

					//Insert key value into OphidiaDB
					keydup = strdup(key);
					if (xtype == NC_CHAR || xtype == NC_STRING) {
						//Check attribute length
						if (nc_inq_attlen(ncid, dimvar_ids[ii], (const char *) key, &att_len)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute length from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute length from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}

						if (att_len >= OPH_COMMON_BUFFER_LEN) {

							if (!(big_value = (char *) malloc((att_len + 1) * sizeof(char)))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name,
									"Tmp big attribute buffer");
								hashtbl_destroy(key_tbl);
								hashtbl_destroy(required_tbl);
								free(dimvar_ids);
								if (keydup)
									free(keydup);
								if (big_value)
									free(big_value);
								goto __OPH_EXIT_1;
							}

							if (!keydup || nc_get_att(ncid, dimvar_ids[ii], (const char *) key, (void *) big_value)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
								hashtbl_destroy(key_tbl);
								hashtbl_destroy(required_tbl);
								free(dimvar_ids);
								free(big_value);
								if (keydup)
									free(keydup);
								goto __OPH_EXIT_1;
							}

						} else {

							if (!keydup || nc_get_att(ncid, dimvar_ids[ii], (const char *) key, (void *) &value)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
								hashtbl_destroy(key_tbl);
								hashtbl_destroy(required_tbl);
								free(dimvar_ids);
								if (keydup)
									free(keydup);
								goto __OPH_EXIT_1;
							}
							value[att_len] = 0;
						}

					} else {

						if (!keydup || nc_get_att(ncid, dimvar_ids[ii], (const char *) key, (void *) &value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}
					}
					strcpy(key, keydup);
					free(keydup);

					switch (xtype) {
						case NC_BYTE:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((char *) value));
							break;
						case NC_UBYTE:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned char *) value));
							break;
						case NC_SHORT:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((short *) value));
							break;
						case NC_USHORT:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned short *) value));
							break;
						case NC_INT:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((int *) value));
							break;
						case NC_UINT:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned int *) value));
							break;
						case NC_UINT64:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((unsigned long long *) value));
							break;
						case NC_INT64:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((long long *) value));
							break;
						case NC_FLOAT:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((float *) value));
							break;
						case NC_DOUBLE:
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((double *) value));
							break;
						default:
							strcpy(svalue, value);
					}

					// Update base_time if any
					if (measure->base_time && (ii == measure->dim_unlim) && !strcmp(key, OPH_IN_PARAM_UNITS)) {
						char *pch = strchr(svalue, ' ');
						if (pch) {
							pch = strchr(pch + 1, ' ');
							if (pch) {
								pch++;
								strcpy(pch, measure->base_time);
							}
						}
					}
					if (base_time_conversion && (ii == time_dimension) && strchr(svalue, OPH_DIM_DATA_FORMAT_CHECK)) {
						char *pch = strchr(svalue, ' ');
						if (pch)
							snprintf(pch, OPH_COMMON_BUFFER_LEN - strlen(pch), "s %s %s", OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR, OPH_DIM_DATA_DEFAULT);
					}
					//Insert metadata instance (also manage relation)
					if (oph_odb_meta_insert_into_metadatainstance_manage_tables
					    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, measure->dims_name[ii], sid_key_type, id_user, big_value ? big_value : svalue,
					     &id_metadatainstance)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_METADATAINSTANCE_ERROR, key, big_value ? big_value : svalue);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						if (big_value)
							free(big_value);
						goto __OPH_EXIT_1;
					}

					if (big_value)
						free(big_value);
					big_value = NULL;

					// Drop the metadata out of the hashtable
					if (id_key)
						hashtbl_remove(key_tbl, key_and_variable);
				}
			}
			if (dimvar_ids) {
				free(dimvar_ids);
				dimvar_ids = NULL;
			}
			//Get local attributes from nc file for the measure variable
			natts = 0;
			if (nc_inq_varnatts(ncid, measure->varid, &natts)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of local attributes\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_NATTS_LOCAL_ERROR);
				hashtbl_destroy(key_tbl);
				hashtbl_destroy(required_tbl);
				goto __OPH_EXIT_1;
			}
			//For each local attribute find the corresponding key
			for (i = 0; i < natts; i++) {
				memset(key, 0, OPH_COMMON_BUFFER_LEN);
				if (nc_inq_attname(ncid, measure->varid, i, keyptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a local attribute\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_LOCAL_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
				// Check for attribute type
				if (nc_inq_atttype(ncid, measure->varid, keyptr, &xtype)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering a global attribute\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NC_ATTRIBUTE_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
				if (xtype != NC_CHAR) {
					switch (xtype) {
						case NC_BYTE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case NC_UBYTE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case NC_SHORT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case NC_USHORT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case NC_INT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case NC_UINT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case NC_UINT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case NC_INT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case NC_FLOAT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
							break;
						case NC_DOUBLE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
							break;
					}
					if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_METADATATYPE_ID_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						goto __OPH_EXIT_1;
					}
				} else
					sid_key_type = id_key_type;

				memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
				snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", measure->varname, key);
				memset(value, 0, OPH_COMMON_BUFFER_LEN);
				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

				//Check if the variable:key is inside the hashtble
				id_key = hashtbl_get(key_tbl, key_and_variable);

				//Insert key value into OphidiaDB
				keydup = strdup(key);
				if (xtype == NC_CHAR || xtype == NC_STRING) {
					//Check attribute length
					if (nc_inq_attlen(ncid, measure->varid, (const char *) key, &att_len)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute length from file\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute length from file\n");
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						if (keydup)
							free(keydup);
						goto __OPH_EXIT_1;
					}

					if (att_len >= OPH_COMMON_BUFFER_LEN) {

						if (!(big_value = (char *) malloc((att_len + 1) * sizeof(char)))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name,
								"Tmp big attribute buffer");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							if (keydup)
								free(keydup);
							if (big_value)
								free(big_value);
							goto __OPH_EXIT_1;
						}

						if (!keydup || nc_get_att(ncid, measure->varid, (const char *) key, (void *) big_value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(big_value);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}

					} else {

						if (!keydup || nc_get_att(ncid, measure->varid, (const char *) key, (void *) &value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							if (keydup)
								free(keydup);
							goto __OPH_EXIT_1;
						}
						value[att_len] = 0;
					}

				} else {

					if (!keydup || nc_get_att(ncid, measure->varid, (const char *) key, (void *) &value)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get attribute value from file\n");
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						if (keydup)
							free(keydup);
						goto __OPH_EXIT_1;
					}
				}
				strcpy(key, keydup);
				free(keydup);

				switch (xtype) {
					case NC_BYTE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((char *) value));
						break;
					case NC_UBYTE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned char *) value));
						break;
					case NC_SHORT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((short *) value));
						break;
					case NC_USHORT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned short *) value));
						break;
					case NC_INT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((int *) value));
						break;
					case NC_UINT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned int *) value));
						break;
					case NC_UINT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((unsigned long long *) value));
						break;
					case NC_INT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((long long *) value));
						break;
					case NC_FLOAT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((float *) value));
						break;
					case NC_DOUBLE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((double *) value));
						break;
					default:
						strcpy(svalue, value);
				}

				//Insert metadata instance (also manage relation)
				if (oph_odb_meta_insert_into_metadatainstance_manage_tables
				    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, measure->varname, sid_key_type, id_user, big_value ? big_value : svalue,
				     &id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_INSERT_METADATAINSTANCE_ERROR, key, big_value ? big_value : svalue);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					if (big_value)
						free(big_value);
					goto __OPH_EXIT_1;
				}

				if (big_value)
					free(big_value);
				big_value = NULL;

				// Drop the metadata out of the hashtable
				if (id_key)
					hashtbl_remove(key_tbl, key_and_variable);

				if (!strcmp(key, OPH_COMMON_FILLVALUE) && oph_odb_cube_update_missingvalue(oDB, id_datacube_out, id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set the missing value\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to set the missing value\n");
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
			}

			if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->check_compliance)	// Check if all the mandatory metadata are taken
			{
				short found_a_required_attribute = 0;
				while (!found_a_required_attribute && (id_key = hashtbl_pop_key(key_tbl))) {
					keyptr = hashtbl_get(required_tbl, id_key);
					found_a_required_attribute = keyptr ? (short) strtol(keyptr, NULL, 10) : 0;
					free(id_key);
				}
				if (found_a_required_attribute) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_COMPLIANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_COMPLIANCE_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
			}

			hashtbl_destroy(key_tbl);
			hashtbl_destroy(required_tbl);

			if (create_container)	// Check for time dimensions
			{
				for (i = 0; i < measure->ndims; i++) {
					ii = oph_odb_dim_set_time_dimension(oDB, id_datacube_out, measure->dims_name[i]);
					if (!ii)
						break;
					else if (ii != OPH_ODB_NO_ROW_FOUND) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_UPDATE_TIME_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_UPDATE_TIME_ERROR);
						goto __OPH_EXIT_1;
					}
				}
			} else if (not_exists && oph_odb_dim_set_time_dimension(oDB, id_datacube_out, measure->dims_name[time_dimension])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTNC_SET_TIME_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_SET_TIME_ERROR);
				goto __OPH_EXIT_1;
			}
		}
		if (dimvar_ids) {
			free(dimvar_ids);
			dimvar_ids = NULL;
		}
	  /********************************
	   *    METADATA IMPORT - END     *
	   ********************************/

	  /********************************
	   * DB INSTANCE CREATION - BEGIN *
	   ********************************/
		int dbmss_length, host_num;
		dbmss_length = host_num = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number;
		int *id_dbmss = NULL, *id_hosts = NULL;
		//Retreive ID dbms list
		if (oph_odb_stge_retrieve_dbmsinstance_id_list
		    (oDB, ioserver_type, id_host_partition, hidden, host_num, id_datacube_out, &id_dbmss, &id_hosts, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->policy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DBMS_LIST_ERROR);
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
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DBMS_ERROR, db.id_dbms);
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}
			db.dbms_instance = &dbms;

			if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server) {
				if (oph_dc_setup_dbms(&(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server), dbms.io_server_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_IOPLUGIN_SETUP_ERROR, db.id_dbms);
					free(id_dbmss);
					free(id_hosts);
					goto __OPH_EXIT_1;
				}
			}

			if (oph_dc_connect_to_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DBMS_CONNECTION_ERROR, dbms.id_dbms);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms));
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}

			if (oph_dc_generate_db_name(oDB->name, id_datacube_out, db.id_dbms, 0, 1, &db_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of Db instance  name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_STRING_BUFFER_OVERFLOW, "DB instance name", db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			strcpy(db.db_name, db_name);
			if (oph_dc_create_db(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create new db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_NEW_DB_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			//Insert new database instance and partitions
			if (oph_odb_stge_insert_into_dbinstance_partitioned_tables(oDB, &db, id_datacube_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dbinstance table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_DB_INSERT_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			oph_dc_disconnect_from_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server, &(dbms));
		}
		free(id_dbmss);
		free(id_hosts);
	  /********************************
	   *  DB INSTANCE CREATION - END  *
	   ********************************/

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = id_datacube_out;
		new_task.id_job = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		i = snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_DC_SQ_MULTI_INSERT_FRAG, "frag") - 1;
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->compressed)
			i += snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) - 1;
		else
			i += snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, OPH_DC_SQ_MULTI_INSERT_ROW) - 1;
		snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, ";");
		new_task.input_cube_number = 0;
		new_task.id_inputcube = NULL;
		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_TASK_INSERT_ERROR, new_task.operator);
			goto __OPH_EXIT_1;
		}

		id_datacube[0] = id_datacube_out;
		id_datacube[1] = id_container_out;
		id_datacube[2] = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number;
		id_datacube[3] = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number;
		id_datacube[4] = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		id_datacube[5] = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number;
		id_datacube[6] = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->number_unven_frag;

		flush = 0;
	}
      __OPH_EXIT_1:
	if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (!handle->proc_rank && flush) {
		while (id_container_out && create_container) {
			ophidiadb *oDB = &((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB;

			//Remove also grid related to container dimensions
			if (oph_odb_dim_delete_from_grid_table(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting grid related to container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_GRID_DELETE_ERROR);
				break;
			}
			//Delete container and related dimensions/ dimension instances
			if (oph_odb_fs_delete_from_container_table(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting input container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_CONTAINER_DELETE_ERROR);
				break;
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);

			if (oph_dim_delete_table(db, index_dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_DELETE_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_delete_table(db, label_dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_DIM_TABLE_DELETE_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

			break;
		}
		oph_odb_cube_delete_from_datacube_table(&((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB, id_datacube_out);
	}
	//Broadcast to all other processes the result
	MPI_Bcast(id_datacube, 7, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (!id_datacube[0] || !id_datacube[1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube[1], OPH_LOG_OPH_IMPORTNC_MASTER_TASK_INIT_FAILED_NO_CONTAINER, container_name);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_output_datacube = id_datacube[0];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container = id_datacube[1];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->host_number = id_datacube[2];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragxdb_number = id_datacube[3];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->tuplexfrag_number = id_datacube[4];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number = id_datacube[5];
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->number_unven_frag = id_datacube[6];

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int frag_total_number = ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->total_frag_number;

	//All processes compute the fragment number to work on
	int div_result = (frag_total_number) / (handle->proc_number);
	int div_remainder = (frag_total_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	} else {
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id += div_result;
		}
		if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id >= frag_total_number)
			((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_IMPORTNCS_operator_handle *oper_handle = (OPH_IMPORTNCS_operator_handle *) handle->operator_handle;

	if (!oper_handle->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (oper_handle->fragment_first_id < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int l;
	int id_datacube_out = oper_handle->id_output_datacube;

	oph_odb_fragment *new_frag = NULL;
	if (!(new_frag = (oph_odb_fragment *) calloc(oper_handle->fragment_number, sizeof(oph_odb_fragment)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int num_threads = (oper_handle->nthread <= oper_handle->fragment_number ? oper_handle->nthread : oper_handle->fragment_number);
	int res[num_threads];

	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_OPHIDIADB_CONFIGURATION_FILE, oper_handle->container_input);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		free(new_frag);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_OPHIDIADB_CONNECTION_ERROR, oper_handle->container_input);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		free(new_frag);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//Compute DB list starting position and number of rows
	int start_position = (int) floor((double) oper_handle->fragment_first_id / oper_handle->fragxdb_number);
	int row_number = (int) ceil((double) (oper_handle->fragment_first_id + oper_handle->fragment_number) / oper_handle->fragxdb_number) - start_position;

	if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube_out, start_position, row_number, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		free(new_frag);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	struct _thread_struct {
		OPH_IMPORTNCS_operator_handle *oper_handle;
		unsigned int current_thread;
		unsigned int total_threads;
		int id_datacube;
		int proc_rank;
		oph_odb_fragment *frags;
		oph_odb_db_instance_list *dbs;
		oph_odb_dbms_instance_list *dbmss;
	};
	typedef struct _thread_struct thread_struct;


	void *exec_thread(void *ts) {

		OPH_IMPORTNCS_operator_handle *oper_handle = ((thread_struct *) ts)->oper_handle;
		int l = ((thread_struct *) ts)->current_thread;
		int num_threads = ((thread_struct *) ts)->total_threads;
		int id_datacube_out = ((thread_struct *) ts)->id_datacube;
		int proc_rank = ((thread_struct *) ts)->proc_rank;
		oph_odb_fragment *new_frag = ((thread_struct *) ts)->frags;
		oph_odb_db_instance_list *dbs = ((thread_struct *) ts)->dbs;
		oph_odb_dbms_instance_list *dbmss = ((thread_struct *) ts)->dbmss;

		int fragxthread = (int) floor((double) (oper_handle->fragment_number / num_threads));
		int remainder = (int) oper_handle->fragment_number % num_threads;

		//Compute starting number of fragments inserted by other threads
		unsigned int current_frag_count = l * fragxthread + (l < remainder ? l : remainder);

		//Update number of fragments to be inserted
		if (l < remainder)
			fragxthread += 1;

		//Compute relative DB list starting position and number of rows
		int start_position = (int) floor((double) (oper_handle->fragment_first_id + current_frag_count) / oper_handle->fragxdb_number);
		int row_number = (int) ceil((double) (oper_handle->fragment_first_id + current_frag_count + fragxthread) / oper_handle->fragxdb_number) - start_position;
		int rel_start = start_position - (int) floor((double) oper_handle->fragment_first_id / oper_handle->fragxdb_number);
		int rel_row_num = row_number + rel_start;

		int i, k;
		int res = OPH_ANALYTICS_OPERATOR_SUCCESS;

		char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];

		int frag_to_insert = 0;
		int frag_count = 0;
		int actual_tuplexfrag_number = 0;

		oph_ioserver_handler *server = NULL;
		if (oph_dc_setup_dbms_thread(&(server), dbmss->value[0].io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_IOPLUGIN_SETUP_ERROR, dbmss->value[0].id_dbms);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DBMS
		for (i = rel_start; i < rel_row_num && res == OPH_ANALYTICS_OPERATOR_SUCCESS; i++) {
			if (oph_dc_connect_to_dbms(server, &(dbmss->value[i]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_DB_SELECTION_ERROR, (dbs->value[i]).db_name);
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
				oph_dc_cleanup_dbms(server);
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				mysql_thread_end();
				break;
			}
			//Compute number of fragments to insert in DB
			frag_to_insert = oper_handle->fragxdb_number - (oper_handle->fragment_first_id + current_frag_count + frag_count - start_position * oper_handle->fragxdb_number);

			//For each fragment
			for (k = 0; k < frag_to_insert && res == OPH_ANALYTICS_OPERATOR_SUCCESS; k++) {

				//Set new fragment
				new_frag[current_frag_count + frag_count].id_datacube = id_datacube_out;
				new_frag[current_frag_count + frag_count].id_db = dbs->value[i].id_db;
				new_frag[current_frag_count + frag_count].frag_relative_index = oper_handle->fragment_first_id + current_frag_count + frag_count + 1;

				//For each fragment define correct number of rows
				if (oper_handle->number_unven_frag == 0) {
					actual_tuplexfrag_number = oper_handle->tuplexfrag_number;
				} else {
					if (new_frag[current_frag_count + frag_count].frag_relative_index <= oper_handle->number_unven_frag) {
						actual_tuplexfrag_number = oper_handle->tuplexfrag_number;
					} else {
						actual_tuplexfrag_number = ((oper_handle->tuplexfrag_number / oper_handle->int_dim_product) - 1) * oper_handle->int_dim_product;
					}
				}
				int frag_already_inserted = oper_handle->fragment_first_id + current_frag_count + frag_count;

				new_frag[current_frag_count + frag_count].key_start =
				    (oper_handle->number_unven_frag - frag_already_inserted > 0 ? frag_already_inserted : oper_handle->number_unven_frag)
				    * oper_handle->tuplexfrag_number + (frag_already_inserted - oper_handle->number_unven_frag >
									0 ? frag_already_inserted - oper_handle->number_unven_frag : 0) * actual_tuplexfrag_number + 1;
				new_frag[current_frag_count + frag_count].key_end = (new_frag[current_frag_count + frag_count].key_start - 1) + actual_tuplexfrag_number;
				new_frag[current_frag_count + frag_count].db_instance = &(dbs->value[i]);

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, proc_rank, (current_frag_count + frag_count + 1), &fragment_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_STRING_BUFFER_OVERFLOW, "fragment name", fragment_name);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				strcpy(new_frag[current_frag_count + frag_count].fragment_name, fragment_name);
				//Create and populate fragment
				if (oph_nc_populate_fragment_from_nc5
				    (server, &(new_frag[current_frag_count + frag_count]), oper_handle->nc_file_path_orig, actual_tuplexfrag_number, oper_handle->compressed,
				     (NETCDF_var *) & (oper_handle->measure))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_FRAG_POPULATE_ERROR,
						new_frag[current_frag_count + frag_count].fragment_name, "");
					res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				frag_count++;
				if (frag_count == fragxthread)
					break;
			}
			start_position++;

			oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));

			if (res != OPH_ANALYTICS_OPERATOR_SUCCESS) {
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
			}
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			oph_dc_cleanup_dbms(server);
			mysql_thread_end();
		}

		int *ret_val = (int *) malloc(sizeof(int));
		*ret_val = res;
		pthread_exit((void *) ret_val);
	}

	pthread_t threads[num_threads];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	thread_struct ts[num_threads];

	int rc;
	for (l = 0; l < num_threads; l++) {
		ts[l].oper_handle = oper_handle;
		ts[l].total_threads = num_threads;
		ts[l].proc_rank = handle->proc_rank;
		ts[l].id_datacube = id_datacube_out;
		ts[l].current_thread = l;
		ts[l].frags = new_frag;
		ts[l].dbs = &dbs;
		ts[l].dbmss = &dbmss;
		rc = pthread_create(&threads[l], &attr, exec_thread, (void *) &(ts[l]));
		if (rc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create thread %d: %d.\n", l, rc);
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, "Unable to create thread %d: %d.\n", l, rc);
		}
	}

	pthread_attr_destroy(&attr);
	void *ret_val = NULL;
	for (l = 0; l < num_threads; l++) {
		rc = pthread_join(threads[l], &ret_val);
		res[l] = *((int *) ret_val);
		free(ret_val);
		if (rc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while joining thread %d: %d.\n", l, rc);
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, "Error while joining thread %d: %d.\n", l, rc);
		}
	}

	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);

	//Insert all new fragment
	if (oph_odb_stge_insert_into_fragment_table2(&oDB_slave, new_frag, oper_handle->fragment_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, "Unable to update fragment table.\n");
		oper_handle->execute_error = 1;
		free(new_frag);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	oph_odb_free_ophidiadb_thread(&oDB_slave);
	free(new_frag);
	mysql_thread_end();

	for (l = 0; l < num_threads; l++) {
		if (res[l] != OPH_ANALYTICS_OPERATOR_SUCCESS) {
			oper_handle->execute_error = 1;
			return res[l];
		}
	}

	oper_handle->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_IMPORTNCS_operator_handle *oper_handle = (OPH_IMPORTNCS_operator_handle *) handle->operator_handle;

	if (!oper_handle->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	short int proc_error = oper_handle->execute_error;
	int id_datacube = oper_handle->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid(oper_handle->id_input_container, oper_handle->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, oper_handle->id_input_container, oper_handle->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_IMPORTNCS)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTNCS, "Output Cube", jsonbuf)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, "ADD TEXT error\n");
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
			ophidiadb *oDB = &(oper_handle->oDB);
			oph_odb_datacube cube;
			oph_odb_cube_init_datacube(&cube);

			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, id_datacube, &cube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_DATACUBE_READ_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_MASTER_TASK_INIT_FAILED);
		} else {
			if (oper_handle->fragment_first_id >= 0 || handle->proc_rank == 0) {
				//Partition fragment relative index string
				char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
				char *new_ptr = new_id_string;
				if (oph_ids_get_substring_from_string(id_string, oper_handle->fragment_first_id, oper_handle->fragment_number, &new_ptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_IMPORTNC_ID_STRING_SPLIT_ERROR);
				} else {
					//Delete fragments
					int num_threads = (oper_handle->nthread <= oper_handle->fragment_number ? oper_handle->nthread : oper_handle->fragment_number);

					int start_position = (int) floor((double) oper_handle->fragment_first_id / oper_handle->fragxdb_number);
					int row_number = (int)
					    ceil((double)
						 (oper_handle->fragment_first_id + oper_handle->fragment_number) / oper_handle->fragxdb_number) - start_position;

					if (oph_dproc_delete_data(id_datacube, oper_handle->id_input_container, new_id_string, start_position, row_number, num_threads)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
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
			oph_dproc_clean_odb(&(oper_handle->oDB), id_datacube, oper_handle->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, retval;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_free_ophidiadb(&((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->container_input = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server)
		oph_dc_cleanup_dbms(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->server);


	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	}

	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths) {
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths,
						      ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths_num);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_paths = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	}
	if ((retval = nc_close(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids[0])))
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %s\n", nc_strerror(retval));
	free(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->ncids);

	NETCDF_var *measure = ((NETCDF_var *) & (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->measure));

	if (measure->dims_name) {
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_name[i]) {
				free((char *) measure->dims_name[i]);
				measure->dims_name[i] = NULL;
			}
		}
		free(measure->dims_name);
		measure->dims_name = NULL;
	}

	if (measure->dims_id) {
		free((int *) measure->dims_id);
		measure->dims_id = NULL;
	}

	if (measure->dims_unlim) {
		free((char *) measure->dims_unlim);
		measure->dims_unlim = NULL;
	}

	if (measure->dims_length) {
		free((size_t *) measure->dims_length);
		measure->dims_length = NULL;
	}

	if (measure->dims_type) {
		free((short int *) measure->dims_type);
		measure->dims_type = NULL;
	}

	if (measure->dims_oph_level) {
		free((short int *) measure->dims_oph_level);
		measure->dims_oph_level = NULL;
	}

	if (measure->dims_start_index) {
		free((int *) measure->dims_start_index);
		measure->dims_start_index = NULL;
	}

	if (measure->dims_end_index) {
		free((int *) measure->dims_end_index);
		measure->dims_end_index = NULL;
	}

	if (measure->dims_concept_level) {
		free((char *) measure->dims_concept_level);
		measure->dims_concept_level = NULL;
	}

	if (measure->order_src_path)
		free(measure->order_src_path);

	if (measure->dim_unlim_array)
		free(measure->dim_unlim_array);

	if (measure->base_time)
		free(measure->base_time);

	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->partition_input) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->partition_input);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->partition_input = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy) {
		free((int *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->base_time = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->units = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->calendar = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	}
	if (((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description);
		((OPH_IMPORTNCS_operator_handle *) handle->operator_handle)->description = NULL;
	}

	free((OPH_IMPORTNCS_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
