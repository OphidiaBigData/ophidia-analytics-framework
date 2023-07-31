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

#include <math.h>
#include <ctype.h>

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_IMPORTFITS_operator.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_driver_procedure_library.h"

int check_subset_string(char *curfilter, int i, FITS_var * measure, int is_index)
{

	int ii;
	char *endfilter = strchr(curfilter, OPH_DIM_SUBSET_SEPARATOR2);
	if (!endfilter) {
		//Only single point
		//Check curfilter
		if (strlen(curfilter) < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//Always index
		if (is_index) {
			//Input filter is index
			for (ii = 0; ii < (int) strlen(curfilter); ii++) {
				if (!isdigit(curfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer values allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			measure->dims_start_index[i] = (int) (strtol(curfilter, (char **) NULL, 10));
			measure->dims_end_index[i] = measure->dims_start_index[i];
		}

	} else {
		//Start and end point
		char *startfilter = curfilter;
		if (endfilter) {
			*endfilter = '\0';
			endfilter++;
		} else
			endfilter = startfilter;

		if (strlen(startfilter) < 1 || strlen(endfilter) < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//Always index
		if (is_index) {
			//Input filter is index         
			for (ii = 0; ii < (int) strlen(startfilter); ii++) {
				if (!isdigit(startfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}

			for (ii = 0; ii < (int) strlen(endfilter); ii++) {
				if (!isdigit(endfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			measure->dims_start_index[i] = (int) (strtol(startfilter, (char **) NULL, 10));
			measure->dims_end_index[i] = (int) (strtol(endfilter, (char **) NULL, 10));
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

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

	if (!(handle->operator_handle = (OPH_IMPORTFITS_operator_handle *) calloc(1, sizeof(OPH_IMPORTFITS_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->create_container = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->check_grid = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->import_metadata = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->check_compliance = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu = 1;
	FITS_var *fits_measure = &(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure);
	fits_measure->dims_name = NULL;
	fits_measure->dims_id = NULL;
	fits_measure->dims_length = NULL;
	fits_measure->dims_type = NULL;
	fits_measure->dims_oph_level = NULL;
	fits_measure->dims_start_index = NULL;
	fits_measure->dims_end_index = NULL;
	fits_measure->dims_concept_level = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run = 1;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_vocabulary = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->base_time = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->units = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->calendar = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->leap_year = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->leap_month = 2;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->memory_size = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->policy = 0;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));
	if ((value = strchr(container_name, OPH_COMMON_DIESIS)))
		*value = 0;
	if (!strlen(container_name))
		container_name = "NO-CONTAINER";

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_POLICY);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_POLICY_PORT))
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->policy = 1;
	else if (strcmp(value, OPH_COMMON_POLICY_RR)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->import_metadata = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *input = hashtbl_get(task_tbl, OPH_IN_PARAM_INPUT);
	if (input && strlen(input))
		value = input;
	else if (!strlen(value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The param '%s' is mandatory; at least the param '%s' needs to be set\n", OPH_IN_PARAM_SRC_FILE_PATH, OPH_IN_PARAM_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "fits file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig = strdup(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "fits file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FITS_HDU);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FITS_HDU);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_FITS_HDU);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *_container_name = NULL;
	if (!strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->create_container = 1;
		char _fits_file_path[1 + strlen(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path)];
		strcpy(_fits_file_path, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path);
		if ((value = strchr(_fits_file_path, OPH_COMMON_DIESIS)))
			*value = 0;
		char *pointer = strrchr(_fits_file_path, '/');
		while (pointer && (strlen(pointer) <= 1)) {
			*pointer = 0;
			pointer = strrchr(_fits_file_path, '/');
		}
		if (!pointer) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Needed input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Needed input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		_container_name = strdup(pointer + 1);
		container_name = _container_name;
	} else
		container_name = value;
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(container_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");
		if (_container_name)
			free(_container_name);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (_container_name)
		free(_container_name);
	container_name = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input;

	if (strstr(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strstr(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path, "http://")
	    && !strstr(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path, "https://")) {
		char *pointer = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			char tmp[OPH_COMMON_BUFFER_LEN];
			if (*pointer != '/') {
				value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (*value != '/') {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", value);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", value);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (strlen(value) > 1) {
					if (strstr(value, "..")) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", value + 1, pointer);
					free(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path);
					((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path = strdup(tmp);
					pointer = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path;
				}
			}
			if (oph_pid_get_base_src_path(&value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
			free(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path);
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path = strdup(tmp);
			free(value);
		}
	}
	if (oph_pid_get_memory_size(&(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->memory_size))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OPHIDIADB_CONFIGURATION_FILE, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "cwd");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HOST_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HOST_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HOST_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number == 0)
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number = -1;	// All the host of the partition

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FRAGMENENT_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FRAGMENENT_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_FRAGMENENT_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number == 0)
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number = -1;	// The maximum number of fragments

	//Additional check (all distrib arguments must be bigger than 0 or at least -1 if default values are given)
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number == 0 || ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAG_PARAMS_ERROR, container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FRAG_PARAMS_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	FITS_var *measure = ((FITS_var *) & (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		char message[OPH_COMMON_BUFFER_LEN];
		snprintf(message, OPH_COMMON_BUFFER_LEN, "No measure name specified; using default 'image'");
		printf("%s\n", message);
		if (oph_json_is_objkey_printable
		    (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num,
		     OPH_JSON_OBJKEY_IMPORTFITS_SUMMARY)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTFITS_SUMMARY, "Message", message)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}
	strncpy(measure->varname, value, strlen(value));

	int i;
	char **exp_dim_names = NULL;
	char **imp_dim_names = NULL;
	int exp_number_of_dim_names = 0;
	int imp_number_of_dim_names = 0;

	//Open fits file
	int j;
	int status = 0;
	char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code
	fits_open_file(&(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr), ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path, READONLY, &status);
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open fits file '%s': %s\n", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path, err_text);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FITS_OPEN_ERROR_NO_CONTAINER, container_name, err_text);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	fitsfile *fptr = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr;
	//Extract measured variable information
	measure->varid = 0;
	//Get information from id
	// Only IMGs are supported
	// Fits files are formed by various data HDUs. Get data from the provided (input) HDU (default = primary HDU)

	int hdunum;
	fits_get_num_hdus(fptr, &hdunum, &status);
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", err_text);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FITS_IFITS_VAR_ERROR_NO_CONTAINER, container_name, err_text);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (hdunum < ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find the required HDU: %d\n", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "[CONTAINER: %s] Unable to find the required HDU: %d\n", container_name,
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int hdutype = 0;
	int naxis = -1;

	// Move to and get the type of the hdu. It should be IMAGE
	fits_movabs_hdu(fptr, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->hdu, &hdutype, &status);
	if (hdutype != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not IMG HDU provided\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "[CONTAINER: %s] Not IMG HDU provided\n", container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	fits_get_img_dim(fptr, &naxis, &status);
	if (naxis == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to identify the number of axis\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "[CONTAINER: %s] Unable to identify the number of axis\n", container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	// status contains the error code; if status > 0 the subsequent calls of fits functions will be skipped
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", err_text);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FITS_IFITS_VAR_ERROR_NO_CONTAINER, container_name, err_text);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	// fptr points to the correct hdu

	measure->ndims = naxis;

	fits_get_img_equivtype(fptr, &(measure->vartype), &status);
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", err_text);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FITS_IFITS_VAR_ERROR_NO_CONTAINER, container_name, err_text);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ndims = naxis;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (strncmp(value, OPH_IMPORTFITS_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTFITS_DIMENSION_DEFAULT, strlen(OPH_IMPORTFITS_DIMENSION_DEFAULT))) {
		//If implicit is different from auto use standard approach
		if (oph_tp_parse_multiple_value_param(value, &imp_dim_names, &imp_number_of_dim_names)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		measure->nimp = imp_number_of_dim_names;

		if (measure->nimp > ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (strncmp(value, OPH_IMPORTFITS_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTFITS_DIMENSION_DEFAULT, strlen(OPH_IMPORTFITS_DIMENSION_DEFAULT))) {
			//Explicit is not auto, use standard approach
			if (oph_tp_parse_multiple_value_param(value, &exp_dim_names, &exp_number_of_dim_names)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
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
		measure->ndims = measure->nexp + measure->nimp;
	} else {
		//Implicit dimension is auto, import as fits file order: NAXISn, NAXISn-1, ..., NAXIS1
		measure->nimp = 1;
		measure->nexp = ndims - 1;
		measure->ndims = ndims;
		exp_dim_names = imp_dim_names = NULL;
		exp_number_of_dim_names = imp_number_of_dim_names = 0;
	}

	char *tmp_concept_levels = NULL;
	if (!(tmp_concept_levels = (char *) malloc(measure->ndims * sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "Tmp concept levels");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(tmp_concept_levels, 0, measure->ndims * sizeof(char));
	for (i = 0; i < measure->nexp; i++)
		tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;

	for (i = measure->nexp; i < measure->ndims; i++)
		tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;

	if (ndims != measure->ndims) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (!(measure->dims_name = (char **) malloc(measure->ndims * sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_name");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(measure->dims_name, 0, measure->ndims * sizeof(char *));

	if (!(measure->dims_length = (long *) malloc(measure->ndims * sizeof(long)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_length");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_type = (short int *) malloc(measure->ndims * sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_type");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_oph_level");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_concept_level = (char *) calloc(measure->ndims, sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_concept_level");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	memset(measure->dims_concept_level, 0, measure->ndims * sizeof(char));

	//For fits files, there are no dimension ids.
	if (!(measure->dims_id = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_id");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int counter = ndims;
	for (i = 0; i < ndims; i++) {
		// For fits file more internal dimension is NAXIS1, more external is NAXISn
		// see cfitsio.pdf page 51
		measure->dims_id[i] = counter;
		counter--;
	}

	//Extract dimensions information and check names provided by task string
	char *dimname;
	short int flag = 0;
	// Get the size of the dimensions
	fits_get_img_size(fptr, ndims, measure->dims_length, &status);
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", err_text);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FITS_IFITS_VAR_ERROR_NO_CONTAINER, container_name, err_text);
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	counter = ndims;
	for (i = 0; i < ndims; i++) {
		// Dimensions name are: NAXIS1, NAXIS2, ..., NAXISn
		// For fits file more internal dimension is NAXIS1, more external is NAXISn
		measure->dims_name[i] = (char *) calloc(16, sizeof(char));
		snprintf(measure->dims_name[i], 16, "NAXIS%d", counter);
		counter--;
	}

	int level = 1;
	if (exp_dim_names != NULL) {
		for (i = 0; i < measure->nexp; i++) {
			flag = 0;
			dimname = exp_dim_names[i];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension name in fits files should be NAXISn (e.g. NASIX1); found %s\n in %s variable\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname,
					measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_oph_level[j] = level++;
			measure->dims_type[j] = 1;
			measure->dims_concept_level[j] = tmp_concept_levels[i];
		}
	} else {
		if (imp_dim_names != NULL) {
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
					measure->dims_oph_level[i] = level++;
					measure->dims_type[i] = 1;
					measure->dims_concept_level[i] = tmp_concept_levels[k];
					k++;
				}
			}
		} else {
			//Use order in nc file
			for (i = 0; i < measure->nexp; i++) {

				measure->dims_oph_level[i] = level++;
				measure->dims_type[i] = 1;
				measure->dims_concept_level[i] = tmp_concept_levels[i];
			}
		}
	}

	level = 1;
	if (imp_dim_names != NULL) {
		for (i = measure->nexp; i < measure->ndims; i++) {
			flag = 0;
			dimname = imp_dim_names[i - measure->nexp];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension name in fits files should be NAXISn (e.g. NASIX1); found %s\n in %s variable\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname,
					measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
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
			measure->dims_concept_level[i] = tmp_concept_levels[i];
			measure->dims_type[i] = 0;
			measure->dims_oph_level[i] = level++;
		}
	}

	oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
	oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
	free(tmp_concept_levels);

//ADDED TO MANAGE SUBSETTED IMPORT

	char **sub_dims = 0;
	char **sub_filters = 0;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;

	// Only subsetting by index is allowed
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int issubset = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int isfilter = 0;

	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		isfilter = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check dimension names
	for (i = 0; i < number_of_sub_dims; i++) {
		dimname = sub_dims[i];
		for (j = 0; j < ndims; j++)
			if (!strcmp(dimname, measure->dims_name[j]))
				break;
		if (j == ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension '%s' related to variable '%s' in in fits file\n", dimname, measure->varname);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	//Check the sub_filters strings
	for (i = 0; i < number_of_sub_dims; i++) {
		if (strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2) != strrchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided range are not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB;

		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OPHIDIADB_CONFIGURATION_FILE, container_name);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OPHIDIADB_CONNECTION_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		// Check for container PID
		int id_container_out = 0;
		char *value2 = strrchr(container_name, '/');
		if (value2)
			id_container_out = (int) strtol(1 + value2, NULL, 10);
		if (id_container_out && !oph_odb_fs_retrieve_container_name_from_container(oDB, id_container_out, &container_name, NULL)) {
			free(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input = container_name;
		}
	}
	//Alloc space for subsetting parameters
	if (!(measure->dims_start_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_start_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_end_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

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
		} else if ((ii = check_subset_string(curfilter, i, measure, 1))) {
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return ii;
		} else if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
			   || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
	oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);

	//Check explicit dimension oph levels (all values in interval [1 - nexp] should be supplied)
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_EXP_DIM_LEVEL_ERROR, container_name);
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_COMPRESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->compressed = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SIMULATE_RUN);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SIMULATE_RUN);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SIMULATE_RUN);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NO_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run = 0;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PARTITION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PARTITION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_PARTITION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->partition_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input partition");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IOSERVER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "I/O server type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB;

		if (!(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(measure->ndims * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, measure->ndims * sizeof(int));

		//Retrieve hierarchy ID
		int id_hierarchy = 0;
		if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_DEFAULT_HIERARCHY, &id_hierarchy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, OPH_COMMON_DEFAULT_HIERARCHY);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		for (i = 0; i < measure->ndims; i++)
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] = id_hierarchy;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int id_datacube[6] = { 0, 0, 0, 0, 0, 0 };

	int i, id_datacube_out = 0, id_container_out = 0;

	FITS_var *measure = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure;
	//****COMPUTE DEFAULT fragxdb AND tuplexfrag NUMBER (START)*********//
	//Compute tuplexfragment as the number of values of last (internal) explicit dimension
	//Compute fragxdb_number as the number of values of the explicit dimensions excluding the last one
	//Find the last explicit dimension checking oph_value
	short int max_lev = 0;
	short int last_dimid = 0;

	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number > 1 || ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number > 1) {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
	} else {


		for (i = 0; i < measure->ndims; i++) {
			//Consider only explicit dimensions
			if (measure->dims_type[i]) {
				if (measure->dims_oph_level[i] > max_lev) {
					last_dimid = measure->dims_id[i];
					max_lev = measure->dims_oph_level[i];
					//Modified to allow subsetting
					if (measure->dims_end_index[i] == measure->dims_start_index[i])
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
					else
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				}
			}
		}
	}

	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number = 1;
	for (i = 0; i < measure->ndims; i++) {
		//Consider only explicit dimensions
		//Modified to allow subsetting
		if (measure->dims_type[i] && measure->dims_id[i] != last_dimid) {
			if (measure->dims_end_index[i] == measure->dims_start_index[i])
				continue;
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
		}
	}
	//****COMPUTE DEFAULT fragxdb AND tuplexfrag NUMBER (END)*********//

	// Compute array_length
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->array_length = 1;
	for (i = 0; i < measure->ndims; i++) {
		//Consider only implicit dimensions
		//Modified to allow subsetting
		if (!measure->dims_type[i]) {
			if (measure->dims_end_index[i] == measure->dims_start_index[i])
				continue;
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->array_length *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
		}
	}

	char *container_name = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB;

		int i, j;
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

		int container_exists = 0;
		int last_insertd_id = 0;
		int *host_number = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number;
		int *fragxdb_number = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number;
		char *host_partition = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->partition_input;
		char *ioserver_type = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type;
		int run = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run;

		//Retrieve user id
		char *username = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user;
		int id_user = 0;
		if (oph_odb_user_retrieve_user_id(oDB, username, &id_user)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive user id\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_USER_ID_ERROR);
			goto __OPH_EXIT_1;
		}

	  /********************************
	   *INPUT PARAMETERS CHECK - BEGIN*
	   ********************************/
		int exist_part = 0;
		int nhost = 0;
		int frag_param_error = 0;
		int final_frag_number = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number;

		int max_frag_number = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number * ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number;

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
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number, host_partition);
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
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Failed to load partition '%s'!\n", host_partition);
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
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
							((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number, host_partition);
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
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name,
						host_partition);
					goto __OPH_EXIT_1;
				} else {
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name,
						host_partition);
					frag_param_error = 1;
				}
			}

			if (*fragxdb_number <= 0) {
				user_arg_prod = (1 * 1);
				if (final_frag_number < user_arg_prod) {
					//If import is executed then return error, else simply return a message
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
							final_frag_number);
						goto __OPH_EXIT_1;
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						frag_param_error = 1;
					}
				} else {
					if (final_frag_number % user_arg_prod != 0) {
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							goto __OPH_EXIT_1;
						} else {
							frag_param_error = 1;
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
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
				}
			} else {
				//If user specified fragxdb_number then check if frag number is lower than product of parameters                       
				user_arg_prod = (1 * (*fragxdb_number));
				if (final_frag_number < user_arg_prod) {
					//If import is executed then return error, else simply return a message
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
							final_frag_number);
						goto __OPH_EXIT_1;
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						frag_param_error = 1;
					}
				} else {
					if (final_frag_number % user_arg_prod != 0) {
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							goto __OPH_EXIT_1;
						} else {
							frag_param_error = 1;
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
						}
					} else {
						admissible_frag_number = final_frag_number / user_arg_prod;
						if (admissible_frag_number <= nhost) {
							*host_number = admissible_frag_number;
						} else {
							//Get highest divisor for host_number
							int ii = 0;
							for (ii = nhost; ii > 0; ii--) {
								if (admissible_frag_number % ii == 0)
									break;
							}
							*host_number = ii;
							//Since fragxdb is fixed recompute tuplexfrag
							((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number =
							    (int) ceilf((float) max_frag_number / ((*host_number) * user_arg_prod));
						}
					}
				}
			}
		} else {
			if (*fragxdb_number <= 0) {
				user_arg_prod = ((*host_number) * 1);
				if (final_frag_number < user_arg_prod) {
					//If import is executed then return error, else simply return a message
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
							final_frag_number);
						goto __OPH_EXIT_1;
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
						frag_param_error = 1;
					}
				} else {
					if (final_frag_number % user_arg_prod != 0) {
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							goto __OPH_EXIT_1;
						} else {
							frag_param_error = 1;
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
						}
					} else {
						//Compute fragments
						*fragxdb_number = final_frag_number / user_arg_prod;
					}
				}
			} else {
				//User has set all parameters - in this case allow further fragmentation
				user_arg_prod = ((*host_number) * (*fragxdb_number));
				if (max_frag_number < user_arg_prod) {
					//If import is executed then return error, else simply return a message
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, max_frag_number);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name,
							max_frag_number);
						goto __OPH_EXIT_1;
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, max_frag_number);
						frag_param_error = 1;
					}
				} else {
					if (max_frag_number % user_arg_prod != 0) {
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
							goto __OPH_EXIT_1;
						} else {
							frag_param_error = 1;
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTFITS_FRAGMENTATION_ERROR);
						}
					} else {
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number = (int) ceilf((float) max_frag_number / user_arg_prod);
					}
				}
			}
		}

		if (frag_param_error) {
			//Check how many DBMS and HOST are available into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ioserver_type, id_host_partition, &nhost) || !nhost) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or server type and partition are not available!\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name, host_partition);
				goto __OPH_EXIT_1;
			}

			int ii = 0;
			for (ii = nhost; ii > 0; ii--) {
				if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number % ii == 0)
					break;
			}

			//Simulate simple arguments
			*host_number = ii;
			*fragxdb_number = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number / ii;
		} else {
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number = ((*host_number) * (*fragxdb_number));
		}

		if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run) {
			char message[OPH_COMMON_BUFFER_LEN] = { 0 };
			int len = 0;

			if (frag_param_error)
				printf("Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				printf("Specified parameters are:\n");
			printf("\tNumber of hosts: %d\n", *host_number);
			printf("\tNumber of fragments per database: %d\n", *fragxdb_number);
			printf("\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (frag_param_error)
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters are:\n");
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of hosts: %d\n", *host_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of fragments per database: %d\n", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (oph_json_is_objkey_printable
			    (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num,
			     OPH_JSON_OBJKEY_IMPORTFITS)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTFITS, "Fragmentation parameters", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, "ADD TEXT error\n");
				}
			}
			goto __OPH_EXIT_1;
		}
		//Check if are available DBMS and HOST number into specified partition and of server type
		if ((oph_odb_stge_check_number_of_host_dbms(oDB, ioserver_type, id_host_partition, *host_number, &exist_part)) || !exist_part) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts - dbms per host is too big or server type and partition are not available!\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
				((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number, host_partition);
			goto __OPH_EXIT_1;
		}

		fitsfile *fptr = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr;
		char *cwd = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd;
		char *user = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user;
		FITS_var tmp_var;

		int permission = 0;
		int folder_id = 0;
		//Check if input path exists
		if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_CWD_ERROR, container_name, cwd);
			goto __OPH_EXIT_1;
		}
		if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DATACUBE_PERMISSION_ERROR, container_name, user);
			goto __OPH_EXIT_1;
		}
		//Check if container exists
		if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->create_container) {
			if (container_exists) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input container already exist\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
				goto __OPH_EXIT_1;
			} else {
				if (!oph_odb_fs_is_allowed_name(container_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", container_name);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_NAME_NOT_ALLOWED_ERROR, container_name);
					goto __OPH_EXIT_1;
				}
				//Check if container exists in folder
				int container_unique = 0;
				if ((oph_odb_fs_is_unique(folder_id, container_name, oDB, &container_unique))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
					goto __OPH_EXIT_1;
				}
				if (!container_unique) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Container '%s' already exists in this path or a folder has the same name\n", container_name);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
					goto __OPH_EXIT_1;
				}
			}

			//If it doesn't then create new container and get last id
			oph_odb_container cont;
			strncpy(cont.container_name, container_name, OPH_ODB_FS_CONTAINER_SIZE);
			cont.id_parent = 0;
			cont.id_folder = folder_id;
			cont.operation[0] = 0;
			cont.id_vocabulary = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_vocabulary;
			cont.description[0] = 0;

			if (oph_odb_fs_insert_into_container_table(oDB, &cont, &id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container = id_container_out;

			if (container_exists && oph_odb_fs_add_suffix_to_container_name(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			//Create new dimensions
			oph_odb_dimension dim;
			dim.id_container = id_container_out;
			strncpy(dim.base_time, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
			dim.leap_year = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->leap_year;
			dim.leap_month = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->leap_month;

			for (i = 0; i < measure->ndims; i++) {
				tmp_var.dims_id = NULL;
				tmp_var.dims_length = NULL;

				if (oph_fits_get_fits_var(OPH_GENERIC_CONTAINER_ID, measure->dims_name[i], &(measure->dims_length[i]), &tmp_var, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIM_READ_ERROR, "");
					if (tmp_var.dims_id)
						free(tmp_var.dims_id);
					if (tmp_var.dims_length)
						free(tmp_var.dims_length);
					goto __OPH_EXIT_1;
				}
				free(tmp_var.dims_id);
				free(tmp_var.dims_length);

				// Load dimension names and types
				strncpy(dim.dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE);
				if (oph_fits_get_c_type(tmp_var.vartype, dim.dimension_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIM_READ_ERROR, "type cannot be converted");
					goto __OPH_EXIT_1;
				}
				dim.id_hierarchy = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i];
				if (dim.id_hierarchy >= 0)
					dim.units[0] = dim.calendar[0] = 0;
				else {
					int j = 0;
					dim.id_hierarchy *= -1;
					strncpy(dim.units, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
					strncpy(dim.calendar, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
					char *tmp = NULL, *save_pointer = NULL, month_lengths[OPH_ODB_DIM_TIME_SIZE];
					strncpy(month_lengths, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
					while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
						dim.month_lengths[j++] = (int) strtol(tmp, NULL, 10);
					while (j < OPH_ODB_DIM_MONTH_NUMBER)
						dim.month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
				}
				if (oph_odb_dim_insert_into_dimension_table(oDB, &(dim), &last_insertd_id, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert dimension.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_INSERT_DIMENSION_ERROR);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_create_db(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DB_CREATION);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			char dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

		} else if (!container_exists) {
			//If it doesn't exist then return an error
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Else retreive container ID and check for dimension table
		if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->create_container
		    && oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Check vocabulary
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->import_metadata) {
			if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_vocabulary
			    && oph_odb_meta_retrieve_vocabulary_id_from_container(oDB, id_container_out, &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_vocabulary)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown vocabulary\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTFITS_NO_VOCABULARY_NO_CONTAINER, container_name, "");
				goto __OPH_EXIT_1;
			}
		}
		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_USE_DB);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

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
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name
		    && !oph_odb_dim_retrieve_grid_id(oDB, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &id_grid) && id_grid) {
			//Check if ophidiadb dimensions are the same of input dimensions

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
			    (oDB, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &dims, &dim_inst, &dim_inst_num)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input grid name not usable! It is already used by another container.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_NO_GRID, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name);
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
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_INPUT_GRID_DIMENSION_MISMATCH, "number");
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
							//No varid in fits file
							//The id will be a number between 1 and ndims (names: NAXIS1, ...NAXISn)
							tmp_var.varid = (int) strtol((measure->dims_name[i]) + 5, NULL, 10);
							dimvar_ids[i] = tmp_var.varid;

							//Modified to allow subsetting
							tmp_var.dims_start_index = &(measure->dims_start_index[i]);
							tmp_var.dims_end_index = &(measure->dims_end_index[i]);

							//Get information from id
							tmp_var.ndims = 1;
							tmp_var.dims_id = malloc(tmp_var.ndims * sizeof(int));
							if (!(tmp_var.dims_id)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_MEMORY_ERROR_INPUT, "dimensions nc ids");
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							*tmp_var.dims_id = 1;
							dim_array = NULL;

							if (oph_fits_get_dim_array2(id_container_out, dim_inst[j].size, &dim_array)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information", "");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_READ_ERROR, "");
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(tmp_var.dims_id);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}
							free(tmp_var.dims_id);

							if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->check_grid || (!oph_dim_compare_dimension
																	  (db_dimension, label_dimension_table_name,
																	   dims[j].dimension_type, dim_inst[j].size, dim_array,
																	   dim_inst[j].fk_id_dimension_label, &match)
																	  && !match)) {
								free(dim_array);
								found_flag = 1;
								break;
							}

							free(dim_array);
						}
					}
				}

				if (!found_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n", measure->dims_name[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_INPUT_DIMENSION_MISMATCH, measure->dims_name[i]);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dims);
					free(dim_inst);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
			}
		} else {
		 /****************************
	      * BEGIN - IMPORT DIMENSION *
		  ***************************/
			int number_of_dimensions_c = 0, i, j;
			oph_odb_dimension *tot_dims = NULL;

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions .\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIMENSION_READ_ERROR);
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
					      ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
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
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_HIERARCHY_ERROR);
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
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (!exists) {
					if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->import_metadata && (time_dimension < 0)
					    && !strcmp(hier.hierarchy_name, OPH_COMMON_TIME_HIERARCHY))
						time_dimension = i;
					else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c': check container specifications\n", measure->dims_concept_level[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_BAD2_PARAMETER, measure->dims_concept_level[i]);
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
			if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name) {
				strncpy(new_grid.grid_name, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				int last_inserted_grid_id = 0;
				if (oph_odb_dim_insert_into_grid_table(oDB, &new_grid, &last_inserted_grid_id, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert grid in OphidiaDB, or grid already exists.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_GRID_INSERT_ERROR);
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
					      ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				tmp_var.dims_id = NULL;
				tmp_var.dims_length = NULL;

				if (oph_fits_get_fits_var(OPH_GENERIC_CONTAINER_ID, measure->dims_name[i], &(measure->dims_length[i]), &tmp_var, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_READ_ERROR, "");
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
				}
				dimvar_ids[i] = tmp_var.varid;
				free(tmp_var.dims_id);
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
				if (measure->dims_start_index[i] == measure->dims_end_index[i])
					tmp_var.varsize = 1;
				else
					tmp_var.varsize = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				dim_inst[i].size = tmp_var.varsize;
				dim_inst[i].concept_level = measure->dims_concept_level[i];

				if (oph_fits_compare_fits_c_types(id_container_out, tmp_var.vartype, tot_dims[j].dimension_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension type in FITS file doesn't correspond to the one stored in OphidiaDB\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_TYPE_MISMATCH_ERROR, measure->dims_name[i]);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				//Modified to allow subsetting
				tmp_var.dims_start_index = &(measure->dims_start_index[i]);
				tmp_var.dims_end_index = &(measure->dims_end_index[i]);

				if (oph_fits_get_dim_array2(id_container_out, tmp_var.varsize, &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information", "");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_READ_ERROR, "");
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				if (oph_dim_insert_into_dimension_table(db_dimension, label_dimension_table_name, tot_dims[j].dimension_type, tmp_var.varsize, dim_array, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_ROW_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					free(dim_array);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				free(dim_array);
				dim_inst[i].fk_id_dimension_label = dimension_array_id;	// Real dimension

				index_array = (long long *) malloc(tmp_var.varsize * sizeof(long long));
				for (kk = 0; kk < tmp_var.varsize; ++kk)
					index_array[kk] = 1 + kk;	// Non 'C'-like indexing
				if (oph_dim_insert_into_dimension_table(db_dimension, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, tmp_var.varsize, (char *) index_array, &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIM_ROW_ERROR, tot_dims[j].dimension_name);
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
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIMINST_INSERT_ERROR, tot_dims[j].dimension_name);
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
		if (oph_ids_create_new_id_string(&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment ids string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_CREATE_ID_STRING_ERROR);
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
		strncpy(src.uri, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig, OPH_ODB_CUBE_SOURCE_URI_SIZE);
		if (oph_odb_cube_insert_into_source_table(oDB, &src, &id_src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert source URI\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_INSERT_SOURCE_URI_ERROR, src.uri);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		//Set datacube params
		cube.hostxdatacube = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number;
		cube.fragmentxdb = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number;
		cube.tuplexfragment = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		cube.id_container = id_container_out;
		strncpy(cube.measure, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure.varname, OPH_ODB_CUBE_MEASURE_SIZE);
		if (oph_fits_get_c_type(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure.vartype, cube.measure_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_VAR_TYPE_NOT_SUPPORTED, cube.measure_type);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		strncpy(cube.frag_relative_index_set, id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		cube.db_number = cube.hostxdatacube;
		cube.compressed = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->compressed;
		cube.id_db = NULL;
		//New fields
		cube.id_source = id_src;
		cube.level = 0;
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description);
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
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DIMENSION_ODB_ERROR, measure->dims_name[i]);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DATACUBE_INSERT_ERROR);
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
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_CUBEHASDIM_INSERT_ERROR);
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
		// Not supported yet
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->import_metadata) {
			int status = 0;
			char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code
			char keyname[80];
			char keyvalue[80];
			char keycomment[80];
			char valcomm[80];
			// Get the number of cards (attributes)
			int keysexist = 0;
			fits_get_hdrspace(fptr, &keysexist, NULL, &status);
			if (status) {
				fits_get_errstatus(status, err_text);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get keywords from fits file: %s\n", err_text);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get get keywords from fits file: %s\n", err_text);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			int mya;
			// Get the keywords
			for (mya = 0; mya < keysexist; mya++) {
				fits_read_keyn(fptr, mya + 1, keyname, keyvalue, keycomment, &status);
				if (status) {
					fits_get_errstatus(status, err_text);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get keywords from file: %s\n", err_text);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to get keywords from file: %s\n", err_text);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				snprintf(valcomm, 80, "%s / %s", keyvalue, keycomment);
				//NOTE: the type of the key values is fixed to text
				//Retrieve 'text' type id
				char key_type[OPH_COMMON_TYPE_SIZE];
				snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_METADATA_TYPE_TEXT);
				int id_key_type = 0, sid_key_type;
				if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &id_key_type)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_METADATATYPE_ID_ERROR);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				sid_key_type = id_key_type;
				int id_metadatainstance;
				if (oph_odb_meta_insert_into_metadatainstance_manage_tables(oDB, id_datacube_out, -1, keyname, NULL, sid_key_type, id_user, valcomm, &id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_METADATA_INSERT_INSTANCE_ERROR);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
			}
		}
	  /********************************
	   *    METADATA IMPORT - END     *
	   ********************************/

	  /********************************
	   * DB INSTANCE CREATION - BEGIN *
	   ********************************/
		int dbmss_length, host_num;
		dbmss_length = host_num = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number;
		int *id_dbmss = NULL, *id_hosts = NULL;
		//Retreive ID dbms list
		if (oph_odb_stge_retrieve_dbmsinstance_id_list
		    (oDB, ioserver_type, id_host_partition, hidden, host_num, id_datacube_out, &id_dbmss, &id_hosts, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->policy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DBMS_LIST_ERROR);
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
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DBMS_ERROR, db.id_dbms);
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}
			db.dbms_instance = &dbms;

			if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server) {
				if (oph_dc_setup_dbms(&(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server), dbms.io_server_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_IOPLUGIN_SETUP_ERROR, db.id_dbms);
					free(id_dbmss);
					free(id_hosts);
					goto __OPH_EXIT_1;
				}
			}

			if (oph_dc_connect_to_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DBMS_CONNECTION_ERROR, dbms.id_dbms);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms));
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}

			if (oph_dc_generate_db_name(oDB->name, id_datacube_out, db.id_dbms, 0, 1, &db_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of Db instance  name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_STRING_BUFFER_OVERFLOW, "DB instance name", db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			strcpy(db.db_name, db_name);
			if (oph_dc_create_db(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create new db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_NEW_DB_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			//Insert new database instance and partitions
			if (oph_odb_stge_insert_into_dbinstance_partitioned_tables(oDB, &db, id_datacube_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dbinstance table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTFITS_DB_INSERT_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbms));
		}
		free(id_dbmss);
		free(id_hosts);

	  /********************************
	   *  DB INSTANCE CREATION - END  *
	   ********************************/

		id_datacube[0] = id_datacube_out;
		id_datacube[1] = id_container_out;
		id_datacube[2] = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number;
		id_datacube[3] = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number;
		id_datacube[4] = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		id_datacube[5] = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number;
	}
      __OPH_EXIT_1:
	if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Broadcast to all other processes the result
	MPI_Bcast(id_datacube, 6, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (!id_datacube[0] || !id_datacube[1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube[1], OPH_LOG_OPH_IMPORTFITS_MASTER_TASK_INIT_FAILED_NO_CONTAINER, container_name);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube = id_datacube[0];
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container = id_datacube[1];
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->host_number = id_datacube[2];
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number = id_datacube[3];
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number = id_datacube[4];
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number = id_datacube[5];

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int frag_total_number = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->total_frag_number;

	//All processes compute the fragment number to work on
	int div_result = (frag_total_number) / (handle->proc_number);
	int div_remainder = (frag_total_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	} else {
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id += div_result;
		}
		if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id >= frag_total_number)
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i, j, k;
	int id_datacube_out = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Compute DB list starting position and number of rows
	int start_position =
	    (int) floor((double) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id / ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number);
	int row_number =
	    (int) ceil((double) (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id + ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number) /
		       ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_OPHIDIADB_CONFIGURATION_FILE,
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_OPHIDIADB_CONNECTION_ERROR,
			((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube_out, start_position, row_number, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_fragment new_frag;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];

	int frag_to_insert = 0;
	int frag_count = 0;

	if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server) {
		if (oph_dc_setup_dbms(&(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server), dbmss.value[0].io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_IOPLUGIN_SETUP_ERROR,
				dbmss.value[0].id_dbms);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}
	//For each DBMS
	for (i = 0; i < dbmss.size; i++) {

		if (oph_dc_connect_to_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; j < dbs.size; j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//Compute number of fragments to insert in DB
			frag_to_insert =
			    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number - (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id +
													    frag_count -
													    start_position *
													    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number);

			//For each fragment
			for (k = 0; k < frag_to_insert; k++) {

				//Set new fragment
				new_frag.id_datacube = id_datacube_out;
				new_frag.id_db = dbs.value[j].id_db;
				new_frag.frag_relative_index = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id + frag_count + 1;
				new_frag.key_start =
				    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number + 1;
				new_frag.key_end =
				    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number;
				new_frag.db_instance = &(dbs.value[j]);

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &fragment_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTFITS_STRING_BUFFER_OVERFLOW, "fragment name", fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				strcpy(new_frag.fragment_name, fragment_name);
				//Create Empty fragment
				if (oph_dc_create_empty_fragment(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTFITS_FRAGMENT_CREATION_ERROR, new_frag.fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//Populate fragment
				if (oph_fits_populate_fragment_from_fits3
				    (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &new_frag, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr,
				     ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->tuplexfrag_number, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->array_length,
				     ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->compressed, (FITS_var *) & (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure),
				     ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->memory_size)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTFITS_FRAG_POPULATE_ERROR, new_frag.fragment_name, "");
					oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTFITS_FRAGMENT_INSERT_ERROR, new_frag.fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				frag_count++;
				if (frag_count == ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number)
					break;
			}
			start_position++;
		}
		oph_dc_disconnect_from_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));

	}
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->execute_error = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	short int proc_error = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
			 ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_IMPORTFITS)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTFITS, "Output Cube", jsonbuf)) {
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
			ophidiadb *oDB = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB;
			oph_odb_datacube cube;
			oph_odb_cube_init_datacube(&cube);

			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, id_datacube, &cube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_DATACUBE_READ_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTFITS_MASTER_TASK_INIT_FAILED);
		} else {
			if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id >= 0 || handle->proc_rank == 0) {
				//Partition fragment relative index string
				char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
				char *new_ptr = new_id_string;
				if (oph_ids_get_substring_from_string
				    (id_string, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id,
				     ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTFITS_ID_STRING_SPLIT_ERROR);
				} else {
					//Delete fragments
					int start_position =
					    (int) floor((double) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id /
							((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number);
					int row_number = (int)
					    ceil((double)
						 (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_first_id +
						  ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragment_number) /
						 ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

					if (oph_dproc_delete_data
					    (id_datacube, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, new_id_string, start_position, row_number, 1)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container,
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

		if (handle->proc_rank == 0) {
			int id_container = ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container;
			while (id_container && ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->create_container) {
				ophidiadb *oDB = &((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB;

				//Remove also grid related to container dimensions
				if (oph_odb_dim_delete_from_grid_table(oDB, id_container)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting grid related to container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_GRID_DELETE_ERROR);
					break;
				}
				//Delete container and related dimensions/ dimension instances
				if (oph_odb_fs_delete_from_container_table(oDB, id_container)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting input container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_CONTAINER_DELETE_ERROR);
					break;
				}

				oph_odb_db_instance db_;
				oph_odb_db_instance *db = &db_;
				if (oph_dim_load_dim_dbinstance(db)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_DIM_LOAD);
					oph_dim_unload_dim_dbinstance(db);
					break;
				}
				if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_DIM_CONNECT);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					break;
				}
				if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_DIM_USE_DB);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					break;
				}

				char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
				snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container);
				snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container);

				if (oph_dim_delete_table(db, index_dimension_table_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_DELETE_ERROR);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					break;
				}
				if (oph_dim_delete_table(db, label_dimension_table_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_IMPORTFITS_DIM_TABLE_DELETE_ERROR);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					break;
				}

				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);

				break;
			}
		}
		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

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
		oph_odb_free_ophidiadb(&((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->container_input = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server)
		oph_dc_cleanup_dbms(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->server);


	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	}

	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fits_file_path_orig = NULL;
	}
	int status = 0;
	char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code

	fits_close_file(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->fptr, &status);
	if (status) {
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %s\n", err_text);
	}

	FITS_var *measure = ((FITS_var *) & (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->measure));

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

	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->partition_input) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->partition_input);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->partition_input = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy) {
		free((int *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->base_time) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->base_time);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->base_time = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->units) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->units);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->units = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->calendar) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->calendar);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->calendar = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->month_lengths) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->month_lengths);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	}
	if (((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description);
		((OPH_IMPORTFITS_operator_handle *) handle->operator_handle)->description = NULL;
	}

	free((OPH_IMPORTFITS_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
