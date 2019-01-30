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

#include "drivers/OPH_CUBESCHEMA_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

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
#include "oph_utility_library.h"

#include <float.h>

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

	if (!(handle->operator_handle = (OPH_CUBESCHEMA_operator_handle *) calloc(1, sizeof(OPH_CUBESCHEMA_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action = 0;	// read mode
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_time = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name_number = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64 = 0;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level = 'c';
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level = 1;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_array = 1;

	ophidiadb *oDB = &((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	//3 - Fill struct with the correct data
	char *datacube_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ACTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ACTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_ACTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, "add"))
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action = 1;
	else if (!strcmp(value, "clear"))
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action = 2;
	else if (strcmp(value, "read")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Action unrecognized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Action unrecognized\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_OPHIDIADB_CONFIGURATION_FILE);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_OPHIDIADB_CONNECTION_ERROR);

		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	int id_container = 0, id_datacube = 0;
	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	char *uri = NULL;
	int folder_id = 0;
	int permission = 0;
	if (oph_pid_parse_pid(datacube_name, &id_container, &id_datacube, &uri)) {
		if (uri)
			free(uri);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_PID_ERROR, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_container, id_datacube, &exists)) || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_NO_INPUT_DATACUBE, datacube_name);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_container, &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DATACUBE_FOLDER_ERROR, datacube_name);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DATACUBE_PERMISSION_ERROR, username);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	free(uri);
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container = id_container;
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level = (int) strtol(value, NULL, 10);
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level < 0 || ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level > 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_BAD_LEVEL_PARAMETER, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		char **dim_names;
		int number_of_dimensions_names = 0;
		if (oph_tp_parse_multiple_value_param(value, &dim_names, &number_of_dimensions_names)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if ((((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action == 1) && (number_of_dimensions_names != 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Wrong number of dimensions\n");
			oph_tp_free_multiple_value_param_list(dim_names, number_of_dimensions_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name = dim_names;
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name_number = number_of_dimensions_names;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_INDEX);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHOW_INDEX);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHOW_TIME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_time = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BASE64);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BASE64);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_BASE64);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64 = 1;

	if (!(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name = (char *) strndup(datacube_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "datacube name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strlen(value) > 0)
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level = value[0];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EI_DIMENSION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EI_DIMENSION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_EI_DIMENSION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level = (int) strtol(value, NULL, 10);
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_BAD_LEVEL_PARAMETER, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EI_DIMENSION_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EI_DIMENSION_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_EI_DIMENSION_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_array = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->oDB;
	char *datacube_name = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name;
	char **dimension_name = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name;
	int id_container = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container;
	int id_datacube = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_datacube;
	int level = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->level;

	oph_odb_cubehasdim *cubedims = NULL;
	int l, j, number_of_dimensions = 0;

	//Read dimension
	if (oph_odb_cube_retrieve_cubehasdim_list(oDB, id_datacube, &cubedims, &number_of_dimensions)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_CUBEHASDIM_READ_ERROR);
		number_of_dimensions = 0;
	}

	char error_message[OPH_COMMON_BUFFER_LEN], success = 0, *concept_level_long = NULL;
	*error_message = 0;

	while (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action == 1) {

		char **dimension_names = NULL;
		int l = 0, ll, dimension_names_num = 0;
		if (oph_odb_dim_retrieve_dimensions(oDB, id_datacube, &dimension_names, &dimension_names_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimensions.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to retrieve dimensions.\n");
		} else {
			for (; l < dimension_names_num; ++l)
				if (dimension_names[l] && !strcmp(dimension_name[0], dimension_names[l]))
					break;
		}
		if (dimension_names) {
			for (ll = 0; ll < dimension_names_num; ++ll)
				if (dimension_names[ll])
					free(dimension_names[ll]);
			free(dimension_names);
		}
		if (l < dimension_names_num) {
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Dimension '%s' is already set\n", dimension_name[0]);
			break;
		}

		char is_last, first_implicit = -1;
		int number_of_dimensions_ext = 1 + number_of_dimensions, found = -1, k = 0, kk, last_level = 0;
		oph_odb_cubehasdim *cubedims_ext = (oph_odb_cubehasdim *) malloc(number_of_dimensions_ext * sizeof(oph_odb_cubehasdim));
		if (!cubedims_ext) {
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Memory error");
			break;
		}
		for (l = j = 0; l <= number_of_dimensions; ++l) {
			is_last = l == number_of_dimensions;
			if (!is_last) {
				if (!cubedims[l].explicit_dim && (first_implicit < 0))
					first_implicit = last_level = 0;
				else if (!first_implicit)
					first_implicit = 1;
			} else {
				if ((first_implicit < 0) && ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_array)
					last_level = 0;
			}
			if (found < 0) {
				if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_array) {
					if (is_last || (!cubedims[l].explicit_dim && (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level == cubedims[l].level))) {
						cubedims_ext[j].id_datacube = id_datacube;
						cubedims_ext[j].id_dimensioninst = 0;
						cubedims_ext[j].explicit_dim = 0;
						cubedims_ext[j].level = 1 + last_level;
						cubedims_ext[j].size = 1;
						found = j++;
						for (k = l; k < number_of_dimensions; ++k)
							cubedims[k].level++;
					}
				} else {
					if (is_last || !cubedims[l].explicit_dim || (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level == cubedims[l].level)) {
						cubedims_ext[j].id_datacube = id_datacube;
						cubedims_ext[j].id_dimensioninst = 0;
						cubedims_ext[j].explicit_dim = 1;
						cubedims_ext[j].level = is_last || !cubedims[l].explicit_dim ? 1 + last_level : ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dim_level;
						cubedims_ext[j].size = 1;
						found = j++;
						for (k = l; cubedims[k].explicit_dim && (k < number_of_dimensions); ++k)
							cubedims[k].level++;
					}
				}
			}
			if (!is_last) {
				last_level = cubedims[l].level;
				memcpy(cubedims_ext + j, cubedims + l, sizeof(oph_odb_cubehasdim));
				j++;
			}
		}
		if (found < 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store dimension '%s'\n", dimension_name[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to store dimension '%s'\n", dimension_name[0]);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to store dimension '%s'\n", dimension_name[0]);
			break;
		}

		int number_of_dimensions_c = 0;
		oph_odb_dimension *tot_dims = NULL;
		if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container, &tot_dims, &number_of_dimensions_c)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimensions\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to retrieve dimensions\n");
			if (tot_dims)
				free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve dimensions");
			break;
		}
		//Find container dimension
		for (j = 0; j < number_of_dimensions_c; j++)
			if (!strncmp(tot_dims[j].dimension_name, dimension_name[0], OPH_ODB_DIM_DIMENSION_SIZE))
				break;
		if (j == number_of_dimensions_c) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found in the container\n", dimension_name[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Dimension %s not found in the container\n", dimension_name[0]);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "There is no dimension '%s' in the container", dimension_name[0]);
			break;
		}
		//Find dimension level into hierarchy file
		oph_odb_hierarchy hier;
		if (oph_odb_dim_retrieve_hierarchy(oDB, tot_dims[j].id_hierarchy, &hier)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve concept hierachy");
			break;
		}

		int exist_flag = 0;
		char filename[2 * OPH_TP_BUFLEN];
		snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
		if (oph_hier_get_concept_level_long(filename, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level, &concept_level_long)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error retrieving hierarchy\n");
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve concept hierachy");
			break;
		}
		if (!concept_level_long) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to set concept level to '%c'\n", ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to set concept level to '%c'", ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level);
			break;
		}

		oph_odb_dimension_instance dim_inst;
		dim_inst.id_dimension = tot_dims[j].id_dimension;
		dim_inst.fk_id_dimension_index = 0;
		dim_inst.concept_level = ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->concept_level;
		dim_inst.unlimited = 0;
		dim_inst.size = 1;
		dim_inst.fk_id_dimension_label = 0;
		dim_inst.id_grid = 0;
		dim_inst.id_dimensioninst = 0;

		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while loading dimension db paramters\n");
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimension");
			break;

		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while connecting to dimension dbms\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container);

		if (oph_dim_check_if_dimension_table_exists(db, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while retrieving dimension table\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Dimensions table doesn't exists\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}
		if (oph_dim_check_if_dimension_table_exists(db, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while retrieving dimension table\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Dimensions table doesn't exists\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while opening dimension db\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to load dimensions");
			break;
		}

		int dimension_array_id = 0;
		if (oph_dim_insert_into_dimension_table_rand_data(db, label_dimension_table_name, tot_dims[j].dimension_type, dim_inst.size, &dimension_array_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to insert new dimension row\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to store dimension '%s'", dimension_name[0]);
			break;
		}
		dim_inst.fk_id_dimension_label = dimension_array_id;	// Real data

		long long index_array = 1;
		if (oph_dim_insert_into_dimension_table(db, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, (long long) dim_inst.size, (char *) &index_array, &dimension_array_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to insert new dimension row\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(tot_dims);
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to store dimension '%s'", dimension_name[0]);
			break;
		}
		dim_inst.fk_id_dimension_index = dimension_array_id;	// Indexes

		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		free(tot_dims);

		if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &dim_inst, &dimension_array_id, id_datacube, dimension_name[0], concept_level_long)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to insert new dimension instance row\n");
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to store dimension '%s'", dimension_name[0]);
			break;
		}
		cubedims_ext[found].id_dimensioninst = dimension_array_id;

		if (oph_odb_cube_insert_into_cubehasdim_table(oDB, cubedims_ext + found, &dimension_array_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to insert new datacube - dimension relations.\n");
			free(cubedims_ext);
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to store dimension '%s'", dimension_name[0]);
			break;
		}

		for (kk = found + 1; kk <= k; ++kk) {
			if (oph_odb_cube_update_level_in_cubehasdim_table(oDB, cubedims_ext[kk].level, cubedims_ext[kk].id_cubehasdim)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension level.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to update dimension level.\n");
				free(cubedims_ext);
				snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to update dimensions");
				break;
			}
		}
		if (kk <= k)
			break;

		free(cubedims);
		cubedims = cubedims_ext;
		number_of_dimensions = number_of_dimensions_ext;

		success = 1;
		break;
	}
	if (concept_level_long)
		free(concept_level_long);

	while (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action == 2) {

		oph_odb_cubehasdim *cubedims_ext = (oph_odb_cubehasdim *) malloc(number_of_dimensions * sizeof(oph_odb_cubehasdim));
		if (!cubedims_ext) {
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Memory error");
			break;
		}
		for (l = j = 0; l < number_of_dimensions; ++l)
			if (!cubedims[l].level || !cubedims[l].size) {
				if (oph_odb_dim_delete_dimensioninstance(oDB, cubedims[l].id_dimensioninst)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to clear dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to clear dimension instance row\n");
					free(cubedims_ext);
					snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to clear dimensions");
					break;
				}
			} else {
				memcpy(cubedims_ext + j, cubedims + l, sizeof(oph_odb_cubehasdim));
				j++;
			}
		if (l < number_of_dimensions)
			break;

		free(cubedims);
		cubedims = cubedims_ext;
		number_of_dimensions = j;

		success = 1;
		break;
	}

	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->action && !success) {
		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
		     OPH_JSON_OBJKEY_CUBESCHEMA_DIMINFO)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_DIMINFO, "Error", strlen(error_message) > 0 ? error_message : NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if (level != 1) {
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, id_datacube, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DATACUBE_READ_ERROR);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Compute derivated fields
		long long num_elements = 0;
		if ((oph_odb_cube_get_datacube_num_elements(oDB, id_datacube, &num_elements))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_CUBESCHEMA_GET_ELEMENTS_ERROR, datacube_name);
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_GET_ELEMENTS_ERROR, datacube_name);
			num_elements = 0;
		}
		if (num_elements == 0) {
			oph_odb_cubehasdim *cubedims = NULL;
			int number_of_dimensions = 0;

			if (oph_odb_cube_retrieve_cubehasdim_list(oDB, id_datacube, &cubedims, &number_of_dimensions)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_CUBESCHEMA_GET_ELEMENTS_ERROR, datacube_name);
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_GET_ELEMENTS_ERROR, datacube_name);
				num_elements = 0;
			} else {
				int l;
				num_elements = 1;
				for (l = 0; l < number_of_dimensions; l++) {
					if (cubedims[l].level && cubedims[l].size) {
						num_elements *= cubedims[l].size;
					}
				}
				free(cubedims);

				//Set cubelements number 
				if ((oph_odb_cube_set_datacube_num_elements(oDB, id_datacube, num_elements))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CUBESCHEMA_SET_NUMBER_ELEMENTS_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_SET_NUMBER_ELEMENTS_ERROR);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			}
		}
		long long size = 0;
		if ((oph_odb_cube_get_datacube_size(oDB, id_datacube, &size))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve size\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_GET_SIZE_ERROR, datacube_name);
			size = 0;
		}

		int id_number = 0;
		if (oph_ids_count_number_of_ids(cube.frag_relative_index_set, &id_number)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get total number of fragments\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_COUNT_FRAG_ERROR);
			id_number = 0;
		}

		double size_mb = 0;
		int byte_unit = 0;
		char unit[OPH_UTL_UNIT_SIZE] = { '\0' };
		if (size) {
			if (oph_utl_auto_compute_size(size, &size_mb, &byte_unit) || oph_utl_unit_to_str(byte_unit, &unit)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to compute size\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_SIZE_COMPUTE_ERROR);
				snprintf(unit, OPH_UTL_UNIT_SIZE, OPH_UTL_MB_UNIT);
				size_mb = (double) size / (1024.0 * 1024.0);
			}
		}
		char size_buf[OPH_COMMON_MAX_LONG_LENGHT];
		char elements_buf[OPH_COMMON_MAX_LONG_LENGHT];
		char number_buf[OPH_COMMON_MAX_LONG_LENGHT];
		int n;

		if (size)
			n = snprintf(size_buf, OPH_COMMON_MAX_LONG_LENGHT, "%f", size_mb);
		else
			n = snprintf(size_buf, OPH_COMMON_MAX_LONG_LENGHT, "%s", "-");
		if (n >= OPH_COMMON_MAX_LONG_LENGHT) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size exceed buffer limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_STRING_BUFFER_OVERFLOW, "size", size_buf);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (num_elements)
			n = snprintf(elements_buf, OPH_COMMON_MAX_LONG_LENGHT, "%lld", num_elements);
		else
			n = snprintf(elements_buf, OPH_COMMON_MAX_LONG_LENGHT, "%s", "-");
		if (n >= OPH_COMMON_MAX_LONG_LENGHT) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size exceed buffer limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_STRING_BUFFER_OVERFLOW, "Number of elements", num_elements);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (id_number)
			n = snprintf(number_buf, OPH_COMMON_MAX_LONG_LENGHT, "%d", id_number);
		else
			n = snprintf(number_buf, OPH_COMMON_MAX_LONG_LENGHT, "%s", "-");
		if (n >= OPH_COMMON_MAX_LONG_LENGHT) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size exceed buffer limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_STRING_BUFFER_OVERFLOW, "number of fragments", number_buf);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		char *creationdate = NULL;
		char *description = NULL;
		if ((oph_odb_cube_find_datacube_additional_info(oDB, id_datacube, &creationdate, &description))) {
			if (creationdate)
				free(creationdate);
			if (description)
				free(description);
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve creation date and description\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_ADDITIONAL_INFO_ERROR);
			creationdate = NULL;
			description = NULL;
		}

		oph_odb_source src;
		src.uri[0] = 0;
		if (cube.id_source) {
			if (oph_odb_cube_retrieve_source(oDB, cube.id_source, &src)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to find source URI\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_RETRIEVE_SOURCE_URI_ERROR);
				src.uri[0] = 0;
			}
		}
		//Build PID
		char *tmp_uri = NULL;
		char *pid = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_PID_URI_ERROR);
		} else if (oph_pid_create_pid(tmp_uri, cube.id_container, cube.id_datacube, &pid)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_PID_CREATE_ERROR);
		}
		if (tmp_uri)
			free(tmp_uri);

		printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n");
		printf("| %-60s%-s%-59s |\n", "", "DATACUBE INFORMATION", "");
		printf("+--------------------------------------------------+--------------------------+----------------------+---------------+--------+---------------+\n");
		printf("| %-48s | %-24s | %-20s | %-13s | %-6s | %-13s |\n", "PID", "CREATION DATE", "MEASURE", "MEASURE TYPE", "LEVEL", "NUM.FRAGMENTS");
		printf("+--------------------------------------------------+--------------------------+----------------------+---------------+--------+---------------+\n");
		printf("| %-48s | %-24s | %-20s | %-13s | %-6d | %-13s |\n", (pid ? pid : "-"), (creationdate ? creationdate : "-"), (cube.measure ? cube.measure : "-"),
		       (cube.measure_type ? cube.measure_type : "-"), cube.level, number_buf);
		printf("+--------------+-----------------------------------+--------------------------+----------------------+---------------+--------+---------------+\n");
		printf("| %-12s | %-124s |\n", "SOURCE FILE", ((!src.uri || src.uri[0] == 0) ? "-" : src.uri));
		printf("+--------------+------------------------------------------------------------------------------------------------------------------------------+\n");

		//Create particular description field
		if (!description)
			printf("| %-12s | %-124s |\n", "DESCRIPTION", "-");
		else {
			int len = strlen(description);
			int i;
			char *tmp = description;
			char tmp_buffer[125];
			for (i = 0; i < len; i += 125) {
				strncpy(tmp_buffer, tmp, 125);
				tmp_buffer[110] = 0;
				tmp += 110;
				if (i == 0)
					printf("| %-12s | %-124s |\n", "DESCRIPTION", tmp_buffer);
				else
					printf("| %-12s | %-124s |\n", "", tmp_buffer);
			}
		}

		long long array_length = 0;
		array_length = 1;
		for (l = 0; l < number_of_dimensions; l++) {
			if (!cubedims[l].explicit_dim && cubedims[l].level && cubedims[l].size) {
				array_length *= cubedims[l].size;
			}
		}

		char array_buf[OPH_COMMON_MAX_LONG_LENGHT];
		if (array_length)
			n = snprintf(array_buf, OPH_COMMON_MAX_LONG_LENGHT, "%lld", array_length);
		else
			n = snprintf(array_buf, OPH_COMMON_MAX_LONG_LENGHT, "%s", "-");
		if (n >= OPH_COMMON_MAX_LONG_LENGHT) {
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (creationdate)
				free(creationdate);
			if (description)
				free(description);
			if (pid)
				free(pid);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Array length exceed buffer limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_STRING_BUFFER_OVERFLOW, "array length", array_buf);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		printf("+-----------------+-----------------+-----------------+-----------------+------------+-----------------------+--------------------------------+\n");
		printf("| %-15s | %-15s | %-15s | %-15s | %-10s | CUBESIZE[%s%-10s | %-30s |\n", "HOSTxCUBE", "FRAGxDB", "ROWxFRAG", "ELEMxROW", "COMPRESSED",
		       (unit[0] ? unit : "  "), "]", "NUM.ELEMENTS");
		printf("+-----------------+-----------------+-----------------+-----------------+------------+-----------------------+--------------------------------+\n");
		printf("| %-15d | %-15d | %-15d | %-15s | %-10s | %-21s | %-30s |\n", cube.hostxdatacube, cube.fragmentxdb, cube.tuplexfragment,
		       array_buf, (cube.compressed == 1 ? OPH_COMMON_YES_VALUE : OPH_COMMON_NO_VALUE), size_buf, elements_buf);
		printf("+-----------------+-----------------+-----------------+-----------------+------------+-----------------------+--------------------------------+\n\n");

// JSON
		if (oph_json_is_objkey_printable
		    (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
		     OPH_JSON_OBJKEY_CUBESCHEMA_CUBEINFO)) {
			char jsontmp[OPH_COMMON_BUFFER_LEN];
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 7, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "keys");
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonkeys[jjj] = strdup("PID");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("CREATION DATE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("MEASURE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("MEASURE TYPE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("LEVEL");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("NUMBER OF FRAGMENTS");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("SOURCE FILE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_CUBEINFO, "Datacube Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "values");
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			jsonvalues[jjj] = strdup(pid ? pid : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(creationdate ? creationdate : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(cube.measure ? cube.measure : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(cube.measure_type ? cube.measure_type : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cube.level);
			jsonvalues[jjj] = strdup(jsontmp);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(strcmp(number_buf, "-") ? number_buf : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup((!src.uri || src.uri[0] == 0) ? "" : src.uri);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_CUBEINFO, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
		}

		if (oph_json_is_objkey_printable
		    (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
		     OPH_JSON_OBJKEY_CUBESCHEMA_MORECUBEINFO)) {
			char jsontmp[OPH_COMMON_BUFFER_LEN];
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 9, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "keys");
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			jsonkeys[jjj] = strdup("DESCRIPTION");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("HOST x CUBE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("FRAGMENTS x DATABASE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("ROWS x FRAGMENT");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("ELEMENTS x ROW");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("COMPRESSED");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("CUBE SIZE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("UNIT");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("NUMBER OF ELEMENTS");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_LONG);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_MORECUBEINFO, "Datacube Additional Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "values");
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			jsonvalues[jjj] = strdup(description ? description : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cube.hostxdatacube);
			jsonvalues[jjj] = strdup(jsontmp);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cube.fragmentxdb);
			jsonvalues[jjj] = strdup(jsontmp);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cube.tuplexfragment);
			jsonvalues[jjj] = strdup(jsontmp);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(strcmp(array_buf, "-") ? array_buf : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(cube.compressed == 1 ? OPH_COMMON_YES_VALUE : OPH_COMMON_NO_VALUE);
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(strcmp(size_buf, "-") ? size_buf : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup((unit[0]) ? unit : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonvalues[jjj] = strdup(strcmp(elements_buf, "-") ? elements_buf : "");
			if (!jsonvalues[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
				for (iii = 0; iii < jjj; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_MORECUBEINFO, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				oph_odb_cube_free_datacube(&cube);
				if (creationdate)
					free(creationdate);
				if (description)
					free(description);
				if (pid)
					free(pid);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
		}

		oph_odb_cube_free_datacube(&cube);
		if (creationdate)
			free(creationdate);
		if (description)
			free(description);
		if (pid)
			free(pid);

		oph_odb_dimension dim;
		oph_odb_dimension_instance dim_inst;
		oph_odb_dimension_grid dim_grid;
		oph_odb_hierarchy hier;

		char filename[2 * OPH_TP_BUFLEN];
		char *concept_level = 0;

		printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n");
		printf("| %-59s%-s%-59s |\n", "", "DIMENSION INFORMATION", "");
		printf("+------------------+------------+--------------+-----------------+---------------+-----------+-------+----------------------------------------+\n");
		printf("| %-16s | %-10s | %-12s | %-15s | %-13s | %-9s | %-5s | %-38s |\n", "NAME", "TYPE", "SIZE", "HIERARCHY", "CONCEPT LEVEL", "ARRAY", "LEVEL", "LATTICE NAME");
		printf("+------------------+------------+--------------+-----------------+---------------+-----------+-------+----------------------------------------+\n");

		int is_objkey_printable =
		    oph_json_is_objkey_printable(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_CUBESCHEMA_DIMINFO);
		if (is_objkey_printable) {
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 8, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "keys");
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jsonkeys[jjj] = strdup("NAME");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("TYPE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("SIZE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("HIERARCHY");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("CONCEPT LEVEL");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("ARRAY");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("LEVEL");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			jsonkeys[jjj] = strdup("LATTICE NAME");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_INT);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_DIMINFO, "Dimension Information", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				free(cubedims);
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

		char jsontmp[OPH_COMMON_BUFFER_LEN];

		//Read cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			if (oph_odb_dim_retrieve_full_dimension_info(oDB, cubedims[l].id_dimensioninst, &dim, &dim_inst, &dim_grid, &hier, id_datacube)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIMENSION_READ_ERROR);
				continue;
			}

			if (dim_inst.concept_level != OPH_COMMON_CONCEPT_LEVEL_UNKNOWN) {
				snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
				if (oph_hier_get_concept_level_long(filename, dim_inst.concept_level, &concept_level)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve long format of concept level\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_ADDITIONAL_INFO_ERROR);
					if (concept_level)
						free(concept_level);
					concept_level = (char *) malloc(2 * sizeof(char));
					concept_level[0] = dim_inst.concept_level;
					concept_level[1] = 0;
				}
			} else {
				concept_level = (char *) malloc(2 * sizeof(char));
				concept_level[0] = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
				concept_level[1] = 0;
			}

			if (cubedims[l].size)
				printf("| %-16s | %-10s | %-12d | %-15s | %-13s | %-9s | %-5d | %-38s |\n", (dim.dimension_name ? dim.dimension_name : "-"),
				       (dim.dimension_type ? dim.dimension_type : "-"), cubedims[l].size, (hier.hierarchy_name ? hier.hierarchy_name : "-"), concept_level ? concept_level : "-",
				       (cubedims[l].explicit_dim == 1 ? OPH_COMMON_NO_VALUE : OPH_COMMON_YES_VALUE), cubedims[l].level, (dim_grid.grid_name ? dim_grid.grid_name : "-"));
			else
				printf("| %-16s | %-10s | %-12s | %-15s | %-13s | %-9s | %-5d | %-38s |\n", (dim.dimension_name ? dim.dimension_name : "-"),
				       (dim.dimension_type ? dim.dimension_type : "-"), OPH_COMMON_FULL_REDUCED_DIM, (hier.hierarchy_name ? hier.hierarchy_name : "-"),
				       concept_level ? concept_level : "-", (cubedims[l].explicit_dim == 1 ? OPH_COMMON_NO_VALUE : OPH_COMMON_YES_VALUE), cubedims[l].level,
				       (dim_grid.grid_name ? dim_grid.grid_name : "-"));

			if (is_objkey_printable) {
				// Data
				int num_fields = 8, iii, jjj = 0;
				char **jsonvalues = NULL;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "values");
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				jsonvalues[jjj] = strdup(dim.dimension_name ? dim.dimension_name : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(dim.dimension_type ? dim.dimension_type : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				if (dim_inst.size) {
					snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", dim_inst.size);
					jsonvalues[jjj] = strdup(jsontmp);
				} else
					jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(hier.hierarchy_name ? hier.hierarchy_name : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(concept_level ? concept_level : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(cubedims[l].explicit_dim == 1 ? OPH_COMMON_NO_VALUE : OPH_COMMON_YES_VALUE);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%d", cubedims[l].level);
				jsonvalues[jjj] = strdup(jsontmp);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonvalues[jjj] = strdup(dim_grid.grid_name ? dim_grid.grid_name : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_DIMINFO, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					if (concept_level)
						free(concept_level);
					free(cubedims);
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
		printf("+------------------+------------+--------------+-----------------+---------------+-----------+-------+----------------------------------------+\n");

	}
	if (level != 0 && number_of_dimensions) {
		int i = 0;

		oph_odb_dimension dim;
		oph_odb_dimension_instance dim_inst;

		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container);
		int exist_flag = 0;

		if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char *dim_row = NULL;
		char *dim_row_index = NULL;

		printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n");

		int len = 0;
		int first_row = 1;
		int j;
		char *tmp_row = NULL, *tmp_row2 = NULL;
		char tmp_row_buffer[140];
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

		int is_objkey_printable =
		    oph_json_is_objkey_printable(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_CUBESCHEMA_DIMVALUES);

		for (i = 0; i < number_of_dimensions; i++) {
			if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[i].id_dimensioninst, &dim_inst, id_datacube)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIMENSION_READ_ERROR);
				continue;
			}
			if (oph_odb_dim_retrieve_dimension(oDB, dim_inst.id_dimension, &dim, id_datacube)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIMENSION_READ_ERROR);
				continue;
			}

			if (dimension_name) {
				for (j = 0; j < ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name_number; j++) {
					if (strncmp(dim.dimension_name, dimension_name[j], OPH_ODB_DIM_DIMENSION_SIZE) == 0)
						break;
				}
				if (j == ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name_number)
					continue;
			}
			if (dim_inst.size == 0) {
				continue;
			}

			dim_row_index = NULL;
			dim_row = NULL;

			if (dim_inst.fk_id_dimension_label) {
				if (oph_dim_read_dimension_data(db_dimension, label_dimension_table_name, dim_inst.fk_id_dimension_label, MYSQL_DIMENSION, 0, &dim_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_inst.fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_row_index)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(cubedims);
					if (!dim_row)
						free(dim_row);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			} else {
				strncpy(dim.dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
				dim.dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
				if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_inst.fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

			}
			if (!dim_row) {
				if (dim_inst.size) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension data are corrupted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(cubedims);
					if (dim_row_index)
						free(dim_row_index);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				} else
					continue;
			}

			if (!first_row)
				printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n");

			printf("| %-*s%-s %s %-*s |\n", 70 - (int) strlen(dim.dimension_name) / 2 - 4, "", dim.dimension_name,
			       (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index) ? "[index]" : "       ",
			       139 - (70 - (int) strlen(dim.dimension_name) / 2 - 4) - (int) strlen(dim.dimension_name) - 9, "");
			printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n");

			if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_time && dim.calendar && strlen(dim.calendar)) {
				buff_size = OPH_COMMON_MAX_DATE_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_STRING);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_INT_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_INT);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_LONG_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_LONG);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_SHORT_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_SHORT);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_BYTE_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_BYTE);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_FLOAT_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_FLOAT);
			} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				buff_size = OPH_COMMON_MAX_DOUBLE_LENGHT + 2;
				strcpy(tmp_type, OPH_JSON_DOUBLE);
			} else {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Type not supported.\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_TYPE_NOT_SUPPORTED, dim.dimension_type);
				free(dim_row);
				if (dim_row_index)
					free(dim_row_index);
				continue;
			}

			if (is_objkey_printable) {
				// Header
				char **jsonkeys = NULL;
				char **fieldtypes = NULL;
				int num_fields = 1 + (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index ? 1 : 0), iii, jjj = 0;
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "keys");
					free(dim_row);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					if (dim_row_index)
						free(dim_row_index);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index) {
					jsonkeys[jjj] = strdup("INDEX");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
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
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
				}
				jsonkeys[jjj] = strdup("VALUE");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "key");
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
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtypes");
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
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
				}
				fieldtypes[jjj] = strdup(tmp_type);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "fieldtype");
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
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_DIMVALUES, dim.dimension_name, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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

			if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index) {
				buff_size += 3 + OPH_COMMON_MAX_INT_LENGHT;
			}
			tmp_row = (char *) malloc(dim_inst.size * buff_size * sizeof(char));
			if (!tmp_row) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocationg memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "dimension row");
				free(dim_row);
				if (dim_row_index)
					free(dim_row_index);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}

			len = 0;
			for (j = 0; j < dim_inst.size; j++) {
				index = 0;
				if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_time && dim.calendar && strlen(dim.calendar)) {
					index = dim_row_index ? *((long long *) (dim_row_index + j * sizeof(long long))) - 1 : j;
					if (oph_dim_get_time_string_of(dim_row, index, &dim, tmp_type)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
						free(tmp_row);
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%s [%d]", tmp_type, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%s", tmp_type);
					if (is_objkey_printable)
						snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", tmp_type);
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_i = (int *) (dim_row + index * sizeof(int));
					} else {
						dim_i = (int *) (dim_row + j * sizeof(int));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-d [%d]", *dim_i, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-d", *dim_i);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_i, sizeof(int), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
					}
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_l = (long long *) (dim_row + index * sizeof(long long));
					} else {
						dim_l = (long long *) (dim_row + j * sizeof(long long));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-lld [%d]", *dim_l, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-lld", *dim_l);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_l, sizeof(long long), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
					}
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_s = (short *) (dim_row + index * sizeof(short));
					} else {
						dim_s = (short *) (dim_row + j * sizeof(short));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-d [%d]", *dim_s, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-d", *dim_s);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_s, sizeof(short), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
					}
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_b = (char *) (dim_row + index * sizeof(char));
					} else {
						dim_b = (char *) (dim_row + j * sizeof(char));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-d [%d]", *dim_b, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-d", *dim_b);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_b, sizeof(char), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
					}
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_f = (float *) (dim_row + index * sizeof(float));
					} else {
						dim_f = (float *) (dim_row + j * sizeof(float));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-f [%d]", *dim_f, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-f", *dim_f);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_f, sizeof(float), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
					}
				} else if (!strncasecmp(dim.dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					if (dim_row_index) {
						index = *((long long *) (dim_row_index + j * sizeof(long long))) - 1;
						dim_d = (double *) (dim_row + index * sizeof(double));
					} else {
						dim_d = (double *) (dim_row + j * sizeof(double));
					}
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index)
						len += snprintf(tmp_row + len, buff_size - len, "%-f [%d]", *dim_d, j + 1);
					else
						len += snprintf(tmp_row + len, buff_size - len, "%-f", *dim_d);
					if (is_objkey_printable) {
						if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->base64) {
							memset(tmp_value, 0, OPH_COMMON_BUFFER_LEN);
							oph_utl_base64encode(dim_d, sizeof(double), tmp_value, OPH_COMMON_BUFFER_LEN - 1);
						} else
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
					}
				} else {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Type not supported.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBESCHEMA_TYPE_NOT_SUPPORTED, dim.dimension_type);
					if (dim_row_index)
						free(dim_row_index);
					free(dim_row);
					continue;
				}
				if (j != (dim_inst.size - 1))
					len += snprintf(tmp_row + len, 3, ",  ");
				len--;

				if (is_objkey_printable) {
					// Data
					int num_fields = 1 + (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index ? 1 : 0), iii, jjj = 0;
					char **jsonvalues = NULL;
					jsonvalues = (char **) calloc(num_fields, sizeof(char *));
					if (!jsonvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "values");
						free(tmp_row);
						free(dim_row);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row_index)
							free(dim_row_index);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj = 0;
					if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->show_index) {
						snprintf(tmp_index, OPH_COMMON_BUFFER_LEN, "%d", j + 1);
						jsonvalues[jjj] = strdup(tmp_index);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
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
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
					}
					jsonvalues[jjj] = strdup(tmp_value);
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESCHEMA_MEMORY_ERROR_INPUT, "value");
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
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESCHEMA_DIMVALUES, jsonvalues)) {
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
			for (j = 0; j < len; j += 139) {
				strncpy(tmp_row_buffer, tmp_row2, 139);
				tmp_row_buffer[139] = 0;
				tmp_row2 += 139;
				printf("| %-*s |\n", 139, tmp_row_buffer);
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
		printf("+---------------------------------------------------------------------------------------------------------------------------------------------+\n\n");

	}
	free(cubedims);


	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESCHEMA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name) {
		free((char *) ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name);
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	}
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name) {
		oph_tp_free_multiple_value_param_list(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name,
						      ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name_number);
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->dimension_name = NULL;
	}
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_CUBESCHEMA_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
