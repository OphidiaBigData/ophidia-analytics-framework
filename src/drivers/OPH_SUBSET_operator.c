/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

#include "drivers/OPH_SUBSET_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <math.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"
#include "oph_driver_procedure_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#include <pthread.h>

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

	if (!(handle->operator_handle = (OPH_SUBSET_operator_handle *) calloc(1, sizeof(OPH_SUBSET_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->check_grid = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset_num = 0;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types = NULL;
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 0;

	int i, j;
	for (i = 0; i < OPH_SUBSET_LIB_MAX_DIM; ++i) {
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[i] = 0;
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[i] = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[i] = NULL;
	}

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys, &((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->nthread = (unsigned int) strtol(value, NULL, 10);

	//3 - Fill struct with the correct data 
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int data_on_ids[3 + OPH_SUBSET_LIB_MAX_DIM];
	int *id_datacube_in = data_on_ids;
	int *id_dimension = data_on_ids + 3;
	for (i = 0; i < 3 + OPH_SUBSET_LIB_MAX_DIM; ++i)
		data_on_ids[i] = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = 0;
	int s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (s_offset_num > 0) {
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "offset");
			oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset_num = s_offset_num;
		for (i = 0; i < s_offset_num; ++i)
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset[i] = (double) strtod(s_offset[i], NULL);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
	}

	char **sub_dims = 0;
	char **sub_filters = 0;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim = number_of_sub_dims;

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_SUBSET_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_OPHIDIADB_CONFIGURATION_FILE);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_PID_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_NO_INPUT_DATACUBE, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], 1, &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_DATACUBE_FOLDER_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else
			for (i = 0; i < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++i) {
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

		if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_user))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_USER_ID_ERROR);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		id_datacube_in[2] = id_datacube_in[1];
		if (id_datacube_in[1]) {
			value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
				if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, 0, &id_datacube_in[2])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container or it is hidden\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value);
					id_datacube_in[0] = 0;
					id_datacube_in[1] = 0;
				}
			}
		}
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(data_on_ids, 3 + ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_NO_INPUT_DATACUBE, datacube_in);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_NO_INPUT_CONTAINER, datacube_in);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2];

	for (i = 0; i < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++i) {
		if (id_dimension[i] == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIMENSION_MISSED);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_dimension[i] = id_dimension[i];
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[i] = (char *) strndup(sub_filters[i], OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "subset task list");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
	oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);

	char **sub_types = NULL;
	int number_of_sub_types = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &sub_types, &number_of_sub_types)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (number_of_sub_dims && (number_of_sub_types > number_of_sub_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SUBSET_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types = (char *) calloc(1 + number_of_sub_dims, sizeof(char));
	if (!((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "subset_types");
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (number_of_sub_dims) {
		for (i = 0; i < number_of_sub_types; ++i)
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types[i] = !strncmp(sub_types[i], OPH_SUBSET_TYPE_COORD, OPH_TP_TASKLEN);
		if (number_of_sub_types == 1)
			for (; i < number_of_sub_dims; ++i)
				((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types[i] = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types[0];
	}
	oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "grid name");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_GRID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_GRID);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CHECK_GRID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN))
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->check_grid = 1;

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, stream_max_size = 6 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int) + 3 * OPH_TP_TASKLEN;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[4], *params, *params2, *params3;

	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[3] = stream + pointer;
	pointer += 1 + sizeof(int);
	params = stream + pointer;
	pointer += 1 + OPH_TP_TASKLEN;
	params2 = stream + pointer;
	pointer += 1 + OPH_TP_TASKLEN;
	params3 = stream + pointer;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_SUBSET_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DATACUBE_READ_ERROR);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			goto __OPH_EXIT_1;
		}
		// Change the container id
		cube.id_container = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container;

		((OPH_SUBSET_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int last_insertd_id = 0, l;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_CUBEHASDIM_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}

		unsigned long long dim_size[((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim][number_of_dimensions], new_size[number_of_dimensions], block_size;
		int dim_number[((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim], first_explicit, d, i, explicit_dim_number = 0, implicit_dim_number = 0;
		int subsetted_dim[number_of_dimensions];
		char size_string_vector[((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim][OPH_COMMON_BUFFER_LEN], *size_string, *dim_row;
		char temp[OPH_COMMON_BUFFER_LEN];
		oph_subset *subset_struct[((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim];
		oph_odb_dimension dim[number_of_dimensions];
		oph_odb_dimension_instance dim_inst[number_of_dimensions];
		char subarray_param[OPH_TP_TASKLEN];

		for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d)
			dim_number[d] = 0;
		for (l = 0; l < number_of_dimensions; l++) {
			new_size[l] = cubedims[l].size;
			subsetted_dim[l] = -1;
		}

		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			goto __OPH_EXIT_1;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN + 1], operation2[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container);
		char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(o_index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container);
		snprintf(o_label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container);

		int n, compressed = 0;

		for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d)	// Loop on dimensions set as input
		{
			first_explicit = 1;
			for (l = number_of_dimensions - 1; l >= 0; l--) {
				if (cubedims[l].explicit_dim && first_explicit)
					dim_number[d] = first_explicit = 0;	// The considered dimension is explicit
				if (cubedims[l].size)
					dim_size[d][dim_number[d]++] = cubedims[l].size;
				if (cubedims[l].id_dimensioninst == ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_dimension[d]) {
					if (!cubedims[l].size) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This dimension has been completely reduced.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_SUBSET_DIMENSION_REDUCED);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						goto __OPH_EXIT_1;
					} else
						break;
				}
			}
			if (l < 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension informations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIMENSION_INFO_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			subsetted_dim[l] = d;
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d] = cubedims[l].explicit_dim;
			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d] && !dim_number[d]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension sizes.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIMENSION_SIZE_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d])
				explicit_dim_number++;
			else
				implicit_dim_number++;

			block_size = 1;
			for (i = 0; i < dim_number[d] - 1; ++i)
				block_size *= dim_size[d][i];
			size_string = &(size_string_vector[d][0]);
			snprintf(size_string, OPH_COMMON_BUFFER_LEN, "%lld,%lld", block_size, dim_size[d][dim_number[d] - 1]);

			if (!((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types[d]) {

				// Parsing indexes
				subset_struct[d] = 0;
				if (oph_subset_init(&subset_struct[d])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate subset struct.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT,
						"subset");
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (oph_subset_parse
				    (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], strlen(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]), subset_struct[d],
				     dim_size[d][dim_number[d] - 1])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot parse subset string '%s'. Select values in [1,%lld].\n",
					      ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], dim_size[d][dim_number[d] - 1]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_PARSE_ERROR,
						((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], dim_size[d][dim_number[d] - 1]);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (oph_subset_size(subset_struct[d], dim_size[d][dim_number[d] - 1], &new_size[l], 0, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot evaluate subset size.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_SIZE_ERROR);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}
			// Dimension extraction
			if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types[d]) {

				if (compressed) {
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "uncompress(%s)", MYSQL_DIMENSION);
					if (n >= OPH_COMMON_BUFFER_LEN) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						goto __OPH_EXIT_1;
					}
				} else
					strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);
				dim_row = 0;

				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, 0, &dim_row) || !dim_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					goto __OPH_EXIT_1;
				}
				if (dim_inst[l].fk_id_dimension_label) {
					if (oph_dim_read_dimension_filtered_data
					    (db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, operation, 0, &dim_row, dim[l].dimension_type, dim_inst[l].size) || !dim_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						goto __OPH_EXIT_1;
					}
				} else
					strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);	// A reduced dimension is handled by indexes

				// Pre-parsing time dimensions
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->time_filter && dim[l].calendar && strlen(dim[l].calendar)) {
					if ((n =
					     oph_dim_parse_season_subset(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], &(dim[l]), temp, dim_row,
									 dim_size[d][dim_number[d] - 1]))) {
						if (n != OPH_DIM_TIME_PARSING_ERROR) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_SUBSET_PARSE_ERROR, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
							if (dim_row)
								free(dim_row);
							oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
							oph_odb_cube_free_datacube(&cube);
							free(cubedims);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							goto __OPH_EXIT_1;
						}
					} else {
						free(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
						((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = strndup(temp, OPH_TP_TASKLEN);
					}
					if ((n = oph_dim_parse_time_subset(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], &(dim[l]), temp))) {
						if (n != OPH_DIM_TIME_PARSING_ERROR) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_SUBSET_PARSE_ERROR, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
							if (dim_row)
								free(dim_row);
							oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
							oph_odb_cube_free_datacube(&cube);
							free(cubedims);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							goto __OPH_EXIT_1;
						}
					} else {
						free(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
						((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = strndup(temp, OPH_TP_TASKLEN);
					}
				}
				// Real parsing values
				subset_struct[d] = 0;
				if (oph_subset_value_to_index
				    (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], dim_row, dim_size[d][dim_number[d] - 1], dim[l].dimension_type,
				     d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset_num ? ((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset[d] : 0, temp,
				     &subset_struct[d])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot convert the subset '%s' into a subset expressed as indexes.\n",
					      ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_PARSE_ERROR,
						((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
					if (dim_row)
						free(dim_row);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (dim_row)
					free(dim_row);

				if (!strlen(temp))	// Empty set
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_SUBSET_EMPTY_DATACUBE);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_EMPTY_DATACUBE);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}

				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]) {
					free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
					((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = NULL;
				}
				if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = (char *) strndup(temp, OPH_TP_TASKLEN))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT,
						"task");
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (oph_subset_size(subset_struct[d], dim_size[d][dim_number[d] - 1], &new_size[l], 0, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot evaluate subset size.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_SIZE_ERROR);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}

			snprintf(subarray_param, OPH_TP_TASKLEN, "'%s',1,%d", ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], dim_inst[l].size);
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d] = (char *) strndup(subarray_param, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT, "task");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			dim_inst[l].size = new_size[l];

			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d]) {
				// WHERE clause building
				char where_clause[OPH_TP_TASKLEN];
				*where_clause = '\0';
				for (i = 0; i < (int) subset_struct[d]->number; ++i)	// loop on subsets
				{
					if (i)
						strcat(where_clause, " OR ");
					snprintf(temp, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_ISINSUBSET_PLUGIN, MYSQL_FRAG_ID, size_string, subset_struct[d]->start[i], subset_struct[d]->stride[i],
						 subset_struct[d]->end[i]);
					strncat(where_clause, temp, OPH_TP_TASKLEN - strlen(where_clause) - 1);
				}
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]) {
					free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
					((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = NULL;
				}
				if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = (char *) strndup(where_clause, OPH_TP_TASKLEN))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT,
						"task");
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			} else {
				snprintf(subarray_param, OPH_TP_TASKLEN, "'%s',%s", ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], size_string);
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]) {
					free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d]);
					((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = NULL;
				}
				if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d] = (char *) strndup(subarray_param, OPH_TP_TASKLEN))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT,
						"task");
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}
		}

		for (l = 0; l < number_of_dimensions; ++l) {
			cubedims[l].size = new_size[l];	// Size update
			if (subsetted_dim[l] < 0) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}
		}

		if (explicit_dim_number) {
			// Fragment keys
			oph_odb_fragment_list2 frags;
			if (oph_odb_stge_retrieve_fragment_list2(oDB, datacube_id, &frags)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve fragment keys\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_KEY_ERROR);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (!frags.size) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "OphidiaDB parameters are corrupted - check 'fragment' table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_BAD_PARAMETER);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_odb_stge_free_fragment_list2(&frags);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size = frags.size;
			int *keys = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys = (int *) malloc(3 * frags.size * sizeof(int));

			int id = 1, is_in_subset, max = 0, number_of_frags = 0, offset;
			unsigned long block_size[((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim], min_block_size = 0;
			for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d) {
				block_size[d] = 1;
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d]) {
					for (i = 0; i < dim_number[d] - 1; i++)
						block_size[d] *= dim_size[d][i];
					if (!min_block_size || (min_block_size > block_size[d]))
						min_block_size = block_size[d];
				}
			}
			for (i = 0; i < frags.size; i++) {
				keys[i] = keys[i + frags.size] = keys[i + 2 * frags.size] = 0;
				if (frags.value[i].frag_relative_index != i + 1) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Value of fragmentrelativeindex is not expected\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_SUBSET_RETREIVE_FRAGMENT_ID_ERROR);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_odb_stge_free_fragment_list2(&frags);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			}
			for (i = 0; i < frags.size;) {
				if (id <= frags.value[i].key_end) {
					is_in_subset = 0;
					for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d)
						if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d]) {
							is_in_subset = oph_subset_index_is_in_subset(oph_subset_id_to_index(id, &(dim_size[d][0]), dim_number[d]), subset_struct[d]);
							if (!is_in_subset)
								break;
						}
					if (is_in_subset) {
						if (!keys[i]) {
							keys[i] = max;	// key_end
							keys[i + frags.size] = keys[i] + 1;	// key_start
						}
						max = keys[i] += min_block_size;
						id += min_block_size;
					} else
						id += block_size[d];
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

			cube.hostxdatacube = 0;
			cube.fragmentxdb = 0;
			cube.tuplexfragment = 0;

			int id_db = -1, id_dbms = -1, id_host = -1, num_frag = 0, num_db = 0, num_dbms = 0;

			int first = 1, prev_id = 0, interval = 0, j, new_db_number = 0;
			char *tmp = cube.frag_relative_index_set, tmp2[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
			int new_id_db[cube.db_number];
			for (j = 0; j < cube.db_number; j++)
				new_id_db[j] = 0;

			tmp[0] = '\0';
			for (i = 0; i < frags.size; i++)
				if (keys[i]) {
					// Partitioned table update
					for (j = 0; j < new_db_number; j++)
						if (new_id_db[j] == frags.value[i].id_db)
							break;
					if (j == new_db_number) {
						if (new_db_number > cube.db_number) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_SUBSET_INTERNAL_ERROR);
							oph_odb_cube_free_datacube(&cube);
							free(cubedims);
							oph_odb_stge_free_fragment_list2(&frags);
							oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							goto __OPH_EXIT_1;
						}
						new_id_db[new_db_number++] = frags.value[i].id_db;
					}

					if (id_host >= 0) {
						if (id_host == frags.value[i].id_host) {
							if (id_dbms < 0) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error\n");
								logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
									OPH_LOG_OPH_SUBSET_INTERNAL_ERROR);
								oph_odb_cube_free_datacube(&cube);
								free(cubedims);
								oph_odb_stge_free_fragment_list2(&frags);
								oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
								oph_dim_disconnect_from_dbms(db->dbms_instance);
								oph_dim_unload_dim_dbinstance(db);
								goto __OPH_EXIT_1;
							}
							if (id_dbms != frags.value[i].id_dbms)
								num_dbms++;
						} else {
							id_host = frags.value[i].id_host;
							num_dbms = 1;
							cube.hostxdatacube++;
						}
					} else {
						id_host = frags.value[i].id_host;
						cube.hostxdatacube = num_dbms = 1;
					}

					if (id_dbms >= 0) {
						if (id_dbms == frags.value[i].id_dbms) {
							if (id_db < 0) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error\n");
								logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
									OPH_LOG_OPH_SUBSET_INTERNAL_ERROR);
								oph_odb_cube_free_datacube(&cube);
								free(cubedims);
								oph_odb_stge_free_fragment_list2(&frags);
								oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
								oph_dim_disconnect_from_dbms(db->dbms_instance);
								oph_dim_unload_dim_dbinstance(db);
								goto __OPH_EXIT_1;
							}
							if (id_db != frags.value[i].id_db)
								num_db++;
						} else {
							id_dbms = frags.value[i].id_dbms;
							num_db = 1;
						}
					} else {
						id_dbms = frags.value[i].id_dbms;
						num_db = 1;
					}

					// cube.fragmentxdb
					if (id_db >= 0) {
						if (id_db == frags.value[i].id_db)
							num_frag++;
						else {
							if (cube.fragmentxdb < num_frag)
								cube.fragmentxdb = num_frag;
							id_db = frags.value[i].id_db;
							num_frag = 1;
						}
					} else {
						id_db = frags.value[i].id_db;
						num_frag = 1;
					}

					// cube.tuplexfragment
					if (cube.tuplexfragment < (max = keys[i] - keys[i + frags.size] + 1))
						cube.tuplexfragment = max;

					keys[i + 2 * frags.size] = ++number_of_frags;
					if (!first) {
						if (frags.value[i].frag_relative_index > prev_id + 1) {
							if (interval) {
								snprintf(tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "-%d;", prev_id);
								strncat(tmp, tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
								interval = 0;
							} else
								strncat(tmp, ";", OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
						} else
							interval = 1;	// Found a contiguous fragment (frags.value[i].frag_relative_index == prev_id+1)
					} else
						first = 0;
					if (!interval) {
						snprintf(tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", frags.value[i].frag_relative_index);
						strncat(tmp, tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
					}
					prev_id = frags.value[i].frag_relative_index;
				}

			if (cube.fragmentxdb < num_frag)
				cube.fragmentxdb = num_frag;	// Last fragment

			if (interval) {
				snprintf(tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "-%d", prev_id);
				strncat(tmp, tmp2, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			}

			oph_odb_stge_free_fragment_list2(&frags);

			if (!number_of_frags || !cube.tuplexfragment) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Empty datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_EMPTY_DATACUBE);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (new_db_number < cube.db_number) {
				if (!cube.id_db || !new_db_number) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_INTERNAL_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				free(cube.id_db);
				cube.id_db = (int *) malloc(new_db_number * sizeof(int));
				for (j = 0; j < new_db_number; j++)
					cube.id_db[j] = new_id_db[j];
				cube.db_number = new_db_number;
			}
			//Copy fragment id relative index set
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_RETREIVE_IDS_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_ids_create_new_id_string(&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1, number_of_frags)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment relative index set\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_UPDATE_IDS_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			// WHERE clause
			char where_clause[OPH_TP_TASKLEN];
			*where_clause = '\0';
			i = 0;
			for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d)	// loop on dimensions
			{
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d]) {
					if (i)
						strcat(where_clause, " AND ");
					else
						i = 1;
					strcat(where_clause, "(");
					strncat(where_clause, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], OPH_TP_TASKLEN - strlen(where_clause) - 1);
					strcat(where_clause, ")");
				}
			}
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause = (char *) strndup(where_clause, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT,
					"WHERE clause");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
		} else {
			//Copy fragment id relative index set   
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_RETREIVE_IDS_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
		}

		int tot_frag_num = 0;
		if (oph_ids_count_number_of_ids(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids, &tot_frag_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DUPLICATE_RETREIVE_IDS_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			goto __OPH_EXIT_1;
		}
		//Check that product of ncores and nthread is at most equal to total number of fragments        
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > (unsigned int) tot_frag_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of cores per number of threads is bigger than total fragments\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
				"Number of cores per number of threads is bigger than total fragments\n");
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			goto __OPH_EXIT_1;
		}

		if (implicit_dim_number) {
			int first = 1;
			char buffer[OPH_TP_TASKLEN];
			*buffer = '\0';
			for (d = 0; d < ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim; ++d)	// loop on dimensions
			{
				if (!((OPH_SUBSET_operator_handle *) handle->operator_handle)->explicited[d]) {
					if (first)
						first = 0;
					else
						strncat(buffer, ",", OPH_TP_TASKLEN - strlen(buffer) - 1);
					strncat(buffer, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[d], OPH_TP_TASKLEN - strlen(buffer) - 1);
				}
			}
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause = (char *) strndup(buffer, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT,
					"APPLY clause");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			snprintf(buffer, OPH_TP_TASKLEN, "'oph_%s','oph_%s'", cube.measure_type, cube.measure_type);
			if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type = (char *) strndup(buffer, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT,
					"APPLY clause (type)");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
		}
		// Begin - Dimension table management

		// Grid management
		int id_grid = 0, new_grid = 0, stored_dim_num = 0, grid_exist = 0;
		oph_odb_dimension *stored_dims = NULL;
		oph_odb_dimension_instance *stored_dim_insts = NULL;

		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name) {
			if (oph_odb_dim_retrieve_grid_id
			    (oDB, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, &id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading grid id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_GRID_LOAD_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (id_grid) {
				if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
				    (oDB, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
				     &stored_dims, &stored_dim_insts, &stored_dim_num)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension list from grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_SUBSET_DIM_LOAD_FROM_GRID_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
			} else {
				new_grid = 1;
				oph_odb_dimension_grid grid;
				strncpy(grid.grid_name, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				grid.grid_name[OPH_ODB_DIM_GRID_SIZE] = 0;
				if (oph_odb_dim_insert_into_grid_table(oDB, &grid, &id_grid, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while storing grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_GRID_STORE_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						"Grid already exists: dimensions will be not associated to a grid.\n");
					id_grid = 0;
				}
			}
		}
		//New fields
		cube.id_source = 0;
		cube.level++;
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_SUBSET_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_user)) {
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		char dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container);
		char o_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(o_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container);

		for (l = 0; l < number_of_dimensions; ++l) {
			if (!dim_inst[l].fk_id_dimension_index) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
				break;
			}
			dim_row = 0;
			if ((d = subsetted_dim[l]) < 0) {
				// This dimension has not been subsetted
				if (compressed) {
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "uncompress('','',%s)", MYSQL_DIMENSION);
					if (n >= OPH_COMMON_BUFFER_LEN) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						goto __OPH_EXIT_1;
					}
				} else {
					strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);
					operation[OPH_COMMON_BUFFER_LEN] = 0;
				}
				strncpy(operation2, operation, OPH_COMMON_BUFFER_LEN);
			} else	// Subsetted dimension
			{
				if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d] && strlen(((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d])) {
					if (compressed)
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN_COMPR, dim[l].dimension_type, dim[l].dimension_type, MYSQL_DIMENSION,
							     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d]);
					else
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN, dim[l].dimension_type, dim[l].dimension_type, MYSQL_DIMENSION,
							     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d]);
				} else
					n = OPH_COMMON_BUFFER_LEN;
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					goto __OPH_EXIT_1;
				}
				if (compressed)
					n = snprintf(operation2, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN_COMPR, OPH_DIM_INDEX_DATA_TYPE, OPH_DIM_INDEX_DATA_TYPE, MYSQL_DIMENSION,
						     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d]);
				else
					n = snprintf(operation2, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN, OPH_DIM_INDEX_DATA_TYPE, OPH_DIM_INDEX_DATA_TYPE, MYSQL_DIMENSION,
						     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[d]);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					goto __OPH_EXIT_1;
				}
			}

			if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation2, compressed, &dim_row)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
				if (dim_row)
					free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
				goto __OPH_EXIT_1;
			}

			if (new_grid || !((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name
			    || (((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container != ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container)) {
				if (oph_dim_insert_into_dimension_table(db, o_index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_ROW_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].id_grid = id_grid;
				if (oph_odb_dim_insert_into_dimensioninstance_table
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_SUBSET_DIM_INSTANCE_STORE_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
					goto __OPH_EXIT_1;
				}
			} else	// Check for grid correctness...
			{
				int match = 1;
				for (d = 0; d < stored_dim_num; ++d)
					if (!strcmp(stored_dims[d].dimension_name, dim[l].dimension_name) && !strcmp(stored_dims[d].dimension_type, dim[l].dimension_type)
					    && (stored_dim_insts[d].size == dim_inst[l].size) && (stored_dim_insts[d].concept_level == dim_inst[l].concept_level))
						break;
				//If original dimension is found and has size 0 then do not compare
				if (!((d < stored_dim_num) && !dim_inst[l].size) && ((OPH_SUBSET_operator_handle *) handle->operator_handle)->check_grid) {
					if ((d >= stored_dim_num)
					    || oph_dim_compare_dimension(db, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, stored_dim_insts[d].fk_id_dimension_index,
									 &match) || match) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This grid cannot be used in this context or error in checking dimension data or metadata\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_SUBSET_DIM_CHECK_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						goto __OPH_EXIT_1;
					}
				}
				cubedims[l].id_dimensioninst = stored_dim_insts[d].id_dimensioninst;

				if (stored_dim_insts[d].fk_id_dimension_label)	// Check for values
				{
					if (dim_row)
						free(dim_row);
					dim_row = NULL;

					if (oph_dim_read_dimension_data(db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, operation, compressed, &dim_row) || !dim_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						goto __OPH_EXIT_1;
					}

					if (oph_dim_compare_dimension2
					    (db, label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, operation, dim_row, stored_dim_insts[d].fk_id_dimension_label, &match) || match) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This grid cannot be used in this context or error in checking dimension data or metadata\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_SUBSET_DIM_CHECK_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						goto __OPH_EXIT_1;
					}
				}
			}
			if (dim_row)
				free(dim_row);
		}
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		if (stored_dims)
			free(stored_dims);
		if (stored_dim_insts)
			free(stored_dim_insts);

		if (id_grid && oph_odb_dim_enable_grid(oDB, id_grid)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to enable grid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, "Unable to enable grid\n");
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		// End - Dimension table management

		oph_subset_vector_free(subset_struct, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->number_of_dim);

		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause) {
			char buff_tmp[OPH_TP_TASKLEN];
			snprintf(buff_tmp, OPH_TP_TASKLEN, "%s,%s,%s", ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type, MYSQL_FRAG_MEASURE,
				 ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause);
			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause)
				snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_SUBSET_QUERY, "ID", buff_tmp, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause);
			else
				snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_SUBSET_QUERY, "ID", buff_tmp, "");
		} else {
			if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause)
				snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_SUBSET_QUERY, "ID", MYSQL_FRAG_MEASURE,
					 ((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause);
			else
				snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_SUBSET_QUERY, "ID", MYSQL_FRAG_MEASURE, "");
		}
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_datacube;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_TASK_INSERT_ERROR, new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_SUBSET_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		memcpy(id_string[3], &((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size, sizeof(int));

		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause)
			strncpy(params, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause, OPH_TP_TASKLEN);
		else
			*params = '\0';
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause)
			strncpy(params2, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause, OPH_TP_TASKLEN);
		else
			*params2 = '\0';
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type)
			strncpy(params3, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type, OPH_TP_TASKLEN);
		else
			*params3 = '\0';
	}
      __OPH_EXIT_1:

	//Broadcast to all other processes the fragment relative index
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MASTER_TASK_INIT_FAILED);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "fragment ids");
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause = (char *) strndup(params, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "WHERE clause");
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause = (char *) strndup(params2, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "APPLY clause");
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type = (char *) strndup(params3, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "APPLY clause");
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size = *((int *) id_string[3]);

		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size) {
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys = (int *) malloc(3 * ((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size * sizeof(int));
			*(((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys) = -1;
		}
	}

	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size) {
		MPI_Bcast(((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys, 3 * ((OPH_SUBSET_operator_handle *) handle->operator_handle)->frags_size, MPI_INT, 0, MPI_COMM_WORLD);
		if (*(((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys) < 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MASTER_TASK_INIT_FAILED);
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{

	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_SUBSET_operator_handle *oper_handle = (OPH_SUBSET_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int l;

	int num_threads = oper_handle->nthread;
	int res[num_threads];

	//In multi-thread code mysql_library_init must be called before starting the threads
	if (mysql_library_init(0, NULL, NULL)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, "MySQL initialization error\n");
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_OPHIDIADB_CONFIGURATION_FILE);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, oper_handle->id_input_datacube, oper_handle->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	struct _thread_struct {
		OPH_SUBSET_operator_handle *oper_handle;
		unsigned int current_thread;
		unsigned int total_threads;
		int proc_rank;
		oph_odb_fragment_list *frags;
		oph_odb_db_instance_list *dbs;
		oph_odb_dbms_instance_list *dbmss;
	};
	typedef struct _thread_struct thread_struct;

	void *exec_thread(void *ts) {

		OPH_SUBSET_operator_handle *oper_handle = ((thread_struct *) ts)->oper_handle;
		int l = ((thread_struct *) ts)->current_thread;
		int num_threads = ((thread_struct *) ts)->total_threads;
		int proc_rank = ((thread_struct *) ts)->proc_rank;

		int id_datacube_out = oper_handle->id_output_datacube;
		int compressed = oper_handle->compressed;

		oph_odb_fragment_list *frags = ((thread_struct *) ts)->frags;
		oph_odb_db_instance_list *dbs = ((thread_struct *) ts)->dbs;
		oph_odb_dbms_instance_list *dbmss = ((thread_struct *) ts)->dbmss;

		int i, k;

		int res = OPH_ANALYTICS_OPERATOR_SUCCESS;

		int fragxthread = (int) floor((double) (frags->size / num_threads));
		int remainder = (int) frags->size % num_threads;
		//Compute starting number of fragments inserted by other threads
		unsigned int current_frag_count = l * fragxthread + (l < remainder ? l : remainder);

		//Update number of fragments to be inserted
		if (l < remainder)
			fragxthread += 1;

		char operation[OPH_COMMON_BUFFER_LEN];
		char query[OPH_COMMON_BUFFER_LEN];
		char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
		int n, in_frag_count = 0, out_frag_count = 0, index = 0;

		oph_ioserver_handler *server = NULL;

		if (oph_dc_setup_dbms_thread(&(server), (dbmss->value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_IOPLUGIN_SETUP_ERROR, (dbmss->value[0]).id_dbms);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		int first_dbms, first_db, first_frag = current_frag_count;

		for (first_db = 0; first_db < dbs->size && res == OPH_ANALYTICS_OPERATOR_SUCCESS; first_db++) {
			//Find db associated to fragment
			if (frags->value[current_frag_count].id_db == dbs->value[first_db].id_db)
				break;
		}
		for (first_dbms = 0; first_dbms < dbmss->size && res == OPH_ANALYTICS_OPERATOR_SUCCESS; first_dbms++) {
			//Find dbms associated to db
			if (dbs->value[first_db].id_dbms == dbmss->value[first_dbms].id_dbms)
				break;
		}

		//For each DBMS
		for (i = first_dbms; (i < dbmss->size) && (in_frag_count < fragxthread) && (res == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {

			if (oph_dc_connect_to_dbms(server, &(dbmss->value[i]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_DB_SELECTION_ERROR, (dbs->value[i]).db_name);
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = first_frag; (k < frags->size) && (in_frag_count < fragxthread) && (res == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags->value[k].db_instance != &(dbs->value[i]))
					continue;

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, proc_rank, (current_frag_count + out_frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//SUBSET mysql plugin
				if (oper_handle->frags_size) {
					// Change some fragment fields
					index = frags->value[k].frag_relative_index - 1;
					frags->value[k].key_start = oper_handle->keys[index + oper_handle->frags_size];
					frags->value[k].key_end = oper_handle->keys[index];
				}

				if (oper_handle->apply_clause && strlen(oper_handle->apply_clause)) {
					if (compressed)
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN_COMPR2, oper_handle->apply_clause_type, MYSQL_FRAG_MEASURE, oper_handle->apply_clause);
					else
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_PLUGIN2, oper_handle->apply_clause_type, MYSQL_FRAG_MEASURE, oper_handle->apply_clause);
				} else
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "%s", MYSQL_FRAG_MEASURE);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//SUBSET fragment
				int tmp = frags->value[k].key_start;
				if (tmp) {

					//SUBSET mysql plugin
#ifdef OPH_DEBUG_MYSQL
					if (oper_handle->frags_size)
						printf("ORIGINAL QUERY: " OPH_SUBSET_QUERY2_MYSQL "\n", frags->value[k].db_instance->db_name, frags->value[k].fragment_name, tmp, operation,
						       frags->value[k].db_instance->db_name, frag_name_out, oper_handle->where_clause);
					else
						printf("ORIGINAL QUERY: " OPH_SUBSET_QUERY2_MYSQL "\n", frags->value[k].db_instance->db_name, frags->value[k].fragment_name, 0, operation,
						       frags->value[k].db_instance->db_name, frag_name_out, "");
#endif
					if (oper_handle->frags_size)
						n = snprintf(query, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_QUERY2, frags->value[k].db_instance->db_name, frags->value[k].fragment_name, tmp, operation,
							     frags->value[k].db_instance->db_name, frag_name_out, oper_handle->where_clause);
					else
						n = snprintf(query, OPH_COMMON_BUFFER_LEN, OPH_SUBSET_QUERY2, frags->value[k].db_instance->db_name, frags->value[k].fragment_name, 0, operation,
							     frags->value[k].db_instance->db_name, frag_name_out, "");
					if (n >= OPH_COMMON_BUFFER_LEN) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
						res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					//SUBSET fragment
					if (oph_dc_create_fragment_from_query(server, &(frags->value[k]), NULL, query, 0, 0, 0)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in creating function.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_SUBSET_NEW_FRAG_ERROR, frag_name_out);
						res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
					//Change the other fragment fields
					frags->value[k].id_datacube = id_datacube_out;
					strncpy(frags->value[k].fragment_name, frag_name_out, OPH_ODB_STGE_FRAG_NAME_SIZE);
					frags->value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;
					if (oper_handle->frags_size)
						frags->value[k].frag_relative_index = oper_handle->keys[index + 2 * oper_handle->frags_size];

					out_frag_count++;
				}
				in_frag_count++;
			}

			oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));

			if (res != OPH_ANALYTICS_OPERATOR_SUCCESS) {
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
			}
			//Update fragment counter
			first_frag += in_frag_count;
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
		ts[l].current_thread = l;
		ts[l].frags = &frags;
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
	if (oph_odb_stge_insert_into_fragment_table2(&oDB_slave, frags.value, frags.size)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, "Unable to update fragment table.\n");
		oper_handle->execute_error = 1;
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		//In multi-thread code mysql_library_end must be called after executing the threads
		mysql_library_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_free_ophidiadb_thread(&oDB_slave);
	mysql_thread_end();
	//In multi-thread code mysql_library_end must be called after executing the threads
	mysql_library_end();

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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_SUBSET_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_container,
			 ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_SUBSET)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_SUBSET, "Output Cube", jsonbuf)) {
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
		//Delete fragments
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container,
						   ((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids, 0, 0, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->nthread))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
			}
		}
		//Before deleting wait for all process to reach this point
		MPI_Barrier(MPI_COMM_WORLD);

		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_SUBSET_operator_handle *) handle->operator_handle)->oDB, id_datacube, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->id_input_container);
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_disconnect_from_ophidiadb(&((OPH_SUBSET_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_SUBSET_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	int i;
	for (i = 0; i < OPH_SUBSET_LIB_MAX_DIM; ++i) {
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[i]) {
			free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[i]);
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->task[i] = NULL;
		}
		if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[i]) {
			free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[i]);
			((OPH_SUBSET_operator_handle *) handle->operator_handle)->dim_task[i] = NULL;
		}
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys) {
		free((int *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->keys = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause) {
		free((int *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->where_clause = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause) {
		free((int *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type) {
		free((int *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->apply_clause_type = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name) {
		free((int *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys, ((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->description);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset) {
		free((double *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->offset = NULL;
	}
	if (((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types) {
		free((char *) ((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types);
		((OPH_SUBSET_operator_handle *) handle->operator_handle)->subset_types = NULL;
	}
	free((OPH_SUBSET_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
