/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_CONCATNC_operator.h"

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

	if (!(handle->operator_handle = (OPH_CONCATNC_operator_handle *) calloc(1, sizeof(OPH_CONCATNC_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->check_exp_dim = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->ncid = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->memory_size = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset = NULL;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue = 0;
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 0;

	NETCDF_var *nc_measure = &(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->measure);
	nc_measure->dims_name = NULL;
	nc_measure->dims_id = NULL;
	nc_measure->dims_length = NULL;
	nc_measure->dims_type = NULL;
	nc_measure->dims_oph_level = NULL;
	nc_measure->dims_concept_level = NULL;

	char *value;
	char *datacube_in = NULL;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int stream_size = OPH_ODB_CUBE_MEASURE_SIZE + 2 * sizeof(int) + 1;
	char stream[stream_size];
	memset(stream, 0, sizeof(stream));
	int id_in_datacube = 0, id_in_container = 0;

	value = hashtbl_get(task_tbl, OPH_ARG_USERID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_ARG_USERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_user = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	while (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_OPHIDIADB_CONFIGURATION_FILE);
			break;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_OPHIDIADB_CONNECTION_ERROR);
			break;
		}
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		if (oph_pid_parse_pid(datacube_in, &id_in_container, &id_in_datacube, &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_PID_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_in_container, id_in_datacube, &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_NO_INPUT_DATACUBE, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_in_datacube, 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_in_container, &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_DATACUBE_FOLDER_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_DATACUBE_PERMISSION_ERROR, username);
			id_in_datacube = id_in_container = 0;
		}
		memcpy(stream, (char *) &id_in_datacube, sizeof(id_in_datacube));
		memcpy(stream + sizeof(int), (char *) &id_in_container, sizeof(id_in_container));

		if (uri)
			free(uri);
		uri = NULL;

		//Get id from measure name
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, id_in_datacube, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_DATACUBE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			break;
		}
		strcpy(stream + 2 * sizeof(int), cube.measure);

		oph_odb_cube_free_datacube(&cube);
		break;
	}
	//Broadcast to all other processes the fragment relative index
	//MPI_Barrier(MPI_COMM_WORLD);
	MPI_Bcast(stream, stream_size, MPI_CHAR, 0, MPI_COMM_WORLD);

	id_in_datacube = *((int *) stream);
	id_in_container = *((int *) (stream + sizeof(int)));
	char *var_name = (char *) (stream + 2 * sizeof(int));

	//Check if sequential part has been completed
	if (id_in_datacube == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube = id_in_datacube;

	if (id_in_container == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATNC_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container = id_in_container;

	NETCDF_var *measure = ((NETCDF_var *) & (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->measure));
	strcpy(measure->varname, var_name);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *input = hashtbl_get(task_tbl, OPH_IN_PARAM_INPUT);
	if (input && strlen(input))
		value = input;
	else if (!strlen(value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The param '%s' is mandatory; at least the param '%s' needs to be set\n", OPH_IN_PARAM_SRC_FILE_PATH, OPH_IN_PARAM_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig = strdup(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (strstr(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strstr(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path, "http://")
	    && !strstr(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path, "https://")) {
		char *pointer = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			char tmp[OPH_COMMON_BUFFER_LEN];
			if (*pointer != '/') {
				value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
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
					free(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path);
					((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path = strdup(tmp);
					pointer = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path;
				}
			}
			if (oph_pid_get_base_src_path(&value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
			free(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path);
			((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path = strdup(tmp);
			free(value);
		}
	}

	if (oph_pid_get_memory_size(&(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->memory_size))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_OPHIDIADB_CONFIGURATION_FILE, "NO-CONTAINER");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	int i;

	//Open netcdf file
	int retval, j = 0;
	if ((retval = nc_open(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path, NC_NOWRITE, &(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->ncid)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", value, nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_OPEN_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ncid = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->ncid;
	//Extract measured variable information
	if ((retval = nc_inq_varid(ncid, measure->varname, &(measure->varid)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Get information from id
	if ((retval = nc_inq_vartype(ncid, measure->varid, &(measure->vartype)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ndims;
	if ((retval = nc_inq_varndims(ncid, measure->varid, &(ndims)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	measure->ndims = ndims;

	if (!(measure->dims_name = (char **) calloc(measure->ndims, sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_length = (size_t *) calloc(measure->ndims, sizeof(size_t)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_length");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_unlim = (char *) calloc(measure->ndims, sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_unlim");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_type = (short int *) calloc(measure->ndims, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_oph_level");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_concept_level = (char *) calloc(measure->ndims, sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_concept_level");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_id = (int *) calloc(measure->ndims, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_id");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if ((retval = nc_inq_vardimid(ncid, measure->varid, measure->dims_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	int unlimdimid;
	if ((retval = nc_inq_unlimdim(ncid, &unlimdimid))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Extract dimensions information and check names provided by task string
	char *dimname;
	for (i = 0; i < ndims; i++) {
		measure->dims_unlim[i] = measure->dims_id[i] == unlimdimid;
		measure->dims_name[i] = (char *) malloc((NC_MAX_NAME + 1) * sizeof(char));
		if ((retval = nc_inq_dim(ncid, measure->dims_id[i], measure->dims_name[i], &measure->dims_length[i]))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", nc_strerror(retval));
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	// This part is useless: effettive setting is postponed

	// TODO: it is assumed that there is only one implicit dimension
	measure->nexp = ndims - 1;
	measure->nimp = 1;

	// TODO: use order in nc file, but OPH_IMPORTNC exploits user data
	int level = 1;
	for (i = 0; i < measure->nexp; i++) {
		measure->dims_oph_level[i] = level++;
		measure->dims_type[i] = 1;
	}

	// TODO: use order in nc file, but OPH_IMPORTNC exploits user data
	level = 1;
	for (i = measure->nexp; i < measure->ndims; i++) {
		measure->dims_type[i] = 0;
		measure->dims_oph_level[i] = level++;
	}

//ADDED TO MANAGE SUBSETTED IMPORT

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = NULL;
	int s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	double *offset = NULL;
	if (s_offset_num > 0) {
		offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "offset");
			oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < s_offset_num; ++i)
			offset[i] = (double) strtod(s_offset[i], NULL);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
	}

	char **sub_dims = 0;
	char **sub_filters = 0;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	int is_index = strncmp(value, OPH_CONCATNC_SUBSET_COORD, OPH_TP_TASKLEN);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int issubset = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_FILTER);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check dimension names
	for (i = 0; i < number_of_sub_dims; i++) {
		dimname = sub_dims[i];
		for (j = 0; j < ndims; j++)
			if (!strcmp(dimname, measure->dims_name[j]))
				break;
		if (j == ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension '%s' related to variable '%s' in in nc file\n", dimname, measure->varname);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, "NO-CONTAINER", dimname, measure->varname);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	//Check the sub_filters strings
	int tf = -1;		// Id of time filter
	for (i = 0; i < number_of_sub_dims; i++) {
		if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->time_filter && strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR[1])) {
			if (tf >= 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not more than one time dimension can be considered\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			tf = i;
			dimname = sub_dims[i];
			for (j = 0; j < ndims; j++)
				if (!strcmp(dimname, measure->dims_name[j]))
					break;
		} else if (strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2) != strrchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided range are not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if (tf >= 0) {
		oph_odb_dimension dim;
		oph_odb_dimension *time_dim = &dim, *tot_dims = NULL;


		size_t packet_size = sizeof(oph_odb_dimension);
		char buffer[packet_size];

		if (handle->proc_rank == 0) {
			int flag = 1, number_of_dimensions_c = 0;
			while (flag) {
				ophidiadb *oDB = &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->oDB;

				if (oph_odb_dim_retrieve_dimension_list_from_container
				    (oDB, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, &tot_dims, &number_of_dimensions_c)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions .\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_CONCATNC_DIMENSION_READ_ERROR);
					break;
				}

				for (i = 0; i < number_of_dimensions_c; ++i)
					if (!strncmp(tot_dims[i].dimension_name, measure->dims_name[j], OPH_ODB_DIM_DIMENSION_SIZE))
						break;
				if (i >= number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found\n", measure->dims_name[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_DIM_CONT_ERROR,
						measure->dims_name[j]);
					break;
				}

				time_dim = tot_dims + i;

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

		long long max_size = QUERY_BUFLEN;
		oph_pid_get_buffer_size(&max_size);
		char temp[max_size];
		if (oph_dim_parse_time_subset(sub_filters[tf], time_dim, temp)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values '%s'\n", sub_filters[tf]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_start_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_end_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
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
		} else if ((ii = check_subset_string(curfilter, i, measure, is_index, ncid, j < s_offset_num ? offset[j] : 0.0))) {
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return ii;
		} else if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
			   || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_INVALID_INPUT_STRING);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_EXP_DIM_LEVEL_ERROR, "NO-CONTAINER");
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

	// Other arguments
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_EXP_DIM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_EXP_DIM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_CHECK_EXP_DIM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->check_exp_dim = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT_NO_CONTAINER, "NO-CONTAINER", "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DIM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE) != 0) {
		if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset = (double *) malloc(sizeof(double)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT_NO_CONTAINER, "NO-CONTAINER", OPH_IN_PARAM_DIM_OFFSET);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		*((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset = (double) strtod(value, NULL);
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIM_CONTINUE);
	if (value && !strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue = 1;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_NULL_OPERATOR_HANDLE,
			((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int pointer, stream_max_size = 5 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int);
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[4];
	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[3] = stream + pointer;

	int retval = 0;

	//Access data in the netcdf file

	NETCDF_var *measure = &(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->measure);
	int ndim = 0;
	int *measure_stream = NULL;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->oDB;

		int id_container_in = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container;
		int last_insertd_id = 0;
		int ncid = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->ncid;
		int i;
		int imp_ndim = 0;

	  /********************************
	   *INPUT PARAMETERS CHECK - BEGIN*
	   ********************************/

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int *tmp_dims_id = NULL;
		oph_odb_dimension *dim = NULL;
		oph_odb_dimension_instance *dim_inst = NULL;
		oph_odb_dimension_grid dim_grid;
		oph_odb_hierarchy *hier = NULL;
		char **dim_rows_index = NULL;
		char **dim_rows = NULL;
		NETCDF_var tmp_var;
		tmp_var.dims_id = NULL;
		tmp_var.dims_length = NULL;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_CUBEHASDIM_READ_ERROR);
			goto __OPH_EXIT_1;
		}

		ndim = number_of_dimensions;
		measure_stream = (int *) malloc((3 + 3 * ndim) * sizeof(int));
		if (!measure_stream) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "measure structure");
			goto __OPH_EXIT_1;
		}
		tmp_dims_id = (int *) malloc(number_of_dimensions * sizeof(int));
		if (!tmp_dims_id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "measure structure");
			goto __OPH_EXIT_1;
		}
		for (i = 0; i < number_of_dimensions; i++) {
			//Count number of implicit dimensions
			if (!cubedims[i].explicit_dim)
				imp_ndim++;
			measure_stream[3 + i] = cubedims[i].level;
			measure_stream[3 + i + number_of_dimensions] = cubedims[i].size;
			measure_stream[3 + i + 2 * number_of_dimensions] = cubedims[i].explicit_dim;
		}
		measure_stream[2] = number_of_dimensions - imp_ndim;

		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_in);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_in);
		int exist_flag = 0;

		if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		int l, k;

		// Grid management
		int id_grid = 0, new_grid = 0, grid_exist = 0;
		if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name) {
			if (oph_odb_dim_retrieve_grid_id
			    (oDB, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, &id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading grid id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_GRID_LOAD_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}

			if (id_grid) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use an already defined grid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_OLD_GRID_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			} else {
				new_grid = 1;
				oph_odb_dimension_grid grid;
				strncpy(grid.grid_name, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				if (oph_odb_dim_insert_into_grid_table(oDB, &grid, &id_grid, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while storing grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_GRID_STORE_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container,
						"Grid already exists: dimensions will be not associated to a grid.\n");
					id_grid = 0;
				}
			}
		}

		dim = (oph_odb_dimension *) malloc(number_of_dimensions * sizeof(oph_odb_dimension));
		dim_inst = (oph_odb_dimension_instance *) malloc(number_of_dimensions * sizeof(oph_odb_dimension_instance));
		hier = (oph_odb_hierarchy *) malloc(number_of_dimensions * sizeof(oph_odb_hierarchy));
		if (!dim || !dim_inst || !hier) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimension structures");
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		//Get all dimension related information
		for (l = 0; l < number_of_dimensions; l++) {
			if (oph_odb_dim_retrieve_full_dimension_info
			    (oDB, cubedims[l].id_dimensioninst, &(dim[l]), &(dim_inst[l]), &(dim_grid), &(hier[l]), ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBESCHEMA_DIMENSION_READ_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}
		}

		int match = 1;

		//Set implicit dimension arrays for values
		dim_rows_index = (char **) malloc(imp_ndim * sizeof(char *));
		dim_rows = (char **) malloc(imp_ndim * sizeof(char *));
		memset(dim_rows, 0, imp_ndim * sizeof(char *));
		memset(dim_rows_index, 0, imp_ndim * sizeof(char *));

		int imp_dim_count = 0;

		//Read cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {

			if (oph_nc_get_nc_var(id_container_in, dim[l].dimension_name, ncid, 1, &tmp_var)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_READ_ERROR, nc_strerror(retval));
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				if (tmp_var.dims_id)
					free(tmp_var.dims_id);
				if (tmp_var.dims_length)
					free(tmp_var.dims_length);
				goto __OPH_EXIT_1;
			}
			tmp_dims_id[l] = tmp_var.dims_id[0];

			if (tmp_var.dims_id)
				free(tmp_var.dims_id);
			if (tmp_var.dims_length)
				free(tmp_var.dims_length);
			tmp_var.dims_id = NULL;
			tmp_var.dims_length = NULL;

			//Modified to allow subsetting
			for (i = 0; i < measure->ndims; i++)
				if (!strncmp(dim[l].dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
					break;
			if (i >= measure->ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension '%s' not found\n", dim[l].dimension_name);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, "Dimension '%s' not found\n", dim[l].dimension_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}

			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				tmp_var.varsize = 1;
			else
				tmp_var.varsize = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;

			if (cubedims[l].explicit_dim) {
				if (tmp_var.varsize != cubedims[l].size) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of dimension '%s' in NC file (%d) does not correspond to the one stored in OphidiaDB (%d)\n", dim[l].dimension_name,
					      tmp_var.varsize, cubedims[l].size);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_SIZE_MISMATCH, dim[l].dimension_name);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
			} else
				measure_stream[3 + number_of_dimensions + l] = tmp_var.varsize;

			if (oph_nc_compare_nc_c_types(id_container_in, tmp_var.vartype, dim[l].dimension_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension type in NC file does not correspond to the one stored in OphidiaDB\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_TYPE_MISMATCH_ERROR, dim[l].dimension_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}
			//Modified to allow subsetting
			tmp_var.dims_start_index = &(measure->dims_start_index[i]);
			tmp_var.dims_end_index = &(measure->dims_end_index[i]);

			//If dimension is implicit then retrieve the values
			if (!cubedims[l].explicit_dim && (new_grid || !((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name)) {
				char *dim_row_index = NULL;
				char *dim_row = NULL;
				if (dim_inst[l].fk_id_dimension_label) {
					if (oph_dim_read_dimension_data(db_dimension, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, MYSQL_DIMENSION, 0, &dim_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row)
							free(dim_row);
						goto __OPH_EXIT_1;
					}
					if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, 0, &dim_row_index)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						if (dim_row)
							free(dim_row);
						if (dim_row_index)
							free(dim_row_index);
						goto __OPH_EXIT_1;
					}
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBESCHEMA_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}

				char *dim_array = NULL;
				if (oph_nc_get_dim_array2
				    (id_container_in, ncid, tmp_var.varid, dim[l].dimension_type, tmp_var.varsize, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index), &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_READ_ERROR, nc_strerror(retval));
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dim_row);
					free(dim_row_index);
					goto __OPH_EXIT_1;
				}

				if (!strncasecmp(OPH_COMMON_INT_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(int));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					int i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(int *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(int));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (int) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							i_tmp_start = (3 * i_tmp_start - *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(int))) / 2;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						i_tmp = i_tmp_start + *(int *) (dim_array + (k) * sizeof(int));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(int)), (int *) &i_tmp, sizeof(int));
					}
				} else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(long long));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					long long i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(long long *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(long long));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (long long) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							i_tmp_start = (3 * i_tmp_start - *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(long long))) / 2;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						i_tmp = i_tmp_start + *(long long *) (dim_array + (k) * sizeof(long long));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(long long)), (long long *) &i_tmp, sizeof(long long));
					}
				} else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(short));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					short i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(short *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(short));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (short) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							i_tmp_start = (3 * i_tmp_start - *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(short))) / 2;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						i_tmp = i_tmp_start + *(short *) (dim_array + (k) * sizeof(short));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(short)), (short *) &i_tmp, sizeof(short));
					}
				} else if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(char));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					char i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(char));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (char) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							i_tmp_start = (3 * i_tmp_start - *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(char))) / 2;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						i_tmp = i_tmp_start + *(char *) (dim_array + (k) * sizeof(char));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(char)), (char *) &i_tmp, sizeof(char));
					}
				} else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(float));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					float f_tmp_start = 0, f_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						f_tmp_start = *(float *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(float));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							f_tmp_start += (float) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							f_tmp_start = (3.0 * f_tmp_start - *(float *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(float))) / 2.0;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						f_tmp = f_tmp_start + *(float *) (dim_array + (k) * sizeof(float));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(float)), (float *) &f_tmp, sizeof(float));
					}
				} else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_rows[imp_dim_count] = realloc(dim_row, (tmp_var.varsize + dim_inst[l].size) * sizeof(double));
					if (!dim_rows[imp_dim_count]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					double d_tmp_start = 0, d_tmp;
					if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_continue) {
						d_tmp_start = *(double *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(double));
						if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset)
							d_tmp_start += (double) *((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset;
						else if (dim_inst[l].size > 1)
							d_tmp_start = (3.0 * d_tmp_start - *(double *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 2) * sizeof(double))) / 2.0;
					}
					for (k = 0; k < tmp_var.varsize; k++) {
						d_tmp = d_tmp_start + *(double *) (dim_array + (k) * sizeof(double));
						memcpy((char *) (dim_rows[imp_dim_count] + (k + dim_inst[l].size) * sizeof(double)), (double *) &d_tmp, sizeof(double));
					}
				}
				free(dim_array);
				dim_array = NULL;

				dim_rows_index[imp_dim_count] = realloc(dim_row_index, (tmp_var.varsize + dim_inst[l].size) * sizeof(long long));
				if (!dim_rows_index[imp_dim_count]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dimensions binary array");
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dim_row_index);
					goto __OPH_EXIT_1;
				}
				long long tmp;
				tmp = *(long long *) (dim_rows_index[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(long long));
				for (k = 0; k < tmp_var.varsize; k++) {
					tmp++;
					memcpy((char *) (dim_rows_index[imp_dim_count] + (k + dim_inst[l].size) * sizeof(long long)), (long long *) &tmp, sizeof(long long));
				}
				imp_dim_count++;

			} else if ((!new_grid && ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name)
				   || (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->check_exp_dim && cubedims[l].explicit_dim)) {
				// If old grid is given on dimension is explicit and check_exp_dim is set then compare the values
				char *dim_array = NULL;

				if (oph_nc_get_dim_array2
				    (id_container_in, ncid, tmp_var.varid, dim[l].dimension_type, dim_inst[l].size, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index), &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_READ_ERROR, nc_strerror(retval));
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}

				if (oph_dim_compare_dimension(db_dimension, label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, dim_array, dim_inst[l].fk_id_dimension_label, &match)
				    || match) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n", dim[l].dimension_name);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_INPUT_DIMENSION_MISMATCH, dim[l].dimension_name);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dim_array);
					goto __OPH_EXIT_1;
				}

				free(dim_array);
				dim_array = NULL;
			}
		}

		//Get id from measure name
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_DATACUBE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		//retrieve measure id
		if ((retval = nc_inq_varid(ncid, cube.measure, &(tmp_var.varid)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_READ_ERROR, nc_strerror(retval));
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		measure_stream[0] = tmp_var.varid;
		nc_type tmp_nc;
		oph_nc_get_nc_type(cube.measure_type, &tmp_nc);
		measure_stream[1] = (short int) tmp_nc;

		if ((retval = nc_inq_varndims(ncid, tmp_var.varid, &(tmp_var.ndims)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_READ_ERROR, nc_strerror(retval));
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (tmp_var.ndims != number_of_dimensions) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "NetCDF dimension number doesn't match with specified datacube dimension number\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_INPUT_DIMENSION_MISMATCH, "number");
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		tmp_var.dims_id = malloc(tmp_var.ndims * sizeof(int));
		if (!(tmp_var.dims_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "NetCDF dimension number doesn't match with specified datacube dimension number\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_INPUT_DIMENSION_MISMATCH, "number");
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if ((retval = nc_inq_vardimid(ncid, tmp_var.varid, tmp_var.dims_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "NetCDF dimension number doesn't match with specified datacube dimension number\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_INPUT_DIMENSION_MISMATCH, "number");
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		//Reorder measure_stream to match nc file order
		int j, p;
		int *swap_value = (int *) malloc((3 * ndim) * sizeof(int));
		if (!swap_value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "measure structure swap");
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		for (j = 0; j < number_of_dimensions; j++) {
			for (p = 0; p < number_of_dimensions; p++) {
				if (tmp_dims_id[j] == tmp_var.dims_id[p]) {
					swap_value[p] = (int) measure_stream[3 + j];
					swap_value[p + number_of_dimensions] = (int) measure_stream[3 + j + number_of_dimensions];
					swap_value[p + 2 * number_of_dimensions] = (int) measure_stream[3 + j + 2 * number_of_dimensions];
				}
			}
		}
		for (j = 0; j < number_of_dimensions; j++) {
			measure_stream[3 + j] = (int) swap_value[j];
			measure_stream[3 + j + number_of_dimensions] = (int) swap_value[j + number_of_dimensions];
			measure_stream[3 + j + 2 * number_of_dimensions] = (int) swap_value[j + 2 * number_of_dimensions];
		}
		free(swap_value);
		free(tmp_var.dims_id);
		tmp_var.dims_id = NULL;
		free(tmp_dims_id);
		tmp_dims_id = NULL;
		//INSERT of implicit dimensions

		int dimension_array_id = 0;
		imp_dim_count = 0;

		for (l = 0; l < number_of_dimensions; l++) {
			if (!cubedims[l].explicit_dim) {
				dim_inst[l].fk_id_dimension_index = 0;
				dim_inst[l].fk_id_dimension_label = 0;
				dim_inst[l].id_grid = 0;
				dim_inst[l].id_dimensioninst = 0;
				dim_inst[l].size += tmp_var.varsize;
				if (oph_dim_insert_into_dimension_table
				    (db_dimension, label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, dim_rows[imp_dim_count], &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_ROW_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].fk_id_dimension_label = dimension_array_id;	// Real dimension

				if (oph_dim_insert_into_dimension_table
				    (db_dimension, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_rows_index[imp_dim_count], &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIM_ROW_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].fk_id_dimension_index = dimension_array_id;	// Indexes

				if (new_grid || !((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name)
					dim_inst[l].id_grid = id_grid;
				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIMINST_INSERT_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				cubedims[l].id_dimensioninst = dimension_array_id;
				cubedims[l].size = dim_inst[l].size;
				imp_dim_count++;
			}
			if (new_grid || !((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name) {
				dim_inst[l].id_grid = id_grid;
				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_DIMINST_INSERT_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				cubedims[l].id_dimensioninst = dimension_array_id;
			}

		}

		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		for (k = 0; k < imp_ndim; k++) {
			if (dim_rows_index[k])
				free(dim_rows_index[k]);
			dim_rows_index[k] = NULL;
		}
		free(dim_rows_index);
		dim_rows_index = NULL;
		for (k = 0; k < imp_ndim; k++) {
			if (dim_rows[k])
				free(dim_rows[k]);
			dim_rows[k] = NULL;
		}
		free(dim_rows);
		dim_rows = NULL;
		free(dim);
		dim = NULL;
		free(dim_inst);
		dim_inst = NULL;
		free(hier);
		hier = NULL;
	  /********************************
	   * INPUT PARAMETERS CHECK - END *
	   ********************************/

	  /********************************
	   *  DATACUBE CREATION - BEGIN   *
	   ********************************/
		//Import source name
		oph_odb_source src;
		int id_src = 0;
		strncpy(src.uri, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig, OPH_ODB_CUBE_SOURCE_URI_SIZE);
		if (oph_odb_cube_insert_into_source_table(oDB, &src, &id_src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert source URI\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATNC_INSERT_SOURCE_URI_ERROR, src.uri);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}

		cube.id_source = id_src;
		cube.level++;
		if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;
		//Copy fragment id relative index set 
		if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "fragment ids");
			goto __OPH_EXIT_1;
		}
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_DATACUBE_INSERT_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_CUBEHASDIM_INSERT_ERROR);
				goto __OPH_EXIT_1;
			}
		}

		free(cubedims);
		cubedims = NULL;

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		last_insertd_id = 0;
		//Insert new task REMEBER TO MEMSET query to 0 IF NOT NEEDED
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_job;
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_datacube;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_TASK_INSERT_ERROR,
				new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_CONCATNC_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		memcpy(id_string[3], &number_of_dimensions, sizeof(int));

	  /********************************
	   *   DATACUBE CREATION - END    *
	   ********************************/
	      __OPH_EXIT_1:
		//Free init global resources
		if (cubedims)
			free(cubedims);
		if (tmp_dims_id)
			free(tmp_dims_id);
		if (dim)
			free(dim);
		if (dim_inst)
			free(dim_inst);
		if (hier)
			free(hier);
		if (dim_rows_index) {
			for (k = 0; k < imp_ndim; k++) {
				if (dim_rows_index[k])
					free(dim_rows_index[k]);
				dim_rows_index[k] = NULL;
			}
			free(dim_rows_index);
		}
		if (dim_rows) {
			for (k = 0; k < imp_ndim; k++) {
				if (dim_rows[k])
					free(dim_rows[k]);
				dim_rows[k] = NULL;
			}
			free(dim_rows);
		}
		if (tmp_var.dims_id)
			free(tmp_var.dims_id);
		if (tmp_var.dims_length)
			free(tmp_var.dims_length);

	}
	//Broadcast to all other processes the result
	//MPI_Barrier(MPI_COMM_WORLD);
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATNC_MASTER_TASK_INIT_FAILED_NO_CONTAINER, "NO-CONTAINER");
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "fragment ids");
			((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;
			if (measure_stream)
				free(measure_stream);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		ndim = *((int *) id_string[3]);
		measure_stream = (int *) malloc((3 + 3 * ndim) * sizeof(int));
	}

	MPI_Bcast(measure_stream, 3 + 3 * ndim, MPI_INT, 0, MPI_COMM_WORLD);

	// Previous data are not considered
	if (measure->dims_length)
		free(measure->dims_length);
	if (measure->dims_type)
		free(measure->dims_type);
	if (measure->dims_oph_level)
		free(measure->dims_oph_level);
	measure->dims_length = (size_t *) malloc(ndim * sizeof(size_t));
	if (!measure->dims_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dim_length");
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	measure->dims_type = (short int *) malloc(ndim * sizeof(short int));
	if (!measure->dims_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "dims_type");
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	measure->dims_oph_level = (short int *) malloc(ndim * sizeof(short int));
	if (!measure->dims_oph_level) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "oph_level");
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int l;
	for (l = 0; l < ndim; l++) {
		measure->dims_length[l] = (size_t) measure_stream[3 + ndim + l];
		measure->dims_type[l] = (short int) measure_stream[3 + 2 * ndim + l];
		measure->dims_oph_level[l] = (short int) measure_stream[3 + l];
	}
	measure->varid = measure_stream[0];
	measure->vartype = measure_stream[1];
	measure->nexp = measure_stream[2];
	measure->ndims = ndim;

	if (measure_stream)
		free(measure_stream);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_CONCATNC_operator_handle *oper_handle = (OPH_CONCATNC_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int i, j, k;

	int id_datacube_out = oper_handle->id_output_datacube;
	int id_datacube_in = oper_handle->id_input_datacube;

	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in, oper_handle->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
	int result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0;
	oph_odb_fragment tmp_frag;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];

	if (oph_dc_setup_dbms(&(oper_handle->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For each DBMS
	for (i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {

		if (oph_dc_connect_to_dbms(oper_handle->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(oper_handle->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_DB_SELECTION_ERROR, (dbs.value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = 0; (k < frags.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;
				tmp_frag.db_instance = frags.value[k].db_instance;
				tmp_frag.frag_relative_index = frags.value[k].frag_relative_index;
				tmp_frag.id_datacube = id_datacube_out;
				tmp_frag.id_db = frags.value[k].id_db;
				tmp_frag.key_end = frags.value[k].key_end;
				tmp_frag.key_start = frags.value[k].key_start;

				//Connection string
				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &fragment_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_STRING_BUFFER_OVERFLOW, "fragment name", fragment_name);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				strcpy(tmp_frag.fragment_name, fragment_name);
				//Create Empty fragment
				if (oph_dc_create_empty_fragment(oper_handle->server, &tmp_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_FRAGMENT_CREATION_ERROR, tmp_frag.fragment_name);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Append fragment
				if (oph_nc_append_fragment_from_nc2
				    (oper_handle->server, &(frags.value[k]), &tmp_frag, oper_handle->ncid, oper_handle->compressed, (NETCDF_var *) & (oper_handle->measure),
				     oper_handle->memory_size)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_FRAG_POPULATE_ERROR, tmp_frag.fragment_name, "");
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &tmp_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_FRAGMENT_INSERT_ERROR, frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(oper_handle->server, &(dbmss.value[i]));
	}
	if (oph_dc_cleanup_dbms(oper_handle->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATNC_IOPLUGIN_CLEANUP_ERROR, (dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	if (result == OPH_ANALYTICS_OPERATOR_SUCCESS)
		oper_handle->execute_error = 0;

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process prints output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATNC_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container,
			 ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_CONCATNC)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CONCATNC, "Output Cube", jsonbuf)) {
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
		if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container,
						   ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids, 0, 0, 1))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
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
			oph_dproc_clean_odb(&((OPH_CONCATNC_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

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
		oph_odb_free_ophidiadb(&((OPH_CONCATNC_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}


	NETCDF_var *measure = ((NETCDF_var *) & (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->measure));

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

	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->user);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	}
	if ((retval = nc_close(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->ncid)))
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %s\n", nc_strerror(retval));
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset) {
		free((char *) ((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset);
		((OPH_CONCATNC_operator_handle *) handle->operator_handle)->dim_offset = NULL;
	}

	free((OPH_CONCATNC_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
