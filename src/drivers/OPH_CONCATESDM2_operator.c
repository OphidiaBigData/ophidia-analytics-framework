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
#include "drivers/OPH_CONCATESDM2_operator.h"

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

	if (!(handle->operator_handle = (OPH_CONCATESDM2_operator_handle *) calloc(1, sizeof(OPH_CONCATESDM2_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->check_exp_dim = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->memory_size = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset = NULL;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue = 0;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 0;

	ESDM_var *nc_measure = &(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure);
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
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int stream_size = OPH_ODB_CUBE_MEASURE_SIZE + 2 * sizeof(int) + 1;
	char stream[stream_size];
	memset(stream, 0, sizeof(stream));
	int id_in_datacube = 0, id_in_container = 0;

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nthread = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	while (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_OPHIDIADB_CONFIGURATION_FILE);
			break;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_PID_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_in_container, id_in_datacube, &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_NO_INPUT_DATACUBE, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_in_datacube, 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_in_container, &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_DATACUBE_FOLDER_ERROR, datacube_in);
			id_in_datacube = id_in_container = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_DATACUBE_PERMISSION_ERROR, username);
			id_in_datacube = id_in_container = 0;
		}
		memcpy(stream, (char *) &id_in_datacube, sizeof(id_in_datacube));
		memcpy(stream + sizeof(int), (char *) &id_in_container, sizeof(id_in_container));

		if (uri)
			free(uri);
		uri = NULL;

		if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_user))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_USER_ID_ERROR);
			break;
		}
		//Get id from measure name
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, id_in_datacube, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_DATACUBE_READ_ERROR);
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
		logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube = id_in_datacube;

	if (id_in_container == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_in_container, OPH_LOG_OPH_CONCATESDM_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container = id_in_container;

	ESDM_var *measure = ((ESDM_var *) & (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure));
	strncpy(measure->varname, var_name, OPH_ODB_DIM_DIMENSION_SIZE);
	measure->varname[OPH_ODB_DIM_DIMENSION_SIZE] = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *input = hashtbl_get(task_tbl, OPH_IN_PARAM_INPUT);
	if (input && strlen(input))
		value = input;
	else if (!strlen(value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The param '%s' is mandatory; at least the param '%s' needs to be set\n", OPH_IN_PARAM_SRC_FILE_PATH, OPH_IN_PARAM_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig = strdup(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (strstr(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path, OPH_ESDM_PREFIX, 7)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong ESDM object\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong ESDM object\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *tmp = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path;
	value = strstr(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path, "//") + 2;
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path = strdup(value);
	free(tmp);

	if (oph_pid_get_memory_size(&(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->memory_size))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_OPHIDIADB_CONFIGURATION_FILE, "NO-CONTAINER");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	int i;

	//Open netcdf file
	int j = 0;
	esdm_status ret = esdm_init();
	if (ret) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ESDM cannot be initialized\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if ((ret = esdm_container_open(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path, ESDM_MODE_FLAG_READ, &measure->container))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM object '%s': %s\n", ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path, measure->varname);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_NC_OPEN_ERROR_NO_CONTAINER, "NO-CONTAINER", "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Extract measured variable information
	if ((ret = esdm_dataset_open(measure->container, measure->varname, ESDM_MODE_FLAG_READ, &measure->dataset))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	esdm_dataset_t *dataset = measure->dataset;

	if ((ret = esdm_dataset_get_dataspace(dataset, &measure->dspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	esdm_dataspace_t *dspace = measure->dspace;

	if (oph_esdm_set_esdm_type(measure->vartype, dspace->type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_NC_INC_VAR_ERROR_NO_CONTAINER, "NO-CONTAINER", "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ndims = measure->ndims = dspace->dims;
/*
	if (!(measure->dims_name = (char **) calloc(measure->ndims, sizeof(char *)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
*/
	ret = esdm_dataset_get_name_dims(dataset, &measure->dims_name);
	if (!(measure->dims_length = (size_t *) calloc(measure->ndims, sizeof(size_t)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_length");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_unlim = (char *) calloc(measure->ndims, sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_unlim");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_type = (short int *) calloc(measure->ndims, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_oph_level");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_concept_level = (char *) calloc(measure->ndims, sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_concept_level");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(measure->dims_id = (int *) calloc(measure->ndims, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_id");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int64_t const *size = esdm_dataset_get_actual_size(dataset);
	measure->dim_dataset = (esdm_dataset_t **) calloc(measure->ndims, sizeof(esdm_dataset_t *));
	measure->dim_dspace = (esdm_dataspace_t **) calloc(measure->ndims, sizeof(esdm_dataspace_t *));

	//Extract dimensions information and check names provided by task string
	char *dimname;
	for (i = 0; i < ndims; i++) {
		measure->dims_unlim[i] = !dspace->size[i];
		measure->dims_length[i] = dspace->size[i] ? dspace->size[i] : size[i];
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
	for (; i < measure->ndims; i++) {
		measure->dims_oph_level[i] = level++;
		measure->dims_type[i] = 0;
	}

//ADDED TO MANAGE SUBSETTED IMPORT

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = NULL;
	int s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	double *offset = NULL;
	if (s_offset_num > 0) {
		offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "offset");
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	int is_index = strncmp(value, OPH_CONCATESDM_SUBSET_COORD, OPH_TP_TASKLEN);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char issubset = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char isfilter = 0;
	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		isfilter = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, "NO-CONTAINER", dimname, measure->varname);
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
		if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->time_filter && strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR[1])) {
			if (tf >= 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not more than one time dimension can be considered\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
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
				ophidiadb *oDB = &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->oDB;

				if (oph_odb_dim_retrieve_dimension_list_from_container
				    (oDB, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, &tot_dims, &number_of_dimensions_c)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions .\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_CONCATESDM_DIMENSION_READ_ERROR);
					break;
				}

				for (i = 0; i < number_of_dimensions_c; ++i)
					if (!strncmp(tot_dims[i].dimension_name, measure->dims_name[j], OPH_ODB_DIM_DIMENSION_SIZE))
						break;
				if (i >= number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found\n", measure->dims_name[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_DIM_CONT_ERROR,
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_start_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_NO_CONTAINER, "NO-CONTAINER", "measure dims_end_index");
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
		} else if ((ii = oph_esdm_check_subset_string(curfilter, i, measure, is_index, j < s_offset_num ? offset[j] : 0.0, 0))) {
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return ii;
		} else if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
			   || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_INVALID_INPUT_STRING);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_EXP_DIM_LEVEL_ERROR, "NO-CONTAINER");
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_CHECK_EXP_DIM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->check_exp_dim = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, "NO-CONTAINER", "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DIM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE) != 0) {
		if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset = (double *) malloc(sizeof(double)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, "NO-CONTAINER", OPH_IN_PARAM_DIM_OFFSET);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		*((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset = (double) strtod(value, NULL);
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIM_CONTINUE);
	if (value && !strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue = 1;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_NULL_OPERATOR_HANDLE,
			((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container);
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

	esdm_status ret;

	//Access data in the netcdf file

	ESDM_var *measure = &(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure);
	int ndim = 0;
	int *measure_stream = NULL;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->oDB;

		int id_container_in = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container;
		int last_insertd_id = 0;
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
		ESDM_var tmp_var;
		tmp_var.dims_id = NULL;
		tmp_var.dims_length = NULL;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_CUBEHASDIM_READ_ERROR);
			goto __OPH_EXIT_1;
		}

		if (measure->ndims != number_of_dimensions) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "NetCDF dimension number doesn't match with specified datacube dimension number\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_INPUT_DIMENSION_MISMATCH, "number");
			goto __OPH_EXIT_1;
		}

		ndim = number_of_dimensions;
		measure_stream = (int *) malloc((3 + 3 * ndim) * sizeof(int));
		if (!measure_stream) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "measure structure");
			goto __OPH_EXIT_1;
		}
		tmp_dims_id = (int *) malloc(number_of_dimensions * sizeof(int));
		if (!tmp_dims_id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "measure structure");
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_USE_DB);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		int l, k;

		// Grid management
		int id_grid = 0, new_grid = 0, grid_exist = 0;
		if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name) {
			if (oph_odb_dim_retrieve_grid_id
			    (oDB, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container,
			     &id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading grid id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_GRID_LOAD_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}

			if (id_grid) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use an already defined grid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_OLD_GRID_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			} else {
				new_grid = 1;
				oph_odb_dimension_grid grid;
				strncpy(grid.grid_name, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				if (oph_odb_dim_insert_into_grid_table(oDB, &grid, &id_grid, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while storing grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_CONCATESDM_GRID_STORE_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container,
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimension structures");
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		//Get all dimension related information
		for (l = 0; l < number_of_dimensions; l++) {
			if (oph_odb_dim_retrieve_full_dimension_info
			    (oDB, cubedims[l].id_dimensioninst, &(dim[l]), &(dim_inst[l]), &(dim_grid), &(hier[l]), ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube)) {
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
			tmp_dims_id[l] = i;	// TODO: to be checked

			tmp_var.varsize = 1 + measure->dims_end_index[i] - measure->dims_start_index[i];

			if (cubedims[l].explicit_dim) {
				if (tmp_var.varsize != cubedims[l].size) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of dimension '%s' in NC file (%d) does not correspond to the one stored in OphidiaDB (%d)\n", dim[l].dimension_name,
					      tmp_var.varsize, cubedims[l].size);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_SIZE_MISMATCH, dim[l].dimension_name);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
			} else
				measure_stream[number_of_dimensions + l] = tmp_var.varsize;

			if (!measure->dim_dataset[i])
				if ((ret = esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}

			if (!measure->dim_dspace[i])
				if ((ret = esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}

			if (oph_esdm_compare_types(id_container_in, measure->dim_dspace[i]->type, dim[l].dimension_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension type in NC file does not correspond to the one stored in OphidiaDB\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_TYPE_MISMATCH_ERROR, dim[l].dimension_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				goto __OPH_EXIT_1;
			}
			//Modified to allow subsetting
			tmp_var.dims_start_index = &(measure->dims_start_index[i]);
			tmp_var.dims_end_index = &(measure->dims_end_index[i]);

			//If dimension is implicit then retrieve the values
			if (!cubedims[l].explicit_dim && (new_grid || !((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name)) {
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
				if (oph_esdm_get_dim_array
				    (id_container_in, measure->dim_dataset[i], l, dim[l].dimension_type, tmp_var.varsize, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index), &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_READ_ERROR, "");
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					int i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(int *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(int));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (int) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					long long i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(long long *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(long long));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (long long) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					short i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(short *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(short));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (short) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					char i_tmp_start = 0, i_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						i_tmp_start = *(char *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(char));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							i_tmp_start += (char) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					float f_tmp_start = 0, f_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						f_tmp_start = *(float *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(float));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							f_tmp_start += (float) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dim_row);
						free(dim_row_index);
						free(dim_array);
						goto __OPH_EXIT_1;
					}
					double d_tmp_start = 0, d_tmp;
					if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_continue) {
						d_tmp_start = *(double *) (dim_rows[imp_dim_count] + (dim_inst[l].size - 1) * sizeof(double));
						if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset)
							d_tmp_start += (double) *((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset;
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
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dimensions binary array");
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

			} else if ((!new_grid && ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name)
				   || (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->check_exp_dim && cubedims[l].explicit_dim)) {
				// If old grid is given on dimension is explicit and check_exp_dim is set then compare the values
				char *dim_array = NULL;

				if (oph_esdm_get_dim_array
				    (id_container_in, measure->dim_dataset[i], l, dim[l].dimension_type, dim_inst[l].size, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index), &dim_array)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_READ_ERROR, "");
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				if (oph_dim_compare_dimension(db_dimension, label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, dim_array, dim_inst[l].fk_id_dimension_label, &match)
				    || match) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n", dim[l].dimension_name);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_INPUT_DIMENSION_MISMATCH, dim[l].dimension_name);
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
		if (oph_odb_cube_retrieve_datacube(oDB, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_DATACUBE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		//Reorder measure_stream to match nc file order
		int j;
		int *swap_value = (int *) malloc((3 * ndim) * sizeof(int));
		if (!swap_value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "measure structure swap");
			oph_odb_cube_free_datacube(&cube);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		for (j = 0; j < number_of_dimensions; j++) {
			swap_value[tmp_dims_id[j]] = (int) measure_stream[j];
			swap_value[tmp_dims_id[j] + number_of_dimensions] = (int) measure_stream[j + number_of_dimensions];
			swap_value[tmp_dims_id[j] + 2 * number_of_dimensions] = (int) measure_stream[j + 2 * number_of_dimensions];
		}
		for (j = 0; j < number_of_dimensions; j++) {
			measure_stream[j] = (int) swap_value[j];
			measure_stream[j + number_of_dimensions] = (int) swap_value[j + number_of_dimensions];
			measure_stream[j + 2 * number_of_dimensions] = (int) swap_value[j + 2 * number_of_dimensions];
		}
		free(swap_value);
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
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_ROW_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].fk_id_dimension_label = dimension_array_id;	// Real dimension

				if (oph_dim_insert_into_dimension_table
				    (db_dimension, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_rows_index[imp_dim_count], &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIM_ROW_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].fk_id_dimension_index = dimension_array_id;	// Indexes

				if (new_grid || !((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name)
					dim_inst[l].id_grid = id_grid;
				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIMINST_INSERT_ERROR, dim[l].dimension_name);
					oph_odb_cube_free_datacube(&cube);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					goto __OPH_EXIT_1;
				}
				cubedims[l].id_dimensioninst = dimension_array_id;
				cubedims[l].size = dim_inst[l].size;
				imp_dim_count++;
			}
			if (new_grid || !((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name) {
				dim_inst[l].id_grid = id_grid;
				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_DIMINST_INSERT_ERROR, dim[l].dimension_name);
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
		strncpy(src.uri, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig, OPH_ODB_CUBE_SOURCE_URI_SIZE);
		if (oph_odb_cube_insert_into_source_table(oDB, &src, &id_src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert source URI\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CONCATESDM_INSERT_SOURCE_URI_ERROR, src.uri);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}

		cube.id_source = id_src;
		cube.level++;
		if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;
		//Copy fragment id relative index set 
		if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT,
				"fragment ids");
			goto __OPH_EXIT_1;
		}
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		int tot_frag_num = 0;
		if (oph_ids_count_number_of_ids(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids, &tot_frag_num)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_RETREIVE_IDS_ERROR);
			goto __OPH_EXIT_1;
		}
		//Check that product of ncores and nthread is at most equal to total number of fragments        
		if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > tot_frag_num) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
		}
		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_DATACUBE_INSERT_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_CONCATESDM_CUBEHASDIM_INSERT_ERROR);
				goto __OPH_EXIT_1;
			}
		}

		free(cubedims);
		cubedims = NULL;

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		last_insertd_id = 0;
		//Insert new task REMEBER TO MEMSET query to 0 IF NOT NEEDED
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_job;
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_datacube;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_TASK_INSERT_ERROR,
				new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CONCATESDM_MASTER_TASK_INIT_FAILED_NO_CONTAINER, "NO-CONTAINER");
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT,
				"fragment ids");
			((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;
			if (measure_stream)
				free(measure_stream);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		ndim = *((int *) id_string[3]);
		measure_stream = (int *) malloc((3 * ndim) * sizeof(int));
	}

	MPI_Bcast(measure_stream, 3 * ndim, MPI_INT, 0, MPI_COMM_WORLD);

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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dim_length");
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	measure->dims_type = (short int *) malloc(ndim * sizeof(short int));
	if (!measure->dims_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "dims_type");
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	measure->dims_oph_level = (short int *) malloc(ndim * sizeof(short int));
	if (!measure->dims_oph_level) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "oph_level");
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;
		if (measure_stream)
			free(measure_stream);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int l;
	for (l = 0; l < ndim; l++) {
		measure->dims_oph_level[l] = (short int) measure_stream[l];
		measure->dims_length[l] = (size_t) measure_stream[ndim + l];
		measure->dims_type[l] = (short int) measure_stream[2 * ndim + l];
	}

	if (measure_stream)
		free(measure_stream);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_CONCATESDM2_operator_handle *oper_handle = (OPH_CONCATESDM2_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int l;

	int num_threads = (oper_handle->nthread <= oper_handle->fragment_number ? oper_handle->nthread : oper_handle->fragment_number);
	int res[num_threads];

	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_OPHIDIADB_CONFIGURATION_FILE);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, oper_handle->id_input_datacube, oper_handle->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Only Ophidia IO server can be used
	if (strcasecmp((dbmss.value[0]).io_server_type, OPH_IOSERVER_OPHIDIAIO_TYPE) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_CONCATESDM_IOSERVER_ERROR, (dbmss.value[0]).io_server_type);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_IOSERVER_ERROR,
			(dbmss.value[0]).io_server_type);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	struct _thread_struct {
		OPH_CONCATESDM2_operator_handle *oper_handle;
		unsigned int current_thread;
		unsigned int total_threads;
		int proc_rank;
		oph_odb_fragment_list *frags;
		oph_odb_db_instance_list *dbs;
		oph_odb_dbms_instance_list *dbmss;
	};
	typedef struct _thread_struct thread_struct;

	void *exec_thread(void *ts) {

		OPH_CONCATESDM2_operator_handle *oper_handle = ((thread_struct *) ts)->oper_handle;
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

		char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
		oph_odb_fragment tmp_frag;
		int frag_count = 0;

		oph_ioserver_handler *server = NULL;
		if (oph_dc_setup_dbms_thread(&(server), (dbmss->value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_IOPLUGIN_SETUP_ERROR, (dbmss->value[0]).id_dbms);
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
		for (i = first_dbms; (i < dbmss->size) && (frag_count < fragxthread) && (res == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {


			if (oph_dc_connect_to_dbms(server, &(dbmss->value[i]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_DB_SELECTION_ERROR, (dbs->value[i]).db_name);
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = first_frag; (k < frags->size) && (frag_count < fragxthread) && (res == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags->value[k].db_instance != &(dbs->value[i]))
					continue;

				tmp_frag.db_instance = frags->value[k].db_instance;
				tmp_frag.frag_relative_index = frags->value[k].frag_relative_index;
				tmp_frag.id_datacube = id_datacube_out;
				tmp_frag.id_db = frags->value[k].id_db;
				tmp_frag.key_end = frags->value[k].key_end;
				tmp_frag.key_start = frags->value[k].key_start;

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, proc_rank, (current_frag_count + frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				strcpy(tmp_frag.fragment_name, frag_name_out);

				//Append fragment
				if (oph_nc_append_fragment_from_nc4
				    (server, &(frags->value[k]), &tmp_frag, oper_handle->nc_file_path, (tmp_frag.key_end - tmp_frag.key_start + 1), compressed,
				     (ESDM_var *) & (oper_handle->measure))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_FRAG_POPULATE_ERROR, tmp_frag.fragment_name, "");
					res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Change fragment fields
				frags->value[k].id_datacube = tmp_frag.id_datacube;
				strncpy(frags->value[k].fragment_name, tmp_frag.fragment_name, OPH_ODB_STGE_FRAG_NAME_SIZE);
				frags->value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;

				frag_count++;
			}

			oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));

			if (res != OPH_ANALYTICS_OPERATOR_SUCCESS) {
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
			}
			//Update fragment counter
			first_frag = current_frag_count + frag_count;
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
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_free_ophidiadb_thread(&oDB_slave);
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CONCATESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_CONCATESDM2_operator_handle *oper_handle = (OPH_CONCATESDM2_operator_handle *) handle->operator_handle;

	short int proc_error = oper_handle->execute_error;
	int id_datacube = oper_handle->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process prints output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid(oper_handle->id_input_container, oper_handle->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_CONCATESDM_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, oper_handle->id_input_container, oper_handle->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_CONCATESDM2)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_CONCATESDM2, "Output Cube", jsonbuf)) {
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
		int num_threads = (oper_handle->nthread <= oper_handle->fragment_number ? oper_handle->nthread : oper_handle->fragment_number);

		//Delete fragments
		if (oper_handle->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, oper_handle->id_input_container, oper_handle->fragment_ids, 0, 0, num_threads))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
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
			oph_dproc_clean_odb(&oper_handle->oDB, id_datacube, oper_handle->id_input_container);
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

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_free_ophidiadb(&((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}


	ESDM_var *measure = ((ESDM_var *) & (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure));
/*
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
*/
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

	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->user);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset) {
		free((char *) ((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset);
		((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->dim_offset = NULL;
	}

	esdm_dataset_close(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure.dataset);
	esdm_container_close(((OPH_CONCATESDM2_operator_handle *) handle->operator_handle)->measure.container);

	esdm_finalize();

	free((OPH_CONCATESDM2_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

#ifdef OPH_ESDM
	handle->dlh = NULL;
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
