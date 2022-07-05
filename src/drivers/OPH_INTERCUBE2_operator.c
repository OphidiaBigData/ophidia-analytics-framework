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

#include "drivers/OPH_INTERCUBE2_operator.h"
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

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"
#include "oph_driver_procedure_library.h"

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

	if (!(handle->operator_handle = (OPH_INTERCUBE2_operator_handle *) calloc(1, sizeof(OPH_INTERCUBE2_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = NAN;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->user_missing_value = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes = NULL;
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num = 0;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys, &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MEASURE_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT, "output measure");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_REDUCTION_OPERATION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_REDUCTION_OPERATION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_REDUCTION_OPERATION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT, "operation");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MISSINGVALUE);
	if (value && strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (strncmp(value, OPH_COMMON_NAN, OPH_TP_TASKLEN))
			((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = strtod(value, NULL);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->user_missing_value = 1;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_MULTI_INPUT);
	if (!value || !strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_MULTI_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_MULTI_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (oph_tp_parse_multiple_value_param(value, &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes, &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (!((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The number of datacubes is not correct: at least a PID is expected\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The number of datacubes is not correct: at least a PID is expected\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char **cubes = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes;
	int cubes_num = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num;

	if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube = (int *) malloc(cubes_num * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT, "id_input_datacube");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//For error checking
	int i, id_datacube_in[cubes_num + 2];
	for (i = 0; i < cubes_num + 2; i++);
	id_datacube_in[i] = 0;

	value = hashtbl_get(task_tbl, OPH_ARG_USERID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MISSING_INPUT_PARAMETER, OPH_ARG_USERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_user = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCUBE_OPHIDIADB_CONNECTION_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		int container2 = 0;
		for (i = 0; i < cubes_num; i++) {

			if (oph_pid_parse_pid(cubes[i], i ? &container2 : id_datacube_in + cubes_num, id_datacube_in + i, &uri)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_PID_ERROR, cubes[i]);
				id_datacube_in[i] = 0;
				id_datacube_in[cubes_num] = 0;
			} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, i ? container2 : id_datacube_in[cubes_num], id_datacube_in[i], &exists)) || !exists) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_NO_INPUT_DATACUBE, cubes[i]);
				id_datacube_in[i] = 0;
				id_datacube_in[cubes_num] = 0;
			} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[i], 0, &status)) || !status) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, cubes[i]);
				id_datacube_in[i] = 0;
				id_datacube_in[cubes_num] = 0;
			}
			if (uri)
				free(uri);
			uri = NULL;
			if (id_datacube_in[i]) {
				if ((oph_odb_fs_retrive_container_folder_id(oDB, i ? container2 : id_datacube_in[cubes_num], &folder_id))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_DATACUBE_FOLDER_ERROR, cubes[i]);
					id_datacube_in[i] = 0;
					id_datacube_in[cubes_num] = 0;
				} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
					//Check if user can work on datacube
					pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_DATACUBE_PERMISSION_ERROR, username);
					id_datacube_in[i] = 0;
					id_datacube_in[cubes_num] = 0;
				}
			}
		}

		id_datacube_in[cubes_num + 1] = id_datacube_in[cubes_num];
		if (id_datacube_in[cubes_num]) {
			value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
				if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, id_datacube_in + cubes_num + 1)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value);
					id_datacube_in[cubes_num] = 0;
				}
			}
		}
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, cubes_num + 2, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	for (i = 0; i < cubes_num; ++i) {
		if (!id_datacube_in[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_NO_INPUT_DATACUBE, cubes[i]);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube[i] = id_datacube_in[i];
	}

	if (!id_datacube_in[cubes_num]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[cubes_num], OPH_LOG_OPH_INTERCUBE_NO_INPUT_CONTAINER, cubes[0]);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[cubes_num];
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[cubes_num + 1];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCUBE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT, "description");
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, stream_max_size = 4 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 2 * sizeof(int) + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[3], *data_type;
	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	data_type = stream + pointer;

	if (handle->proc_rank == 0) {

		ophidiadb *oDB = &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube, cube2;

		int i, cubes_num = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num;
		int *datacube_id = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube;

		oph_odb_cube_init_datacube(&cube);
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id[0], &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube %s\n", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_DATACUBE_READ_ERROR, "first");
			goto __OPH_EXIT_1;
		}

		for (i = 1; i < cubes_num; i++) {
			oph_odb_cube_init_datacube(&cube2);
			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, datacube_id[i], &cube2)) {
				oph_odb_cube_free_datacube(&cube);
				oph_odb_cube_free_datacube(&cube2);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube %s\n", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_DATACUBE_READ_ERROR,
					"first");
				goto __OPH_EXIT_1;
			}
			// Checking fragmentation structure
			if ((cube.hostxdatacube != cube2.hostxdatacube) || (cube.fragmentxdb != cube2.fragmentxdb) || (cube.tuplexfragment != cube2.tuplexfragment)) {
				oph_odb_cube_free_datacube(&cube);
				oph_odb_cube_free_datacube(&cube2);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube fragmentation structures are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "fragmentation structures");
				goto __OPH_EXIT_1;
			}
			// Checking fragment indexes
			if (strcmp(cube.frag_relative_index_set, cube2.frag_relative_index_set)) {
				oph_odb_cube_free_datacube(&cube);
				oph_odb_cube_free_datacube(&cube2);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube relative index sets are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "relative index sets");
				goto __OPH_EXIT_1;
			}
			// Checking data types
			if (strcasecmp(cube.measure_type, cube2.measure_type)) {
				oph_odb_cube_free_datacube(&cube);
				oph_odb_cube_free_datacube(&cube2);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube types are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "measure types");
				goto __OPH_EXIT_1;
			}
			if (cube.compressed != cube2.compressed) {
				oph_odb_cube_free_datacube(&cube);
				oph_odb_cube_free_datacube(&cube2);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube are compressed differently\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "compression methods");
				goto __OPH_EXIT_1;
			}
			oph_odb_cube_free_datacube(&cube2);
		}

		// Change the container id
		cube.id_container = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container;

		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		//Copy fragment id relative index set 
		if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT,
				"fragment ids");
			goto __OPH_EXIT_1;
		}
		//Copy measure_type relative index set
		if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT,
				"measure type");
			goto __OPH_EXIT_1;
		}

		oph_odb_cubehasdim *cubedims = NULL, *cubedims2 = NULL;
		int number_of_dimensions = 0, number_of_dimensions2 = 0;
		int last_insertd_id = 0;
		int l, ll;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id[0], &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube1 - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_CUBEHASDIM_READ_ERROR, "first");
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}

		for (i = 1; i < cubes_num; i++) {

			if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id[i], &cubedims2, &number_of_dimensions2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube2 - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_CUBEHASDIM_READ_ERROR,
					"second");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				free(cubedims2);
				goto __OPH_EXIT_1;
			}
			// Dimension comparison
			for (l = ll = 0; (l < number_of_dimensions) && (ll < number_of_dimensions2); l++, ll++) {
				while (!cubedims[l].size && (l < number_of_dimensions))
					l++;
				while (!cubedims2[ll].size && (ll < number_of_dimensions2))
					ll++;
				if ((l >= number_of_dimensions) || (ll >= number_of_dimensions2) || (cubedims[l].size != cubedims2[ll].size) || (cubedims[l].explicit_dim != cubedims2[ll].explicit_dim)
				    || (cubedims[l].level != cubedims2[ll].level))
					break;
			}
			for (; l < number_of_dimensions; l++)
				if (cubedims[l].size)
					break;
			for (; ll < number_of_dimensions2; ll++)
				if (cubedims2[ll].size)
					break;
			if ((l < number_of_dimensions) || (ll < number_of_dimensions2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube dimensions are not comparable.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "dimensions");
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				free(cubedims2);
				goto __OPH_EXIT_1;
			}
			free(cubedims2);
		}

		// If given, change the measure name
		char *old_measure = NULL;
		if (strncasecmp(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure, OPH_COMMON_NULL_VALUE, OPH_TP_TASKLEN)) {
			old_measure = strdup(cube.measure);
			snprintf(cube.measure, OPH_ODB_CUBE_MEASURE_SIZE, "%s", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure);
		}
		//New fields
		cube.id_source = 0;
		cube.level++;
		if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (old_measure)
				free(old_measure);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		// Copy the dimension in case of output has to be saved in a new container or the grid has been changed
		if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container != ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container) {
			oph_odb_dimension dim[number_of_dimensions];
			oph_odb_dimension_instance dim_inst[number_of_dimensions];
			for (l = 0; l < number_of_dimensions; ++l) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id[0])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					if (old_measure)
						free(old_measure);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id[0])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					if (old_measure)
						free(old_measure);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_LOAD_ERROR);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				if (old_measure)
					free(old_measure);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_CONNECT_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				if (old_measure)
					free(old_measure);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_USE_DB_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				if (old_measure)
					free(old_measure);
				goto __OPH_EXIT_1;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container);
			char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(o_index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container);
			snprintf(o_label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container);

			char *dim_row, *cl_value;
			int compressed = 0;
			for (l = 0; l < number_of_dimensions; ++l) {
				if (!dim_inst[l].fk_id_dimension_index) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
					break;
				}

				dim_row = 0;
				if (dim_inst[l].size) {
					// Load input labels
					dim_row = NULL;
					if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, compressed, &dim_row) || !dim_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_GENERIC_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (old_measure)
							free(old_measure);
						goto __OPH_EXIT_1;
					}
					if (dim_inst[l].fk_id_dimension_label) {
						if (oph_dim_read_dimension_filtered_data
						    (db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, MYSQL_DIMENSION, compressed, &dim_row, dim[l].dimension_type, dim_inst[l].size)
						    || !dim_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_GENERIC_DIM_READ_ERROR);
							if (dim_row)
								free(dim_row);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							free(cubedims);
							if (old_measure)
								free(old_measure);
							goto __OPH_EXIT_1;
						}
					} else {
						strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);	// A reduced dimension is handled by indexes
						dim[l].dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
					}
					// Store output labels
					if (oph_dim_insert_into_dimension_table
					    (db, o_label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_label))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (old_measure)
							free(old_measure);
						goto __OPH_EXIT_1;
					}
					if (dim_row)
						free(dim_row);

					// Set indexes
					snprintf(operation, OPH_COMMON_BUFFER_LEN, MYSQL_DIM_INDEX_ARRAY, OPH_DIM_INDEX_DATA_TYPE, 1, dim_inst[l].size);
					dim_row = NULL;
					if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, compressed, &dim_row) || !dim_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_GENERIC_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (old_measure)
							free(old_measure);
						goto __OPH_EXIT_1;
					}
					// Store output indexes
					if (oph_dim_insert_into_dimension_table
					    (db, o_index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (old_measure)
							free(old_measure);
						goto __OPH_EXIT_1;
					}
					if (dim_row)
						free(dim_row);
				} else
					dim_inst[l].fk_id_dimension_index = dim_inst[l].fk_id_dimension_label = 0;

				dim_inst[l].id_grid = 0;
				cl_value = NULL;
				if (oph_odb_dim_insert_into_dimensioninstance_table
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube, dim[l].dimension_name,
				     cl_value)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_GENERIC_DIM_INSTANCE_STORE_ERROR);
					if (cl_value)
						free(cl_value);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					if (old_measure)
						free(old_measure);
					goto __OPH_EXIT_1;
				}
				if (cl_value)
					free(cl_value);
			}
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
		}
		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				if (old_measure)
					free(old_measure);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube[0], ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			if (old_measure)
				free(old_measure);
			goto __OPH_EXIT_1;
		}

		if (old_measure) {
			if (oph_odb_meta_update_metadatakeys
			    (oDB, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube, old_measure,
			     ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
				goto __OPH_EXIT_1;
			}
			free(old_measure);
			old_measure = NULL;
		}

		if (!((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->user_missing_value) {
			int idmissingvalue = 0;
			if (oph_odb_cube_retrieve_missingvalue(oDB, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube, &idmissingvalue, NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve missing value\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, "Unable to retrieve missing value\n");
				goto __OPH_EXIT_1;
			}
			if (idmissingvalue) {
				char *mtype = NULL, *mvalue = NULL;
				if (oph_odb_meta_retrieve_single_metadata_instance(oDB, idmissingvalue, &mtype, &mvalue) || !mtype || !mvalue) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve missing value\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, "Unable to retrieve missing value\n");
					if (mtype)
						free(mtype);
					if (mvalue)
						free(mvalue);
					goto __OPH_EXIT_1;
				}

				if (!strcmp(mtype, OPH_COMMON_BYTE_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (unsigned char) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_SHORT_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (short) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_INT_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (int) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_LONG_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (long long) strtoll(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_FLOAT_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (float) strtof(mvalue, NULL);
				else if (!strcmp(mtype, OPH_COMMON_DOUBLE_TYPE))
					((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms = (double) strtod(mvalue, NULL);
				else if (!strcmp(mtype, OPH_COMMON_METADATA_TYPE_TEXT))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Missing value is a text: skipping\n");

				free(mtype);
				free(mvalue);
			}
		}

		last_insertd_id = 0;
		//Insert new task REMEBER TO MEMSET query to 0 IF NOT NEEDED
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		new_task.id_job = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_job;
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		char _ms[OPH_COMMON_MAX_DOUBLE_LENGHT];
		if (isnan(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms))
			snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "NULL");
		else
			snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "%f", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->ms);

		char compressed = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed;
		char intercube2_operand[OPH_COMMON_BUFFER_LEN], intercube2_operands[OPH_COMMON_BUFFER_LEN];
		char intercube2_datatype[OPH_COMMON_BUFFER_LEN], intercube2_datatypes[OPH_COMMON_BUFFER_LEN];
		char intercube2_from[OPH_COMMON_BUFFER_LEN], intercube2_froms[OPH_COMMON_BUFFER_LEN];
		char intercube2_where[OPH_COMMON_BUFFER_LEN], intercube2_wheres[OPH_COMMON_BUFFER_LEN];
		*intercube2_operands = *intercube2_datatypes = *intercube2_froms = *intercube2_wheres = 0;
		for (i = 0; i < cubes_num; i++) {
			snprintf(intercube2_operand, OPH_COMMON_BUFFER_LEN, "%s%sfact_in%d.%s%s", i ? "," : "", compressed ? "oph_uncompress('',''," : "", i + 1, MYSQL_FRAG_MEASURE,
				 compressed ? ")" : "");
			strncat(intercube2_operands, intercube2_operand, OPH_COMMON_BUFFER_LEN - strlen(intercube2_operands));
			snprintf(intercube2_datatype, OPH_COMMON_BUFFER_LEN, "%soph_%s", i ? "|" : "", ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type);
			strncat(intercube2_datatypes, intercube2_datatype, OPH_COMMON_BUFFER_LEN - strlen(intercube2_datatypes));
			snprintf(intercube2_from, OPH_COMMON_BUFFER_LEN, "%sfact_in%d", i ? "|" : "", i + 1);
			strncat(intercube2_froms, intercube2_from, OPH_COMMON_BUFFER_LEN - strlen(intercube2_froms));
			if (i) {
				snprintf(intercube2_where, OPH_COMMON_BUFFER_LEN, "%sfact_in1.%s=fact_in%d.%s", i > 1 ? " AND " : "", MYSQL_FRAG_ID, i + 1, MYSQL_FRAG_ID);
				strncat(intercube2_wheres, intercube2_where, OPH_COMMON_BUFFER_LEN - strlen(intercube2_wheres));
			}
		}

		char intercube2_operation[OPH_COMMON_BUFFER_LEN];
		snprintf(intercube2_operation, OPH_COMMON_BUFFER_LEN, "fact_in1.%s|%soph_operation_array('%s','oph_%s',%s,'oph_%s',%s)%s", MYSQL_FRAG_ID, compressed ? "oph_compress('',''," : "",
			 intercube2_datatypes, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type, intercube2_operands,
			 ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation, _ms, compressed ? ")" : "");
		if (*intercube2_wheres)
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_INTERCUBE2_QUERY, intercube2_operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, intercube2_froms, intercube2_wheres);
		else
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_INTERCUBE2_QUERY_WW, intercube2_operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, intercube2_froms);

		new_task.input_cube_number = cubes_num;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		for (i = 0; i < cubes_num; i++)
			new_task.id_inputcube[i] = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_datacube[i];

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_TASK_INSERT_ERROR,
				new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed, sizeof(int));

		strncpy(data_type, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
	}
      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MASTER_TASK_INIT_FAILED);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT,
				"fragment ids");
			((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT,
				"measure type");
			((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_INTERCUBE2_operator_handle *oper_handle = (OPH_INTERCUBE2_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int l, i, j, k, kk, ii;

	int id_datacube_out = oper_handle->id_output_datacube;
	int *id_datacube_in = oper_handle->id_input_datacube;
	int compressed = oper_handle->compressed;
	int cubes_num = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num;

	oph_odb_fragment_list frags[cubes_num];
	oph_odb_db_instance_list dbs[cubes_num];
	oph_odb_dbms_instance_list dbmss[cubes_num];

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	char multi_host = 0;

	//retrieve connection string
	for (l = 0; l < cubes_num; l++) {
		if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in[l], oper_handle->fragment_ids, frags + l, dbs + l, dbmss + l)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_CONNECTION_STRINGS_NOT_FOUND, "second");
			for (l--; l >= 0; l--) {
				oph_odb_stge_free_fragment_list(frags + l);
				oph_odb_stge_free_db_list(dbs + l);
				oph_odb_stge_free_dbms_list(dbmss + l);
			}
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (l) {
			if ((dbmss[0].size != dbmss[l].size) || (dbs[0].size != dbs[l].size) || (frags[0].size != frags[l].size)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube structures are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DATACUBE_COMPARISON_ERROR, "structures");
				for (l--; l >= 0; l--) {
					oph_odb_stge_free_fragment_list(frags + l);
					oph_odb_stge_free_db_list(dbs + l);
					oph_odb_stge_free_dbms_list(dbmss + l);
				}
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (!multi_host)
				multi_host = dbmss[0].value[0].id_dbms != dbmss[l].value[0].id_dbms;
		}
	}

	char operation[OPH_COMMON_BUFFER_LEN];
	char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
	int n, result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0;
	unsigned long long tot_rows;

	char _ms[OPH_COMMON_MAX_DOUBLE_LENGHT];
	if (isnan(oper_handle->ms))
		snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "NULL");
	else
		snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "%f", oper_handle->ms);

	oph_ioserver_handler *first_server = NULL;
	if (oph_dc_setup_dbms(&(first_server), (dbmss[0].value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_IOPLUGIN_SETUP_ERROR, (dbmss[0].value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_ioserver_handler *second_server[cubes_num];
	if ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && multi_host) {
		second_server[0] = first_server;
		for (l = 1; l < cubes_num; l++) {
			if (oph_dc_setup_dbms(second_server + l, (dbmss[l].value[0]).io_server_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_IOPLUGIN_SETUP_ERROR, (dbmss[l].value[0]).id_dbms);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
		}
	}
	// This implementation assumes a perfect correspondence between datacube structures
	int i2[cubes_num], j2[cubes_num], k2[cubes_num];
	oph_odb_fragment *_value[cubes_num];

	//For each DBMS
	for (l = 1; l < cubes_num; l++)
		i2[l] = 0;
	for (i = 0; (i < dbmss[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {

		i2[0] = i;

		if (oph_dc_connect_to_dbms(first_server, &(dbmss[0].value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DBMS_CONNECTION_ERROR, (dbmss[0].value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		for (l = 1; l < cubes_num; l++) {
			// This implementation considers data exchange within the same dbms, databases could be different
			if (dbmss[0].value[i].id_dbms != dbmss[l].value[i].id_dbms) {
				// Find the correct dbms
				k = i * dbs[0].size * frags[0].size;
				for (; i2[l] < dbmss[l].size; ++i2[l]) {
					kk = i2[l] * dbs[l].size * frags[l].size;
					if (frags[0].value[k].frag_relative_index == frags[l].value[kk].frag_relative_index)
						break;
				}
				if (i2[l] >= dbmss[l].size) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot compare this datacube because of the different fragmentation structure.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DIFFERENT_DBMS_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			} else
				i2[l] = i;

			if (multi_host && oph_dc_connect_to_dbms(second_server[l], &(dbmss[l].value[i2[l]]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DBMS_CONNECTION_ERROR, (dbmss[0].value[i]).id_dbms);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}

			if (result)
				break;
		}

		//For each DB
		for (l = 0; l < cubes_num; l++)
			j2[l] = 0;
		for (j = 0; (j < dbs[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {

			j2[0] = j;

			//Check DB - DBMS Association
			if (dbs[0].value[j].dbms_instance != &(dbmss[0].value[i]))
				continue;
			if (oph_dc_use_db_of_dbms(first_server, &(dbmss[0].value[i]), &(dbs[0].value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DB_SELECTION_ERROR, (dbs[0].value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			for (l = 1; l < cubes_num; l++) {

				//Check DB - DBMS Association
				if (!multi_host) {
					j2[l] = j;
					if (dbs[l].value[j2[l]].dbms_instance != &(dbmss[l].value[i2[l]])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Databases are not comparable.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DIFFERENT_DB_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
				} else {
					for (; j2[l] < dbs[l].size; j2[l]++)
						if (dbs[l].value[j2[l]].dbms_instance == &(dbmss[l].value[i2[l]]))
							break;	// Search the correct db associated to the dbms
					if (j2[l] >= dbs[l].size) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot compare this datacube because of the different fragmentation structure.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DIFFERENT_DBMS_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oph_dc_use_db_of_dbms(second_server[l], &(dbmss[l].value[i2[l]]), &(dbs[l].value[j2[l]]))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DB_SELECTION_ERROR, (dbs[l].value[j2[l]]).db_name);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
				}
				if (result)
					break;
			}

			//For each fragment
			for (l = 1; l < cubes_num; l++)
				k2[l] = 0;
			for (k = 0; (k < frags[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {

				k2[0] = k;

				//Check Fragment - DB Association
				if (frags[0].value[k].db_instance != &(dbs[0].value[j]))
					continue;

				for (l = 1; l < cubes_num; l++) {

					//Check Fragment - DB Association
					if (!multi_host) {
						k2[l] = k;
						if (frags[l].value[k2[l]].db_instance != &(dbs[l].value[j2[l]])) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_FRAGMENT_COMPARISON_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					} else {
						for (; k2[l] < dbs[l].size; k2[l]++)
							if (frags[l].value[k2[l]].db_instance == &(dbs[l].value[j2[l]]))
								break;	// Search the correct fragment associated to the db
						if (k2[l] >= frags[l].size) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot compare this datacube because of the different fragmentation structure.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_DIFFERENT_DBMS_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
					if (result)
						break;
				}

				if (oph_dc_generate_fragment_name(dbs[0].value[j].db_name, id_datacube_out, handle->proc_rank, (frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (multi_host) {

					tot_rows = frags[0].value[k].key_end - frags[0].value[k].key_start + 1;

					// Create an empty fragment
					if (oph_dc_create_empty_fragment_from_name(first_server, frag_name_out, frags[0].value[k].db_instance)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_NEW_FRAG_ERROR, frag_name_out);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}

					for (l = 0; l < cubes_num; l++)
						_value[l] = &frags[l].value[k2[l]];

					if (oph_dc_copy_and_process_fragment2
					    (cubes_num, second_server, tot_rows, _value, frag_name_out, compressed, oper_handle->operation, oper_handle->measure_type, _ms)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_NEW_FRAG_ERROR, frag_name_out);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}

					frags[0].value[k].id_datacube = id_datacube_out;
					strncpy(frags[0].value[k].fragment_name, 1 + strchr(frag_name_out, '.'), OPH_ODB_STGE_FRAG_NAME_SIZE);
					frags[0].value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;

					// Insert new fragment in OphDB
					if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags[0].value[k]))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_FRAGMENT_INSERT_ERROR, frag_name_out);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}

					for (l = 1; l < cubes_num; l++)
						k2[l]++;

				} else {

					char compressed = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->compressed;
					char intercube2_operand[OPH_COMMON_BUFFER_LEN], intercube2_operands[OPH_COMMON_BUFFER_LEN];
					char intercube2_datatype[OPH_COMMON_BUFFER_LEN], intercube2_datatypes[OPH_COMMON_BUFFER_LEN];
					char intercube2_from[OPH_COMMON_BUFFER_LEN], intercube2_froms[OPH_COMMON_BUFFER_LEN];
					char intercube2_alias[OPH_COMMON_BUFFER_LEN], intercube2_aliass[OPH_COMMON_BUFFER_LEN];
					char intercube2_where[OPH_COMMON_BUFFER_LEN], intercube2_wheres[OPH_COMMON_BUFFER_LEN];
					*intercube2_operands = *intercube2_datatypes = *intercube2_froms = *intercube2_aliass = *intercube2_wheres = 0;
					for (ii = 0; ii < cubes_num; ii++) {
						snprintf(intercube2_operand, OPH_COMMON_BUFFER_LEN, "%s%sfrag%d.%s%s", ii ? "," : "", compressed ? "oph_uncompress('',''," : "", ii + 1,
							 MYSQL_FRAG_MEASURE, compressed ? ")" : "");
						strncat(intercube2_operands, intercube2_operand, OPH_COMMON_BUFFER_LEN - strlen(intercube2_operands));
						snprintf(intercube2_datatype, OPH_COMMON_BUFFER_LEN, "%soph_%s", ii ? "|" : "",
							 ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type);
						strncat(intercube2_datatypes, intercube2_datatype, OPH_COMMON_BUFFER_LEN - strlen(intercube2_datatypes));
						snprintf(intercube2_from, OPH_COMMON_BUFFER_LEN, "%s%s.%s", ii ? "|" : "", frags[ii].value[k2[ii]].db_instance->db_name,
							 frags[ii].value[k2[ii]].fragment_name);
						strncat(intercube2_froms, intercube2_from, OPH_COMMON_BUFFER_LEN - strlen(intercube2_froms));
						snprintf(intercube2_alias, OPH_COMMON_BUFFER_LEN, "%sfrag%d", ii ? "|" : "", ii + 1);
						strncat(intercube2_aliass, intercube2_alias, OPH_COMMON_BUFFER_LEN - strlen(intercube2_aliass));
						if (ii) {
							snprintf(intercube2_where, OPH_COMMON_BUFFER_LEN, "%sfrag1.%s=frag%d.%s", ii > 1 ? " AND " : "", MYSQL_FRAG_ID, ii + 1, MYSQL_FRAG_ID);
							strncat(intercube2_wheres, intercube2_where, OPH_COMMON_BUFFER_LEN - strlen(intercube2_wheres));
						}
					}
					char intercube2_operation[OPH_COMMON_BUFFER_LEN];
					snprintf(intercube2_operation, OPH_COMMON_BUFFER_LEN, "frag1.%s|%soph_operation_array('%s','oph_%s',%s,'oph_%s',%s)%s", MYSQL_FRAG_ID,
						 compressed ? "oph_compress('',''," : "", intercube2_datatypes, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type,
						 intercube2_operands, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation, _ms, compressed ? ")" : "");
					if (*intercube2_wheres)
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_INTERCUBE2_QUERY2, frag_name_out, intercube2_operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
							     intercube2_froms, intercube2_aliass, intercube2_wheres);
					else
						n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_INTERCUBE2_QUERY2_WW, frag_name_out, intercube2_operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
							     intercube2_froms, intercube2_aliass);
					if (n >= OPH_COMMON_BUFFER_LEN) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_STRING_BUFFER_OVERFLOW, "MySQL operation name",
							operation);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					//INTERCUBE fragment
					if (oph_dc_create_fragment_from_query(first_server, &(frags[0].value[k]), NULL, operation, 0, 0, 0)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_NEW_FRAG_ERROR, frag_name_out);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
					//Change fragment fields
					frags[0].value[k].id_datacube = id_datacube_out;
					strncpy(frags[0].value[k].fragment_name, 1 + strchr(frag_name_out, '.'), OPH_ODB_STGE_FRAG_NAME_SIZE);
					frags[0].value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;

					//Insert new fragment
					if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags[0].value[k]))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_FRAGMENT_INSERT_ERROR, frag_name_out);
						result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						break;
					}
				}

				frag_count++;
			}

			if (multi_host)
				for (l = 1; l < cubes_num; l++)
					j2[l]++;
		}

		oph_dc_disconnect_from_dbms(first_server, &(dbmss[0].value[i]));

		if (multi_host)
			for (l = 1; l < cubes_num; l++)
				oph_dc_disconnect_from_dbms(second_server[l], &(dbmss[l].value[i2[l]]));
	}

	if (oph_dc_cleanup_dbms(first_server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_IOPLUGIN_CLEANUP_ERROR, (dbmss[0].value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (multi_host) {
		for (l = 1; l < cubes_num; l++) {
			if (oph_dc_cleanup_dbms(second_server[l])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_INTERCUBE_IOPLUGIN_CLEANUP_ERROR, (dbmss[l].value[0]).id_dbms);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}
	}

	oph_odb_free_ophidiadb(&oDB_slave);
	for (l = 0; l < cubes_num; l++) {
		oph_odb_stge_free_fragment_list(frags + l);
		oph_odb_stge_free_db_list(dbs + l);
		oph_odb_stge_free_dbms_list(dbmss + l);
	}

	if (result == OPH_ANALYTICS_OPERATOR_SUCCESS)
		oper_handle->execute_error = 0;

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCUBE_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_container,
			 ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_INTERCUBE)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_INTERCUBE, "Output Cube", jsonbuf)) {
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
		if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data
			     (id_datacube, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids,
			      0, 0, 1))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
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
			oph_dproc_clean_odb(&((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

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
		oph_odb_disconnect_from_ophidiadb(&((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->operation = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes) {
		oph_tp_free_multiple_value_param_list(((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes, ((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes_num);
		((OPH_INTERCUBE2_operator_handle *) handle->operator_handle)->cubes = NULL;
	}
	free((OPH_INTERCUBE2_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
