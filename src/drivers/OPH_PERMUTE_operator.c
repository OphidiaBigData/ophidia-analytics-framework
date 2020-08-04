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

#include "drivers/OPH_PERMUTE_operator.h"
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

#include <pthread.h>

int oph_permute_parse(const char *cond, unsigned int *permutation_indexes, unsigned int max)
{
	unsigned int i;
	char *result, temp[1 + OPH_TP_TASKLEN], flags[max], *savepointer;
	for (i = 0; i < max; ++i)
		flags[i] = 0;

	strncpy(temp, cond, OPH_TP_TASKLEN);
	temp[OPH_TP_TASKLEN] = 0;
	result = strtok_r(temp, OPH_PERMUTE_SEPARATOR, &savepointer);
	i = 0;
	while (result && (i < max)) {
		permutation_indexes[i] = atol(result);
		if (!permutation_indexes[i] || (permutation_indexes[i] > max)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong index '%d' in permutation string\n", permutation_indexes[i]);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		permutation_indexes[i]--;	// C-like indexing
		flags[permutation_indexes[i]] = 1;
		i++;
		result = strtok_r(NULL, OPH_PERMUTE_SEPARATOR, &savepointer);
	}
	if (result || (i < max)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of elements in permutation string or size parameters\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	for (i = 0; i < max; ++i)
		if (!flags[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed some dimensions in permutation string\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
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

	if (!(handle->operator_handle = (OPH_PERMUTE_operator_handle *) calloc(1, sizeof(OPH_PERMUTE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->folder_id = 0;

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->nthread = (unsigned int) strtol(value, NULL, 10);

	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[3] = { 0, 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_ARG_USERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_user = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);
#ifdef OPH_ODB_MNG
		oph_odb_init_mongodb(oDB);
#endif
		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_OPHIDIADB_CONNECTION_ERROR);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
#ifdef OPH_ODB_MNG
		if (oph_odb_connect_to_mongodb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PERMUTE_OPHIDIADB_CONNECTION_ERROR);
			return OPH_ANALYTICS_OPERATOR_MONGODB_ERROR;
		}
#endif
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		if (oph_pid_parse_pid(datacube_in, &id_datacube_in[1], &id_datacube_in[0], &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_PID_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_NO_INPUT_DATACUBE, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_DATACUBE_FOLDER_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;

		id_datacube_in[2] = id_datacube_in[1];

		if (strcasecmp(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
			char *sessionid = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid;
			char *path = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path;
			char *cwd = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd;
			char *abs_path = NULL;
			if (oph_odb_fs_path_parsing(path, cwd, &folder_id, &abs_path, oDB) || !folder_id) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to parse path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if (abs_path)
				free(abs_path);
			if ((oph_odb_fs_check_folder_session(folder_id, sessionid, oDB, &permission)) || !permission) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path '%s' is not allowed\n", path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Path '%s' is not allowed\n", path);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}
		}
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->folder_id = folder_id;
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, 3, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "imppermutation");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(value = strchr(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation, ','))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s: at least 2 implicit dimensions must be given\n", OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char tmp[OPH_TP_TASKLEN], *save_pointer = NULL;
	int pos, found = 0;
	strcpy(tmp, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation);
	for (value = strtok_r(tmp, ",", &save_pointer); value; value = strtok_r(NULL, ",", &save_pointer)) {
		pos = (int) strtol(value, NULL, 10);
		if (pos < 1)
			break;
		found++;
	}
	if (value || (found < 2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s: at least the indexes of 2 implicit dimensions must be given\n", OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "description");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, OPH_IN_PARAM_OUTPUT_PATH);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CWD);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, stream_max_size = 5 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 2 * sizeof(int) + OPH_ODB_CUBE_MEASURE_TYPE_SIZE + OPH_TP_TASKLEN;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[3], *data_type, *params;
	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	data_type = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	params = stream + pointer;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_DATACUBE_READ_ERROR);
			goto __OPH_EXIT_1;
		}
		// Change the container id
		cube.id_container = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container;

		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		//Copy fragment id relative index set 
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "fragment ids");
			goto __OPH_EXIT_1;
		}
		//Copy measure_type relative index set  
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "measure type");
			goto __OPH_EXIT_1;
		}

		int tot_frag_num = 0;
		if (oph_ids_count_number_of_ids(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids, &tot_frag_num)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DUPLICATE_RETREIVE_IDS_ERROR);
		} else {
			//Check that product of ncores and nthread is at most equal to total number of fragments        
			if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > (unsigned int) tot_frag_num) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
			}
		}

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int last_insertd_id = 0;
		int l;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_CUBEHASDIM_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}

		unsigned int max = 0;
		char temp[OPH_COMMON_BUFFER_LEN];
		*params = '\0';
		for (l = number_of_dimensions - 1; l >= 0; l--) {
			if (cubedims[l].size) {
				if (!cubedims[l].explicit_dim)	// Implicit dimension
				{
					if (*params)
						strcat(params, ",");
					snprintf(temp, OPH_COMMON_BUFFER_LEN, "%d", cubedims[l].size);
					strncat(params, temp, OPH_TP_TASKLEN - strlen(params) - 1);
					max++;
				} else
					break;	// Explicit dimension
			}
		}
		if (!max)
			strcat(params, "1");	// No implicit dimensions
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes = (char *) strndup(params, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "sizes");
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (max) {
			unsigned int ll, permutation_indexes[max];
			if (oph_permute_parse(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation, permutation_indexes, max)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse '%s'; number of elements should be %d and each value should be in [1,%d].\n",
				      ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation, max, max);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container,
					OPH_LOG_OPH_PERMUTE_PARSE_IMPLICIT_PERMUTAION_ERROR, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation, max, max);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			for (ll = 0; ll < max; ll++)
				permutation_indexes[ll]++;
			for (l = 0; l < number_of_dimensions; l++)
				if (!cubedims[l].explicit_dim && cubedims[l].size) {
					for (ll = 0; ll < max; ll++)
						if (cubedims[l].level == (int) permutation_indexes[ll]) {
							cubedims[l].level = 1 + ll;
							break;
						}
					if (ll >= max) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in update of dimension levels.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_PERMUTE_DIMENSION_LEVEL_ERROR);
						oph_odb_cube_free_datacube(&cube);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
				}
		}
		//New fields
		cube.id_source = 0;
		cube.level++;
		cube.id_folder = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->folder_id;
		if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		// Copy the dimension in case of output has to be saved in a new container
		if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container != ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container) {
			oph_odb_dimension dim[number_of_dimensions];
			oph_odb_dimension_instance dim_inst[number_of_dimensions];
			for (l = 0; l < number_of_dimensions; ++l) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_LOAD_ERROR);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_CONNECT_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_USE_DB_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container);
			char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(o_index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container);
			snprintf(o_label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_container);

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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					if (dim_inst[l].fk_id_dimension_label) {
						if (oph_dim_read_dimension_filtered_data
						    (db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, MYSQL_DIMENSION, compressed, &dim_row, dim[l].dimension_type, dim_inst[l].size)
						    || !dim_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_GENERIC_DIM_READ_ERROR);
							if (dim_row)
								free(dim_row);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							free(cubedims);
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					if (dim_row)
						free(dim_row);

					// Set indexes
					snprintf(operation, OPH_COMMON_BUFFER_LEN, MYSQL_DIM_INDEX_ARRAY, OPH_DIM_INDEX_DATA_TYPE, 1, dim_inst[l].size);
					dim_row = NULL;
					if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, compressed, &dim_row) || !dim_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					// Store output indexes
					if (oph_dim_insert_into_dimension_table
					    (db, o_index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					if (dim_row)
						free(dim_row);
				} else
					dim_inst[l].fk_id_dimension_index = dim_inst[l].fk_id_dimension_label = 0;

				dim_inst[l].id_grid = 0;
				cl_value = NULL;
				if (oph_odb_dim_insert_into_dimensioninstance_table
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube, dim[l].dimension_name,
				     cl_value, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_user)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_INSTANCE_STORE_ERROR);
					if (cl_value)
						free(cl_value);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
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
			cubedims[l].id_datacube = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube;
			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);
		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_datacube,
		     ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->compressed)
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_PERMUTE_QUERY_COMPR, MYSQL_FRAG_ID,
				 ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type,
				 MYSQL_FRAG_MEASURE, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation,
				 ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes, MYSQL_FRAG_MEASURE);
		else
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_PERMUTE_QUERY, MYSQL_FRAG_ID,
				 ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type,
				 MYSQL_FRAG_MEASURE, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation,
				 ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes, MYSQL_FRAG_MEASURE);
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_datacube;
		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_TASK_INSERT_ERROR, new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);
		strncpy(id_string[0], ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_PERMUTE_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		strncpy(data_type, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
	}

      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MASTER_TASK_INIT_FAILED);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "fragment ids");
			((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "measure type");
			((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes = (char *) strndup(params, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "sizes");
			((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 1;
	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);
	//Every process must process at least divResult
	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_number = div_result;
	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_PERMUTE_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_PERMUTE_operator_handle *oper_handle = (OPH_PERMUTE_operator_handle *) handle->operator_handle;
	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	oper_handle->execute_error = 1;
	int l;
	int num_threads = (oper_handle->nthread <= (unsigned int) oper_handle->fragment_number ? oper_handle->nthread : (unsigned int) oper_handle->fragment_number);
	int res[num_threads];
	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;
	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_OPHIDIADB_CONFIGURATION_FILE);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, oper_handle->id_input_datacube, oper_handle->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	struct _thread_struct {
		OPH_PERMUTE_operator_handle *oper_handle;
		unsigned int current_thread;
		unsigned int total_threads;
		int proc_rank;
		oph_odb_fragment_list *frags;
		oph_odb_db_instance_list *dbs;
		oph_odb_dbms_instance_list *dbmss;
	};
	typedef struct _thread_struct thread_struct;
	void *exec_thread(void *ts) {

		OPH_PERMUTE_operator_handle *oper_handle = ((thread_struct *) ts)->oper_handle;
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
		char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
		int n, frag_count = 0;
		oph_ioserver_handler *server = NULL;
		if (oph_dc_setup_dbms_thread(&(server), (dbmss->value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_IOPLUGIN_SETUP_ERROR, (dbmss->value[0]).id_dbms);
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
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_DB_SELECTION_ERROR, (dbs->value[i]).db_name);
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = first_frag; (k < frags->size) && (frag_count < fragxthread) && (res == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags->value[k].db_instance != &(dbs->value[i]))
					continue;
				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, proc_rank, (current_frag_count + frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//PERMUTE mysql plugin
				if (compressed)
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_PERMUTE_PLUGIN_COMPR, oper_handle->measure_type,
						     oper_handle->measure_type, MYSQL_FRAG_MEASURE, oper_handle->imppermutation, oper_handle->sizes);
				else
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_PERMUTE_PLUGIN, oper_handle->measure_type,
						     oper_handle->measure_type, MYSQL_FRAG_MEASURE, oper_handle->imppermutation, oper_handle->sizes);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//PERMUTE fragment
				if (oph_dc_create_fragment_from_query(server, &(frags->value[k]), frag_name_out, operation, 0, 0, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_NEW_FRAG_ERROR, frag_name_out);
					res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Change fragment fields
				frags->value[k].id_datacube = id_datacube_out;
				strncpy(frags->value[k].fragment_name, frag_name_out, OPH_ODB_STGE_FRAG_NAME_SIZE);
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
	//Insert new fragment
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PERMUTE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_PERMUTE_operator_handle *oper_handle = (OPH_PERMUTE_operator_handle *) handle->operator_handle;
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
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid(oper_handle->id_output_container, oper_handle->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_PERMUTE_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, oper_handle->id_output_container, oper_handle->id_output_datacube);
		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_PERMUTE)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_PERMUTE, "Output Cube", jsonbuf)) {
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
		int num_threads = (oper_handle->nthread <= (unsigned int) oper_handle->fragment_number ? oper_handle->nthread : (unsigned int) oper_handle->fragment_number);
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
		oph_odb_free_ophidiadb(&((OPH_PERMUTE_operator_handle *) handle->operator_handle)->oDB);
#ifdef OPH_ODB_MNG
		oph_odb_free_mongodb(&((OPH_PERMUTE_operator_handle *) handle->operator_handle)->oDB);
#endif
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->imppermutation = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sizes = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->output_path = NULL;
	}
	if (((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd);
		((OPH_PERMUTE_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	free((OPH_PERMUTE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
