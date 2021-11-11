/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "drivers/OPH_EXPORTESDM_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"
#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_esdm_library.h"

#include <errno.h>

#define OPH_EXPORTESDM_DEFAULT_OUTPUT "default"

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

	if (!(handle->operator_handle = (OPH_EXPORTESDM_operator_handle *) calloc(1, sizeof(OPH_EXPORTESDM_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->export_metadata = 0;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->cached_flag = 0;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid = NULL;

	char *datacube_name;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys, &((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[2] = { 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value, user_space;
	if (oph_pid_get_user_space(&user_space)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read user_space\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read user_space\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_EXPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->export_metadata = 1;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_OPHIDIADB_CONFIGURATION_FILE);

			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_OPHIDIADB_CONNECTION_ERROR);

			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		if (oph_pid_parse_pid(datacube_name, &id_datacube_in[1], &id_datacube_in[0], &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_PID_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_NO_INPUT_DATACUBE, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_DATACUBE_FOLDER_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, 2, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_NO_INPUT_DATACUBE, datacube_name);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTESDM_NO_INPUT_CONTAINER, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	//Save datacube PID
	if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->datacube_input = (char *) strdup(datacube_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_EXPORTESDM_DEFAULT_OUTPUT)) {
		if (strncmp(value, OPH_ESDM_PREFIX, 7)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong ESDM object\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "Wrong ESDM object\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name = (char *) strdup(value + 7))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"output name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	esdm_status ret = esdm_init();
	if (ret) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "ESDM cannot be initialized\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

/*
	ret = esdm_mkfs(ESDM_FORMAT_PURGE_RECREATE, ESDM_ACCESSIBILITY_GLOBAL);
	if (ret) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM fs cannot be created\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "ESDM fs cannot be created\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	ret = esdm_mkfs(ESDM_FORMAT_PURGE_RECREATE, ESDM_ACCESSIBILITY_NODELOCAL);
	if (ret) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM fs cannot be created\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "ESDM fs cannot be created\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
*/

	//For error checking
	char id_string[5][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0][0] = 0;

	char *stream_broad = 0;
	if (handle->proc_rank == 0) {

		MYSQL_RES *dim_rows = NULL;
		MYSQL_ROW row;

		ophidiadb *oDB = &((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DATACUBE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->compressed = cube.compressed;
		//Copy fragment id relative index set   
		if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))
		    || !(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))
		    || !(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure = (char *) strndup(cube.measure, OPH_ODB_CUBE_MEASURE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"fragment ids");
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		//Set dimensions
		int number_of_dimensions = 0;

		if (oph_odb_dim_find_dimensions_features(oDB, datacube_id, &dim_rows, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIMENSION_FEATURES_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}

		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims = number_of_dimensions;
		int i = 0, j = 0;
		char stream[number_of_dimensions][OPH_DIM_STREAM_ELEMENTS][OPH_DIM_STREAM_LENGTH];
		while ((row = mysql_fetch_row(dim_rows))) {
			if (i == number_of_dimensions) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_INTERNAL_ERROR);
				oph_odb_cube_free_datacube(&cube);
				mysql_free_result(dim_rows);
				goto __OPH_EXIT_1;
			}
			for (j = 0; j < OPH_DIM_STREAM_ELEMENTS; j++) {
				strncpy(stream[i][j], row[j], OPH_DIM_STREAM_LENGTH * sizeof(char));
			}
			i++;
		}
		stream_broad = (char *) malloc(number_of_dimensions * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char));
		if (!stream_broad) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR, "stream broad");
			oph_odb_cube_free_datacube(&cube);
			mysql_free_result(dim_rows);
			goto __OPH_EXIT_1;
		}
		memcpy(stream_broad, stream, (size_t) (number_of_dimensions * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char)));

		oph_odb_cube_free_datacube(&cube);
		mysql_free_result(dim_rows);

		strncpy(id_string[0], ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		strncpy(id_string[1], ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		strncpy(id_string[2], ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		snprintf(id_string[3], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->compressed);
		snprintf(id_string[4], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims);
	}
      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index 
	MPI_Bcast(id_string, 5 * OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0][0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else if (id_string[0][0] == -1) {
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->cached_flag = 1;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"fragment ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure = (char *) strndup(id_string[1], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"measure name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(id_string[2], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"measure type");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->compressed = (int) strtol(id_string[3], NULL, 10);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims = (int) strtol(id_string[4], NULL, 10);
		stream_broad = (char *) malloc(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char));
		if (!stream_broad) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR, "stream broad");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(stream_broad, 0, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char));
	}

	MPI_Bcast(stream_broad, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);

	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims = (ESDM_dim *) malloc(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims * sizeof(ESDM_dim));
	if (!((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
			"NETCDF dimensions");
		free(stream_broad);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	ESDM_dim *dims = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims;
	int i;
	for (i = 0; i < ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims; i++) {
		dims[i].dimid = (int) strtol(stream_broad + (i * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH), NULL, 10);
		strncpy(dims[i].dimname, stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 1) * OPH_DIM_STREAM_LENGTH), OPH_ODB_DIM_DIMENSION_SIZE);
		dims[i].dimname[OPH_ODB_DIM_DIMENSION_SIZE] = 0;
		strncpy(dims[i].dimtype, stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 2) * OPH_DIM_STREAM_LENGTH), OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
		dims[i].dimtype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
		dims[i].dimsize = (int) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 3) * OPH_DIM_STREAM_LENGTH), NULL, 10);
		dims[i].dimexplicit = (short int) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 4) * OPH_DIM_STREAM_LENGTH), NULL, 10);
		dims[i].dimophlevel = (short int) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 5) * OPH_DIM_STREAM_LENGTH), NULL, 10);
		dims[i].dimfkid = (int) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 6) * OPH_DIM_STREAM_LENGTH), NULL, 10);
		dims[i].dimfklabel = (int) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 9) * OPH_DIM_STREAM_LENGTH), NULL, 10);
		dims[i].dimunlimited = (char) strtol(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 10) * OPH_DIM_STREAM_LENGTH), NULL, 10);
	}

	if (stream_broad) {
		free(stream_broad);
		stream_broad = NULL;
	}

	if (!((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name) {
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name =
		    (char *) malloc(strlen(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure) + OPH_COMMON_MAX_INT_LENGHT + 10);
		if (!((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT,
				"output name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		sprintf(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name, "%s_%d", ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure,
			((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->total_fragment_number = id_number;

	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	int fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position, fragment_number,
	     &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, j, k, inc;

	int datacube_id = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube;
	char *file = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name;
	char *measure_name = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure;
	char *data_type = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type;
	int compressed = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->compressed;
	int num_of_dims = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->num_of_dims;
	ESDM_dim *dims = ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims;

	// I need an array of dimensions size and I need to know the number of explicit dimensions
	unsigned int dims_size[num_of_dims];
	short int nexp = 0;
	for (i = 0; i < num_of_dims; i++) {
		dims_size[i] = dims[i].dimsize;
		if (dims[i].dimexplicit)
			nexp++;
	}

	int total_size = 1;
	for (inc = 0; inc < nexp; inc++)
		total_size *= dims_size[inc];

	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	// Get data type of variable
	esdm_type_t type_nc = SMD_DTYPE_UNKNOWN;
	if (oph_esdm_get_esdm_type(data_type, &type_nc)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_VAR_TYPE_NOT_SUPPORTED, data_type);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_OPHIDIADB_CONFIGURATION_FILE);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, datacube_id, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int n;
	int frag_count = 0;

	char file_name[OPH_COMMON_BUFFER_LEN] = { '\0' };

	oph_ioserver_result *frag_rows = NULL;
	oph_ioserver_row *curr_row = NULL;

	//Load dimension table database infos and connect
	oph_odb_db_instance db_;
	oph_odb_db_instance *db_dimension = &db_;
	if (oph_dim_load_dim_dbinstance(db_dimension)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_LOAD);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_CONNECT);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
	snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container);
	snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container);
	int exist_flag = 0;

	if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_TABLE_RETREIVE_ERROR,
			index_dimension_table_name);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (!exist_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_TABLE_RETREIVE_ERROR,
			label_dimension_table_name);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (!exist_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char **dim_rows = (char **) malloc(num_of_dims * sizeof(char *));
	memset(dim_rows, 0, num_of_dims * sizeof(char *));
	char operation[OPH_COMMON_BUFFER_LEN];
	int m, l;

	MYSQL_RES *read_result = NULL;
	MYSQL_ROW row;
	char *mvariable, *mkey, *mtype, *mvalue;
	char *names[num_of_dims];

	for (m = 0; m < num_of_dims; m++) {
		n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "%s", MYSQL_DIMENSION);
		if (n >= OPH_COMMON_BUFFER_LEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_STRING_BUFFER_OVERFLOW,
				"MySQL operation name", operation);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			free(dim_rows);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dims[m].dimfkid, operation, 0, &(dim_rows[m])) || !dim_rows[m]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_READ_ERROR);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			for (l = 0; l < num_of_dims; l++) {
				if (dim_rows[l]) {
					free(dim_rows[l]);
					dim_rows[l] = NULL;
				}
			}
			free(dim_rows);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (dims[m].dimfklabel) {
			if (oph_dim_read_dimension_filtered_data(db_dimension, label_dimension_table_name, dims[m].dimfklabel, operation, 0, &(dim_rows[m]), dims[m].dimtype, dims[m].dimsize)
			    || !dim_rows[m]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DIM_READ_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				for (l = 0; l < num_of_dims; l++) {
					if (dim_rows[l]) {
						free(dim_rows[l]);
						dim_rows[l] = NULL;
					}
				}
				free(dim_rows);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		} else {
			strncpy(dims[i].dimtype, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
			dims[i].dimtype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
		}
	}
	oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
	oph_dim_unload_dim_dbinstance(db_dimension);

	int64_t start[num_of_dims];
	int64_t count[num_of_dims];

	int64_t dim_val_max[nexp];
	int64_t dim_val_min[nexp];
	int64_t dim_val_num[num_of_dims];
	int64_t *maxptr[nexp];
	int64_t *minptr[nexp];

	int64_t dimids[num_of_dims];

	int dim_start[num_of_dims];
	int dim_divider[nexp];

	int tmp_div = 0;
	int tmp_rem = 0;

	int retval;
	esdm_status ret;
	esdm_container_t *container = NULL;
	esdm_dataspace_t *dataspace = NULL, *subspace = NULL, *dimspace[num_of_dims];
	esdm_dataset_t *dataset = NULL, *dimset[num_of_dims];
	esdm_type_t type_dim;

	for (l = 0; l < num_of_dims; l++) {
		dimset[l] = NULL;
		dimspace[l] = NULL;
	}

	if (oph_dc_setup_dbms(&(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_IOPLUGIN_SETUP_ERROR,
			(dbmss.value[0]).id_dbms);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		for (l = 0; l < num_of_dims; l++) {
			if (dim_rows[l]) {
				free(dim_rows[l]);
				dim_rows[l] = NULL;
			}
		}
		free(dim_rows);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For each DBMS
	for (i = 0; i < dbmss.size; i++) {

		if (oph_dc_connect_to_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			for (l = 0; l < num_of_dims; l++) {
				if (dim_rows[l]) {
					free(dim_rows[l]);
					dim_rows[l] = NULL;
				}
			}
			free(dim_rows);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; j < dbs.size; j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				for (l = 0; l < num_of_dims; l++) {
					if (dim_rows[l]) {
						free(dim_rows[l]);
						dim_rows[l] = NULL;
					}
				}
				free(dim_rows);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//For each fragment
			for (k = 0; k < frags.size; k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				// TODO: in case of more fragments...
				if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
					n = snprintf(file_name, sizeof(file_name), "%s", file);
				else
					n = snprintf(file_name, sizeof(file_name), "%s_%d", file,
						     ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_id_start_position + frag_count);

				if (n >= (int) sizeof(file_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_STRING_BUFFER_OVERFLOW, "path", file_name);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				ret = esdm_container_create(file_name, 1, &container);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create output object: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_NC_OUTPUT_FILE_ERROR, "");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				//Assume that every fragment contains an entire set of lower explicit dimensions (ophlevel higher); it means that for each lower dimension the first value is the lower, the last is the higher 

				//Find max values for all explicit dimensions and min value for explicit ophlevel=1 dimension
				memset(dim_val_max, 0, sizeof(dim_val_max));
				memset(dim_val_min, 0, sizeof(dim_val_min));
				memset(dim_val_num, 0, sizeof(dim_val_num));

				memset(maxptr, 0, sizeof(maxptr));
				memset(minptr, 0, sizeof(minptr));

				memset(dimids, 0, sizeof(dimids));

				memset(dim_start, 0, sizeof(dim_start));
				memset(dim_divider, 0, sizeof(dim_divider));

				if (nexp) {
					for (inc = 0; inc < nexp; inc++)
						maxptr[inc] = dim_val_max + inc;
					for (inc = 0; inc < nexp; inc++)
						minptr[inc] = dim_val_min + inc;
					oph_esdm_compute_dimension_id(frags.value[k].key_start, dims_size, nexp, minptr);
					oph_esdm_compute_dimension_id(frags.value[k].key_end, dims_size, nexp, maxptr);
					for (inc = 0; inc < nexp; inc++) {
						if (dim_val_max[inc] < dim_val_min[inc])
							dim_val_num[inc] = dims[inc].dimsize;	// Explicit dimension is wrapped around in the same fragment
						else
							dim_val_num[inc] = 1 + dim_val_max[inc] - dim_val_min[inc];
					}
				}
				// I know that implicit dimensions are at the end of dims array
				for (inc = nexp; inc < num_of_dims; inc++)
					dim_val_num[inc] = dims[inc].dimsize;

				tmp_div = total_size;
				tmp_rem = frags.value[k].key_start - 1;
				for (inc = 0; inc < nexp; inc++) {
					dim_divider[inc] = tmp_div / dims_size[inc];
					tmp_div = dim_divider[inc];
					dim_start[inc] = tmp_rem / dim_divider[inc];
					tmp_rem = tmp_rem % dim_divider[inc];
				}
				for (inc = nexp; inc < num_of_dims; inc++)
					dim_start[inc] = 0;

				retval = 0;
				for (inc = 0; inc < num_of_dims; inc++) {
					if (!dims[inc].dimsize) {
						dims[inc].dimunlimited = 0;
						continue;
					}
					if (retval) {
						if (dims[inc].dimunlimited) {
							dims[inc].dimunlimited = 0;
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to adopt '%s' as unlimited dimension. Move it to the most outer level\n", dims[inc].dimname);
							logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
								"Unable to adopt '%s' as unlimited dimension. Move it to the most outer level\n", dims[inc].dimname);
						}
					} else
						retval = 1;
				}

				for (inc = 0; inc < num_of_dims; inc++) {

					type_dim = SMD_DTYPE_UNKNOWN;
					if (oph_esdm_get_esdm_type(dims[inc].dimtype, &type_dim)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension type not supported\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_VAR_TYPE_NOT_SUPPORTED, data_type);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					ret = esdm_dataspace_create(1, dim_val_num + inc, type_dim, dimspace + inc);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define dataspace: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_NC_DEFINE_VAR_ERROR, "");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = 0; l < inc; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					ret = esdm_dataset_create(container, dims[inc].dimname, dimspace[inc], dimset + inc);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define variable: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_NC_DEFINE_VAR_ERROR, "");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = 0; l < inc; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataspace_destroy(dimspace[inc]);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					names[0] = dims[inc].dimname;
					ret = esdm_dataset_name_dims(dimset[inc], names);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define variable: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_NC_DEFINE_VAR_ERROR, "");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = 0; l < inc; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataspace_destroy(dimspace[inc]);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}

				ret = esdm_dataspace_create(num_of_dims, dimids, type_nc, &dataspace);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define dataspace: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_NC_DEFINE_VAR_ERROR, "");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						esdm_dataset_close(dimset[l]);
						esdm_dataspace_destroy(dimspace[l]);
					}
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				ret = esdm_dataset_create(container, measure_name, dataspace, &dataset);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define variable: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_NC_DEFINE_VAR_ERROR, "");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						esdm_dataset_close(dimset[l]);
						esdm_dataspace_destroy(dimspace[l]);
					}
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				for (inc = 0; inc < num_of_dims; inc++)
					names[inc] = dims[inc].dimname;
				ret = esdm_dataset_name_dims(dataset, names);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to assign name to dimensions\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						"Unable to assign name to dimensions\n");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						esdm_dataset_close(dimset[l]);
						esdm_dataspace_destroy(dimspace[l]);
					}
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->export_metadata)	// Add metadata
				{
					int idmissingvalue = 0;
					if (oph_odb_cube_retrieve_missingvalue(&oDB_slave, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube, &idmissingvalue, NULL)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve missing value id");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to retrieve missing value id");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = 0; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}

					smd_attr_t *attr = NULL;

					if (oph_odb_meta_find_complete_metadata_list
					    (&oDB_slave, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_datacube, NULL, 0, NULL, NULL, NULL, NULL, &read_result)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTESDM_READ_METADATA_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_READ_METADATA_ERROR);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = 0; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
					while ((row = mysql_fetch_row(read_result))) {
						mvariable = row[1];
						mkey = row[2];
#ifdef OPH_ESDM_SKIP_ATTRIBUTES
						if ((!mvariable && !strcmp(mkey, _NC_PROPERTIES)) || (mvariable && !strcmp(mkey, _NC_BOUNDS)))
							continue;
#endif
						mtype = row[3];
						mvalue = row[4];
						retval = ESDM_ERROR;

						if (mvariable) {
							for (l = 0; l < num_of_dims; l++)
								if (!strcmp(mvariable, dims[l].dimname))
									break;
						} else
							l = num_of_dims;

						if (!mvariable || (l < num_of_dims)) {

							if (!strcmp(mtype, OPH_COMMON_METADATA_TYPE_TEXT)) {
								attr = smd_attr_new(mkey, SMD_DTYPE_STRING, mvalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, mvalue);
							} else if (!strcmp(mtype, OPH_COMMON_BYTE_TYPE)) {
								unsigned char svalue = (unsigned char) strtol(mvalue, NULL, 10);
								attr = smd_attr_new(mkey, SMD_DTYPE_UINT8, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							} else if (!strcmp(mtype, OPH_COMMON_SHORT_TYPE)) {
								short svalue = (short) strtol(mvalue, NULL, 10);
								attr = smd_attr_new(mkey, SMD_DTYPE_INT16, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							} else if (!strcmp(mtype, OPH_COMMON_INT_TYPE)) {
								int svalue = (int) strtol(mvalue, NULL, 10);
								attr = smd_attr_new(mkey, SMD_DTYPE_INT32, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							} else if (!strcmp(mtype, OPH_COMMON_LONG_TYPE)) {
								long long svalue = (long long) strtoll(mvalue, NULL, 10);
								attr = smd_attr_new(mkey, SMD_DTYPE_INT64, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							} else if (!strcmp(mtype, OPH_COMMON_FLOAT_TYPE)) {
								float svalue = (float) strtof(mvalue, NULL);
								attr = smd_attr_new(mkey, SMD_DTYPE_FLOAT, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							} else if (!strcmp(mtype, OPH_COMMON_DOUBLE_TYPE)) {
								double svalue = (double) strtod(mvalue, NULL);
								attr = smd_attr_new(mkey, SMD_DTYPE_DOUBLE, &svalue);
								if (idmissingvalue && row[0] && (idmissingvalue == strtol(row[0], NULL, 10)))
									esdm_dataset_set_fill_value(dataset, &svalue);
							}

							if (!attr) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTESDM_WRITE_METADATA_ERROR, mvariable ? mvariable : "", mkey, "");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_WRITE_METADATA_ERROR,
									mvariable ? mvariable : "", mkey, "");
								mysql_free_result(read_result);
								oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server,
											    frags.value[k].db_instance->dbms_instance);
								oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
								oph_odb_stge_free_fragment_list(&frags);
								oph_odb_stge_free_db_list(&dbs);
								oph_odb_stge_free_dbms_list(&dbmss);
								oph_odb_free_ophidiadb(&oDB_slave);
								for (l = 0; l < num_of_dims; l++) {
									esdm_dataset_close(dimset[l]);
									esdm_dataspace_destroy(dimspace[l]);
								}
								esdm_dataset_close(dataset);
								esdm_dataspace_destroy(dataspace);
								esdm_container_close(container);
								for (l = 0; l < num_of_dims; l++) {
									if (dim_rows[l]) {
										free(dim_rows[l]);
										dim_rows[l] = NULL;
									}
								}
								free(dim_rows);
								return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							}

							if (mvariable)
								ret = esdm_dataset_link_attribute(dimset[l], 0, attr);
							else
								ret = esdm_container_link_attribute(container, 0, attr);
						}

						if (ret) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTESDM_WRITE_METADATA_ERROR, mvariable ? mvariable : "", mkey, "");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTESDM_WRITE_METADATA_ERROR, mvariable ? mvariable : "", mkey,
								"");
							mysql_free_result(read_result);
							oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							oph_odb_free_ophidiadb(&oDB_slave);
							for (l = 0; l < num_of_dims; l++) {
								esdm_dataset_close(dimset[l]);
								esdm_dataspace_destroy(dimspace[l]);
							}
							esdm_dataset_close(dataset);
							esdm_dataspace_destroy(dataspace);
							esdm_container_close(container);
							for (l = 0; l < num_of_dims; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						}
					}
					mysql_free_result(read_result);
				}

				ret = 0;
				for (inc = 0; !ret && (inc < num_of_dims); inc++)
					ret = esdm_dataset_commit(dimset[inc]);
				if (!ret)
					ret = esdm_dataset_commit(dataset);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to complete output definitions: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_NC_END_DEFINITION_ERROR, "");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						esdm_dataset_close(dimset[l]);
						esdm_dataspace_destroy(dimspace[l]);
					}
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				ret = esdm_container_commit(container);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to complete output definitions: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_NC_END_DEFINITION_ERROR, "");
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						esdm_dataset_close(dimset[l]);
						esdm_dataspace_destroy(dimspace[l]);
					}
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				// Write dimension values
				for (m = 0; m < num_of_dims; m++) {
					if ((m < nexp) && (dim_val_max[m] < dim_val_min[m])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This cube is too fragmented to be split in different files. Try to merge it\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							"This cube is too fragmented to be split in different files. Try to merge it\n");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = m; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					if (!strcmp(dims[m].dimtype, OPH_COMMON_BYTE_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(char), dimspace[m]);
					else if (!strcmp(dims[m].dimtype, OPH_COMMON_SHORT_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(short), dimspace[m]);
					else if (!strcmp(dims[m].dimtype, OPH_COMMON_INT_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(int), dimspace[m]);
					else if (!strcmp(dims[m].dimtype, OPH_COMMON_LONG_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(long long), dimspace[m]);
					else if (!strcmp(dims[m].dimtype, OPH_COMMON_FLOAT_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(float), dimspace[m]);
					else if (!strcmp(dims[m].dimtype, OPH_COMMON_DOUBLE_TYPE))
						ret = esdm_write(dimset[m], dim_rows[m] + dim_start[m] * sizeof(double), dimspace[m]);
					else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_VAR_TYPE_NOT_SUPPORTED, dims[m].dimtype);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = m; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_VAR_WRITE_ERROR, "");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = m; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					ret = esdm_dataset_commit(dimset[m]);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_VAR_WRITE_ERROR, "");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						for (l = m; l < num_of_dims; l++) {
							esdm_dataset_close(dimset[l]);
							esdm_dataspace_destroy(dimspace[l]);
						}
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					esdm_dataset_close(dimset[m]);
					esdm_dataspace_destroy(dimspace[m]);
				}

				// Read the variable
				if (oph_dc_read_fragment_data
				    (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(frags.value[k]), data_type, compressed, NULL, NULL, NULL, 0, 1, &frag_rows)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_READ_FRAG_ERROR,
						(frags.value[k]).fragment_name);
					oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					esdm_dataspace_destroy(dataspace);
					esdm_dataset_close(dataset);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				if (frag_rows->num_rows < 1) {
					oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
					esdm_dataspace_destroy(dataspace);
					esdm_dataset_close(dataset);
					esdm_container_close(container);
					frag_count++;
					continue;
				}

				if (frag_rows->num_fields != 2) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_MISSING_FIELDS);
					oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				for (inc = 0; inc < num_of_dims; inc++)
					start[inc] = 0;
				for (inc = 0; inc < nexp; inc++)
					count[inc] = 1;
				for (inc = nexp; inc < num_of_dims; inc++)
					count[inc] = dims[inc].dimsize;

				if (oph_ioserver_fetch_row(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
					oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				while ((curr_row->row)) {

					ret = esdm_dataspace_subspace(dataspace, num_of_dims, count, start, &subspace);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set subspace\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "Unable to set subspace\n");
						oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						esdm_dataspace_destroy(subspace);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}

					ret = esdm_write(dataset, curr_row->row[1], subspace);
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTESDM_VAR_WRITE_ERROR, "");
						oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						esdm_dataspace_destroy(subspace);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					esdm_dataspace_destroy(subspace);

					oph_esdm_get_next_id(start, dim_val_num, nexp);
					if (oph_ioserver_fetch_row(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
						oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						esdm_dataset_close(dataset);
						esdm_dataspace_destroy(dataspace);
						esdm_container_close(container);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
				}

				oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);

				ret = esdm_dataset_commit(dataset);
				if (ret) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTESDM_VAR_WRITE_ERROR, "");
					oph_ioserver_free_result(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					esdm_dataset_close(dataset);
					esdm_dataspace_destroy(dataspace);
					esdm_container_close(container);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				esdm_dataset_close(dataset);
				esdm_dataspace_destroy(dataspace);
				esdm_container_close(container);
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
	}
	if (oph_dc_cleanup_dbms(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_IOPLUGIN_CLEANUP_ERROR,
			(dbmss.value[0]).id_dbms);
	}
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);
	for (l = 0; l < num_of_dims; l++) {
		if (dim_rows[l]) {
			free(dim_rows[l]);
			dim_rows[l] = NULL;
		}
	}
	free(dim_rows);

	if (!handle->proc_rank) {

		char jsonbuf[OPH_COMMON_BUFFER_LEN];

		// TODO: output link

		if (oph_json_is_objkey_printable
		    (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_EXPORTESDM)) {
			if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
				snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, "%s%s", OPH_ESDM_PREFIX, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name);
			else
				snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, "%s%s_i for i from 0 to %d", OPH_ESDM_PREFIX, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name,
					 ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->total_fragment_number - 1);
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPORTESDM, "Output File", jsonbuf)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		// ADD FILE TO NOTIFICATION STRING
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s%s;", OPH_IN_PARAM_FILE, OPH_ESDM_PREFIX, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name);
		if (handle->output_string) {
			strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
			free(handle->output_string);
		}
		handle->output_string = strdup(tmp_string);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	esdm_finalize();

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_disconnect_from_ophidiadb(&((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->output_name = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->datacube_input) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->datacube_input);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims) {
		free((ESDM_dim *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->dims = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_EXPORTESDM_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_EXPORTESDM_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

#ifdef OPH_ESDM
	handle->dlh = NULL;
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
