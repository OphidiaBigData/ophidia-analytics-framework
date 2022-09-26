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

#include "drivers/OPH_MERGE_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifndef MULTI_NODE_SUPPORT
#include <mpi.h>
#endif

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_ioserver_library.h"

#include <math.h>

#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"
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

	if (!(handle->operator_handle = (OPH_MERGE_operator_handle *) calloc(1, sizeof(OPH_MERGE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 0;

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[3] = { 0, 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_MISSING_INPUT_PARAMETER, OPH_ARG_USERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_user = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_MERGE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_OPHIDIADB_CONFIGURATION_FILE);

			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_OPHIDIADB_CONNECTION_ERROR);

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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_PID_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_NO_INPUT_DATACUBE, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_DATACUBE_FOLDER_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;

		id_datacube_in[2] = id_datacube_in[1];
		if (id_datacube_in[1]) {
			value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
				if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, &id_datacube_in[2])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value);
					id_datacube_in[0] = 0;
					id_datacube_in[1] = 0;
				}
			}
		}
	}
#ifndef MULTI_NODE_SUPPORT
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, 3, MPI_INT, 0, MPI_COMM_WORLD);
#endif
	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_NO_INPUT_CONTAINER, datacube_in);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];
	((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MERGE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MERGE_ON_FRAGMENTS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MERGE_ON_FRAGMENTS);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MERGE_ON_FRAGMENTS);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number = (int) strtol(value, NULL, 10);
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number < 0 || ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number == 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad input parameter %s\n", OPH_IN_PARAM_MERGE_ON_FRAGMENTS);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_BAD_INPUT_PARAMETER, OPH_IN_PARAM_MERGE_ON_FRAGMENTS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_MERGE_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_MERGE_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT, "description");
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	char id_string[3][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0][0] = 0;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_MERGE_operator_handle *) handle->operator_handle)->oDB;

		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube_with_ordered_partitions(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_DATACUBE_READ_ERROR);
			goto __OPH_EXIT_1;
		}
		//Change the datacube info
		int i, j;

		// Change the container id
		cube.id_container = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container;

		oph_odb_fragment_list frags;
		if (oph_odb_stge_retrieve_fragment_list(oDB, datacube_id, &frags)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve fragment keys\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_FRAGMENT_KEYS_ERROR);
			oph_odb_cube_free_datacube(&cube);
			oph_odb_stge_free_fragment_list(&frags);
			goto __OPH_EXIT_1;
		}
		if (!frags.size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "OphidiaDB parameters are corrupted - check 'fragment' table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_FRAGMENT_CORRUPTED);
			oph_odb_cube_free_datacube(&cube);
			oph_odb_stge_free_fragment_list(&frags);
			goto __OPH_EXIT_1;
		}
		//if merge_number is 0 then merge all fragments
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number == 0) {
			((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number = frags.size;
		}
		if (frags.size % ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value '%d'. It should be a sub-multiplier of the number of fragments (%d)\n",
			      ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number, frags.size);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MERGE_NUMBER_ERROR,
				((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number, frags.size);
			oph_odb_cube_free_datacube(&cube);
			oph_odb_stge_free_fragment_list(&frags);
			goto __OPH_EXIT_1;
		}
		//If merging is really executed update fragmentation info
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number > 1) {

			//Save old fragmentation params
			int temp_fragxdb = cube.fragmentxdb;
			int temp_hostxcube = cube.hostxdatacube;
			int *tmp_db_id = NULL;

			//It total merge is performed
			if (frags.size == ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number) {
				int db_id = 0;
				//All fragmentation params are set to one
				cube.fragmentxdb = 1;
				cube.hostxdatacube = 1;

				//New fragment will be stored in the first db available
				db_id = (cube.id_db)[0];

				//Realloc cube partitioned info
				tmp_db_id = (int *) realloc(cube.id_db, 1 * sizeof(int));
				if (tmp_db_id != NULL) {
					cube.id_db = tmp_db_id;
				} else {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"datacube db ids");
					goto __OPH_EXIT_1;
				}
				(cube.id_db)[0] = db_id;
				cube.db_number = 1;

			}
			//If block merge is performed (This section checks the merging number to avoid non-homogeneous fragmentaion)
			else {
				//Compute maximum values for fragmentation params
				cube.fragmentxdb = ceil((double) temp_fragxdb / ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
				//malloc space for new set of db used to partition datacube
				int *new_id_db = NULL;
				if (!(new_id_db = (int *) malloc(temp_hostxcube * sizeof(int)))) {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"database ids");
					goto __OPH_EXIT_1;
				}
				//malloc space for counters of fragment for each db
				int *out_fragxdb;
				if (!(out_fragxdb = (int *) malloc(temp_hostxcube * sizeof(int)))) {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					free(new_id_db);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"output fragxdb counter");
					goto __OPH_EXIT_1;
				}
				memset(out_fragxdb, 0, temp_hostxcube * sizeof(int));
				//malloc space for counters of db for each dbms
				int *out_dbxdbms;
				if (!(out_dbxdbms = (int *) malloc(temp_hostxcube * sizeof(int)))) {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					free(new_id_db);
					free(out_fragxdb);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"output dbxdbms counter");
					goto __OPH_EXIT_1;
				}
				memset(out_dbxdbms, 0, temp_hostxcube * sizeof(int));
				//malloc space for counters of dbms for each host
				int *out_dbmsxhost;
				if (!(out_dbmsxhost = (int *) malloc(temp_hostxcube * sizeof(int)))) {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					free(new_id_db);
					free(out_fragxdb);
					free(out_dbxdbms);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"output dbmsxcube counter");
					goto __OPH_EXIT_1;
				}
				memset(out_dbmsxhost, 0, temp_hostxcube * sizeof(int));

				int frag_to_merge = ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
				j = 0;
				int new_frag_counter = 0;
				int frag_counter = 0;

				//count number of merged fragment per db        
				for (i = 0; i < temp_hostxcube; i++) {
					new_frag_counter = 0;
					j = abs(i * temp_fragxdb - frag_counter);
					if (i == temp_hostxcube - 1)
						temp_fragxdb = abs(frags.size - frag_counter);
					for (; j < temp_fragxdb; j += frag_to_merge) {
						new_frag_counter++;
					}
					out_fragxdb[i] = new_frag_counter;
					frag_counter += new_frag_counter * frag_to_merge;
				}
				//count number of dbs for dbms
				for (i = 0; i < temp_hostxcube; i++) {
					if (out_fragxdb[i]) {
						out_dbxdbms[(int) i]++;
					}
				}
				//count number of dbmss for host
				for (i = 0; i < temp_hostxcube; i++) {
					if (out_dbxdbms[i]) {
						out_dbmsxhost[(int) i]++;
					}
				}

				//Check if fragmentation params are homogeneous
				int wrong_partition_flag = 0;
				int num_dbxdbms = 0;
				int num_dbmsxhost = 0;
				int num_hostxcube = 0;
				for (i = 0; i < temp_hostxcube; i++) {
					//Check only non-empty host
					if (out_dbmsxhost[i]) {
						//If number of dbms x host is 0 set as first value
						if (!num_dbmsxhost)
							num_dbmsxhost = out_dbmsxhost[i];
						//If this host doesn't match number of dbms break
						if (num_dbmsxhost != out_dbmsxhost[i]) {
							wrong_partition_flag = 1;
							break;
						}
						//Check only non-empty dbms
						if (out_dbxdbms[i]) {
							//If number of db x dbms is 0 set as first value
							if (!num_dbxdbms)
								num_dbxdbms = out_dbxdbms[i];
							//If this dbms doesn't match number of db break
							if (num_dbxdbms != out_dbxdbms[i]) {
								wrong_partition_flag = 1;
								break;
							}
						}
						num_hostxcube++;
					}
				}
				//If input fragmentaion is not homogeneous then exit
				if (wrong_partition_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value '%d'. It cannot be used to merge this datacube.\n",
					      ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_BAD_FRAGMENTATION_PARAMS,
						((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					free(new_id_db);
					free(out_fragxdb);
					free(out_dbxdbms);
					free(out_dbmsxhost);
					goto __OPH_EXIT_1;
				}
				//Use computed values
				cube.hostxdatacube = num_hostxcube;

				//Set new partitioned set
				j = 0;
				for (i = 0; i < temp_hostxcube; i++) {
					if (out_fragxdb[i]) {
						new_id_db[j] = (cube.id_db)[i];
						j++;
					}
				}

				//Realloc cube partitioned info
				tmp_db_id = (int *) realloc(cube.id_db, j * sizeof(int));
				if (tmp_db_id != NULL) {
					cube.id_db = tmp_db_id;
				} else {
					oph_odb_cube_free_datacube(&cube);
					oph_odb_stge_free_fragment_list(&frags);
					free(new_id_db);
					free(out_fragxdb);
					free(out_dbxdbms);
					free(out_dbmsxhost);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT,
						"datacube db ids");
					goto __OPH_EXIT_1;
				}

				for (i = 0; i < j; i++) {
					(cube.id_db)[i] = new_id_db[i];
				}
				cube.db_number = j;
				free(new_id_db);
				free(out_fragxdb);
				free(out_dbxdbms);
				free(out_dbmsxhost);
			}
		}
		//Compute supremum value for tuplexfragment
		cube.tuplexfragment = 0;
		int l, tuplexfragment = 0;

		for (l = 0; l < frags.size; ++l) {
			if (frags.value[l].key_start)
				tuplexfragment += frags.value[l].key_end - frags.value[l].key_start + 1;
			if (!((l + 1) % ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number)) {
				if (cube.tuplexfragment < tuplexfragment)
					cube.tuplexfragment = tuplexfragment;
				tuplexfragment = 0;
			}
		}
		if (cube.tuplexfragment < tuplexfragment)
			cube.tuplexfragment = tuplexfragment;
		oph_odb_stge_free_fragment_list(&frags);

		if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT, "input fragment ids");
			goto __OPH_EXIT_1;
		}
		//Get total number of input fragment IDs
		int id_number;
		if (oph_ids_count_number_of_ids(cube.frag_relative_index_set, &id_number)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_RETREIVE_IDS_ERROR, "fragments");
			goto __OPH_EXIT_1;
		}
		//Create output relative id string
		char *tmp = cube.frag_relative_index_set;
		if (oph_ids_create_new_id_string(&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1, ceil((double) id_number / ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment ids string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_CREATE_ID_STRING_ERROR);
			goto __OPH_EXIT_1;
		}
		//New fields
		cube.id_source = 0;
		cube.level++;
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_MERGE_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		//Copy output fragment id relative index set  
		if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT,
				"output fragment ids");
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int last_insertd_id = 0;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_CUBEHASDIM_READ_ERROR);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		// Copy the dimension in case of output has to be saved in a new container
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container != ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container) {
			oph_odb_dimension dim[number_of_dimensions];
			oph_odb_dimension_instance dim_inst[number_of_dimensions];
			for (l = 0; l < number_of_dimensions; ++l) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_LOAD_ERROR);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_CONNECT_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_USE_DB_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container);
			char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(o_index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container);
			snprintf(o_label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container);

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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
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
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
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
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube, dim[l].dimension_name,
				     cl_value)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_INSTANCE_STORE_ERROR);
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
			cubedims[l].id_datacube = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		last_insertd_id = 0;
		//Insert new task REMEBER TO MEMSET query to 0 IF NOT NEEDED
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_job;
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE * sizeof(char));
		if (!(new_task.id_inputcube = (int *) malloc(1 * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube;
		new_task.input_cube_number = 1;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_TASK_INSERT_ERROR, new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		strncpy(id_string[1], ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		snprintf(id_string[2], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube);
	}
      __OPH_EXIT_1:

#ifndef MULTI_NODE_SUPPORT
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_string, 3 * OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0][0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MASTER_TASK_INIT_FAILED);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#endif

	if (handle->proc_rank != 0) {
		if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT, "input fragment ids");
			((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids = (char *) strndup(id_string[1], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT,
				"output fragment ids");
			((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube = (int) strtol(id_string[2], NULL, 10);
		//if merge_number is 0 then merge all fragments
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number == 0) {
			//Get total number of fragment IDs
			if (oph_ids_count_number_of_ids
			    (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids, &((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_RETREIVE_IDS_ERROR);
				((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int out_id_number, in_id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;

//FIRST DISTRIBUTE OUTPUT FRAGMENTS
	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids, &out_id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_RETREIVE_IDS_ERROR, "ouput fragments");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (out_id_number) / (handle->proc_number);
	int div_remainder = (out_id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position = -1;
	} else {
		((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position += div_result;
		}
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position >= out_id_number)
			((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position = -1;
	}

	//If the process is idle then stop
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids, ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position,
	     ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids);
	if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT, "output fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
//THEN COMPUTE INPUT FRAGMENTS TO BE READ 
	if (oph_ids_count_number_of_ids(((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids, &in_id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_RETREIVE_IDS_ERROR, "input fragments");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	double remainder_frag_number =
	    ((double) in_id_number / ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number) -
	    floor((double) in_id_number / ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);

	//Check which process will get the last (smallest) merged fragment
	int fragxproc;
	if (remainder_frag_number != 0) {
		if (out_id_number <= handle->proc_number) {
			if (handle->proc_rank == (out_id_number - 1)) {
				fragxproc =
				    (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number - 1) * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
				fragxproc += round(remainder_frag_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
			} else {
				fragxproc = ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
			}
		} else {
			if (handle->proc_rank == (out_id_number - handle->proc_number - 1)) {
				fragxproc =
				    (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number - 1) * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
				fragxproc += round(remainder_frag_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
			} else {
				fragxproc = ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
			}
		}
	} else {
		fragxproc = ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
	}

	//Every process must process fragxproc
	((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_number = fragxproc;

	//Compute fragment IDs starting position
	int temp_start_position = 0;
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position = -1;
	} else {
		((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			//Output fragment start position
			if (div_remainder != 0)
				temp_start_position = (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				temp_start_position = div_result;

			//Find input fragment start position
			if (remainder_frag_number != 0) {
				if (out_id_number <= handle->proc_number) {
					if (i == (out_id_number - 1)) {
						fragxproc = (temp_start_position - 1) * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
						fragxproc += round(remainder_frag_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
					} else
						fragxproc = temp_start_position * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
				} else {
					if (i == (out_id_number - handle->proc_number - 1)) {
						fragxproc = (temp_start_position - 1) * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
						fragxproc += round(remainder_frag_number * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number);
					} else
						fragxproc = temp_start_position * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
				}
			} else
				fragxproc = temp_start_position * ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number;
			((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position += fragxproc;
		}
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position >= in_id_number)
			((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position = -1;
	}

	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	memset(new_id_string, 0, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
	new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids, ((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position,
	     ((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids);
	if (!(((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MEMORY_ERROR_INPUT, "input fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//if the process is idle then stop
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i, j, k, l;

	int id_datacube_out = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube;
	int id_datacube_in = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_datacube;

	oph_odb_db_instance db_out;
	oph_odb_dbms_instance dbms_out;

	oph_odb_fragment_list frags_in;
	oph_odb_db_instance_list dbs_in;
	oph_odb_dbms_instance_list dbmss_in;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve input connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in, ((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids, &frags_in, &dbs_in, &dbmss_in)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_CONNECTION_STRINGS_NOT_FOUND, "intput datacube");
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_fragment new_frag;

	int frag_count = 0;
	int new_input_frag_count = 0;

	long long first_id, last_id;
	int new_frag_flag = 0;

	short int exec_flag = 0;

	//Precompute total number of rows for each output fragment
	unsigned long long *tot_rows = (unsigned long long *) calloc(((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number, sizeof(unsigned long long));
	if (!tot_rows) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGE_MEMORY_ERROR_HANDLE);
		oph_odb_stge_free_fragment_list(&frags_in);
		oph_odb_stge_free_db_list(&dbs_in);
		oph_odb_stge_free_dbms_list(&dbmss_in);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	l = 0;
	for (k = 0; k < frags_in.size; k++) {
		if (!frags_in.value[k].key_start)
			continue;
		//IF next output fragment, then update flags
		if (new_frag_flag) {
			new_frag_flag = 0;
			new_input_frag_count = 0;
			l++;
		}

		tot_rows[l] += (frags_in.value[k].key_end - frags_in.value[k].key_start + 1);
		new_input_frag_count++;

		if (new_input_frag_count == ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number && !new_frag_flag) {
			new_frag_flag = 1;
		}
		if (l >= ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number)
			break;
	}

	oph_ioserver_query *exec_query = NULL;
	oph_ioserver_query_arg **exec_args = NULL;

	oph_ioserver_handler *output_server = NULL;
	oph_ioserver_handler *input_server = ((OPH_MERGE_operator_handle *) handle->operator_handle)->server;

	if (oph_dc_setup_dbms(&(input_server), (dbmss_in.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_IOPLUGIN_SETUP_ERROR,
			(dbmss_in.value[0]).id_dbms);
		oph_odb_stge_free_fragment_list(&frags_in);
		oph_odb_stge_free_db_list(&dbs_in);
		oph_odb_stge_free_dbms_list(&dbmss_in);
		oph_odb_free_ophidiadb(&oDB_slave);
		free(tot_rows);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	if (oph_dc_setup_dbms(&(output_server), (dbmss_in.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_IOPLUGIN_SETUP_ERROR,
			(dbmss_in.value[0]).id_dbms);
		oph_dc_cleanup_dbms(input_server);
		oph_odb_stge_free_fragment_list(&frags_in);
		oph_odb_stge_free_db_list(&dbs_in);
		oph_odb_stge_free_dbms_list(&dbmss_in);
		oph_odb_free_ophidiadb(&oDB_slave);
		free(tot_rows);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}


	frag_count = 0;
	new_input_frag_count = 0;
	new_frag_flag = 0;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];


	//For each input DBMS
	for (i = 0; i < dbmss_in.size; i++) {
		//START FROM RIGHT POINT
		if (oph_dc_connect_to_dbms(input_server, &(dbmss_in.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_DBMS_CONNECTION_ERROR, "input",
				(dbmss_in.value[i]).id_dbms);
			if (new_frag_flag)
				oph_dc_disconnect_from_dbms(output_server, &dbms_out);
			oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
			oph_dc_cleanup_dbms(output_server);
			oph_dc_cleanup_dbms(input_server);
			oph_odb_stge_free_fragment_list(&frags_in);
			oph_odb_stge_free_db_list(&dbs_in);
			oph_odb_stge_free_dbms_list(&dbmss_in);
			oph_odb_free_ophidiadb(&oDB_slave);
			free(tot_rows);
			//Delete intra append data structures
			if (exec_query)
				oph_ioserver_free_query(output_server, exec_query);
			if (exec_args) {
				for (i = 0; i < 2; i++) {
					if (exec_args[i]) {
						if (exec_args[i]->arg)
							free(exec_args[i]->arg);
						free(exec_args[i]);
					}
				}
				free(exec_args);
			}
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each input DB
		for (j = 0; j < dbs_in.size; j++) {
			//Check DB - DBMS Association
			if (dbs_in.value[j].dbms_instance != &(dbmss_in.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(input_server, &(dbmss_in.value[i]), &(dbs_in.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_DB_SELECTION_ERROR, "intput",
					(dbs_in.value[j]).db_name);
				if (new_frag_flag)
					oph_dc_disconnect_from_dbms(output_server, &dbms_out);
				oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
				oph_dc_cleanup_dbms(output_server);
				oph_dc_cleanup_dbms(input_server);
				oph_odb_stge_free_fragment_list(&frags_in);
				oph_odb_stge_free_db_list(&dbs_in);
				oph_odb_stge_free_dbms_list(&dbmss_in);
				oph_odb_free_ophidiadb(&oDB_slave);
				free(tot_rows);
				//Delete intra append data structures
				if (exec_query)
					oph_ioserver_free_query(output_server, exec_query);
				if (exec_args) {
					for (i = 0; i < 2; i++) {
						if (exec_args[i]) {
							if (exec_args[i]->arg)
								free(exec_args[i]->arg);
							free(exec_args[i]);
						}
					}
					free(exec_args);
				}
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//For each input fragment
			for (k = 0; k < frags_in.size; k++) {
				//Check Fragment - DB Association
				if (frags_in.value[k].db_instance != &(dbs_in.value[j]))
					continue;

				//READ FIRST AND LAST FRAGMENT IDs
				if (!frags_in.value[k].key_start)
					continue;

				if (!new_frag_flag) {
					new_input_frag_count = 0;

					//Copy dbmss_in.value[i] in dbms_out
					strncpy(dbms_out.hostname, dbmss_in.value[i].hostname, OPH_ODB_STGE_HOST_NAME_SIZE);
					dbms_out.hostname[OPH_ODB_STGE_HOST_NAME_SIZE] = 0;
					dbms_out.id_dbms = dbmss_in.value[i].id_dbms;
					strncpy(dbms_out.login, dbmss_in.value[i].login, OPH_ODB_STGE_LOGIN_SIZE);
					dbms_out.login[OPH_ODB_STGE_LOGIN_SIZE] = 0;
					strncpy(dbms_out.pwd, dbmss_in.value[i].pwd, OPH_ODB_STGE_PWD_SIZE);
					dbms_out.pwd[OPH_ODB_STGE_PWD_SIZE] = 0;
					dbms_out.port = dbmss_in.value[i].port;
					//dbms_out.fs_type = dbmss_in.value[i].fs_type;

					//Copy dbs_in.value[j] in db_out
					strncpy(dbms_out.hostname, dbmss_in.value[i].hostname, OPH_ODB_STGE_HOST_NAME_SIZE);
					dbms_out.hostname[OPH_ODB_STGE_HOST_NAME_SIZE] = 0;
					db_out.dbms_instance = &dbms_out;
					db_out.id_dbms = dbs_in.value[j].id_dbms;
					db_out.id_db = dbs_in.value[j].id_db;
					strncpy(db_out.db_name, dbs_in.value[j].db_name, OPH_ODB_STGE_DB_NAME_SIZE);
					db_out.db_name[OPH_ODB_STGE_DB_NAME_SIZE] = 0;

					if (oph_dc_connect_to_dbms(output_server, &(dbms_out), 0)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_MERGE_DBMS_CONNECTION_ERROR, "input", dbms_out.id_dbms);
						oph_dc_disconnect_from_dbms(output_server, &(dbms_out));
						oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
						oph_dc_cleanup_dbms(input_server);
						oph_dc_cleanup_dbms(output_server);
						oph_odb_stge_free_fragment_list(&frags_in);
						oph_odb_stge_free_db_list(&dbs_in);
						oph_odb_stge_free_dbms_list(&dbmss_in);
						oph_odb_free_ophidiadb(&oDB_slave);
						free(tot_rows);
						//Delete intra append data structures
						if (exec_query)
							oph_ioserver_free_query(output_server, exec_query);
						if (exec_args) {
							for (i = 0; i < 2; i++) {
								if (exec_args[i]) {
									if (exec_args[i]->arg)
										free(exec_args[i]->arg);
									free(exec_args[i]);
								}
							}
							free(exec_args);
						}
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}

					if (oph_dc_use_db_of_dbms(output_server, &(dbms_out), &(db_out))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_MERGE_DB_SELECTION_ERROR, "intput", db_out.db_name);
						oph_dc_disconnect_from_dbms(output_server, &(dbms_out));
						oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
						oph_dc_cleanup_dbms(output_server);
						oph_dc_cleanup_dbms(input_server);
						oph_odb_stge_free_fragment_list(&frags_in);
						oph_odb_stge_free_db_list(&dbs_in);
						oph_odb_stge_free_dbms_list(&dbmss_in);
						oph_odb_free_ophidiadb(&oDB_slave);
						free(tot_rows);
						//Delete intra append data structures
						if (exec_query)
							oph_ioserver_free_query(output_server, exec_query);
						if (exec_args) {
							for (i = 0; i < 2; i++) {
								if (exec_args[i]) {
									if (exec_args[i]->arg)
										free(exec_args[i]->arg);
									free(exec_args[i]);
								}
							}
							free(exec_args);
						}
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
					//Set new fragment
					new_frag.id_datacube = id_datacube_out;
					new_frag.id_db = db_out.id_db;
					new_frag.frag_relative_index = ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position + frag_count + 1;
					new_frag.db_instance = &(db_out);

					if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &fragment_name)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_MERGE_STRING_BUFFER_OVERFLOW, "fragment name", new_frag.fragment_name);
						oph_dc_disconnect_from_dbms(output_server, &dbms_out);
						oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
						oph_dc_cleanup_dbms(output_server);
						oph_dc_cleanup_dbms(input_server);
						oph_odb_stge_free_fragment_list(&frags_in);
						oph_odb_stge_free_db_list(&dbs_in);
						oph_odb_stge_free_dbms_list(&dbmss_in);
						oph_odb_free_ophidiadb(&oDB_slave);
						free(tot_rows);
						//Delete intra append data structures
						if (exec_query)
							oph_ioserver_free_query(output_server, exec_query);
						if (exec_args) {
							for (i = 0; i < 2; i++) {
								if (exec_args[i]) {
									if (exec_args[i]->arg)
										free(exec_args[i]->arg);
									free(exec_args[i]);
								}
							}
							free(exec_args);
						}
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					strcpy(new_frag.fragment_name, fragment_name);

					//Create Empty fragment
					if (oph_dc_create_empty_fragment(output_server, &new_frag)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating fragment.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NEW_FRAG_ERROR,
							new_frag.fragment_name);
						oph_dc_disconnect_from_dbms(output_server, &dbms_out);
						oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
						oph_dc_cleanup_dbms(output_server);
						oph_dc_cleanup_dbms(input_server);
						oph_odb_stge_free_fragment_list(&frags_in);
						oph_odb_stge_free_db_list(&dbs_in);
						oph_odb_stge_free_dbms_list(&dbmss_in);
						oph_odb_free_ophidiadb(&oDB_slave);
						free(tot_rows);
						//Delete intra append data structures
						if (exec_query)
							oph_ioserver_free_query(output_server, exec_query);
						if (exec_args) {
							for (i = 0; i < 2; i++) {
								if (exec_args[i]) {
									if (exec_args[i]->arg)
										free(exec_args[i]->arg);
									free(exec_args[i]);
								}
							}
							free(exec_args);
						}
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					}
					new_frag_flag = 1;
				}

				if (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number == 1)
					exec_flag = 3;
				else {
					if (new_input_frag_count == 0)
						exec_flag = 1;
					else {
						if (new_input_frag_count == (((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number - 1))
							exec_flag = 2;
						else
							exec_flag = 0;
					}
				}

				if (oph_dc_append_fragment_to_fragment
				    (input_server, output_server, tot_rows[frag_count], exec_flag, &new_frag, &frags_in.value[k], &first_id, &last_id, &exec_query, &exec_args)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while filling fragment with merged data.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_MERGING_ERROR,
						new_frag.fragment_name, frags_in.value[k].fragment_name);
					oph_dc_disconnect_from_dbms(output_server, &dbms_out);
					oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
					oph_dc_cleanup_dbms(output_server);
					oph_dc_cleanup_dbms(input_server);
					oph_odb_stge_free_fragment_list(&frags_in);
					oph_odb_stge_free_db_list(&dbs_in);
					oph_odb_stge_free_dbms_list(&dbmss_in);
					oph_odb_free_ophidiadb(&oDB_slave);
					free(tot_rows);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				if (new_input_frag_count == 0) {
					//watch out for first id
					new_frag.key_start = first_id;
				}
				new_input_frag_count++;

				if (new_input_frag_count == ((OPH_MERGE_operator_handle *) handle->operator_handle)->merge_number && new_frag_flag) {
					new_frag_flag = 0;

					new_frag.key_end = last_id;

					//Insert new fragment
					if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &new_frag)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_MERGE_FRAGMENT_INSERT_ERROR, new_frag.fragment_name);
						oph_dc_disconnect_from_dbms(output_server, &dbms_out);
						oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
						oph_dc_cleanup_dbms(output_server);
						oph_dc_cleanup_dbms(input_server);
						oph_odb_stge_free_fragment_list(&frags_in);
						oph_odb_stge_free_db_list(&dbs_in);
						oph_odb_stge_free_dbms_list(&dbmss_in);
						oph_odb_free_ophidiadb(&oDB_slave);
						free(tot_rows);
						//Delete intra append data structures
						if (exec_query)
							oph_ioserver_free_query(output_server, exec_query);
						if (exec_args) {
							for (i = 0; i < 2; i++) {
								if (exec_args[i]) {
									if (exec_args[i]->arg)
										free(exec_args[i]->arg);
									free(exec_args[i]);
								}
							}
							free(exec_args);
						}
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					oph_dc_disconnect_from_dbms(output_server, &dbms_out);
					frag_count++;
				}
				if (frag_count == ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number)
					break;
			}
			if (frag_count == ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number)
				break;
		}
		oph_dc_disconnect_from_dbms(input_server, &(dbmss_in.value[i]));
		if (frag_count == ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_number)
			break;
	}
	if (oph_dc_cleanup_dbms(input_server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_IOPLUGIN_CLEANUP_ERROR,
			(dbmss_in.value[0]).id_dbms);
	}
	if (oph_dc_cleanup_dbms(output_server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_IOPLUGIN_CLEANUP_ERROR,
			(dbmss_in.value[0]).id_dbms);
	}

	oph_odb_free_ophidiadb(&oDB_slave);
	oph_odb_stge_free_fragment_list(&frags_in);
	oph_odb_stge_free_db_list(&dbs_in);
	oph_odb_stge_free_dbms_list(&dbmss_in);
	free(tot_rows);

	((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_datacube = ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

#ifndef MULTI_NODE_SUPPORT
	short int proc_error = ((OPH_MERGE_operator_handle *) handle->operator_handle)->execute_error;
	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);
#endif

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_MERGE_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_container,
			 ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_output_datacube);

		if (oph_json_is_objkey_printable
		    (((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_MERGE)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_MERGE, "Output Cube", jsonbuf)) {
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
		if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container,
						   ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids, 0, 0, 1))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
			}
		}
#ifndef MULTI_NODE_SUPPORT
		if (handle->output_code)
			proc_error = (short int) handle->output_code;
		else
			proc_error = OPH_ODB_JOB_STATUS_DESTROY_ERROR;

		MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MIN, MPI_COMM_WORLD);
#endif
		handle->output_code = global_error;

		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_MERGE_operator_handle *) handle->operator_handle)->oDB, id_datacube, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

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
		oph_odb_disconnect_from_ophidiadb(&((OPH_MERGE_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_MERGE_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids) {
		free((char *) ((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->input_fragment_ids = NULL;
	}
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids) {
		free((char *) ((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->output_fragment_ids = NULL;
	}
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_MERGE_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_MERGE_operator_handle *) handle->operator_handle)->description);
		((OPH_MERGE_operator_handle *) handle->operator_handle)->description = NULL;
	}
	free((OPH_MERGE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
