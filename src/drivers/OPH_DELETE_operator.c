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

#include "drivers/OPH_DELETE_operator.h"
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
#include "oph_json_library.h"
#include "oph_driver_procedure_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"

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

	if (!(handle->operator_handle = (OPH_DELETE_operator_handle *) calloc(1, sizeof(OPH_DELETE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_DELETE_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid = NULL;


	//3 - Fill struct with the correct data
	char *value;
	char *datacube_name;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_MISSING_INPUT_PARAMETER, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_DELETE_operator_handle *) handle->operator_handle)->nthread = (unsigned int) strtol(value, NULL, 10);

	//For error checking
	int id_datacube_in[2] = { 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_DELETE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_OPHIDIADB_CONFIGURATION_FILE);

			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_DELETE_OPHIDIADB_CONNECTION_ERROR);

			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		if (oph_pid_parse_pid(datacube_name, &id_datacube_in[1], &id_datacube_in[0], &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_PID_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_NO_INPUT_DATACUBE, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
			/*} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			   pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			   logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			   id_datacube_in[0] = 0;
			   id_datacube_in[1] = 0; */
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_DATACUBE_FOLDER_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_DATACUBE_PERMISSION_ERROR, username);
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
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_NO_INPUT_DATACUBE, datacube_name);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_NO_INPUT_CONTAINER, datacube_name);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_DELETE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_DELETE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0] = 0;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_DELETE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DATACUBE_READ_ERROR);
		} else {
			//Copy fragment id relative index set 
			if (!(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
				oph_odb_cube_free_datacube(&cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_MEMORY_ERROR_INPUT,
					"fragment ids");
			} else {
				oph_odb_cube_free_datacube(&cube);
				strncpy(id_string, ((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			}
		}

		int tot_frag_num = 0;
		if (oph_ids_count_number_of_ids(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids, &tot_frag_num)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_AGGREGATE2_RETREIVE_IDS_ERROR);
		} else {
			//Check that product of ncores and nthread is at most equal to total number of fragments        
			if (((OPH_DELETE_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > (unsigned int) tot_frag_num) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
			}
		}
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_MEMORY_ERROR_INPUT, "fragment ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_DELETE_operator_handle *oper_handle = (OPH_DELETE_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int id_datacube = oper_handle->id_input_datacube;

	int num_threads = (oper_handle->nthread <= (unsigned int) oper_handle->fragment_number ? oper_handle->nthread : (unsigned int) oper_handle->fragment_number);

	int ret = OPH_ANALYTICS_OPERATOR_SUCCESS;
	if ((ret = oph_dproc_delete_data(id_datacube, oper_handle->id_input_container, oper_handle->fragment_ids, 0, 0, num_threads))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
	}

	return ret;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int result = OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Before deleting wait for all process to reach this point
	MPI_Barrier(MPI_COMM_WORLD);

	if (handle->proc_rank == 0) {
		int id_datacube = ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_datacube;
		result = oph_dproc_clean_odb(&((OPH_DELETE_operator_handle *) handle->operator_handle)->oDB, id_datacube, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container);
	}
	//Broadcast to all other processes the operation result       
	MPI_Bcast(&result, 1, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (result != OPH_ANALYTICS_OPERATOR_SUCCESS) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master destroy procedure has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_DELETE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_MASTER_TASK_DESTROY_FAILED);
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_DELETE_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_DELETE_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_DELETE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_DELETE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_DELETE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_DELETE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
