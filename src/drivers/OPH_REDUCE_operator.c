/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#include "drivers/OPH_REDUCE_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <strings.h>
#include <math.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"
#include "oph_driver_procedure_library.h"

#include <pthread.h>

struct _thread_struct {
	OPH_REDUCE_operator_handle *oper_handle;
	unsigned int current_thread;
	unsigned int total_threads;
	int proc_rank;
	oph_odb_fragment_list *frags;
	oph_odb_db_instance_list *dbs;
	oph_odb_dbms_instance_list *dbmss;
	char *_ms;
};
typedef struct _thread_struct thread_struct;

void *exec_thread(void *ts)
{

	OPH_REDUCE_operator_handle *oper_handle = ((thread_struct *) ts)->oper_handle;
	int l = ((thread_struct *) ts)->current_thread;
	int num_threads = ((thread_struct *) ts)->total_threads;
	int proc_rank = ((thread_struct *) ts)->proc_rank;

	int id_datacube_out = oper_handle->id_output_datacube;
	int compressed = oper_handle->compressed;

	oph_odb_fragment_list *frags = ((thread_struct *) ts)->frags;
	oph_odb_db_instance_list *dbs = ((thread_struct *) ts)->dbs;
	oph_odb_dbms_instance_list *dbmss = ((thread_struct *) ts)->dbmss;

	char *_ms = ((thread_struct *) ts)->_ms;

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
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_IOPLUGIN_SETUP_ERROR, (dbmss->value[0]).id_dbms);
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
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
			oph_dc_cleanup_dbms(server);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			break;
		}


		if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[i]))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_DB_SELECTION_ERROR, (dbs->value[i]).db_name);
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
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
				res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			//OPH_REDUCE mysql plugin
			if (compressed)
				n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_REDUCE_PLUGIN_COMPR, oper_handle->measure_type,
					     oper_handle->measure_type, MYSQL_FRAG_MEASURE, oper_handle->operation, oper_handle->size, oper_handle->order, _ms);
			else
				n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_REDUCE_PLUGIN, oper_handle->measure_type,
					     oper_handle->measure_type, MYSQL_FRAG_MEASURE, oper_handle->operation, oper_handle->size, oper_handle->order, _ms);
			if (n >= OPH_COMMON_BUFFER_LEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);
				res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			//OPH_REDUCE fragment
			if (oph_dc_create_fragment_from_query(server, &(frags->value[k]), frag_name_out, operation, 0, 0, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_NEW_FRAG_ERROR, frag_name_out);
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

	if (!(handle->operator_handle = (OPH_REDUCE_operator_handle *) calloc(1, sizeof(OPH_REDUCE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->nthread = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->check_grid = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->order = 2.0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = NAN;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->user_missing_value = 0;
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 0;

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_NTHREAD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_NTHREAD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_ARG_NTHREAD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->nthread = (unsigned int) strtol(value, NULL, 10);

	//For error checking
	int id_datacube_in[3] = { 0, 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_REDUCE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_REDUCE_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_PID_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_NO_INPUT_DATACUBE, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_DATACUBE_FOLDER_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;

		if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_user))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_USER_ID_ERROR);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

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
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, 3, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_REDUCTION_OPERATION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_REDUCTION_OPERATION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_REDUCTION_OPERATION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "operation");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ORDER);
	if (value)
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->order = strtod(value, NULL);
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->order < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong value of parameter '%s'. It should be not negative\n", OPH_IN_PARAM_ORDER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], "Wrong value of parameter '%s'. It should be not negative\n", OPH_IN_PARAM_ORDER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MISSINGVALUE);
	if (value && strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (strncmp(value, OPH_COMMON_NAN, OPH_TP_TASKLEN))
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = strtod(value, NULL);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->user_missing_value = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_REDUCTION_SIZE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_REDUCTION_SIZE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_REDUCTION_SIZE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncasecmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN) || !strncasecmp(value, OPH_COMMON_MAX_VALUE, OPH_TP_TASKLEN)) {
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->size = 0;
	} else if ((((OPH_REDUCE_operator_handle *) handle->operator_handle)->size = (int) strtol(value, NULL, 10)) < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_BAD_PARAMETER, "size", ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "grid name");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_GRID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_GRID);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CHECK_GRID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN))
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->check_grid = 1;

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_job = value ? (int) strtol(value, NULL, 10) : 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "description");
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, stream_max_size = 5 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int) + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[4], *data_type;
	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[3] = stream + pointer;
	pointer += 1 + sizeof(int);
	data_type = stream + pointer;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_REDUCE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DATACUBE_READ_ERROR);
			goto __OPH_EXIT_1;
		}
		// Change the container id
		cube.id_container = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_container;

		((OPH_REDUCE_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		//Copy fragment id relative index set 
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "fragment ids");
			goto __OPH_EXIT_1;
		}
		//Copy measure_type relative index set
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "measure type");
			goto __OPH_EXIT_1;
		}

		int tot_frag_num = 0;
		if (oph_ids_count_number_of_ids(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids, &tot_frag_num)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_RETREIVE_IDS_ERROR);
		} else {
			//Check that product of ncores and nthread is at most equal to total number of fragments        
			if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->nthread * handle->proc_number > (unsigned int) tot_frag_num) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_RESOURCE_CHECK_ERROR);
			}
		}

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int last_insertd_id = 0;
		int l, reduced_dim = -1, reduction = 0;
		int size = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size, residual_size = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size, real_aggregate_set = 1;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_CUBEHASDIM_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			if (cubedims)
				free(cubedims);
			goto __OPH_EXIT_1;
		}
		// Dimension instances
		oph_odb_dimension dim[number_of_dimensions];
		oph_odb_dimension_instance dim_inst[number_of_dimensions];
		for (l = 0; l < number_of_dimensions; ++l) {
			if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}

		for (l = number_of_dimensions - 1; l >= 0; l--) {
			if (!cubedims[l].explicit_dim)	// Implicit dimension
			{
				if (cubedims[l].size) {
					if (!residual_size)	// Every implicit dimension will be collapsed
					{
						real_aggregate_set *= cubedims[l].size;
						cubedims[l].size = cubedims[l].level = 0;
						dim_inst[l].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
					} else if (cubedims[l].size > residual_size) {
						if (residual_size == 1)
							break;
						if (cubedims[l].size % residual_size) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension information with size '%d'\n",
							      ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size);
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_REDUCE_BAD_PARAMETER, "size", ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size);
							oph_odb_cube_free_datacube(&cube);
							free(cubedims);
							goto __OPH_EXIT_1;
						}
						reduced_dim = l;
						reduction = residual_size;
						cubedims[l].size /= residual_size;
						real_aggregate_set *= residual_size;
						dim_inst[l].concept_level = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
						break;
					} else	// This dimension will be collapsed, so there is another size to be OPH_REDUCEd
					{
						if (residual_size % cubedims[l].size) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension information with size '%d'\n",
							      ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size);
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_REDUCE_BAD_PARAMETER, "size", ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size);
							oph_odb_cube_free_datacube(&cube);
							free(cubedims);
							goto __OPH_EXIT_1;
						}
						residual_size /= cubedims[l].size;
						real_aggregate_set *= cubedims[l].size;
						cubedims[l].size = cubedims[l].level = 0;
						dim_inst[l].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
					}
				}
			} else
				break;
		}
		if (!residual_size)
			size = real_aggregate_set;
		if (real_aggregate_set < ((OPH_REDUCE_operator_handle *) handle->operator_handle)->size)
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->size = real_aggregate_set;

		// Begin - Dimension table management

		for (l = 0; l < number_of_dimensions; ++l)
			dim_inst[l].size = cubedims[l].size;

		// Grid management
		int id_grid = 0, new_grid = 0, stored_dim_num = 0, grid_exist = 0;
		oph_odb_dimension *stored_dims = NULL;
		oph_odb_dimension_instance *stored_dim_insts = NULL;

		if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name) {
			if (oph_odb_dim_retrieve_grid_id
			    (oDB, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, &id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading grid id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_GRID_LOAD_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (id_grid) {
				if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
				    (oDB, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
				     &stored_dims, &stored_dim_insts, &stored_dim_num)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension list from grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_REDUCE_DIM_LOAD_FROM_GRID_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					goto __OPH_EXIT_1;
				}
			} else {
				new_grid = 1;
				oph_odb_dimension_grid grid;
				strncpy(grid.grid_name, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				grid.grid_name[OPH_ODB_DIM_GRID_SIZE] = 0;
				if (oph_odb_dim_insert_into_grid_table(oDB, &grid, &id_grid, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while storing grid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_GRID_STORE_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
						"Grid already exists: dimensions will be not associated to a grid.\n");
					id_grid = 0;
				}
			}
		}

		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (stored_dims)
				free(stored_dims);
			if (stored_dim_insts)
				free(stored_dim_insts);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (stored_dims)
				free(stored_dims);
			if (stored_dim_insts)
				free(stored_dim_insts);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (stored_dims)
				free(stored_dims);
			if (stored_dim_insts)
				free(stored_dim_insts);
			goto __OPH_EXIT_1;
		}
		//New fields
		cube.id_source = 0;
		cube.level++;
		if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_REDUCE_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			if (stored_dims)
				free(stored_dims);
			if (stored_dim_insts)
				free(stored_dim_insts);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_user)) {
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			if (stored_dims)
				free(stored_dims);
			if (stored_dim_insts)
				free(stored_dim_insts);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		oph_odb_hierarchy hier;
		char filename[2 * OPH_TP_BUFLEN];

		char dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[1 + OPH_COMMON_BUFFER_LEN];
		snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container);
		char o_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(o_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_container);
		char new_container = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_container != ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container;

		int n, residual_dim_number = 0, d;
		char *dim_row, *cl_value;
		int compressed = 0;
		for (l = 0; l < number_of_dimensions; ++l) {
			if (!dim_inst[l].fk_id_dimension_index) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
				break;
			}
			residual_dim_number++;
			dim_row = 0;
			if (l != reduced_dim)	// This dimension has not been reduced
			{
				strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);
				operation[OPH_COMMON_BUFFER_LEN] = 0;
			} else if (reduction)	// Reduced dimension
			{
				n = snprintf(operation, OPH_COMMON_BUFFER_LEN, MYSQL_DIM_INDEX_ARRAY, OPH_DIM_INDEX_DATA_TYPE, 1, cubedims[l].size);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_STRING_BUFFER_OVERFLOW,
						"MySQL operation name", operation);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].fk_id_dimension_label = 0;
			}
			if (cubedims[l].size)	// Extract the subset only in case the dimension is not collapsed
			{
				if (oph_dim_read_dimension_data(db, dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, compressed, &dim_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_READ_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					goto __OPH_EXIT_1;
				}
			} else {
				dim_inst[l].fk_id_dimension_label = 0;
				if (dim[l].calendar && strlen(dim[l].calendar))	// Time dimension (the check can be improved by checking hierarchy name)
				{
					if (oph_odb_meta_put
					    (oDB, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube, NULL, OPH_ODB_TIME_FREQUENCY, 0, OPH_COMMON_FULL_REDUCED_DIM)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						goto __OPH_EXIT_1;
					}
				}
			}

			if (new_grid || !((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name || new_container) {
				if (oph_dim_insert_into_dimension_table(db, o_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_ROW_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					goto __OPH_EXIT_1;
				}
				dim_inst[l].id_grid = id_grid;
				cl_value = NULL;
				if ((l == reduced_dim) && dim[l].calendar && strlen(dim[l].calendar))	// Time dimension (the check can be improved by checking hierarchy name)
				{
					if (oph_odb_dim_retrieve_hierarchy_from_dimension_of_datacube(oDB, datacube_id, dim[l].dimension_name, &hier, NULL, NULL)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find hierarchy information associated to '%s'\n", dim[l].dimension_name);
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_BAD_PARAMETER,
							dim[l].dimension_name);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						if (cl_value)
							free(cl_value);
						goto __OPH_EXIT_1;
					}
					snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
					if (oph_hier_get_concept_level_long(filename, dim_inst[l].concept_level, &cl_value) || !cl_value) {
						if (cl_value)
							free(cl_value);
						cl_value = strdup(" ");
						cl_value[0] = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
					}
				}
				if (oph_odb_dim_insert_into_dimensioninstance_table
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube, dim[l].dimension_name,
				     cl_value)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_REDUCE_DIM_INSTANCE_STORE_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					if (stored_dims)
						free(stored_dims);
					if (stored_dim_insts)
						free(stored_dim_insts);
					if (cl_value)
						free(cl_value);
					goto __OPH_EXIT_1;
				}
				if (cl_value)
					free(cl_value);
			} else	// Check for grid correctness
			{
				int match = 1;
				for (d = 0; d < stored_dim_num; ++d)
					if (!strcmp(stored_dims[d].dimension_name, dim[l].dimension_name) && !strcmp(stored_dims[d].dimension_type, dim[l].dimension_type)
					    && (stored_dim_insts[d].size == dim_inst[l].size) && (stored_dim_insts[d].concept_level == dim_inst[l].concept_level))
						break;
				//If original dimension is found and has size 0 then do not compare
				if (!((d < stored_dim_num) && !dim_inst[l].size) && ((OPH_REDUCE_operator_handle *) handle->operator_handle)->check_grid) {
					if ((d >= stored_dim_num)
					    || oph_dim_compare_dimension(db, dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, stored_dim_insts[d].fk_id_dimension_index,
									 &match) || match) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This grid cannot be used in this context or error in checking dimension data or metadata\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_REDUCE_DIM_CHECK_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						if (stored_dims)
							free(stored_dims);
						if (stored_dim_insts)
							free(stored_dim_insts);
						goto __OPH_EXIT_1;
					}
				}
				cubedims[l].id_dimensioninst = stored_dim_insts[d].id_dimensioninst;
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

		if (!new_grid && ((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name && (residual_dim_number != stored_dim_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "This grid cannot be used in this context or error in checking dimension data or metadata\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_DIM_CHECK_ERROR);
			free(cubedims);
			goto __OPH_EXIT_1;
		}

		if (id_grid && oph_odb_dim_enable_grid(oDB, id_grid)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to enable grid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, "Unable to enable grid\n");
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		// End - Dimension table management

		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		if (!((OPH_REDUCE_operator_handle *) handle->operator_handle)->user_missing_value) {
			int idmissingvalue = 0;
			if (oph_odb_cube_retrieve_missingvalue(oDB, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube, &idmissingvalue, NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve missing value\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, "Unable to retrieve missing value\n");
				goto __OPH_EXIT_1;
			}
			if (idmissingvalue) {
				char *mtype = NULL, *mvalue = NULL;
				if (oph_odb_meta_retrieve_single_metadata_instance(oDB, idmissingvalue, &mtype, &mvalue) || !mtype || !mvalue) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve missing value\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, "Unable to retrieve missing value\n");
					if (mtype)
						free(mtype);
					if (mvalue)
						free(mvalue);
					goto __OPH_EXIT_1;
				}

				if (!strcmp(mtype, OPH_COMMON_BYTE_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (unsigned char) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_SHORT_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (short) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_INT_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (int) strtol(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_LONG_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (long long) strtoll(mvalue, NULL, 10);
				else if (!strcmp(mtype, OPH_COMMON_FLOAT_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (float) strtof(mvalue, NULL);
				else if (!strcmp(mtype, OPH_COMMON_DOUBLE_TYPE))
					((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms = (double) strtod(mvalue, NULL);
				else if (!strcmp(mtype, OPH_COMMON_METADATA_TYPE_TEXT))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Missing value is a text: skipping\n");

				free(mtype);
				free(mvalue);
			}
		}

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		char _ms[OPH_COMMON_MAX_DOUBLE_LENGHT];
		if (isnan(((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms))
			snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "NULL");
		else
			snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "%f", ((OPH_REDUCE_operator_handle *) handle->operator_handle)->ms);
		if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->compressed)
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_REDUCE_QUERY_COMPR, MYSQL_FRAG_ID, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type,
				 ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation, size,
				 ((OPH_REDUCE_operator_handle *) handle->operator_handle)->order, _ms, MYSQL_FRAG_MEASURE);
		else
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_REDUCE_QUERY, MYSQL_FRAG_ID, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type,
				 ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation, size,
				 ((OPH_REDUCE_operator_handle *) handle->operator_handle)->order, _ms, MYSQL_FRAG_MEASURE);
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_datacube;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_TASK_INSERT_ERROR, new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_REDUCE_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		memcpy(id_string[3], &((OPH_REDUCE_operator_handle *) handle->operator_handle)->size, sizeof(int));

		strncpy(data_type, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
	}
      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MASTER_TASK_INIT_FAILED);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "fragment ids");
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "measure type");
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->size = *((int *) id_string[3]);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_REDUCE_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_REDUCE_operator_handle *oper_handle = (OPH_REDUCE_operator_handle *) handle->operator_handle;

	if (oper_handle->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oper_handle->execute_error = 1;

	int l;

	int num_threads = (oper_handle->nthread <= (unsigned int) oper_handle->fragment_number ? oper_handle->nthread : (unsigned int) oper_handle->fragment_number);
	int res[num_threads];

	char _ms[OPH_COMMON_MAX_DOUBLE_LENGHT];
	if (isnan(oper_handle->ms))
		snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "NULL");
	else
		snprintf(_ms, OPH_COMMON_MAX_DOUBLE_LENGHT, "%f", oper_handle->ms);

	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_OPHIDIADB_CONFIGURATION_FILE);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, oper_handle->id_input_datacube, oper_handle->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
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
		ts[l]._ms = _ms;

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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_REDUCE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	OPH_REDUCE_operator_handle *oper_handle = (OPH_REDUCE_operator_handle *) handle->operator_handle;

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
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid(oper_handle->id_output_container, oper_handle->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, oper_handle->id_input_container, OPH_LOG_OPH_REDUCE_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, oper_handle->id_output_container, oper_handle->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable(oper_handle->objkeys, oper_handle->objkeys_num, OPH_JSON_OBJKEY_REDUCE)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_REDUCE, "Output Cube", jsonbuf)) {
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_REDUCE_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_REDUCE_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation) {
		free((char *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->operation = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name) {
		free((int *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_REDUCE_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_REDUCE_operator_handle *) handle->operator_handle)->description);
		((OPH_REDUCE_operator_handle *) handle->operator_handle)->description = NULL;
	}
	free((OPH_REDUCE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
