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

#define _GNU_SOURCE
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include "drivers/OPH_PUBLISH_operator.h"

#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"

#include <errno.h>

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

void oph_free_vector(char **vect, int nn)
{
	if (!vect || !nn)
		return;
	int ii;
	for (ii = 0; ii < nn; ii++)
		if (vect[ii])
			free(vect[ii]);
	free(vect);
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_PUBLISH_operator_handle *) calloc(1, sizeof(OPH_PUBLISH_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->cached_flag = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_maps_number = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_nums = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_fragment_number = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_id = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_time = 0;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid = NULL;

	char *datacube_name;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys, &((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[2] = { 0, 0 };
	int n;

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_PUBLISH_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_PID_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_NO_INPUT_DATACUBE, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_DATACUBE_FOLDER_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_DATACUBE_PERMISSION_ERROR, username);
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
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_NO_INPUT_DATACUBE, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	//Check if sequential part has been completed
	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_PUBLISH_NO_INPUT_CONTAINER, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_PUBLISH_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PUBLISH2_CONTENT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PUBLISH2_CONTENT);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_PUBLISH2_CONTENT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "macro");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);

	if (!strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_COMMON_ALL_FILTER))
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata = 1;
	else if (!strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_PUBLISH_PUBLISH_METADATA))
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata = -1;
	else if (!strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_PUBLISH_PUBLISH_MAPS) ||
		 !strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) ||
		 !strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK) ||
		 !strcasecmp(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro, OPH_PUBLISH_PUBLISH_MAPS_NCLINK)) {
		n = snprintf(NULL, 0, OPH_FRAMEWORK_MAP_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
			     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube) + 1;
		if (n >= OPH_TP_TASKLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of maps input path exceeded limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
				"maps input path", OPH_FRAMEWORK_MAP_FILES_PATH);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path = (char *) malloc(n * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "input path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		n = snprintf(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path, n, OPH_FRAMEWORK_MAP_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
			     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube);
	}

	n = snprintf(NULL, 0, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
		     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube) + 1;
	if (n >= OPH_TP_TASKLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of html output path exceeded limit.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW, "html output path",
			OPH_FRAMEWORK_HTML_FILES_PATH);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path = (char *) malloc(n * sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "output path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	n = snprintf(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path, n, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
		     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube);

	if (oph_pid_uri()) {
		n = snprintf(NULL, 0, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_uri(), session_code, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
			     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube) + 1;
		if (n >= OPH_TP_TASKLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of html output path exceeded limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
				"html output path", OPH_FRAMEWORK_HTML_FILES_PATH);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link = (char *) malloc(n * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "output path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		n = snprintf(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link, n, OPH_FRAMEWORK_HTML_FILES_PATH, oph_pid_uri(), session_code,
			     ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube);
	}

	if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input = (char *) strndup(datacube_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "datacube name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_ID);
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_ID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_ID);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER, "NO-CONTAINER",
			OPH_IN_PARAM_SHOW_ID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_id = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_INDEX);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER, "NO-CONTAINER",
			OPH_IN_PARAM_SHOW_INDEX);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SHOW_TIME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_time = 1;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	char *macro = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro;

	int flag = 0;
	int i;
	int dimension_array[7][OPH_PUBLISH_MAX_DIMENSION];
	char id_string[4][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	memset(dimension_array, 0, sizeof(dimension_array));

	if (handle->proc_rank == 0) {

		char measure_name[OPH_COMMON_BUFFER_LEN];
		ophidiadb *oDB = &((OPH_PUBLISH_operator_handle *) handle->operator_handle)->oDB;

		if (oph_odb_cube_retrieve_datacube_measure(oDB, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube, measure_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown datacube variable\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DATACUBE_READ_ERROR);
			goto __OPH_EXIT_1;
		}

		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var = (char *) strndup(measure_name, OPH_COMMON_BUFFER_LEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "var");
			goto __OPH_EXIT_1;
		}

		struct stat st;
		char *output_path = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path;

		//in case of DATA
		if (!strcasecmp(macro, OPH_COMMON_ALL_FILTER) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA_NCLINK) ||
		    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK)) {

			oph_odb_datacube cube;
			oph_odb_cube_init_datacube(&cube);

			int datacube_id = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube;

			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DATACUBE_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				goto __OPH_EXIT_1;
			}
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->compressed = cube.compressed;
			//Copy fragment id relative index set
			if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))
			    || !(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT,
					"fragment ids");
				oph_odb_cube_free_datacube(&cube);
				goto __OPH_EXIT_1;
			}

			oph_odb_cubehasdim *cubedims = NULL;
			oph_odb_dimension_instance dim_inst;
			oph_odb_dimension dim;
			int number_of_dimensions = 0;
			//Read cube - dimension relation rows
			if (oph_odb_cube_retrieve_cubehasdim_list(oDB, cube.id_datacube, &cubedims, &number_of_dimensions)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_CUBEHASDIM_READ_ERROR);
				oph_odb_cube_free_datacube(&cube);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			dimension_array[0][0] = number_of_dimensions;
			for (i = 0; i < number_of_dimensions; i++) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[i].id_dimensioninst, &dim_inst, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_PUBLISH_CUBEHASDIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst.id_dimension, &dim, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_PUBLISH_CUBEHASDIM_READ_ERROR);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					goto __OPH_EXIT_1;
				}

				dimension_array[1][i] = cubedims[i].size;
				dimension_array[2][i] = dim_inst.id_dimension;
				dimension_array[3][i] = cubedims[i].explicit_dim;
				dimension_array[4][i] = dim_inst.fk_id_dimension_index;
				dimension_array[5][i] = dim_inst.fk_id_dimension_label;
				if (!strncasecmp(dim.dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_BYTE;
				else if (!strncasecmp(dim.dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_SHORT;
				else if (!strncasecmp(dim.dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_INT;
				else if (!strncasecmp(dim.dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_LONG;
				else if (!strncasecmp(dim.dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_FLOAT;
				else if (!strncasecmp(dim.dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					dimension_array[6][i] = OPH_PUBLISH_DOUBLE;
				else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_TYPE_NOT_SUPPORTED,
						dim.dimension_type);
					oph_odb_cube_free_datacube(&cube);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
		}
		//in case of MAPS
		if (!strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) ||
		    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_NCLINK)) {
			int map_number;
			char *input_path = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path;

			//Get total number of maps
			if (oph_dir_get_num_of_files_in_dir(input_path, &map_number, OPH_DIR_ALL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of maps and relative filenames\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MAPS_FILE_ERROR);
				flag = -1;
				goto __OPH_EXIT_1;
			}
		}
		//Create dir if not exist
		if (stat(output_path, &st)) {
			if (oph_dir_r_mkdir(output_path)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create dir %s\n", output_path);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIR_CREATION_ERROR,
					output_path);
				goto __OPH_EXIT_1;
			}
		}
		//If dir already exists then exit
		else {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Cuboid has been already published %s\n", output_path);
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DATACUBE_PUBLISHED, output_path);
			flag = 1;
			goto __OPH_EXIT_1;
		}

		strncpy(id_string[0], ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);

		//in case of DATA
		if (!strcasecmp(macro, OPH_COMMON_ALL_FILTER) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA_NCLINK) ||
		    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK)) {
			strncpy(id_string[1], ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			strncpy(id_string[2], ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			snprintf(id_string[3], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->compressed);
		}
	}
      __OPH_EXIT_1:

	MPI_Bcast(&flag, 1, MPI_INT, 0, MPI_COMM_WORLD);

	if (flag == 1) {
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->cached_flag = 1;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	MPI_Bcast(id_string, 4 * OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0][0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "var");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	//in case of MAPS
	if (!strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_NCLINK)) {

		if (flag == -1)
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//in case of DATA
	if (!strcasecmp(macro, OPH_COMMON_ALL_FILTER) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA_NCLINK) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK)) {

		if (handle->proc_rank != 0) {
			if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[1], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT,
					"fragment ids");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(id_string[2], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT,
					"measure_type");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->compressed = (int) strtol(id_string[3], NULL, 10);
		}

		MPI_Bcast(dimension_array, 7 * OPH_PUBLISH_MAX_DIMENSION, MPI_INT, 0, MPI_COMM_WORLD);

		if (!dimension_array[0][0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MASTER_TASK_INIT_FAILED);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (dimension_array[0][0] > OPH_PUBLISH_MAX_DIMENSION) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of dimensions is biegger than maximum value allowed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MAX_DIM_ERROR,
				OPH_PUBLISH_MAX_DIMENSION);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num = dimension_array[0][0];

		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim sizes");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim IDs");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim oph types");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim fk ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim fk labels");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types = (int *) malloc(sizeof(int) * ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "dim fk labels");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		for (i = 0; i < ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num; i++) {
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes[i] = dimension_array[1][i];
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids[i] = dimension_array[2][i];
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types[i] = dimension_array[3][i];
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids[i] = dimension_array[4][i];
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels[i] = dimension_array[5][i];
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types[i] = dimension_array[6][i];
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *macro = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro;

	//in case of DATA
	if (!strcasecmp(macro, OPH_COMMON_ALL_FILTER) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA_NCLINK) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK)) {

		int id_number;
		char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

		//Get total number of fragment IDs
		if (oph_ids_count_number_of_ids(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_RETREIVE_IDS_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_fragment_number = id_number;

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
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
		} else {
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
			for (i = handle->proc_rank - 1; i >= 0; i--) {
				if (div_remainder != 0)
					((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
				else
					((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
			}
			if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
				((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
		}

		if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
			goto __OPH_MAPS;

		//Partition fragment relative index string
		char *new_ptr = new_id_string;
		if (oph_ids_get_substring_from_string
		    (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position, fragment_number,
		     &new_ptr)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_ID_STRING_SPLIT_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		free(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids);
		if (!(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "fragment ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
      __OPH_MAPS:
	//in case of MAPS
	if (!strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_NCLINK)) {
		int map_number;
		char *input_path = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path;

		//Get total number of maps
		if (oph_dir_get_num_of_files_in_dir(input_path, &map_number, OPH_DIR_ALL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of maps and relative filenames\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MAPS_FILE_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_maps_number = map_number;

		//All processes compute the map number to work on
		int div_result = (map_number) / (handle->proc_number);
		int div_remainder = (map_number) % (handle->proc_number);

		//Every process must process at least divResult
		int number = div_result;

		if (div_remainder != 0) {
			//Only some certain processes must process an additional part
			if (handle->proc_rank / div_remainder == 0)
				number++;
		}

		int i;
		//Compute map num starting position
		if (number == 0) {
			// In case number of process is higher than map number
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position = -1;
		} else {
			((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position = 0;
			for (i = handle->proc_rank - 1; i >= 0; i--) {
				if (div_remainder != 0)
					((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
				else
					((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position += div_result;
			}
			if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position >= map_number)
				((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position = -1;
		}

		if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position < 0 && handle->proc_rank != 0)
			return OPH_ANALYTICS_OPERATOR_SUCCESS;

		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_nums = number;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *macro = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro;
	char *var = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var;
	char *datacube_input = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input;
	char *output_path = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path;
	int i, j, k, l, n;
	int new_file;
	FILE *fp = NULL;
	char file_name[OPH_COMMON_BUFFER_LEN];
	char img_path[OPH_COMMON_BUFFER_LEN];

	//PUBLISHING PHASE

	//in case of MAPS
	if (!strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_NCLINK)) {

		char *input_path = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path;
		char *link = strstr(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path, "img/") + 4;
		int map_nums = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_nums;
		int map_num_start_position = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_num_start_position;
		int total_maps_number = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_maps_number;

		n = snprintf(img_path, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MAP_FILES_PATH_NO_PREFIX, link);
		if (n >= OPH_COMMON_BUFFER_LEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of maps link path exceeded limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
				"maps input path", img_path);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list = (char **) malloc(total_maps_number * sizeof(char *));
		if (!((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_INPUT, "map list");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		char **map_list = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list;
		for (n = 0; n < total_maps_number; n++) {
			map_list[n] = (char *) malloc(OPH_COMMON_BUFFER_LEN);
			if (!map_list[n]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_STRUCT,
					"map_list");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}

		if (oph_dir_get_files_in_dir(input_path, map_list, OPH_DIR_ALL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of maps and relative filenames\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MAPS_FILE_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		for (i = 0; i < map_nums; i++) {
			n = snprintf(file_name, sizeof(file_name), OPH_PUBLISH_FILE, output_path, map_num_start_position + i);
			if (n >= (int) sizeof(file_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
					"html output path", file_name);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			//check if file already exists
			fp = fopen(file_name, "r");
			if (!fp) {
				new_file = 1;
			} else {
				new_file = 0;
				fclose(fp);
			}

			fp = fopen(file_name, "a");
			if (!fp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open file %s\n", file_name);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_FILE_OPEN_ERROR, file_name);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			//PUBLISH MAP
			if (new_file) {
				if (total_maps_number > 1)
					fprintf(fp, "<!DOCTYPE html>\n <html> \n <head> \n <title> \n Variable: %s Datacube: %s Fragment: %d\n</title>\n</head>\n\n", var, datacube_input,
						map_num_start_position + i);
				else
					fprintf(fp, "<!DOCTYPE html>\n <html> \n <head> \n <title> \n Variable: %s Datacube: %s\n</title>\n</head>\n\n", var, datacube_input);
				fprintf(fp, "<body>\n");
				if (total_maps_number > 1)
					fprintf(fp, "<h1 align=\"center\">\n Variable: <i>%s</i><br/>\n Datacube: <i><a href=\"%s\">%s</a></i>&nbsp;&nbsp;Fragment: <i>%d</i>\n</h1>\n<br/>\n", var,
						datacube_input, datacube_input, map_num_start_position + i);
				else
					fprintf(fp, "<h1 align=\"center\">\n Variable: <i>%s</i><br/>\n Datacube: <i><a href=\"%s\">%s</a></i>\n</h1>\n<br/>\n", var, datacube_input, datacube_input);
				fprintf(fp, "<table border=\"1\" align=\"center\">\n<tr><td>\n&nbsp;");
				for (l = 0; l < total_maps_number; l++) {
					if (l != 0) {
						fprintf(fp, " - ");
					}
					if (l == map_num_start_position + i)
						fprintf(fp, "%d", l);
					else {
						snprintf(file_name, sizeof(file_name), "page%d.html", l);
						fprintf(fp, "<a href=\"%s\">%d</a>", file_name, l);
					}
				}
				fprintf(fp, "&nbsp;\n</td></tr>\n</table>\n<br/>\n");

				//nav links
				fprintf(fp, "<div style=\"text-align:center;\">\n");

				if ((map_num_start_position + i == 0) && (map_num_start_position + i == (total_maps_number - 1))) {
					fprintf(fp,
						"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
						OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
						OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
				} else if (map_num_start_position + i == 0) {
					fprintf(fp,
						"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
						OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, map_num_start_position + i + 1,
						OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_maps_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
				} else if (map_num_start_position + i == (total_maps_number - 1)) {
					fprintf(fp,
						"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
						0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, map_num_start_position + i - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX,
						OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
				} else {
					fprintf(fp,
						"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
						0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, map_num_start_position + i - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, map_num_start_position + i + 1,
						OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_maps_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
				}
				fprintf(fp, "</div>\n");
			}
			fprintf(fp, "<br/> <b>MAP:</b>\n");
			fprintf(fp, "<div style=\"background-image:url('%s/%s');width:1024px;height:565px;background-position:center;margin:0 auto;\"></div>\n",
				input_path, map_list[map_num_start_position + i]);
			fprintf(fp, "<br/><br/>\n");

			fprintf(fp, "<table border=\"1\" align=\"center\">\n<tr><td>\n&nbsp;");
			for (l = 0; l < total_maps_number; l++) {
				if (l != 0) {
					fprintf(fp, " - ");
				}
				if (l == map_num_start_position + i)
					fprintf(fp, "%d", l);
				else {
					snprintf(file_name, sizeof(file_name), OPH_PUBLISH_PAGE, l);
					fprintf(fp, "<a href=\"%s\">%d</a>", file_name, l);
				}
			}
			fprintf(fp, "&nbsp;\n</td></tr>\n</table>\n<br/>\n");

			//nav links
			fprintf(fp, "<div style=\"text-align:center;\">\n");

			if ((map_num_start_position + i == 0) && (map_num_start_position + i == (total_maps_number - 1))) {
				fprintf(fp,
					"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
					OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
					OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
			} else if (map_num_start_position + i == 0) {
				fprintf(fp,
					"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
					OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, map_num_start_position + i + 1, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
					total_maps_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
			} else if (map_num_start_position + i == (total_maps_number - 1)) {
				fprintf(fp,
					"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
					0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, map_num_start_position + i - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
					OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
			} else {
				fprintf(fp,
					"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
					0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, map_num_start_position + i - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, map_num_start_position + i + 1,
					OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_maps_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
			}
			fprintf(fp, "</div>\n");

			fprintf(fp, "<br/><br/>\n");
			fclose(fp);
		}
	}


	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//in case of DATA
	if (!strcasecmp(macro, OPH_COMMON_ALL_FILTER) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_DATA_NCLINK) ||
	    !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA) || !strcasecmp(macro, OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK)) {

		int datacube_id = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube;
		char *data_type = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type;
		int compressed = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->compressed;
		int fragment_id_start_position = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_id_start_position;
		int total_fragment_number = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_fragment_number;
		oph_odb_fragment_list frags;
		oph_odb_db_instance_list dbs;
		oph_odb_dbms_instance_list dbmss;
		int frag_count = 0;
		oph_ioserver_result *frag_rows = NULL;
		oph_ioserver_row *curr_row = NULL;

		//Each process has to be connected to a slave ophidiadb
		ophidiadb oDB_slave;
		oph_odb_init_ophidiadb(&oDB_slave);

		if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONNECTION_ERROR);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		int dim_num = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_num;
		int *dim_sizes = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes;
		int *dim_oph_types = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types;
		int *dim_fk_ids = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids;
		int *dim_fk_labels = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels;
		int *dim_types = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types;
		int *dim_ids = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids;
		oph_odb_dimension *dims = (oph_odb_dimension *) malloc(sizeof(oph_odb_dimension) * dim_num);
		for (i = 0; i < dim_num; i++) {
			if (oph_odb_dim_retrieve_dimension(&oDB_slave, dim_ids[i], &(dims)[i], ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIMENSION_READ_ERROR);
				free(dims);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}

		short int *type_codes = (short int *) malloc(sizeof(short int) * dim_num);
		for (i = 0; i < dim_num; i++) {
			if (!dim_fk_labels[i])
				type_codes[i] = OPH_PUBLISH_LONG;	// Indexes are long
			else if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_time && dims[i].calendar && strlen(dims[i].calendar))
				type_codes[i] = OPH_PUBLISH_TIME;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_BYTE;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_SHORT;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_INT;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_LONG;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_FLOAT;
			else if (!strncasecmp(dims[i].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				type_codes[i] = OPH_PUBLISH_DOUBLE;
			else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_TYPE_NOT_SUPPORTED,
					dims[i].dimension_type);
				free(type_codes);
				free(dims);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}

		char *dimension_index = NULL;
		char dim_index[OPH_COMMON_BUFFER_LEN];
		int m, first = 1, mm;
		n = 0;
		int explicit_dim_number = 0, explicit_dim_total_number = 0;
		//Write ID to index dimension string
		for (i = 0; i < dim_num; i++) {
			if (!dim_oph_types[i])
				continue;
			explicit_dim_total_number++;
			if (!dim_sizes[i])
				continue;
			if (!first)
				n += sprintf(dim_index + n, "|");
			n += sprintf(dim_index + n, "oph_id_to_index(%s", MYSQL_FRAG_ID);
			first = 0;
			for (j = (dim_num - 1); j >= i; j--) {
				if (!dim_sizes[j])
					continue;
				if (dim_oph_types[j] == 0)
					continue;
				n += sprintf(dim_index + n, ",%d", dim_sizes[j]);
			}
			n += sprintf(dim_index + n, ")");
			dimension_index = dim_index;
			explicit_dim_number++;
		}

		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container);
		int exist_flag = 0;

		if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_TABLE_RETREIVE_ERROR,
				index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_TABLE_RETREIVE_ERROR,
				label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//retrieve connection string
		if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, datacube_id, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_CONNECTION_STRINGS_NOT_FOUND);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			free(dims);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char **dim_rows = NULL;
		if (explicit_dim_total_number)
			dim_rows = (char **) malloc(explicit_dim_total_number * sizeof(char *));
		char *tmp_row;
		memset(dim_rows, 0, explicit_dim_number * sizeof(char *));
		char operation[OPH_COMMON_BUFFER_LEN];
		double *dim_d = NULL;
		float *dim_f = NULL;
		int *dim_i = NULL;
		long long *dim_l = NULL;

		for (m = 0; m < explicit_dim_total_number; m++) {
			if (!dim_sizes[m])	// Full reduced dimension
			{
				dim_rows[m] = 0;
				continue;
			}
			n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "%s", MYSQL_DIMENSION);
			if (n >= OPH_COMMON_BUFFER_LEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
					"MySQL operation name", operation);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				free(dims);
				free(dim_rows);
				free(type_codes);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (oph_dim_read_dimension_data(db_dimension, index_dimension_table_name, dim_fk_ids[m], operation, 0, &(dim_rows[m])) || !dim_rows[m]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_READ_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				free(dims);
				for (l = 0; l < explicit_dim_number; l++) {
					if (dim_rows[l]) {
						free(dim_rows[l]);
						dim_rows[l] = NULL;
					}
				}
				free(dim_rows);
				free(type_codes);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if (dim_fk_labels[m]) {
				char dimension_type[1 + OPH_ODB_DIM_DIMENSION_TYPE_SIZE];
				switch (dim_types[m]) {
					case OPH_PUBLISH_TIME:
						strncpy(dimension_type, OPH_COMMON_STRING_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_INT:
						strncpy(dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_DOUBLE:
						strncpy(dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_FLOAT:
						strncpy(dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_LONG:
						strncpy(dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_SHORT:
						strncpy(dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					case OPH_PUBLISH_BYTE:
						strncpy(dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
						break;
					default:
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_PUBLISH_DIM_READ_ERROR);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						free(dims);
						for (l = 0; l < explicit_dim_number; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						free(type_codes);
						oph_odb_free_ophidiadb(&oDB_slave);
						return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
				if (oph_dim_read_dimension_filtered_data(db_dimension, label_dimension_table_name, dim_fk_labels[m], operation, 0, &(dim_rows[m]), dimension_type, dim_sizes[m])
				    || !dim_rows[m]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DIM_READ_ERROR);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					free(dims);
					for (l = 0; l < explicit_dim_number; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					free(type_codes);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			} else
				dim_types[m] = OPH_PUBLISH_LONG;	// Indexes are long
		}
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);

		if (oph_dc_setup_dbms(&(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_IOPLUGIN_SETUP_ERROR,
				(dbmss.value[0]).id_dbms);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			free(dims);
			for (l = 0; l < explicit_dim_number; l++) {
				if (dim_rows[l]) {
					free(dim_rows[l]);
					dim_rows[l] = NULL;
				}
			}
			free(dim_rows);
			free(type_codes);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//LOAD METADATA
		int ii = 0, nn = 0;
		char **mvariable = NULL, **mkey = NULL, **mvalue = NULL;
		if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata) {
			MYSQL_RES *read_result = NULL;
			MYSQL_ROW row;
			if (oph_odb_meta_find_complete_metadata_list(&oDB_slave, datacube_id, NULL, 0, NULL, NULL, NULL, NULL, &read_result)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load metadata.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_READ_METADATA_ERROR);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				free(dims);
				for (l = 0; l < explicit_dim_number; l++) {
					if (dim_rows[l]) {
						free(dim_rows[l]);
						dim_rows[l] = NULL;
					}
				}
				free(dim_rows);
				free(type_codes);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			nn = mysql_num_rows(read_result);
			if (nn) {
				mvariable = (char **) malloc(nn * sizeof(char *));
				mkey = (char **) malloc(nn * sizeof(char *));
				mvalue = (char **) malloc(nn * sizeof(char *));
				while ((row = mysql_fetch_row(read_result))) {
					mvariable[ii] = row[1] ? strdup(row[1]) : NULL;
					mkey[ii] = row[2] ? strdup(row[2]) : NULL;
					mvalue[ii] = row[4] ? strdup(row[4]) : NULL;
					ii++;
				}
			}
			mysql_free_result(read_result);
		}
		oph_odb_free_ophidiadb(&oDB_slave);

		//For each DBMS
		for (i = 0; i < dbmss.size; i++) {

			if (oph_dc_connect_to_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DBMS_CONNECTION_ERROR,
					(dbmss.value[i]).id_dbms);
				oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				free(dims);
				for (l = 0; l < explicit_dim_number; l++) {
					if (dim_rows[l]) {
						free(dim_rows[l]);
						dim_rows[l] = NULL;
					}
				}
				free(dim_rows);
				free(type_codes);
				oph_free_vector(mvariable, nn);
				oph_free_vector(mkey, nn);
				oph_free_vector(mvalue, nn);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//For each DB
			for (j = 0; j < dbs.size; j++) {
				//Check DB - DBMS Association
				if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
					continue;

				if (oph_dc_use_db_of_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_DB_SELECTION_ERROR,
						(dbs.value[j]).db_name);
					oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_fragment_list(&frags);
					free(dims);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					for (l = 0; l < explicit_dim_number; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					free(type_codes);
					oph_free_vector(mvariable, nn);
					oph_free_vector(mkey, nn);
					oph_free_vector(mvalue, nn);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//For each fragment
				for (k = 0; k < frags.size; k++) {
					//Check Fragment - DB Association
					if (frags.value[k].db_instance != &(dbs.value[j]))
						continue;

					n = snprintf(file_name, sizeof(file_name), OPH_PUBLISH_FILE, output_path, fragment_id_start_position + frag_count);
					if (n >= (int) sizeof(file_name)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of output path exceeded limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW, "path", file_name);
						oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						free(dims);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						for (l = 0; l < explicit_dim_number; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						free(type_codes);
						oph_free_vector(mvariable, nn);
						oph_free_vector(mkey, nn);
						oph_free_vector(mvalue, nn);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					//check if file already exists
					fp = fopen(file_name, "r");
					if (!fp) {
						new_file = 1;
					} else {
						new_file = 0;
						fclose(fp);
					}

					fp = fopen(file_name, "a");
					if (!fp) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open file %s\n", file_name);
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_PUBLISH_FILE_OPEN_ERROR, file_name);
						oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						free(dims);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						for (l = 0; l < explicit_dim_number; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						free(type_codes);
						oph_free_vector(mvariable, nn);
						oph_free_vector(mkey, nn);
						oph_free_vector(mvalue, nn);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					//ACTUAL PUBLISH
					if (new_file) {
						//PUBLISH HEADER
						if (total_fragment_number > 1)
							fprintf(fp, "<!DOCTYPE html>\n <html> \n <head> \n <title> \n Variable: %s Datacube: %s Fragment: %d\n</title>\n</head>\n\n", var,
								datacube_input, fragment_id_start_position + frag_count);
						else
							fprintf(fp, "<!DOCTYPE html>\n <html> \n <head> \n <title> \n Variable: %s Datacube: %s\n</title>\n</head>\n\n", var, datacube_input);
						fprintf(fp, "<body>\n");
						if (total_fragment_number > 1)
							fprintf(fp,
								"<h1 align=\"center\">\n Variable: <i>%s</i><br/>\n Datacube: <i><a href=\"%s\">%s</a></i>&nbsp;&nbsp;Fragment: <i>%d</i>\n</h1>\n<br/>\n",
								var, datacube_input, datacube_input, fragment_id_start_position + frag_count);
						else
							fprintf(fp, "<h1 align=\"center\">\n Variable: <i>%s</i><br/>\n Datacube: <i><a href=\"%s\">%s</a></i>\n</h1>\n<br/>\n", var, datacube_input,
								datacube_input);
						if (frags.size > 1) {
							fprintf(fp, "<table border=\"1\" align=\"center\">\n<tr><td>\n&nbsp;");
							for (l = 0; l < total_fragment_number; l++) {
								if (l != 0) {
									fprintf(fp, " - ");
								}
								if (l == fragment_id_start_position + frag_count)
									fprintf(fp, "%d", l);
								else {
									snprintf(file_name, sizeof(file_name), "page%d.html", l);
									fprintf(fp, "<a href=\"%s\">%d</a>", file_name, l);
								}
							}
							fprintf(fp, "&nbsp;\n</td></tr>\n</table>\n<br/>\n");

							//nav links

							fprintf(fp, "<div style=\"text-align:center;\">\n");
							if ((fragment_id_start_position + frag_count == 0) && (fragment_id_start_position + frag_count == (total_fragment_number - 1))) {
								fprintf(fp,
									"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
									OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
									OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
							} else if (fragment_id_start_position + frag_count == 0) {
								fprintf(fp,
									"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
									OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count + 1,
									OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_fragment_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
							} else if (fragment_id_start_position + frag_count == (total_fragment_number - 1)) {
								fprintf(fp,
									"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
									0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count - 1,
									OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
							} else {
								fprintf(fp,
									"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
									0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count - 1,
									OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count + 1, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
									total_fragment_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
							}
							fprintf(fp, "</div>\n");
						}
					}

					if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata) {
						fprintf(fp, "<br/> <b>METADATA:</b> <br/><br/>\n<table border=\"1\" align=\"center\">\n<tr><th>Key</th><th>Value</th></tr>\n");
						for (ii = 0; ii < nn; ++ii)
							fprintf(fp, "<tr><td><b>%s</b>:%s</td><td>%s</td></tr>\n", mvariable[ii] ? mvariable[ii] : "", mkey[ii], mvalue[ii]);
						fprintf(fp, "</table>\n<br/>\n");
					}

					if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata >= 0) {
						//PUBLISH MEASURE
						fprintf(fp, "<br/> <b>MEASURES:</b> <br/><br/><br/>\n");
						fprintf(fp, "<table border=\"1\" align=\"center\">\n <tr>");
						if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_id)
							fprintf(fp, " \n <th>ID</th>\n");
						for (m = 0; m < explicit_dim_total_number; m++) {
							if (!dim_rows[m])
								continue;
							if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
								fprintf(fp, " <th>(index) %s</th> \n", dims[m].dimension_name);
							else
								fprintf(fp, " <th>%s</th> \n", dims[m].dimension_name);
						}
						for (m = 0; m < explicit_dim_total_number; m++) {
							if (dim_rows[m])
								continue;
							if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
								fprintf(fp, " <th>(index) %s</th> \n", dims[m].dimension_name);
							else
								fprintf(fp, " <th>%s</th> \n", dims[m].dimension_name);
						}
						fprintf(fp, " <th>%s</th> \n  </tr>\n", var);

						if (oph_dc_read_fragment_data
						    (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(frags.value[k]), data_type, compressed, dimension_index, NULL, NULL, 0, 0,
						     &frag_rows)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_PUBLISH_READ_FRAG_ERROR, (frags.value[k]).fragment_name);
							oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
							oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							free(dims);
							fclose(fp);
							for (l = 0; l < explicit_dim_number; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							free(type_codes);
							oph_free_vector(mvariable, nn);
							oph_free_vector(mkey, nn);
							oph_free_vector(mvalue, nn);
							return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						}

						if (frag_rows->num_rows < 1) {
							fprintf(fp, "<tr>\n<td colspan=\"2\">Empty set</td>\n</tr>\n");
							oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
							frag_count++;
							fclose(fp);
							continue;
						}

						if (frag_rows->num_fields != (unsigned int) explicit_dim_number + 2) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_PUBLISH_MISSING_FIELDS);
							oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
							oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							free(dims);
							fclose(fp);
							for (l = 0; l < explicit_dim_number; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							free(type_codes);
							oph_free_vector(mvariable, nn);
							oph_free_vector(mkey, nn);
							oph_free_vector(mvalue, nn);
							return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						}

						if (oph_ioserver_fetch_row(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_IOPLUGIN_FETCH_ROW_ERROR);
							oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
							oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							free(dims);
							fclose(fp);
							for (l = 0; l < explicit_dim_number; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							free(type_codes);
							oph_free_vector(mvariable, nn);
							oph_free_vector(mkey, nn);
							oph_free_vector(mvalue, nn);
							return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						}

						while ((curr_row->row)) {
							fprintf(fp, "<tr>\n");
							if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_id)
								fprintf(fp, "<td>%s</td>\n", curr_row->row[0]);
							mm = 1;
							for (m = 1; m < explicit_dim_total_number + 1; m++) {
								if (!dim_rows[m - 1])
									continue;
								tmp_row = dim_rows[m - 1];
								switch (type_codes[m - 1]) {
									case OPH_PUBLISH_TIME:
										{
											char dim_index_time[OPH_COMMON_BUFFER_LEN];
											if (oph_dim_get_time_string_of
											    (tmp_row, ((int) strtol(curr_row->row[mm], NULL, 10) - 1), &(dims[m - 1]), dim_index_time)) {
												pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
												logging(LOG_ERROR, __FILE__, __LINE__,
													((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container,
													OPH_LOG_OPH_PUBLISH_DIM_READ_ERROR);
												oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
												oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server,
															    frags.value[k].db_instance->dbms_instance);
												oph_odb_stge_free_fragment_list(&frags);
												oph_odb_stge_free_db_list(&dbs);
												oph_odb_stge_free_dbms_list(&dbmss);
												free(dims);
												fclose(fp);
												for (l = 0; l < explicit_dim_number; l++)
													if (dim_rows[l]) {
														free(dim_rows[l]);
														dim_rows[l] = NULL;
													}
												free(dim_rows);
												free(type_codes);
												oph_free_vector(mvariable, nn);
												oph_free_vector(mkey, nn);
												oph_free_vector(mvalue, nn);
												return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
											}
											dim_f = (float *) (tmp_row + ((int) strtol(curr_row->row[mm], NULL, 10) - 1) * sizeof(float));
											if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
												fprintf(fp, "<td>(%s) %s</td>\n", curr_row->row[mm], dim_index_time);
											else
												fprintf(fp, "<td>%s</td>\n", dim_index_time);
										}
										break;
									case OPH_PUBLISH_INT:
										dim_i = (int *) (tmp_row + ((int) strtol(curr_row->row[mm], NULL, 10) - 1) * sizeof(int));
										if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
											fprintf(fp, "<td>(%s) %d</td>\n", curr_row->row[mm], *dim_i);
										else
											fprintf(fp, "<td>%d</td>\n", *dim_i);
										break;
									case OPH_PUBLISH_LONG:
										dim_l = (long long *) (tmp_row + ((int) strtol(curr_row->row[mm], NULL, 10) - 1) * sizeof(long long));
										if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
											fprintf(fp, "<td>(%s) %lld</td>\n", curr_row->row[mm], *dim_l);
										else
											fprintf(fp, "<td>%lld</td>\n", *dim_l);
										break;
									case OPH_PUBLISH_FLOAT:
										dim_f = (float *) (tmp_row + ((int) strtol(curr_row->row[mm], NULL, 10) - 1) * sizeof(float));
										if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
											fprintf(fp, "<td>(%s) %f</td>\n", curr_row->row[mm], *dim_f);
										else
											fprintf(fp, "<td>%f</td>\n", *dim_f);
										break;
									default:
										dim_d = (double *) (tmp_row + ((int) strtol(curr_row->row[mm], NULL, 10) - 1) * sizeof(double));
										if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->show_index)
											fprintf(fp, "<td>(%s) %f</td>\n", curr_row->row[mm], *dim_d);
										else
											fprintf(fp, "<td>%f</td>\n", *dim_d);
								}
								mm++;
							}
							for (m = 1; m < explicit_dim_total_number + 1; m++)
								if (!dim_rows[m - 1])
									fprintf(fp, "<td>%s</td>\n", OPH_COMMON_FULL_REDUCED_DIM);
							fprintf(fp, "<td>%s</td>\n", curr_row->row[mm]);
							fprintf(fp, "</tr>\n");

							if (oph_ioserver_fetch_row(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_PUBLISH_IOPLUGIN_FETCH_ROW_ERROR);
								oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);
								oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server,
											    frags.value[k].db_instance->dbms_instance);
								oph_odb_stge_free_fragment_list(&frags);
								oph_odb_stge_free_db_list(&dbs);
								oph_odb_stge_free_dbms_list(&dbmss);
								free(dims);
								fclose(fp);
								for (l = 0; l < explicit_dim_number; l++) {
									if (dim_rows[l]) {
										free(dim_rows[l]);
										dim_rows[l] = NULL;
									}
								}
								free(dim_rows);
								free(type_codes);
								oph_free_vector(mvariable, nn);
								oph_free_vector(mkey, nn);
								oph_free_vector(mvalue, nn);
								return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							}
						}

						fprintf(fp, "</table>\n");
						fprintf(fp, "<br/><br/>\n");
						oph_ioserver_free_result(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, frag_rows);

					}
					//PUBLISH FOOTER
					if (frags.size > 1) {
						fprintf(fp, "<table border=\"1\" align=\"center\">\n<tr><td>\n&nbsp;");
						for (l = 0; l < total_fragment_number; l++) {
							if (l != 0) {
								fprintf(fp, " - ");
							}
							if (l == fragment_id_start_position + frag_count)
								fprintf(fp, "%d", l);
							else {
								snprintf(file_name, sizeof(file_name), "page%d.html", l);
								fprintf(fp, "<a href=\"%s\">%d</a>", file_name, l);
							}
						}
						fprintf(fp, "&nbsp;\n</td></tr>\n</table>\n<br/>\n");

						//nav links
						fprintf(fp, "<div style=\"text-align:center;\">\n");
						if ((fragment_id_start_position + frag_count == 0) && (fragment_id_start_position + frag_count == (total_fragment_number - 1))) {
							fprintf(fp,
								"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
								OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX,
								OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
						} else if (fragment_id_start_position + frag_count == 0) {
							fprintf(fp,
								"<img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
								OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count + 1,
								OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_fragment_number - 1, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
						} else if (fragment_id_start_position + frag_count == (total_fragment_number - 1)) {
							fprintf(fp,
								"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/>&nbsp;&nbsp;<img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/>\n",
								0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX,
								OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
						} else {
							fprintf(fp,
								"<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to first fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to previous fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to next fragment\" style=\"width:30px;height:30px;\"/></a>&nbsp;&nbsp;<a href=\"page%d.html\"><img src=\"%s\" alt=\"Go to last fragment\" style=\"width:30px;height:30px;\"/></a>\n",
								0, OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX, fragment_id_start_position + frag_count - 1, OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX,
								fragment_id_start_position + frag_count + 1, OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX, total_fragment_number - 1,
								OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX);
						}
						fprintf(fp, "</div>\n");
					}
					fprintf(fp, "<br/><br/>\n");

					fclose(fp);
					frag_count++;
				}
			}
			oph_dc_disconnect_from_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
		}

		free(dims);
		for (l = 0; l < explicit_dim_number; l++) {
			if (dim_rows[l]) {
				free(dim_rows[l]);
				dim_rows[l] = NULL;
			}
		}
		free(dim_rows);
		free(type_codes);

		oph_free_vector(mvariable, nn);
		oph_free_vector(mkey, nn);
		oph_free_vector(mvalue, nn);
	}
	//in case of METADATA
	if (!strcasecmp(macro, OPH_PUBLISH_PUBLISH_METADATA) && (handle->proc_rank == 0)) {
		int datacube_id = ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_datacube;

		//Each process has to be connected to a slave ophidiadb
		ophidiadb oDB_slave;
		oph_odb_init_ophidiadb(&oDB_slave);

		if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_OPHIDIADB_CONNECTION_ERROR);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//LOAD METADATA
		int ii = 0, nn = 0;
		char **mvariable = NULL, **mkey = NULL, **mvalue = NULL;
		if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata) {
			MYSQL_RES *read_result = NULL;
			MYSQL_ROW row;
			if (oph_odb_meta_find_complete_metadata_list(&oDB_slave, datacube_id, NULL, 0, NULL, NULL, NULL, NULL, &read_result)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load metadata.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_READ_METADATA_ERROR);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			nn = mysql_num_rows(read_result);
			if (nn) {
				mvariable = (char **) malloc(nn * sizeof(char *));
				mkey = (char **) malloc(nn * sizeof(char *));
				mvalue = (char **) malloc(nn * sizeof(char *));
				while ((row = mysql_fetch_row(read_result))) {
					mvariable[ii] = row[1] ? strdup(row[1]) : NULL;
					mkey[ii] = row[2] ? strdup(row[2]) : NULL;
					mvalue[ii] = row[4] ? strdup(row[4]) : NULL;
					ii++;
				}
			}
			mysql_free_result(read_result);
		}
		oph_odb_free_ophidiadb(&oDB_slave);

		n = snprintf(file_name, sizeof(file_name), OPH_PUBLISH_FILE, output_path, 0);
		if (n >= (int) sizeof(file_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of output path exceeded limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW, "path",
				file_name);
			oph_free_vector(mvariable, nn);
			oph_free_vector(mkey, nn);
			oph_free_vector(mvalue, nn);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//check if file already exists
		fp = fopen(file_name, "r");
		if (!fp) {
			new_file = 1;
		} else {
			new_file = 0;
			fclose(fp);
		}

		fp = fopen(file_name, "a");
		if (!fp) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open file %s\n", file_name);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_FILE_OPEN_ERROR, file_name);
			oph_free_vector(mvariable, nn);
			oph_free_vector(mkey, nn);
			oph_free_vector(mvalue, nn);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (new_file) {
			fprintf(fp, "<!DOCTYPE html>\n <html> \n <head> \n <title> \n Variable: %s Datacube: %s\n</title>\n</head>\n\n", var, datacube_input);
			fprintf(fp, "<body>\n");
			fprintf(fp, "<h1 align=\"center\">\n Variable: <i>%s</i><br/>\n Datacube: <i><a href=\"%s\">%s</a></i>\n</h1>\n<br/>\n", var, datacube_input, datacube_input);

			if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->publish_metadata) {
				fprintf(fp, "<br/> <b>METADATA:</b> <br/><br/>\n<table border=\"1\" align=\"center\">\n<tr><th>Key</th><th>Value</th></tr>\n");
				for (ii = 0; ii < nn; ++ii)
					fprintf(fp, "<tr><td><b>%s</b>:%s</td><td>%s</td></tr>\n", mvariable[ii] ? mvariable[ii] : "", mkey[ii], mvalue[ii]);
				fprintf(fp, "</table>\n<br/>\n");
			}

			fclose(fp);
		}

		oph_free_vector(mvariable, nn);
		oph_free_vector(mkey, nn);
		oph_free_vector(mvalue, nn);
	}
	//COMPLETE html files (macro-independent)
	//sequential
	if (handle->proc_rank == 0) {
		int html_number;
		char **list;

		//Get total number of html pages
		if (oph_dir_get_num_of_files_in_dir(output_path, &html_number, OPH_DIR_ALL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of pages\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NUMBER_PAGES_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		list = (char **) malloc(html_number * sizeof(char *));
		if (!list) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_STRUCT,
				"html file list");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < html_number; i++) {
			list[i] = (char *) malloc(OPH_COMMON_BUFFER_LEN);
			if (!list[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MEMORY_ERROR_STRUCT,
					"html file list");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}

		//Get filenames of html pages
		if (oph_dir_get_files_in_dir(output_path, list, OPH_DIR_ALL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get html filenames\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_HTML_FILENAME_READ_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		for (i = 0; i < html_number; i++) {

			n = snprintf(file_name, sizeof(file_name), "%s/%s", output_path, list[i]);
			if (n >= (int) sizeof(file_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_STRING_BUFFER_OVERFLOW,
					"path", file_name);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			//check if file already exists
			fp = fopen(file_name, "r");
			if (!fp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "File %s does not exist\n", file_name);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_MISSING_FILE, file_name);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			fclose(fp);

			fp = fopen(file_name, "a");
			if (!fp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open file %s\n", file_name);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_FILE_OPEN_ERROR, file_name);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			//close body and html
			fprintf(fp, "</body>\n</html>\n");

			fclose(fp);
		}

		if (list) {
			for (i = 0; i < html_number; i++) {
				if (list[i]) {
					free(list[i]);
					list[i] = NULL;
				}
			}
			free(list);
			list = NULL;
		}
	}

	if (!handle->proc_rank && ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link) {
		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PUBLISH_FILE, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link, 0);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_PUBLISH)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_PUBLISH, "Output URL", jsonbuf)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		// ADD OUTPUT PID TO NOTIFICATION STRING
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_LINK, jsonbuf);
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_PUBLISH_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_PUBLISH_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_PUBLISH_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server)
		oph_dc_cleanup_dbms(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->server);

	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_path = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->output_link = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->input_path = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list) {
		int n;
		for (n = 0; n < ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->total_maps_number; n++) {
			if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list[n]) {
				free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list[n]);
				((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list[n] = NULL;
			}
		}
		free(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->map_list = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->macro = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->var = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_sizes = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_ids = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_oph_types = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_ids = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_fk_labels = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types) {
		free((int *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->dim_types = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys, ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_PUBLISH_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_PUBLISH_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
