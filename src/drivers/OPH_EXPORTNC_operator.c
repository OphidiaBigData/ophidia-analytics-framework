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

#define _GNU_SOURCE
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "drivers/OPH_EXPORTNC_operator.h"
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

#include <errno.h>

#define OPH_EXPORTNC_DEFAULT_OUTPUT_PATH "default"
#define OPH_EXPORTNC_LOCAL_OUTPUT_PATH "local"

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

	if (!(handle->operator_handle = (OPH_EXPORTNC_operator_handle *) calloc(1, sizeof(OPH_EXPORTNC_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
#ifdef OPH_ZARR
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dlh = oph_nc_dlopen();
#endif

	//1 - Set up struct to empty values
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user_defined = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->export_metadata = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->cached_flag = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->force = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->misc = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->shuffle = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->deflate = 0;
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr = 0;

	char *datacube_name;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, &((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[2] = { 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value, user_space, user_space_default = 0;
	if (oph_pid_get_user_space(&user_space)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read user_space\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read user_space\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_EXPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->export_metadata = 1;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_OPHIDIADB_CONFIGURATION_FILE);

			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_OPHIDIADB_CONNECTION_ERROR);

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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_PID_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_NO_INPUT_DATACUBE, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_DATACUBE_FOLDER_ERROR, datacube_name);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_DATACUBE_PERMISSION_ERROR, username);
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
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_NO_INPUT_DATACUBE, datacube_name);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_EXPORTNC_NO_INPUT_CONTAINER, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];

	//Save datacube PID
	if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->datacube_input = (char *) strdup(datacube_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER,
			OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MISC);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MISC);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MISC);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->misc = 1;

	char *output_path = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_PATH);
	char *output_name = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT_NAME);
	char *output = hashtbl_get(task_tbl, OPH_IN_PARAM_OUTPUT);
	char tmp[1 + (output ? strlen(output) : 0)];
	char home[2];
	home[0] = '/';
	home[1] = 0;

	if (output && strstr(output, OPH_FILE_PREFIX)) {

		output_path = output_name = NULL;
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr = 1;	// file

		output_path = strstr(output, OPH_FILE_PREFIX);
		if (output_path) {
			output_path += strlen(OPH_FILE_PREFIX);
			strcpy(tmp, output_path);
			value = strchr(tmp, OPH_COMMON_DIESIS);
			if (value)
				*value = 0;
			char *pointer = strrchr(tmp, '/');
			while (pointer && (strlen(pointer) <= 1)) {
				*pointer = 0;
				pointer = strrchr(tmp, '/');
			}
			output_name = strdup(pointer ? pointer + 1 : tmp);
			if (pointer) {
				output_path = tmp;
				*pointer = 0;
			} else
				output_path = home;
			output = NULL;
		}
	}

	if (output && strstr(output, OPH_S3_PREFIX)) {

		// TODO: no safety control for Zarr output
		output_path = output_name = NULL;
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr = 2;	// s3
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name = strdup(output);

	} else {

		size_t size;
		if (output && ((size = strlen(output)))) {
			char *pointer = output + size;
			while ((pointer >= output) && (*pointer != '/'))
				pointer--;
			if (pointer < output) {
				output_name = output;
				if ((output[size - 3] == '.') && (output[size - 2] == 'n') && (output[size - 1] == 'c'))
					output[size - 3] = 0;
			} else {
				if (pointer == output)
					output_path = home;
				else
					output_path = output;
				*pointer = 0;
				pointer++;
				if (pointer && *pointer) {
					output_name = pointer;
					if ((output[size - 3] == '.') && (output[size - 2] == 'n') && (output[size - 1] == 'c'))
						output[size - 3] = 0;
				}
			}
		}

		value = output_path;
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_PATH);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_PATH);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		char session_code[OPH_COMMON_BUFFER_LEN];
		oph_pid_get_session_code(hashtbl_get(task_tbl, OPH_ARG_SESSIONID), session_code);

		char *cdd = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
		if (!cdd) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (*cdd != '/') {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", OPH_IN_PARAM_CDD);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if ((strlen(cdd) > 1) && strstr(cdd, "..")) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (!strcmp(value, OPH_EXPORTNC_LOCAL_OUTPUT_PATH) || (user_space && !strcmp(value, OPH_EXPORTNC_DEFAULT_OUTPUT_PATH)))
			value = &user_space_default;
		else if (!user_space && !strcmp(value, OPH_EXPORTNC_DEFAULT_OUTPUT_PATH))
			value = cdd;

		if (!strcmp(value, OPH_EXPORTNC_DEFAULT_OUTPUT_PATH)) {
			if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->misc) {
				value = hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_WORKFLOWID);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				int n = snprintf(NULL, 0, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH "/%s", oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code, value) + 1;
				if (n >= OPH_TP_TASKLEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTNC_STRING_BUFFER_OVERFLOW, "path", OPH_FRAMEWORK_NC_FILES_PATH);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = (char *) malloc(n * sizeof(char)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT,
						"output path");
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				n = snprintf(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path, n, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH "/%s",
					     oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code, value);
				if (oph_pid_uri()) {
					char tmp[OPH_COMMON_BUFFER_LEN];
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_MISCELLANEA_FILES_PATH "/%s", oph_pid_uri(), session_code, value);
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link = strdup(tmp);
				}
			} else {
				int n = snprintf(NULL, 0, OPH_FRAMEWORK_NC_FILES_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code,
						 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube) + 1;
				if (n >= OPH_TP_TASKLEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTNC_STRING_BUFFER_OVERFLOW, "path", OPH_FRAMEWORK_NC_FILES_PATH);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = (char *) malloc(n * sizeof(char)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT,
						"output path");
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				n = snprintf(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path, n, OPH_FRAMEWORK_NC_FILES_PATH,
					     oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
					     ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube);
				if (oph_pid_uri()) {
					char tmp[OPH_COMMON_BUFFER_LEN];
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_NC_FILES_PATH, oph_pid_uri(), session_code,
						 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube);
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link = strdup(tmp);
				}
			}
		} else {
			if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = (char *) strdup(value))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT,
					"output path");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (strstr(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path, "..")) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			char *pointer = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path;
			while (pointer && (*pointer == ' '))
				pointer++;
			if (pointer) {
				char tmp[OPH_COMMON_BUFFER_LEN];
				if ((*pointer != '/') && (strlen(cdd) > 1)) {
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", cdd + 1, pointer);
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user = strdup(tmp);
					free(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path);
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = strdup(tmp);
					pointer = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path;
				}
				if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user)
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user = strdup(pointer);
				if (oph_pid_get_base_src_path(&value)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base user_path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base user path\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
				free(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path);
				((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = strdup(tmp);
				if (value)
					free(value);
			}
			((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user_defined = 1;
		}

		value = output_name;
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OUTPUT_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OUTPUT_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_EXPORTNC_DEFAULT_OUTPUT_PATH) && strcmp(value, OPH_EXPORTNC_LOCAL_OUTPUT_PATH)) {
			if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name = (char *) strdup(value))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT,
					"output name");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}
		int s;
		output_name = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name;
		if (output_name && strncmp(output_name, OPH_ESDM_PREFIX, 7)) {
			for (s = 0; s < (int) strlen(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name); s++) {
				if ((((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name[s] == '/')
				    || (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name[s] == ':')) {
					((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name[s] = '_';
				}
			}
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FORCE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FORCE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_FORCE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->force = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHUFFLE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHUFFLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHUFFLE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE))
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->shuffle = 1;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DEFLATE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DEFLATE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DEFLATE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->deflate = (char) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	char id_string[5][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0][0] = 0;

	char *stream_broad = 0;
	if (handle->proc_rank == 0) {

		MYSQL_RES *dim_rows = NULL;
		MYSQL_ROW row;
		struct stat st;
		char *path = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path;

		ophidiadb *oDB = &((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DATACUBE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->compressed = cube.compressed;
		//Copy fragment id relative index set   
		if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))
		    || !(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))
		    || !(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure = (char *) strndup(cube.measure, OPH_ODB_CUBE_MEASURE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "fragment ids");
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}
		//Set dimensions
		int number_of_dimensions = 0;

		if (oph_odb_dim_find_dimensions_features(oDB, datacube_id, &dim_rows, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIMENSION_FEATURES_ERROR);
			oph_odb_cube_free_datacube(&cube);
			goto __OPH_EXIT_1;
		}

		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims = number_of_dimensions;
		int i = 0, j = 0;
		char stream[number_of_dimensions][OPH_DIM_STREAM_ELEMENTS][OPH_DIM_STREAM_LENGTH];
		while ((row = mysql_fetch_row(dim_rows))) {
			if (i == number_of_dimensions) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_INTERNAL_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR, "stream broad");
			oph_odb_cube_free_datacube(&cube);
			mysql_free_result(dim_rows);
			goto __OPH_EXIT_1;
		}
		memcpy(stream_broad, stream, (size_t) (number_of_dimensions * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char)));

		oph_odb_cube_free_datacube(&cube);
		mysql_free_result(dim_rows);

		//Create dir if not exist
		if (stat(path, &st)) {
			if (errno == EACCES) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTNC_PERMISSION_ERROR, path);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_PERMISSION_ERROR, path);
				free(stream_broad);
				goto __OPH_EXIT_1;
			} else if ((errno != ENOENT) || oph_dir_r_mkdir(path)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTNC_DIR_CREATION_ERROR, path);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIR_CREATION_ERROR, path);
			}
		}
		//If dir already exists then exit
		else {
			if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->force && !(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user_defined)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTNC_DATACUBE_EXPORTED);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DATACUBE_EXPORTED);
				id_string[0][0] = -1;
				free(stream_broad);
				goto __OPH_EXIT_1;
			}
		}

		strncpy(id_string[0], ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		strncpy(id_string[1], ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		strncpy(id_string[2], ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		snprintf(id_string[3], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->compressed);
		snprintf(id_string[4], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims);
	}
      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index 
	MPI_Bcast(id_string, 5 * OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0][0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else if (id_string[0][0] == -1) {
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->cached_flag = 1;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "fragment ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure = (char *) strndup(id_string[1], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "measure name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(id_string[2], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "measure type");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->compressed = (int) strtol(id_string[3], NULL, 10);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims = (int) strtol(id_string[4], NULL, 10);
		stream_broad = (char *) malloc(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char));
		if (!stream_broad) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR, "stream broad");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(stream_broad, 0, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH * sizeof(char));
	}

	MPI_Bcast(stream_broad, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);

	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims = (NETCDF_dim *) malloc(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims * sizeof(NETCDF_dim));
	if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "NETCDF dimensions");
		free(stream_broad);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	NETCDF_dim *dims = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims;
	int i;
	for (i = 0; i < ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims; i++) {
		dims[i].dimid = (int) strtol(stream_broad + (i * OPH_DIM_STREAM_ELEMENTS * OPH_DIM_STREAM_LENGTH), NULL, 10);
		strncpy(dims[i].dimname, stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 1) * OPH_DIM_STREAM_LENGTH), OPH_DIM_STREAM_LENGTH * sizeof(char));
		if (oph_nc_get_nc_type(stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 2) * OPH_DIM_STREAM_LENGTH), &(dims[i].dimtype))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_VAR_TYPE_NOT_SUPPORTED,
				stream_broad + ((i * OPH_DIM_STREAM_ELEMENTS + 2) * OPH_DIM_STREAM_LENGTH));
			free(stream_broad);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

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

	if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name) {
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name =
		    (char *) malloc(strlen(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure) + OPH_COMMON_MAX_INT_LENGHT + 2);
		if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "output name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		sprintf(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name, "%s_%d", ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure,
			((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number = id_number;

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
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position, fragment_number,
	     &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->cached_flag)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, j, k, inc;

	int datacube_id = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube;
	char *path = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path;
	char *file = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name;
	char *measure_name = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure;
	char *data_type = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type;
	int compressed = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->compressed;
	int num_of_dims = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->num_of_dims;
	NETCDF_dim *dims = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims;

	if (!file)
		file = measure_name;

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

	// Get nc_type of variable
	nc_type type_nc = 0;
	if (oph_nc_get_nc_type(data_type, &type_nc)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_VAR_TYPE_NOT_SUPPORTED, data_type);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_OPHIDIADB_CONFIGURATION_FILE);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, datacube_id, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_CONNECTION_STRINGS_NOT_FOUND);
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_LOAD);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_CONNECT);
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_USE_DB);
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
	snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container);
	snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container);
	int exist_flag = 0;

	if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_TABLE_RETREIVE_ERROR,
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_USE_DB);
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_TABLE_RETREIVE_ERROR,
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_USE_DB);
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
	int varidp;

	for (m = 0; m < num_of_dims; m++) {
		n = snprintf(operation, OPH_COMMON_BUFFER_LEN, "%s", MYSQL_DIMENSION);
		if (n >= OPH_COMMON_BUFFER_LEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_STRING_BUFFER_OVERFLOW,
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
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_READ_ERROR);
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
			char dimtype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1];
			switch (dims[m].dimtype) {
				case NC_BYTE:
				case NC_CHAR:
					strncpy(dimtype, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				case NC_SHORT:
					strncpy(dimtype, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				case NC_INT:
					strncpy(dimtype, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				case NC_INT64:
					strncpy(dimtype, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				case NC_FLOAT:
					strncpy(dimtype, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				case NC_DOUBLE:
					strncpy(dimtype, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_READ_ERROR);
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
			dimtype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE] = 0;
			if (oph_dim_read_dimension_filtered_data(db_dimension, label_dimension_table_name, dims[m].dimfklabel, operation, 0, &(dim_rows[m]), dimtype, dims[m].dimsize) || !dim_rows[m]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve dimension data\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DIM_READ_ERROR);
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
		} else
			dims[m].dimtype = NC_INT64;
	}
	oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
	oph_dim_unload_dim_dbinstance(db_dimension);

	size_t start_dim[1] = { 0 };
	size_t count_dim[1] = { 1 };
	size_t start[num_of_dims];
	size_t count[num_of_dims];

	size_t dim_val_max[nexp];
	size_t dim_val_min[nexp];
	int dim_start[num_of_dims];
	int dim_divider[nexp];
	unsigned int dim_val_num[num_of_dims];

	size_t *maxptr[nexp];
	size_t *minptr[nexp];

	int dimids[num_of_dims];
	int vardimsids[num_of_dims];

	int tmp_div = 0;
	int tmp_rem = 0;

	if (oph_dc_setup_dbms(&(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_IOPLUGIN_SETUP_ERROR,
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

		if (oph_dc_connect_to_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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

			if (oph_dc_use_db_of_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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

			int retval, ncid, cmode = NC_NETCDF4;
			if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->force)
				cmode |= NC_NOCLOBBER;
			//For each fragment
			for (k = 0; k < frags.size; k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				if (!strncmp(file, OPH_ESDM_PREFIX, 7)) {

					if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
						n = snprintf(file_name, sizeof(file_name), "%s", file);
					else
						n = snprintf(file_name, sizeof(file_name), "%s_%d", file,
							     ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position + frag_count);

				} else if (!strncmp(file, OPH_S3_PREFIX, 7)) {

					if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
						n = snprintf(file_name, sizeof(file_name), "%s", file);
					else {
						char _file[1 + strlen(file)];
						strcpy(_file, file);
						char *value = strchr(_file, OPH_COMMON_DIESIS);
						if (value) {
							*value = 0;
							value++;
						}
						n = snprintf(file_name, sizeof(file_name), "%s_%d%s%s", _file,
							     ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position + frag_count, value ? "#" : "", value ? value : "");
					}

				} else if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr) {

					if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
						n = snprintf(file_name, sizeof(file_name), "%s%s/%s#mode=nczarr", OPH_FILE_PREFIX, path, file);
					else
						n = snprintf(file_name, sizeof(file_name), "%s%s/%s_%d#mode=nczarr", OPH_FILE_PREFIX, path, file,
							     ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position + frag_count);

				} else {

					if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number == 1)
						n = snprintf(file_name, sizeof(file_name), OPH_EXPORTNC_OUTPUT_PATH_SINGLE_FILE, path, file,
							     strstr(file, OPH_EXPORTNC_OUTPUT_FILE_EXT) ? "" : OPH_EXPORTNC_OUTPUT_FILE_EXT);
					else
						n = snprintf(file_name, sizeof(file_name), OPH_EXPORTNC_OUTPUT_PATH_MORE_FILES, path, file,
							     ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_id_start_position + frag_count);

				}

				if (n >= (int) sizeof(file_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of path exceeded limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTNC_STRING_BUFFER_OVERFLOW, "path", file_name);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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

				if ((retval = nc_create(file_name, cmode, &ncid))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create nc output file %s: %s\n", file_name, nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTNC_NC_OUTPUT_FILE_ERROR, nc_strerror(retval));
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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
				memset(dim_start, 0, sizeof(dim_start));
				memset(dim_divider, 0, sizeof(dim_divider));
				memset(dim_val_num, 0, sizeof(dim_val_num));

				memset(maxptr, 0, sizeof(maxptr));
				memset(minptr, 0, sizeof(minptr));

				memset(dimids, 0, sizeof(dimids));
				memset(vardimsids, 0, sizeof(vardimsids));

				if (nexp) {
					for (inc = 0; inc < nexp; inc++)
						maxptr[inc] = &(dim_val_max[inc]);

					for (inc = 0; inc < nexp; inc++)
						minptr[inc] = &(dim_val_min[inc]);
					oph_nc_compute_dimension_id(frags.value[k].key_start, dims_size, nexp, minptr);
					oph_nc_compute_dimension_id(frags.value[k].key_end, dims_size, nexp, maxptr);
					for (inc = 0; inc < nexp; inc++) {
						if (dim_val_max[inc] < dim_val_min[inc])
							dim_val_num[inc] = dims[inc].dimsize;	// Explicit dimension is wrapped around in the same fragment
						else
							dim_val_num[inc] = dim_val_max[inc] - dim_val_min[inc] + 1;
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
					dim_start[inc] = (int) (tmp_rem) / dim_divider[inc];
					tmp_rem = (int) (tmp_rem) % dim_divider[inc];
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
							logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
								"Unable to adopt '%s' as unlimited dimension. Move it to the most outer level\n", dims[inc].dimname);
						}
					} else
						retval = 1;
				}
				for (inc = 0; inc < num_of_dims; inc++) {
					if ((retval = nc_def_dim(ncid, dims[inc].dimname, dims[inc].dimunlimited ? NC_UNLIMITED : dim_val_num[inc], &dimids[inc]))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define dimensions %s: %s\n", dims[inc].dimname, nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_NC_DEFINE_DIM_ERROR, dims[inc].dimname, nc_strerror(retval));
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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
						nc_close(ncid);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					//Also define coordinate varible associated to dimensions
					if ((retval = nc_def_var(ncid, dims[inc].dimname, dims[inc].dimtype, 1, &dimids[inc], &vardimsids[inc]))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define variable: %s\n", nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_NC_DEFINE_VAR_ERROR, nc_strerror(retval));
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
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

				int varid;
				if ((retval = nc_def_var(ncid, measure_name, type_nc, num_of_dims, dimids, &varid))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to define variable: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NC_DEFINE_VAR_ERROR,
						nc_strerror(retval));
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					nc_close(ncid);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->shuffle || ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->deflate) {
					size_t chunksize[num_of_dims];
					for (inc = 0; inc < nexp; inc++)
						chunksize[inc] = 1;
					for (inc = nexp; inc < num_of_dims; inc++)
						chunksize[inc] = dims[inc].dimsize;
					if ((retval = nc_def_var_chunking(ncid, varid, NC_CHUNKED, chunksize))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set chunking configuration for the variable: %s\n", nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_NC_DEFINE_VAR_ERROR, nc_strerror(retval));
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					char shuffle = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->shuffle;
					char deflate = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->deflate;
					if ((retval = nc_def_var_deflate(ncid, varid, shuffle ? NC_SHUFFLE : NC_NOSHUFFLE, deflate ? 1 : 0, (int) deflate))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set compression configuration for the variable: %s\n", nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_NC_DEFINE_VAR_ERROR, nc_strerror(retval));
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
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

				if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->export_metadata)	// Add metadata
				{
					if (oph_odb_meta_find_complete_metadata_list
					    (&oDB_slave, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_datacube, NULL, 0, NULL, NULL, NULL, NULL, &read_result)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTNC_READ_METADATA_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_READ_METADATA_ERROR);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
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
#ifdef OPH_NC_SKIP_ATTRIBUTES
						if ((!mvariable && !strcmp(mkey, OPH_NC_PROPERTIES)) || (mvariable && !strcmp(mkey, OPH_NC_BOUNDS)))
							continue;
#endif
						mtype = row[3];
						mvalue = row[4];
						retval = NC_EBADTYPE;
						if (mvariable && ((retval = nc_inq_varid(ncid, mvariable, &varidp)))) {
							if (retval == NC_ENOTVAR)
								retval = NC_NOERR;	// Skip metadata associated with collapsed variables
						} else if (!strcmp(mtype, OPH_COMMON_METADATA_TYPE_TEXT))
							retval = nc_put_att_text(ncid, mvariable ? varidp : NC_GLOBAL, mkey, strlen(mvalue), mvalue);
						else if (!strcmp(mtype, OPH_COMMON_BYTE_TYPE)) {
							unsigned char svalue = (unsigned char) strtol(mvalue, NULL, 10);
							retval = nc_put_att_uchar(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_BYTE, 1, &svalue);
						} else if (!strcmp(mtype, OPH_COMMON_SHORT_TYPE)) {
							short svalue = (short) strtol(mvalue, NULL, 10);
							retval = nc_put_att_short(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_SHORT, 1, &svalue);
						} else if (!strcmp(mtype, OPH_COMMON_INT_TYPE)) {
							int svalue = (int) strtol(mvalue, NULL, 10);
							retval = nc_put_att_int(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_INT, 1, &svalue);
						} else if (!strcmp(mtype, OPH_COMMON_LONG_TYPE)) {
							long long svalue = (long long) strtoll(mvalue, NULL, 10);
							retval = nc_put_att_longlong(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_INT64, 1, &svalue);
						} else if (!strcmp(mtype, OPH_COMMON_FLOAT_TYPE)) {
							float svalue = (float) strtof(mvalue, NULL);
							retval = nc_put_att_float(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_FLOAT, 1, &svalue);
						} else if (!strcmp(mtype, OPH_COMMON_DOUBLE_TYPE)) {
							double svalue = (double) strtod(mvalue, NULL);
							retval = nc_put_att_double(ncid, mvariable ? varidp : NC_GLOBAL, mkey, NC_DOUBLE, 1, &svalue);
						}
						if (retval) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPORTNC_WRITE_METADATA_ERROR, mvariable ? mvariable : "", mkey, nc_strerror(retval));
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_WRITE_METADATA_ERROR, mvariable ? mvariable : "", mkey,
								nc_strerror(retval));
							mysql_free_result(read_result);
							oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							oph_odb_free_ophidiadb(&oDB_slave);
							nc_close(ncid);
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

				if ((retval = nc_enddef(ncid))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to complete output nc definition: %s\n", nc_strerror(retval));
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_EXPORTNC_NC_END_DEFINITION_ERROR, nc_strerror(retval));
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					nc_close(ncid);
					for (l = 0; l < num_of_dims; l++) {
						if (dim_rows[l]) {
							free(dim_rows[l]);
							dim_rows[l] = NULL;
						}
					}
					free(dim_rows);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				for (m = 0; m < num_of_dims; m++) {
					if ((m < nexp) && (dim_val_max[m] < dim_val_min[m])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "This cube is too fragmented to be split in different files. Try to merge it\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							"This cube is too fragmented to be split in different files. Try to merge it\n");
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}

					retval = 1;
					count_dim[0] = dim_val_num[m];
					start_dim[0] = 0;
					switch (dims[m].dimtype) {
						case NC_BYTE:
						case NC_CHAR:
							retval = nc_put_vara_uchar(ncid, vardimsids[m], start_dim, count_dim, (unsigned char *) (dim_rows[m] + dim_start[m] * sizeof(char)));
							break;
						case NC_SHORT:
							retval = nc_put_vara_short(ncid, vardimsids[m], start_dim, count_dim, (short *) (dim_rows[m] + dim_start[m] * sizeof(short)));
							break;
						case NC_INT:
							retval = nc_put_vara_int(ncid, vardimsids[m], start_dim, count_dim, (int *) (dim_rows[m] + dim_start[m] * sizeof(int)));
							break;
						case NC_INT64:
							retval = nc_put_vara_longlong(ncid, vardimsids[m], start_dim, count_dim, (long long *) (dim_rows[m] + dim_start[m] * sizeof(long long)));
							break;
						case NC_FLOAT:
							retval = nc_put_vara_float(ncid, vardimsids[m], start_dim, count_dim, (float *) (dim_rows[m] + dim_start[m] * sizeof(float)));
							break;
						case NC_DOUBLE:
							retval = nc_put_vara_double(ncid, vardimsids[m], start_dim, count_dim, (double *) (dim_rows[m] + dim_start[m] * sizeof(double)));
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_EXPORTNC_VAR_TYPE_NOT_SUPPORTED, dims[m].dimtype);
							oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							oph_odb_free_ophidiadb(&oDB_slave);
							nc_close(ncid);
							for (l = 0; l < num_of_dims; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					if (retval) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_VAR_WRITE_ERROR, nc_strerror(retval));
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
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

				if (oph_dc_read_fragment_data
				    (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(frags.value[k]), data_type, compressed, NULL, NULL, NULL, 0, 1, &frag_rows)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_READ_FRAG_ERROR,
						(frags.value[k]).fragment_name);
					oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					nc_close(ncid);
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
					oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
					nc_close(ncid);
					frag_count++;
					continue;
				}

				if (frag_rows->num_fields != 2) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_MISSING_FIELDS);
					oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
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
					nc_close(ncid);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				for (inc = 0; inc < num_of_dims; inc++)
					start[inc] = 0;
				for (inc = 0; inc < nexp; inc++)
					count[inc] = 1;
				for (inc = nexp; inc < num_of_dims; inc++)
					count[inc] = dims[inc].dimsize;

				if (oph_ioserver_fetch_row(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
					oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
					oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					nc_close(ncid);
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
					retval = 1;
					switch (type_nc) {
						case NC_BYTE:
						case NC_CHAR:
							retval = nc_put_vara_uchar(ncid, varid, start, count, (unsigned char *) (curr_row->row[1]));
							break;
						case NC_SHORT:
							retval = nc_put_vara_short(ncid, varid, start, count, (short *) (curr_row->row[1]));
							break;
						case NC_INT:
							retval = nc_put_vara_int(ncid, varid, start, count, (int *) (curr_row->row[1]));
							break;
						case NC_INT64:
							retval = nc_put_vara_longlong(ncid, varid, start, count, (long long *) (curr_row->row[1]));
							break;
						case NC_FLOAT:
							retval = nc_put_vara_float(ncid, varid, start, count, (float *) (curr_row->row[1]));
							break;
						case NC_DOUBLE:
							retval = nc_put_vara_double(ncid, varid, start, count, (double *) (curr_row->row[1]));
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
								OPH_LOG_OPH_EXPORTNC_VAR_TYPE_NOT_SUPPORTED, data_type);
							oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
							oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
							oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
							oph_odb_stge_free_fragment_list(&frags);
							oph_odb_stge_free_db_list(&dbs);
							oph_odb_stge_free_dbms_list(&dbmss);
							oph_odb_free_ophidiadb(&oDB_slave);
							nc_close(ncid);
							for (l = 0; l < num_of_dims; l++) {
								if (dim_rows[l]) {
									free(dim_rows[l]);
									dim_rows[l] = NULL;
								}
							}
							free(dim_rows);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					if (retval) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write variable values: %s\n", nc_strerror(retval));
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_EXPORTNC_VAR_WRITE_ERROR, nc_strerror(retval));
						oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
						for (l = 0; l < num_of_dims; l++) {
							if (dim_rows[l]) {
								free(dim_rows[l]);
								dim_rows[l] = NULL;
							}
						}
						free(dim_rows);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					oph_nc_get_next_nc_id(start, dim_val_num, nexp);
					if (oph_ioserver_fetch_row(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows, &curr_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
						oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);
						oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
						oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server);
						oph_odb_stge_free_fragment_list(&frags);
						oph_odb_stge_free_db_list(&dbs);
						oph_odb_stge_free_dbms_list(&dbmss);
						oph_odb_free_ophidiadb(&oDB_slave);
						nc_close(ncid);
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

				oph_ioserver_free_result(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, frag_rows);

				nc_close(ncid);
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
	}
	if (oph_dc_cleanup_dbms(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_IOPLUGIN_CLEANUP_ERROR,
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
		int type = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number == 1;

		if (!strncmp(file, OPH_ESDM_PREFIX, 7)) {

			// ADD OUTPUT PID TO JSON AS TEXT
			if (oph_json_is_objkey_printable
			    (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_EXPORTNC)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPORTNC2, "Output File", file)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			// ADD FILE TO NOTIFICATION STRING
			char tmp_string[OPH_COMMON_BUFFER_LEN];
			snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;%s=%s;", OPH_IN_PARAM_LINK, file, OPH_IN_PARAM_FILE, file);
			if (handle->output_string) {
				strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
				free(handle->output_string);
			}
			handle->output_string = strdup(tmp_string);

		} else if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link) {

			if (type)
				snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_EXPORTNC_OUTPUT_PATH_SINGLE_FILE, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link, file,
					 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr || strstr(file, OPH_EXPORTNC_OUTPUT_FILE_EXT) ? "" : OPH_EXPORTNC_OUTPUT_FILE_EXT);
			else {
				if (!((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user_defined) {
					// Save the summary
					snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_EXPORTNC_OUTPUT_PATH_SUMMARY, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path, file);
					FILE *html_file = fopen(jsonbuf, "w");
					if (html_file) {
						fprintf(html_file, "<HTML>\n<HEAD>\n<TITLE>File list</TITLE>\n</HEAD>\n<BODY>\n<UL>\n");
						for (type = 0; type < ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->total_fragment_number; ++type)
							fprintf(html_file, "<LI><A href=" OPH_EXPORTNC_OUTPUT_PATH_MORE_FILES ">" OPH_EXPORTNC_OUTPUT_FILE "</A></LI>\n",
								((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link, file, type, file, type);
						fprintf(html_file, "</UL>\n</BODY>\n</HTML>\n");
						fclose(html_file);
					}
				}
				// Save the link
				snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_EXPORTNC_OUTPUT_PATH_SUMMARY, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link, file);
				type = 0;
			}

			// ADD OUTPUT PID TO JSON AS TEXT
			if (oph_json_is_objkey_printable
			    (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_EXPORTNC)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPORTNC, type ? "Output File" : "Output Files", jsonbuf)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			// ADD OUTPUT PID TO NOTIFICATION STRING
			char tmp_string[OPH_COMMON_BUFFER_LEN];
			snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;%s=%s;", OPH_IN_PARAM_LINK, jsonbuf, OPH_IN_PARAM_FILE, jsonbuf);
			if (handle->output_string) {
				strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
				free(handle->output_string);
			}
			handle->output_string = strdup(tmp_string);

		} else if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user) {

			char *output_path_file = ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user;
			size_t size = strlen(output_path_file);
			if (size && (output_path_file[size - 1] == '/'))
				output_path_file[--size] = 0;
			if (oph_json_is_objkey_printable
			    (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_EXPORTNC)) {
				if (type)
					snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, "%s" OPH_EXPORTNC_OUTPUT_PATH_SINGLE_FILE, size
						 && *output_path_file != '/' ? "/" : "", output_path_file, file, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr
						 || strstr(file, OPH_EXPORTNC_OUTPUT_FILE_EXT) ? "" : OPH_EXPORTNC_OUTPUT_FILE_EXT);
				else
					snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, "%s" OPH_EXPORTNC_OUTPUT_PATH "%s_*%s", size
						 && *output_path_file != '/' ? "/" : "", output_path_file, file,
						 ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->is_zarr ? "" : OPH_EXPORTNC_OUTPUT_FILE_EXT);
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPORTNC, type ? "Output File" : "Output Files", jsonbuf)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			// ADD FILE TO NOTIFICATION STRING
			char tmp_string[OPH_COMMON_BUFFER_LEN];
			snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_FILE, jsonbuf);
			if (handle->output_string) {
				strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
				free(handle->output_string);
			}
			handle->output_string = strdup(tmp_string);
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_EXPORTNC_NULL_OPERATOR_HANDLE);
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_path_user = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_link = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->output_name = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->datacube_input) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->datacube_input);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->datacube_input = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims) {
		free((NETCDF_dim *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dims = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
#ifdef OPH_ZARR
	oph_nc_dlclose(((OPH_EXPORTNC_operator_handle *) handle->operator_handle)->dlh);
#endif

	free((OPH_EXPORTNC_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

#ifdef OPH_ESDM
	handle->dlh = NULL;
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
