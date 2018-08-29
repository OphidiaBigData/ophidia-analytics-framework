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

#include "drivers/OPH_CUBESIZE_operator.h"
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
#include "oph_datacube_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_utility_library.h"

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

	if (!(handle->operator_handle = (OPH_CUBESIZE_operator_handle *) calloc(1, sizeof(OPH_CUBESIZE_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->partial_size = 0;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation = 1;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit = 1;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid = NULL;

	//3 - Fill struct with the correct data
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *tmp_username = NULL;
	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(tmp_username = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BYTE_UNIT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BYTE_UNIT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_BYTE_UNIT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_utl_unit_to_value(value, &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit))
		((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit = OPH_UTL_MB_UNIT_VALUE;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	char *datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int result[3] = { 0, 0, 0 };

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_OPHIDIADB_CONFIGURATION_FILE);
			free(tmp_username);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_OPHIDIADB_CONNECTION_ERROR);
			free(tmp_username);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		if (oph_pid_parse_pid(value, &result[2], &result[0], &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_PID_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, result[2], result[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_NO_INPUT_DATACUBE, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, result[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, result[2], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_DATACUBE_FOLDER_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", tmp_username);
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_DATACUBE_PERMISSION_ERROR, tmp_username);
			result[0] = 0;
			result[2] = 0;
		}

		if (uri)
			free(uri);
		uri = NULL;
		if (result[0]) {
			long long size = 0;
			if ((oph_odb_cube_get_datacube_size(oDB, result[0], &size))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve size\n");
				logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_SIZE_READ_ERROR, datacube_name);
				result[0] = 0;
				result[2] = 0;
			} else {
				//Find cubesize
				if (size) {
					double convert_size = 0;;
					char unit[OPH_UTL_UNIT_SIZE] = { '\0' };
					if (oph_utl_compute_size(size, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit, &convert_size)
					    || oph_utl_unit_to_str(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit, &unit)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to compute size\n");
						logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_SIZE_COMPUTE_ERROR);
						result[0] = 0;
						result[2] = 0;
					}

					printf("+-----------------------+\n");
					printf("| CUBE SIZE [%s%-8s |\n", unit, "]");
					printf("+-----------------------+\n");
					printf("| %-21f |\n", convert_size);
					printf("+-----------------------+\n");

					if (oph_json_is_objkey_printable
					    (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num,
					     OPH_JSON_OBJKEY_CUBESIZE)) {
						char jsontmp[OPH_COMMON_BUFFER_LEN];
						// Header
						char **jsonkeys = NULL;
						char **fieldtypes = NULL;
						int num_fields = 2, iii, jjj = 0;
						jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonkeys) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "keys");
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jsonkeys[jjj] = strdup("CUBE SIZE");
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jjj++;
						jsonkeys[jjj] = strdup("UNIT");
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jjj = 0;
						fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
						if (!fieldtypes) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtypes");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jjj++;
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESIZE, "Cube Size", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < num_fields; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < num_fields; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);

						// Data
						char **jsonvalues = NULL;
						jsonvalues = (char **) calloc(num_fields, sizeof(char *));
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "values");
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jjj = 0;
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%f", convert_size);
						jsonvalues[jjj] = strdup(jsontmp);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						jjj++;
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%s", unit);
						jsonvalues[jjj] = strdup(jsontmp);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESIZE, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
					}

					((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation = 0;
				}
			}
			result[1] = ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation;
		}
	}

      __OPH_EXIT_1:

	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(result, 3, MPI_INT, 0, MPI_COMM_WORLD);

	free(tmp_username);

	//Check if sequential part has been completed
	if (result[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_NO_INPUT_DATACUBE, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (result[2] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_NO_INPUT_CONTAINER, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube = result[0];
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation = result[1];
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container = result[2];


	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBESIZE_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);



	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int result = 0;
	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->oDB;
		int number_dbms = 0;
		int datacube_id = ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube;
		//retrieve input datacube
		if (oph_odb_cube_get_datacube_dbmsinstance_number(oDB, datacube_id, &number_dbms)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving number of DBMS for this datacube!\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_DATACUBE_READ_ERROR);
		} else {
			//Copy necessary data   
			result = number_dbms;
		}
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(&result, 1, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (result == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Compute total number of DBMS used by datacube
	int total_dbms = result;

	//Compute the necessary parameters
	int dbmsxproc_quotient = total_dbms / handle->proc_number;
	int dbmsxproc_remainder = total_dbms % handle->proc_number;

	int i, number_of_dbms_by_proc = 0;

	//Every process must process at least dbmsxprox_quotient
	number_of_dbms_by_proc = dbmsxproc_quotient;
	if (dbmsxproc_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / dbmsxproc_remainder == 0)
			number_of_dbms_by_proc++;
	}
	//Compute DBMS IDs starting position
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position = 0;
	for (i = (handle->proc_rank) - 1; i >= 0; i--) {
		if (dbmsxproc_remainder != 0)
			((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position += (dbmsxproc_quotient + (i / dbmsxproc_remainder == 0 ? 1 : 0));
		else
			((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position += dbmsxproc_quotient;
	}
	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position >= total_dbms)
		((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position = -1;

	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_number = number_of_dbms_by_proc;

	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->frag_on_dbms_number = 0;
	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->frag_on_dbms_start_position = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;


	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->frag_on_dbms_start_position < 0 || ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position < 0) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "The process is idle\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_PROCESS_IDLE, handle->proc_rank);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	int i;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_dbms_instance_list dbmss;

	//retrieve DBMS connection string
	if (oph_odb_stge_fetch_dbms_connection_string
	    (&oDB_slave, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_id_start_position,
	     ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->dbms_number, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_DBMS_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	long long total_size = 0;
	long long partial_elements;

	MYSQL_RES *frag_list = NULL;
	int num_rows = 0;
	MYSQL_ROW row;
	char *frag_name = NULL;
	char *tmp_names = NULL;
	int tmp_counter = 0;
	int buffer_length = 0;

	if (oph_dc_setup_dbms(&(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_IOPLUGIN_SETUP_ERROR,
			(dbmss.value[0]).id_dbms);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//For each DBMS
	for (i = 0; i < dbmss.size; i++) {

		if (oph_dc_connect_to_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			if (frag_name) {
				free(frag_name);
				frag_name = NULL;
			}
			oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Get all fragment_name related to process
		if (oph_odb_stge_find_fragment_name_list
		    (&oDB_slave, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube, dbmss.value[i].id_dbms,
		     ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->frag_on_dbms_start_position, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->frag_on_dbms_number,
		     &frag_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment name list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_FRAG_LIST_READ_ERROR);
			if (frag_name) {
				free(frag_name);
				frag_name = NULL;
			}
			mysql_free_result(frag_list);
			oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(frag_list))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows find by query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NO_ROWS_FOUND);
			if (frag_name) {
				free(frag_name);
				frag_name = NULL;
			}
			mysql_free_result(frag_list);
			oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		if (mysql_field_count(oDB_slave.conn) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_MISSING_FIELDS);
			if (frag_name) {
				free(frag_name);
				frag_name = NULL;
			}
			mysql_free_result(frag_list);
			oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each ROW
		tmp_counter = 0;
		buffer_length = 0;
		while ((row = mysql_fetch_row(frag_list))) {
			buffer_length += (strlen(row[0]) + 3);
			//Resize fragment_name buffer for each result
			if (!(tmp_names = (char *) realloc(frag_name, (buffer_length + 1) * sizeof(char)))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR,
					"fragment name buffer");
				if (frag_name) {
					free(frag_name);
					frag_name = NULL;
				}
				mysql_free_result(frag_list);
				oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			frag_name = tmp_names;
			tmp_names = NULL;
			snprintf(frag_name + tmp_counter, buffer_length + 1, "'%s'|", row[0]);
			tmp_counter += (strlen(row[0]) + 3);
		}
		mysql_free_result(frag_list);
		if (frag_name)
			frag_name[tmp_counter - 1] = '\0';

		partial_elements = 0;

		//CUBESIZE size
		if (oph_dc_get_fragments_size_in_bytes(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), frag_name, &partial_elements)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve fragment size\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_GET_FRAG_SIZE_ERROR, frag_name);
			if (frag_name) {
				free(frag_name);
				frag_name = NULL;
			}
			oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		total_size += partial_elements;

		oph_dc_disconnect_from_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
		if (frag_name) {
			free(frag_name);
			frag_name = NULL;
		}
	}
	if (oph_dc_cleanup_dbms(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_IOPLUGIN_CLEANUP_ERROR,
			(dbmss.value[0]).id_dbms);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->partial_size = total_size;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	long long partial_result = ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->partial_size;
	long long final_result = 0;

	//Reduce results
	MPI_Reduce(&partial_result, &final_result, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (handle->proc_rank == 0) {
		double res;
		char unit[OPH_UTL_UNIT_SIZE] = { '\0' };
		if (oph_utl_compute_size(final_result, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit, &res)
		    || oph_utl_unit_to_str(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->byte_unit, &unit)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to compute size\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_SIZE_COMPUTE_ERROR);
		} else {
			printf("+-----------------------+\n");
			printf("| CUBE SIZE [%s%-8s |\n", unit, "]");
			printf("+-----------------------+\n");
			printf("| %-21f |\n", res);
			printf("+-----------------------+\n");

			if (oph_json_is_objkey_printable
			    (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_CUBESIZE)) {
				char jsontmp[OPH_COMMON_BUFFER_LEN];
				// Header
				char **jsonkeys = NULL;
				char **fieldtypes = NULL;
				int num_fields = 2, iii, jjj = 0;
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "keys");
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jsonkeys[jjj] = strdup("CUBE SIZE");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				jsonkeys[jjj] = strdup("UNIT");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBESIZE, "Cube Size", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);

				// Data
				char **jsonvalues = NULL;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "values");
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj = 0;
				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%f", res);
				jsonvalues[jjj] = strdup(jsontmp);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%s", unit);
				jsonvalues[jjj] = strdup(jsontmp);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBESIZE_MEMORY_ERROR, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBESIZE, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}
			//Set cubesize
			ophidiadb *oDB = &((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->oDB;

			if ((oph_odb_cube_set_datacube_size(oDB, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_datacube, final_result))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert cubesize\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_SET_DATACUBE_SIZE_ERROR);
			}
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBESIZE_NULL_OPERATOR_HANDLE);
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CUBESIZE_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_CUBESIZE_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
