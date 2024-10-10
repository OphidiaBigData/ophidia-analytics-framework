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

#include "drivers/OPH_CUBEELEMENTS_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <strings.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_datacube_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

int env_set(HASHTBL *task_tbl, oph_operator_struct *handle)
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

	if (!(handle->operator_handle = (OPH_CUBEELEMENTS_operator_handle *) calloc(1, sizeof(OPH_CUBEELEMENTS_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->partial_count = 0;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation = 1;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid = NULL;

	//3 - Fill struct with the correct data
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *tmp_username = NULL;
	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(tmp_username = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int algorithm = 0;
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (strncasecmp(value, OPH_CUBEELEMENTS_COUNT_ALGORITHM, STRLEN_MAX(value, OPH_CUBEELEMENTS_COUNT_ALGORITHM)) == 0)
		algorithm = OPH_CUBEELEMENTS_COUNT_ALGORITHM_VALUE;
	else
		algorithm = OPH_CUBEELEMENTS_PRODUCT_ALGORITHM_VALUE;


	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	char *datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		free(tmp_username);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int result[3] = { 0, 0, 0 };

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_OPHIDIADB_CONFIGURATION_FILE);
			free(tmp_username);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_PID_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, result[2], result[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_NO_INPUT_DATACUBE, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, result[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_DATACUBE_AVAILABILITY_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, result[2], &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_DATACUBE_FOLDER_ERROR, datacube_name);
			result[0] = 0;
			result[2] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", tmp_username);
			logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_DATACUBE_PERMISSION_ERROR, tmp_username);
			result[0] = 0;
			result[2] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;

		if (result[0]) {
			long long num_elements = 0;
			if ((oph_odb_cube_get_datacube_num_elements(oDB, result[0], &num_elements))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve cubeelements number\n");
				logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_ELEMENTS_NUMBER_ERROR, datacube_name);
				result[0] = 0;
				result[2] = 0;
			} else {
				//Find CUBEELEMENTS
				if (num_elements) {
					printf("+-----------------------+\n");
					printf("| %-21s |\n", "NUM.ELEMENTS");
					printf("+-----------------------+\n");
					printf("| %-21lld |\n", num_elements);
					printf("+-----------------------+\n");
					((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation = 0;
				} else if (algorithm == OPH_CUBEELEMENTS_PRODUCT_ALGORITHM_VALUE) {
					//PRODUCT OF ALL DIMENSIONS SIZES
					//Read dimension
					oph_odb_cubehasdim *cubedims = NULL;
					int number_of_dimensions = 0;

					if (oph_odb_cube_retrieve_cubehasdim_list(oDB, result[0], &cubedims, &number_of_dimensions)) {
						pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
						logging(LOG_WARNING, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_CUBEHASDIM_READ_ERROR);
						number_of_dimensions = 0;
					}

					int l;
					num_elements = 1;
					for (l = 0; l < number_of_dimensions; l++) {
						if (cubedims[l].level && cubedims[l].size) {
							num_elements *= cubedims[l].size;
						}
					}
					free(cubedims);

					printf("+-----------------------+\n");
					printf("| %-21s |\n", "NUM.ELEMENTS");
					printf("+-----------------------+\n");
					printf("| %-21lld |\n", num_elements);
					printf("+-----------------------+\n");

					//Set cubelements number 
					if ((oph_odb_cube_set_datacube_num_elements(oDB, result[0], num_elements))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert cubeelements number\n");
						logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_SET_NUMBER_ELEMENTS_ERROR);
					}

					((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation = 0;
				}

				if (!((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation) {
					if (oph_json_is_objkey_printable
					    (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num,
					     OPH_JSON_OBJKEY_CUBEELEMENTS)) {
						char jsontmp[OPH_COMMON_BUFFER_LEN];
						// Header
						char **jsonkeys = NULL;
						char **fieldtypes = NULL;
						int num_fields = 1, iii, jjj = 0;
						jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonkeys) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "keys");
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "NUMBER OF ELEMENTS");
						jsonkeys[jjj] = strdup(jsontmp);
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "key");
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
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "fieldtypes");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "fieldtype");
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
						if (oph_json_add_grid
						    (handle->operator_json, OPH_JSON_OBJKEY_CUBEELEMENTS, "Number of Cube Elements", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "values");
							result[0] = result[2] = 0;
							goto __OPH_EXIT_1;
						}
						for (jjj = 0; jjj < num_fields; jjj++) {
							snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%lld", num_elements);
							jsonvalues[jjj] = strdup(jsontmp);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result[0] = result[2] = 0;
								goto __OPH_EXIT_1;
							}
						}
						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBEELEMENTS, jsonvalues)) {
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
				}
			}
			result[1] = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation;
		}
	}

      __OPH_EXIT_1:

	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(result, 3, MPI_INT, 0, MPI_COMM_WORLD);

	free(tmp_username);

	//Check if sequential part has been completed
	if (result[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_NO_INPUT_DATACUBE, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (result[2] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_NO_INPUT_CONTAINER, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_datacube = result[0];
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation = result[1];
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container = result[2];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, result[2], OPH_LOG_OPH_CUBEELEMENTS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//For error checking
	char id_string[3][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0][0] = 0;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_DATACUBE_READ_ERROR);
		} else {
			//Copy fragment id relative index set   
			if (!(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))
			    || !(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT,
					"fragment ids");
			} else {
				((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->compressed = cube.compressed;
				strncpy(id_string[0], ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
				strncpy(id_string[1], ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
				snprintf(id_string[2], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, "%d", ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->compressed);
			}
		}
		oph_odb_cube_free_datacube(&cube);

	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_string, 3 * OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_string[0][0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT,
				"fragment ids");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(id_string[1], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT,
				"measure type");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->compressed = (int) strtol(id_string[2], NULL, 10);
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}


	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, j, k;
	int id_datacube = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_datacube;
	char *data_type = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type;
	int compressed = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->compressed;

	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_free_ophidiadb(&oDB_slave);

	long long total_elements = 0;
	long long partial_elements;

	if (oph_dc_setup_dbms(&(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_IOPLUGIN_SETUP_ERROR,
			(dbmss.value[0]).id_dbms);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For each DBMS
	for (i = 0; i < dbmss.size; i++) {

		if (oph_dc_connect_to_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_dc_cleanup_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; j < dbs.size; j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				oph_dc_disconnect_from_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_dc_cleanup_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//For each fragment
			for (k = 0; k < frags.size; k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				partial_elements = 0;

				//CUBEELEMENTS count
				if (oph_dc_get_total_number_of_elements_in_fragment
				    (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(frags.value[k]), data_type, compressed, &partial_elements)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to count elements in fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_CUBEELEMENTS_COUNT_NUMBER_ELEMENTS_ERROR, (frags.value[k]).fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, frags.value[k].db_instance->dbms_instance);
					oph_dc_cleanup_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server);
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				total_elements += partial_elements;
			}
		}
		oph_dc_disconnect_from_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
	}
	if (oph_dc_cleanup_dbms(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_IOPLUGIN_CLEANUP_ERROR,
			(dbmss.value[0]).id_dbms);
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);

	((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->partial_count = total_elements;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->first_time_computation == 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	long long partial_result = ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->partial_count;
	long long final_result = 0;

	//Reduce results
	MPI_Reduce(&partial_result, &final_result, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (handle->proc_rank == 0) {
		printf("+-----------------------+\n");
		printf("| %-21s |\n", "NUM.ELEMENTS");
		printf("+-----------------------+\n");
		printf("| %-21lld |\n", final_result);
		printf("+-----------------------+\n");

		if (oph_json_is_objkey_printable
		    (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num,
		     OPH_JSON_OBJKEY_CUBEELEMENTS)) {
			char jsontmp[OPH_COMMON_BUFFER_LEN];
			// Header
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			int num_fields = 1, iii, jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "keys");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "NUMBER OF ELEMENTS");
			jsonkeys[jjj] = strdup(jsontmp);
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "key");
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_LONG);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "fieldtype");
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
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBEELEMENTS, "Number of Cube Elements", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "values");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; jjj++) {
				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%lld", final_result);
				jsonvalues[jjj] = strdup(jsontmp);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEELEMENTS_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBEELEMENTS, jsonvalues)) {
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

		ophidiadb *oDB = &((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->oDB;

		//Set cubelements number 
		if ((oph_odb_cube_set_datacube_num_elements(oDB, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_datacube, final_result))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert cubeelements number\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_SET_NUMBER_ELEMENTS_ERROR);
		}

	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEELEMENTS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_disconnect_from_ophidiadb(&((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_CUBEELEMENTS_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
