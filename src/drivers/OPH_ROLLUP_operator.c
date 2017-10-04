/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#include "drivers/OPH_ROLLUP_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
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

	if (!(handle->operator_handle = (OPH_ROLLUP_operator_handle *) calloc(1, sizeof(OPH_ROLLUP_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 0;

	char *datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys, &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_ROLLUP_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//For error checking
	int id_datacube_in[3] = { 0, 0, 0 };

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_ROLLUP_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_ROLLUP_OPHIDIADB_CONFIGURATION_FILE);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_ROLLUP_OPHIDIADB_CONNECTION_ERROR);
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
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_PID_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_NO_INPUT_DATACUBE, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_DATACUBE_AVAILABILITY_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], 1, &folder_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_DATACUBE_FOLDER_ERROR, datacube_in);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_DATACUBE_PERMISSION_ERROR, username);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
		}
		if (uri)
			free(uri);
		uri = NULL;

		if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_user))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_USER_ID_ERROR);
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
				if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, 0, &id_datacube_in[2])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container or it is hidden\n");
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
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_NO_INPUT_DATACUBE, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube_in[0];

	if (id_datacube_in[1] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_NO_INPUT_CONTAINER, datacube_in);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container = id_datacube_in[1];
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2];

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if ((((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size = (int) strtol(value, NULL, 10)) < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad parameter value\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_BAD_PARAMETER, "size", ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "description");
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
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, stream_max_size = 4 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int) + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
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
		ophidiadb *oDB = &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		int datacube_id = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube;

		//retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube)) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_DATACUBE_READ_ERROR);
			goto __OPH_EXIT_1;
		}
		// Change the container id
		cube.id_container = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container;

		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed = cube.compressed;

		//Copy fragment id relative index set 
		if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "fragment ids");
			goto __OPH_EXIT_1;
		}
		//Copy measure_type relative index set
		if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			oph_odb_cube_free_datacube(&cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "measure type");
			goto __OPH_EXIT_1;
		}

		oph_odb_cubehasdim *cubedims = NULL;
		int number_of_dimensions = 0;
		int last_insertd_id = 0;
		int l;

		//Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_CUBEHASDIM_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}

		int dimensionnumber = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, size = 1, nexp = 0, level_imp = 0, increased_level_imp;
		while (cubedims[nexp].explicit_dim && (nexp < number_of_dimensions))
			nexp++;
		if (dimensionnumber > nexp) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Bad number of dimensions (%d).\n", dimensionnumber);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_WRONG_DIMENSION_NUMBER,
				dimensionnumber);
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		for (l = number_of_dimensions - 1; l >= 0; l--)
			if (cubedims[l].size && cubedims[l].explicit_dim && (level_imp < dimensionnumber)) {
				level_imp++;
				if (level_imp >= dimensionnumber)
					break;
			}
		increased_level_imp = level_imp;
		for (l = number_of_dimensions - 1; l >= 0; l--)
			if (cubedims[l].size) {
				if (cubedims[l].explicit_dim)	// Explicit dimension
				{
					if (dimensionnumber) {
						size *= cubedims[l].size;
						cubedims[l].explicit_dim = 0;
						cubedims[l].level = level_imp--;
						dimensionnumber--;
						if (!dimensionnumber)
							break;
					}
				} else
					cubedims[l].level += increased_level_imp;	// Implicit dimension
			}
		if ((cube.tuplexfragment < size) || (cube.tuplexfragment % size)) {
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error: datacube fragmentation doesn't allow this operation, try to merge fragments before rolling up the dimension\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MAX_TUPLE_ROLLINGUP_ERROR);
			goto __OPH_EXIT_1;
		}
		if (dimensionnumber > 0)
			for (l = number_of_dimensions - 1; l >= 0; l--)
				if (!cubedims[l].size && cubedims[l].explicit_dim && dimensionnumber)	// Check for collapsed explicit dimensions
				{
					cubedims[l].explicit_dim = 0;
					cubedims[l].level = 0;
					dimensionnumber--;
					if (!dimensionnumber)
						break;
				}
		if (dimensionnumber > 0) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Too dimensions. Only %d will be rolled up\n", ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size - dimensionnumber);
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, "Too dimensions. Only %d will be rolled up\n",
				((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size - dimensionnumber);
		}

		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size = size;
		cube.tuplexfragment /= size;

		//New fields
		cube.id_source = 0;
		cube.level++;
		if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			oph_odb_cube_free_datacube(&cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		// Copy the dimension in case of output has to be saved in a new container
		if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container != ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container) {
			oph_odb_dimension dim[number_of_dimensions];
			oph_odb_dimension_instance dim_inst[number_of_dimensions];
			for (l = 0; l < number_of_dimensions; ++l) {
				if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_LOAD_ERROR);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_CONNECT_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_USE_DB_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container);
			char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(o_index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container);
			snprintf(o_label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container);

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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
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
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container,
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);
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
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);
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
				    (oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube, dim[l].dimension_name,
				     cl_value)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_INSTANCE_STORE_ERROR);
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
			cubedims[l].id_datacube = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube,
		     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_user)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube;
		new_task.id_job = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;

		if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed)
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_ROLLUP_QUERY_COMPR, MYSQL_FRAG_ID, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size);
		else
			snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_ROLLUP_QUERY, MYSQL_FRAG_ID, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID,
				 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size);
		new_task.input_cube_number = 1;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_STRUCT, "task");
			goto __OPH_EXIT_1;
		}
		new_task.id_inputcube[0] = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube;

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_TASK_INSERT_ERROR, new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		memcpy(id_string[3], &((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, sizeof(int));

		strncpy(data_type, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
	}

      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MASTER_TASK_INIT_FAILED);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "fragment ids");
			((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type = (char *) strndup(data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "measure type");
			((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size = *((int *) id_string[3]);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i, j, k;

	int id_datacube_out = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube;
	int id_datacube_in = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_datacube;
	int compressed = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->compressed;

	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char operation[OPH_COMMON_BUFFER_LEN];
	char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
	int n, result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0, tuplexfragment;
	long long size_;

	if (oph_dc_setup_dbms(&(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server), (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For each DBMS
	for (i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {

		if (oph_dc_connect_to_dbms(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = 0; (k < frags.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				tuplexfragment = frags.value[k].key_end - frags.value[k].key_start + 1;	// Under the assumption that IDs are consecutive without any holes
				if (frags.value[k].key_end && ((tuplexfragment < ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size)
							       || (tuplexfragment % (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__,
					      "Internal error: too many tuples (%d) to be aggregated (maximum is %d, try to merge fragments before rolling up the dimension) or bad parameter value\n",
					      ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, tuplexfragment);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_ROLLUP_MAX_TUPLE_ROLLINGUP_ERROR, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size, tuplexfragment);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_STRING_BUFFER_OVERFLOW,
						"fragment name", frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//OPH_ROLLUP mysql plugin
				if (compressed)
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_ROLLUP_PLUGIN_COMPR, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type,
						     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE,
						     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size);
				else
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_ROLLUP_PLUGIN, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type,
						     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE,
						     ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size);
				if (n >= OPH_COMMON_BUFFER_LEN) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_STRING_BUFFER_OVERFLOW,
						"MySQL operation name", operation);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				//ROLLUP fragment
				size_ = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size;
				if (oph_dc_create_fragment_from_query(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server, &(frags.value[k]), frag_name_out, operation, 0, &size_, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NEW_FRAG_ERROR,
						frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				//Change fragment fields
				frags.value[k].id_datacube = id_datacube_out;
				strncpy(frags.value[k].fragment_name, frag_name_out, OPH_ODB_STGE_FRAG_NAME_SIZE);
				frags.value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;
				if (frags.value[k].key_end) {
					frags.value[k].key_start = 1 + (frags.value[k].key_start - 1) / ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size;
					frags.value[k].key_end = 1 + (frags.value[k].key_end - 1) / ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->size;
				}
				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags.value[k]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_FRAGMENT_INSERT_ERROR,
						frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
	}

	if (oph_dc_cleanup_dbms(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_IOPLUGIN_CLEANUP_ERROR,
			(dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_free_ophidiadb(&oDB_slave);
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);

	if (handle->proc_rank == 0 && (result == OPH_ANALYTICS_OPERATOR_SUCCESS)) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_container,
			 ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_ROLLUP)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_ROLLUP, "Output Cube", jsonbuf)) {
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

	if (result == OPH_ANALYTICS_OPERATOR_SUCCESS)
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error = 0;

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_ROLLUP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (global_error) {
		//Delete fragments
		if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container,
						   ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
			}
		}
		//Before deleting wait for all process to reach this point
		MPI_Barrier(MPI_COMM_WORLD);

		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_ROLLUP_operator_handle *) handle->operator_handle)->oDB, id_datacube, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->id_input_container);
		}
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
		oph_odb_disconnect_from_ophidiadb(&((OPH_ROLLUP_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_ROLLUP_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type) {
		free((char *) ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description);
		((OPH_ROLLUP_operator_handle *) handle->operator_handle)->description = NULL;
	}
	free((OPH_ROLLUP_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
