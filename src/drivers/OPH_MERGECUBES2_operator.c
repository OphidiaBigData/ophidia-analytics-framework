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

#include "drivers/OPH_MERGECUBES2_operator.h"
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

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube_library.h"
#include "oph_driver_procedure_library.h"

#define OPH_MERGECUBES2_ARG_BUFFER 1024

int build_mergecubes_query(int datacube_num, char *output_cube, char **input_db, char **input_frag, char **input_type, int compressed, char **query)
{
	if (datacube_num < 1 || output_cube == NULL || input_db == NULL || input_frag == NULL || input_type == NULL || query == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return 1;
	}
	*query = NULL;

	size_t max_size = (datacube_num + 1) * OPH_MERGECUBES2_ARG_BUFFER;

	char tmp_buffer_small[3][max_size];
	char tmp_buffer[4][max_size];

	int cc = 0;

	//First compute output query length
	int tmp_len = 0;
	for (cc = 0; cc < datacube_num; cc++) {
		if (cc != 0)
			tmp_len += snprintf(tmp_buffer_small[1] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_INTYPE_SEPARATOR);
		tmp_len += snprintf(tmp_buffer_small[1] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_TYPE, input_type[cc]);
	}
	tmp_len = 0;
	for (cc = 0; cc < datacube_num; cc++) {
		if (cc != 0)
			tmp_len += snprintf(tmp_buffer_small[2] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_OUTTYPE_SEPARATOR);
		tmp_len += snprintf(tmp_buffer_small[2] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_TYPE, input_type[cc]);
	}

	int buf_len = snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_OPERATION, output_cube);
	tmp_len = 0;
	if (compressed) {
		for (cc = 0; cc < datacube_num; cc++) {
			if (cc != 0)
				tmp_len += snprintf(tmp_buffer_small[0] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_SEPARATOR);
			tmp_len += snprintf(tmp_buffer_small[0] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_PART_CMPR, cc, MYSQL_FRAG_MEASURE);	// input_type[cc]
		}
		tmp_len = snprintf(tmp_buffer[0], max_size, OPH_MERGECUBES2_ARG_SELECT_CMPR, 0, MYSQL_FRAG_ID, tmp_buffer_small[1], tmp_buffer_small[2], tmp_buffer_small[0]);
	} else {
		for (cc = 0; cc < datacube_num; cc++) {
			if (cc != 0)
				tmp_len += snprintf(tmp_buffer_small[0] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_SEPARATOR);
			tmp_len += snprintf(tmp_buffer_small[0] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_SELECT_PART, cc, MYSQL_FRAG_MEASURE);	// input_type[cc]
		}
		tmp_len = snprintf(tmp_buffer[0], max_size, OPH_MERGECUBES2_ARG_SELECT, 0, MYSQL_FRAG_ID, tmp_buffer_small[1], tmp_buffer_small[2], tmp_buffer_small[0]);
	}

	buf_len += snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_SELECT, tmp_buffer[0]);
	buf_len += snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_ALIAS, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE);

	tmp_len = 0;
	for (cc = 0; cc < datacube_num; cc++) {
		if (cc != 0)
			tmp_len += snprintf(tmp_buffer[1] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_FROM_SEPARATOR);
		tmp_len += snprintf(tmp_buffer[1] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_FROM_PART, input_db[cc], input_frag[cc]);
	}
	buf_len += snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_FROM, tmp_buffer[1]);

	tmp_len = 0;
	for (cc = 0; cc < datacube_num; cc++) {
		if (cc != 0)
			tmp_len += snprintf(tmp_buffer[2] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_FROM_SEPARATOR);
		tmp_len += snprintf(tmp_buffer[2] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_FROM_ALIAS_PART, cc);
	}
	buf_len += snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_FROM_ALIAS, tmp_buffer[2]);

	tmp_len = 0;
	for (cc = 1; cc < datacube_num; cc++) {
		if (cc > 1)
			tmp_len += snprintf(tmp_buffer[3] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_WHERE_SEPARATOR);
		tmp_len += snprintf(tmp_buffer[3] + tmp_len, OPH_MERGECUBES2_ARG_BUFFER, OPH_MERGECUBES2_ARG_WHERE_PART, 0, MYSQL_FRAG_ID, cc, MYSQL_FRAG_ID);
	}
	if (tmp_len)
		buf_len += snprintf(NULL, 0, OPH_MERGECUBES2_QUERY_WHERE, tmp_buffer[3]);

	//Build structures for output quey
	char *out_buffer = (char *) malloc(++buf_len * sizeof(char));
	if (!(out_buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return 1;
	}

	int out_len = snprintf(out_buffer, buf_len, OPH_MERGECUBES2_QUERY_OPERATION, output_cube);
	out_len += snprintf(out_buffer + out_len, buf_len - out_len, OPH_MERGECUBES2_QUERY_SELECT, tmp_buffer[0]);
	out_len += snprintf(out_buffer + out_len, buf_len - out_len, OPH_MERGECUBES2_QUERY_ALIAS, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE);
	out_len += snprintf(out_buffer + out_len, buf_len - out_len, OPH_MERGECUBES2_QUERY_FROM, tmp_buffer[1]);
	out_len += snprintf(out_buffer + out_len, buf_len - out_len, OPH_MERGECUBES2_QUERY_FROM_ALIAS, tmp_buffer[2]);
	if (tmp_len)
		out_len += snprintf(out_buffer + out_len, buf_len - out_len, OPH_MERGECUBES2_QUERY_WHERE, tmp_buffer[3]);

	if (out_len >= buf_len) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size %d is not enough for query '%s'\n", buf_len - 1, out_buffer);
		free(out_buffer);
		return 2;
	}

	*query = out_buffer;

	return 0;
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

	if (!(handle->operator_handle = (OPH_MERGECUBES2_operator_handle *) calloc(1, sizeof(OPH_MERGECUBES2_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	int i;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_job = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_user = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type = NULL;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size = -1;

	char **datacube_in;
	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys, &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//3 - Fill struct with the correct data

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_ORDER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ORDER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_ORDER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	int order = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		if (!strncmp(value, "source", OPH_TP_TASKLEN))
			order = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_MULTI_INPUT);

	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_MULTI_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_MULTI_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &datacube_in, &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube =
	    (int *) calloc(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num, sizeof(int));
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "input datacube id");
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container =
	    (int *) calloc(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num, sizeof(int));
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "input datacube id");
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//For error checking
	int *id_datacube_in = (int *) calloc(2 * ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + 1, sizeof(int));
	if (id_datacube_in == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "input datacube id");
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_init_ophidiadb(oDB);

		//Check if datacube exists (by ID container and datacube)
		int exists = 0;
		int status = 0;
		char *uri = NULL;
		int folder_id = 0;
		int permission = 0;
		int cc = 0;

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_OPHIDIADB_CONFIGURATION_FILE);
			id_datacube_in[0] = 0;
		} else if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_OPHIDIADB_CONNECTION_ERROR);
			id_datacube_in[0] = 0;
		}
		//Parse first datacube
		else if (oph_pid_parse_pid(datacube_in[0], &(id_datacube_in[0]), &(id_datacube_in[1]), &uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_PID_ERROR, datacube_in[0]);
			id_datacube_in[0] = 0;
		} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[0], id_datacube_in[1], &exists)) || !exists) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_NO_INPUT_DATACUBE, datacube_in[0]);
			id_datacube_in[0] = 0;
		} else if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[1], 0, &status)) || !status) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, datacube_in[0]);
			id_datacube_in[0] = 0;
		} else {
			//Parse other datacubes
			for (cc = 1; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++) {
				if (uri)
					free(uri);
				uri = NULL;
				if (oph_pid_parse_pid(datacube_in[cc], &(id_datacube_in[2 * cc]), &(id_datacube_in[2 * cc + 1]), &uri)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_PID_ERROR, datacube_in[cc]);
					id_datacube_in[0] = 0;
					break;
				}
				if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[2 * cc], id_datacube_in[2 * cc + 1], &exists)) || !exists) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_NO_INPUT_DATACUBE, datacube_in[cc]);
					id_datacube_in[0] = 0;
					break;
				}
				if ((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[2 * cc + 1], 0, &status)) || !status) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, datacube_in[cc]);
					id_datacube_in[0] = 0;
					break;
				}
			}
		}
		if (uri)
			free(uri);
		uri = NULL;

		if (id_datacube_in[0]) {
			if ((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[0], &folder_id))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_DATACUBE_FOLDER_ERROR, datacube_in[1]);
				id_datacube_in[0] = 0;
			} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_DATACUBE_PERMISSION_ERROR, username);
				id_datacube_in[0] = 0;
			}
		}
		if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_user))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_GENERIC_USER_ID_ERROR);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (oph_odb_cube_order_by(oDB, order, id_datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to order cubes.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], "Unable to order cubes.\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		id_datacube_in[2 * ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num] = id_datacube_in[0];
		if (id_datacube_in[0]) {
			value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
				if (oph_odb_fs_retrieve_container_id_from_container_name
				    (oDB, folder_id, value, &id_datacube_in[2 * ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value);
					id_datacube_in[0] = 0;
				}
			}
		}
	}
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(id_datacube_in, 2 * ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + 1, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (id_datacube_in[0] == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_NO_INPUT_CONTAINER, datacube_in[1]);
		free(id_datacube_in);
		oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container = id_datacube_in[2 * ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num];

	for (i = 0; i < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; ++i) {
		if (id_datacube_in[2 * i + 1] == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[0], OPH_LOG_OPH_MERGECUBES_NO_INPUT_DATACUBE, datacube_in[i]);
			free(id_datacube_in);
			oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[i] = id_datacube_in[2 * i + 1];
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[i] = id_datacube_in[2 * i];
	}
	int id_container = id_datacube_in[0];
	free(id_datacube_in);
	oph_tp_free_multiple_value_param_list(datacube_in, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "dimension name");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "dimension type");
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number = (int) strtol(value, NULL, 10) - 1;
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong value of %s\n", OPH_IN_PARAM_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_MERGECUBES_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_MERGECUBES_DATACUBE_NUMBER_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_DATACUBE_NUMBER_ERROR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int pointer, input_datacube_num =
	    ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number, stream_max_size =
	    4 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 3 * sizeof(int) + input_datacube_num * OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	char stream[stream_max_size];
	memset(stream, 0, sizeof(stream));
	*stream = 0;
	char *id_string[4], *data_type[input_datacube_num];
	pointer = 0;
	id_string[0] = stream + pointer;
	pointer += 1 + OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE;
	id_string[1] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[2] = stream + pointer;
	pointer += 1 + sizeof(int);
	id_string[3] = stream + pointer;
	pointer += 1 + sizeof(int);

	int cc = 0, ccc;
	for (cc = 0; cc < input_datacube_num; cc++) {
		data_type[cc] = stream + pointer;
		pointer += 1 + OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
	}

	if (handle->proc_rank == 0) {

		// Set dim_name if any
		char *dim_name = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name;
		if (!dim_name) {
			char dimension_name[OPH_ODB_DIM_DIMENSION_SIZE + 1];
			snprintf(dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "d%d", ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0]);
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name = dim_name = strdup(dimension_name);
			if (!dim_name) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set dimension name due to a memory error.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					"Unable to set dimension name due to a memory error.\n");
				goto __OPH_EXIT_1;
			}
		}

		ophidiadb *oDB = &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->oDB;
		oph_odb_datacube *cube = (oph_odb_datacube *) malloc(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num * sizeof(oph_odb_datacube));
		if (cube == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "datacube structs");
			goto __OPH_EXIT_1;
		}
		for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++) {
			oph_odb_cube_init_datacube(&(cube[cc]));
		}

		oph_odb_cubehasdim *cubedims = NULL, *cubedims2 = NULL;
		int number_of_dimensions = 0, number_of_dimensions2 = 0;
		int last_insertd_id = 0;
		int l, ll;

		// Retrieve input datacube
		if (oph_odb_cube_retrieve_datacube(oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0], &cube[0])) {
			for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
				oph_odb_cube_free_datacube(&(cube[cc]));
			free(cube);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DATACUBE_READ_ERROR, 0);
			goto __OPH_EXIT_1;
		}
		// Read old cube - dimension relation rows
		if (oph_odb_cube_retrieve_cubehasdim_list(oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0], &cubedims, &number_of_dimensions)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube1 - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
				OPH_LOG_OPH_MERGECUBES_CUBEHASDIM_READ_ERROR, "first");
			for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
				oph_odb_cube_free_datacube(&(cube[cc]));
			free(cube);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		// Read old cube dimensions
		oph_odb_dimension dim[1 + number_of_dimensions];
		oph_odb_dimension_instance dim_inst[1 + number_of_dimensions];
		for (l = 0; l < number_of_dimensions; ++l) {
			if (oph_odb_dim_retrieve_dimension_instance
			    (oDB, cubedims[l].id_dimensioninst, dim_inst + l, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_READ_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, dim + l, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_READ_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}

		int found_dim_name = -1;
		for (cc = 1; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++) {
			if (oph_odb_cube_retrieve_datacube(oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[cc], &cube[cc])) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_READ_ERROR, cc);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			// Checking fragmentation structure
			if ((cube[0].hostxdatacube != cube[cc].hostxdatacube) || (cube[0].fragmentxdb != cube[cc].fragmentxdb) || (cube[0].tuplexfragment != cube[cc].tuplexfragment)) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube fragmentation structures are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_COMPARISON_ERROR, "fragmentation structures");
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			// Checking fragment indexes
			if (strcmp(cube[0].frag_relative_index_set, cube[cc].frag_relative_index_set)) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube relative index sets are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_COMPARISON_ERROR, "relative index sets");
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			if (cube[0].compressed != cube[cc].compressed) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube are compressed differently\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_COMPARISON_ERROR, "compression methods");
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			if (oph_odb_cube_retrieve_cubehasdim_list(oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[cc], &cubedims2, &number_of_dimensions2)) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube2 - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_CUBEHASDIM_READ_ERROR, "second");
				free(cubedims);
				free(cubedims2);
				goto __OPH_EXIT_1;
			}
			// Dimension comparison
			for (l = ll = 0; (l < number_of_dimensions) && (ll < number_of_dimensions2); l++, ll++) {
				while (!cubedims[l].size && (l < number_of_dimensions))
					l++;
				while (!cubedims2[ll].size && (ll < number_of_dimensions2))
					ll++;
				if ((l >= number_of_dimensions) || (ll >= number_of_dimensions2) || (cubedims[l].size != cubedims2[ll].size)
				    || (cubedims[l].explicit_dim != cubedims2[ll].explicit_dim)
				    || (cubedims[l].level != cubedims2[ll].level))
					break;
			}
			found_dim_name = -1;
			for (; l < number_of_dimensions; l++) {
				if (cubedims[l].size) {
					if (found_dim_name < 0) {
						found_dim_name = l;
						if (strcmp(dim_name, dim[l].dimension_name)) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "A new dimension name '%s' is already set. Input argument '%s' will be negletted\n",
							      dim[l].dimension_name, dim_name);
							logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
								"A new dimension name '%s' is already set. Input argument '%s' will be negletted\n", dim[l].dimension_name, dim_name);
							free(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name);
							((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name = dim_name = strdup(dim[l].dimension_name);
						}
						if (l < number_of_dimensions - 1) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "The ensamble dimension is not the last dimension: currently unsupported!\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
								"The ensamble dimension is not the last dimension: currently unsupported!\n");
							free(cube);
							free(cubedims);
							free(cubedims2);
							goto __OPH_EXIT_1;
						}
					} else
						break;
				}
			}
			for (; ll < number_of_dimensions2; ll++)
				if (cubedims2[ll].size)
					break;
			if ((l < number_of_dimensions) || (ll < number_of_dimensions2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube dimensions are not comparable.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_COMPARISON_ERROR, "dimensions");
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				free(cubedims);
				free(cubedims2);
				goto __OPH_EXIT_1;
			}
			free(cubedims2);
		}

		// Change the container id
		cube[0].id_container = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container;
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed = cube[0].compressed;

		//Copy fragment id relative index set 
		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(cube[0].frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
				oph_odb_cube_free_datacube(&(cube[cc]));
			free(cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT,
				"fragment ids");
			goto __OPH_EXIT_1;
		}

		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type = (char **) malloc(input_datacube_num * sizeof(char *)))) {
			for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
				oph_odb_cube_free_datacube(&(cube[cc]));
			free(cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT,
				"datacube types");
			goto __OPH_EXIT_1;
		}
		//Copy measure_type relative index set
		for (cc = ccc = 0; cc < input_datacube_num; cc++) {
			if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[cc] = (char *) strndup(cube[ccc].measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				free(cubedims);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "measure type");
				goto __OPH_EXIT_1;
			}
			if (cc >= ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number)
				ccc++;
		}

		//New fields
		cube[0].id_source = 0;
		cube[0].level++;
		if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description)
			snprintf(cube[0].description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description);
		else
			*cube[0].description = 0;

#ifdef STRUCT_DATA_TYPE
		//Update type and measures
		int measure_len, measure_type_len;
		measure_len = strlen(cube[0].measure);
		measure_type_len = strlen(cube[0].measure_type);
		for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number; cc++) {
			if ((measure_len >= OPH_ODB_CUBE_MEASURE_SIZE) || (measure_type_len >= OPH_ODB_CUBE_MEASURE_TYPE_SIZE)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_MERGECUBES_MEASURE_STRING_OVERFLOW);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_MEASURE_STRING_OVERFLOW);
				break;
			}
			measure_len += snprintf(cube[0].measure + measure_len, OPH_ODB_CUBE_MEASURE_SIZE, ";%s", cube[0].measure);
			measure_type_len += snprintf(cube[0].measure_type + measure_type_len, OPH_ODB_CUBE_MEASURE_TYPE_SIZE, ";%s", cube[0].measure_type);
		}
#endif

		for (cc = 1; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++) {
#ifdef STRUCT_DATA_TYPE
			if ((measure_len >= OPH_ODB_CUBE_MEASURE_SIZE) || (measure_type_len >= OPH_ODB_CUBE_MEASURE_TYPE_SIZE)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_LOG_OPH_MERGECUBES_MEASURE_STRING_OVERFLOW);
				logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_MEASURE_STRING_OVERFLOW);
				break;
			}
			measure_len += snprintf(cube[0].measure + measure_len, OPH_ODB_CUBE_MEASURE_SIZE, ";%s", cube[cc].measure);
			measure_type_len += snprintf(cube[0].measure_type + measure_type_len, OPH_ODB_CUBE_MEASURE_TYPE_SIZE, ";%s", cube[cc].measure_type);
#else
			if (strncasecmp(cube[0].measure_type, cube[cc].measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE)) {
				for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
					oph_odb_cube_free_datacube(&(cube[cc]));
				free(cube);
				free(cubedims);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input data types are different\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_MEASURE_STRING_OVERFLOW);
				goto __OPH_EXIT_1;
			}
#endif
		}

		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &(cube[0]), &(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube))) {
			for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
				oph_odb_cube_free_datacube(&(cube[cc]));
			free(cube);
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DATACUBE_INSERT_ERROR);
			goto __OPH_EXIT_1;
		}
		for (cc = 0; cc < ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num; cc++)
			oph_odb_cube_free_datacube(&(cube[cc]));
		free(cube);

		if (oph_odb_meta_copy_from_cube_to_cube
		    (oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[0],
		     ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_user)) {
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_GENERIC_METADATA_COPY_ERROR);
			goto __OPH_EXIT_1;
		}
#ifndef STRUCT_DATA_TYPE
		oph_odb_cubehasdim *cubedims_new = (oph_odb_cubehasdim *) malloc((1 + number_of_dimensions) * sizeof(oph_odb_cubehasdim));
		if (!cubedims_new) {
			free(cubedims);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc new cube dimension metadata\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "cube dimension metadata");
			goto __OPH_EXIT_1;
		}

		int max_level = 0;
		for (l = 0; l < number_of_dimensions; l++) {
			cubedims_new[l].id_datacube = cubedims[l].id_datacube;
			cubedims_new[l].id_dimensioninst = cubedims[l].id_dimensioninst;
			cubedims_new[l].explicit_dim = cubedims[l].explicit_dim;
			cubedims_new[l].level = cubedims[l].level;
			cubedims_new[l].size = cubedims[l].size;
			if (!cubedims[l].explicit_dim && cubedims[l].size)
				max_level = cubedims[l].level;
		}
		if (found_dim_name < 0) {
			cubedims_new[l].id_datacube = cubedims[0].id_datacube;
			cubedims_new[l].id_dimensioninst = 0;
			cubedims_new[l].explicit_dim = 0;
			cubedims_new[l].level = max_level + 1;
			cubedims_new[l].size = input_datacube_num;
		} else {
			l = found_dim_name;
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size = cubedims_new[l].size;
			cubedims_new[l].size += input_datacube_num - 1;
		}

		free(cubedims);
		cubedims = cubedims_new;

		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		// Set and add metadata of new dimension
		if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_DEFAULT_HIERARCHY, &dim[l].id_hierarchy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieve hierachy\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], "Error while retrieve hierachy\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		dim[l].id_container = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container;
		snprintf(dim[l].dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "%s", dim_name);
		int size = 0;
		if (oph_dim_check_data_type(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, &size)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimension type %s\n", ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], "Invalid dimension type %s\n",
				((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		snprintf(dim[l].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
		*dim[l].units = 0;
		*dim[l].calendar = 0;
		if (oph_odb_dim_insert_into_dimension_table(oDB, &dim[l], &dim[l].id_dimension, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while adding new dimension\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], "Error while adding new dimension\n");
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		dim_inst[l].id_dimension = dim[l].id_dimension;
		dim_inst[l].id_grid = 0;
		dim_inst[l].fk_id_dimension_index = 0;
		dim_inst[l].fk_id_dimension_label = 0;
		dim_inst[l].concept_level = OPH_COMMON_BASE_CONCEPT_LEVEL;
		dim_inst[l].unlimited = 0;

		char *dim_row;
		int compressed = 0;
		char dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0]);
		char o_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(o_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container);

		if (found_dim_name >= 0)
			number_of_dimensions--;

		for (l = 0; l <= number_of_dimensions; l++) {
			dim_inst[l].size = cubedims[l].size;

			dim_row = NULL;
			if (l == number_of_dimensions) {
				if (!cubedims[l].size)
					continue;
				size_t size_l = sizeof(long long);
				dim_row = (char *) malloc(cubedims[l].size * size_l);
				if (!dim_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc dimension data.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						"Unable to alloc dimension data.\n");
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				char *cv = dim_row;
				long long vv, tv = (long long) cubedims[l].size;
				for (vv = 1; vv <= tv; ++vv, cv += size_l)
					memcpy(cv, &vv, size_l);

				if (strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_LONG_TYPE, sizeof(OPH_DIM_LONG_TYPE))) {

					char label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
					snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO,
						 ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container);
					char *dim_array = (char *) malloc(cubedims[l].size * size);
					if (!dim_array) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc dimension data.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
							"Unable to alloc dimension data.\n");
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					cv = dim_array;
					if (!strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_DOUBLE_TYPE, sizeof(OPH_DIM_DOUBLE_TYPE))) {
						double vvv, vtv = (double) cubedims[l].size;
						for (vvv = 1; vvv <= vtv; ++vvv, cv += size)
							memcpy(cv, &vvv, size);
					} else if (!strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_FLOAT_TYPE, sizeof(OPH_DIM_FLOAT_TYPE))) {
						float vvv, vtv = (float) cubedims[l].size;
						for (vvv = 1; vvv <= vtv; ++vvv, cv += size)
							memcpy(cv, &vvv, size);
					} else if (!strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_INT_TYPE, sizeof(OPH_DIM_INT_TYPE))) {
						int vvv, vtv = (int) cubedims[l].size;
						for (vvv = 1; vvv <= vtv; ++vvv, cv += size)
							memcpy(cv, &vvv, size);
					} else if (!strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_SHORT_TYPE, sizeof(OPH_DIM_SHORT_TYPE))) {
						short vvv, vtv = (short) cubedims[l].size;
						for (vvv = 1; vvv <= vtv; ++vvv, cv += size)
							memcpy(cv, &vvv, size);
					} else if (!strncmp(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, OPH_DIM_BYTE_TYPE, sizeof(OPH_DIM_BYTE_TYPE))) {
						char vvv, vtv = (char) cubedims[l].size;
						for (vvv = 1; vvv <= vtv; ++vvv, cv += size)
							memcpy(cv, &vvv, size);
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimension type '%s'.\n", ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
							"Invalid dimension type '%s'.\n", ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
						if (dim_row)
							free(dim_row);
						if (dim_array)
							free(dim_array);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					if (oph_dim_insert_into_dimension_table
					    (db, label_dimension_table_name, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type, cubedims[l].size, dim_array,
					     &dim_inst[l].fk_id_dimension_label)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
							OPH_LOG_OPH_MERGECUBES_DIM_ROW_ERROR);
						if (dim_row)
							free(dim_row);
						if (dim_array)
							free(dim_array);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
					if (dim_array)
						free(dim_array);
				}
			} else {
				if (!strcmp(dim[l].dimension_name, dim_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimension name '%s'.\n", dim_name);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_DIM_READ_ERROR);
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (!dim_inst[l].fk_id_dimension_index) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						"Dimension FK not set in cubehasdim.\n");
					if (dim_row)
						free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (dim_inst[l].size) {
					if (oph_dim_read_dimension_data(db, dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, compressed, &dim_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
							OPH_LOG_OPH_MERGECUBES_DIM_READ_ERROR);
						if (dim_row)
							free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
				} else {
					dim_inst[l].fk_id_dimension_label = 0;
					if (dim[l].calendar && strlen(dim[l].calendar))	// Time dimension (the check can be improved by checking hierarchy name)
					{
						if (oph_odb_meta_put
						    (oDB, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube, NULL, OPH_ODB_TIME_FREQUENCY, 0,
						     OPH_COMMON_FULL_REDUCED_DIM)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
								OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);
							if (dim_row)
								free(dim_row);
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							free(cubedims);
							goto __OPH_EXIT_1;
						}
					}
				}
			}

			if (oph_dim_insert_into_dimension_table(db, o_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_DIM_ROW_ERROR);
				if (dim_row)
					free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), 0, NULL, NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DIM_INSTANCE_STORE_ERROR);
				if (dim_row)
					free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			if (dim_row)
				free(dim_row);
		}
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);

		number_of_dimensions++;
#endif

		//Write new cube - dimension relation rows
		for (l = 0; l < number_of_dimensions; l++) {
			//Change iddatacube in cubehasdim
			cubedims[l].id_datacube = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube;

			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_CUBEHASDIM_INSERT_ERROR);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		free(cubedims);

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		new_task.id_job = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_job;
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		new_task.operator[OPH_ODB_CUBE_OPERATOR_SIZE] = 0;
		char *query = NULL;
		char **input_frag = (char **) malloc(input_datacube_num * sizeof(char *));
		char **input_db = (char **) malloc(input_datacube_num * sizeof(char *));
		for (cc = 0; cc < input_datacube_num; cc++) {
			input_frag[cc] = (char *) malloc((strlen("fact_in10000") + 1) * sizeof(char));
			snprintf(input_frag[cc], strlen("fact_in10000") + 1, "fact_in%d", cc + 1);
			input_db[cc] = (char *) malloc((strlen("db_in10000") + 1) * sizeof(char));
			snprintf(input_db[cc], strlen("db_in10000") + 1, "db_in%d", cc + 1);
		}

		if (build_mergecubes_query
		    (input_datacube_num, "fact_out", input_db, input_frag, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type,
		     ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error creating query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_QUERY_BUILD_ERROR);
			goto __OPH_EXIT_1;
		}

		snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, "%s", query);
		free(query);
		for (cc = 0; cc < input_datacube_num; cc++)
			free(input_db[cc]);
		free(input_db);
		for (cc = 0; cc < input_datacube_num; cc++)
			free(input_frag[cc]);
		free(input_frag);

		new_task.input_cube_number = input_datacube_num;
		if (!(new_task.id_inputcube = (int *) malloc(new_task.input_cube_number * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_STRUCT,
				"task");
			goto __OPH_EXIT_1;
		}
		for (cc = ccc = 0; cc < input_datacube_num; cc++) {
			new_task.id_inputcube[cc] = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube[ccc];
			if (cc >= ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number)
				ccc++;
		}

		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_TASK_INSERT_ERROR,
				new_task.operator);
			free(new_task.id_inputcube);
			goto __OPH_EXIT_1;
		}
		free(new_task.id_inputcube);

		strncpy(id_string[0], ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		memcpy(id_string[1], &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube, sizeof(int));
		memcpy(id_string[2], &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed, sizeof(int));
		memcpy(id_string[3], &((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size, sizeof(int));

		for (cc = 0; cc < input_datacube_num; cc++) {
			strncpy(data_type[cc], ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[cc], OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
		}
	}
      __OPH_EXIT_1:
	//Broadcast to all other processes the fragment relative index        
	MPI_Bcast(stream, stream_max_size, MPI_CHAR, 0, MPI_COMM_WORLD);
	if (*stream == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MASTER_TASK_INIT_FAILED);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (handle->proc_rank != 0) {
		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT,
				"fragment ids");
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type = (char **) malloc(input_datacube_num * sizeof(char *)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT,
				"measure type array");
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (cc = 0; cc < input_datacube_num; cc++) {
			if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[cc] = (char *) strndup(data_type[cc], OPH_ODB_CUBE_MEASURE_TYPE_SIZE))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "measure type");
				((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube = *((int *) id_string[1]);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed = *((int *) id_string[2]);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size = *((int *) id_string[3]);
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int id_number;
	char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;

	//Get total number of fragment IDs
	if (oph_ids_count_number_of_ids(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids, &id_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_RETREIVE_IDS_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//All processes compute the fragment number to work on
	int div_result = (id_number) / (handle->proc_number);
	int div_remainder = (id_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	} else {
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position += div_result;
		}
		if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position >= id_number)
			((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position = -1;
	}

	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0) {
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 0;
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Partition fragment relative index string
	char *new_ptr = new_id_string;
	if (oph_ids_get_substring_from_string
	    (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position,
	     ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_ID_STRING_SPLIT_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	free(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids);
	if (!(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids = (char *) strndup(new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 0;
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i = 0, j, k;

	int id_datacube_out = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube;
	int *id_datacube_in = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube;
	int compressed = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->compressed;
	int datacube_num = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number;

	oph_odb_fragment_list *frags = (oph_odb_fragment_list *) calloc(datacube_num, sizeof(oph_odb_fragment_list));
	oph_odb_db_instance_list *dbs = (oph_odb_db_instance_list *) calloc(datacube_num, sizeof(oph_odb_db_instance_list));
	oph_odb_dbms_instance_list *dbmss = (oph_odb_dbms_instance_list *) calloc(datacube_num, sizeof(oph_odb_dbms_instance_list));

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_OPHIDIADB_CONFIGURATION_FILE);
		free(frags);
		free(dbs);
		free(dbmss);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		free(frags);
		free(dbs);
		free(dbmss);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	int cc, ccc;
	for (cc = ccc = 0; cc < datacube_num; cc++) {
		if (oph_odb_stge_fetch_fragment_connection_string
		    (&oDB_slave, id_datacube_in[ccc], ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids, &(frags[cc]), &(dbs[cc]), &(dbmss[cc]))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
				OPH_LOG_OPH_MERGECUBES_CONNECTION_STRINGS_NOT_FOUND, cc);
			oph_odb_free_ophidiadb(&oDB_slave);
			for (cc = 0; cc < datacube_num; cc++) {
				oph_odb_stge_free_fragment_list(&(frags[cc]));
				oph_odb_stge_free_db_list(&(dbs[cc]));
				oph_odb_stge_free_dbms_list(&(dbmss[cc]));
			}
			free(frags);
			free(dbs);
			free(dbmss);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (cc >= ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number) {
			if ((dbmss[0].size != dbmss[cc].size) || (dbs[0].size != dbs[cc].size) || (frags[0].size != frags[cc].size)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube structures are not comparable\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DATACUBE_COMPARISON_ERROR, "structures");
				for (cc = 0; cc < datacube_num; cc++) {
					oph_odb_stge_free_fragment_list(&(frags[cc]));
					oph_odb_stge_free_db_list(&(dbs[cc]));
					oph_odb_stge_free_dbms_list(&(dbmss[cc]));
				}
				free(frags);
				free(dbs);
				free(dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			ccc++;
		}
	}

	char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
	int result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0;

	char **input_frag = (char **) malloc(datacube_num * sizeof(char *));
	char **input_db = (char **) malloc(datacube_num * sizeof(char *));
	char *query = NULL;

	if (oph_dc_setup_dbms(&(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server), (dbmss[0].value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_IOPLUGIN_SETUP_ERROR,
			(dbmss[0].value[i]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	// This implementation assumes a perfect correspondence between datacube structures

	//For each DBMS
	for (i = 0; (i < dbmss[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {
		// Current implementation considers data exchange within the same dbms, databases could be different
		for (cc = 1 + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number; cc < datacube_num; cc++) {
			if (dbmss[0].value[i].id_dbms != dbmss[cc].value[i].id_dbms) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to compare datacubes in different dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DIFFERENT_DBMS_ERROR);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			if (result == OPH_ANALYTICS_OPERATOR_UTILITY_ERROR)
				break;
		}

		if (oph_dc_connect_to_dbms(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server, &(dbmss[0].value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
				OPH_LOG_OPH_MERGECUBES_DBMS_CONNECTION_ERROR, (dbmss[0].value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; (j < dbs[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {
			//Check DB - DBMS Association
			if (dbs[0].value[j].dbms_instance != &(dbmss[0].value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server, &(dbmss[0].value[i]), &(dbs[0].value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
					OPH_LOG_OPH_MERGECUBES_DB_SELECTION_ERROR, (dbs[0].value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//Check DB - DBMS Association
			for (cc = 1 + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number; cc < datacube_num; cc++) {
				if (dbs[cc].value[j].dbms_instance != &(dbmss[cc].value[i]))	// continue;
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Databases are not comparable.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_DIFFERENT_DB_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}

			//For each fragment
			for (k = 0; (k < frags[0].size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++) {
				//Check Fragment - DB Association
				if (frags[0].value[k].db_instance != &(dbs[0].value[j]))
					continue;

				for (cc = 1 + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number; cc < datacube_num; cc++) {
					if (frags[cc].value[k].db_instance != &(dbs[cc].value[j]))	// continue;
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
							OPH_LOG_OPH_MERGECUBES_FRAGMENT_COMPARISON_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
				}

				if (oph_dc_generate_fragment_name(dbs[0].value[j].db_name, id_datacube_out, handle->proc_rank, (frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				query = NULL;

				for (cc = 0; cc < datacube_num; cc++) {
					input_frag[cc] = frags[cc].value[k].fragment_name;
					input_db[cc] = frags[cc].value[k].db_instance->db_name;
				}

				if (build_mergecubes_query
				    (datacube_num, frag_name_out, input_db, input_frag, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type, compressed, &query)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error creating query\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_QUERY_BUILD_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					if (query != NULL) {
						free(query);
						query = NULL;
					}
					break;
				}
#ifdef OPH_DEBUG_MYSQL
				if (compressed)
					printf("ORIGINAL QUERY: " OPH_MERGECUBES2_QUERY2_COMPR_MYSQL "\n", frag_name_out, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, frags[0].value[k].fragment_name,
					       MYSQL_FRAG_ID, MYSQL_FRAG_ID, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[0],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[1],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[0],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[1], frags[0].value[k].fragment_name, MYSQL_FRAG_MEASURE,
					       frags[1].value[k].fragment_name, MYSQL_FRAG_MEASURE, MYSQL_FRAG_MEASURE, frags[0].value[k].db_instance->db_name, frags[0].value[k].fragment_name,
					       frags[1].value[k].db_instance->db_name, frags[1].value[k].fragment_name, frags[0].value[k].fragment_name, MYSQL_FRAG_ID,
					       frags[1].value[k].fragment_name, MYSQL_FRAG_ID);
				else
					printf("ORIGINAL QUERY: " OPH_MERGECUBES2_QUERY2_MYSQL "\n", frag_name_out, MYSQL_FRAG_ID,
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[0],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[1],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[0],
					       ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[1], MYSQL_FRAG_MEASURE, frags[0].value[k].fragment_name,
					       MYSQL_FRAG_ID, MYSQL_FRAG_ID, frags[0].value[k].fragment_name, MYSQL_FRAG_MEASURE, frags[1].value[k].fragment_name, MYSQL_FRAG_MEASURE,
					       MYSQL_FRAG_MEASURE, frags[0].value[k].db_instance->db_name, frags[0].value[k].fragment_name, frags[1].value[k].db_instance->db_name,
					       frags[1].value[k].fragment_name, frags[0].value[k].fragment_name, MYSQL_FRAG_ID, frags[1].value[k].fragment_name, MYSQL_FRAG_ID);
#endif
				long long *list = NULL, list_, list_num = 0;
				if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size >= 0) {
					list_ = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->ensamble_size;
					list = &list_;
					list_num = sizeof(long long);
				}
				//MERGECUBES2 fragment
				if (oph_dc_create_fragment_from_query_with_param
				    (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server, &(frags[0].value[k]), NULL, query, 0, 0, 0, (char *) list, list_num)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_NEW_FRAG_ERROR, frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					free(query);
					query = NULL;
					break;
				}

				free(query);
				query = NULL;

				//Change fragment fields
				frags[0].value[k].id_datacube = id_datacube_out;
				strncpy(frags[0].value[k].fragment_name, 1 + strchr(frag_name_out, '.'), OPH_ODB_STGE_FRAG_NAME_SIZE);
				frags[0].value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;

				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags[0].value[k]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						OPH_LOG_OPH_MERGECUBES_FRAGMENT_INSERT_ERROR, frag_name_out);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server, &(dbmss[0].value[i]));
	}

	if (oph_dc_cleanup_dbms(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_IOPLUGIN_CLEANUP_ERROR,
			(dbmss[0].value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_free_ophidiadb(&oDB_slave);
	for (cc = 0; cc < datacube_num; cc++) {
		oph_odb_stge_free_fragment_list(&(frags[cc]));
		oph_odb_stge_free_db_list(&(dbs[cc]));
		oph_odb_stge_free_dbms_list(&(dbmss[cc]));
	}
	free(frags);
	free(dbs);
	free(dbmss);
	free(input_db);
	free(input_frag);

	if (result == OPH_ANALYTICS_OPERATOR_SUCCESS)
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error = 0;

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	short int proc_error = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_MERGECUBES_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_container,
			 ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_MERGECUBES)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_MERGECUBES, "Output Cube", jsonbuf)) {
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
		if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_id_start_position >= 0 || handle->proc_rank == 0) {
			if ((oph_dproc_delete_data(id_datacube, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0],
						   ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids, 0, 0, 1))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_OPH_DELETE_DB_READ_ERROR);
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
			oph_dproc_clean_odb(&((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0]);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container[0], OPH_LOG_GENERIC_PROCESS_ERROR);

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
		oph_odb_disconnect_from_ophidiadb(&((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->oDB);
		oph_odb_free_ophidiadb(&((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids) {
		free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->fragment_ids = NULL;
	}
	int i = 0;
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type) {
		int input_datacube_num = ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->input_datacube_num + ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->number;
		for (i = 0; i < input_datacube_num; i++) {
			if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[i]) {
				free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[i]);
				((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type[i] = NULL;
			}
		}
		free((char **) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->measure_type = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube) {
		free((char **) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_datacube = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container) {
		free((char **) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->id_input_container = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name) {
		free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_name = NULL;
	}
	if (((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type) {
		free((char *) ((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type);
		((OPH_MERGECUBES2_operator_handle *) handle->operator_handle)->dim_type = NULL;
	}
	free((OPH_MERGECUBES2_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
