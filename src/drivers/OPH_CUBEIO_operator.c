/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#include "drivers/OPH_CUBEIO_operator.h"
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

	if (!(handle->operator_handle = (OPH_CUBEIO_operator_handle *) calloc(1, sizeof(OPH_CUBEIO_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->direction = 0;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_datacube = 0;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid = NULL;

	ophidiadb *oDB = &((OPH_CUBEIO_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);


	//3 - Fill struct with the correct data
	char *datacube_name, *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys, &((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_name = value;
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_OPHIDIADB_CONFIGURATION_FILE);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_OPHIDIADB_CONNECTION_ERROR);

		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *username = value;

	int id_container = 0, id_datacube = 0;

	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	char *uri = NULL;
	int folder_id = 0;
	int permission = 0;

	if (oph_pid_parse_pid(datacube_name, &id_container, &id_datacube, &uri)) {
		if (uri)
			free(uri);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBEIO_PID_ERROR, datacube_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_container, id_datacube, &exists)) || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBEIO_NO_INPUT_DATACUBE, datacube_name);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	} else if ((oph_odb_fs_retrive_container_folder_id(oDB, id_container, &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBEIO_DATACUBE_FOLDER_ERROR, datacube_name);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBEIO_DATACUBE_PERMISSION_ERROR, username);
		free(uri);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	free(uri);
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container = id_container;
	((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_datacube = id_datacube;


	if (!(((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name = (char *) strndup(datacube_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR_INPUT, "datacube name");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}


	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CUBEIO_HIERARCHY_DIRECTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CUBEIO_HIERARCHY_DIRECTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CUBEIO_HIERARCHY_DIRECTION);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_CUBEIO_PARENT_VALUE) == 0)
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->direction = OPH_CUBEIO_BRANCH_IN;
	else if (strcmp(value, OPH_CUBEIO_CHILDREN_VALUE) == 0)
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->direction = OPH_CUBEIO_BRANCH_OUT;
	else
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->direction = OPH_CUBEIO_BRANCH_BOTH;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_cubeio_print_children(char *uri, ophidiadb * oDB, unsigned int level, int id_datacube_root, int **child_type, int id_container_in, oph_json * oper_json, char **objkeys, int objkeys_num)
{
	if (id_datacube_root < 1 || id_container_in < 1 || (*child_type) == NULL || !oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NULL_FUNCTION_PARAMS_CHILD, id_datacube_root);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	level++;

	MYSQL_RES *io_datacubes = NULL;
	int num_rows = 0;
	MYSQL_ROW row;
	unsigned int i, j;
	int *new_type = NULL;
	char source_buffer[OPH_PID_SIZE];
	char source_buffer_json[OPH_PID_SIZE];

	if (oph_odb_cube_find_datacube_hierarchy(oDB, 0, id_datacube_root, &io_datacubes)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive information of input output datacubes\n");
		logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_IO_DATACUBE_READ_ERROR, id_datacube_root);
		mysql_free_result(io_datacubes);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}
	//Empty set
	if (!(num_rows = mysql_num_rows(io_datacubes))) {
		logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NO_ROWS_FOUND);
	} else {
		if (mysql_field_count(oDB->conn) != 4) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No enough fields found by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NO_ROWS_FOUND);
			mysql_free_result(io_datacubes);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		i = num_rows - 1;
		char *pid = NULL;
		char *pid2 = NULL;	//root name
		if (uri) {
			if (oph_pid_create_pid(uri, id_container_in, id_datacube_root, &pid2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
				if (pid2)
					free(pid2);
				pid2 = NULL;
			}
		}

		while ((row = mysql_fetch_row(io_datacubes))) {
			new_type = (int *) realloc(*child_type, (level + 1) * sizeof(int));
			if (!new_type) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "child_type array");
				mysql_free_result(io_datacubes);
				if (pid2)
					free(pid2);
				pid2 = NULL;
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			} else {
				*child_type = new_type;
			}
			(*child_type)[level] = i;

			//Shift output string proportionally to its level
			for (j = 1; j < level; j++) {
				if ((*child_type)[j]) {
					printf(" │   ");
				} else {
					printf("     ");
				}
			}

			if (uri && row[0] && row[2]) {
				if (oph_pid_create_pid(uri, (int) strtol(row[0], NULL, 10), (int) strtol(row[2], NULL, 10), &pid)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
					if (pid)
						free(pid);
					pid = NULL;
				}
			}

			if (row[3]) {
				snprintf(source_buffer, OPH_PID_SIZE, "- SOURCE: %s", row[3]);
				snprintf(source_buffer_json, OPH_PID_SIZE, "%s", row[3]);
			} else {
				source_buffer[0] = 0;
				source_buffer_json[0] = 0;
			}

			//If last element of tree
			if (!i) {
				printf(" └─ %s (%s) %s\n", (pid ? pid : "-"), (row[1] ? row[1] : "-"), source_buffer);
			} else {
				printf(" ├─ %s (%s) %s\n", (pid ? pid : "-"), (row[1] ? row[1] : "-"), source_buffer);
			}
			i--;

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO)) {
				char *my_row[4] = { (pid2 ? pid2 : ""), (row[1] ? row[1] : ""), (pid ? pid : ""), source_buffer_json };
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_CUBEIO, my_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output grid row");
					mysql_free_result(io_datacubes);
					if (pid)
						free(pid);
					if (pid2)
						free(pid2);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
				unsigned int nn, nnn;
				int found = 0;
				for (nn = 0; nn < oper_json->response_num; nn++) {
					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
							if (!strcmp((pid ? pid : ""), ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0])) {
								found = 1;
								break;
							}
						}
						break;
					}
				}
				char *my_row[2] = { (pid ? pid : ""), source_buffer_json };
				if (!found) {
					if (oph_json_add_graph_node(oper_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, my_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH NODE error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph node");
						mysql_free_result(io_datacubes);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
				int node1 = -1, node2 = -1;
				for (nn = 0; nn < oper_json->response_num; nn++) {
					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
							if (node1 != -1 && node2 != -1)
								break;
							if (!strcmp((pid2 ? pid2 : ""), ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0]))
								node1 = nnn;
							else if (!strcmp((pid ? pid : ""), ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0]))
								node2 = nnn;
						}
						break;
					}
				}

				int node_link = 0;
				found = 0;
				for (nn = 0; nn < oper_json->response_num; nn++) {
					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) oper_json->response[nn].objcontent)[0].nodelinks[node1].links_num; nnn++) {
							node_link = (int) strtol(((oph_json_obj_graph *) oper_json->response[nn].objcontent)[0].nodelinks[node1].links[nnn].node, NULL, 10);
							if (node_link == node2) {
								found = 1;
								break;
							}
						}
						if (found == 1)
							break;
					}
				}
				if (!found) {
					if (oph_json_add_graph_link(oper_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, node1, node2, (const char *) (row[1] ? row[1] : ""))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH LINK error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph link");
						mysql_free_result(io_datacubes);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
			}

			if (_oph_cubeio_print_children(uri, oDB, level, (int) strtol(row[2], NULL, 10), child_type, id_container_in, oper_json, objkeys, objkeys_num))
				break;

			if (pid)
				free(pid);
			pid = NULL;

		}
		if (pid2)
			free(pid2);
		pid2 = NULL;
	}
	mysql_free_result(io_datacubes);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_cubeio_print_parents(char *uri, ophidiadb * oDB, unsigned int level, int id_datacube_leaf, int **parent_type, int id_container_in, char (*operation)[OPH_ODB_CUBE_OPERATOR_SIZE],
			      oph_json * oper_json, char **objkeys, int objkeys_num, char childcube[OPH_COMMON_BUFFER_LEN], char (*parentcube)[OPH_COMMON_BUFFER_LEN])
{
	if (id_datacube_leaf < 1 || id_container_in < 1 || (*parent_type) == 0 || !oDB || !operation || !childcube || !parentcube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NULL_FUNCTION_PARAMS_PARENT, id_datacube_leaf);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	level++;

	MYSQL_RES *io_datacubes = NULL;
	int num_rows = 0;
	MYSQL_ROW row;
	unsigned int i, j, length;
	int *new_type = NULL;
	int kk = 0;

	if (oph_odb_cube_find_datacube_hierarchy(oDB, 1, id_datacube_leaf, &io_datacubes)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive information of input output datacubes\n");
		logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_IO_DATACUBE_READ_ERROR, id_datacube_leaf);
		mysql_free_result(io_datacubes);
		//Prepare operation for root element
		length = strlen("ROOT") + 1;
		strncpy(*operation, "ROOT", length);
		(*operation)[length] = '\0';
		snprintf(*parentcube, OPH_COMMON_BUFFER_LEN, "%s", "");
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	char parent_operation[OPH_ODB_CUBE_OPERATOR_SIZE];
	memset(parent_operation, 0, OPH_ODB_CUBE_OPERATOR_SIZE);
	char parent_parentcube[OPH_COMMON_BUFFER_LEN];
	memset(parent_parentcube, 0, OPH_COMMON_BUFFER_LEN);
	char source_buffer[OPH_PID_SIZE];
	char source_buffer_json[OPH_PID_SIZE];

	//Empty set
	if (!(num_rows = mysql_num_rows(io_datacubes))) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
		logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NO_ROWS_FOUND);
		//Prepare operation for root element
		length = strlen("ROOT") + 1;
		strncpy(*operation, "ROOT", length);
		(*operation)[length] = '\0';
		snprintf(*parentcube, OPH_COMMON_BUFFER_LEN, "%s", "");
	} else {
		if (mysql_field_count(oDB->conn) != 4) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_NO_ROWS_FOUND);
			mysql_free_result(io_datacubes);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		i = 0;
		char *pid;
		int ii = 0;

		while ((row = mysql_fetch_row(io_datacubes))) {
			new_type = (int *) realloc(*parent_type, (level + 1) * sizeof(int));
			if (!new_type) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_in, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "parent_type array");
				mysql_free_result(io_datacubes);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			} else {
				*parent_type = new_type;
			}
			(*parent_type)[level] = i;

			if (uri && row[0] && row[2]) {
				if (oph_pid_create_pid(uri, (int) strtol(row[0], NULL, 10), (int) strtol(row[2], NULL, 10), &pid)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
					if (pid)
						free(pid);
					pid = NULL;
				}
			}
			//Prepare operation for children elements
			length = (strlen(row[1]) >= (OPH_ODB_CUBE_OPERATOR_SIZE - 1) ? OPH_ODB_CUBE_OPERATOR_SIZE : strlen(row[1]) + 1);
			strncpy(*operation, (row[1] ? row[1] : "-"), length);
			(*operation)[length] = '\0';

			if (row[3]) {
				snprintf(source_buffer, OPH_PID_SIZE, "- SOURCE: %s", row[3]);
				snprintf(source_buffer_json, OPH_PID_SIZE, "%s", row[3]);
			} else {
				source_buffer[0] = 0;
				source_buffer_json[0] = 0;
			}

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
				unsigned int nn, nnn;
				int found = 0;
				for (nn = 0; nn < oper_json->response_num; nn++) {
					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
							if (!strcmp((pid ? pid : ""), ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0])) {
								found = 1;
								break;
							}
						}
						break;
					}
				}
				char *my_row[2] = { (pid ? pid : ""), source_buffer_json };
				if (!found) {
					if (oph_json_add_graph_node(oper_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, my_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH NODE error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph node");
						mysql_free_result(io_datacubes);
						if (pid)
							free(pid);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}

				int node1 = -1, node2 = -1;
				for (nn = 0; nn < oper_json->response_num; nn++) {

					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
							if (node1 != -1 && node2 != -1)
								break;
							if (!strcmp((pid ? pid : ""), ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0]))
								node1 = nnn;
							else if (!strcmp(childcube, ((oph_json_obj_graph *) (oper_json->response[nn].objcontent))[0].nodevalues[nnn][0]))
								node2 = nnn;
						}
						break;
					}
				}

				int node_link = 0;
				found = 0;
				for (nn = 0; nn < oper_json->response_num; nn++) {
					if (!strcmp(oper_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) oper_json->response[nn].objcontent)[0].nodelinks[node1].links_num; nnn++) {
							node_link = (int) strtol(((oph_json_obj_graph *) oper_json->response[nn].objcontent)[0].nodelinks[node1].links[nnn].node, NULL, 10);
							if (node_link == node2) {
								found = 1;
								break;
							}
						}
						if (found == 1)
							break;
					}
				}
				if (!found) {
					if (oph_json_add_graph_link(oper_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, node1, node2, (const char *) operation)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH LINK error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph link");
						mysql_free_result(io_datacubes);
						if (pid)
							free(pid);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
			}

			if (_oph_cubeio_print_parents
			    (uri, oDB, level, (int) strtol(row[2], NULL, 10), parent_type, id_container_in, &parent_operation, oper_json, objkeys, objkeys_num, pid, &parent_parentcube))
				break;

			ii++;
			if (ii == num_rows)
				kk += snprintf((*parentcube) + kk, OPH_COMMON_BUFFER_LEN - kk, "%s", (pid ? pid : ""));
			else
				kk += snprintf((*parentcube) + kk, OPH_COMMON_BUFFER_LEN - kk, "%s - ", (pid ? pid : ""));

			//Shift output string proportionally to its level
			for (j = 1; j < level; j++) {
				if ((*parent_type)[j]) {
					printf(" │   ");
				} else {
					printf("     ");
				}
			}

			//If first element of tree
			if (!i) {
				printf(" ┌─ %s (%s) %s\n", (pid ? pid : "-"), parent_operation, source_buffer);
			} else {
				printf(" ├─ %s (%s) %s\n", (pid ? pid : "-"), parent_operation, source_buffer);
			}
			i++;

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO)) {
				char *my_row[4] = { parent_parentcube, parent_operation, (pid ? pid : ""), source_buffer_json };
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_CUBEIO, my_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output grid row");
					mysql_free_result(io_datacubes);
					if (pid)
						free(pid);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}


			if (pid)
				free(pid);
			pid = NULL;
		}
	}
	mysql_free_result(io_datacubes);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_CUBEIO_operator_handle *) handle->operator_handle)->oDB;
	int direction = ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->direction;
	char *datacube_name = ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name;
	char **objkeys = ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys;
	int objkeys_num = ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys_num;

	char operation[OPH_ODB_CUBE_OPERATOR_SIZE];
	memset(operation, 0, OPH_ODB_CUBE_OPERATOR_SIZE);
	char parentcube_name[OPH_COMMON_BUFFER_LEN];
	memset(parentcube_name, 0, OPH_COMMON_BUFFER_LEN);
	int *type = (int *) malloc(sizeof(int));
	type[0] = 0;

	oph_odb_datacube cube;
	oph_odb_cube_init_datacube(&cube);

	//retrieve input datacube
	if (oph_odb_cube_retrieve_datacube(oDB, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_datacube, &cube)) {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_DATACUBE_READ_ERROR);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	char source_buffer[OPH_PID_SIZE];
	char source_buffer_json[OPH_PID_SIZE];
	oph_odb_source src;
	if (cube.id_source) {
		//retrieve input datacube
		if (oph_odb_cube_retrieve_source(oDB, cube.id_source, &src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving source name\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_SOURCE_READ_ERROR);
			oph_odb_cube_free_datacube(&cube);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		} else {
			snprintf(source_buffer, OPH_PID_SIZE, "- SOURCE: %s", src.uri);
			snprintf(source_buffer_json, OPH_PID_SIZE, "%s", src.uri);
		}
	} else {
		source_buffer[0] = 0;
		source_buffer_json[0] = 0;
	}

	oph_odb_cube_free_datacube(&cube);

	char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_PID_URI_ERROR);
		if (tmp_uri)
			free(tmp_uri);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO)) {
		int num_fields = 4;
		char *keys[4] = { "INPUT CUBE", "OPERATION", "OUTPUT CUBE", "SOURCE" };
		char *fieldtypes[4] = { "string", "string", "string", "string" };
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO, "Cube Provenance", NULL, keys, num_fields, fieldtypes, num_fields)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output grid");
			free(type);
			if (tmp_uri)
				free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
		int num_fields = 2;
		char *keys[2] = { "PID", "SOURCE" };
		if (oph_json_add_graph(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, 1, "Cube Provenance Graph", NULL, keys, num_fields)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph");
			free(type);
			if (tmp_uri)
				free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	if (direction != OPH_CUBEIO_BRANCH_OUT) {
		//Retrieve upper hierarchy
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
			unsigned int nn, nnn;
			int found = 0;
			for (nn = 0; nn < handle->operator_json->response_num; nn++) {
				if (!strcmp(handle->operator_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
					for (nnn = 0; nnn < ((oph_json_obj_graph *) (handle->operator_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
						if (!strcmp(datacube_name, ((oph_json_obj_graph *) (handle->operator_json->response[nn].objcontent))[0].nodevalues[nnn][0])) {
							found = 1;
							break;
						}
					}
					break;
				}
			}
			char *my_row[2] = { datacube_name, source_buffer_json };
			if (!found) {
				if (oph_json_add_graph_node(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, my_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH NODE error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph node");
					free(type);
					if (tmp_uri)
						free(tmp_uri);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
		}
		if (_oph_cubeio_print_parents
		    (tmp_uri, oDB, 0, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_datacube, &type, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container,
		     &operation, handle->operator_json, objkeys, objkeys_num, datacube_name, &parentcube_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error launching parents search function\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_HIERARCHY_FUNCT_ERROR, "parent",
				datacube_name);
			free(type);
			if (tmp_uri)
				free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		printf("--> %s (%s) %s\n", datacube_name, operation, source_buffer);

		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO)) {
			char *my_row[4] = { parentcube_name, operation, datacube_name, source_buffer_json };
			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO, my_row)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output grid row");
				free(type);
				if (tmp_uri)
					free(tmp_uri);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}

	}
	if (direction != OPH_CUBEIO_BRANCH_IN) {
		//Retrive lower hierarchy
		if (direction != 0) {
			printf("--> %s (%s) %s\n", datacube_name, "ROOT", source_buffer);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO)) {
				char *my_row[4] = { "", "ROOT", datacube_name, source_buffer_json };
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO, my_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output grid row");
					free(type);
					if (tmp_uri)
						free(tmp_uri);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
				unsigned int nn, nnn;
				int found = 0;
				for (nn = 0; nn < handle->operator_json->response_num; nn++) {
					if (!strcmp(handle->operator_json->response[nn].objkey, OPH_JSON_OBJKEY_CUBEIO_GRAPH)) {
						for (nnn = 0; nnn < ((oph_json_obj_graph *) (handle->operator_json->response[nn].objcontent))[0].nodevalues_num1; nnn++) {
							if (!strcmp(datacube_name, ((oph_json_obj_graph *) (handle->operator_json->response[nn].objcontent))[0].nodevalues[nnn][0])) {
								found = 1;
								break;
							}
						}
						break;
					}
				}
				char *my_row[2] = { datacube_name, source_buffer_json };
				if (!found) {
					if (oph_json_add_graph_node(handle->operator_json, OPH_JSON_OBJKEY_CUBEIO_GRAPH, my_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRAPH NODE error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_CUBEIO_MEMORY_ERROR, "output graph node");
						free(type);
						if (tmp_uri)
							free(tmp_uri);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
			}
		}
		if (_oph_cubeio_print_children
		    (tmp_uri, oDB, 0, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_datacube, &type, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container,
		     handle->operator_json, objkeys, objkeys_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error launching children search function\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_HIERARCHY_FUNCT_ERROR, "children",
				datacube_name);
			free(type);
			if (tmp_uri)
				free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	if (tmp_uri)
		free(tmp_uri);
	free(type);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_CUBEIO_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_CUBEIO_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_CUBEIO_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name) {
		free((char *) ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name);
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	}
	if (((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys, ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_CUBEIO_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_CUBEIO_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
