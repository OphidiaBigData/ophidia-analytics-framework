/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#include "drivers/OPH_FOLDER_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

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
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_FOLDER_operator_handle *) calloc(1, sizeof(OPH_FOLDER_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode = -1;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->path = NULL;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->path_num = -1;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->userrole = OPH_ROLE_NONE;

	ophidiadb *oDB = &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//3 - Fill struct with the correct data

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMMAND);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMMAND);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_COMMAND);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcasecmp(value, OPH_FOLDER_CMD_CD)) {
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode = OPH_FOLDER_MODE_CD;
	} else if (!strcasecmp(value, OPH_FOLDER_CMD_MKDIR)) {
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode = OPH_FOLDER_MODE_MKDIR;
	} else if (!strcasecmp(value, OPH_FOLDER_CMD_MV)) {
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode = OPH_FOLDER_MODE_MV;
	} else if (!strcasecmp(value, OPH_FOLDER_CMD_RM)) {
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode = OPH_FOLDER_MODE_RM;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", value);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_INVALID_INPUT_PARAMETER, value);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->path, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->path_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->path_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FOLDER_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERROLE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERROLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_MISSING_INPUT_PARAMETER, OPH_ARG_USERROLE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_FOLDER_operator_handle *) handle->operator_handle)->userrole = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int folderid1, folderid2, answer, last_insertd_id, home_id;
	char *abs_path1 = NULL;
	char *abs_path2 = NULL;
	char *first_part = NULL;
	char *last_token = NULL;
	char *first_part2 = NULL;
	char *last_token2 = NULL;
	char *folder_to_be_printed = NULL, *new_folder_to_be_printed = NULL;
	int permission = 0;
	char home_path[MYSQL_BUFLEN];

	switch (((OPH_FOLDER_operator_handle *) handle->operator_handle)->mode) {

			// COMMAND: CD
		case OPH_FOLDER_MODE_CD:
			if (!strcasecmp(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
				//cd to session home
				if (oph_odb_fs_get_session_home_id
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &home_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (oph_odb_fs_build_path(home_id, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &home_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", home_path);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, home_path);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				folder_to_be_printed = home_path;
			} else {
				//cd to cwd/path
				if (oph_odb_fs_path_parsing
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid1, &abs_path1,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid1, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				folder_to_be_printed = abs_path1;
			}

			// Change folder_to_be_printed in user format
			if (oph_pid_drop_session_prefix(folder_to_be_printed, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &new_folder_to_be_printed)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Folder conversion error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Folder conversion error\n");
				if (abs_path1) {
					free(abs_path1);
					abs_path1 = NULL;
				}
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			if (strlen(new_folder_to_be_printed) > 1)
				new_folder_to_be_printed[strlen(new_folder_to_be_printed) - 1] = 0;
			printf(OPH_FOLDER_CD_MESSAGE " is: %s\n", new_folder_to_be_printed);
			// ADD OUTPUT TO JSON AS TEXT
			if (oph_json_is_objkey_printable
			    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_FOLDER)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_FOLDER, OPH_FOLDER_CD_MESSAGE, new_folder_to_be_printed)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			// ADD OUTPUT CWD TO NOTIFICATION STRING
			char tmp_string[OPH_COMMON_BUFFER_LEN];
			snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_CWD, new_folder_to_be_printed);
			if (handle->output_string) {
				strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
				free(handle->output_string);
			}
			handle->output_string = strdup(tmp_string);

			free(new_folder_to_be_printed);

			break;

			// COMMAND: MKDIR
		case OPH_FOLDER_MODE_MKDIR:
			if (!IS_OPH_ROLE_PRESENT(((OPH_FOLDER_operator_handle *) handle->operator_handle)->userrole, OPH_ROLE_WRITE_POS)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}

			if (!strcasecmp(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path must be specified\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_PRESENT_ERROR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			} else {
				if (oph_odb_fs_str_last_token(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], &first_part, &last_token)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (oph_odb_fs_path_parsing
				    (first_part, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid1, &abs_path1,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid1, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (!oph_odb_fs_is_allowed_name(last_token)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders\n", last_token);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NAME_NOT_ALLOWED_ERROR, last_token);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_is_unique(folderid1, last_token, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &answer)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check uniqueness\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_UNIQUE_CHECK_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (answer == 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "There is already a folder or a visible container with the same name\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NOT_UNIQUE_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				//insert new folder
				if (oph_odb_fs_insert_into_folder_table(&((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, folderid1, last_token, &last_insertd_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_FOLDER_INSERT_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_INSERT_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			}
			break;

			// COMMAND: MV
		case OPH_FOLDER_MODE_MV:
			if (!IS_OPH_ROLE_PRESENT(((OPH_FOLDER_operator_handle *) handle->operator_handle)->userrole, OPH_ROLE_WRITE_POS)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}

			if (!strcasecmp(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], OPH_FRAMEWORK_FS_DEFAULT_PATH)
			    || ((OPH_FOLDER_operator_handle *) handle->operator_handle)->path_num != 2) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path must be specified\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_PRESENT_ERROR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			} else {
				//first path processing start
				if (oph_odb_fs_str_last_token(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], &first_part, &last_token)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (oph_odb_fs_path_parsing
				    (first_part, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid1, &abs_path1,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid1, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (!oph_odb_fs_is_allowed_name(last_token)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", last_token);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, last_token);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_is_visible_container(folderid1, last_token, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &answer)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check object type\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_TYPE_CHECK_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (answer) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "%s is not a folder\n", last_token);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NO_FOLDER_ERROR, last_token);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (abs_path1) {
					free(abs_path1);
					abs_path1 = NULL;
				}
				if (oph_odb_fs_path_parsing
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid1, &abs_path1,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid1, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_get_session_home_id
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &home_id)
				    || folderid1 == home_id) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				//first path processing end

				//second path processing start
				if (oph_odb_fs_str_last_token(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[1], &first_part2, &last_token2)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (oph_odb_fs_path_parsing
				    (first_part2, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid2, &abs_path2,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid2, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path2);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path2);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (!oph_odb_fs_is_allowed_name(last_token2)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", last_token2);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NAME_NOT_ALLOWED_ERROR, last_token2);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_is_unique(folderid2, last_token2, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &answer)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check uniqueness\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_UNIQUE_CHECK_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (answer == 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "There is already a folder or a visible container with the same name\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NOT_UNIQUE_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_has_ascendant_equal_to_folder(&((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, folderid1, folderid2, &answer)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check ascendants\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_ASC_CHECK_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (answer == 1) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "It is not possible to move a folder into one of its subfolders\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_SUBFOLDER_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				//update folder
				if (oph_odb_fs_update_folder_table(&((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, folderid1, folderid2, last_token2)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_FOLDER_UPDATE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_UPDATE_ERROR);
					if (first_part) {
						free(first_part);
						first_part = NULL;
					}
					if (last_token) {
						free(last_token);
						last_token = NULL;
					}
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					if (first_part2) {
						free(first_part2);
						first_part2 = NULL;
					}
					if (last_token2) {
						free(last_token2);
						last_token2 = NULL;
					}
					if (abs_path2) {
						free(abs_path2);
						abs_path2 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//second path processing end
			}
			break;

			// COMMAND: RM
		case OPH_FOLDER_MODE_RM:
			if (!IS_OPH_ROLE_PRESENT(((OPH_FOLDER_operator_handle *) handle->operator_handle)->userrole, OPH_ROLE_WRITE_POS)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}

			if (!strcasecmp(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path must be specified\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_PRESENT_ERROR);
				return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			} else {
				if (oph_odb_fs_path_parsing
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->path[0], ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd, &folderid1, &abs_path1,
				     &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_PARSING_ERROR);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if ((oph_odb_fs_check_folder_session
				     (folderid1, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &permission))
				    || !permission) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_get_session_home_id
				    (((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &home_id)
				    || folderid1 == home_id) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s not allowed\n", abs_path1);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_PATH_NOT_ALLOWED_ERROR, abs_path1);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				if (oph_odb_fs_is_empty_folder(folderid1, &((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, &answer)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check folder emptiness\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_EMPTY_CHECK_ERROR);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				if (answer == 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Folder is not empty\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_FOLDER_NOT_EMPTY_ERROR);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}
				//remove folder
				if (oph_odb_fs_delete_from_folder_table(&((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB, folderid1)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_FOLDER_DELETE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_DELETE_ERROR);
					if (abs_path1) {
						free(abs_path1);
						abs_path1 = NULL;
					}
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
			}
			break;

		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", OPH_IN_PARAM_COMMAND);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_INVALID_INPUT_PARAMETER, OPH_IN_PARAM_COMMAND);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (abs_path1) {
		free(abs_path1);
		abs_path1 = NULL;
	}
	if (abs_path2) {
		free(abs_path2);
		abs_path2 = NULL;
	}
	if (first_part) {
		free(first_part);
		first_part = NULL;
	}
	if (last_token) {
		free(last_token);
		last_token = NULL;
	}
	if (first_part2) {
		free(first_part2);
		first_part2 = NULL;
	}
	if (last_token2) {
		free(last_token2);
		last_token2 = NULL;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FOLDER_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_FOLDER_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd);
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_FOLDER_operator_handle *) handle->operator_handle)->path) {
		oph_tp_free_multiple_value_param_list(((OPH_FOLDER_operator_handle *) handle->operator_handle)->path, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->path_num);
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->path = NULL;
	}
	if (((OPH_FOLDER_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_FOLDER_operator_handle *) handle->operator_handle)->user);
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_FOLDER_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_FOLDER_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
