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

#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "drivers/OPH_METADATA_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_pid_library.h"

int oph_metadata_crud(OPH_METADATA_operator_handle * handle, MYSQL_RES ** read_result)
{
	*read_result = NULL;
	ophidiadb *oDB = &((OPH_METADATA_operator_handle *) handle)->oDB;

	//Fill and execute the appropriate query
	switch (handle->mode) {
		case OPH_METADATA_MODE_INSERT_VALUE:
			{
				if (!IS_OPH_ROLE_PRESENT(handle->userrole, OPH_ROLE_WRITE_POS)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}

				int idkey, idtype, idmetadatainstance;
				if (!handle->metadata_keys || !handle->metadata_value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_INSERT_INSTANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_INSERT_INSTANCE_ERROR);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//retrieve type id
				if (oph_odb_meta_retrieve_metadatatype_id(oDB, handle->metadata_type, &idtype)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_TYPE_ID_ERROR, handle->metadata_type);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_TYPE_ID_ERROR, handle->metadata_type);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}

				char **metadata_values = NULL;
				int i, metadata_values_num = 0;
				if (handle->metadata_keys_num > 1) {
					if (oph_tp_parse_multiple_value_param(handle->metadata_value, &metadata_values, &metadata_values_num)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
						oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
						return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					if (handle->metadata_keys_num != metadata_values_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
						oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
						return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
				}

				for (i = 0; i < handle->metadata_keys_num; ++i) {
					//retrieve key id
					if (oph_odb_meta_retrieve_metadatakey_id(oDB, handle->metadata_keys[i], handle->variable, handle->id_container_input, &idkey)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_KEY_ID_ERROR, handle->metadata_keys[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_KEY_ID_ERROR, handle->metadata_keys[i]);
						if (handle->metadata_keys_num > 1)
							oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
						return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					//insert into medatainstance table
					if (oph_odb_meta_insert_into_metadatainstance_table
					    (oDB, handle->id_datacube_input, idkey, idtype, handle->metadata_keys[i], handle->variable,
					     handle->metadata_keys_num > 1 ? metadata_values[i] : handle->metadata_value, &idmetadatainstance)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_INSERT_INSTANCE_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_INSERT_INSTANCE_ERROR);
						if (handle->metadata_keys_num > 1)
							oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					//insert into manage table
					if (oph_odb_meta_insert_into_manage_table(oDB, idmetadatainstance, handle->id_user)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_INSERT_MANAGE_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_INSERT_MANAGE_ERROR);
						if (handle->metadata_keys_num > 1)
							oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}

				if (handle->metadata_keys_num > 1)
					oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);

				break;
			}
		case OPH_METADATA_MODE_READ_VALUE:
			{
				char *id_metadatainstance = NULL;
				if (handle->metadata_id > 0)	//read one or more metadata with filters
					id_metadatainstance = handle->metadata_id_str;
				if (oph_odb_meta_find_complete_metadata_list
				    (oDB, handle->id_datacube_input, (const char **) handle->metadata_keys, handle->metadata_keys_num, id_metadatainstance, handle->variable_filter,
				     handle->metadata_type_filter, handle->metadata_value_filter, read_result)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_READ_METADATA_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_READ_METADATA_ERROR);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				break;
			}
		case OPH_METADATA_MODE_UPDATE_VALUE:
			{
				if (!IS_OPH_ROLE_PRESENT(handle->userrole, OPH_ROLE_WRITE_POS)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}

				int exists = 0;
				if (!handle->metadata_value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}

				if (handle->metadata_id) {	// Update by id (more specific)

					//Check metadata instance id
					if (oph_odb_meta_check_metadatainstance_existance(oDB, handle->metadata_id, handle->id_datacube_input, &exists) || !exists) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_METADATAINSTANCE_FORCE_ERROR, handle->metadata_id);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_METADATAINSTANCE_FORCE_ERROR, handle->metadata_id);
						return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					//update medatainstance table
					if (oph_odb_meta_update_metadatainstance_table(oDB, handle->metadata_id, handle->id_datacube_input, handle->metadata_value, handle->force)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}

				} else if (handle->metadata_keys_num) {	// Update by label (less specific)

					char **metadata_values = NULL;
					int i, idinstance, metadata_values_num = 0;
					if (handle->metadata_keys_num > 1) {
						if (oph_tp_parse_multiple_value_param(handle->metadata_value, &metadata_values, &metadata_values_num)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
							oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
						}
						if (handle->metadata_keys_num != metadata_values_num) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_VALUE_ERROR, handle->metadata_value);
							oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
						}
					}
					for (i = 0; i < handle->metadata_keys_num; ++i) {
						//retrieve key id
						if (oph_odb_meta_retrieve_metadatainstance_id(oDB, handle->metadata_keys[i], handle->variable, handle->id_datacube_input, &idinstance) || !idinstance) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_RETRIEVE_KEY_ID_ERROR, handle->metadata_keys[i]);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_RETRIEVE_KEY_ID_ERROR, handle->metadata_keys[i]);
							if (handle->metadata_keys_num > 1)
								oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
						}
						//update into medatainstance table
						if (oph_odb_meta_update_metadatainstance_table
						    (oDB, idinstance, handle->id_datacube_input, handle->metadata_keys_num > 1 ? metadata_values[i] : handle->metadata_value, handle->force)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
							if (handle->metadata_keys_num > 1)
								oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
					}
					if (handle->metadata_keys_num > 1)
						oph_tp_free_multiple_value_param_list(metadata_values, metadata_values_num);

				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_UPDATE_INSTANCE_ERROR);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}

				break;
			}
		case OPH_METADATA_MODE_DELETE_VALUE:
			{
				if (!IS_OPH_ROLE_PRESENT(handle->userrole, OPH_ROLE_WRITE_POS)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "At least \"%s\" permission is needed for this particular operation\n", OPH_ROLE_WRITE_STR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_INVALID_USERROLE_ERROR, OPH_ROLE_WRITE_STR);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
				}

				if (oph_odb_meta_delete_from_metadatainstance_table
				    (oDB, handle->id_datacube_input, (const char **) handle->metadata_keys, handle->metadata_keys_num, handle->metadata_id, handle->variable_filter,
				     handle->metadata_type_filter, handle->metadata_value_filter, handle->force)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_DELETE_INSTANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_DELETE_INSTANCE_ERROR);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}

				break;
			}
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_METADATA_MODE_ERROR, handle->mode);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MODE_ERROR, handle->mode);
			return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	//End of query processing

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_METADATA_operator_handle *) calloc(1, sizeof(OPH_METADATA_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_METADATA_operator_handle *) handle->operator_handle)->id_datacube_input = 0;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input = 0;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id = 0;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id_str = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num = 0;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->variable = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->variable_filter = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->mode = -1;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->force = 0;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_METADATA_operator_handle *) handle->operator_handle)->userrole = OPH_ROLE_NONE;

	ophidiadb *oDB = &((OPH_METADATA_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
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
	if (oph_tp_parse_multiple_value_param(value, &((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, &((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MODE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MODE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MODE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcasecmp(value, OPH_METADATA_MODE_INSERT)) {
		((OPH_METADATA_operator_handle *) handle->operator_handle)->mode = OPH_METADATA_MODE_INSERT_VALUE;
	} else if (!strcasecmp(value, OPH_METADATA_MODE_READ)) {
		((OPH_METADATA_operator_handle *) handle->operator_handle)->mode = OPH_METADATA_MODE_READ_VALUE;
	} else if (!strcasecmp(value, OPH_METADATA_MODE_UPDATE)) {
		((OPH_METADATA_operator_handle *) handle->operator_handle)->mode = OPH_METADATA_MODE_UPDATE_VALUE;
	} else if (!strcasecmp(value, OPH_METADATA_MODE_DELETE)) {
		((OPH_METADATA_operator_handle *) handle->operator_handle)->mode = OPH_METADATA_MODE_DELETE_VALUE;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", value);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_INVALID_INPUT_PARAMETER, value);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	//Check if user can access container and retrieve container id
	value = hashtbl_get(task_tbl, OPH_ARG_USERID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_ARG_USERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_METADATA_operator_handle *) handle->operator_handle)->id_user = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check if datacube exists (by ID container and datacube)
	int exists = 0;
	int status = 0;
	int folder_id = 0;
	int permission = 0;
	char *uri = NULL;
	if (oph_pid_parse_pid
	    (value, &(((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input), &(((OPH_METADATA_operator_handle *) handle->operator_handle)->id_datacube_input), &uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, OPH_LOG_OPH_METADATA_PID_ERROR, value);
		if (uri)
			free(uri);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	} else
	    if ((oph_odb_cube_check_if_datacube_not_present_by_pid
		 (oDB, uri, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_datacube_input, &exists))
		|| !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, OPH_LOG_OPH_METADATA_NO_INPUT_DATACUBE, value);
		if (uri)
			free(uri);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	} else if ((oph_odb_cube_check_datacube_availability(oDB, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_datacube_input, 0, &status)) || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, OPH_LOG_OPH_METADATA_DATACUBE_AVAILABILITY_ERROR, value);
		if (uri)
			free(uri);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	} else if ((oph_odb_fs_retrive_container_folder_id(oDB, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, &folder_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, OPH_LOG_OPH_METADATA_DATACUBE_FOLDER_ERROR, value);
		if (uri)
			free(uri);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	} else if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", ((OPH_METADATA_operator_handle *) handle->operator_handle)->user);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_METADATA_operator_handle *) handle->operator_handle)->id_container_input, OPH_LOG_OPH_METADATA_DATACUBE_PERMISSION_ERROR,
			((OPH_METADATA_operator_handle *) handle->operator_handle)->user);
		if (uri)
			free(uri);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (uri)
		free(uri);
	uri = NULL;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_KEY);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_KEY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_KEY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys, &((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys,
						      ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//If first value supplied as key is ALL, then free the structure and set values to NULL and 0
	if (strncmp(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys[0], OPH_COMMON_ALL_FILTER, strlen(OPH_COMMON_ALL_FILTER)) == 0) {
		oph_tp_free_multiple_value_param_list(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys,
						      ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys = NULL;
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num = 0;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_ID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_ID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_ID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id_str = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_ID);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_VARIABLE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_VARIABLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_VARIABLE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_GLOBAL_VALUE)) {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->variable = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_VARIABLE);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_TYPE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_VALUE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_VALUE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_VALUE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN) != 0) {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_VALUE);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_TYPE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_TYPE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_TYPE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_TYPE_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_TYPE_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_VARIABLE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_VARIABLE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_VARIABLE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->variable_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_VARIABLE_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_METADATA_VALUE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_METADATA_VALUE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_METADATA_VALUE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_VALUE_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, OPH_IN_PARAM_METADATA_VALUE_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FORCE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FORCE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_FORCE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
		((OPH_METADATA_operator_handle *) handle->operator_handle)->force = 1;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERROLE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERROLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MISSING_INPUT_PARAMETER, OPH_ARG_USERROLE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_METADATA_operator_handle *) handle->operator_handle)->userrole = (int) strtol(value, NULL, 10);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	MYSQL_RES *read_result = NULL;
	MYSQL_FIELD *fields;
	MYSQL_ROW row;
	int num_fields;
	int num_rows;
	int i, j, len;

	//execute requested crud operation
	if (oph_metadata_crud(((OPH_METADATA_operator_handle *) handle->operator_handle), &read_result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to manage metadata\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_CRUD_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->mode == OPH_METADATA_MODE_READ_VALUE) {
		num_rows = mysql_num_rows(read_result);
		char message[OPH_COMMON_BUFFER_LEN];
		snprintf(message, OPH_COMMON_BUFFER_LEN, "Found %d items", num_rows);
		printf("%s\n", message);
		if (oph_json_is_objkey_printable
		    (((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_METADATA_SUMMARY)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_METADATA_SUMMARY, "Summary", message)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		//Empty set
		if (!num_rows) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NO_ROWS_FOUND);
			mysql_free_result(read_result);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}

		fields = mysql_fetch_fields(read_result);
		num_fields = mysql_num_fields(read_result);

		printf("+");
		for (i = 0; i < num_fields; i++) {
			printf("-");
			len = (fields[i].max_length > fields[i].name_length) ? fields[i].max_length : fields[i].name_length;
			for (j = 0; j < len; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");

		printf("|");
		for (i = 0; i < num_fields; i++) {
			printf(" ");
			len = (fields[i].max_length > fields[i].name_length) ? fields[i].max_length : fields[i].name_length;
			printf("%-*s", len, fields[i].name);
			printf(" |");
		}
		printf("\n");

		int objkey_printable =
		    oph_json_is_objkey_printable(((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_METADATA_LIST), iii, jjj;
		if (objkey_printable) {
			char **jsonkeys = NULL;
			char **fieldtypes = NULL;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "keys");
				mysql_free_result(read_result);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; ++jjj) {
				jsonkeys[jjj] = strdup(fields[jjj].name ? fields[jjj].name : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					mysql_free_result(read_result);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				mysql_free_result(read_result);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			for (jjj = 0; jjj < num_fields; ++jjj) {
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "fieldtype");
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
					mysql_free_result(read_result);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_METADATA_LIST, "Searching results", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
				mysql_free_result(read_result);
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
		}

		printf("+");
		for (i = 0; i < num_fields; i++) {
			printf("-");
			len = (fields[i].max_length > fields[i].name_length) ? fields[i].max_length : fields[i].name_length;
			for (j = 0; j < len; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");

		//For each ROW
		while ((row = mysql_fetch_row(read_result))) {
			printf("|");
			for (i = 0; i < num_fields; i++) {
				printf(" ");
				len = (fields[i].max_length > fields[i].name_length) ? fields[i].max_length : fields[i].name_length;
				printf("%-*s", len, row[i] ? row[i] : "");
				printf(" |");
			}
			printf("\n");

			if (objkey_printable) {
				char **jsonvalues = NULL;
				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "values");
					mysql_free_result(read_result);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				for (jjj = 0; jjj < num_fields; ++jjj) {
					jsonvalues[jjj] = strdup(row[jjj] ? row[jjj] : "");
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						mysql_free_result(read_result);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_METADATA_LIST, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					mysql_free_result(read_result);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
			}
		}

		printf("+");
		for (i = 0; i < num_fields; i++) {
			printf("-");
			len = (fields[i].max_length > fields[i].name_length) ? fields[i].max_length : fields[i].name_length;
			for (j = 0; j < len; j++) {
				printf("-");
			}
			printf("-+");
		}
		printf("\n");

		mysql_free_result(read_result);
	} else {
		char message[OPH_COMMON_BUFFER_LEN];
		snprintf(message, OPH_COMMON_BUFFER_LEN, "Operation successfully completed!");
		printf("%s\n", message);
		if (oph_json_is_objkey_printable
		    (((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_METADATA_SUMMARY)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_METADATA_SUMMARY, "Summary", message)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_METADATA_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_METADATA_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_METADATA_operator_handle *) handle->operator_handle)->oDB);

	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id_str) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id_str);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_id_str = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys) {
		oph_tp_free_multiple_value_param_list(((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys,
						      ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys_num);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_keys = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->variable) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->variable);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->variable = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->variable_filter) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->variable_filter);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->variable_filter = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_type_filter = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->metadata_value_filter = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->user);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys, ((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_METADATA_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_METADATA_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
