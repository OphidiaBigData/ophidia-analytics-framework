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

#include "drivers/OPH_OPERATORS_LIST_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

int strcmp2(const void *s1, const void *s2)
{
	//This function receives pointer to char* while strcmp needs char*, hence the reason for cast and deference 
	return strcmp(*(const char **) s1, *(const char **) s2);
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}


	if (!(handle->operator_handle = (OPH_OPERATORS_LIST_operator_handle *) calloc(1, sizeof(OPH_OPERATORS_LIST_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter = NULL;
	((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->limit = 0;
	((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num = -1;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;


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
	    (value, &((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys, &((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OPERATOR_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OPERATOR_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_OPERATOR_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "name_filter");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LIMIT_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LIMIT_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LIMIT_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->limit = strtol(value, NULL, 10);
	if (((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->limit < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Limit must be positive\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_POSITIVE_LIMIT, OPH_IN_PARAM_LIMIT_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_INFO_START);

	char *name_filter = ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter;
	int limit = ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->limit;

	char list[1000];
	snprintf(list, sizeof(list), OPH_ANALYTICS_DYNAMIC_LIBRARY_FILE_PATH, OPH_ANALYTICS_LOCATION);
	FILE *file = fopen(list, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Drivers list file not found\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_FILE_NOT_FOUND);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char buffer[256], operator[30];
	char *position1, *position2;
	char **oplist;
	int i, j, oplist_size = 0;

	// get oplist size
	if (limit == 0) {
		//no limit
		while (fscanf(file, "%s", buffer) != EOF) {
			position1 = strchr(buffer, '[');
			if (position1 != NULL) {
				position2 = strchr(position1 + 1, ']');
				if (position2 == NULL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Matching closing bracket not found\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_BRACKET_NOT_FOUND);
					fclose(file);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				strncpy(operator, position1 + 1, strlen(position1) - strlen(position2) - 1);
				operator[strlen(position1) - strlen(position2) - 1] = 0;

				if (name_filter == NULL) {
					oplist_size++;
				} else {
					if (strcasestr(operator, name_filter)) {
						oplist_size++;
					}
				}
			}
		}
	} else {
		int count = 0;
		while (fscanf(file, "%s", buffer) != EOF && count < limit) {
			position1 = strchr(buffer, '[');
			if (position1 != NULL) {
				position2 = strchr(position1 + 1, ']');
				if (position2 == NULL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Matching closing bracket not found\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_BRACKET_NOT_FOUND);
					fclose(file);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				strncpy(operator, position1 + 1, strlen(position1) - strlen(position2) - 1);
				operator[strlen(position1) - strlen(position2) - 1] = 0;

				if (name_filter == NULL) {
					oplist_size++;
					count++;
				} else {
					if (strcasestr(operator, name_filter)) {
						oplist_size++;
						count++;
					}
				}
			}
		}
	}
	fclose(file);

	// alloc oplist
	oplist = (char **) malloc(sizeof(char *) * oplist_size);
	if (!oplist) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	for (i = 0; i < oplist_size; i++) {
		oplist[i] = (char *) malloc(OPH_COMMON_BUFFER_LEN);
		if (!oplist[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory for path\n");
			for (j = 0; j < i; j++) {
				free(oplist[j]);
			}
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	// fill oplist
	int p = 0;
	file = fopen(list, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Drivers list file not found\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_FILE_NOT_FOUND);
		for (i = 0; i < oplist_size; i++) {
			free(oplist[i]);
		}
		free(oplist);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (limit == 0) {
		//no limit
		while (fscanf(file, "%s", buffer) != EOF) {
			position1 = strchr(buffer, '[');
			if (position1 != NULL) {
				position2 = strchr(position1 + 1, ']');
				if (position2 == NULL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Matching closing bracket not found\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_BRACKET_NOT_FOUND);
					fclose(file);
					for (i = 0; i < oplist_size; i++) {
						free(oplist[i]);
					}
					free(oplist);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				strncpy(operator, position1 + 1, strlen(position1) - strlen(position2) - 1);
				operator[strlen(position1) - strlen(position2) - 1] = 0;

				if (name_filter == NULL) {
					strncpy(oplist[p], operator, strlen(operator));
					oplist[p][strlen(operator)] = 0;
					p++;
				} else {
					if (strcasestr(operator, name_filter)) {
						strncpy(oplist[p], operator, strlen(operator));
						oplist[p][strlen(operator)] = 0;
						p++;
					}
				}
			}
		}
	} else {
		int count = 0;
		while (fscanf(file, "%s", buffer) != EOF && count < limit) {
			position1 = strchr(buffer, '[');
			if (position1 != NULL) {
				position2 = strchr(position1 + 1, ']');
				if (position2 == NULL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Matching closing bracket not found\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_BRACKET_NOT_FOUND);
					fclose(file);
					for (i = 0; i < oplist_size; i++) {
						free(oplist[i]);
					}
					free(oplist);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				strncpy(operator, position1 + 1, strlen(position1) - strlen(position2) - 1);
				operator[strlen(position1) - strlen(position2) - 1] = 0;

				if (name_filter == NULL) {
					strncpy(oplist[p], operator, strlen(operator));
					oplist[p][strlen(operator)] = 0;
					p++;
					count++;
				} else {
					if (strcasestr(operator, name_filter)) {
						strncpy(oplist[p], operator, strlen(operator));
						oplist[p][strlen(operator)] = 0;
						p++;
						count++;
					}
				}
			}
		}
	}
	fclose(file);

	// sort oplist
	qsort(oplist, oplist_size, sizeof(char *), strcmp2);

	printf("+-----------------------------+\n");
	printf("|          OPERATORS          |\n");
	printf("+-----------------------------+\n");

	for (i = 0; i < oplist_size; i++) {
		printf("| %-27s |\n", oplist[i]);
	}

	printf("+-----------------------------+\n");

	int is_objkey_printable =
	    oph_json_is_objkey_printable(((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys, ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num,
					 OPH_JSON_OBJKEY_OPERATORS_LIST_LIST);
	if (is_objkey_printable) {
		// Header
		char **jsonkeys = NULL;
		char **fieldtypes = NULL;
		int num_fields = 1, iii, jjj = 0;
		jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
		if (!jsonkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "keys");
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jsonkeys[jjj] = strdup("OPERATORS");
		if (!jsonkeys[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "key");
			for (iii = 0; iii < jjj; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		jjj = 0;
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
		if (!fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		fieldtypes[jjj] = strdup(OPH_JSON_STRING);
		if (!fieldtypes[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "fieldtype");
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
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_OPERATORS_LIST_LIST, "Operators List", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
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
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "values");
			for (i = 0; i < oplist_size; i++)
				free(oplist[i]);
			free(oplist);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < oplist_size; i++) {
			for (jjj = 0; jjj < num_fields; jjj++) {
				jsonvalues[jjj] = strdup(oplist[i]);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					for (i = 0; i < oplist_size; i++)
						free(oplist[i]);
					free(oplist);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_OPERATORS_LIST_LIST, jsonvalues)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				for (i = 0; i < oplist_size; i++)
					free(oplist[i]);
				free(oplist);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
		}
		if (jsonvalues)
			free(jsonvalues);
	}

	for (i = 0; i < oplist_size; i++)
		free(oplist[i]);
	free(oplist);

	printf(OPH_OPERATORS_LIST_HELP_MESSAGE);
	if (oph_json_is_objkey_printable
	    (((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys, ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num,
	     OPH_JSON_OBJKEY_OPERATORS_LIST_TIP)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_OPERATORS_LIST_TIP, "Help Message", OPH_OPERATORS_LIST_HELP_MESSAGE)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_INFO_END);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_OPERATORS_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter) {
		free((char *) ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter);
		((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->name_filter = NULL;
	}
	if (((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys,
						      ((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_OPERATORS_LIST_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
