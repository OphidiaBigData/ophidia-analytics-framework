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

#include "drivers/OPH_LIST_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"
#include "oph_json_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_pid_library.h"
#include "oph_utility_library.h"
#include "oph_subset_library.h"
#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#define OPH_SHORT_STRING_SIZE 128
#define OPH_MAX_STRING_SIZE OPH_COMMON_BUFFER_LEN
#define OPH_SEPARATOR_NULL ' '
#define OPH_SEPARATOR_KV "="
#define OPH_SEPARATOR_PARAM ";"
#define OPH_SEPARATOR_SUBPARAM_OPEN '['
#define OPH_SEPARATOR_SUBPARAM_CLOSE ']'
#define OPH_SEPARATOR_SUBPARAM_STR "|"
#define OPH_SEPARATOR_FOLDER "/"

#define OPH_MASSIVE_OPERATOR	"OPH_MASSIVE"

#define OPH_MF_OK 0
#define OPH_MF_ERROR 1

#define OPH_MF_QUERY "SELECT DISTINCT datacube.iddatacube, datacube.idcontainer FROM %s WHERE %s"
#define OPH_MF_QUERY_DUMMY "SELECT datacube.iddatacube, datacube.idcontainer FROM %s"

#define OPH_MF_ARG_DATACUBE "datacube"
#define OPH_MF_ARG_RUN "run"
#define OPH_MF_ARG_RECURSIVE "recursive"
#define OPH_MF_ARG_DEPTH "depth"
#define OPH_MF_ARG_OBJKEY_FILTER "objkey_filter"
#define OPH_MF_ARG_QUERY "query"

#define OPH_MF_ARG_VALUE_YES "yes"
#define OPH_MF_ARG_VALUE_NO "no"
#define OPH_MF_ARG_VALUE_CMIP5 "cmip5"
#define OPH_MF_ROOT_FOLDER "/"

// Datacube filters
#define OPH_MF_ARG_LEVEL "level"
#define OPH_MF_ARG_MEASURE "measure"
#define OPH_MF_ARG_PARENT "parent_cube"
#define OPH_MF_ARG_DATACUBE_FILTER "cube_filter"

// Container filters
#define OPH_MF_ARG_CONTAINER "container"
#define OPH_MF_ARG_CONTAINER_PID "container_pid"
#define OPH_MF_ARG_METADATA_KEY "metadata_key"
#define OPH_MF_ARG_METADATA_VALUE "metadata_value"

// Container and file filters
#define OPH_MF_ARG_PATH "path"

// File filters
#define OPH_MF_ARG_FILE "file"
#define OPH_MF_ARG_CONVENTION "convention"

// Const
#define OPH_MF_SYMBOL_NOT "!"	// "`"

#define OPH_FILTER_AND1 " AND "
#define OPH_FILTER_AND2 OPH_FILTER_AND1"("
#define OPH_FILTER_OR " OR "
#define OPH_FILTER_TASK "task AS taskp,hasinput AS hasinputp,datacube AS datacubep"
#define OPH_FILTER_NOT1 "!"
#define OPH_FILTER_NOT2 "NOT "

#define OPH_SUBSET_ISINSUBSET_PLUGIN "mysql.oph_is_in_subset(%s.%s,%lld,%lld,%lld)"

int oph_get_session_code(const char *sessionid, char *code, const char *oph_web_server)
{
	char tmp[OPH_MAX_STRING_SIZE];
	strncpy(tmp, sessionid, OPH_MAX_STRING_SIZE);

	char *tmp2 = tmp, *savepointer = NULL;
	unsigned short i, max = 3;
	if (oph_web_server) {
		unsigned int length = strlen(oph_web_server);
		if ((length >= OPH_MAX_STRING_SIZE) || strncmp(sessionid, oph_web_server, length))
			return OPH_MF_ERROR;
		tmp2 += length;
		max = 1;
	}

	tmp2 = strtok_r(tmp2, OPH_SEPARATOR_FOLDER, &savepointer);
	if (!tmp2)
		return OPH_MF_ERROR;
	for (i = 0; i < max; ++i) {
		tmp2 = strtok_r(NULL, OPH_SEPARATOR_FOLDER, &savepointer);
		if (!tmp2)
			return OPH_MF_ERROR;
	}
	strcpy(code, tmp2);

	return OPH_MF_OK;
}

int oph_filter_level(char *value, char *tables, char *where_clause, char not_clause)
{
	UNUSED(tables);

	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_LEVEL, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	int i, level, key_num = 0;
	unsigned int s;
	char condition[OPH_MAX_STRING_SIZE], **key_list = NULL;

	if (oph_tp_parse_multiple_value_param(value, &key_list, &key_num) || !key_num)
		return OPH_MF_ERROR;

	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND2)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, OPH_FILTER_AND2, s);
	} else
		snprintf(where_clause, OPH_MAX_STRING_SIZE, "(");

	for (i = 0; i < key_num; ++i) {
		level = (int) strtol(key_list[i], NULL, 10);
		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR)) {
				oph_tp_free_multiple_value_param_list(key_list, key_num);
				return OPH_MF_ERROR;
			}
			strncat(where_clause, not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR, s);
		}
		snprintf(condition, OPH_MAX_STRING_SIZE, "%s.level%s='%d'", OPH_MF_ARG_DATACUBE, not_clause ? OPH_FILTER_NOT1 : "", level);
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, condition, s);
	}

	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= 1) {
		oph_tp_free_multiple_value_param_list(key_list, key_num);
		return OPH_MF_ERROR;
	}
	strncat(where_clause, ")", s);

	oph_tp_free_multiple_value_param_list(key_list, key_num);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_LEVEL, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_measure(const char *value, char *tables, char *where_clause, char not_clause)
{
	UNUSED(tables);

	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_MEASURE, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	char condition[OPH_MAX_STRING_SIZE];
	unsigned int s;
	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND1, s);
	}

	snprintf(condition, OPH_MAX_STRING_SIZE, "%s.measure%s='%s'", OPH_MF_ARG_DATACUBE, not_clause ? OPH_FILTER_NOT1 : "", value);
	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_MEASURE, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_parent(char *value, char *tables, char *where_clause, char not_clause, char *oph_web_server)
{
	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_PARENT, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	if (strncasecmp(value, oph_web_server, strlen(oph_web_server))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}
	char *pointer1 = value + strlen(oph_web_server);
	if (*pointer1 != OPH_MF_ROOT_FOLDER[0]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}
	pointer1++;

	char condition[OPH_MAX_STRING_SIZE];
	strncpy(condition, pointer1, OPH_MAX_STRING_SIZE);
	char *savepointer = NULL, *pointer2 = strtok_r(condition, OPH_MF_ROOT_FOLDER, &savepointer);
	if (!pointer2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}
	pointer2 = strtok_r(NULL, OPH_MF_ROOT_FOLDER, &savepointer);
	if (!pointer2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}

	int unsigned s;
	int idcontainer = (int) strtol(pointer1, NULL, 10), idparent = (int) strtol(pointer2, NULL, 10);

	if (*tables) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= 1)
			return OPH_MF_ERROR;
		strncat(tables, ",", s);
	}
	if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= strlen(OPH_FILTER_TASK))
		return OPH_MF_ERROR;
	strncat(tables, OPH_FILTER_TASK, OPH_MAX_STRING_SIZE - strlen(tables));

	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND1, s);
	}
	snprintf(condition, OPH_MAX_STRING_SIZE,
		 "%s.iddatacube=taskp.idoutputcube AND taskp.idtask=hasinputp.idtask AND hasinputp.iddatacube=datacubep.iddatacube AND datacubep.iddatacube%s='%d' AND datacubep.idcontainer='%d'",
		 OPH_MF_ARG_DATACUBE, not_clause ? OPH_FILTER_NOT1 : "", idparent, idcontainer);
	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_PARENT, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_using_subset(char *value, char *tables, char *where_clause, char not_clause)
{
	UNUSED(tables);

	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_DATACUBE_FILTER, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	unsigned int s;

	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND2))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND2, s);
	} else
		snprintf(where_clause, OPH_MAX_STRING_SIZE, "(");

	unsigned long size = strlen(value), j = 0;
	char _value[1 + size];
	if (strstr(value, OPH_SEPARATOR_SUBPARAM_STR)) {
		strcpy(_value, value);
		for (; j < size; ++j)
			if (_value[j] == OPH_SEPARATOR_SUBPARAM_STR[0])
				_value[j] = OPH_SUBSET_LIB_SUBSET_SEPARATOR[0];
	}

	oph_subset *subset_struct = NULL;
	if (oph_subset_init(&subset_struct)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		oph_subset_free(subset_struct);
		return OPH_MF_ERROR;
	}
	if (oph_subset_parse(j ? _value : value, size, subset_struct, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		oph_subset_free(subset_struct);
		return OPH_MF_ERROR;
	}

	char condition[OPH_MAX_STRING_SIZE], temp[OPH_MAX_STRING_SIZE];
	*condition = 0;

	unsigned int i;
	for (i = 0; i < subset_struct->number; ++i) {
		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(condition) - 1) <= strlen(not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR)) {
				oph_subset_free(subset_struct);
				return OPH_MF_ERROR;
			}
			strncat(condition, not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR, s);
		}
		snprintf(temp, OPH_MAX_STRING_SIZE, "%s" OPH_SUBSET_ISINSUBSET_PLUGIN, not_clause ? OPH_FILTER_NOT2 : "", OPH_MF_ARG_DATACUBE, "iddatacube", subset_struct->start[i],
			 subset_struct->stride[i], subset_struct->end[i]);
		if ((s = OPH_MAX_STRING_SIZE - strlen(condition) - 1) <= strlen(temp)) {
			oph_subset_free(subset_struct);
			return OPH_MF_ERROR;
		}
		strncat(condition, temp, s);
	}
	oph_subset_free(subset_struct);

	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);

	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= 1)
		return OPH_MF_ERROR;
	strncat(where_clause, ")", s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_DATACUBE_FILTER, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_container(char *value, char *tables, char *where_clause, char not_clause)
{
	UNUSED(tables);

	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_CONTAINER, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	char condition[OPH_MAX_STRING_SIZE];
	unsigned int s;

	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND1, s);
	}

	snprintf(condition, OPH_MAX_STRING_SIZE, "%s.containername%s='%s'", OPH_MF_ARG_CONTAINER, not_clause ? OPH_FILTER_NOT1 : "", value);

	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_CONTAINER, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_container_pid(char *value, char *tables, char *where_clause, char not_clause, char *oph_web_server)
{
	UNUSED(tables);

	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_CONTAINER_PID, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	if (strncasecmp(value, oph_web_server, strlen(oph_web_server))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}
	char *pointer = value + strlen(oph_web_server);
	if (*pointer != OPH_MF_ROOT_FOLDER[0]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", value);
		return OPH_MF_ERROR;
	}
	pointer++;
	int idcontainer = (int) strtol(pointer, NULL, 10);
	char condition[OPH_MAX_STRING_SIZE];
	unsigned int s;
	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND1, s);
	}
	snprintf(condition, OPH_MAX_STRING_SIZE, "%s.idcontainer%s='%d'", OPH_MF_ARG_DATACUBE, not_clause ? OPH_FILTER_NOT1 : "", idcontainer);
	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_CONTAINER_PID, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_metadata_key(char *value, char *tables, char *where_clause, char not_clause)
{
	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_METADATA_KEY, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	char condition[OPH_MAX_STRING_SIZE];

	int key_num = 0;
	char **key_list = NULL;
	if (oph_tp_parse_multiple_value_param(value, &key_list, &key_num) || !key_num)
		return OPH_MF_ERROR;

	unsigned int s;
	if (*tables) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= 1) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			return OPH_MF_ERROR;
		}
		strncat(tables, ",", s);
	}
	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, OPH_FILTER_AND1, s);
	}

	int i;
	for (i = 0; i < key_num; ++i) {
		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= 1) {
				oph_tp_free_multiple_value_param_list(key_list, key_num);
				return OPH_MF_ERROR;
			}
			strncat(tables, ",", s);
		}
		snprintf(condition, OPH_MAX_STRING_SIZE, "metadatainstance AS metadatainstance%d", i);
		if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= strlen(condition))
			return OPH_MF_ERROR;
		strncat(tables, condition, s);

		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(not_clause ? OPH_FILTER_OR : OPH_FILTER_AND1)) {
				oph_tp_free_multiple_value_param_list(key_list, key_num);
				return OPH_MF_ERROR;
			}
			strncat(where_clause, not_clause ? OPH_FILTER_OR : OPH_FILTER_AND1, s);
		}
		snprintf(condition, OPH_MAX_STRING_SIZE, "metadatainstance%d.iddatacube=%s.iddatacube AND metadatainstance%d.label%s='%s'", i, OPH_MF_ARG_DATACUBE, i,
			 not_clause ? OPH_FILTER_NOT1 : "", key_list[i]);
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, condition, s);
	}
	oph_tp_free_multiple_value_param_list(key_list, key_num);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_METADATA_KEY, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int _oph_filter_metadata_value(char *key, char *value, char *tables, char *where_clause, unsigned int prefix, char not_clause)
{
	if (!value || !strlen(value))
		return OPH_MF_OK;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s='%s'\n", OPH_MF_ARG_METADATA_KEY, key);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s%s='%s'\n", OPH_MF_ARG_METADATA_VALUE, not_clause ? OPH_MF_SYMBOL_NOT : "", value);

	char condition[OPH_MAX_STRING_SIZE];

	int key_num = 0, value_num;
	char **key_list = NULL;
	char **value_list = NULL;
	if (oph_tp_parse_multiple_value_param(key, &key_list, &key_num) || !key_num)
		return OPH_MF_ERROR;
	if (oph_tp_parse_multiple_value_param(value, &value_list, &value_num) || !value_num) {
		oph_tp_free_multiple_value_param_list(key_list, key_num);
		return OPH_MF_ERROR;
	}
	if (key_num != value_num) {
		oph_tp_free_multiple_value_param_list(key_list, key_num);
		oph_tp_free_multiple_value_param_list(value_list, value_num);
		return OPH_MF_ERROR;
	}

	unsigned int s;
	if (*tables) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= 1) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			oph_tp_free_multiple_value_param_list(value_list, value_num);
			return OPH_MF_ERROR;
		}
		strncat(tables, ",", s);
	}
	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND1)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			oph_tp_free_multiple_value_param_list(value_list, value_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, OPH_FILTER_AND1, s);
	}

	int i;
	for (i = 0; i < key_num; ++i) {
		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= 1) {
				oph_tp_free_multiple_value_param_list(key_list, key_num);
				oph_tp_free_multiple_value_param_list(value_list, value_num);
				return OPH_MF_ERROR;
			}
			strncat(tables, ",", s);
		}
		snprintf(condition, OPH_MAX_STRING_SIZE, "metadatainstance AS metadatainstance%dk%d", prefix, i);
		if ((s = OPH_MAX_STRING_SIZE - strlen(tables) - 1) <= strlen(condition)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			oph_tp_free_multiple_value_param_list(value_list, value_num);
			return OPH_MF_ERROR;
		}
		strncat(tables, condition, s);

		if (i) {
			if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(not_clause ? OPH_FILTER_OR : OPH_FILTER_AND1)) {
				oph_tp_free_multiple_value_param_list(key_list, key_num);
				oph_tp_free_multiple_value_param_list(value_list, value_num);
				return OPH_MF_ERROR;
			}
			strncat(where_clause, not_clause ? OPH_FILTER_OR : OPH_FILTER_AND1, s);
		}
		snprintf(condition, OPH_MAX_STRING_SIZE,
			 "metadatainstance%dk%d.iddatacube=%s.iddatacube AND metadatainstance%dk%d.label='%s' AND CONVERT(metadatainstance%dk%d.value USING latin1) %sLIKE '%%%s%%'", prefix, i,
			 OPH_MF_ARG_DATACUBE, prefix, i, key_list[i], prefix, i, not_clause ? OPH_FILTER_NOT2 : "", value_list[i]);
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition)) {
			oph_tp_free_multiple_value_param_list(key_list, key_num);
			oph_tp_free_multiple_value_param_list(value_list, value_num);
			return OPH_MF_ERROR;
		}
		strncat(where_clause, condition, s);
	}
	oph_tp_free_multiple_value_param_list(key_list, key_num);
	oph_tp_free_multiple_value_param_list(value_list, value_num);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s%s='%s'\n", OPH_MF_ARG_METADATA_VALUE, not_clause ? OPH_MF_SYMBOL_NOT : "", value);
	return OPH_MF_OK;
}

int oph_filter_metadata_value(char *key, char *value, char *tables, char *where_clause, char not_clause)
{
	return _oph_filter_metadata_value(key, value, tables, where_clause, 0, not_clause);
}

int oph_add_folder(int folder_id, int *counter, char *where_clause, ophidiadb * oDB, int recursive_flag, char not_clause)
{
	unsigned int s;
	char condition[OPH_MAX_STRING_SIZE];
	if (*counter) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR))
			return OPH_MF_ERROR;
		strncat(where_clause, not_clause ? OPH_FILTER_AND1 : OPH_FILTER_OR, s);
	}
	snprintf(condition, OPH_MAX_STRING_SIZE, "%s.idfolder%s='%d'", OPH_MF_ARG_CONTAINER, not_clause ? OPH_FILTER_NOT1 : "", folder_id);
	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(condition))
		return OPH_MF_ERROR;
	strncat(where_clause, condition, s);
	(*counter)++;

	if (recursive_flag < 0)
		recursive_flag++;
	if (recursive_flag) {
		int *subfolder_id = 0, num_subfolders, i;
		if (oph_odb_fs_get_subfolders(folder_id, &subfolder_id, &num_subfolders, oDB))
			return OPH_MF_ERROR;
		for (i = 0; i < num_subfolders; ++i)
			if (oph_add_folder(subfolder_id[i], counter, where_clause, oDB, recursive_flag, not_clause))
				break;
		if (subfolder_id)
			free(subfolder_id);
		if (i < num_subfolders)
			return OPH_MF_ERROR;
	}

	return OPH_MF_OK;
}

int oph_filter_path(char *path, char *recursive, char *depth, char *sessionid, ophidiadb * oDB, char *tables, char *where_clause, char not_clause)
{
	UNUSED(tables);

	if (!path || !strlen(path))
		return OPH_MF_OK;

	int permission = 0, folder_id = 0, counter = 0, recursive_flag = recursive && !strcmp(recursive, OPH_MF_ARG_VALUE_YES);
	unsigned int s;
	if (recursive_flag && depth) {
		int rdepth = (int) strtol(depth, NULL, 10);
		if (rdepth > 0)
			recursive_flag = -rdepth;
	}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Process argument %s='%s' (%s=%s)\n", OPH_MF_ARG_PATH, path, OPH_MF_ARG_RECURSIVE, recursive_flag ? OPH_MF_ARG_VALUE_YES : OPH_MF_ARG_VALUE_NO);

	if (oph_odb_fs_path_parsing("", path, &folder_id, NULL, oDB)) {
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Path '%s' doesn't exists\n", path);
		return OPH_MF_ERROR;
	}
	if (oph_odb_fs_check_folder_session(folder_id, sessionid, oDB, &permission) || !permission)	// Only the permission on specified folder is checked, subfolders will be accessed in case the specified one is accessible
	{
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Access in folder '%s' is not allowed\n", path);
		return OPH_MF_ERROR;
	}

	if (*where_clause) {
		if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= strlen(OPH_FILTER_AND2))
			return OPH_MF_ERROR;
		strncat(where_clause, OPH_FILTER_AND2, s);
	} else
		snprintf(where_clause, OPH_MAX_STRING_SIZE, "(");

	if (oph_add_folder(folder_id, &counter, where_clause, oDB, recursive_flag, not_clause)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Folder '%s' cannot be explored\n", path);
		return OPH_MF_ERROR;
	}

	if ((s = OPH_MAX_STRING_SIZE - strlen(where_clause) - 1) <= 1)
		return OPH_MF_ERROR;
	strncat(where_clause, ")", s);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Processed argument %s='%s' (%s=%s)\n", OPH_MF_ARG_PATH, path, OPH_MF_ARG_RECURSIVE, recursive_flag ? OPH_MF_ARG_VALUE_YES : OPH_MF_ARG_VALUE_NO);

	return OPH_MF_OK;
}

int oph_filter_free_kvp(HASHTBL * task_tbl, char *tables, char *where_clause, char not_clause)
{
	unsigned int i, j = 0;
	struct hashnode_s *node;
	for (i = 0; i < task_tbl->size; ++i)
		for (node = task_tbl->nodes[i]; node; node = node->next)
			if (_oph_filter_metadata_value(node->key, (char *) node->data, tables, where_clause, ++j, not_clause))
				return OPH_MF_ERROR;

	return OPH_MF_OK;
}

int oph_filter(HASHTBL * task_tbl, char *query, char *cwd, char *sessionid, ophidiadb * oDB, char *oph_web_server)
{
	if (!query || !sessionid || !cwd) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_MF_ERROR;
	}
	// Filter on current session
	char ext_path[OPH_MAX_STRING_SIZE], ext_path_n[OPH_MAX_STRING_SIZE];
	*ext_path = OPH_MF_ROOT_FOLDER[0];

	if (oph_get_session_code(sessionid, ext_path + 1, oph_web_server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get session code\n");
		return OPH_MF_ERROR;
	}
	size_t offset = strlen(ext_path);
	if (strlen(cwd) >= offset) {
		cwd += offset;
		if (!*cwd) {
			cwd--;
			*cwd = OPH_MF_ROOT_FOLDER[0];
		}
	}

	char tables[OPH_MAX_STRING_SIZE], where_clause[OPH_MAX_STRING_SIZE];

	char *container = task_tbl ? hashtbl_get(task_tbl, OPH_MF_ARG_CONTAINER) : NULL;
	char *container_n = task_tbl ? hashtbl_get(task_tbl, OPH_MF_ARG_CONTAINER "" OPH_MF_SYMBOL_NOT) : NULL;
	char *path = task_tbl ? hashtbl_get(task_tbl, OPH_MF_ARG_PATH) : cwd;
	char *path_n = task_tbl ? hashtbl_get(task_tbl, OPH_MF_ARG_PATH "" OPH_MF_SYMBOL_NOT) : NULL;

	// Basic tables and where_clause
	snprintf(tables, OPH_MAX_STRING_SIZE, "%s,%s", OPH_MF_ARG_DATACUBE, OPH_MF_ARG_CONTAINER);
	snprintf(where_clause, OPH_MAX_STRING_SIZE, "%s.idcontainer=%s.idcontainer", OPH_MF_ARG_DATACUBE, OPH_MF_ARG_CONTAINER);

	if (path_n && strlen(path_n))
		strcpy(ext_path_n, ext_path);

	if (path && strlen(path)) {
		char *first_nospace = path;
		while (first_nospace && *first_nospace && (*first_nospace == ' '))
			first_nospace++;
		if (!first_nospace || (*first_nospace != OPH_MF_ROOT_FOLDER[0])) {
			strncat(ext_path, cwd, OPH_MAX_STRING_SIZE - strlen(ext_path));
			first_nospace = ext_path + strlen(ext_path) - 1;
			while (first_nospace && (*first_nospace == ' '))
				first_nospace--;
			if (first_nospace && (*first_nospace != OPH_MF_ROOT_FOLDER[0]))
				strncat(ext_path, OPH_MF_ROOT_FOLDER, OPH_MAX_STRING_SIZE - strlen(ext_path));
		}
		strncat(ext_path, path, OPH_MAX_STRING_SIZE - strlen(ext_path));
	} else
		strncat(ext_path, cwd, OPH_MAX_STRING_SIZE - strlen(ext_path));
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Extended path to be explored is '%s'\n", ext_path);

	if (path_n && strlen(path_n)) {
		char *first_nospace = path_n;
		while (first_nospace && *first_nospace && (*first_nospace == ' '))
			first_nospace++;
		if (!first_nospace || (*first_nospace != OPH_MF_ROOT_FOLDER[0])) {
			strncat(ext_path_n, cwd, OPH_MAX_STRING_SIZE - strlen(ext_path_n));
			first_nospace = ext_path_n + strlen(ext_path_n) - 1;
			while (first_nospace && (*first_nospace == ' '))
				first_nospace--;
			if (first_nospace && (*first_nospace != OPH_MF_ROOT_FOLDER[0]))
				strncat(ext_path_n, OPH_MF_ROOT_FOLDER, OPH_MAX_STRING_SIZE - strlen(ext_path_n));
		}
		strncat(ext_path_n, path_n, OPH_MAX_STRING_SIZE - strlen(ext_path_n));
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Extended path to be excluded is '%s'\n", ext_path_n);
	}

	if (task_tbl) {
		char *value = NULL, *value2 = NULL, *metadata_key = NULL, *metadata_value = NULL;

		// Filters
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Parse positive filters\n");

		if (oph_filter_level(value = hashtbl_get(task_tbl, OPH_MF_ARG_LEVEL), tables, where_clause, 0))
			return OPH_MF_ERROR;
		if (oph_filter_measure(value = hashtbl_get(task_tbl, OPH_MF_ARG_MEASURE), tables, where_clause, 0))
			return OPH_MF_ERROR;
		if (oph_filter_parent(value = hashtbl_get(task_tbl, OPH_MF_ARG_PARENT), tables, where_clause, 0, oph_web_server))
			return OPH_MF_ERROR;
		if (oph_filter_using_subset(value = hashtbl_get(task_tbl, OPH_MF_ARG_DATACUBE_FILTER), tables, where_clause, 0))
			return OPH_MF_ERROR;
		if (container && strlen(container)) {
			if (oph_filter_container(container, tables, where_clause, 0))
				return OPH_MF_ERROR;
		} else {
			if (oph_filter_container_pid(value = hashtbl_get(task_tbl, OPH_MF_ARG_CONTAINER_PID), tables, where_clause, 0, oph_web_server))
				return OPH_MF_ERROR;
		}
		metadata_key = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_KEY);
		if (metadata_key && strlen(metadata_key)) {
			metadata_value = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE);
			if (metadata_value && strlen(metadata_value)) {
				if (oph_filter_metadata_value(metadata_key, metadata_value, tables, where_clause, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong arguments '%s' and '%s'\n", OPH_MF_ARG_METADATA_KEY, OPH_MF_ARG_METADATA_VALUE);
					return OPH_MF_ERROR;
				}
			} else {
				metadata_value = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE "" OPH_MF_SYMBOL_NOT);
				if (metadata_value && strlen(metadata_value)) {
					if (oph_filter_metadata_value(metadata_key, metadata_value, tables, where_clause, 1)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong arguments '%s' and '%s'\n", OPH_MF_ARG_METADATA_KEY, OPH_MF_ARG_METADATA_VALUE);
						return OPH_MF_ERROR;
					}
				} else
					metadata_value = NULL;	// In case strlen() returns 0
			}
			if (!metadata_value && oph_filter_metadata_key(metadata_key, tables, where_clause, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", OPH_MF_ARG_METADATA_KEY);
				return OPH_MF_ERROR;
			}
		} else {
			metadata_value = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE);
			if (metadata_value && strlen(metadata_value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", OPH_MF_ARG_METADATA_VALUE);
				return OPH_MF_ERROR;
			}
		}
		if (oph_filter_path(ext_path, value = hashtbl_get(task_tbl, OPH_MF_ARG_RECURSIVE), value2 = hashtbl_get(task_tbl, OPH_MF_ARG_DEPTH), sessionid, oDB, tables, where_clause, 0))
			return OPH_MF_ERROR;	// path or cwd

		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Parse negative filters\n");

		if (oph_filter_level(value = hashtbl_get(task_tbl, OPH_MF_ARG_LEVEL "" OPH_MF_SYMBOL_NOT), tables, where_clause, 1))
			return OPH_MF_ERROR;
		if (oph_filter_measure(value = hashtbl_get(task_tbl, OPH_MF_ARG_MEASURE "" OPH_MF_SYMBOL_NOT), tables, where_clause, 1))
			return OPH_MF_ERROR;
		if (oph_filter_parent(value = hashtbl_get(task_tbl, OPH_MF_ARG_PARENT "" OPH_MF_SYMBOL_NOT), tables, where_clause, 1, oph_web_server))
			return OPH_MF_ERROR;
		if (oph_filter_using_subset(value = hashtbl_get(task_tbl, OPH_MF_ARG_DATACUBE_FILTER "" OPH_MF_SYMBOL_NOT), tables, where_clause, 1))
			return OPH_MF_ERROR;
		if (container_n && strlen(container_n)) {
			if (oph_filter_container(container_n, tables, where_clause, 1))
				return OPH_MF_ERROR;
		} else {
			if (oph_filter_container_pid(value = hashtbl_get(task_tbl, OPH_MF_ARG_CONTAINER_PID "" OPH_MF_SYMBOL_NOT), tables, where_clause, 1, oph_web_server))
				return OPH_MF_ERROR;
		}
		metadata_key = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_KEY "" OPH_MF_SYMBOL_NOT);
		if (metadata_key && strlen(metadata_key)) {
			if (hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE) || hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE "" OPH_MF_SYMBOL_NOT)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Negation cannot be used for '%s' in case '%s' is also set\n", OPH_MF_ARG_METADATA_KEY, OPH_MF_ARG_METADATA_VALUE);
				return OPH_MF_ERROR;
			} else if (oph_filter_metadata_key(metadata_key, tables, where_clause, 1)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", OPH_MF_ARG_METADATA_KEY);
				return OPH_MF_ERROR;
			}
		} else {
			metadata_value = hashtbl_get(task_tbl, OPH_MF_ARG_METADATA_VALUE "" OPH_MF_SYMBOL_NOT);
			if (metadata_value && strlen(metadata_value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong argument '%s'\n", OPH_MF_ARG_METADATA_VALUE);
				return OPH_MF_ERROR;
			}
		}
		if (path_n && strlen(path_n)
		    && oph_filter_path(ext_path_n, value = hashtbl_get(task_tbl, OPH_MF_ARG_RECURSIVE), value2 = hashtbl_get(task_tbl, OPH_MF_ARG_DEPTH), sessionid, oDB, tables, where_clause, 1))
			return OPH_MF_ERROR;	// path or cwd

		if ((value = hashtbl_get(task_tbl, OPH_MF_ARG_FILE)))
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Argument '%s' will be skipped\n", OPH_MF_ARG_FILE);
		if ((value = hashtbl_get(task_tbl, OPH_MF_ARG_CONVENTION)))
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Argument '%s' will be skipped\n", OPH_MF_ARG_CONVENTION);

	} else if (oph_filter_path(ext_path, OPH_MF_ARG_VALUE_NO, NULL, sessionid, oDB, tables, where_clause, 0))
		return OPH_MF_ERROR;

	if (*where_clause)
		snprintf(query, OPH_MAX_STRING_SIZE, OPH_MF_QUERY, tables, where_clause);
	else
		snprintf(query, OPH_MAX_STRING_SIZE, OPH_MF_QUERY_DUMMY, tables);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Query for massive operation: %s;\n", query);
	return OPH_MF_OK;
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

	if (!(handle->operator_handle = (OPH_LIST_operator_handle *) calloc(1, sizeof(OPH_LIST_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_LIST_operator_handle *) handle->operator_handle)->level = 0;
	((OPH_LIST_operator_handle *) handle->operator_handle)->path = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->container_name = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->filter = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->hostname = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->db_name = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->id_dbms = 0;
	((OPH_LIST_operator_handle *) handle->operator_handle)->recursive_search = 0;
	((OPH_LIST_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->src = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->oper_level = -1;
	((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid = NULL;
	ophidiadb *oDB = &((OPH_LIST_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
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
	if (oph_tp_parse_multiple_value_param(value, &((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys, &((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LIST_operator_handle *) handle->operator_handle)->level = (int) strtol(value, NULL, 10);
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->level < 0 || ((OPH_LIST_operator_handle *) handle->operator_handle)->level > 9) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_BAD_LEVEL_PARAMETER, ((OPH_LIST_operator_handle *) handle->operator_handle)->level);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_CONTAINER_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->container_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "container name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DATACUBE_INPUT);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "datacube name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if ((((OPH_LIST_operator_handle *) handle->operator_handle)->level >= 4) && (((OPH_LIST_operator_handle *) handle->operator_handle)->level < 9)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "A datacube PID must be specified if level is bigger than 3\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_DATACUBE_ARGUMENT);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_FRAMEWORK_FS_DEFAULT_PATH) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->path = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "path");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->cwd = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_RECURSIVE_SEARCH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_RECURSIVE_SEARCH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_RECURSIVE_SEARCH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LIST_operator_handle *) handle->operator_handle)->recursive_search = strcmp(value, OPH_COMMON_NO_VALUE);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DATACUBE_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->filter = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, OPH_IN_PARAM_DATACUBE_NAME_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HOSTNAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HOSTNAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_HOSTNAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->hostname = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "hostname");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DBMS_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DBMS_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DBMS_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) && strcmp(value, "0")) {
		((OPH_LIST_operator_handle *) handle->operator_handle)->id_dbms = (int) strtol(value, NULL, 10);
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DB_NAME_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DB_NAME_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DB_NAME_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->db_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "database instance name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_MEASURE_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->measure = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "measure filter");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LIST_operator_handle *) handle->operator_handle)->src = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "source filter");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEVEL_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEVEL_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LEVEL_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) && strcmp(value, "0"))
		((OPH_LIST_operator_handle *) handle->operator_handle)->oper_level = (int) strtol(value, NULL, 10);

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_list_recursive_list_folders(ophidiadb * oDB, int level, int folder_id, char *container_name, char *tmp_uri, char *path, int recursive_search, short int first_time,
				     oph_json * oper_json, char **objkeys, int objkeys_num, const char *sessionid)
{
	if (!oDB || !folder_id || !path || !oper_json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	MYSQL_RES *info_list = NULL;
	int num_rows = 0;
	char recursive_path[OPH_COMMON_BUFFER_LEN];
	char leaf_folder[OPH_COMMON_BUFFER_LEN];
	char *pid = NULL;
	int recursive_search_stop = 0;

	if (first_time)
		snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", "");
	else
		snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", path);

	//Find all child folders
	MYSQL_RES *tmp_info_list = NULL;
	if (recursive_search) {
		//retrieve information list
		if (oph_odb_fs_find_fs_objects(oDB, 0, folder_id, NULL, &tmp_info_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
			if (recursive_search)
				mysql_free_result(tmp_info_list);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(tmp_info_list))) {
			mysql_free_result(tmp_info_list);
			tmp_info_list = NULL;
			recursive_search_stop = 1;
		}
	}
	//retrieve information list
	if (oph_odb_fs_find_fs_objects(oDB, level, folder_id, container_name, &info_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
		if (recursive_search)
			mysql_free_result(tmp_info_list);
		mysql_free_result(info_list);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	char **my_row = NULL;
	int num_fields = 0;
	//Empty set
	if ((num_rows = mysql_num_rows(info_list))) {
		MYSQL_ROW row;
		//For each ROW
		while ((row = mysql_fetch_row(info_list))) {
			switch (level) {
				case 2:
					if (row[2] && row[3]) {
						if (oph_pid_create_pid(tmp_uri, (int) strtol(row[2], NULL, 10), (int) strtol(row[3], NULL, 10), &pid)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_CREATE_ERROR);
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
					}
					if (!strcmp(row[1], "1")) {
						if (recursive_search && !first_time)
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH, recursive_path, (row[0] ? row[0] : ""));
						else
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH_NAME, (row[0] ? row[0] : ""));
					} else if (!strcmp(row[1], "2")) {
						if (recursive_search && !first_time)
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH, recursive_path, (row[0] ? row[0] : ""));
						else
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH_NAME, (row[0] ? row[0] : ""));
					}
					//ADD ROW TO JSON
					num_fields = 4;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup((!strcmp(row[1], "1") ? "f" : "c"));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(leaf_folder);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((pid ? pid : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(row[4] ? row[4] : "");
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					printf("| %-1s | %-70s | %-40s |\n", (!strcmp(row[1], "1") ? "f" : "c"), leaf_folder, (pid ? pid : ""));
					if (pid)
						free(pid);
					pid = NULL;
					break;
				case 1:
					if (!strcmp(row[1], "1")) {
						if (recursive_search && !first_time)
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH, recursive_path, (row[0] ? row[0] : ""));
						else
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH_NAME, (row[0] ? row[0] : ""));
					} else if (!strcmp(row[1], "2")) {
						if (recursive_search && !first_time)
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH, recursive_path, (row[0] ? row[0] : ""));
						else
							snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH_NAME, (row[0] ? row[0] : ""));
					}
					//ADD ROW TO JSON
					num_fields = 2;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup((!strcmp(row[1], "1") ? "f" : "c"));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(leaf_folder);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					printf("| %-1s | %-70s |\n", (!strcmp(row[1], "1") ? "f" : "c"), leaf_folder);
					break;
				case 0:
					if (recursive_search && !first_time)
						snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH, recursive_path, (row[0] ? row[0] : ""));
					else
						snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, OPH_LIST_FOLDER_PATH_NAME, (row[0] ? row[0] : ""));

					//ADD ROW TO JSON
					num_fields = 2;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup("f");
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(leaf_folder);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					printf("| %-1s | %-70s |\n", "f", leaf_folder);
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_BAD_LEVEL_PARAMETER, level);
					mysql_free_result(info_list);
					if (recursive_search)
						mysql_free_result(tmp_info_list);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

		}
	}
	mysql_free_result(info_list);

	if (recursive_search_stop)
		recursive_search = 0;

	if (!recursive_search)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	MYSQL_ROW tmp_row;
	//For each ROW
	while ((tmp_row = mysql_fetch_row(tmp_info_list))) {
		//Set new path
		if (first_time)
			snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", tmp_row[0]);
		else {
			char recursive_path_buf[OPH_COMMON_BUFFER_LEN];
			snprintf(recursive_path_buf, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH, recursive_path, tmp_row[0]);
			snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", recursive_path_buf);
		}
		if (_oph_list_recursive_list_folders(oDB, level, (int) strtol(tmp_row[1], NULL, 10), container_name, tmp_uri, recursive_path, 1, 0, oper_json, objkeys, objkeys_num, sessionid))
			break;
	}
	if (recursive_search)
		mysql_free_result(tmp_info_list);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_list_recursive_filtered_list_folders(ophidiadb * oDB, int folder_id, char *container_name, char *tmp_uri, char *path, int recursive_search, short int first_time, char *measure,
					      int oper_level, char *src, oph_json * oper_json, char **objkeys, int objkeys_num, const char *sessionid)
{
	if (!oDB || !folder_id || !path || !oper_json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	MYSQL_RES *info_list = NULL;
	int num_rows = 0;
	char recursive_path[OPH_COMMON_BUFFER_LEN];
	char leaf_folder[OPH_COMMON_BUFFER_LEN];
	char source_buffer[OPH_COMMON_BUFFER_LEN];
	char *pid = NULL;
	char *pid2 = NULL;
	int tmp_level = -1;
	char *new_leaf_folder2 = NULL;
	int recursive_search_stop = 0;

	if (first_time)
		snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", "");
	else
		snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", path);

	//Find all child folders
	MYSQL_RES *tmp_info_list = NULL;
	if (recursive_search) {
		//retrieve information list
		if (oph_odb_fs_find_fs_objects(oDB, 0, folder_id, NULL, &tmp_info_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
			if (recursive_search)
				mysql_free_result(tmp_info_list);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(tmp_info_list))) {
			mysql_free_result(tmp_info_list);
			tmp_info_list = NULL;
			recursive_search_stop = 1;
		}
	}
	//retrieve information list
	if (oph_odb_fs_find_fs_filtered_objects(oDB, folder_id, container_name, measure, oper_level, src, &info_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
		if (recursive_search)
			mysql_free_result(tmp_info_list);
		mysql_free_result(info_list);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	char **my_row = NULL;
	int num_fields = 7;
	//Empty set
	if ((num_rows = mysql_num_rows(info_list))) {
		MYSQL_ROW row;
		//For each ROW
		while ((row = mysql_fetch_row(info_list))) {
			if (row[5])
				tmp_level = (int) strtol(row[5], NULL, 10);

			if (row[2] && row[3]) {
				//retrieve input datacube
				if (oph_pid_create_pid(tmp_uri, (int) strtol(row[2], NULL, 10), (int) strtol(row[3], NULL, 10), &pid)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_CREATE_ERROR);
					if (recursive_search)
						mysql_free_result(tmp_info_list);
					mysql_free_result(info_list);
					if (pid)
						free(pid);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			if (row[2] && row[8]) {
				if (strncmp(row[8], OPH_LIST_TASK_MULTIPLE_INPUT, strlen(OPH_LIST_TASK_MULTIPLE_INPUT)) == 0)
					pid2 = (char *) strndup("...", strlen("..."));
				else {
					//retrieve input datacube
					if (oph_pid_create_pid(tmp_uri, (int) strtol(row[2], NULL, 10), (int) strtol(row[8], NULL, 10), &pid2)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_CREATE_ERROR);
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}
			} else if (!row[8] && tmp_level > 0 && !row[7])
				pid2 = (char *) strndup(OPH_LIST_TASK_MISSING_INPUT, strlen(OPH_LIST_TASK_MISSING_INPUT));
			else if (!row[8] && tmp_level == 0 && !row[7])
				pid2 = (char *) strndup(OPH_LIST_TASK_RANDOM_INPUT, strlen(OPH_LIST_TASK_RANDOM_INPUT));

			if (recursive_search && !first_time)
				snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, "%s/%s%s", recursive_path, (row[0] ? row[0] : ""), (!strcmp(row[1], "1") ? "/" : (!strcmp(row[1], "2") ? "" : "*")));
			else
				snprintf(leaf_folder, OPH_COMMON_BUFFER_LEN, "%s%s", (row[0] ? row[0] : ""), (!strcmp(row[1], "1") ? "/" : (!strcmp(row[1], "2") ? "" : "*")));
			char old_leaf_folder[OPH_COMMON_BUFFER_LEN];
			snprintf(old_leaf_folder, OPH_COMMON_BUFFER_LEN, "%s", leaf_folder);
			if (oph_utl_short_folder(18, 0, leaf_folder)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
				if (recursive_search)
					mysql_free_result(tmp_info_list);
				mysql_free_result(info_list);
				if (pid)
					free(pid);
				if (pid2)
					free(pid2);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			//Create source field
			if (!row[7] && !pid2) {
				//ADD ROW TO JSON
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
					my_row = (char **) malloc(sizeof(char *) * num_fields);
					if (!my_row) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					int iii, jjj = 0;
					my_row[jjj] = strdup((!strcmp(row[1], "1") ? "f" : "c"));
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup(leaf_folder);
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup((pid ? pid : ""));
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup(row[4] ? row[4] : "");
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup((row[6] ? row[6] : ""));
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup((row[5] ? row[5] : ""));
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;
					my_row[jjj] = strdup("");
					if (!my_row[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < jjj; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					jjj++;

					if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						if (recursive_search)
							mysql_free_result(tmp_info_list);
						mysql_free_result(info_list);
						if (pid)
							free(pid);
						if (pid2)
							free(pid2);
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					for (iii = 0; iii < num_fields; iii++)
						if (my_row[iii])
							free(my_row[iii]);
					if (my_row)
						free(my_row);
				}
				//ADD ROW TO JSON
				printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", (!strcmp(row[1], "1") ? "f" : "c"), new_leaf_folder2, (pid ? pid : ""), (row[6] ? row[6] : ""),
				       (row[5] ? row[5] : ""), "");
			} else {
				if (row[7]) {
					int residual_len = 0;
					int source_len = 0;
					if (pid2) {
						snprintf(source_buffer, OPH_COMMON_BUFFER_LEN, OPH_LIST_TASK_FILE_INPUT " %s - " OPH_LIST_TASK_DATACUBE_INPUT " %s", row[7], pid2);
						source_len = strlen(row[7]) + strlen(OPH_LIST_TASK_FILE_INPUT) + 1;
					} else {
						snprintf(source_buffer, OPH_COMMON_BUFFER_LEN, "%s", row[7]);
						source_len = strlen(row[7]);
					}
					int len = strlen(source_buffer);
					if (len > 45) {
						int ii;
						char *tmp = source_buffer;
						char tmp_buffer[45] = { '\0' };
						residual_len = (source_len >= 45 ? 45 : source_len);
						for (ii = 0; ii < len; ii += residual_len) {
							residual_len = (source_len - ii >= 45 ? 45 : source_len - ii);
							if (residual_len <= 0)
								residual_len = 45;

							strncpy(tmp_buffer, tmp, residual_len);
							tmp_buffer[residual_len] = 0;
							tmp += residual_len;
							if (ii == 0)
								printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", (!strcmp(row[1], "1") ? "f" : "c"), new_leaf_folder2, (pid ? pid : ""),
								       (row[6] ? row[6] : ""), (row[5] ? row[5] : ""), tmp_buffer);
							else
								printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", "", "", "", "", "", tmp_buffer);
						}
					} else {
						printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", (!strcmp(row[1], "1") ? "f" : "c"), new_leaf_folder2, (pid ? pid : ""),
						       (row[6] ? row[6] : ""), (row[5] ? row[5] : ""), source_buffer);
					}
					//ADD ROW TO JSON
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup((!strcmp(row[1], "1") ? "f" : "c"));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(leaf_folder);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((pid ? pid : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(row[4] ? row[4] : "");
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((row[6] ? row[6] : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((row[5] ? row[5] : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(source_buffer);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
				} else {
					printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", (!strcmp(row[1], "1") ? "f" : "c"), new_leaf_folder2, (pid ? pid : ""), (row[6] ? row[6] : ""),
					       (row[5] ? row[5] : ""), (pid2 ? pid2 : ""));
					//ADD ROW TO JSON
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup((!strcmp(row[1], "1") ? "f" : "c"));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(leaf_folder);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((pid ? pid : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(row[4] ? row[4] : "");
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((row[6] ? row[6] : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((row[5] ? row[5] : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup((pid2 ? pid2 : ""));
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							if (recursive_search)
								mysql_free_result(tmp_info_list);
							mysql_free_result(info_list);
							if (pid)
								free(pid);
							if (pid2)
								free(pid2);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
				}
			}
			printf("+---+--------------------+------------------------------------------+----------------------+-------+-----------------------------------------------+\n");
			if (pid)
				free(pid);
			pid = NULL;
			if (pid2)
				free(pid2);
			pid2 = NULL;

		}
	}
	mysql_free_result(info_list);

	if (recursive_search_stop)
		recursive_search = 0;

	if (!recursive_search)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	MYSQL_ROW tmp_row;
	//For each ROW
	while ((tmp_row = mysql_fetch_row(tmp_info_list))) {
		//Set new path
		if (first_time)
			snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", tmp_row[0]);
		else {
			char recursive_path_buf[OPH_COMMON_BUFFER_LEN];
			snprintf(recursive_path_buf, OPH_COMMON_BUFFER_LEN, OPH_LIST_CONTAINER_PATH, recursive_path, tmp_row[0]);
			snprintf(recursive_path, OPH_COMMON_BUFFER_LEN, "%s", recursive_path_buf);
		}
		if (_oph_list_recursive_filtered_list_folders
		    (oDB, (int) strtol(tmp_row[1], NULL, 10), container_name, tmp_uri, recursive_path, 1, 0, measure, oper_level, src, oper_json, objkeys, objkeys_num, sessionid))
			break;
	}
	if (recursive_search)
		mysql_free_result(tmp_info_list);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}


int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	ophidiadb *oDB = &((OPH_LIST_operator_handle *) handle->operator_handle)->oDB;
	int level = ((OPH_LIST_operator_handle *) handle->operator_handle)->level;
	int recursive_search = ((OPH_LIST_operator_handle *) handle->operator_handle)->recursive_search;
	char *path = ((OPH_LIST_operator_handle *) handle->operator_handle)->path;
	char *user = ((OPH_LIST_operator_handle *) handle->operator_handle)->user;
	char *container_name = ((OPH_LIST_operator_handle *) handle->operator_handle)->container_name;
	char *datacube_name = ((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name;
	char *task_string = ((OPH_LIST_operator_handle *) handle->operator_handle)->filter;
	char *hostname = ((OPH_LIST_operator_handle *) handle->operator_handle)->hostname;
	char *db_name = ((OPH_LIST_operator_handle *) handle->operator_handle)->db_name;
	int id_dbms = ((OPH_LIST_operator_handle *) handle->operator_handle)->id_dbms;
	char *measure = ((OPH_LIST_operator_handle *) handle->operator_handle)->measure;
	char *src = ((OPH_LIST_operator_handle *) handle->operator_handle)->src;
	int oper_level = ((OPH_LIST_operator_handle *) handle->operator_handle)->oper_level;
	char *cwd = ((OPH_LIST_operator_handle *) handle->operator_handle)->cwd;
	char **objkeys = ((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys;
	int objkeys_num = ((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys_num;

	MYSQL_RES *info_list = NULL;
	int num_rows = 0;

	int id_container = 0;
	int id_datacube = 0;

	int permission = 0;
	int folder_id = 0;
	char tmp_path[OPH_COMMON_BUFFER_LEN], *new_path = NULL;
	int exists = 0;
	char *uri = NULL;
	char *parsed_path = NULL;
	int home_id = 0;
	char home_path[MYSQL_BUFLEN];

	if (level < 4) {

		if (path || cwd) {
			//Check if user can operate on container and if container exists
			if (oph_odb_fs_path_parsing((path ? path : ""), cwd, &folder_id, &parsed_path, oDB)) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_CWD_ERROR, path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_DATACUBE_PERMISSION_ERROR, user);
				free(parsed_path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			path = parsed_path;
		} else {
			if (oph_odb_fs_get_session_home_id(((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, oDB, &home_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_DATACUBE_PERMISSION_ERROR, user);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			if (oph_odb_fs_build_path(home_id, oDB, &home_path)) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_CWD_ERROR, path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			folder_id = home_id;
			path = home_path;
		}

	} else if (level < 9) {

		//Check if datacube exists (by ID container and datacube)
		if (datacube_name) {
			if (oph_pid_parse_pid(datacube_name, &id_container, &id_datacube, &uri)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_ERROR, datacube_name);
				if (uri)
					free(uri);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			} else if ((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_container, id_datacube, &exists)) || !exists) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input datacube doesn't exist\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_DATACUBE_NOT_EXIST, datacube_name);
				if (uri)
					free(uri);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (uri)
				free(uri);
			if ((oph_odb_fs_retrive_container_folder_id(oDB, id_container, &folder_id))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_DATACUBE_FOLDER_ERROR, datacube_name);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", user);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_DATACUBE_PERMISSION_ERROR, user);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "A datacube filter must be specified if level is bigger than 3\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MISSING_DATACUBE_ARGUMENT);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	char **keys = NULL;
	char **fieldtypes = NULL;
	int num_fields = 0;

	switch (level) {
		case 9:
			printf("+------------------------------------------+\n");
			printf("| %-40s |\n", "DATACUBE PID");
			printf("+------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 1;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, "Datacube List", NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					free(new_path);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 8:
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+----------------------+\n");
			printf("| %-18s | %-40s | %-20s | %-10s | %-30s | %-20s |\n", "CONTAINER PATH", "DATACUBE PID", "HOSTNAME", "ID DBMS", "DB NAME", "FRAGMENT NAME");
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+----------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 6;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("CONTAINER PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("HOSTNAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("ID DBMS");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DB NAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("FRAGMENT NAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_INT);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: cube %s", datacube_name);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 7:
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+\n");
			printf("| %-18s | %-40s | %-20s | %-10s | %-30s |\n", "CONTAINER PATH", "DATACUBE PID", "HOSTNAME", "ID DBMS", "DB NAME");
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 5;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("CONTAINER PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("HOSTNAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("ID DBMS");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DB NAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_INT);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: cube %s", datacube_name);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 6:
			printf("+--------------------+------------------------------------------+----------------------+------------+\n");
			printf("| %-18s | %-40s | %-20s | %-10s |\n", "CONTAINER PATH", "DATACUBE PID", "HOSTNAME", "ID DBMS");
			printf("+--------------------+------------------------------------------+----------------------+------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 4;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("CONTAINER PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("HOSTNAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("ID DBMS");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_INT);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: cube %s", datacube_name);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 5:
			printf("+--------------------+------------------------------------------+----------------------+\n");
			printf("| %-18s | %-40s | %-20s |\n", "CONTAINER PATH", "DATACUBE PID", "HOSTNAME");
			printf("+--------------------+------------------------------------------+----------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 3;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("CONTAINER PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("HOSTNAME");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: cube %s", datacube_name);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 4:
			printf("+------------------------------------------------------------------------+------------------------------------------+\n");
			printf("| %-70s | %-40s |\n", "CONTAINER PATH", "DATACUBE PID");
			printf("+------------------------------------------------------------------------+------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 2;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("CONTAINER PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: cube %s", datacube_name);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 3:
			printf("+---+--------------------+------------------------------------------+----------------------+-------+-----------------------------------------------+\n");
			printf("| %-1s | %-18s | %-40s | %-20s | %-5s | %-45s |\n", "T", "PATH", "DATACUBE PID", "MEASURE", "LEVEL", "SOURCE");
			printf("+---+--------------------+------------------------------------------+----------------------+-------+-----------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 7;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("T");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DESCRIPTION");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("MEASURE");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("LEVEL");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("SOURCE");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_INT);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				if (oph_pid_drop_session_prefix(path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: %.*s", (strlen(new_path) == 1) ? 1 : (int) strlen(new_path) - 1, new_path);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					free(new_path);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 2:
			printf("+---+------------------------------------------------------------------------+------------------------------------------+\n");
			printf("| %-1s | %-70s | %-40s |\n", "T", "PATH", "DATACUBE PID");
			printf("+---+------------------------------------------------------------------------+------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 4;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("T");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DATACUBE PID");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("DESCRIPTION");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				if (oph_pid_drop_session_prefix(path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: %.*s", (strlen(new_path) == 1) ? 1 : (int) strlen(new_path) - 1, new_path);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					free(new_path);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 1:
			printf("+---+------------------------------------------------------------------------+\n");
			printf("| %-1s | %-70s |\n", "T", "PATH");
			printf("+---+------------------------------------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 2;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("T");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				if (oph_pid_drop_session_prefix(path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: %.*s", (strlen(new_path) == 1) ? 1 : (int) strlen(new_path) - 1, new_path);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					free(new_path);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		case 0:
			printf("+---+------------------------------------------------------------------------+\n");
			printf("| %-1s | %-70s |\n", "T", "PATH");
			printf("+---+------------------------------------------------------------------------+\n");
			//ALLOC/SET VALUES FOR JSON START
			num_fields = 2;
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				int iii, jjj = 0, kkk = 0;
				keys = (char **) malloc(sizeof(char *) * num_fields);
				if (!keys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "keys");
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtypes");
					mysql_free_result(info_list);
					if (keys)
						free(keys);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//alloc keys
				keys[jjj] = strdup("T");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				keys[jjj] = strdup("PATH");
				if (!keys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "key");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;
				//alloc fieldtypes
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;
				fieldtypes[kkk] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "fieldtype");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				kkk++;

				if (oph_pid_drop_session_prefix(path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					mysql_free_result(info_list);
					for (iii = 0; iii < jjj; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < kkk; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				snprintf(tmp_path, OPH_COMMON_BUFFER_LEN, "Ophidia Filesystem: %.*s", (strlen(new_path) == 1) ? 1 : (int) strlen(new_path) - 1, new_path);
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LIST, tmp_path, NULL, keys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					mysql_free_result(info_list);
					//FREE JSON VALUES START
					for (iii = 0; iii < num_fields; iii++)
						if (keys[iii])
							free(keys[iii]);
					if (keys)
						free(keys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					//FREE JSON VALUES END
					free(new_path);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				//FREE JSON VALUES START
				for (iii = 0; iii < num_fields; iii++)
					if (keys[iii])
						free(keys[iii]);
				if (keys)
					free(keys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				//FREE JSON VALUES END
			}
			//ALLOC/SET VALUES FOR JSON END
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_BAD_LEVEL_PARAMETER, level);
			mysql_free_result(info_list);
			if (new_path)
				free(new_path);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (level < 4) {

		if (parsed_path)
			free(parsed_path);

		char *tmp_uri = NULL;
		if (level >= 2) {
			if (oph_pid_get_uri(&tmp_uri)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_URI_ERROR);
				if (new_path)
					free(new_path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

		if (level < 3) {
			if (_oph_list_recursive_list_folders
			    (oDB, level, folder_id, container_name, tmp_uri, new_path, recursive_search, 1, handle->operator_json, objkeys, objkeys_num,
			     ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
				if (tmp_uri)
					free(tmp_uri);
				if (new_path)
					free(new_path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		} else {
			if (_oph_list_recursive_filtered_list_folders
			    (oDB, folder_id, container_name, tmp_uri, new_path, recursive_search, 1, measure, oper_level, src, handle->operator_json, objkeys, objkeys_num,
			     ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
				if (tmp_uri)
					free(tmp_uri);
				if (new_path)
					free(new_path);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		if (tmp_uri)
			free(tmp_uri);
		if (new_path)
			free(new_path);

	} else if (level < 9) {

		if (new_path)
			free(new_path);
		//retrieve information list
		if (oph_odb_stge_find_datacube_fragmentation_list(oDB, (level - 3), id_datacube, hostname, db_name, id_dbms, &info_list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive information list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_READ_LIST_INFO_ERROR);
			mysql_free_result(info_list);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Empty set
		if (!(num_rows = mysql_num_rows(info_list))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows find by query\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NO_ROWS_FOUND);
			mysql_free_result(info_list);
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}

		MYSQL_ROW row;

		char host_str[1000];
		char id_dbms_str[1000];
		char db_name_str[1000];
		char frag_name_str[1000];
		char out_path[MYSQL_BUFLEN];
		int first = 0;
		char container_path[MYSQL_BUFLEN];
		char *new_container_path = NULL;

		//For each ROW
		while ((row = mysql_fetch_row(info_list))) {
			if (!first) {
				if (oph_odb_fs_build_path(folder_id, oDB, &out_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				first = 1;
			}
			char **my_row = NULL;
			switch (level) {
				case 8:
					snprintf(container_path, MYSQL_BUFLEN, "%s%s", out_path, (row[0] ? row[0] : ""));
					if (oph_pid_drop_session_prefix(container_path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}

					snprintf(host_str, 1000, "%s (%s)", (row[1] ? row[1] : ""), (row[2] ? row[2] : ""));
					snprintf(id_dbms_str, 1000, "%s", (row[3] ? row[3] : ""));
					if (!strcmp(row[2], OPH_COMMON_DOWN_STATUS)) {
						snprintf(db_name_str, 1000, "{%s}", (row[4] ? row[4] : ""));
						snprintf(frag_name_str, 1000, "{%s}", (row[5] ? row[5] : ""));
					} else {
						snprintf(db_name_str, 1000, "%s", (row[4] ? row[4] : ""));
						snprintf(frag_name_str, 1000, "%s", (row[5] ? row[5] : ""));
					}
					//ADD ROW TO JSON
					num_fields = 6;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup(new_container_path);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(datacube_name);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(host_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(id_dbms_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(db_name_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(frag_name_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					if (oph_utl_short_folder(18, 0, new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					printf("| %-18s | %-40s | %-20s | %-10s | %-30s | %-20s |\n", new_container_path, datacube_name, host_str, id_dbms_str, db_name_str, frag_name_str);

					break;
				case 7:
					snprintf(container_path, MYSQL_BUFLEN, "%s%s", out_path, (row[0] ? row[0] : ""));
					if (oph_pid_drop_session_prefix(container_path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}

					snprintf(host_str, 1000, "%s (%s)", (row[1] ? row[1] : ""), (row[2] ? row[2] : ""));
					snprintf(id_dbms_str, 1000, "%s", (row[3] ? row[3] : ""));
					if (!strcmp(row[2], OPH_COMMON_DOWN_STATUS)) {
						snprintf(db_name_str, 1000, "{%s}", (row[4] ? row[4] : ""));
					} else {
						snprintf(db_name_str, 1000, "%s", (row[4] ? row[4] : ""));
					}
					//ADD ROW TO JSON
					num_fields = 5;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup(new_container_path);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(datacube_name);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(host_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(id_dbms_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(db_name_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					if (oph_utl_short_folder(18, 0, new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					printf("| %-18s | %-40s | %-20s | %-10s | %-30s |\n", new_container_path, datacube_name, host_str, id_dbms_str, db_name_str);
					break;
				case 6:
					snprintf(container_path, MYSQL_BUFLEN, "%s%s", out_path, (row[0] ? row[0] : ""));
					if (oph_pid_drop_session_prefix(container_path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}

					snprintf(host_str, 1000, "%s (%s)", (row[1] ? row[1] : ""), (row[2] ? row[2] : ""));
					snprintf(id_dbms_str, 1000, "%s", (row[3] ? row[3] : ""));
					//ADD ROW TO JSON
					num_fields = 4;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup(new_container_path);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(datacube_name);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(host_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(id_dbms_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					if (oph_utl_short_folder(18, 0, new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					printf("| %-18s | %-40s | %-20s | %-10s |\n", new_container_path, datacube_name, host_str, id_dbms_str);
					break;
				case 5:
					snprintf(container_path, MYSQL_BUFLEN, "%s%s", out_path, (row[0] ? row[0] : ""));
					if (oph_pid_drop_session_prefix(container_path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}

					snprintf(host_str, 1000, "%s (%s)", (row[1] ? row[1] : ""), (row[2] ? row[2] : ""));
					//ADD ROW TO JSON
					num_fields = 3;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup(new_container_path);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(datacube_name);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(host_str);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					if (oph_utl_short_folder(18, 0, new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse container path\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PARSE_CONTAINER_ERROR);
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					printf("| %-18s | %-40s | %-20s |\n", new_container_path, datacube_name, host_str);
					break;
				case 4:
					snprintf(container_path, MYSQL_BUFLEN, "%s%s", out_path, (row[0] ? row[0] : ""));
					if (oph_pid_drop_session_prefix(container_path, ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid, &new_container_path)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
						mysql_free_result(info_list);
						return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					}
					//ADD ROW TO JSON
					num_fields = 2;
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
						my_row = (char **) malloc(sizeof(char *) * num_fields);
						if (!my_row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
							mysql_free_result(info_list);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						int iii, jjj = 0;
						my_row[jjj] = strdup(new_container_path);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;
						my_row[jjj] = strdup(datacube_name);
						if (!my_row[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
							mysql_free_result(info_list);
							for (iii = 0; iii < jjj; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						}
						jjj++;

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							mysql_free_result(info_list);
							for (iii = 0; iii < num_fields; iii++)
								if (my_row[iii])
									free(my_row[iii]);
							if (my_row)
								free(my_row);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						for (iii = 0; iii < num_fields; iii++)
							if (my_row[iii])
								free(my_row[iii]);
						if (my_row)
							free(my_row);
					}
					//ADD ROW TO JSON
					printf("| %-70s | %-40s |\n", new_container_path, datacube_name);
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_BAD_LEVEL_PARAMETER, level);
					mysql_free_result(info_list);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (new_container_path) {
				free(new_container_path);
				new_container_path = NULL;
			}
		}
		mysql_free_result(info_list);

	} else {

		if (new_path)
			free(new_path);

		char *oph_web_server = NULL, *pid = NULL;
		if (oph_pid_get_uri(&oph_web_server)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		size_t len = task_string ? strlen(task_string) : 0;
		char _task_string[2 + len];
		if (!task_string)
			strcpy(_task_string, OPH_SEPARATOR_PARAM);
		else {
			char check = 0;
			size_t i, j;
			for (i = j = 0; i < len; ++i)
				if ((task_string[i] != OPH_SEPARATOR_SUBPARAM_OPEN) && (task_string[i] != OPH_SEPARATOR_SUBPARAM_CLOSE)) {
					_task_string[j++] = task_string[i];
					check = task_string[i] == OPH_SEPARATOR_PARAM[0];
				} else
					check = 0;
			if (!check)
				_task_string[j++] = OPH_SEPARATOR_PARAM[0];
			_task_string[j] = 0;
		}
		task_string = _task_string;

		unsigned int task_string_size = strlen(task_string) + OPH_SHORT_STRING_SIZE;	// Additional size is provided to consider OPH_MF_ARG_PATH or OPH_MF_ARG_DATACUBE_FILTER
		char tmp[task_string_size + 1], filter = 1;

		// No filter set
		if (!strchr(task_string, OPH_SEPARATOR_KV[0])) {
			char stop = 1;
			unsigned int i, j;
			for (i = j = 0; stop && (i < task_string_size) && (j < task_string_size); j++) {
				if (task_string[j] == OPH_SEPARATOR_PARAM[0])
					stop = 0;
				else if (task_string[j] != OPH_SEPARATOR_NULL)
					tmp[i++] = task_string[j];
			}
			tmp[i] = 0;
			if (!stop)	// Skip beyond OPH_SEPARATOR_PARAM[0]
				task_string[j] = 0;
			switch (strlen(tmp)) {
				case 0:
					break;
				case 1:
					if (tmp[0] == '*')
						filter = 0;
					break;
				case 3:
					if (!strcasecmp(tmp, "all"))
						filter = 0;
					break;
				default:
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "Consider '%s' as a path or subset string of cube ids.\n", task_string);
			}
			if (filter)	// By default all the task string is a path in case of src_path or a list of identifiers in case of datacube
			{
				char tmp2[strlen(tmp) + 1];
				int not_clause = tmp[0] == OPH_MF_SYMBOL_NOT[0] ? 1 : 0;
				if (not_clause) {
					strcpy(tmp2, tmp);
					task_string = tmp2;
				}
				snprintf(tmp, task_string_size, "%s%s=%s", OPH_MF_ARG_DATACUBE_FILTER, not_clause ? OPH_MF_SYMBOL_NOT : "", task_string + not_clause);
				task_string = tmp;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Add option '%s' to task string\n", tmp);
			}
		}
		// Check XML
		HASHTBL *task_tbl = NULL;
		if (oph_tp_task_params_parser2(OPH_MASSIVE_OPERATOR, filter ? task_string : "", &task_tbl)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to process input parameters\n");
			if (task_tbl)
				hashtbl_destroy(task_tbl);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char *sessionid = ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid;
		char *cwd = ((OPH_LIST_operator_handle *) handle->operator_handle)->cwd;

		// Filtering
		char query[OPH_MAX_STRING_SIZE];
		if (oph_filter(task_tbl, query, cwd ? cwd : OPH_MF_ROOT_FOLDER, sessionid, oDB, oph_web_server)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create filtering query.\n");
			if (task_tbl)
				hashtbl_destroy(task_tbl);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Filtering query: %s\n", query);

		if (task_tbl)
			hashtbl_destroy(task_tbl);

		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to submit query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to submit query\n");
			if (oph_web_server)
				free(oph_web_server);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (!(info_list = mysql_store_result(oDB->conn))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve data\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to retrieve data\n");
			if (oph_web_server)
				free(oph_web_server);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (mysql_field_count(oDB->conn) != 2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong result from query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong result from query\n");
			mysql_free_result(info_list);
			if (oph_web_server)
				free(oph_web_server);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		MYSQL_ROW row;
		num_fields = 1;
		char **my_row = NULL;
		while ((row = mysql_fetch_row(info_list))) {

			pid = NULL;
			if (oph_pid_create_pid(oph_web_server, (int) strtol(row[1], NULL, 10), (int) strtol(row[0], NULL, 10), &pid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create PID string\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_PID_CREATE_ERROR);
				if (pid)
					free(pid);
				mysql_free_result(info_list);
				if (oph_web_server)
					free(oph_web_server);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			//ADD ROW TO JSON
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_LIST)) {
				my_row = (char **) malloc(sizeof(char *) * num_fields);
				if (!my_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					if (pid)
						free(pid);
					mysql_free_result(info_list);
					if (oph_web_server)
						free(oph_web_server);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				int iii, jjj = 0;
				my_row[jjj] = strdup(pid);
				if (!my_row[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_MEMORY_ERROR_INPUT, "row value");
					if (pid)
						free(pid);
					mysql_free_result(info_list);
					if (oph_web_server)
						free(oph_web_server);
					for (iii = 0; iii < jjj; iii++)
						if (my_row[iii])
							free(my_row[iii]);
					if (my_row)
						free(my_row);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
				jjj++;

				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LIST, my_row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					if (pid)
						free(pid);
					mysql_free_result(info_list);
					if (oph_web_server)
						free(oph_web_server);
					for (iii = 0; iii < num_fields; iii++)
						if (my_row[iii])
							free(my_row[iii]);
					if (my_row)
						free(my_row);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (my_row[iii])
						free(my_row[iii]);
				if (my_row)
					free(my_row);
			}
			//ADD ROW TO JSON
			printf("| %-40s |\n", pid);
			if (pid)
				free(pid);
		}

		mysql_free_result(info_list);
		if (oph_web_server)
			free(oph_web_server);

		// ADD QUERY TO NOTIFICATION STRING
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_MF_ARG_QUERY, query);
		if (handle->output_string) {
			strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
			free(handle->output_string);
		}
		handle->output_string = strdup(tmp_string);
	}

	switch (level) {
		case 9:
			printf("+------------------------------------------+\n");
			break;
		case 8:
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+----------------------+\n");
			break;
		case 7:
			printf("+--------------------+------------------------------------------+----------------------+------------+--------------------------------+\n");
			break;
		case 6:
			printf("+--------------------+------------------------------------------+----------------------+------------+\n");
			break;
		case 5:
			printf("+--------------------+------------------------------------------+----------------------+\n");
			break;
		case 4:
			printf("+------------------------------------------------------------------------+------------------------------------------+\n");
			break;
		case 3:
			break;
		case 2:
			printf("+---+------------------------------------------------------------------------+------------------------------------------+\n");
			break;
		case 1:
			printf("+---+------------------------------------------------------------------------+\n");
			break;
		case 0:
			printf("+---+------------------------------------------------------------------------+\n");
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "List level unrecognized\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_BAD_LEVEL_PARAMETER, level);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LIST_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_disconnect_from_ophidiadb(&((OPH_LIST_operator_handle *) handle->operator_handle)->oDB);
	oph_odb_free_ophidiadb(&((OPH_LIST_operator_handle *) handle->operator_handle)->oDB);
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->container_name) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->container_name);
		((OPH_LIST_operator_handle *) handle->operator_handle)->container_name = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->user);
		((OPH_LIST_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->cwd);
		((OPH_LIST_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->path) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->path);
		((OPH_LIST_operator_handle *) handle->operator_handle)->path = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name);
		((OPH_LIST_operator_handle *) handle->operator_handle)->datacube_name = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->filter) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->filter);
		((OPH_LIST_operator_handle *) handle->operator_handle)->filter = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->measure);
		((OPH_LIST_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->src) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->src);
		((OPH_LIST_operator_handle *) handle->operator_handle)->src = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->hostname) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->hostname);
		((OPH_LIST_operator_handle *) handle->operator_handle)->hostname = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->db_name) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->db_name);
		((OPH_LIST_operator_handle *) handle->operator_handle)->db_name = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_LIST_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_LIST_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	free((OPH_LIST_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
