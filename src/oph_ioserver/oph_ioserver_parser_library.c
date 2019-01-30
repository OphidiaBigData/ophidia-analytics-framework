/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

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

#include "oph_ioserver_parser_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "oph_ioserver_log_error_codes.h"
#include "oph_ioserver_library.h"
#include "oph_ioserver_submission_query.h"

//Auxiliary parser functions 
int oph_ioserver_count_params_query_string(const char *server_name, const char *query_string, int *num_param)
{
	if (!query_string || !num_param) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IOSERVER_NULL_PARAM;
	}

	*num_param = 0;

	int i = 0;
	int k = 0;
	for (i = 0; query_string[i]; i++) {
		if (query_string[i] == OPH_IOSERVER_SQ_PARAM_SEPARATOR)
			k++;
	}
	*num_param = k;

	return OPH_IOSERVER_SUCCESS;
}

int oph_ioserver_validate_query_string(const char *server_name, const char *query_string)
{
	if (!query_string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IOSERVER_NULL_PARAM;
	}
	//Track the last char
	char previous_char = 0;
	int i;

	int delim_number = 0;

	for (i = 0; query_string[i]; i++) {
		switch (query_string[i]) {
			case OPH_IOSERVER_SQ_PARAM_SEPARATOR:{
					if ((previous_char == OPH_IOSERVER_SQ_PARAM_SEPARATOR || previous_char == OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_VALID_ERROR);
						logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_VALID_ERROR);
						return OPH_IOSERVER_VALID_ERROR;
					}
					break;
				}
			case OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR:{
					if ((previous_char == OPH_IOSERVER_SQ_PARAM_SEPARATOR || previous_char == OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_VALID_ERROR);
						logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_VALID_ERROR);
						return OPH_IOSERVER_VALID_ERROR;
					}
					break;
				}
			case OPH_IOSERVER_SQ_STRING_DELIMITER:{
					delim_number++;
					break;
				}
		}
		previous_char = query_string[i];
	}
	//Check that number of string delimiters is even
	if (delim_number % 2 == 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_VALID_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_VALID_ERROR);
		return OPH_IOSERVER_VALID_ERROR;
	}


	return OPH_IOSERVER_SUCCESS;
}

int oph_ioserver_load_query_string_params(const char *server_name, const char *query_string, HASHTBL * hashtbl)
{
	if (!query_string || !hashtbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IOSERVER_NULL_PARAM;
	}

	const char *ptr_begin, *ptr_equal, *ptr_end;

	ptr_begin = query_string;
	ptr_equal = strchr(query_string, OPH_IOSERVER_SQ_VALUE_SEPARATOR);
	ptr_end = strchr(query_string, OPH_IOSERVER_SQ_PARAM_SEPARATOR);

	char param[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char value[OPH_IOSERVER_SQ_LEN] = { '\0' };

	while (ptr_end) {
		if (!ptr_begin || !ptr_equal || !ptr_end) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_VALID_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_VALID_ERROR);
			return OPH_IOSERVER_VALID_ERROR;
		}

		if (ptr_end < ptr_equal) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_VALID_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_VALID_ERROR);
			return OPH_IOSERVER_VALID_ERROR;
		}

		strncpy(param, ptr_begin, strlen(ptr_begin) - strlen(ptr_equal));
		param[strlen(ptr_begin) - strlen(ptr_equal)] = 0;

		strncpy(value, ptr_equal + 1, strlen(ptr_equal + 1) - strlen(ptr_end));
		value[strlen(ptr_equal + 1) - strlen(ptr_end)] = 0;

		hashtbl_insert(hashtbl, (char *) param, (char *) value);
		ptr_begin = ptr_end + 1;
		ptr_equal = strchr(ptr_end + 1, OPH_IOSERVER_SQ_VALUE_SEPARATOR);
		ptr_end = strchr(ptr_end + 1, OPH_IOSERVER_SQ_PARAM_SEPARATOR);
	}

	return OPH_IOSERVER_SUCCESS;
}

char *multival_strchr(char *str)
{

	int i = 0;
	int string_flag = 0;
	for (i = 0; str[i]; i++) {
		//In this case set the flag for an opening string delimiter
		if (str[i] == OPH_IOSERVER_SQ_STRING_DELIMITER && !string_flag)
			string_flag = 1;
		//Count separator only if not in the middle of a string
		else if (str[i] == OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR && !string_flag)
			return &(str[i]);
		//If string is open, then the new delimiter will close it
		else if (str[i] == OPH_IOSERVER_SQ_STRING_DELIMITER && string_flag)
			string_flag = 0;
	}
	return NULL;
}

int oph_ioserver_parse_multivalue_arg(const char *server_name, char *values, char ***value_list, int *value_num)
{
	if (!values || !value_list || !value_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IOSERVER_NULL_PARAM;
	}

	int param_num = 1;
	int j, i;

	*value_list = NULL;

	//Count number of parameters
	//NOTE: Does not count those enclosed by ''
	int string_flag = 0;
	for (i = 0; values[i]; i++) {
		//In this case set the flag for an opening string delimiter
		if (values[i] == OPH_IOSERVER_SQ_STRING_DELIMITER && !string_flag)
			string_flag = 1;
		//Count separator only if not in the middle of a string
		else if (values[i] == OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR && !string_flag)
			param_num++;
		//If string is open, then the new delimiter will close it
		else if (values[i] == OPH_IOSERVER_SQ_STRING_DELIMITER && string_flag)
			string_flag = 0;
	}

	*value_list = (char **) malloc(param_num * sizeof(char *));
	if (!(*value_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MEMORY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_name, OPH_IOSERVER_LOG_MEMORY_ERROR);
		return OPH_IOSERVER_MEMORY_ERR;
	}

	char *ptr_begin, *ptr_end;

	ptr_begin = values;
	ptr_end = multival_strchr(values);
	j = 0;
	while (ptr_begin) {
		if (ptr_end) {
			(*value_list)[j] = ptr_begin;
			(*value_list)[j][strlen(ptr_begin) - strlen(ptr_end)] = 0;
			ptr_begin = ptr_end + 1;
			ptr_end = multival_strchr(ptr_end + 1);
		} else {
			(*value_list)[j] = ptr_begin;
			(*value_list)[j][strlen(ptr_begin)] = 0;
			ptr_begin = NULL;
		}
		j++;
	}

	*value_num = param_num;
	return OPH_IOSERVER_SUCCESS;
}
