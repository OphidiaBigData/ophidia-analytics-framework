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

#define _GNU_SOURCE
#include "MYSQL_ioserver.h"

#include <unistd.h>
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "oph_ioserver_plugins_log_error_codes.h"
#include "oph_ioserver_submission_query.h"
#include "oph_ioserver_parser_library.h"
#include "hashtbl.h"

int oph_map_keyword(oph_ioserver_handler * handle, char *keyword, char (*argument)[OPH_IOSERVER_SQ_LEN])
{
	if (!keyword || !argument) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	int n = 0;
	char before_key[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char key[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char after_key[OPH_IOSERVER_SQ_LEN] = { '\0' };

	char *var = strchr(keyword, '@');
	char *ptr = strchr(var, ' ');

	if (var) {

		if (!ptr) {
			snprintf(before_key, strlen(keyword) - strlen(var) + 1, "%s", keyword);
			snprintf(key, strlen(var) + 1, "%s", var);
		} else {
			snprintf(before_key, strlen(keyword) - strlen(var) + 1, "%s", keyword);
			snprintf(key, strlen(var) - strlen(ptr) + 1, "%s", var);
			snprintf(after_key, strlen(ptr) + 1, "%s", ptr);
		}

		//SWITCH on keyword
		if (strncasecmp(key, OPH_IOSERVER_SQ_KW_TABLE_SIZE, STRLEN_MAX(key, OPH_IOSERVER_SQ_KW_TABLE_SIZE)) == 0) {
			n += snprintf(key, OPH_IOSERVER_SQ_LEN, MYSQL_IO_KW_TABLE_SIZE);
		} else if (strncasecmp(key, OPH_IOSERVER_SQ_KW_INFO_SYSTEM, STRLEN_MAX(key, OPH_IOSERVER_SQ_KW_INFO_SYSTEM)) == 0) {
			n += snprintf(key, OPH_IOSERVER_SQ_LEN, MYSQL_IO_KW_INFO_SYSTEM);
		} else if (strncasecmp(key, OPH_IOSERVER_SQ_KW_INFO_SYSTEM_TABLE, STRLEN_MAX(key, OPH_IOSERVER_SQ_KW_INFO_SYSTEM_TABLE)) == 0) {
			n += snprintf(key, OPH_IOSERVER_SQ_LEN, MYSQL_IO_KW_INFO_SYSTEM_TABLE);
		} else if (strncasecmp(key, OPH_IOSERVER_SQ_KW_FUNCTION_FIELDS, STRLEN_MAX(key, OPH_IOSERVER_SQ_KW_FUNCTION_FIELDS)) == 0) {
			n += snprintf(key, OPH_IOSERVER_SQ_LEN, MYSQL_IO_KW_FUNCTION_FIELDS);
		} else if (strncasecmp(key, OPH_IOSERVER_SQ_KW_FUNCTION_TABLE, STRLEN_MAX(key, OPH_IOSERVER_SQ_KW_FUNCTION_TABLE)) == 0) {
			n += snprintf(key, OPH_IOSERVER_SQ_LEN, MYSQL_IO_KW_FUNCTION_TABLE);
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_UNKNOWN_KEYWORD, keyword);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_UNKNOWN_KEYWORD, keyword);
			return MYSQL_IO_ERROR;
		}

		n += snprintf(*argument, OPH_IOSERVER_SQ_LEN, "%s %s %s", before_key, key, after_key);
	}

	return MYSQL_IO_SUCCESS;
}

int oph_limit_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL;
	int arg_list_num = 0;
	int i = 0;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_LIMIT);
	if (query_arg) {
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}
		//Limit can have at most 2 values
		if (arg_list_num > 2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_TOO_BIG);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_TOO_BIG);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}
		for (i = 0; i < arg_list_num; i++) {
			if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					return MYSQL_IO_ERROR;
				}
				query_arg_list[i] = (char *) &tmp_arg;
			}
			if (i != 0)
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
			else
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_LIMIT, query_arg);
			if (i != (arg_list_num - 1))
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		}
		if (query_arg_list)
			free(query_arg_list);
	}

	return MYSQL_IO_SUCCESS;
}

int oph_where2_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_WHEREL);
	if (query_arg) {
		if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				return MYSQL_IO_ERROR;
			}
			query_arg = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_WHERE, query_arg);

		query_arg = NULL;

		query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_WHEREC);
		if (query_arg) {
			if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					return MYSQL_IO_ERROR;
				}
				query_arg = (char *) &tmp_arg;
			}
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg);
			query_arg = NULL;

			query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_WHERER);
			if (query_arg) {
				if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
					//Execute keyword mapping
					if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
						logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
						return MYSQL_IO_ERROR;
					}
					query_arg = (char *) &tmp_arg;
				}
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg);
			}
		}
	}
	return MYSQL_IO_SUCCESS;
}

int oph_where_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_WHERE);
	if (query_arg) {
		if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				return MYSQL_IO_ERROR;
			}
			query_arg = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_WHERE, query_arg);
	}
	return MYSQL_IO_SUCCESS;
}

int oph_from_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL, **query_arg_list1 = NULL;
	int arg_list_num = 0, arg_list_num1 = 0;
	int i;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FROM);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FROM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FROM);
		return MYSQL_IO_ERROR;
	}
	char *query_arg1 = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FROM_ALIAS);
	if (query_arg1) {
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg1, &query_arg_list1, &arg_list_num1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			if (query_arg_list1)
				free(query_arg_list1);
			return MYSQL_IO_ERROR;
		}
		if (arg_list_num != arg_list_num1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
			if (query_arg_list)
				free(query_arg_list);
			if (query_arg_list1)
				free(query_arg_list1);
			return MYSQL_IO_ERROR;
		}
		for (i = 0; i < arg_list_num; i++) {
			if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					return MYSQL_IO_ERROR;
				}
				query_arg_list[i] = (char *) &tmp_arg;
			}
			if (query_arg_list1[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list1[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					if (query_arg_list1)
						free(query_arg_list1);
					return MYSQL_IO_ERROR;
				}
				query_arg_list1[i] = (char *) &tmp_arg;
			}

			if (i == 0)
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_FROM, query_arg_list[i]);
			else {
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
			}
			if (query_arg_list1[i][0] != '\0')
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_AS, query_arg_list1[i]);
			if (i != (arg_list_num - 1))
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);

		}
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
	} else {
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}

		for (i = 0; i < arg_list_num; i++) {
			if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					return MYSQL_IO_ERROR;
				}
				query_arg_list[i] = (char *) &tmp_arg;
			}

			if (i == 0)
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_FROM, query_arg_list[i]);
			else {
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
			}
			if (i != (arg_list_num - 1))
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		}
		if (query_arg_list)
			free(query_arg_list);
	}
	return MYSQL_IO_SUCCESS;
}

int oph_select_fields_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL, **query_arg_list1 = NULL;
	int arg_list_num = 0, arg_list_num1 = 0;
	int i;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FIELD);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		return MYSQL_IO_ERROR;
	}
	char *query_arg1 = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FIELD_ALIAS);
	if (query_arg1) {
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg1, &query_arg_list1, &arg_list_num1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			if (query_arg_list1)
				free(query_arg_list1);
			return MYSQL_IO_ERROR;
		}
		if (arg_list_num != arg_list_num1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
			if (query_arg_list)
				free(query_arg_list);
			if (query_arg_list1)
				free(query_arg_list1);
			return MYSQL_IO_ERROR;
		}
		for (i = 0; i < arg_list_num; i++) {
			if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					if (query_arg_list1)
						free(query_arg_list1);
					return MYSQL_IO_ERROR;
				}
				query_arg_list[i] = (char *) &tmp_arg;
			}
			if (query_arg_list1[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list1[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					if (query_arg_list1)
						free(query_arg_list1);
					return MYSQL_IO_ERROR;
				}
				query_arg_list1[i] = (char *) &tmp_arg;
			}

			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
			if (query_arg_list1[i][0] != '\0')
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_AS, query_arg_list1[i]);
			if (i != (arg_list_num - 1))
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		}
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
	} else {
		if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
			if (query_arg_list)
				free(query_arg_list);
			return MYSQL_IO_ERROR;
		}

		for (i = 0; i < arg_list_num; i++) {
			if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					if (query_arg_list)
						free(query_arg_list);
					return MYSQL_IO_ERROR;
				}
				query_arg_list[i] = (char *) &tmp_arg;
			}

			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
			if (i != (arg_list_num - 1))
				*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		}
		if (query_arg_list)
			free(query_arg_list);
	}

	return MYSQL_IO_SUCCESS;
}

int oph_order_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_ORDER);
	if (query_arg) {
		if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				return MYSQL_IO_ERROR;
			}
			query_arg = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_ORDER, query_arg);

		//If order clause exists, then evaluate also direction
		query_arg = NULL;

		query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_ORDER_DIR);
		if (query_arg) {
			if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
				//Execute keyword mapping
				if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
					return MYSQL_IO_ERROR;
				}
				query_arg = (char *) &tmp_arg;
			}
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg);
		}
	}

	return MYSQL_IO_SUCCESS;
}

int oph_group_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_GROUP);
	if (query_arg) {
		if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				return MYSQL_IO_ERROR;
			}
			query_arg = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_GROUP, query_arg);
	}

	return MYSQL_IO_SUCCESS;
}

int oph_insert_values_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL, **query_arg_list1 = NULL;
	int arg_list_num = 0, arg_list_num1 = 0;
	int i = 0;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FIELD);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		return MYSQL_IO_ERROR;
	}
	if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		if (query_arg_list)
			free(query_arg_list);
		return MYSQL_IO_ERROR;
	}
	query_arg = NULL;

	//Values section
	query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_VALUE);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_VALUE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_VALUE);
		if (query_arg_list)
			free(query_arg_list);
		return MYSQL_IO_ERROR;
	}
	if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list1, &arg_list_num1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
		return MYSQL_IO_ERROR;
	}
	if (arg_list_num != arg_list_num1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
		return MYSQL_IO_ERROR;
	}

	for (i = 0; i < arg_list_num; i++) {
		if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				if (query_arg_list)
					free(query_arg_list);
				if (query_arg_list1)
					free(query_arg_list1);
				return MYSQL_IO_ERROR;
			}
			query_arg_list[i] = (char *) &tmp_arg;
		}
		if (i == 0)
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_OPEN_BRACKET);
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
		if (i != (arg_list_num - 1))
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		else
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_CLOSE_BRACKET);
	}
	if (query_arg_list)
		free(query_arg_list);

	for (i = 0; i < arg_list_num; i++) {
		if (query_arg_list1[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg_list1[i], &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				if (query_arg_list1)
					free(query_arg_list1);
				return MYSQL_IO_ERROR;
			}
			query_arg_list1[i] = (char *) &tmp_arg;
		}
		if (i == 0)
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_VALUES);
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list1[i]);
		if (i != (arg_list_num - 1))
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		else
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_CLOSE_BRACKET);
	}
	if (query_arg_list1)
		free(query_arg_list1);

	return MYSQL_IO_SUCCESS;
}

int oph_multi_insert_values_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL, **query_arg_list1 = NULL;
	int arg_list_num = 0, arg_list_num1 = 0;
	int i = 0;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FIELD);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FIELD);
		return MYSQL_IO_ERROR;
	}
	if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		if (query_arg_list)
			free(query_arg_list);
		return MYSQL_IO_ERROR;
	}
	query_arg = NULL;

	//Values section
	query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_VALUE);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_VALUE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_VALUE);
		if (query_arg_list)
			free(query_arg_list);
		return MYSQL_IO_ERROR;
	}
	if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list1, &arg_list_num1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
		return MYSQL_IO_ERROR;
	}
	if ((arg_list_num1 < arg_list_num) || (arg_list_num1 % arg_list_num != 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND);
		if (query_arg_list)
			free(query_arg_list);
		if (query_arg_list1)
			free(query_arg_list1);
		return MYSQL_IO_ERROR;
	}

	for (i = 0; i < arg_list_num; i++) {
		if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				if (query_arg_list)
					free(query_arg_list);
				if (query_arg_list1)
					free(query_arg_list1);
				return MYSQL_IO_ERROR;
			}
			query_arg_list[i] = (char *) &tmp_arg;
		}
		if (i == 0)
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_OPEN_BRACKET);
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
		if (i != (arg_list_num - 1))
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		else
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_CLOSE_BRACKET);
	}
	if (query_arg_list)
		free(query_arg_list);

	for (i = 0; i < arg_list_num1; i++) {
		if (query_arg_list1[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg_list1[i], &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				if (query_arg_list1)
					free(query_arg_list1);
				return MYSQL_IO_ERROR;
			}
			query_arg_list1[i] = (char *) &tmp_arg;
		}
		if (i == 0)
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_VALUES_MULTI);
		if (i % arg_list_num == 0) {
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_OPEN_BRACKET);
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list1[i]);
		} else {
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list1[i]);
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_CLOSE_BRACKET);

		}
		if (i != (arg_list_num1 - 1))
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
	}
	if (query_arg_list1)
		free(query_arg_list1);

	return MYSQL_IO_SUCCESS;
}

int oph_func_args_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char **query_arg_list = NULL;
	int arg_list_num = 0;
	int i = 0;
	char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_ARG);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_ARG);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_ARG);
		return MYSQL_IO_ERROR;
	}
	if (oph_ioserver_parse_multivalue_arg(handle->server_type, query_arg, &query_arg_list, &arg_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG, query_arg);
		if (query_arg_list)
			free(query_arg_list);
		return MYSQL_IO_ERROR;
	}
	for (i = 0; i < arg_list_num; i++) {
		if (query_arg_list[i][0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg_list[i], &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				if (query_arg_list)
					free(query_arg_list);
				return MYSQL_IO_ERROR;
			}
			query_arg_list[i] = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, " %s", query_arg_list[i]);
		if (i != (arg_list_num - 1))
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SEPAR);
		else
			*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_CLOSE_BRACKET);
	}
	if (query_arg_list)
		free(query_arg_list);

	return MYSQL_IO_SUCCESS;
}

int oph_first_block(oph_ioserver_handler * handle, HASHTBL * hashtbl, const char *sql_part, const char *argument, int *start_from, char (*query)[OPH_IOSERVER_SQ_LEN])
{
	if (!hashtbl || !start_from || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	char tmp_arg[OPH_IOSERVER_SQ_LEN] = { '\0' };
	char *query_arg = hashtbl_get(hashtbl, argument);
	if (!query_arg) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, argument);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, argument);
		return MYSQL_IO_ERROR;
	} else {
		if (query_arg[0] == MYSQL_IO_QUERY_VAR_CHAR) {
			//Execute keyword mapping
			if (oph_map_keyword(handle, query_arg, &tmp_arg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR);
				return MYSQL_IO_ERROR;
			}
			query_arg = (char *) &tmp_arg;
		}
		*start_from += snprintf(*query + *start_from, OPH_IOSERVER_SQ_LEN, sql_part, query_arg);
	}

	return MYSQL_IO_SUCCESS;
}

int oph_query_parser(oph_ioserver_handler * handle, const char *query_string, char **query_mysql)
{
	if (!query_string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}
	//Check if string has correct format
	if (oph_ioserver_validate_query_string(handle->server_type, query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_QUERY_NOT_VALID);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_QUERY_NOT_VALID);
		return MYSQL_IO_ERROR;
	}

	char query[OPH_IOSERVER_SQ_LEN] = { '\0' };

	//Count number of arguments
	int number_arguments = 0;
	if (oph_ioserver_count_params_query_string(handle->server_type, query_string, &number_arguments)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_COUNT_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_COUNT_ERROR);
		return MYSQL_IO_ERROR;
	}
	//Create hash table for arguments
	HASHTBL *hashtbl = NULL;

	if (!(hashtbl = hashtbl_create(number_arguments + 1, NULL))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_HASHTBL_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_HASHTBL_ERROR);
		return MYSQL_IO_ERROR;
	}
	//Split all arguments and load each one into hast table
	if (oph_ioserver_load_query_string_params(handle->server_type, query_string, hashtbl)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_LOAD_ARGS_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_LOAD_ARGS_ERROR);
		hashtbl_destroy(hashtbl);
		return MYSQL_IO_ERROR;
	}
	//Retrieve operation type
	char *query_oper = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_OPERATION);
	if (!query_oper) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_OPERATION);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_OPERATION);
		hashtbl_destroy(hashtbl);
		return MYSQL_IO_ERROR;
	}

	int n = 0;

	//SWITCH on operation
	if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_CREATE_FRAG_SELECT, OPH_IOSERVER_SQ_ARG_FRAG, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Select fields + select aliases
		if (oph_select_fields_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "SELECT FIELDS");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "SELECT FIELDS");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//From table section
		if (oph_from_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FROM");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FROM");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Left - condition - right part of where clause
		if (oph_where_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "WHERE");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "WHERE");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Group by
		if (oph_group_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "GROUP");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "GROUP");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Order by section
		if (oph_order_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "ORDER");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "ORDER");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Limit clause
		if (oph_limit_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "LIMIT");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "LIMIT");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_SELECT, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_SELECT)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query
		n += snprintf(query + n, OPH_IOSERVER_SQ_LEN, MYSQL_IO_QUERY_SELECT);

		//Select fields + select aliases
		if (oph_select_fields_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "SELECT FIELDS");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "SELECT FIELDS");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//From table section
		if (oph_from_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FROM");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FROM");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Where clause
		if (oph_where_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "WHERE");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "WHERE");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Order by section
		if (oph_order_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "ORDER");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "ORDER");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Limit clause
		if (oph_limit_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "LIMIT");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "LIMIT");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_INSERT, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_INSERT)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_INSERT, OPH_IOSERVER_SQ_ARG_FRAG, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Insert fields
		if (oph_insert_values_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "INSERT VALUES");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "INSERT VALUES");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_MULTI_INSERT, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_MULTI_INSERT)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_INSERT, OPH_IOSERVER_SQ_ARG_FRAG, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		//Insert fields
		if (oph_multi_insert_values_block(handle, hashtbl, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "INSERT VALUES");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "INSERT VALUES");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_CREATE_FRAG, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_CREATE_FRAG)) == 0) {
		//Compose query by selecting fields in the right order 

		//MySQL uses special field name and types (it does not read columns from submission query) 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_CREATE_FRAG, OPH_IOSERVER_SQ_ARG_FRAG, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}

	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_FUNCTION, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_FUNCTION)) == 0) {
		//Compose query by selecting fields in the right order 

		//Check special server functions
		char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_FUNC);
		if (!query_arg) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FUNC);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_FUNC);
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
		if (strncasecmp(query_arg, "oph_export", STRLEN_MAX(query_oper, "oph_export")) == 0) {
			char *query_arg = hashtbl_get(hashtbl, OPH_IOSERVER_SQ_ARG_ARG);
			if (!query_arg) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_ARG);
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MISSING_ARG, OPH_IOSERVER_SQ_ARG_ARG);
				hashtbl_destroy(hashtbl);
				return MYSQL_IO_ERROR;
			}

			snprintf(query, OPH_IOSERVER_SQ_LEN, "SELECT id_dim, measure FROM %s ORDER BY id_dim", query_arg);
		} else if (strncasecmp(query_arg, "oph_size", STRLEN_MAX(query_oper, "oph_size")) == 0) {
			n = snprintf(query, OPH_IOSERVER_SQ_LEN, "SELECT %s AS size FROM %s WHERE table_name IN (", MYSQL_IO_KW_TABLE_SIZE, MYSQL_IO_KW_INFO_SYSTEM);

			//Function and arg section
			if (oph_func_args_block(handle, hashtbl, &n, &query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION ARGUMENTS");
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION ARGUMENTS");
				hashtbl_destroy(hashtbl);
				return MYSQL_IO_ERROR;
			}
		} else {
			//First part of query + new table name
			if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_FUNC, OPH_IOSERVER_SQ_ARG_FUNC, &n, &query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION NAME");
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION NAME");
				hashtbl_destroy(hashtbl);
				return MYSQL_IO_ERROR;
			}
			//Function and arg section
			if (oph_func_args_block(handle, hashtbl, &n, &query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION ARGUMENTS");
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FUNCTION ARGUMENTS");
				hashtbl_destroy(hashtbl);
				return MYSQL_IO_ERROR;
			}
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_DROP_FRAG, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_DROP_FRAG)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_DROP_FRAG, OPH_IOSERVER_SQ_ARG_FRAG, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "FRAG NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_CREATE_DB, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_CREATE_DB)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_CREATE_DB, OPH_IOSERVER_SQ_ARG_DB, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "DB NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "DB NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else if (strncasecmp(query_oper, OPH_IOSERVER_SQ_OP_DROP_DB, STRLEN_MAX(query_oper, OPH_IOSERVER_SQ_OP_DROP_DB)) == 0) {
		//Compose query by selecting fields in the right order 

		//First part of query + new table name
		if (oph_first_block(handle, hashtbl, MYSQL_IO_QUERY_DROP_DB, OPH_IOSERVER_SQ_ARG_DB, &n, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "DB NAME");
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR, "DB NAME");
			hashtbl_destroy(hashtbl);
			return MYSQL_IO_ERROR;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_UNKNOWN_OPERATION, query_oper);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_UNKNOWN_OPERATION, query_oper);
		hashtbl_destroy(hashtbl);
		return MYSQL_IO_ERROR;
	}

	hashtbl_destroy(hashtbl);
	//Return query
	*query_mysql = (char *) strndup(query, OPH_IOSERVER_SQ_LEN);
	if (!*query_mysql) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		return MYSQL_IO_ERROR;
	}

	return MYSQL_IO_SUCCESS;
}

//Support functions
int oph_query_is_statement(oph_ioserver_handler * handle, const char *operation, short int *is_stmt)
{
	if (!operation || !is_stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	*is_stmt = 0;
	char *char_ptr = (char *) operation;
	while (char_ptr[0] != '\0') {
		if (char_ptr[0] == '?') {
			*is_stmt = 1;
			break;
		}
		char_ptr++;
	}

	return MYSQL_IO_SUCCESS;
}

int oph_to_mysql_type(oph_ioserver_handler * handle, oph_ioserver_arg_types oph_type, enum enum_field_types *mysql_type)
{
	if (!mysql_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	switch (oph_type) {
		case OPH_IOSERVER_TYPE_DECIMAL:
			*mysql_type = MYSQL_TYPE_DECIMAL;
			break;
		case OPH_IOSERVER_TYPE_LONG:
			*mysql_type = MYSQL_TYPE_LONG;
			break;
		case OPH_IOSERVER_TYPE_FLOAT:
			*mysql_type = MYSQL_TYPE_FLOAT;
			break;
		case OPH_IOSERVER_TYPE_DOUBLE:
			*mysql_type = MYSQL_TYPE_DOUBLE;
			break;
		case OPH_IOSERVER_TYPE_NULL:
			*mysql_type = MYSQL_TYPE_NULL;
			break;
		case OPH_IOSERVER_TYPE_LONGLONG:
			*mysql_type = MYSQL_TYPE_LONGLONG;
			break;
		case OPH_IOSERVER_TYPE_VARCHAR:
			*mysql_type = MYSQL_TYPE_VARCHAR;
			break;
		case OPH_IOSERVER_TYPE_BIT:
			*mysql_type = MYSQL_TYPE_BIT;
			break;
		case OPH_IOSERVER_TYPE_LONG_BLOB:
			*mysql_type = MYSQL_TYPE_LONG_BLOB;
			break;
		case OPH_IOSERVER_TYPE_BLOB:
			*mysql_type = MYSQL_TYPE_BLOB;
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			return MYSQL_IO_ERROR;
	}
	return MYSQL_IO_SUCCESS;
}

//Initialize storage server plugin
int _mysql_setup(oph_ioserver_handler * handle)
{
	if (!handle)
		pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);

	if (handle->is_thread == 0) {
		if (mysql_library_init(0, NULL, NULL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_LIB_INIT_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_LIB_INIT_ERROR);
			return MYSQL_IO_ERROR;
		}
	}

	return MYSQL_IO_SUCCESS;
}

//Connect or reconnect to storage server
int _mysql_connect(oph_ioserver_handler * handle, oph_ioserver_params * conn_params, void **connection)
{
	if (!connection || !conn_params) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	if (*connection == NULL) {
		if (!(*connection = (void *) mysql_init(NULL))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_INIT_ERROR, mysql_error((MYSQL *) * connection));
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_INIT_ERROR, mysql_error((MYSQL *) * connection));
			return MYSQL_IO_ERROR;
		}

		if (!mysql_real_connect((MYSQL *) * connection, conn_params->host, conn_params->user, conn_params->passwd, conn_params->db_name, conn_params->port, NULL, conn_params->opt_flag)
		    && mysql_errno((MYSQL *) connection)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_CONN_ERROR, mysql_error(*connection));
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_CONN_ERROR, mysql_error((MYSQL *) * connection));
			_mysql_close(handle, *connection);
			return MYSQL_IO_ERROR;
		}
	} else {
		if (mysql_ping((MYSQL *) * connection) && mysql_errno((MYSQL *) connection)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_LOST_CONN);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_LOST_CONN);

			mysql_close((MYSQL *) * connection);	// Flush any data related to previuos connection 

			if (!(*connection = (void *) mysql_init(NULL))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_INIT_ERROR, mysql_error((MYSQL *) * connection));
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_INIT_ERROR, mysql_error((MYSQL *) * connection));
				return MYSQL_IO_ERROR;
			}

			if (!mysql_real_connect((MYSQL *) * connection, conn_params->host, conn_params->user, conn_params->passwd, conn_params->db_name, conn_params->port, NULL, conn_params->opt_flag)
			    && mysql_errno((MYSQL *) connection)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_CONN_ERROR, mysql_error(*connection));
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_CONN_ERROR, mysql_error((MYSQL *) * connection));
				_mysql_close(handle, *connection);
				return MYSQL_IO_ERROR;
			}
		}
	}

	return MYSQL_IO_SUCCESS;
}

int _mysql_use_db(oph_ioserver_handler * handle, const char *db_name, void *connection)
{
	if (!connection || !db_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	if (mysql_select_db((MYSQL *) connection, db_name) && mysql_errno((MYSQL *) connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_USE_DB_ERROR, mysql_error((MYSQL *) connection));
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_USE_DB_ERROR, mysql_error((MYSQL *) connection));
		return MYSQL_IO_ERROR;
	}
	return MYSQL_IO_SUCCESS;
}

//Execute operation in storage server
int _mysql_execute_query(oph_ioserver_handler * handle, void *connection, oph_ioserver_query * query)
{
	if (!connection || !query || !query->statement) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}
	//TODO Handle the number of runs

	switch (query->type) {
		case OPH_IOSERVER_STMT_SIMPLE:
			if (mysql_query((MYSQL *) connection, (char *) query->statement) && mysql_errno((MYSQL *) connection)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_ERROR, mysql_error((MYSQL *) connection));
				logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_ERROR, mysql_error((MYSQL *) connection));
				return MYSQL_IO_ERROR;
			}
			break;
		case OPH_IOSERVER_STMT_BINARY:
			{
				MYSQL_STMT *tmp_stmt = (MYSQL_STMT *) ((_mysql_query_struct *) query->statement)->stmt;
				MYSQL_BIND *tmp_bind = (MYSQL_BIND *) ((_mysql_query_struct *) query->statement)->bind;
				if (mysql_stmt_bind_param(tmp_stmt, tmp_bind) && mysql_stmt_errno(tmp_stmt)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BINDING_ERROR, mysql_stmt_error(tmp_stmt));
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BINDING_ERROR, mysql_stmt_error(tmp_stmt));
					return MYSQL_IO_ERROR;
				}
				if (mysql_stmt_execute(tmp_stmt) && mysql_stmt_errno(tmp_stmt)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_STMT_EXEC_ERROR, mysql_stmt_error(tmp_stmt));
					logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_STMT_EXEC_ERROR, mysql_stmt_error(tmp_stmt));
					return MYSQL_IO_ERROR;
				}
			}
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			return MYSQL_IO_ERROR;
	}

	return MYSQL_IO_SUCCESS;
}

//Setup the query structure with given operation and array argument
int _mysql_setup_query(oph_ioserver_handler * handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg ** args, oph_ioserver_query ** query)
{
	UNUSED(tot_run)		// TODO Handle tot number of runs
	    if (!connection || !operation || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	*query = (oph_ioserver_query *) malloc(1 * sizeof(oph_ioserver_query));
	if (!*query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		return MYSQL_IO_ERROR;
	}

	short int is_stmt = 0;
	oph_query_is_statement(handle, operation, &is_stmt);

	char *sql_query = NULL;
	if (oph_query_parser(handle, operation, &sql_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_ERROR_PARSING);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_ERROR_PARSING);
		free(*query);
		*query = NULL;
		return MYSQL_IO_ERROR;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("PARSED   QUERY: %s\n", sql_query);
#endif

	if (!args || !is_stmt) {
		(*query)->type = OPH_IOSERVER_STMT_SIMPLE;
		(*query)->statement = (void *) strndup(sql_query, strlen(sql_query));
		if (!(*query)->statement) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			free(*query);
			*query = NULL;
			free(sql_query);
			return MYSQL_IO_ERROR;
		}
		free(sql_query);
	} else {
		(*query)->type = OPH_IOSERVER_STMT_BINARY;
		MYSQL_STMT *stmt = mysql_stmt_init((MYSQL *) connection);
		if (!stmt && mysql_errno((MYSQL *) connection)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_error((MYSQL *) connection));
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_error((MYSQL *) connection));
			free(*query);
			*query = NULL;
			free(sql_query);
			return MYSQL_IO_ERROR;
		}

		if (mysql_stmt_prepare(stmt, sql_query, strlen(sql_query)) && mysql_stmt_errno(stmt)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_error((MYSQL *) connection));
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_error((MYSQL *) connection));
			mysql_stmt_close(stmt);
			free(*query);
			*query = NULL;
			free(sql_query);
			return MYSQL_IO_ERROR;
		}
		free(sql_query);

		int arg_count = 0;
		oph_ioserver_query_arg *arg_ptr = args[arg_count++];
		//Reach null-termination
		while (arg_ptr)
			arg_ptr = args[arg_count++];

		//Remove NULL arg from count
		arg_count -= 1;

		int param_count = mysql_stmt_param_count(stmt);

		if (param_count != arg_count) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_stmt_error(stmt));
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_STMT_ERROR, mysql_stmt_error(stmt));
			mysql_stmt_close(stmt);
			free(*query);
			*query = NULL;
			return MYSQL_IO_ERROR;
		}
		MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
		if (!bind) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_BIND_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_BIND_ERROR, mysql_stmt_error(stmt));
			mysql_stmt_close(stmt);
			free(*query);
			*query = NULL;
			return MYSQL_IO_ERROR;
		}

		enum enum_field_types mysql_tmp_type;
		for (arg_count = 0; arg_count < param_count; arg_count++) {
			bind[arg_count].buffer_length = args[arg_count]->arg_length;
			oph_to_mysql_type(handle, args[arg_count]->arg_type, &mysql_tmp_type);
			bind[arg_count].buffer_type = mysql_tmp_type;
			bind[arg_count].length = (args[arg_count]->arg ? &(args[arg_count]->arg_length) : 0);
			bind[arg_count].is_null = (my_bool *) (args[arg_count]->arg_is_null ? 0 : &(args[arg_count]->arg_is_null));
			bind[arg_count].is_unsigned = 0;
			bind[arg_count].buffer = args[arg_count]->arg;
		}

		(*query)->statement = (void *) malloc(1 * sizeof(_mysql_query_struct));
		if (!(*query)->statement) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			free(bind);
			mysql_stmt_close(stmt);
			free(*query);
			*query = NULL;
			return MYSQL_IO_ERROR;
		}
		((_mysql_query_struct *) (*query)->statement)->stmt = stmt;
		((_mysql_query_struct *) (*query)->statement)->bind = bind;
	}

	return MYSQL_IO_SUCCESS;
}

//Release resources allocated for query
int _mysql_free_query(oph_ioserver_handler * handle, oph_ioserver_query * query)
{
	if (!query || !query->statement) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	switch (query->type) {
		case OPH_IOSERVER_STMT_SIMPLE:
			free((char *) query->statement);
			break;
		case OPH_IOSERVER_STMT_BINARY:
			mysql_stmt_close((MYSQL_STMT *) ((_mysql_query_struct *) query->statement)->stmt);
			free((MYSQL_BIND *) ((_mysql_query_struct *) query->statement)->bind);
			free((_mysql_query_struct *) query->statement);
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR);
			return MYSQL_IO_ERROR;
	}
	free(query);

	return MYSQL_IO_SUCCESS;
}


//Close connection to storage server
int _mysql_close(oph_ioserver_handler * handle, void **connection)
{
	if (!(*connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	if (*connection) {
		mysql_close((MYSQL *) (*connection));
		*connection = NULL;
	}

	return MYSQL_IO_SUCCESS;
}

//Finalize storage server plugin
int _mysql_cleanup(oph_ioserver_handler * handle)
{
	if (!handle)
		pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);

	if (handle->is_thread == 0)
		mysql_library_end();

	return MYSQL_IO_SUCCESS;
}

//Get the result set
int _mysql_get_result(oph_ioserver_handler * handle, void *connection, oph_ioserver_result ** result)
{
	if (!connection || !result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}

	if (*result == NULL) {
		*result = (oph_ioserver_result *) malloc(sizeof(oph_ioserver_result));
		if (!(*result)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
			return MYSQL_IO_MEMORY_ERROR;
		}
		(*result)->result_set = NULL;
		(*result)->max_field_length = NULL;
		(*result)->current_row = NULL;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NOT_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NOT_NULL_INPUT_PARAM);
		return MYSQL_IO_ERROR;
	}

	if (!((*result)->result_set = (void *) mysql_store_result((MYSQL *) connection)) && mysql_errno((MYSQL *) connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_STORE_ERROR, mysql_error((MYSQL *) connection));
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_STORE_ERROR, mysql_error((MYSQL *) connection));
		_mysql_free_result(handle, *result);
		return MYSQL_IO_ERROR;
	}

	(*result)->num_rows = mysql_num_rows((*result)->result_set);
	(*result)->num_fields = mysql_num_fields((*result)->result_set);
	(*result)->current_row = (oph_ioserver_row *) malloc(sizeof(oph_ioserver_row));
	(*result)->current_row->field_lengths = NULL;
	(*result)->current_row->row = NULL;
	(*result)->max_field_length = (unsigned long long *) malloc((*result)->num_fields * sizeof(unsigned long long));
	if (!(*result)->max_field_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR);
		_mysql_free_result(handle, *result);
		return MYSQL_IO_MEMORY_ERROR;
	}

	MYSQL_FIELD *fields = mysql_fetch_fields((MYSQL_RES *) ((*result)->result_set));
	unsigned int i;
	for (i = 0; i < (*result)->num_fields; i++) {
		(*result)->max_field_length[i] = fields[i].max_length;
	}

	return MYSQL_IO_SUCCESS;
}

//Get the next row
int _mysql_fetch_row(oph_ioserver_handler * handle, oph_ioserver_result * result, oph_ioserver_row ** current_row)
{
	if (!result || !current_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		_mysql_free_result(handle, result);
		return MYSQL_IO_NULL_PARAM;
	}
	if (result->result_set == NULL || result->current_row == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		_mysql_free_result(handle, result);
		return MYSQL_IO_NULL_PARAM;
	}

	MYSQL_ROW mysql_row = mysql_fetch_row((MYSQL_RES *) (result->result_set));
	result->current_row->row = (char **) mysql_row;
	result->current_row->field_lengths = mysql_fetch_lengths((MYSQL_RES *) (result->result_set));

	*current_row = result->current_row;

	return MYSQL_IO_SUCCESS;
}

//Release result set resources
int _mysql_free_result(oph_ioserver_handler * handle, oph_ioserver_result * result)
{
	if (!result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM);
		return MYSQL_IO_NULL_PARAM;
	}
	mysql_free_result(result->result_set);
	if (result->max_field_length) {
		free(result->max_field_length);
		result->max_field_length = NULL;
	}
	if (result->current_row) {
		free(result->current_row);
		result->current_row = NULL;
	}
	free(result);
	result = NULL;

	return MYSQL_IO_SUCCESS;
}
