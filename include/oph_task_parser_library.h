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

#ifndef __OPH_TASK_PARSER_H
#define __OPH_TASK_PARSER_H

#include "hashtbl.h"
#include "oph_common.h"
#include "oph_framework_paths.h"

#define OPH_TP_TASK_PARSER_SUCCESS		0
#define OPH_TP_TASK_PARSER_ERROR		1

#define	OPH_TP_TASKLEN				OPH_COMMON_BUFFER_LEN
#define	OPH_TP_BUFLEN				OPH_COMMON_BUFFER_LEN

#define OPH_TP_DTD_SCHEMA			OPH_FRAMEWORK_OPERATOR_DTD_PATH
#define OPH_TP_XML_PATH_LENGTH			OPH_COMMON_BUFFER_LEN
#define OPH_TP_XML_OPERATOR_FILE		OPH_FRAMEWORK_OPERATOR_XML_FILE_PATH_DESC

#define OPH_TP_XML_FILE_FORMAT			"%s_%s_%s.xml"
#define OPH_TP_XML_FILE_EXTENSION		"xml"

#define OPH_TP_XML_PRIMITIVE_TYPE		"primitive"
#define OPH_TP_XML_OPERATOR_TYPE		"operator"
#define OPH_TP_XML_HIERARCHY_TYPE		"hierarchy"
#define OPH_TP_XML_PRIMITIVE_TYPE_CODE		1
#define OPH_TP_XML_OPERATOR_TYPE_CODE		2
#define OPH_TP_XML_HIERARCHY_TYPE_CODE		3

#define OPH_TP_VERSION_SEPARATOR		'.'
#define OPH_TP_VERSION_EXTENSION		".xml"

#define OPH_TP_XML_ARGS				"args"
#define OPH_TP_XML_ARGUMENT			"argument"

#define OPH_TP_XML_ATTRIBUTE_TYPE		"type"
#define OPH_TP_XML_ATTRIBUTE_MANDATORY		"mandatory"
#define OPH_TP_XML_ATTRIBUTE_DEFAULT		"default"
#define OPH_TP_XML_ATTRIBUTE_MINVALUE		"minvalue"
#define OPH_TP_XML_ATTRIBUTE_MAXVALUE		"maxvalue"
#define OPH_TP_XML_ATTRIBUTE_VALUES		"values"

#define OPH_TP_PARAM_VALUE_SEPARATOR		'='
#define OPH_TP_PARAM_PARAM_SEPARATOR		';'
#define OPH_TP_MULTI_VALUE_SEPARATOR		'|'
#define OPH_TP_CONT_CUBE_SEPARATOR		'.'
#define OPH_TP_SKIP_SEPARATOR			'\0'	// '#' // Not used

#define OPH_TP_OPERATOR_NAME			OPH_IN_PARAM_OPERATOR_NAME

#define OPH_TP_INT_TYPE				OPH_COMMON_INT_TYPE
#define OPH_TP_REAL_TYPE			"real"

//Retrieve the correct xml file version for operators or primitives
int oph_tp_retrieve_function_xml_file(const char *function_name, const char *function_version, short int function_type_code, char (*xml_filename)[OPH_TP_BUFLEN]);

//Look for value of param in task string
int oph_tp_find_param_in_task_string(const char *task_string, const char *param, char *value);

//Load the operator parameters from task_string and XML into the hash table
int oph_tp_task_params_parser(char *task_string, HASHTBL ** hashtbl);
int oph_tp_task_params_parser2(const char *operator, char *task_string, HASHTBL ** hashtbl);

//Split multiple values params into a value_list of size value_num
int oph_tp_parse_multiple_value_param(char *values, char ***value_list, int *value_num);

//Free the value_list of size value_num
int oph_tp_free_multiple_value_param_list(char **value_list, int value_num);

//Start xml parsing
int oph_tp_start_xml_parser();

//End xml parsing
int oph_tp_end_xml_parser();

#endif				//__OPH_TASK_PARSER_H
