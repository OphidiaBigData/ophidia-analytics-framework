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

#ifndef __OPH_ANALYTICS_OPERATOR_H
#define __OPH_ANALYTICS_OPERATOR_H

#include "oph_framework_paths.h"
#include "oph_common.h"
#include "hashtbl.h"
#include "oph_json_library.h"

//*****************Defines***************//

#define OPH_ANALYTICS_DYNAMIC_LIBRARY_FILE_PATH		OPH_FRAMEWORK_DYNAMIC_LIBRARY_FILE_PATH
#define OPH_ANALYTICS_OPERATOR_BUFLEN			OPH_COMMON_BUFFER_LEN

#define OPH_ANALYTICS_OPERATOR_ENV_SET_FUNC		"env_set"
#define OPH_ANALYTICS_OPERATOR_TASK_INIT_FUNC		"task_init"
#define OPH_ANALYTICS_OPERATOR_TASK_DISTRIBUTE_FUNC	"task_distribute"
#define OPH_ANALYTICS_OPERATOR_TASK_EXECUTE_FUNC	"task_execute"
#define OPH_ANALYTICS_OPERATOR_TASK_REDUCE_FUNC		"task_reduce"
#define OPH_ANALYTICS_OPERATOR_TASK_DESTROY_FUNC	"task_destroy"
#define OPH_ANALYTICS_OPERATOR_ENV_UNSET_FUNC		"env_unset"

//*************Error codes***************//

#define OPH_ANALYTICS_OPERATOR_SUCCESS			0

#define OPH_ANALYTICS_OPERATOR_NULL_HANDLE		-1
#define OPH_ANALYTICS_OPERATOR_NOT_NULL_LIB		-2
#define OPH_ANALYTICS_OPERATOR_LIB_NOT_FOUND		-3
#define OPH_ANALYTICS_OPERATOR_DLINIT_ERR		-4
#define OPH_ANALYTICS_OPERATOR_DLEXIT_ERR		-5
#define OPH_ANALYTICS_OPERATOR_DLOPEN_ERR		-6
#define OPH_ANALYTICS_OPERATOR_DLCLOSE_ERR		-7
#define OPH_ANALYTICS_OPERATOR_DLSYM_ERR		-8
#define OPH_ANALYTICS_OPERATOR_INIT_HANDLE_ERR		-9
#define OPH_ANALYTICS_OPERATOR_MEMORY_ERR		-10

#define OPH_ANALYTICS_OPERATOR_NULL_HANDLE_FIELD	-101
#define OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE	-102
#define OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE	-103
#define OPH_ANALYTICS_OPERATOR_CONNECTION_ERROR		-104
#define OPH_ANALYTICS_OPERATOR_NULL_CONNECTION		-105
#define OPH_ANALYTICS_OPERATOR_INVALID_PARAM		-106
#define OPH_ANALYTICS_OPERATOR_NULL_RESULT_HANDLE	-107
#define OPH_ANALYTICS_OPERATOR_COMMAND_ERROR		-108

#define OPH_ANALYTICS_OPERATOR_NOT_IMPLEMENTED		-201

#define OPH_ANALYTICS_OPERATOR_UTILITY_ERROR		-301
#define OPH_ANALYTICS_OPERATOR_BAD_PARAMETER		-302
#define OPH_ANALYTICS_OPERATOR_MYSQL_ERROR		-303

//****************Handle******************//

struct _oph_operator_struct {
	char *operator_type;
	void *operator_handle;
	oph_json *operator_json;
	char *output_string;
	char *output_json;
	char *output_path;
	char *output_name;
	int output_code;
	int proc_number;
	int proc_rank;
	char *lib;
	void *dlh;
	void *soap_data;
};
typedef struct _oph_operator_struct oph_operator_struct;

//*****************Functions***************//

//Initialize structure (mandatory)
int oph_operator_struct_initializer(int size, int myrank, oph_operator_struct * handle);

//Do operation initialization procedure (optional)
int oph_init_task(oph_operator_struct * handle);

//Set environment library and structures (mandatory)
int oph_set_env(HASHTBL * task_tbl, oph_operator_struct * handle);

//Do distribution stuff (optional)
int oph_distribute_task(oph_operator_struct * handle);

//Execute query or operation (mandatory)
int oph_execute_task(oph_operator_struct * handle);

//Do reduction operation (optional)
int oph_reduce_task(oph_operator_struct * handle);

//Do de-initialization procedure (optional)
int oph_destroy_task(oph_operator_struct * handle);

//Unset environment free struture memory (mandatory)
int oph_unset_env(oph_operator_struct * handle);

//Close dynamic library (mandatory)
int oph_exit_task();

#endif				//__OPH_ANALYTICS_OPERATOR_H
