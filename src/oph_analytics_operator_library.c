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

#include "oph_analytics_operator_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <errno.h>

#include "debug.h"
#include "oph_task_parser_library.h"
#include "oph_input_parameters.h"

extern int msglevel;

static int oph_find_operator_library(char *operator_type, char **dyn_lib);

int oph_operator_struct_initializer(int size, int myrank, oph_operator_struct *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	handle->operator_type = NULL;
	handle->operator_handle = NULL;
	handle->operator_json = NULL;
	handle->output_string = NULL;
	handle->output_json = NULL;
	handle->output_path = NULL;
	handle->output_name = NULL;
	handle->output_code = 0;
	handle->proc_number = size;
	handle->proc_rank = myrank;
	handle->lib = NULL;
	handle->dlh = NULL;

	return 0;
}

int oph_set_env(HASHTBL *task_tbl, oph_operator_struct *handle)
{
	int (*_oph_set_env)(HASHTBL * task_tbl, oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	//If already executed don't procede further
	if (handle->dlh)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *value = hashtbl_get(task_tbl, OPH_IN_PARAM_OPERATOR_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OPERATOR_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Set operator type
	if (!(handle->operator_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocation error\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	lt_dlinit();

	if (oph_find_operator_library(handle->operator_type, &handle->lib)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver library not found\n");
		return OPH_ANALYTICS_OPERATOR_LIB_NOT_FOUND;
	}

	if (!(handle->dlh = (lt_dlhandle) lt_dlopen(handle->lib))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "lt_dlopen error: %s (library '%s')\n", lt_dlerror(), handle->lib);
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_set_env = (int (*)(HASHTBL *, oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_ENV_SET_FUNC))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load driver function\n");
		return OPH_ANALYTICS_OPERATOR_DLSYM_ERR;
	}

	return _oph_set_env(task_tbl, handle);
}

int oph_init_task(oph_operator_struct *handle)
{
	int (*_oph_init_task)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_init_task = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_TASK_INIT_FUNC)))
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	return _oph_init_task(handle);
}

int oph_distribute_task(oph_operator_struct *handle)
{
	int (*_oph_distribute_task)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_distribute_task = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_TASK_DISTRIBUTE_FUNC)))
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	return _oph_distribute_task(handle);
}

int oph_execute_task(oph_operator_struct *handle)
{
	int (*_oph_execute_task)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_execute_task = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_TASK_EXECUTE_FUNC))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load driver function\n");
		return OPH_ANALYTICS_OPERATOR_DLSYM_ERR;
	}

	return _oph_execute_task(handle);
}

int oph_reduce_task(oph_operator_struct *handle)
{
	int (*_oph_reduce_task)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_reduce_task = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_TASK_REDUCE_FUNC)))
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	return _oph_reduce_task(handle);
}

int oph_destroy_task(oph_operator_struct *handle)
{
	int (*_oph_destroy_task)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_destroy_task = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_TASK_DESTROY_FUNC)))
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	return _oph_destroy_task(handle);
}

int oph_unset_env(oph_operator_struct *handle)
{
	int (*_oph_unset_env)(oph_operator_struct * handle);

	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_HANDLE;
	}

	if (!handle->dlh) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver not properly loaded\n");
		return OPH_ANALYTICS_OPERATOR_DLOPEN_ERR;
	}

	if (!(_oph_unset_env = (int (*)(oph_operator_struct *)) lt_dlsym(handle->dlh, OPH_ANALYTICS_OPERATOR_ENV_UNSET_FUNC))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load driver function\n");
		return OPH_ANALYTICS_OPERATOR_DLSYM_ERR;
	}
	//Release operator resources
	int res;
	if ((res = _oph_unset_env(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to release resources\n");
		return res;
	}
	//Release handle resources
	if (handle->operator_type) {
		free(handle->operator_type);
		handle->operator_type = NULL;
	}
	if (handle->lib) {
		free(handle->lib);
		handle->lib = NULL;
	}
#ifndef OPH_WITH_VALGRIND
	if (handle->dlh && (lt_dlclose(handle->dlh))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "lt_dlclose error: %s (library %s)\n", lt_dlerror(), handle->lib);
		return OPH_ANALYTICS_OPERATOR_DLCLOSE_ERR;
	}
#endif

	return res;
}

int oph_exit_task()
{
#ifndef OPH_WITH_VALGRIND
	if (lt_dlexit()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while executing lt_dlexit\n");
		return OPH_ANALYTICS_OPERATOR_DLEXIT_ERR;
	}
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

static int oph_find_operator_library(char *operator_type, char **dyn_lib)
{
	FILE *fp = NULL;
	char line[OPH_ANALYTICS_OPERATOR_BUFLEN] = { '\0' };
	char value[OPH_ANALYTICS_OPERATOR_BUFLEN] = { '\0' };
	char dyn_lib_str[OPH_ANALYTICS_OPERATOR_BUFLEN] = { '\0' };
	char *res_string = NULL;

	if (!operator_type || !dyn_lib) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter\n");
		return -1;
	}

	snprintf(dyn_lib_str, sizeof(dyn_lib_str), OPH_ANALYTICS_DYNAMIC_LIBRARY_FILE_PATH, OPH_ANALYTICS_LOCATION);

	fp = fopen(dyn_lib_str, "r");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver file not found '%s'\n", dyn_lib_str);
		return -1;	// driver not found
	}

	while (!feof(fp)) {
		res_string = fgets(line, OPH_ANALYTICS_OPERATOR_BUFLEN, fp);
		if (!res_string) {
			fclose(fp);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read file line\n");
			return -1;
		}
		sscanf(line, "[%[^]]", value);
		if (!strcasecmp(value, operator_type)) {
			res_string = NULL;
			res_string = fgets(line, OPH_ANALYTICS_OPERATOR_BUFLEN, fp);
			if (!res_string) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read file line\n");
				return -1;
			}
			sscanf(line, "%[^\n]", value);
			*dyn_lib = (char *) strndup(value, OPH_ANALYTICS_OPERATOR_BUFLEN);
			if (!*dyn_lib) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocation error\n");
				return -2;
			}
			fclose(fp);
			return 0;
		}
	}
	fclose(fp);
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Driver file not found\n");
	return -2;		// driver not found
}
