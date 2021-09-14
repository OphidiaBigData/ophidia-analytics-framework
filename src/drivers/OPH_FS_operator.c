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
#include <glob.h>
#include <dirent.h>
#include <sys/stat.h>

#ifdef OPH_NETCDF
#include "oph_nc_library.h"
#endif

#ifdef OPH_ESDM
#include "oph_esdm_library.h"
#endif

#include "drivers/OPH_FS_operator.h"
#include "oph_analytics_operator_library.h"

#include "oph_framework_paths.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "ophidiadb/oph_odb_filesystem_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#include "oph_dimension_library.h"

#define OPH_FS_SUBSET_COORD	    "coord"
#define OPH_FS_HPREFIX '.'
#define OPH_SEPARATOR_PARAM ";"

#ifdef OPH_NETCDF
int _oph_check_nc_measure(const char *file, const char *measure)
{
	int ncid, varidp;

	if (!file || !measure) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing parameter\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Missing parameter\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (nc_open(file, NC_NOWRITE, &ncid))
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

	if (nc_inq_varid(ncid, measure, &varidp))
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

	nc_close(ncid);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
#endif

int cmpfunc(const void *a, const void *b)
{
	char const *aa = (char const *) a;
	char const *bb = (char const *) b;
	return strcmp(aa, bb);
}

int openDir(const char *path, int recursive, size_t * counter, char **buffer, char *file)
{
	if (!path || !counter || !buffer)
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

	DIR *dirp = opendir(path);
	if (!dirp)
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

	struct dirent *entry = NULL, save_entry;
	char *sub;
	int s;

	if (recursive < 0)
		recursive++;

	if (file) {
		glob_t globbuf;
		char *path_and_file = NULL;
		s = asprintf(&path_and_file, "%s/%s", path, file);
		if (!path_and_file) {
			closedir(dirp);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if ((s = glob(path_and_file, GLOB_MARK | GLOB_NOSORT, NULL, &globbuf))) {
			if (s != GLOB_NOMATCH) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse '%s'\n", path_and_file);
				free(path_and_file);
				closedir(dirp);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			} else {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "No object found.\n");
				if (!recursive) {
					free(path_and_file);
					closedir(dirp);
					return OPH_ANALYTICS_OPERATOR_SUCCESS;
				}
			}
		}
		free(path_and_file);
		*counter += globbuf.gl_pathc;
		size_t i;
		for (i = 0; i < globbuf.gl_pathc; ++i) {
			if (*buffer) {
				sub = *buffer;
				s = asprintf(buffer, "%s%s%s", sub, globbuf.gl_pathv[i], OPH_SEPARATOR_PARAM);
				free(sub);
			} else
				s = asprintf(buffer, "%s%s", globbuf.gl_pathv[i], OPH_SEPARATOR_PARAM);
		}
		globfree(&globbuf);
	}

	char full_filename[PATH_MAX];
	struct stat file_stat;

	int result;
	while (!readdir_r(dirp, &save_entry, &entry) && entry) {
		if (*entry->d_name != OPH_FS_HPREFIX) {
			snprintf(full_filename, PATH_MAX, "%s/%s", path, entry->d_name);
			lstat(full_filename, &file_stat);
			if (!file && (S_ISREG(file_stat.st_mode) || S_ISLNK(file_stat.st_mode) || S_ISDIR(file_stat.st_mode))) {
				(*counter)++;
				if (*buffer) {
					sub = *buffer;
					s = asprintf(buffer, "%s%s/%s%s", sub, path, entry->d_name, OPH_SEPARATOR_PARAM);
					free(sub);
				} else
					s = asprintf(buffer, "%s/%s%s", path, entry->d_name, OPH_SEPARATOR_PARAM);
			}
			if (recursive && S_ISDIR(file_stat.st_mode)) {
				sub = NULL;
				s = asprintf(&sub, "%s/%s", path, entry->d_name);
				if (!sub)
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				else {
					result = openDir(sub, recursive, counter, buffer, file);
					free(sub);
				}
				if (result) {
					closedir(dirp);
					return result;
				}
			}
		}
	}
	closedir(dirp);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int write_json(char (*filenames)[OPH_COMMON_BUFFER_LEN], int jj, oph_json * operator_json, int num_fields)
{
	int ii, iii, jjj, result = OPH_ANALYTICS_OPERATOR_SUCCESS;
	char **jsonvalues = NULL;

	qsort(filenames, jj, OPH_COMMON_BUFFER_LEN, cmpfunc);

	for (ii = 0; ii < jj; ++ii) {
		jsonvalues = (char **) calloc(num_fields, sizeof(char *));
		if (!jsonvalues) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "values");
			result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			break;
		}
		jjj = 0;
		jsonvalues[jjj] = strdup(filenames[ii][strlen(filenames[ii]) - 1] == '/' ? "d" : "f");
		if (!jsonvalues[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
			for (iii = 0; iii < jjj; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			break;
		}
		jjj++;
		jsonvalues[jjj] = strdup(filenames[ii]);
		if (!jsonvalues[jjj]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORECUBE_MEMORY_ERROR_INPUT, "value");
			for (iii = 0; iii < jjj; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			break;
		}
		if (oph_json_add_grid_row(operator_json, OPH_JSON_OBJKEY_FS, jsonvalues)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
			for (iii = 0; iii < num_fields; iii++)
				if (jsonvalues[iii])
					free(jsonvalues[iii]);
			if (jsonvalues)
				free(jsonvalues);
			result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			break;
		}
		for (iii = 0; iii < num_fields; iii++)
			if (jsonvalues[iii])
				free(jsonvalues[iii]);
		if (jsonvalues)
			free(jsonvalues);
	}

	return result;
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_FS_operator_handle *) calloc(1, sizeof(OPH_FS_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_FS_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->mode = -1;
	((OPH_FS_operator_handle *) handle->operator_handle)->path = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->path_num = -1;
	((OPH_FS_operator_handle *) handle->operator_handle)->file = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->measure = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->recursive = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->depth = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->realpath = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_FS_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->sub_types = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_filters = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_types = 0;
	((OPH_FS_operator_handle *) handle->operator_handle)->offset = NULL;
	((OPH_FS_operator_handle *) handle->operator_handle)->s_offset_num = 0;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_FS_operator_handle *) handle->operator_handle)->objkeys, &((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SYSTEM_COMMAND);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_SYSTEM_COMMAND);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SYSTEM_COMMAND);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcasecmp(value, OPH_FS_CMD_LS)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->mode = OPH_FS_MODE_LS;
	} else if (!strcasecmp(value, OPH_FS_CMD_CD)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->mode = OPH_FS_MODE_CD;
	} else if (!strcasecmp(value, OPH_FS_CMD_MD)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->mode = OPH_FS_MODE_MD;
	} else if (!strcasecmp(value, OPH_FS_CMD_RM)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->mode = OPH_FS_MODE_RM;
	} else if (!strcasecmp(value, OPH_FS_CMD_MV)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->mode = OPH_FS_MODE_MV;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", value);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_PARAMETER, value);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATA_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_DATA_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATA_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_FS_operator_handle *) handle->operator_handle)->path, &((OPH_FS_operator_handle *) handle->operator_handle)->path_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->path, ((OPH_FS_operator_handle *) handle->operator_handle)->path_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FILE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_FILE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_FILE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FS_operator_handle *) handle->operator_handle)->file = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_MEASURE_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MEASURE_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FS_operator_handle *) handle->operator_handle)->measure = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FS_operator_handle *) handle->operator_handle)->cwd = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CDD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_RECURSIVE_SEARCH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_RECURSIVE_SEARCH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_RECURSIVE_SEARCH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->recursive = 1;
	} else if (strcmp(value, OPH_COMMON_NO_VALUE)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DEPTH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_DEPTH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DEPTH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_FS_operator_handle *) handle->operator_handle)->depth = (int) strtol(value, NULL, 10);
	if (((OPH_FS_operator_handle *) handle->operator_handle)->depth < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", value);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_PARAMETER, value);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_REALPATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_REALPATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_REALPATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_YES_VALUE)) {
		((OPH_FS_operator_handle *) handle->operator_handle)->realpath = 1;
	} else if (strcmp(value, OPH_COMMON_NO_VALUE)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_FS_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, OPH_IN_PARAM_CDD);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_FS_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = NULL, issubset = 0, isfilter = 0;
	int i, s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (s_offset_num > 0) {
		double *offset = ((OPH_FS_operator_handle *) handle->operator_handle)->offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, OPH_IN_PARAM_OFFSET);
			oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < s_offset_num; ++i)
			offset[i] = (double) strtod(s_offset[i], NULL);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		((OPH_FS_operator_handle *) handle->operator_handle)->s_offset_num = s_offset_num;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param
		    (value, &((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims, &((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		isfilter = 1;
		if (oph_tp_parse_multiple_value_param
		    (value, &((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters, &((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims != ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_FS_operator_handle *) handle->operator_handle)->sub_types, &((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_types)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims
	    && (((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_types > ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *abs_path = NULL;
	if (oph_pid_get_base_src_path(&abs_path)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (!abs_path)
		abs_path = strdup("/");

	size_t len = 0;
	char *rel_path = NULL, *dest_path = NULL, *url = NULL;
	char is_valid = strcasecmp(((OPH_FS_operator_handle *) handle->operator_handle)->path[0], OPH_FRAMEWORK_FS_DEFAULT_PATH);
	char file_is_valid = ((OPH_FS_operator_handle *) handle->operator_handle)->file && strcasecmp(((OPH_FS_operator_handle *) handle->operator_handle)->file, OPH_FRAMEWORK_FS_DEFAULT_PATH);
	char path[OPH_COMMON_BUFFER_LEN];

#ifdef OPH_ESDM
	if (file_is_valid) {
		url = strstr(((OPH_FS_operator_handle *) handle->operator_handle)->file, OPH_ESDM_PREFIX);
		if (url && (((OPH_FS_operator_handle *) handle->operator_handle)->mode != OPH_FS_MODE_LS)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse '%s' to perform this operation\n", ((OPH_FS_operator_handle *) handle->operator_handle)->file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_PATH_PARSING_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
#endif

	if (!url) {
		if (oph_odb_fs_path_parsing
		    (is_valid ? ((OPH_FS_operator_handle *) handle->operator_handle)->path[0] : (((OPH_FS_operator_handle *) handle->operator_handle)->mode == OPH_FS_MODE_CD ? "/" : ""),
		     ((OPH_FS_operator_handle *) handle->operator_handle)->cwd, NULL, &rel_path, NULL)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path '%s'\n", ((OPH_FS_operator_handle *) handle->operator_handle)->path[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_PATH_PARSING_ERROR);
			if (abs_path) {
				free(abs_path);
				abs_path = NULL;
			}
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		len = strlen(rel_path);
		if (len > 1)
			rel_path[len - 1] = 0;

		snprintf(path, OPH_COMMON_BUFFER_LEN, "%s%s", abs_path, rel_path);
		printf(OPH_FS_CD_MESSAGE " is: %s from %s and %s\n", path, ((OPH_FS_operator_handle *) handle->operator_handle)->path[0], ((OPH_FS_operator_handle *) handle->operator_handle)->cwd);

	} else
		snprintf(path, OPH_COMMON_BUFFER_LEN, "%s", url);

	int result = OPH_ANALYTICS_OPERATOR_SUCCESS;
	struct stat file_stat;

	switch (((OPH_FS_operator_handle *) handle->operator_handle)->mode) {

		case OPH_FS_MODE_LS:

			// ADD OUTPUT TO JSON AS TEXT
			if (oph_json_is_objkey_printable
			    (((OPH_FS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_FS)) {

				size_t ii = 0, jj = 0;
				int num_fields = 2, iii, jjj = 0;

				// Header
				char **jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jsonkeys[jjj] = strdup("T");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonkeys[jjj] = strdup("OBJECT");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj = 0;
				char **fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_MEMORY_ERROR_INPUT, "fieldtype");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_FS, rel_path ? rel_path : "URL", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
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
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
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

				if (!url) {	// Posix

					int recursive = 0;
					if (((OPH_FS_operator_handle *) handle->operator_handle)->recursive) {
						recursive = 1;
						if (((OPH_FS_operator_handle *) handle->operator_handle)->depth)
							recursive = -((OPH_FS_operator_handle *) handle->operator_handle)->depth;
					}

					char real_path[PATH_MAX], *_real_path;
					if (strchr(path, '*') || strchr(path, '~') || strchr(path, '{') || strchr(path, '}'))	// Use glob
					{
						if (recursive) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Recursive option cannot be selected for '%s'\n",
							      ((OPH_FS_operator_handle *) handle->operator_handle)->path[0]);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Recursive option cannot be selected for '%s'\n",
								((OPH_FS_operator_handle *) handle->operator_handle)->path[0]);
							result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							break;
						}
						int s;
						glob_t globbuf;
						if ((s = glob(path, GLOB_MARK | GLOB_NOSORT | GLOB_TILDE_CHECK | GLOB_BRACE, NULL, &globbuf))) {
							if (s != GLOB_NOMATCH) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse '%s'\n", ((OPH_FS_operator_handle *) handle->operator_handle)->path[0]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to parse '%s'\n",
									((OPH_FS_operator_handle *) handle->operator_handle)->path[0]);
								result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							}
							break;
						}
						for (ii = 0; ii < globbuf.gl_pathc; ++ii) {
							if (!realpath(globbuf.gl_pathv[ii], real_path)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong path name '%s'\n", globbuf.gl_pathv[ii]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong path name '%s'\n", globbuf.gl_pathv[ii]);
								result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								break;
							}
							lstat(real_path, &file_stat);
							if (S_ISREG(file_stat.st_mode) || S_ISLNK(file_stat.st_mode) || S_ISDIR(file_stat.st_mode))
								jj++;
						}
						if (ii < globbuf.gl_pathc) {
							globfree(&globbuf);
							break;
						}
						char filenames[jj][OPH_COMMON_BUFFER_LEN];
						for (ii = jj = 0; ii < globbuf.gl_pathc; ++ii) {
							if (!realpath(globbuf.gl_pathv[ii], real_path))
								break;
							lstat(real_path, &file_stat);
							if (S_ISREG(file_stat.st_mode) || S_ISLNK(file_stat.st_mode) || S_ISDIR(file_stat.st_mode)) {
#ifdef OPH_NETCDF
								if (((OPH_FS_operator_handle *) handle->operator_handle)->measure
								    && strcasecmp(((OPH_FS_operator_handle *) handle->operator_handle)->measure, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
									if (!S_ISREG(file_stat.st_mode))
										continue;
									if (_oph_check_nc_measure(real_path, ((OPH_FS_operator_handle *) handle->operator_handle)->measure))
										continue;
								}
#endif
								if (((OPH_FS_operator_handle *) handle->operator_handle)->realpath) {
									_real_path = real_path;
									if (strlen(abs_path) > 1)
										for (iii = 0; _real_path && *_real_path && (*_real_path == abs_path[iii]); iii++, _real_path++);
								} else
									_real_path = strrchr(real_path, '/') + 1;
								if (!_real_path || !*_real_path) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling real path of '%s'\n", globbuf.gl_pathv[ii]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in handling real path of '%s'\n", globbuf.gl_pathv[ii]);
									result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
									break;
								}
								snprintf(filenames[jj++], OPH_COMMON_BUFFER_LEN, "%s%s", _real_path, S_ISDIR(file_stat.st_mode) ? "/" : "");
							}
						}
						globfree(&globbuf);
						if (ii < globbuf.gl_pathc)
							break;

						result = write_json(filenames, jj, handle->operator_json, num_fields);

					} else {

						if (!realpath(path, real_path)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong path name '%s'\n", path);
							result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							break;
						}

						char *tbuffer = NULL;
						if (openDir(real_path, recursive, &jj, &tbuffer, file_is_valid ? ((OPH_FS_operator_handle *) handle->operator_handle)->file : NULL)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open '%s'.\n", path);
							result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							if (tbuffer)
								free(tbuffer);
							break;
						}

						if (jj && tbuffer) {

							unsigned int nn = strlen(abs_path) > 1, ni;
							char *filename = real_path;
							while ((filename = strchr(filename, '/'))) {
								nn++;
								filename++;
							}

							char filenames[jj][OPH_COMMON_BUFFER_LEN], *start = tbuffer, *save_pointer = NULL;
							int jjc = jj;
							for (ii = jj = 0; ii < jjc; ++ii, start = NULL) {
								filename = strtok_r(start, OPH_SEPARATOR_PARAM, &save_pointer);
								lstat(filename, &file_stat);
#ifdef OPH_NETCDF
								if (((OPH_FS_operator_handle *) handle->operator_handle)->measure
								    && strcasecmp(((OPH_FS_operator_handle *) handle->operator_handle)->measure, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
									if (!S_ISREG(file_stat.st_mode))
										continue;
									if (_oph_check_nc_measure(filename, ((OPH_FS_operator_handle *) handle->operator_handle)->measure))
										continue;
								}
#endif
								if (((OPH_FS_operator_handle *) handle->operator_handle)->realpath) {
									if (nn)
										for (iii = 0; filename && *filename && (*filename == abs_path[iii]); iii++, filename++);
								} else
									for (ni = 0; ni < nn; ++ni)
										filename = strchr(filename, '/') + 1;
								snprintf(filenames[jj++], OPH_COMMON_BUFFER_LEN, "%s%s", filename, S_ISDIR(file_stat.st_mode) ? "/" : "");
							}
							result = write_json(filenames, jj, handle->operator_json, num_fields);
						}
						if (tbuffer)
							free(tbuffer);
					}
#ifdef OPH_ESDM
				} else {	// ESDM Container

					int jj = 0;
					char filenames[1][OPH_COMMON_BUFFER_LEN];	// TODO: consider only one ESDM Container in this implementation

					esdm_status ret = esdm_init();
					if (ret) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ESDM cannot be initialized\n");
						result = OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
						break;
					}

					esdm_dataspace_t *point = NULL;
					if (((OPH_FS_operator_handle *) handle->operator_handle)->measure && ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims) {

						// Build the dataspace
						char *varname = ((OPH_FS_operator_handle *) handle->operator_handle)->measure, *curfilter;
						char const *const *dims_name;
						char **sub_dims = ((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims;
						char **sub_filters = ((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters;
						char **sub_types = ((OPH_FS_operator_handle *) handle->operator_handle)->sub_types;
						int number_of_sub_dims = ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims;
						int number_of_sub_types = ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_types;
						double *offset = ((OPH_FS_operator_handle *) handle->operator_handle)->offset;
						int s_offset_num = ((OPH_FS_operator_handle *) handle->operator_handle)->s_offset_num;
						esdm_container_t *container = NULL;
						esdm_dataset_t *dataset = NULL;
						esdm_dataspace_t *dspace = NULL;

						do {

							if (esdm_container_open(path, ESDM_MODE_FLAG_READ, &container))
								break;
							if (esdm_dataset_open(container, varname, ESDM_MODE_FLAG_READ, &dataset))
								break;
							if (esdm_dataset_get_dataspace(dataset, &dspace))
								break;
							if (esdm_dataset_get_name_dims(dataset, &dims_name))
								break;

							int i, j, ndims = dspace->dims, tf = -1;	// Id of time filter

							//Check dimension names
							for (i = 0; i < number_of_sub_dims; i++) {
								for (j = 0; j < ndims; j++)
									if (!strcmp(sub_dims[i], dims_name[j]))
										break;
								if (j == ndims) {
									pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to find dimension '%s' related to variable '%s'\n", sub_dims[i], varname);
									logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to find dimension '%s' related to variable '%s'\n",
										sub_dims[i], varname);
									break;
								}
							}

							//Check the sub_filters strings
							for (i = 0; i < number_of_sub_dims; i++) {
								if (((OPH_FS_operator_handle *) handle->operator_handle)->time_filter && strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR[1])) {
									if (tf >= 0) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Not more than one time dimension can be considered\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									tf = i;
									for (j = 0; j < ndims; j++)
										if (!strcmp(sub_dims[i], dims_name[j]))
											break;
								} else if (strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2) != strrchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided range are not supported\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
							}
							if (result)
								break;

							// Time dimension
							if (tf >= 0) {

								// TODO
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset of time dimension is not supported yet\n", sub_filters[tf]);
								logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
/*
								oph_odb_dimension dim, *time_dim = &dim;



								long long max_size = QUERY_BUFLEN;
								oph_pid_get_buffer_size(&max_size);
								char temp[max_size];
								if (oph_dim_parse_time_subset(sub_filters[tf], time_dim, temp)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values '%s'\n", sub_filters[tf]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
									result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
									break;
								}
								free(sub_filters[tf]);
								((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters[tf] = sub_filters[tf] = strdup(temp);
*/
							}

							char is_index[1 + number_of_sub_dims];
							if (number_of_sub_dims) {
								for (i = 0; i < number_of_sub_types; ++i)
									is_index[i] = strncmp(sub_types[i], OPH_FS_SUBSET_COORD, OPH_TP_TASKLEN);
								for (; i < number_of_sub_dims; ++i)
									is_index[i] = number_of_sub_types == 1 ? is_index[0] : 1;
							}
							is_index[number_of_sub_dims] = 0;

							size_t dims_length[ndims];
							int dims_start_index[ndims];
							int dims_end_index[ndims];
							ESDM_var measure;
							measure.container = container;
							measure.dataset = dataset;
							measure.dims_name = dims_name;
							measure.ndims = ndims;
							measure.dim_dataset = (esdm_dataset_t **) calloc(measure.ndims, sizeof(esdm_dataset_t *));	// TODO: to be freed
							measure.dim_dspace = (esdm_dataspace_t **) calloc(measure.ndims, sizeof(esdm_dataspace_t *));	// TODO: to be freed
							measure.dims_start_index = dims_start_index;
							measure.dims_end_index = dims_end_index;

							for (i = 0; i < ndims; i++) {
								curfilter = NULL;
								for (j = 0; j < number_of_sub_dims; j++) {
									if (!strcmp(sub_dims[j], dims_name[i])) {
										curfilter = sub_filters[j];
										break;
									}
								}
								if (!curfilter)
									break;
								if (oph_esdm_check_subset_string(curfilter, i, &measure, is_index[j], j < s_offset_num ? offset[j] : 0.0)) {
									result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
									break;
								} else if (dims_start_index[i] < 0 || dims_end_index[i] < 0 || dims_start_index[i] > dims_end_index[i]
									   || dims_start_index[i] >= (int) dims_length[i] || dims_end_index[i] >= (int) dims_length[i]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
									result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
									break;
								}
							}
							if (result)
								break;

							// TODO

						} while (0);

						if (dataset)
							esdm_dataset_close(dataset);
						if (container)
							esdm_container_close(container);
					}

					if (result)
						break;

					if (((OPH_FS_operator_handle *) handle->operator_handle)->measure
					    && strcasecmp(((OPH_FS_operator_handle *) handle->operator_handle)->measure, OPH_FRAMEWORK_FS_DEFAULT_PATH)) {
						if (point)
							result = esdm_dataset_probe_region(path + 7, ((OPH_FS_operator_handle *) handle->operator_handle)->measure, point);
						else
							result = esdm_dataset_probe(path + 7, ((OPH_FS_operator_handle *) handle->operator_handle)->measure);
					} else
						result = esdm_container_probe(path + 7);
					if (result)	// Skip protocol name
						snprintf(filenames[jj++], OPH_COMMON_BUFFER_LEN, "%s", path);

					esdm_finalize();

					result = write_json(filenames, jj, handle->operator_json, num_fields);
#endif
				}
			}

			break;

		case OPH_FS_MODE_CD:

			if (stat(path, &file_stat) || !S_ISDIR(file_stat.st_mode)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to access '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to access '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}
			// ADD OUTPUT TO JSON AS TEXT
			if (oph_json_is_objkey_printable
			    (((OPH_FS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_FS)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_FS, OPH_FS_CD_MESSAGE, rel_path)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}
			// ADD OUTPUT CWD TO NOTIFICATION STRING
			char tmp_string[OPH_COMMON_BUFFER_LEN];
			snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_CDD, rel_path);
			if (handle->output_string) {
				strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
				free(handle->output_string);
			}
			handle->output_string = strdup(tmp_string);

			break;

		case OPH_FS_MODE_MD:

			if (!is_valid) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing target directory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Missing target directory\n");
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}
			if (!stat(path, &file_stat)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to overwrite '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to overwrite '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			} else if (mkdir(path, 0755)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to create '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			break;

		case OPH_FS_MODE_RM:

			if (!is_valid) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing target directory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Missing target directory\n");
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}
			if (stat(path, &file_stat) || (!S_ISDIR(file_stat.st_mode) && !S_ISREG(file_stat.st_mode))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to access '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to access '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			} else if (remove(path)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to remove '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to remove '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			break;

		case OPH_FS_MODE_MV:

			if (!is_valid) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing target directory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Missing target directory\n");
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}

			if (stat(path, &file_stat) || (!S_ISDIR(file_stat.st_mode) && !S_ISREG(file_stat.st_mode))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to access '%s'\n", rel_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to access '%s'\n", rel_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}

			if (((OPH_FS_operator_handle *) handle->operator_handle)->path_num < 2) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing destination directory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Missing destination directory\n");
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}
			if (oph_odb_fs_path_parsing(((OPH_FS_operator_handle *) handle->operator_handle)->path[1], ((OPH_FS_operator_handle *) handle->operator_handle)->cwd, NULL, &dest_path, NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_PATH_PARSING_ERROR);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				break;
			}
			len = strlen(dest_path);
			if (len > 1)
				dest_path[len - 1] = 0;
			char out_path[OPH_COMMON_BUFFER_LEN];
			snprintf(out_path, OPH_COMMON_BUFFER_LEN, "%s%s", abs_path, dest_path);

			struct stat out_stat;
			if (!stat(out_path, &out_stat)) {
				if (S_ISDIR(out_stat.st_mode))
					snprintf(out_path + strlen(out_path), OPH_COMMON_BUFFER_LEN - strlen(out_path), "%s", strrchr(path, '/'));
				else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to overwrite '%s'\n", dest_path);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to overwrite '%s'\n", dest_path);
					result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					break;
				}
			}

			if (rename(path, out_path)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to move '%s' to '%s'\n", rel_path, dest_path);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to move '%s' to '%s'\n", rel_path, dest_path);
				result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			break;

		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid input parameter %s\n", OPH_IN_PARAM_SYSTEM_COMMAND);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_INVALID_INPUT_PARAMETER, OPH_IN_PARAM_SYSTEM_COMMAND);
			result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (abs_path) {
		free(abs_path);
		abs_path = NULL;
	}
	if (rel_path) {
		free(rel_path);
		rel_path = NULL;
	}
	if (dest_path) {
		free(dest_path);
		dest_path = NULL;
	}

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_FS_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_FS_operator_handle *) handle->operator_handle)->path) {
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->path, ((OPH_FS_operator_handle *) handle->operator_handle)->path_num);
		((OPH_FS_operator_handle *) handle->operator_handle)->path = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->file) {
		free((char *) ((OPH_FS_operator_handle *) handle->operator_handle)->file);
		((OPH_FS_operator_handle *) handle->operator_handle)->file = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->measure) {
		free((char *) ((OPH_FS_operator_handle *) handle->operator_handle)->measure);
		((OPH_FS_operator_handle *) handle->operator_handle)->measure = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_FS_operator_handle *) handle->operator_handle)->cwd);
		((OPH_FS_operator_handle *) handle->operator_handle)->cwd = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_FS_operator_handle *) handle->operator_handle)->user);
		((OPH_FS_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->objkeys, ((OPH_FS_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_FS_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->sub_types) {
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->sub_types, ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_types);
		((OPH_FS_operator_handle *) handle->operator_handle)->sub_types = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims) {
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims, ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_dims);
		((OPH_FS_operator_handle *) handle->operator_handle)->sub_dims = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters) {
		oph_tp_free_multiple_value_param_list(((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters, ((OPH_FS_operator_handle *) handle->operator_handle)->number_of_sub_filters);
		((OPH_FS_operator_handle *) handle->operator_handle)->sub_filters = NULL;
	}
	if (((OPH_FS_operator_handle *) handle->operator_handle)->offset) {
		free(((OPH_FS_operator_handle *) handle->operator_handle)->offset);
		((OPH_FS_operator_handle *) handle->operator_handle)->offset = NULL;
	}

	free((OPH_FS_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

#ifdef OPH_ESDM
	handle->dlh = NULL;
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
