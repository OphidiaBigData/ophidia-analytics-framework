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

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "drivers/OPH_B2DROP_operator.h"

#include <errno.h>
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_framework_paths.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"

#include <curl/curl.h>

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

	if (!(handle->operator_handle = (OPH_B2DROP_operator_handle *) calloc(1, sizeof(OPH_B2DROP_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path = NULL;
	((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path = NULL;
	((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path = NULL;
	((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num = -1;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys, &((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (strstr(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strstr(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, "http://")
	    && !strstr(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, "https://")) {
		char *pointer = ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			char tmp[OPH_COMMON_BUFFER_LEN];
			if (*pointer != '/') {
				value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (*value != '/') {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", value);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", value);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (strlen(value) > 1) {
					if (strstr(value, "..")) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", value + 1, pointer);
					free(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
					((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path = strdup(tmp);
					pointer = ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path;
				}
			}
			if (oph_pid_get_base_src_path(&value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
			free(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
			((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path = strdup(tmp);
			free(value);
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path is not correct\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Path is not correct\n");
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of 'http://' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of 'http://' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	//Build B2DROP full URL
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DST_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DST_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_DST_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strstr(value, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strstr(value, "http://") || strstr(value, "https://")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of 'http://' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of 'http://' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (!strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		char *pointer = strrchr(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, '/');
		while (pointer && !strlen(pointer)) {
			*pointer = 0;
			pointer = strrchr(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, '/');
		}
		value = pointer ? pointer + 1 : ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path;
	}
	char *webdav_url = NULL;
	if (oph_pid_get_b2drop_webdav_url(&webdav_url)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read B2DROP webdav URL\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read B2DROP webdav URL\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	char tmp[OPH_TP_TASKLEN];
	snprintf(tmp, OPH_TP_TASKLEN, "%s%s%s", webdav_url ? webdav_url : "", *value != '/' ? "/" : "", value);
	free(webdav_url);

	if (!(((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path = (char *) strndup(tmp, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MEMORY_ERROR_INPUT, OPH_IN_PARAM_DST_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_AUTH_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_AUTH_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_AUTH_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strstr(value, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strstr(value, "http://") || strstr(value, "https://")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of 'http://' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of 'http://' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		char *base_src_path = NULL;
		if (oph_pid_get_base_src_path(&base_src_path)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", base_src_path ? base_src_path : "", *value != '/' ? "/" : "", value);
		free(base_src_path);

		if (!(((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path = (char *) strndup(tmp, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_MEMORY_ERROR_INPUT, OPH_IN_PARAM_AUTH_FILE_PATH);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	typedef struct {
		char *buffer;
		size_t len;
	} curlbuf;

	size_t _write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
		size_t realsize = size * nmemb;
		curlbuf *mem = (curlbuf *) userdata;
		mem->buffer = realloc(mem->buffer, mem->len + realsize + 1);
		if (!mem->buffer)
			return 0;
		memcpy(&(mem->buffer[mem->len]), ptr, realsize);
		mem->len += realsize;
		mem->buffer[mem->len] = 0;
		return realsize;
	}

	FILE *in_file;
	struct stat in_file_info;

	//Open input file
	in_file = fopen(((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path, "rb");
	if (!in_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_B2DROP_FILE_OPEN_ERROR, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_FILE_OPEN_ERROR, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	//Read file size
	if (fstat(fileno(in_file), &in_file_info)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_B2DROP_STAT_FILE_ERROR, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_STAT_FILE_ERROR, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
		fclose(in_file);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	curlbuf res_buf;
	res_buf.buffer = malloc(1);	/* will be grown as needed by the realloc */
	res_buf.len = 0;	/* no data at this point */

	curl_off_t in_file_size = (curl_off_t) in_file_info.st_size;

	// Send the request 
	CURLcode ret;
	CURL *hnd = curl_easy_init();

	//curl_easy_setopt(hnd, CURLOPT_VERBOSE, 1L);
	curl_easy_setopt(hnd, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(hnd, CURLOPT_INFILESIZE_LARGE, in_file_size);
	curl_easy_setopt(hnd, CURLOPT_READDATA, in_file);
	curl_easy_setopt(hnd, CURLOPT_URL, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path);
	curl_easy_setopt(hnd, CURLOPT_NETRC, CURL_NETRC_REQUIRED);
	if (((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path)
		curl_easy_setopt(hnd, CURLOPT_NETRC_FILE, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path);
	curl_easy_setopt(hnd, CURLOPT_USERPWD, NULL);

	// Additional security curl options
	curl_easy_setopt(hnd, CURLOPT_SSL_VERIFYPEER, 1L);
	curl_easy_setopt(hnd, CURLOPT_SSL_VERIFYHOST, 2L);
	curl_easy_setopt(hnd, CURLOPT_PROXY, NULL);
	curl_easy_setopt(hnd, CURLOPT_FAILONERROR, 0L);
	curl_easy_setopt(hnd, CURLOPT_FOLLOWLOCATION, 0L);
	curl_easy_setopt(hnd, CURLOPT_MAXREDIRS, 0L);
	curl_easy_setopt(hnd, CURLOPT_UNRESTRICTED_AUTH, 0L);
	curl_easy_setopt(hnd, CURLOPT_SSH_PRIVATE_KEYFILE, NULL);
	curl_easy_setopt(hnd, CURLOPT_SSH_PUBLIC_KEYFILE, NULL);

	// Additional execution options 
	curl_easy_setopt(hnd, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(hnd, CURLOPT_TIMEOUT, 0L);
	curl_easy_setopt(hnd, CURLOPT_CONNECTTIMEOUT, 0L);
	//curl_easy_setopt(hnd, CURLOPT_LOW_SPEED_LIMIT, 0L);
	//curl_easy_setopt(hnd, CURLOPT_LOW_SPEED_TIME, 0L);
	//curl_easy_setopt(hnd, CURLOPT_MAX_SEND_SPEED_LARGE, (curl_off_t) 0);
	//curl_easy_setopt(hnd, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t) 0);
	//curl_easy_setopt(hnd, CURLOPT_RESUME_FROM_LARGE, (curl_off_t) 0);
	//curl_easy_setopt(hnd, CURLOPT_FTP_CREATE_MISSING_DIRS, 0);

	curl_easy_setopt(hnd, CURLOPT_WRITEDATA, &res_buf);
	curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, _write_callback);

	ret = curl_easy_perform(hnd);
	if (ret != CURLE_OK) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_B2DROP_UPLOAD_FILE_ERROR, curl_easy_strerror(ret));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_UPLOAD_FILE_ERROR, curl_easy_strerror(ret));
		curl_easy_cleanup(hnd);
		fclose(in_file);
		if (res_buf.buffer)
			free(res_buf.buffer);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Check info from call
	double upload_speed, total_time, upload_size;
	char speed_unit = ' ', size_unit = ' ';
	long http_code = 0;

	curl_easy_getinfo(hnd, CURLINFO_SPEED_UPLOAD, &upload_speed);
	curl_easy_getinfo(hnd, CURLINFO_TOTAL_TIME, &total_time);
	curl_easy_getinfo(hnd, CURLINFO_RESPONSE_CODE, &http_code);
	curl_easy_getinfo(hnd, CURLINFO_SIZE_UPLOAD, &upload_size);

	curl_easy_cleanup(hnd);
	fclose(in_file);

	//Exit with error
	if (http_code >= 300) {
		char *p1 = NULL, *p2 = NULL;
		if ((res_buf.buffer) && (p1 = strstr(res_buf.buffer, "<s:message>")) && (p2 = strstr(res_buf.buffer, "</s:message>"))) {
			p1 += strlen("<s:message>");
			*p2 = '\0';
		}
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_B2DROP_HTTP_ERROR, http_code, p1 ? p1 : "Generic Error");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_HTTP_ERROR, http_code, p1 ? p1 : "Generic Error");

		if (oph_json_is_objkey_printable
		    (((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_B2DROP)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_B2DROP, "Error", p1 ? p1 : "Generic Error")) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				if (res_buf.buffer)
					free(res_buf.buffer);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

		if (res_buf.buffer)
			free(res_buf.buffer);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (res_buf.buffer)
		free(res_buf.buffer);

	//Format size and speed
	if (upload_size > 5000) {
		upload_size /= 1024;
		size_unit = 'K';
		if (upload_size > 5000) {
			upload_size /= 1024;
			size_unit = 'M';
		}
	}
	if (upload_speed > 5000) {
		upload_speed /= 1024;
		speed_unit = 'K';
		if (upload_speed > 5000) {
			upload_speed /= 1024;
			speed_unit = 'M';
		}
	}
	//Exit with success
	char message[OPH_COMMON_BUFFER_LEN];
	snprintf(message, OPH_COMMON_BUFFER_LEN, "%.0f %cBytes have been uploaded at %.0f %cB/s in %.3f seconds, with HTTP code %ld\n", upload_size, size_unit, upload_speed, speed_unit, total_time,
		 http_code);

	printf(message);

	if (oph_json_is_objkey_printable
	    (((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_B2DROP)) {
		if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_B2DROP, "Success", message)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_B2DROP_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path) {
		free((char *) ((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path);
		((OPH_B2DROP_operator_handle *) handle->operator_handle)->src_file_path = NULL;
	}
	if (((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path) {
		free((char *) ((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path);
		((OPH_B2DROP_operator_handle *) handle->operator_handle)->dst_file_path = NULL;
	}
	if (((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path) {
		free((char *) ((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path);
		((OPH_B2DROP_operator_handle *) handle->operator_handle)->auth_file_path = NULL;
	}
	if (((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys, ((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_B2DROP_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_B2DROP_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
