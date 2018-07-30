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

#include "oph_ioserver_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <errno.h>

#include "debug.h"
#include "oph_ioserver_log_error_codes.h"

#include <pthread.h>

pthread_mutex_t libtool_lock = PTHREAD_MUTEX_INITIALIZER;
extern int msglevel;

static int oph_find_server_plugin(const char *server_type, char **dyn_lib);

//Parse IO server name SERVER_STORAGE
static int oph_parse_server_name(const char *server_name, char **server_type, char **server_subtype)
{
	if (!server_name || !server_type || !server_subtype) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_COMMON_LOG, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	const char *ptr_begin, *ptr_end;

	ptr_begin = server_name;
	ptr_end = strchr(server_name, OPH_IOSERVER_SEPARATOR);
	int ioserver_len = 0, IOSERVER_len = 0;
	if (!ptr_end) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_INVALID_SERVER);
		logging_server(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_COMMON_LOG, OPH_IOSERVER_LOG_INVALID_SERVER);
		return OPH_IOSERVER_INVALID_PARAM;
	}

	IOSERVER_len = strlen(ptr_end + 1);
	ioserver_len = (strlen(ptr_begin) - IOSERVER_len - 1);

	if (IOSERVER_len == 0 || ioserver_len == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_INVALID_SERVER);
		logging_server(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_COMMON_LOG, OPH_IOSERVER_LOG_INVALID_SERVER);
		return OPH_IOSERVER_INVALID_PARAM;
	}

	*server_type = malloc((ioserver_len + 1) * sizeof(char));
	*server_subtype = malloc((IOSERVER_len + 1) * sizeof(char));

	strncpy(*server_type, ptr_begin, ioserver_len);
	(*server_type)[ioserver_len] = 0;

	strncpy(*server_subtype, ptr_end + 1, IOSERVER_len);
	(*server_subtype)[IOSERVER_len] = 0;

	return OPH_IOSERVER_SUCCESS;
}

int oph_ioserver_setup(const char *server_type, oph_ioserver_handler ** handle, char is_thread)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_COMMON_LOG, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	oph_ioserver_handler *internal_handle = (oph_ioserver_handler *) malloc(sizeof(oph_ioserver_handler));

	internal_handle->server_type = NULL;
	internal_handle->server_subtype = NULL;
	internal_handle->lib = NULL;
	internal_handle->dlh = NULL;
	internal_handle->is_thread = 0;

	if (is_thread != 0)
		internal_handle->is_thread = 1;

	//Set storage server type 
	if (oph_parse_server_name(server_type, &internal_handle->server_type, &internal_handle->server_subtype)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_SERVER_PARSE_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_COMMON_LOG, OPH_IOSERVER_LOG_SERVER_PARSE_ERROR);
		return OPH_IOSERVER_INVALID_PARAM;
	}
	//If already executed don't procede further
	if (internal_handle->dlh)
		return OPH_IOSERVER_SUCCESS;

	//LTDL_SET_PRELOADED_SYMBOLS();
	pthread_mutex_lock(&libtool_lock);
	if (lt_dlinit() != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_DLINIT_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, internal_handle->server_type, OPH_IOSERVER_LOG_DLINIT_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLOPEN_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	if (oph_find_server_plugin(internal_handle->server_type, &internal_handle->lib)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LIB_NOT_FOUND);
		logging_server(LOG_ERROR, __FILE__, __LINE__, internal_handle->server_type, OPH_IOSERVER_LOG_LIB_NOT_FOUND);
		return OPH_IOSERVER_LIB_NOT_FOUND;
	}

	pthread_mutex_lock(&libtool_lock);
	if (!(internal_handle->dlh = (lt_dlhandle) lt_dlopen(internal_handle->lib))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_DLOPEN_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, internal_handle->server_type, OPH_IOSERVER_LOG_DLOPEN_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLOPEN_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_SETUP_FUNC, internal_handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_setup = (int (*)(oph_ioserver_handler *)) lt_dlsym(internal_handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, internal_handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	*handle = internal_handle;
	return _SERVER_setup(*handle);
}

int oph_ioserver_connect(oph_ioserver_handler * handle, oph_ioserver_params * conn_params, void **connection)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_CONNECT_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_connect = (int (*)(oph_ioserver_handler *, oph_ioserver_params *, void **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_connect(handle, conn_params, connection);
}

int oph_ioserver_use_db(oph_ioserver_handler * handle, const char *db_name, void *connection)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_USE_DB_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_use_db = (int (*)(oph_ioserver_handler *, const char *, void *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_use_db(handle, db_name, connection);
}

int oph_ioserver_setup_query(oph_ioserver_handler * handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg ** args, oph_ioserver_query ** query)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_SETUP_QUERY_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_setup_query = (int (*)(oph_ioserver_handler *, void *, const char *, unsigned long long, oph_ioserver_query_arg **, oph_ioserver_query **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_setup_query(handle, connection, operation, tot_run, args, query);
}

int oph_ioserver_execute_query(oph_ioserver_handler * handle, void *connection, oph_ioserver_query * query)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_EXECUTE_QUERY_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_execute_query = (int (*)(oph_ioserver_handler *, void *, oph_ioserver_query *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_execute_query(handle, connection, query);
}

int oph_ioserver_free_query(oph_ioserver_handler * handle, oph_ioserver_query * query)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_FREE_QUERY_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_free_query = (int (*)(oph_ioserver_handler *, oph_ioserver_query *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_free_query(handle, query);
}

int oph_ioserver_close(oph_ioserver_handler * handle, void *connection)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_CLOSE_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_close = (int (*)(oph_ioserver_handler *, void *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_close(handle, connection);
}

int oph_ioserver_cleanup(oph_ioserver_handler * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_CLEANUP_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_cleanup = (int (*)(oph_ioserver_handler *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	//Release operator resources
	int res;
	if ((res = _SERVER_cleanup(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_RELEASE_RES_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_RELEASE_RES_ERROR);
		return res;
	}
	//Release handle resources
	if (handle->server_type) {
		free(handle->server_type);
		handle->server_type = NULL;
	}
	if (handle->server_subtype) {
		free(handle->server_subtype);
		handle->server_subtype = NULL;
	}
	if (handle->lib) {
		free(handle->lib);
		handle->lib = NULL;
	}

	pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(handle->dlh))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_DLCLOSE_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_DLCLOSE_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLCLOSE_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);
	handle->dlh = NULL;

	pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_DLEXIT_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_DLEXIT_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLEXIT_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	free(handle);
	handle = NULL;

	return res;
}

int oph_ioserver_get_result(oph_ioserver_handler * handle, void *connection, oph_ioserver_result ** result)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_GET_RESULT_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_get_result = (int (*)(oph_ioserver_handler *, void *, oph_ioserver_result **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_get_result(handle, connection, result);
}

int oph_ioserver_fetch_row(oph_ioserver_handler * handle, oph_ioserver_result * result, oph_ioserver_row ** current_row)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_FETCH_ROW_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_fetch_row = (int (*)(oph_ioserver_handler *, oph_ioserver_result *, oph_ioserver_row **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_fetch_row(handle, result, current_row);
}

int oph_ioserver_free_result(oph_ioserver_handler * handle, oph_ioserver_result * result)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_HANDLE);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_NULL_HANDLE);
		return OPH_IOSERVER_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->server_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_SERV_ERROR);
		return OPH_IOSERVER_DLOPEN_ERR;
	}

	char func_name[OPH_IOSERVER_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSERVER_BUFLEN, OPH_IOSERVER_FREE_RESULT_FUNC, handle->server_type);

	pthread_mutex_lock(&libtool_lock);
	if (!(_SERVER_free_result = (int (*)(oph_ioserver_handler *, oph_ioserver_result *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSERVER_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _SERVER_free_result(handle, result);
}

static int oph_find_server_plugin(const char *server_type, char **dyn_lib)
{
	FILE *fp = NULL;
	char line[OPH_IOSERVER_BUFLEN] = { '\0' };
	char value[OPH_IOSERVER_BUFLEN] = { '\0' };
	char dyn_lib_str[OPH_IOSERVER_BUFLEN] = { '\0' };
	char *res_string = NULL;

	if (!server_type || !dyn_lib) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_NULL_INPUT_PARAM);
		return -1;
	}

	snprintf(dyn_lib_str, sizeof(dyn_lib_str), OPH_IOSERVER_FILE_PATH, OPH_ANALYTICS_LOCATION);

	fp = fopen(dyn_lib_str, "r");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_FILE_NOT_FOUND, dyn_lib_str);
		logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_FILE_NOT_FOUND, dyn_lib_str);
		return -1;	// driver not found
	}

	while (!feof(fp)) {
		res_string = fgets(line, OPH_IOSERVER_BUFLEN, fp);

		if (!res_string) {
			fclose(fp);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_READ_LINE_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_READ_LINE_ERROR);
			return -1;
		}
		sscanf(line, "[%[^]]", value);

		if (!strcasecmp(value, server_type)) {
			res_string = NULL;
			res_string = fgets(line, OPH_IOSERVER_BUFLEN, fp);
			if (!res_string) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_READ_LINE_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_READ_LINE_ERROR);
				return -1;
			}
			sscanf(line, "%[^\n]", value);
			*dyn_lib = (char *) strndup(value, OPH_IOSERVER_BUFLEN);
			if (!*dyn_lib) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_MEMORY_ERROR);
				logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_MEMORY_ERROR);
				return -2;
			}
			fclose(fp);
			return 0;
		}
	}
	fclose(fp);
	pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_FILE_NOT_FOUND, dyn_lib_str);
	logging_server(LOG_ERROR, __FILE__, __LINE__, server_type, OPH_IOSERVER_LOG_FILE_NOT_FOUND, dyn_lib_str);

	return -2;		// driver not found
}
