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

#define _GNU_SOURCE
#include "OPHIDIA_ioserver.h"
#include "oph_io_client_interface.h"

#include <unistd.h>
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "oph_ioserver_plugins_log_error_codes.h"

#define UNUSED(x) {(void)(x);}

//Initialize storage server plugin
int _ophidiaio_setup(oph_ioserver_handler * handle)
{
	UNUSED(handle);
	return (oph_io_client_setup() == OPH_IO_CLIENT_INTERFACE_OK) ? OPHIDIAIO_IO_SUCCESS : OPHIDIAIO_IO_ERROR;
}

//Connect or reconnect to storage server
int _ophidiaio_connect(oph_ioserver_handler * handle, oph_ioserver_params * conn_params, void **connection)
{
	if (!connection || !conn_params) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	char port[16];
	sprintf(port, "%d", conn_params->port);
	if (oph_io_client_connect(conn_params->host, port, conn_params->db_name, handle->server_subtype, (oph_io_client_connection **) connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_CONN_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_CONN_ERROR);
		return OPHIDIAIO_IO_ERROR;
	}
	return OPHIDIAIO_IO_SUCCESS;
}

int _ophidiaio_use_db(oph_ioserver_handler * handle, const char *db_name, void *connection)
{
	if (!connection || !db_name || !handle->server_subtype) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (oph_io_client_use_db(db_name, handle->server_subtype, (oph_io_client_connection *) connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_USE_DB_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_USE_DB_ERROR);
		return OPHIDIAIO_IO_ERROR;
	}
	return OPHIDIAIO_IO_SUCCESS;
}

//Execute operation in storage server
int _ophidiaio_execute_query(oph_ioserver_handler * handle, void *connection, oph_ioserver_query * query)
{
	if (!connection || !query || !query->statement) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (oph_io_client_execute_query((oph_io_client_connection *) connection, (oph_io_client_query *) (query->statement))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_EXEC_QUERY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_EXEC_QUERY_ERROR);
		return OPHIDIAIO_IO_ERROR;
	}
	return OPHIDIAIO_IO_SUCCESS;
}

//Setup the query structure with given operation and array argument
int _ophidiaio_setup_query(oph_ioserver_handler * handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg ** args, oph_ioserver_query ** query)
{
	if (!connection || !operation || !query || !handle->server_subtype) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	*query = (oph_ioserver_query *) malloc(1 * sizeof(oph_ioserver_query));
	if (!*query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_MEMORY_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_MEMORY_ERROR);
		return OPHIDIAIO_IO_ERROR;
	}

	if (!args) {
		(*query)->type = OPH_IOSERVER_STMT_SIMPLE;
		if (oph_io_client_setup_query((oph_io_client_connection *) connection, operation, handle->server_subtype, tot_run, NULL, (oph_io_client_query **) (&((*query)->statement)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_SETUP_QUERY_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_SETUP_QUERY_ERROR);
			free(*query);
			*query = NULL;
			return OPHIDIAIO_IO_ERROR;
		}
	} else {
		(*query)->type = OPH_IOSERVER_STMT_BINARY;
		oph_io_client_query_arg **ophidiaio_query_arg = (oph_io_client_query_arg **) args;
		if (oph_io_client_setup_query
		    ((oph_io_client_connection *) connection, operation, handle->server_subtype, tot_run, ophidiaio_query_arg, (oph_io_client_query **) (&((*query)->statement)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_SETUP_QUERY_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_SETUP_QUERY_ERROR);
			free(*query);
			*query = NULL;
			return OPHIDIAIO_IO_ERROR;
		}
	}

	return OPHIDIAIO_IO_SUCCESS;
}

//Release resources allocated for query
int _ophidiaio_free_query(oph_ioserver_handler * handle, oph_ioserver_query * query)
{
	if (!query || !query->statement) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (oph_io_client_free_query((oph_io_client_query *) (query->statement))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	free(query);
	return OPHIDIAIO_IO_SUCCESS;
}

//Close connection to storage server
int _ophidiaio_close(oph_ioserver_handler * handle, void **connection)
{
	if (!(*connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (*connection) {
		if (oph_io_client_close((oph_io_client_connection *) (*connection))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
			return OPHIDIAIO_IO_NULL_PARAM;
		}
	}
	*connection = NULL;

	return OPHIDIAIO_IO_SUCCESS;
}

//Finalize storage server plugin
int _ophidiaio_cleanup(oph_ioserver_handler * handle)
{
	UNUSED(handle);
	return (oph_io_client_cleanup() == OPH_IO_CLIENT_INTERFACE_OK) ? OPHIDIAIO_IO_SUCCESS : OPHIDIAIO_IO_ERROR;
}

//Get the result set
int _ophidiaio_get_result(oph_ioserver_handler * handle, void *connection, oph_ioserver_result ** result)
{
	if (!connection || !result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (*result == NULL) {
		*result = (oph_ioserver_result *) malloc(sizeof(oph_ioserver_result));
		if (oph_io_client_get_result((oph_io_client_connection *) connection, (oph_io_client_result **) (&((*result)->result_set)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_GET_RESULT_ERROR);
			logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_GET_RESULT_ERROR);
			_ophidiaio_free_result(handle, *result);
			return OPHIDIAIO_IO_MEMORY_ERROR;
		}
		//Copy the values in the oph_ioserver_result struct
		(*result)->num_rows = ((oph_io_client_result *) (*result)->result_set)->num_rows;
		(*result)->num_fields = ((oph_io_client_result *) (*result)->result_set)->num_fields;
		(*result)->max_field_length = ((oph_io_client_result *) (*result)->result_set)->max_field_length;
		(*result)->current_row = (oph_ioserver_row *) malloc(sizeof(oph_ioserver_row));
		(*result)->current_row->field_lengths = NULL;
		(*result)->current_row->row = NULL;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NOT_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NOT_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_ERROR;
	}

	return OPHIDIAIO_IO_SUCCESS;
}

//Get the next row
int _ophidiaio_fetch_row(oph_ioserver_handler * handle, oph_ioserver_result * result, oph_ioserver_row ** current_row)
{
	if (!result || !current_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		_ophidiaio_free_result(handle, result);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	oph_io_client_record *tmp_current_row = NULL;
	if (oph_io_client_fetch_row((oph_io_client_result *) result->result_set, &tmp_current_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_FETCH_ROW_ERROR);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_FETCH_ROW_ERROR);
		_ophidiaio_free_result(handle, result);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (tmp_current_row != NULL) {
		result->current_row->row = tmp_current_row->field;
		result->current_row->field_lengths = tmp_current_row->field_length;
	} else {
		result->current_row->row = NULL;
		result->current_row->field_lengths = NULL;
	}

	*current_row = result->current_row;

	return OPHIDIAIO_IO_SUCCESS;
}

//Release result set resources
int _ophidiaio_free_result(oph_ioserver_handler * handle, oph_ioserver_result * result)
{
	UNUSED(handle);
	if (!result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	if (oph_io_client_free_result((oph_io_client_result *) result->result_set)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		logging_server(LOG_ERROR, __FILE__, __LINE__, handle->server_type, OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM);
		return OPHIDIAIO_IO_NULL_PARAM;
	}

	result->max_field_length = NULL;
	if (result->current_row) {
		free(result->current_row);
		result->current_row = NULL;
	}
	free(result);
	result = NULL;

	return OPHIDIAIO_IO_SUCCESS;
}
