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

#ifndef __OPHIDIAIO_IOSERVER_H
#define __OPHIDIAIO_IOSERVER_H

#include "oph_ioserver_library.h"

#define OPHIDIAIO_IO_ERROR -1
#define OPHIDIAIO_IO_SUCCESS 0
#define OPHIDIAIO_IO_NULL_PARAM -2
#define OPHIDIAIO_IO_MEMORY_ERROR -3

/**
 * \brief               Function to initialize data store server library.
 * \param handle        Address to pointer for dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_setup(oph_ioserver_handler * handle);

/**
 * \brief               Function to connect or reconnect to data store server.
 * \param handle        Dynamic server plugin handle
 * \param conn_params   Struct with connection params to server
 * \param connection    Adress of pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_connect(oph_ioserver_handler * handle, oph_ioserver_params * conn_params, void **connection);

/**
 * \brief               Function to set default database for specified server.
 * \param handle        Dynamic server plugin handle
 * \param db_name       Name of database to be used
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_use_db(oph_ioserver_handler * handle, const char *db_name, void *connection);

/**
 * \brief               Function to execute an operation on data stored into server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_execute_query(oph_ioserver_handler * handle, void *connection, oph_ioserver_query * query);

/**
 * \brief               Function to setup the query structure with given operation and array argument
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param operation     String with operation to be performed
 * \param tot_run       Total number of runs the given operation will be executed
 * \param args          Array of arguments to be binded to the query (can be NULL)
 * \param query         Pointer to query to be built
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_setup_query(oph_ioserver_handler * handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg ** args, oph_ioserver_query ** query);

/**
 * \brief               Function to release resources allocated for query
 * \param handle        Dynamic server plugin handle
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_free_query(oph_ioserver_handler * handle, oph_ioserver_query * query);

/**
 * \brief               Function to close connection established towards data store server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_close(oph_ioserver_handler * handle, void **connection);

/**
 * \brief               Function to finalize library of data store server and release all dynamic loading resources.
 * \param handle        Dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_cleanup(oph_ioserver_handler * handle);

/**
 * \brief               Function to get result set after executing a query.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param result        Pointer to the result set structure to be filled
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_get_result(oph_ioserver_handler * handle, void *connection, oph_ioserver_result ** result);

/**
 * \brief               Function to fetch the next row in a result set.
 * \param handle        Dynamic server plugin handle
 * \param result        Pointer to the result set structure to scan
 * \param current_row	Pointer to the next row structure in the result set
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_fetch_row(oph_ioserver_handler * handle, oph_ioserver_result * result, oph_ioserver_row ** current_row);

/**
 * \brief               Function to free the allocated result set.
 * \param handle        Dynamic server plugin handle
 * \param result        Pointer to the result set structure to free
 * \return              0 if successfull, non-0 otherwise
 */
int _ophidiaio_free_result(oph_ioserver_handler * handle, oph_ioserver_result * result);

#endif				//__OPHIDIAIO_IOSERVER_H
