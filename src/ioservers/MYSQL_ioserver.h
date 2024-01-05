/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

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

#ifndef __MYSQL_IOSERVER_H
#define __MYSQL_IOSERVER_H

/* MySQL headers */
#include <mysql.h>
#if MYSQL_VERSION_ID >= 80001 && MYSQL_VERSION_ID != 80002
typedef bool my_bool;
#endif

#include "oph_ioserver_library.h"

#define MYSQL_IO_ERROR -1
#define MYSQL_IO_SUCCESS 0
#define MYSQL_IO_NULL_PARAM -2
#define MYSQL_IO_MEMORY_ERROR -3

//Defines for queries

#define MYSQL_IO_QUERY_CREATE_FRAG_SELECT "CREATE TABLE %s (id_dim integer, measure longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT"
#define MYSQL_IO_QUERY_INSERT_SELECT 	  "INSERT INTO %s (id_dim, measure) SELECT"
#define MYSQL_IO_QUERY_SELECT             "SELECT"
#define MYSQL_IO_QUERY_INSERT             "INSERT INTO %s"
#define MYSQL_IO_QUERY_CREATE_FRAG        "CREATE TABLE %s (id_dim integer, measure longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1"
#define MYSQL_IO_QUERY_FUNC               "CALL %s ("
#define MYSQL_IO_QUERY_DROP_FRAG          "DROP TABLE IF EXISTS %s"
#define MYSQL_IO_QUERY_CREATE_DB          "CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci"
#define MYSQL_IO_QUERY_DROP_DB            "DROP DATABASE IF EXISTS %s"

#define MYSQL_IO_KW_TABLE_SIZE "oph_convert_l('OPH_LONG','',oph_aggregate_operator('OPH_LONG','OPH_LONG',oph_value_to_bin('','OPH_LONG',index_length+data_length),'OPH_SUM'))"
#define MYSQL_IO_KW_INFO_SYSTEM "information_schema.TABLES"
#define MYSQL_IO_KW_INFO_SYSTEM_TABLE "table_name"
#define MYSQL_IO_KW_FUNCTION_FIELDS "name,ret,dl,type"
#define MYSQL_IO_KW_FUNCTION_TABLE "mysql.func"

#define MYSQL_IO_QUERY_VAR_CHAR  '@'
#define MYSQL_IO_QUERY_SEPAR  ","
#define MYSQL_IO_QUERY_LIMIT  " LIMIT %s"
#define MYSQL_IO_QUERY_WHERE  " WHERE %s"
#define MYSQL_IO_QUERY_FROM   " FROM %s"
#define MYSQL_IO_QUERY_AS     " AS %s"
#define MYSQL_IO_QUERY_ORDER  " ORDER BY %s"
#define MYSQL_IO_QUERY_GROUP  " GROUP BY %s"
#define MYSQL_IO_QUERY_VALUES " VALUES ("
#define MYSQL_IO_QUERY_VALUES_MULTI " VALUES "
#define MYSQL_IO_QUERY_CLOSE_BRACKET  " )"
#define MYSQL_IO_QUERY_OPEN_BRACKET   " ("
/**
 * \brief        Struct to contain reference to prepared statement structures
 * \param stmt   Pointer to mysql statement struct
 * \param bind   Pointer to mysql bind struct
 */
typedef struct {
	MYSQL_STMT *stmt;
	MYSQL_BIND *bind;
} _mysql_query_struct;


/**
 * \brief               Function to initialize data store server library.
 * \param handle        Address to pointer for dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_setup(oph_ioserver_handler * handle);

/**
 * \brief               Function to connect or reconnect to data store server.
 * \param handle        Dynamic server plugin handle
 * \param conn_params   Struct with connection params to server
 * \param connection    Adress of pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_connect(oph_ioserver_handler * handle, oph_ioserver_params * conn_params, void **connection);

/**
 * \brief               Function to set default database for specified server.
 * \param handle        Dynamic server plugin handle
 * \param db_name       Name of database to be used
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_use_db(oph_ioserver_handler * handle, const char *db_name, void *connection);

/**
 * \brief               Function to execute an operation on data stored into server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_execute_query(oph_ioserver_handler * handle, void *connection, oph_ioserver_query * query);

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
int _mysql_setup_query(oph_ioserver_handler * handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg ** args, oph_ioserver_query ** query);

/**
 * \brief               Function to release resources allocated for query
 * \param handle        Dynamic server plugin handle
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_free_query(oph_ioserver_handler * handle, oph_ioserver_query * query);

/**
 * \brief               Function to close connection established towards data store server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_close(oph_ioserver_handler * handle, void **connection);

/**
 * \brief               Function to finalize library of data store server and release all dynamic loading resources.
 * \param handle        Dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_cleanup(oph_ioserver_handler * handle);

/**
 * \brief               Function to get result set after executing a query.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param result        Pointer to the result set structure to be filled
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_get_result(oph_ioserver_handler * handle, void *connection, oph_ioserver_result ** result);

/**
 * \brief               Function to fetch the next row in a result set.
 * \param handle        Dynamic server plugin handle
 * \param result        Pointer to the result set structure to scan
 * \param current_row	Pointer to the next row structure in the result set
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_fetch_row(oph_ioserver_handler * handle, oph_ioserver_result * result, oph_ioserver_row ** current_row);

/**
 * \brief               Function to free the allocated result set.
 * \param handle        Dynamic server plugin handle
 * \param result        Pointer to the result set structure to free
 * \return              0 if successfull, non-0 otherwise
 */
int _mysql_free_result(oph_ioserver_handler * handle, oph_ioserver_result * result);

#endif				//__MYSQL_IOSERVER_H
