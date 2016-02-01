/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#ifndef __OPH_IOSERVER_H
#define __OPH_IOSERVER_H

#include "oph_framework_paths.h"
#include "oph_common.h"

//*****************Defines***************//

#define OPH_IOSERVER_COMMON_LOG "common"

#define OPH_IOSERVER_FILE_PATH  OPH_FRAMEWORK_DYNAMIC_SERVER_FILE_PATH
#define OPH_IOSERVER_BUFLEN     OPH_COMMON_BUFFER_LEN

#define OPH_IOSERVER_SETUP_FUNC    "_%s_setup"
#define OPH_IOSERVER_CONNECT_FUNC "_%s_connect"
#define OPH_IOSERVER_USE_DB_FUNC "_%s_use_db"
#define OPH_IOSERVER_SETUP_QUERY_FUNC "_%s_setup_query"
#define OPH_IOSERVER_EXECUTE_QUERY_FUNC "_%s_execute_query"
#define OPH_IOSERVER_FREE_QUERY_FUNC "_%s_free_query"
#define OPH_IOSERVER_CLOSE_FUNC   "_%s_close"
#define OPH_IOSERVER_CLEANUP_FUNC     "_%s_cleanup"
#define OPH_IOSERVER_GET_RESULT_FUNC     "_%s_get_result"
#define OPH_IOSERVER_FETCH_ROW_FUNC     "_%s_fetch_row"
#define OPH_IOSERVER_FREE_RESULT_FUNC     "_%s_free_result"

#define OPH_IOSERVER_SEPARATOR '_'

//*************Error codes***************//

#define OPH_IOSERVER_SUCCESS			           0 
#define OPH_IOSERVER_NULL_HANDLE		          -1 
#define OPH_IOSERVER_NOT_NULL_LIB		          -2 
#define OPH_IOSERVER_LIB_NOT_FOUND               -3
#define OPH_IOSERVER_DLINIT_ERR			      -4 
#define OPH_IOSERVER_DLEXIT_ERR			      -5 
#define OPH_IOSERVER_DLOPEN_ERR			      -6 
#define OPH_IOSERVER_DLCLOSE_ERR		          -7 
#define OPH_IOSERVER_DLSYM_ERR			          -8 
#define OPH_IOSERVER_INIT_HANDLE_ERR             -9
#define OPH_IOSERVER_MEMORY_ERR			     -10
#define OPH_IOSERVER_NULL_PARAM			     -11
#define OPH_IOSERVER_VALID_ERROR			     -12

#define OPH_IOSERVER_NULL_HANDLE_FIELD			-101
#define OPH_IOSERVER_NOT_NULL_OPERATOR_HANDLE	-102
#define OPH_IOSERVER_NULL_OPERATOR_HANDLE		-103
#define OPH_IOSERVER_CONNECTION_ERROR			-104
#define OPH_IOSERVER_NULL_CONNECTION			-105
#define OPH_IOSERVER_INVALID_PARAM			    -106
#define OPH_IOSERVER_NULL_RESULT_HANDLE		-107
#define OPH_IOSERVER_COMMAND_ERROR			    -108

#define OPH_IOSERVER_NOT_IMPLEMENTED			-201
#define OPH_IOSERVER_UTILITY_ERROR				-301
#define OPH_IOSERVER_BAD_PARAMETER				-302
#define OPH_IOSERVER_MYSQL_ERROR				-303

//****************Handle******************//

/**
 * \brief                 Handle structure with dynamic server library parameters
 * \param server_type     Name of server plugin to be used
 * \param server_subtype  Name of storage device used within the server
 * \param lib             Dynamic library path
 * \param dlh             Libtool handler to dynamic library
 */
struct _oph_ioserver
{
	char 	*server_type;
	char 	*server_subtype;
	char	*lib;
	void	*dlh;
};
typedef struct _oph_ioserver oph_ioserver_handler;

//****************Other structs******************//

/**
 * \brief           Structure of dbms instance connection params
 * \param host      Hostname of instance
 * \param user      Username to access instance
 * \param passwd    Password to access instance
 * \param port      Port of instance
 * \param db_name   Database to connect
 * \param opt_flag  Optional connection flags
 */
struct _oph_ioserver_params
{
  char          *host;
  char          *user; 
  char          *passwd;
  unsigned int  port;
  char          *db_name;
  unsigned long opt_flag;
};
typedef struct _oph_ioserver_params oph_ioserver_params;

/**
 * \brief			Structure for storing information about the current row
 * \param field_lengths 	Array containing the length for each cell in the row
 * \param row			Array containing the cell values
 */
struct _oph_ioserver_row
{
  unsigned long *field_lengths;
  char **row;
};
typedef struct _oph_ioserver_row oph_ioserver_row;

/**
 * \brief			Structure for result set retrieving
 * \param num_rows		Number of rows of the result set
 * \param num_fields		Number of fields (columns) of the result set
 * \param max_field_length 	Array containing the maximum width of the field; the values of max_length are the length of the string representation of the values in the result set
 * \param result_set		Pointer to the server-specific retrieved result set
 */
struct _oph_ioserver_result
{
  unsigned long long num_rows;
  unsigned int num_fields;
  unsigned long long *max_field_length;
  oph_ioserver_row *current_row;
  void *result_set;
};
typedef struct _oph_ioserver_result oph_ioserver_result;

/**
 * \brief           Enum with admissible argument types
 */
typedef enum { 
      OPH_IOSERVER_TYPE_DECIMAL = 0,  OPH_IOSERVER_TYPE_LONG,
			OPH_IOSERVER_TYPE_FLOAT,  OPH_IOSERVER_TYPE_DOUBLE,
			OPH_IOSERVER_TYPE_NULL, OPH_IOSERVER_TYPE_LONGLONG,
      OPH_IOSERVER_TYPE_VARCHAR, OPH_IOSERVER_TYPE_BIT,
			OPH_IOSERVER_TYPE_LONG_BLOB, OPH_IOSERVER_TYPE_BLOB
}oph_ioserver_arg_types;

/**
 * \brief             Structure to contain a query argument
 * \param arg_type    Type of argument      
 * \param arg_length  Length of argument
 * \param arg         Pointer to argument value
 */
struct _oph_ioserver_query_arg{
  oph_ioserver_arg_types  arg_type;
  unsigned long           arg_length;
  short int               arg_is_null;
void                      *arg;
}; 
typedef struct _oph_ioserver_query_arg oph_ioserver_query_arg;

/**
 * \brief           Enum with admissible argument types
 */
typedef enum  { 
      OPH_IOSERVER_STMT_SIMPLE, OPH_IOSERVER_STMT_BINARY
}oph_ioserver_statement_type;

/**
 * \brief             Structure to contain a query statement
 * \param statement   Pointer to a statement setted by a plugin      
 * \param type        Type of query
 */
struct _oph_ioserver_query{
  void                        *statement;
  oph_ioserver_statement_type type;
};
typedef struct _oph_ioserver_query oph_ioserver_query;

//****************Plugin Interface******************//

//Initialize storage server plugin
int (*_SERVER_setup) (oph_ioserver_handler *handle);

//Connect to storage server
int (*_SERVER_connect) (oph_ioserver_handler *handle, oph_ioserver_params *conn_params, void **connection);

//Set default database
int (*_SERVER_use_db) (oph_ioserver_handler *handle, const char *db_name, void *connection);

//Close connection to storage server
int (*_SERVER_close) (oph_ioserver_handler* handle, void *connection);

//Finalize storage server plugin
int (*_SERVER_cleanup) (oph_ioserver_handler *handle);

//Setup the query structure with given operation and array argument
int (*_SERVER_setup_query) (oph_ioserver_handler* handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg **args, oph_ioserver_query **query); 

//Execute operation in storage server
int (*_SERVER_execute_query) (oph_ioserver_handler* handle, void *connection, oph_ioserver_query *query); 

//Release resources allocated for query
int (*_SERVER_free_query) (oph_ioserver_handler* handle, oph_ioserver_query *query); 

//Get result set from storage server
int (*_SERVER_get_result) (oph_ioserver_handler *handle, void *connection, oph_ioserver_result **result);

//Fetch next row from the result set
int (*_SERVER_fetch_row) (oph_ioserver_handler *handle, oph_ioserver_result *result, oph_ioserver_row **current_row);

//Free result set in the storage server
int (*_SERVER_free_result) (oph_ioserver_handler *handle, oph_ioserver_result *result);

//*****************Internal Functions (used by data access library)***************//

/**
 * \brief               Function to initialize data store server library. WARNING: Call this function before any other function to initialize the dynamic library.
 * \param server_type   String with the name of server plugin to use
 * \param handle        Address to pointer for dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_setup(const char *server_type, oph_ioserver_handler **handle);

/**
 * \brief               Function to connect to data store server.
 * \param handle        Dynamic server plugin handle
 * \param conn_params   Struct with connection params to server
 * \param connection    Adress of pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_connect(oph_ioserver_handler *handle, oph_ioserver_params *conn_params, void **connection);


/**
 * \brief               Function to set default database for specified server.
 * \param handle        Dynamic server plugin handle
 * \param db_name       Name of database to be used
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_use_db(oph_ioserver_handler* handle, const char *db_name, void *connection);

/**
 * \brief               Function to close connection established towards data store server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_close(oph_ioserver_handler* handle, void *connection);

/**
 * \brief               Function to finalize library of data store server and release all dynamic loading resources.
 * \param handle        Dynamic server plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_cleanup(oph_ioserver_handler* handle);

/**
 * \brief               Function to setup the query structure with given operation and array argument
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param operation     String with operation to be performed
 * \param tot_run       Total number of runs the given operation will be executed
 * \param args          Null-terminated array of arguments to be binded to the query (can be NULL)
 * \param query         Pointer to query to be built
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_setup_query(oph_ioserver_handler* handle, void *connection, const char *operation, unsigned long long tot_run, oph_ioserver_query_arg **args, oph_ioserver_query **query); 

/**
 * \brief               Function to execute an operation on data stored into server.
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_execute_query(oph_ioserver_handler* handle, void *connection,  oph_ioserver_query *query); 

/**
 * \brief               Function to release resources allocated for query
 * \param handle        Dynamic server plugin handle
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_free_query(oph_ioserver_handler* handle, oph_ioserver_query *query); 

/**
 * \brief		Function to initialize and get the result set
 * \param handle        Dynamic server plugin handle
 * \param connection    Pointer to server-specific connection structure
 * \param		Pointer to the result set to retrieve
 * \return		0 if successfull, non-0 otherwise
 */
int oph_ioserver_get_result(oph_ioserver_handler* handle, void *connection,  oph_ioserver_result **result); 

/**
 * \brief		Function to fetch the next row in the result set
 * \param handle        Dynamic server plugin handle
 * \param result	Result set to scan
 * \current_row			Pointer to the next row structure in the result set
 * \return		0 if successfull, non-0 otherwise
 */
int oph_ioserver_fetch_row(oph_ioserver_handler* handle, oph_ioserver_result *result, oph_ioserver_row **current_row);

/**
 * \brief		Function to free the pre-allocated result set structure
 * \param handle        Dynamic server plugin handle
 * \param result	Result set to free
 * \return		0 if successfull, non-0 otherwise
 */
int oph_ioserver_free_result(oph_ioserver_handler* handle, oph_ioserver_result *result); 

#endif //__OPH_IOSERVER_H
