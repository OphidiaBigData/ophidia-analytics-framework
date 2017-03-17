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

#ifndef __OPH_IOSERVER_LOG_ERROR_CODES_H
#define __OPH_IOSERVER_LOG_ERROR_CODES_H

/*MYSQL IOSERVER LOG ERRORS*/
#define OPH_IOSERVER_LOG_MYSQL_LIB_INIT_ERROR   "MySQL lib initialization error\n"
#define OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM "Null input parameter\n"
#define OPH_IOSERVER_LOG_MYSQL_INIT_ERROR       "MySQL initialization error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_CONN_ERROR       "MySQL connection error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_LOST_CONN        "Connection was lost. Reconnecting...\n"
#define OPH_IOSERVER_LOG_MYSQL_USE_DB_ERROR     "MySQL use DB error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_ERROR "MySQL execute query error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_NOT_NULL_INPUT_PARAM   "Not null input parameter\n"
#define OPH_IOSERVER_LOG_MYSQL_STORE_ERROR      "Store result error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_MEMORY_ERROR	"Memory allocation error\n"
#define OPH_IOSERVER_LOG_MYSQL_EXEC_QUERY_TYPE_ERROR "MySQL query type not defined\n"
#define OPH_IOSERVER_LOG_MYSQL_STMT_ERROR       "MySQL statement error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_BIND_ERROR       "Cannot allocate input buffers\n"
#define OPH_IOSERVER_LOG_MYSQL_BINDING_ERROR    "Error in mysql_stmt_bind_param() failed: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_STMT_EXEC_ERROR  "Error in mysql_stmt_execute() failed: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_UNKNOWN_KEYWORD  "Unknown keyword '%s'\n"
#define OPH_IOSERVER_LOG_MYSQL_UNKNOWN_OPERATION "Unknown operation '%s'\n"
#define OPH_IOSERVER_LOG_MYSQL_ERROR_PARSING    "Error parsing submission query!\n"
#define OPH_IOSERVER_LOG_MYSQL_QUERY_NOT_VALID  "Submission string is not valid!\n"
#define OPH_IOSERVER_LOG_MYSQL_ARG_COUNT_ERROR  "Unable to count query arguments!\n"
#define OPH_IOSERVER_LOG_MYSQL_HASHTBL_ERROR    "Unable to create hash table\n"
#define OPH_IOSERVER_LOG_MYSQL_LOAD_ARGS_ERROR  "Unable to load query arguments\n"
#define OPH_IOSERVER_LOG_MYSQL_MISSING_ARG      "Missing parameter '%s'\n"
#define OPH_IOSERVER_LOG_MYSQL_ARG_EVAL_ERROR   "Argument evaluation error: %s\n"
#define OPH_IOSERVER_LOG_MYSQL_KEYWORD_EVAL_ERROR  "Keyword evaluation error\n"
#define OPH_IOSERVER_LOG_MYSQL_BAD_MULTI_ARG    "Bad multi-value argument '%s'\n"
#define OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_DONT_CORRESPOND  "Multi-value argument numbers do not correspond'\n"
#define OPH_IOSERVER_LOG_MYSQL_MULTI_ARG_TOO_BIG "Multi-value argument number too big\n"

#define OPH_IOSERVER_LOG_OPHIDIAIO_NULL_INPUT_PARAM OPH_IOSERVER_LOG_MYSQL_NULL_INPUT_PARAM
#define OPH_IOSERVER_LOG_OPHIDIAIO_CONN_ERROR       "OPHIDIAIO connection error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_USE_DB_ERROR     "OPHIDIAIO use DB error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_EXEC_QUERY_ERROR "OPHIDIAIO execute query error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_MEMORY_ERROR	"Memory allocation error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_ERROR_PARSING    "Error parsing submission query!\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_SETUP_QUERY_ERROR "OPHIDIAIO setup query error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_GET_RESULT_ERROR "OPHIDIAIO get result error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_NOT_NULL_INPUT_PARAM OPH_IOSERVER_LOG_MYSQL_NOT_NULL_INPUT_PARAM
#define OPH_IOSERVER_LOG_OPHIDIAIO_FETCH_ROW_ERROR  "OPHIDIAIO fetch row error\n"
#define OPH_IOSERVER_LOG_OPHIDIAIO_EXEC_QUERY_TYPE_ERROR "MySQL query type not defined\n"

#endif				//__OPH_IOSERVER_LOG_ERROR_CODES_H
