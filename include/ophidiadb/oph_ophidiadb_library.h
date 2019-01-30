/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

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

#ifndef __OPH_OPHIDIA_DB__
#define __OPH_OPHIDIA_DB__

/* Project headers */
#include <mysql.h>

#include "oph_framework_paths.h"
#include "oph_common.h"

#define OPH_ODB_SUCCESS 0
#define OPH_ODB_NULL_PARAM 1
#define OPH_ODB_MYSQL_ERROR 2
#define OPH_ODB_STR_BUFF_OVERFLOW 3
#define OPH_ODB_MEMORY_ERROR 4
#define OPH_ODB_TOO_MANY_ROWS 5
#define OPH_ODB_ERROR 6
#define OPH_ODB_NO_ROW_FOUND 7

#define OPH_ODB_DBMS_CONFIGURATION OPH_FRAMEWORK_OPHIDIADB_CONF_FILE_PATH

#define OPH_ODB_BUFFER_LEN 	256
#define OPH_ODB_PATH_LEN 	1000

#define OPH_CONF_OPHDB_NAME	"OPHDB_NAME"
#define OPH_CONF_OPHDB_HOST	"OPHDB_HOST"
#define OPH_CONF_OPHDB_PORT	"OPHDB_PORT"
#define OPH_CONF_OPHDB_LOGIN	"OPHDB_LOGIN"
#define OPH_CONF_OPHDB_PWD	"OPHDB_PWD"

#define OPH_ODB_LOCK_ERROR 1213
#define OPH_ODB_LOCK_WAIT_ERROR 1205
#define OPH_ODB_MAX_ATTEMPTS 5
#define OPH_ODB_WAITING_TIME 2

/**
 * \brief Structure that contain OphidiaDB parameters
 * \param name name of OphidiaDB
 * \param hostname name of OphidiaDB host
 * \param server_port port of the MySQL instance that host OphidiaDB
 * \param username to connect to MySQL instance that host OphidiaDB
 * \param pwd Password to connect to MySQL instance that host OphidiaDB
 * \param conn Pointer to a MYSQL * type that is used to do a query on the db
 */
typedef struct {
	char *name;
	char *hostname;
	int server_port;
	char *username;
	char *pwd;
	MYSQL *conn;
} ophidiadb;

/**
 * \brief Function to read OphidiaDB info from configuration file
 * \param ophidiadb Pointer to an allocated ophidiadb structure
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_read_ophidiadb_config_file(ophidiadb * oDB);

/**
 * \brief Function to initilize OphidiaDB structure.
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_init_ophidiadb(ophidiadb * oDB);

/**
 * \brief Function to initilize OphidiaDB structure in multi-thread environment (mysql_library_init must be called before).
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_init_ophidiadb_thread(ophidiadb * oDB);

/**
 * \brief Function to delete OphidiaDB and to free memory allocated.
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_free_ophidiadb(ophidiadb * oDB);

/**
 * \brief Function to delete OphidiaDB and free memory in multi-thread environment (mysql_library_end must be called after).
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_free_ophidiadb_thread(ophidiadb * oDB);

/**
 * \brief Function to connect to the OphidiaDB. WARNING: Call this function before any other function or the system will crash
 * \param structure containing OphidiaDB parameters
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_connect_to_ophidiadb(ophidiadb * oDB);

/**
 * \brief Function to check connect status to the OphidiaDB. WARNING: Do not call this function (or any other) before calling connect_to_ophidiaDB or the client will crash
 * \param structure containing OphidiaDB parameters
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_check_connection_to_ophidiadb(ophidiadb * oDB);

/**
 * \brief Function to disconnect from the OphidiaDB
 * \param structure containig OphidiaDB parameters
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_disconnect_from_ophidiadb(ophidiadb * oDB);

/**
 * \brief Function to query the OphidiaDB (retries the execution in case of lock-related issues)
 * \param structure containig OphidiaDB parameters
 * \param string with query to be performed
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_query_ophidiadb(ophidiadb * oDB, char *query);

#endif				/* __OPH_OPHIDIA_DB__ */
