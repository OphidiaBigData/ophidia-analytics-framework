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

#include "oph_ophidiadb_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <unistd.h>
#include <ctype.h>
#include <mysql.h>
#include "debug.h"

extern int msglevel;

int oph_odb_read_ophidiadb_config_file(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char config[OPH_ODB_PATH_LEN];
	snprintf(config, sizeof(config), OPH_ODB_DBMS_CONFIGURATION, OPH_ANALYTICS_LOCATION);
	FILE *file = fopen(config, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Configuration file not found\n");
		return OPH_ODB_ERROR;
	}

	unsigned int i;
	char *argument = NULL;
	char *argument_value = NULL;
	int argument_length = 0;
	char *result = NULL;
	char line[OPH_ODB_BUFFER_LEN] = { '\0' };
	while (!feof(file)) {
		result = fgets(line, OPH_ODB_BUFFER_LEN, file);
		if (!result) {
			if (ferror(file)) {
				fclose(file);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read file line from %s\n", OPH_ODB_BUFFER_LEN);
				return OPH_ODB_ERROR;
			} else {
				break;
			}
		}

		/* Remove trailing newline */
		if (line[strlen(line) - 1] == '\n')
			line[strlen(line) - 1] = '\0';

		/* Skip comment lines */
		if (line[0] == '#') {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Read comment line: %s\n", line);
			continue;
		}

		/* Check if line contains only spaces */
		for (i = 0; (i < strlen(line)) && (i < OPH_ODB_BUFFER_LEN); i++) {
			if (!isspace((unsigned char) line[i]))
				break;
		}
		if (i == strlen(line) || i == OPH_ODB_BUFFER_LEN) {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Read empty or blank line\n");
			continue;
		}

		/* Split argument and value on '=' character */
		for (i = 0; (i < strlen(line)) && (i < OPH_ODB_BUFFER_LEN); i++) {
			if (line[i] == '=')
				break;
		}
		if ((i == strlen(line)) || (i == OPH_ODB_BUFFER_LEN)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Read invalid line: %s\n", line);
			continue;
		}

		argument_length = strlen(line) - i - 1;

		argument = (char *) strndup(line, sizeof(char) * i);
		if (!argument) {
			fclose(file);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc memory for configuration argument\n");
			return OPH_ODB_ERROR;
		}

		argument_value = (char *) strndup(line + i + 1, sizeof(char) * argument_length);
		if (!argument_value) {
			fclose(file);
			free(argument);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc memory for configuration argument\n");
			return OPH_ODB_ERROR;
		}

		if (!strncasecmp(argument, OPH_CONF_OPHDB_NAME, strlen(OPH_CONF_OPHDB_NAME))) {
			oDB->name = argument_value;
		} else if (!strncasecmp(argument, OPH_CONF_OPHDB_HOST, strlen(OPH_CONF_OPHDB_HOST))) {
			oDB->hostname = argument_value;
		} else if (!strncasecmp(argument, OPH_CONF_OPHDB_PORT, strlen(OPH_CONF_OPHDB_PORT))) {
			oDB->server_port = (int) strtol(argument_value, NULL, 10);
			free(argument_value);
		} else if (!strncasecmp(argument, OPH_CONF_OPHDB_LOGIN, strlen(OPH_CONF_OPHDB_LOGIN))) {
			oDB->username = argument_value;
		} else if (!strncasecmp(argument, OPH_CONF_OPHDB_PWD, strlen(OPH_CONF_OPHDB_PWD))) {
			oDB->pwd = argument_value;
		} else {
			free(argument_value);
		}

		free(argument);
	}

	fclose(file);

	return OPH_ODB_SUCCESS;
}

int oph_odb_init_ophidiadb(ophidiadb *oDB)
{
	/*if (mysql_library_init(0, NULL, NULL)) {
	   pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
	   return OPH_ODB_MYSQL_ERROR;
	   } */

	return oph_odb_init_ophidiadb_thread(oDB);
}

int oph_odb_init_ophidiadb_thread(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	oDB->name = NULL;
	oDB->hostname = NULL;
	oDB->username = NULL;
	oDB->pwd = NULL;
	oDB->conn = NULL;

	return OPH_ODB_SUCCESS;
}

int oph_odb_free_ophidiadb(ophidiadb *oDB)
{
	//mysql_library_end();

	return oph_odb_free_ophidiadb_thread(oDB);
}

int oph_odb_free_ophidiadb_thread(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oDB->name) {
		free(oDB->name);
		oDB->name = NULL;
	}
	if (oDB->hostname) {
		free(oDB->hostname);
		oDB->hostname = NULL;
	}
	if (oDB->username) {
		free(oDB->username);
		oDB->username = NULL;
	}
	if (oDB->pwd) {
		free(oDB->pwd);
		oDB->pwd = NULL;
	}
	if (oDB->conn) {
		oph_odb_disconnect_from_ophidiadb(oDB);
		oDB->conn = NULL;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_connect_to_ophidiadb(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	oDB->conn = NULL;
	if (!(oDB->conn = mysql_init(NULL))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error: %s\n", mysql_error(oDB->conn));
		oph_odb_disconnect_from_ophidiadb(oDB);
		return OPH_ODB_MYSQL_ERROR;
	}

	/* Connect to database */
	if (!mysql_real_connect(oDB->conn, oDB->hostname, oDB->username, oDB->pwd, oDB->name, oDB->server_port, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL connection error: %s\n", mysql_error(oDB->conn));
		oph_odb_disconnect_from_ophidiadb(oDB);
		return OPH_ODB_MYSQL_ERROR;
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_check_connection_to_ophidiadb(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (!(oDB->conn)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Connection was somehow closed.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	if (mysql_ping(oDB->conn)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Connection was lost. Reconnecting...\n");
		mysql_close(oDB->conn);	// Flush any data related to previuos connection
		/* Connect to database */
		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			oph_odb_disconnect_from_ophidiadb(oDB);
			return OPH_ODB_MYSQL_ERROR;
		}
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_disconnect_from_ophidiadb(ophidiadb *oDB)
{
	if (!oDB) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oDB->conn) {
		mysql_close(oDB->conn);
		oDB->conn = NULL;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_query_ophidiadb(ophidiadb *oDB, char *query)
{
	if (!oDB || !query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	short runs = 1;
	int ret = 0;
	int mysql_code = 0;
	do {
		if ((ret = mysql_query(oDB->conn, query))) {
			mysql_code = mysql_errno(oDB->conn);
			if (((mysql_code == OPH_ODB_LOCK_ERROR) || (mysql_code == OPH_ODB_LOCK_WAIT_ERROR)) && (runs < OPH_ODB_MAX_ATTEMPTS)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				sleep(OPH_ODB_WAITING_TIME);
				runs++;
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
		} else
			break;
	} while (runs <= OPH_ODB_MAX_ATTEMPTS);	// Useless


	return OPH_ODB_SUCCESS;
}
