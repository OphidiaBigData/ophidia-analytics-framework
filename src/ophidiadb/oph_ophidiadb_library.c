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

#include "oph_ophidiadb_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <mysql.h>
#include "debug.h"

extern int msglevel;

int oph_odb_read_ophidiadb_config_file(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	char config[OPH_ODB_PATH_LEN];
  	snprintf(config, sizeof(config), OPH_ODB_DBMS_CONFIGURATION, OPH_ANALYTICS_LOCATION);
    FILE *file = fopen(config, "r");
    if(file == NULL)
    {
            pmesg(LOG_ERROR, __FILE__, __LINE__, "Configuration file not found\n");
			return OPH_ODB_ERROR;
    }
    else
    {
			char buffer[OPH_ODB_BUFFER_LEN];
            char *position;
            if( fscanf(file, "%s", buffer) == EOF)
            {
				    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving data from configuration file\n");
					fclose(file);
					return OPH_ODB_ERROR;
            }

            position = strchr(buffer, '=');
            if(position != NULL){
				if(!(oDB->name=(char*)malloc((strlen(position+1)+1)*sizeof(char)))){
		                pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		                fclose(file);
						return OPH_ODB_MEMORY_ERROR;
		        }
                strncpy(oDB->name, position+1, strlen(position+1)+1);
				oDB->name[strlen(position+1)] = '\0';
			}
            if( fscanf(file, "%s", buffer) == EOF)
            {
				    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving data from configuration file\n");
					fclose(file);
					return OPH_ODB_ERROR;
            }

            position = strchr(buffer, '=');
            if(position != NULL){
				if(!(oDB->hostname=(char*)malloc((strlen(position+1)+1)*sizeof(char)))){
		                pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		                fclose(file);
						return OPH_ODB_MEMORY_ERROR;
		        }
                strncpy(oDB->hostname, position+1, strlen(position+1)+1);
				oDB->hostname[strlen(position+1)] = '\0';
			}
			if( fscanf(file, "%s", buffer) == EOF)
            {
				    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving data from configuration file\n");
					fclose(file);
					return OPH_ODB_ERROR;
            }

            position = strchr(buffer, '=');
            if(position != NULL){
                oDB->server_port = (int)strtol(position+1, NULL, 10);
			}
            if( fscanf(file, "%s", buffer) == EOF)
            {
				    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving data from configuration file\n");
					fclose(file);
					return OPH_ODB_ERROR;
            }

            position = strchr(buffer, '=');
            if(position != NULL){
				if(!(oDB->username=(char*)malloc((strlen(position+1)+1)*sizeof(char)))){
		                pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		                fclose(file);
						return OPH_ODB_MEMORY_ERROR;
		        }
                strncpy(oDB->username, position+1, strlen(position+1)+1);
				oDB->username[strlen(position+1)] = '\0';
			}
            if(fscanf(file, "%s", buffer) == EOF )
            {
				    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving data from configuration file\n");
					fclose(file);
					return OPH_ODB_ERROR;
            }

            position = strchr(buffer, '=');
            if(position != NULL){
				if(!(oDB->pwd=(char*)malloc((strlen(position+1)+1)*sizeof(char)))){
		                pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		                fclose(file);
						return OPH_ODB_MEMORY_ERROR;
		        }
                strncpy(oDB->pwd, position+1, strlen(position+1)+1);
				oDB->pwd[strlen(position+1)] = '\0';
			}
    }
    fclose(file);

	return OPH_ODB_SUCCESS;
}

int oph_odb_init_ophidiadb(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	oDB->name=NULL;
	oDB->hostname = NULL;
	oDB->username=NULL;
	oDB->pwd=NULL;
	oDB->conn=NULL;

	return OPH_ODB_SUCCESS;
}

int oph_odb_free_ophidiadb(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if(oDB->name){
		free(oDB->name);
		oDB->name=NULL;
	}
	if(oDB->hostname){
		free(oDB->hostname);
		oDB->hostname = NULL;
	}
	if(oDB->username){
		free(oDB->username);
		oDB->username=NULL;
	}
	if(oDB->pwd){
		free(oDB->pwd);
		oDB->pwd=NULL;
	}
	if(oDB->conn){
		oph_odb_disconnect_from_ophidiadb(oDB);
		oDB->conn=NULL;
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_connect_to_ophidiadb(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (mysql_library_init(0, NULL, NULL)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	oDB->conn = NULL;
	if(!(oDB->conn = mysql_init(NULL))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error: %s\n", mysql_error(oDB->conn));
		oph_odb_disconnect_from_ophidiadb(oDB);
        return OPH_ODB_MYSQL_ERROR;
    }

	/* Connect to database */
	if (!mysql_real_connect(oDB->conn, oDB->hostname, oDB->username, oDB->pwd, oDB->name, oDB->server_port, NULL, 0)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL connection error: %s\n", mysql_error(oDB->conn));
		oph_odb_disconnect_from_ophidiadb(oDB);
        return OPH_ODB_MYSQL_ERROR;
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_check_connection_to_ophidiadb(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if(!(oDB->conn)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Connection was somehow closed.\n");
        	return OPH_ODB_MYSQL_ERROR;
	}

	if(mysql_ping(oDB->conn))
	{
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Connection was lost. Reconnecting...\n");
		mysql_close(oDB->conn); // Flush any data related to previuos connection
		/* Connect to database */
		if( oph_odb_connect_to_ophidiadb(oDB)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			oph_odb_disconnect_from_ophidiadb(oDB);
		    return OPH_ODB_MYSQL_ERROR;
		}
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_disconnect_from_ophidiadb(ophidiadb *oDB)
{
	if(!oDB){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if(oDB->conn){
		mysql_close(oDB->conn);
		oDB->conn = NULL;
	}
	mysql_library_end();
	return OPH_ODB_SUCCESS;
}
