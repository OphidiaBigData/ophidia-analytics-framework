/*
    Ophidia IO Server
    Copyright (C) 2014-2019 CMCC Foundation

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

#include "oph_server_confs.h"
#define _GNU_SOURCE

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <string.h>

#include "hashtbl.h"
#include "debug.h"

#define STRCMP(a,b) strncasecmp(a, b, (strlen(a) > strlen(b) ? strlen(a) : strlen(b)))

// #include "oph_server_utility.h"

// #include "debug.h"

extern int msglevel;
// extern char *oph_server_conf_file;

int oph_server_conf_load(short unsigned int instance, HASHTBL ** hashtbl, char *oph_server_conf_file)
{
	if (!hashtbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_SERVER_CONF_NULL_PARAM;
	}

	if (!(*hashtbl = hashtbl_create(OPH_SERVER_CONF_LOAD_SIZE, NULL))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create hash table\n");
		return OPH_SERVER_CONF_ERROR;
	}

	FILE *file = fopen(oph_server_conf_file, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Configuration file not found\n");
		hashtbl_destroy(*hashtbl);
		*hashtbl = NULL;
		return OPH_SERVER_CONF_ERROR;
	} else {
		char buffer[OPH_SERVER_CONF_LINE_LEN] = { 0 };
		char tmp_buffer[OPH_SERVER_CONF_LINE_LEN] = { 0 };
		char *value = NULL;
		char *position = NULL, *position2 = NULL;
		const char *param_iter = NULL;
		int i = 0, j = 0;
		char instance_flag = 0;
		char *instance_string = 0;
		short unsigned int instance_number = 0;

		while (!feof(file)) {

			//Retrieve instance header
			memset(tmp_buffer, 0, sizeof(buffer));
			memset(buffer, 0, sizeof(buffer));
			if (!fgets(buffer, sizeof(buffer), file)) {
				if (feof(file)) {
					fclose(file);
					return OPH_SERVER_CONF_SUCCESS;
				} else {
					fclose(file);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while reading configuration file\n");
					hashtbl_destroy(*hashtbl);
					return OPH_SERVER_CONF_ERROR;
				}
			}
			//Check if row is a comment
			j = 0;
			while (buffer[j] == ' ')
				j++;
			//Row is a comment
			if (buffer[j] == OPH_SERVER_CONF_COMMENT)
				continue;
			//Check if row is newline
			j = 0;
			while (buffer[j] == ' ')
				j++;
			//Row is a newline
			if (buffer[j] == '\n')
				continue;

			sscanf(buffer, "[%[^]]", tmp_buffer);

			if (instance_flag == 0) {
				//If row does not contain an instance ref then step to next row
				if (!(instance_string = strstr(tmp_buffer, OPH_SERVER_CONF_INSTANCE))) {
					continue;
				} else {
					//Check instance number
					if (instance != 0) {
						instance_string += strlen(OPH_SERVER_CONF_INSTANCE);
						instance_number = (short unsigned int) strtol(instance_string, NULL, 10);
						if (instance_number == 0)
							continue;
						if (instance_number != instance)
							continue;
					}
					//If instance header is found read all lines until next one
					instance_flag = 1;
				}
			} else {
				if (!strstr(tmp_buffer, OPH_SERVER_CONF_INSTANCE)) {
					//Parse row          

					//Get param-value separator
					value = NULL;
					position = position2 = NULL;
					position = strchr(buffer, '=');
					if (position != NULL) {
						//Set = symbol to NULL
						position[0] = 0;
						position++;
						//If new line char is read, then remove it
						if ((position2 = strchr(position, '\n')) != NULL) {
							position2[0] = 0;
						}
						//Check if param is allowed
						i = 0;
						param_iter = oph_server_conf_params[i++];
						while (param_iter) {
							if (STRCMP(buffer, param_iter) == 0)
								break;
							param_iter = oph_server_conf_params[i++];
						}

						if (param_iter == NULL) {
							pmesg(LOG_DEBUG, __FILE__, __LINE__, "Configuration param not allowed %s\n", buffer);
							printf("\nConfiguration param not allowed %s\n", buffer);
							continue;
						}
						//Alloc value to be added to hash table
						value = strndup(position, OPH_SERVER_CONF_LINE_LEN);
						if (value == NULL) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while copying param %s\n", buffer);
							printf("\nError while copying param %s\n", buffer);
							fclose(file);
							hashtbl_destroy(*hashtbl);
							*hashtbl = NULL;
							return OPH_SERVER_CONF_ERROR;
						}

						if (hashtbl_insert(*hashtbl, buffer, value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading param %s\n", buffer);
							printf("\nError while loading param %s\n", buffer);
							fclose(file);
							free(value);
							hashtbl_destroy(*hashtbl);
							*hashtbl = NULL;
							return OPH_SERVER_CONF_ERROR;
						}

						free(value);
						// pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM: %s = %s \n", buffer, position);
					} else {
						pmesg(LOG_DEBUG, __FILE__, __LINE__, "Configuration param corrupted %s\n", buffer);
					}
				} else {
					//Found second instance ref, end cycle
					break;
				}
			}
		}

		fclose(file);
	}

	return OPH_SERVER_CONF_SUCCESS;
}

int oph_server_conf_get_param(HASHTBL * hashtbl, const char *param, char **value)
{
	if (!hashtbl || !param) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_SERVER_CONF_NULL_PARAM;
	}

	*value = hashtbl_get(hashtbl, param);

	if (!*value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get parameter '%s'\n", param);
		return OPH_SERVER_CONF_ERROR;
	}

	return OPH_SERVER_CONF_SUCCESS;
}

int oph_server_conf_unload(HASHTBL ** hashtbl)
{
	if (hashtbl && *hashtbl) {
		hashtbl_destroy(*hashtbl);
		*hashtbl = NULL;
	}

	return OPH_SERVER_CONF_SUCCESS;
}
