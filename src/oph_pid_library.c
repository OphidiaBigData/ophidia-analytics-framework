/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#include "oph_pid_library.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>

#include "oph_framework_paths.h"

#include "oph_common.h"

#include "debug.h"

extern int msglevel;
char *oph_web_server_name = NULL;
char *oph_web_server_location = NULL;
long long oph_memory_size = -1;
char *oph_base_src_path = NULL;
char oph_user_space = -1;

int oph_pid_create_pid(const char *url, int id_container, int id_datacube, char **pid)
{
	if (!url || !id_container || !id_datacube || !pid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}

	char new_pid[OPH_PID_SIZE];

	int n = snprintf(new_pid, OPH_PID_SIZE, OPH_PID_FORMAT, url, id_container, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of PID exceed limit.\n");
		return OPH_PID_MEMORY_ERROR;
	}

	if (!(*pid = (char *) strndup(new_pid, OPH_PID_SIZE))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}

	return OPH_PID_SUCCESS;
}

int _oph_pid_load_data()
{
	char config[OPH_FRAMEWORK_CONF_PATH_SIZE];
	snprintf(config, sizeof(config), OPH_FRAMEWORK_OPHIDIADB_CONF_FILE_PATH, OPH_ANALYTICS_LOCATION);

	FILE *file = fopen(config, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Configuration file not found\n");
		return OPH_PID_GENERIC_ERROR;
	}

	char buffer[OPH_PID_BUFFER_SIZE];
	char *position;

	while (fscanf(file, "%s", buffer) != EOF) {
		position = strchr(buffer, '=');
		if (position != NULL) {
			*position = '\0';
			position++;
			if (!oph_web_server_name && !strncmp(buffer, OPH_PID_WEBSERVER_NAME, strlen(OPH_PID_WEBSERVER_NAME))) {
				if (!(oph_web_server_name = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					fclose(file);
					return OPH_PID_MEMORY_ERROR;
				}
				strncpy(oph_web_server_name, position, strlen(position) + 1);
				oph_web_server_name[strlen(position)] = '\0';
			} else if (!oph_web_server_location && !strncmp(buffer, OPH_PID_WEBSERVER_LOCATION, strlen(OPH_PID_WEBSERVER_LOCATION))) {
				if (!(oph_web_server_location = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					fclose(file);
					return OPH_PID_MEMORY_ERROR;
				}
				strncpy(oph_web_server_location, position, strlen(position) + 1);
				oph_web_server_location[strlen(position)] = '\0';
			} else if (!strncmp(buffer, OPH_PID_MEMORY, strlen(OPH_PID_MEMORY)) && !strncmp(buffer, OPH_PID_MEMORY, strlen(buffer))) {
				oph_memory_size = (long long) strtoll(position, NULL, 10);
			} else if (!strncmp(buffer, OPH_PID_BASE_SRC_PATH, strlen(OPH_PID_BASE_SRC_PATH)) && !strncmp(buffer, OPH_PID_BASE_SRC_PATH, strlen(buffer))) {
				if (!(oph_base_src_path = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					fclose(file);
					return OPH_PID_MEMORY_ERROR;
				}
				strncpy(oph_base_src_path, position, strlen(position) + 1);
				oph_base_src_path[strlen(position)] = '\0';
			} else if (!strncmp(buffer, OPH_PID_USER_SPACE, strlen(OPH_PID_USER_SPACE)) && !strncmp(buffer, OPH_PID_USER_SPACE, strlen(buffer))) {
				if (!strcasecmp(position, "yes"))
					oph_user_space = 1;
			}
		}
	}
	fclose(file);

	if (oph_memory_size < 0)
		oph_memory_size = 0;

	if (oph_user_space < 0)
		oph_user_space = 0;

	return OPH_PID_SUCCESS;
}

int oph_pid_get_memory_size(long long *memory_size)
{
	if (!memory_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	*memory_size = 0;

	if (oph_memory_size < 0) {
		int res;
		if ((res = _oph_pid_load_data()))
			return res;
	}
	*memory_size = oph_memory_size;

	return OPH_PID_SUCCESS;
}

int oph_pid_get_base_src_path(char *suffix, char **base_src_path)
{
	if (!base_src_path) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	*base_src_path = NULL;

	if (!oph_base_src_path) {
		int res;
		if ((res = _oph_pid_load_data()))
			return res;
	}
	if (!oph_base_src_path)
		return OPH_PID_SUCCESS;

	if (oph_user_space && suffix) {
		char tmp[strlen(oph_base_src_path) + strlen(suffix) + 1];
		sprintf(tmp, "%s/%s", oph_base_src_path, suffix);
		if (!(*base_src_path = strdup(tmp))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_PID_MEMORY_ERROR;
		}
	} else if (!(*base_src_path = strdup(oph_base_src_path))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}

	return OPH_PID_SUCCESS;
}

int oph_pid_get_uri(char **uri)
{
	if (!uri) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	*uri = NULL;

	if (!oph_web_server_name) {
		int res;
		if ((res = _oph_pid_load_data()))
			return res;
	}

	if (!(*uri = strdup(oph_web_server_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}

	return OPH_PID_SUCCESS;
}

char *oph_pid_uri()
{
	if (!oph_web_server_name)
		_oph_pid_load_data();
	return oph_web_server_name;
}

int oph_pid_get_path(char **path)
{
	if (!path) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	*path = NULL;

	if (!oph_web_server_location) {
		int res;
		if ((res = _oph_pid_load_data()))
			return res;
	}

	if (!(*path = strdup(oph_web_server_location))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}

	return OPH_PID_SUCCESS;
}

char *oph_pid_path()
{
	if (!oph_web_server_location)
		_oph_pid_load_data();
	return oph_web_server_location;
}

/** 
 * \brief Function to validate a pid format
 * \param pid String containing PID to validate
 * \param valid Pointer containing 1 if PID is valid, otherwise it'll contain 0
 * \return 0 if successfull, N otherwise
 */
int oph_pid_validate_pid(const char *pid, int *valid)
{
	if (!pid || !valid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	return OPH_PID_SUCCESS;
}

int oph_pid_parse_pid(const char *pid, int *id_container, int *id_datacube, char **uri)
{
	if (!pid || !id_container || !id_datacube || !uri) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}

	*id_container = *id_datacube = 0;

	char *ptr_pid = 0;

	//Parse to last '/' and find datacube id        
	if (!(ptr_pid = strrchr(pid, OPH_PID_SLASH[0]))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error parsing PID string.\n");
		return OPH_PID_MEMORY_ERROR;
	}
	*id_datacube = (int) strtol(ptr_pid + 1, NULL, 10);

	//Remove datacube id from string and parse to last / to find container id
	char tmp_pid[OPH_PID_SIZE] = { 0 };
	strncpy(tmp_pid, pid, strlen(pid) - strlen(ptr_pid));
	tmp_pid[strlen(pid) - strlen(ptr_pid)] = 0;
	if (!(ptr_pid = strrchr(tmp_pid, OPH_PID_SLASH[0]))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error parsing PID string.\n");
		return OPH_PID_MEMORY_ERROR;
	}
	*id_container = (int) strtol(ptr_pid + 1, NULL, 10);

	tmp_pid[strlen(tmp_pid) - strlen(ptr_pid)] = 0;
	if (!(*uri = (char *) malloc((strlen(tmp_pid) + 1) * sizeof(char)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}
	strncpy(*uri, tmp_pid, (strlen(tmp_pid)));
	(*uri)[strlen(tmp_pid)] = '\0';

	return OPH_PID_SUCCESS;
}

int oph_pid_show_pid(const int id_container, const int id_datacube, char *uri)
{
	if (!uri || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}

	if (id_datacube)
		printf(OPH_PID_OUTPUT_MESSAGE OPH_PID_FORMAT "\n", uri, id_container, id_datacube);
	else
		printf(OPH_PID_OUTPUT_MESSAGE2 OPH_PID_FORMAT2 "\n", uri, id_container);
	return OPH_PID_SUCCESS;
}

int oph_pid_free()
{
	if (oph_web_server_name)
		free(oph_web_server_name);
	oph_web_server_name = NULL;

	if (oph_web_server_location)
		free(oph_web_server_location);
	oph_web_server_location = NULL;

	if (oph_base_src_path)
		free(oph_base_src_path);
	oph_base_src_path = NULL;

	return OPH_PID_SUCCESS;
}

int oph_pid_get_session_code(const char *sessionid, char *code)
{
	if (!sessionid || !code) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	if (!oph_web_server_name) {
		int res;
		if ((res = _oph_pid_load_data()))
			return res;
	}

	char tmp[1 + OPH_COMMON_BUFFER_LEN];
	strncpy(tmp, sessionid, OPH_COMMON_BUFFER_LEN);
	tmp[OPH_COMMON_BUFFER_LEN] = 0;

	char *tmp2 = tmp, *savepointer = NULL;
	unsigned short i, max = 3;
#ifndef OPH_STANDALONE_MODE
	if (oph_web_server_name) {
		unsigned int length = strlen(oph_web_server_name);
		if ((length >= OPH_COMMON_BUFFER_LEN) || strncmp(sessionid, oph_web_server_name, length))
			return OPH_PID_GENERIC_ERROR;
		tmp2 += length;
		max = 1;
	}
#else
	max = 1;
#endif

	tmp2 = strtok_r(tmp2, OPH_PID_SLASH, &savepointer);
	if (!tmp2)
		return OPH_PID_GENERIC_ERROR;
	for (i = 0; i < max; ++i) {
		tmp2 = strtok_r(NULL, OPH_PID_SLASH, &savepointer);
		if (!tmp2)
			return OPH_PID_GENERIC_ERROR;
	}
	strcpy(code, tmp2);

	return OPH_PID_SUCCESS;
}

int oph_pid_drop_session_prefix(const char *folder, const char *sessionid, char **new_folder)
{
	if (!folder || !sessionid || !new_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_PID_NULL_PARAM;
	}
	*new_folder = NULL;

	if (*folder != OPH_PID_SLASH[0]) {
		*new_folder = strndup(folder, OPH_COMMON_BUFFER_LEN);
		if (!(*new_folder)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_PID_MEMORY_ERROR;
		}
		return OPH_PID_SUCCESS;
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	if (oph_pid_get_session_code(sessionid, session_code)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Session code cannot be extracted\n");
		return OPH_PID_GENERIC_ERROR;
	}

	char *pointer = strstr(folder, session_code);
	if (!pointer || (pointer != folder + 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Session code not found or unexpected format folder\n");
		return OPH_PID_GENERIC_ERROR;
	}
	pointer = strchr(pointer, OPH_PID_SLASH[0]);

	*new_folder = strndup(pointer && (strlen(pointer) > 1) ? pointer : OPH_PID_SLASH, OPH_COMMON_BUFFER_LEN);
	if (!(*new_folder)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_PID_MEMORY_ERROR;
	}

	return OPH_PID_SUCCESS;
}
