/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2022 CMCC Foundation

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

#ifndef __OPH_PID_H__
#define __OPH_PID_H__

#define OPH_PID_OUTPUT_MESSAGE		"PID of output datacube is: "
#define OPH_PID_FORMAT				"%s/%d/%d"

#define OPH_PID_OUTPUT_MESSAGE2		"PID of output container is: "
#define OPH_PID_FORMAT2				"%s/%d"

#define OPH_PID_WEBSERVER_NAME		"WEB_SERVER"
#define OPH_PID_WEBSERVER_LOCATION	"WEB_SERVER_LOCATION"
#define OPH_PID_MEMORY				"MEMORY"
#define OPH_PID_BUFFER				"IO_BUFFER"
#define OPH_PID_BASE_SRC_PATH		"BASE_SRC_PATH"
#define OPH_PID_BASE_USER_PATH		"BASE_USER_PATH"
#define OPH_PID_USER_SPACE			"USER_SPACE"
#define OPH_PID_B2DROP_WEBDAV		"B2DROP_WEBDAV"
#define OPH_PID_CDO_PATH			"CDO_PATH"
#define OPH_PID_ENABLE_UNREGISTERED_SCRIPT "ENABLE_UNREGISTERED_SCRIPT"

#define OPH_PID_SLASH				"/"

#define OPH_PID_SUCCESS				0
#define OPH_PID_NULL_PARAM			1
#define OPH_PID_MEMORY_ERROR		2
#define OPH_PID_GENERIC_ERROR		3

#define OPH_PID_SIZE				512

#define OPH_PID_BUFFER_SIZE			256

/** 
 * \brief Function to load configuration data
 * \brief b2drop_webdav Pointer to the memory area where b2drop_webdav will be written; it has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_b2drop_webdav_url(char **b2drop_webdav);

/** 
 * \brief Function to load configuration data
 * \brief memory_size Pointer to the memory area where memory size will be written
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_memory_size(long long *memory_size);

/** 
 * \brief Function to load configuration data
 * \brief memory_size Pointer to the memory area where memory size will be written
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_buffer_size(long long *buffer_size);

/** 
 * \brief Function to load configuration data
 * \brief base_src_path Pointer to the memory area where base_src_path will be written; it has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_base_src_path(char **base_src_path);

/** 
 * \brief Function to load configuration data
 * \brief cdo_path Pointer to the memory area where cdo_path will be written; it has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_cdo_path(char **cdo_path);

/** 
 * \brief Function to load configuration data
 * \brief username User that has submitted the task
 * \brief base_src_path Pointer to the memory area where base_src_path will be written; it has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_base_user_path(char *username, char **base_user_path);

/** 
 * \brief Function to load configuration data
 * \brief user_space Pointer to the space to store the flag
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_user_space(char *user_space);

/** 
 * \brief Function to load configuration data
 * \brief enabled Pointer to the space to store the flag
 * \return 0 if successfull, N otherwise
 */
int oph_pid_is_script_enabled(char *enabled);

/** 
 * \brief Function to create a new pid given container and datacube id
 * \param id_container Id of the container idenfied by PID
 * \param id_datacube Id of the datacube idenfied by PID
 * \param pid Pointer to string used to store new PID (it has to be freed)
 * \return 0 if successfull, N otherwise
 */
int oph_pid_create_pid(const char *url, int id_container, int id_datacube, char **pid);

/** 
 * \brief Function to retrieve PID server uri
 * \param uri String containing PID uri (it has to be freed)
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_uri(char **uri);

/** 
 * \brief Function to retrieve PID server uri
 * \return String containing PID uri (it does not have to be freed)
 */
char *oph_pid_uri();

/** 
 * \brief Function to retrieve base dir for PID
 * \param uri String containing the path to basedir (it has to be freed)
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_path(char **path);

/** 
 * \brief Function to retrieve base dir for PID
 * \return String containing the path to basedir (it does not have to be freed)
 */
char *oph_pid_path();

/** 
 * \brief Function to validate a pid format
 * \param pid String containing PID to validate
 * \param valid Pointer containing 1 if PID is valid, otherwise it'll contain 0
 * \return 0 if successfull, N otherwise
 */
int oph_pid_validate_pid(const char *pid, int *valid);

/** 
 * \brief Function to parse a PID and extract container and datacube ID
 * \param pid String containing PID to parse
 * \param id_container Pointer with container id parsed
 * \param id_datacube Pointer with datacube id parsed
 * \param uri String containing PID uri (it has to be freed)
 * \return 0 if successfull, N otherwise
 */
int oph_pid_parse_pid(const char *pid, int *id_container, int *id_datacube, char **uri);

/** 
 * \brief Function used to show a PID
 * \param id_container Container id
 * \param id_datacube Datacube id
 * \param uri String containing PID uri
 * \return 0 if successfull, N otherwise
 */
int oph_pid_show_pid(const int id_container, const int id_datacube, char *uri);


/** 
 * \brief Function used to free internal data 
 * \return 0 if successfull, N otherwise
 */
int oph_pid_free();

/** 
 * \brief Function used to extract session code from sessionid
 * \param sessionid Sessionid
 * \param code Session code. It has to be an pre-allocated string 
 * \return 0 if successfull, N otherwise
 */
int oph_pid_get_session_code(const char *sessionid, char *code);

/** 
 * \brief Function to convert folder name into user format
 * \param folder Address of the string to be converted
 * \param session_code Session code of the session which the folder belongs to
 * \param new_folder. It has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_pid_drop_session_prefix(const char *folder, const char *sessionid, char **new_folder);

#endif
