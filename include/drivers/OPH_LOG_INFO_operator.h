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

#ifndef __OPH_LOG_INFO_OPERATOR_H
#define __OPH_LOG_INFO_OPERATOR_H

#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_LOG_INFO_SERVER_LOG_REQUEST 1
#define OPH_LOG_INFO_CONTAINER_LOG_REQUEST 2
#define OPH_LOG_INFO_IOSERVER_LOG_REQUEST 3

#define OPH_LOG_INFO_SERVER "server"
#define OPH_LOG_INFO_CONTAINER "container"
#define OPH_LOG_INFO_IOSERVER "ioserver"

#define OPH_LOG_INFO_DEFAULT_NLINES 10

/**
 * \brief Structure of parameters needed by the operator OPH_LOG_INFO. It shows the last "nlines" of a container's log or the server log.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param user Name of the user that wants to read log file
 * \param log_type Type of log: server's (1) or container's (2) log.
 * \param container_id ID of the container, if log_type is 2.
 * \param nlines Number of lines to be displayed from the end of the log.
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
typedef struct _OPH_LOG_INFO_operator_handle {
	int log_type;
	int container_id;
	char *server_type;
	int nlines;
	ophidiadb oDB;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
} OPH_LOG_INFO_operator_handle;

#endif				//__OPH_LOG_INFO_OPERATOR_H
