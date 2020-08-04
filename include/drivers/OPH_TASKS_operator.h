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

#ifndef __OPH_TASKS_OPERATOR_H
#define __OPH_TASKS_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_TASKS. It shows information about executed task
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param datacube_name Name of the datacube to filter on
 * \param path Absolute/relative path where the container is created
 * \param cwd : current working directory as an absolute path 
 * \param container_name Name of the container to filter on
 * \param user Name of the user calling the import operation
 * \param operatior Name of the operator to filter on
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_TASKS_operator_handle {
	ophidiadb oDB;
	char *datacube_name;
	char *path;
	char *cwd;
	char *container_name;
	char *user;
	char *operator;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int recursive_search;
};
typedef struct _OPH_TASKS_operator_handle OPH_TASKS_operator_handle;

#endif				//__OPH_TASKS_OPERATOR_H
