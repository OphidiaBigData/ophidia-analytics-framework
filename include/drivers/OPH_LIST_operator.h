/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#ifndef __OPH_LIST_OPERATOR_H
#define __OPH_LIST_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_LIST_FOLDER_PATH "%s/%s/"
#define OPH_LIST_CONTAINER_PATH "%s/%s"
#define OPH_LIST_FOLDER_PATH_NAME "%s/"
#define OPH_LIST_CONTAINER_PATH_NAME "%s"

#define OPH_LIST_TASK_MULTIPLE_INPUT OPH_ODB_FS_TASK_MULTIPLE_INPUT
#define OPH_LIST_TASK_RANDOM_INPUT "RANDOM"
#define OPH_LIST_TASK_MISSING_INPUT "MISSING (DELETED)"
#define OPH_LIST_TASK_FILE_INPUT "FILE"
#define OPH_LIST_TASK_DATACUBE_INPUT "CUBE"

/**
 * \brief Structure of parameters needed by the operator OPH_LIST. It shows information about the datacubes fragmentation (file system)
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param path Absolute/relative path where the container is created
 * \param cwd : current working directory as an absolute path 
 * \param level Level of verbosity 0 to 8
 * \param container_name Name of the container to filter on
 * \param datacube_name Name of the datacube to filter on
 * \param hostname Name of the host to filter on
 * \param id_dbms ID of dbms to filter on
 * \param db_name Database name to filter on
 * \param measure Measure name to filter on
 * \param src Source name to filter on
 * \param oper_level Level of transformation to filter on
 * \param recursive_search Type of search
 * \param user User executing the research
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_LIST_operator_handle {
	ophidiadb oDB;
	int level;
	int recursive_search;
	char *path;
	char *cwd;
	char *container_name;
	char *datacube_name;
	char *hostname;
	char *src;
	char *measure;
	int oper_level;
	int id_dbms;
	char *db_name;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_LIST_operator_handle OPH_LIST_operator_handle;

#endif				//__OPH_LIST_OPERATOR_H
