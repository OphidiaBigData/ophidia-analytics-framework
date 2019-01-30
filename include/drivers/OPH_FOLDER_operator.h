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

#ifndef __OPH_FOLDER_OPERATOR_H
#define __OPH_FOLDER_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_FOLDER_CMD_CD       "cd"
#define OPH_FOLDER_CMD_MKDIR    "mkdir"
#define OPH_FOLDER_CMD_MV       "mv"
#define OPH_FOLDER_CMD_RM       "rm"

#define OPH_FOLDER_MODE_CD              0
#define OPH_FOLDER_MODE_MKDIR           1
#define OPH_FOLDER_MODE_MV              2
#define OPH_FOLDER_MODE_RM              3

#define OPH_FOLDER_CD_MESSAGE "Current Working Directory"

/**
 * \brief Structure of parameters needed by the operator OPH_FOLDER. It manages the Ophidia filesystem folders.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param mode : number corresponding to one of the provided commands
 * \param path : 1 or 2 absolute or relative paths
 * \param path_num : number of input paths
 * \param cwd : current working directory as an absolute path
 * \param user : username of the user managing the filesystem.
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 * \param userrole User role
 */
typedef struct _OPH_FOLDER_operator_handle {
	ophidiadb oDB;
	int mode;
	char **path;
	int path_num;
	char *cwd;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int userrole;
} OPH_FOLDER_operator_handle;


#endif				//__OPH_FOLDER_OPERATOR_H
