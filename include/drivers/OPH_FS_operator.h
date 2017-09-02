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

#ifndef __OPH_FS_OPERATOR_H
#define __OPH_FS_OPERATOR_H

//Operator specific headers
#include "oph_common.h"

#define OPH_FS_CMD_LS	"ls"
#define OPH_FS_CMD_CD	"cd"
#define OPH_FS_CMD_MD	"mkdir"
#define OPH_FS_CMD_RM	"rm"
#define OPH_FS_CMD_MV	"mv"

#define OPH_FS_MODE_LS	0
#define OPH_FS_MODE_CD	1
#define OPH_FS_MODE_MD	2
#define OPH_FS_MODE_RM	3
#define OPH_FS_MODE_MV	4

#define OPH_FS_CD_MESSAGE "Current Data Directory"

/**
 * \brief Structure of parameters needed by the operator OPH_FS. It manages the Ophidia filesystem FSs.
 * \param mode : number corresponding to one of the provided commands
 * \param path : array of absolute or relative paths given as input
 * \param path_num : number of input paths
 * \param file : file filter
 * \param cwd : current working directory as an absolute path
 * \param user : user that has submitted the task
 * \param recursive : flag used to recursive search
 * \param depth : maximum folder depth has to be explored in case of recursion
 * \param realpath : flag used to list real path of files
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_FS_operator_handle {
	int mode;
	char **path;
	int path_num;
	char *file;
	char *cwd;
	char *user;
	int recursive;
	int depth;
	int realpath;
	char **objkeys;
	int objkeys_num;
} OPH_FS_operator_handle;


#endif				//__OPH_FS_OPERATOR_H
