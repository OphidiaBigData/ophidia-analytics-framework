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

#define OPH_FS_MODE_LS	0
#define OPH_FS_MODE_CD	1

#define OPH_FS_CD_MESSAGE "Current Data Directory"

/**
 * \brief Structure of parameters needed by the operator OPH_FS. It manages the Ophidia filesystem FSs.
 * \param mode : number corresponding to one of the provided commands
 * \param path : absolute or relative path given as input
 * \param cwd : current working directory as an absolute path
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_FS_operator_handle {
	int mode;
	char *path;
	char *cwd;
	char **objkeys;
	int objkeys_num;
} OPH_FS_operator_handle;


#endif				//__OPH_FS_OPERATOR_H
