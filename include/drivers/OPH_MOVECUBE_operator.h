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

#ifndef __OPH_MOVECUBE_OPERATOR_H
#define __OPH_MOVECUBE_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_MOVECUBE. It moves cubes
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_input_datacube ID of the input container to operate on
 * \param path_input Path of the input path to be moved
 * \param path_output Path of the output path to be moved
 * \param user Name of the user that wants to move the cube
 * \param cwd Absolute path of working directory 
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_MOVECUBE_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	char *cwd;
	char *user;
	char *path_input;
	char *path_output;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_MOVECUBE_operator_handle OPH_MOVECUBE_operator_handle;

#endif				//__OPH_MOVECUBE_OPERATOR_H

