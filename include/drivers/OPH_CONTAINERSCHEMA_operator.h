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

#ifndef __OPH_CONTAINERSCHEMA_OPERATOR_H
#define __OPH_CONTAINERSCHEMA_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_CONTAINERSCHEMA. It removes a datacube from the system
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param container_input Name of the input container to be deleted
 * \param user Name of the user that wants to create the container
 * \param cwd Absolute path where the container is created 
 * \param id_input_container ID of the input container to delete
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_CONTAINERSCHEMA_operator_handle {
	ophidiadb oDB;
	char *cwd;
	char *user;
	int id_input_container;
	char *container_input;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_CONTAINERSCHEMA_operator_handle OPH_CONTAINERSCHEMA_operator_handle;

#endif				//__OPH_CONTAINERSCHEMA_OPERATOR_H
