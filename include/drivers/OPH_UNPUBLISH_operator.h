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

#ifndef __OPH_UNPUBLISH_OPERATOR_H
#define __OPH_UNPUBLISH_OPERATOR_H

#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_UNPUBLISH. It deletes the files (and folders) of the published cuboid
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param datacube_path Name of the path of the datacube to be deleted
 * \param id_input_container ID of the input container related to datacube to delete
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_UNPUBLISH_operator_handle {
	ophidiadb oDB;
	char *datacube_path;
	int id_input_container;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_UNPUBLISH_operator_handle OPH_UNPUBLISH_operator_handle;

#endif				//__OPH_UNPUBLISH_OPERATOR_H
