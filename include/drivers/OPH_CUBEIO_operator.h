/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#ifndef __OPH_CUBEIO_OPERATOR_H
#define __OPH_CUBEIO_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_CUBEIO_BRANCH_BOTH 0
#define OPH_CUBEIO_BRANCH_IN 1
#define OPH_CUBEIO_BRANCH_OUT 2
#define OPH_CUBEIO_PARENT_VALUE "parent"
#define OPH_CUBEIO_CHILDREN_VALUE "children"

/**
 * \brief Structure of parameters needed by the operator OPH_CUBEIO. It shows information about datacubes used to generate the input datacube and derived from that
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param datacube_name Name of the datacube to filter on
 * \param id_input_container ID of the container related to input datacube
 * \param id_input_datacube ID of the input datacube
 * \param direction Filter the direction of the hierarchy to show: in (1) shows the parent datacubes, out (2) shows the children datacubes, all (0) shows both the directions
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_CUBEIO_operator_handle {
	ophidiadb oDB;
	char *datacube_name;
	int id_input_container;
	int id_input_datacube;
	int direction;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_CUBEIO_operator_handle OPH_CUBEIO_operator_handle;

#endif				//__OPH_CUBEIO_OPERATOR_H
