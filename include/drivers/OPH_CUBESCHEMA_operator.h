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

#ifndef __OPH_CUBESCHEMA_OPERATOR_H
#define __OPH_CUBESCHEMA_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_CUBESCHEMA. It shows information about the dimensions related to the datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_container ID of the input container related to datacube
 * \param id_input_datacube ID of the input datacube
 * \param level Level of information shown
 * \param dimension_name Array of dimension to filter on
 * \param dimension_name_number Number of dimension to filter on
 * \param datacube_name Name of the datacube to filter on
 * \param show_index Flag setted to 1 if the dimensions ID has to be shown
 * \param show_time Flag setted to 1 if the values of time dimensions have to be shown as string with date and time
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 * \param base64 Flag used in representation of output data
 */
struct _OPH_CUBESCHEMA_operator_handle {
	ophidiadb oDB;
	int id_input_container;
	int id_input_datacube;
	int level;
	char **dimension_name;
	char *datacube_name;
	int dimension_name_number;
	short int show_index;
	short int show_time;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int base64;
};
typedef struct _OPH_CUBESCHEMA_operator_handle OPH_CUBESCHEMA_operator_handle;

#endif				//__OPH_CUBESCHEMA_OPERATOR_H
