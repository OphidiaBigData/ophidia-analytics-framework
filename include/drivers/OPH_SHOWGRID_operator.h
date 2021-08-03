/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#ifndef __OPH_SHOWGRID_OPERATOR_H
#define __OPH_SHOWGRID_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_SHOWGRID_TABLE_SIZE 98
#define OPH_SHOWGRID_HELP_MESSAGE "\nTo get additional information about a grid defined above use the following: \n OPH_TERM: oph_showgrid container=CONTAINERNAME;grid=GRIDNAME;\n SUBMISSION STRING: \"operator=oph_showgrid;container=CONTAINERNAME;grid=GRIDNAME;\"\n"

/**
 * \brief Structure of parameters needed by the operator OPH_SHOWGRID. It shows information about the grids related to a container.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_container ID of the input container
 * \param grid_name Name of the grid to filter on
 * \param cwd Current working directory
 * \param dimension_names Names of the dimensions to filter on
 * \param dimension_names_num Number of provided dimension names
 * \param show_index Flag setted to 1 if the dimensions ID has to be shown
 * \param user Username of the calling user
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_SHOWGRID_operator_handle {
	ophidiadb oDB;
	int id_input_container;
	char *grid_name;
	char *cwd;
	char **dimension_names;
	int dimension_names_num;
	short int show_index;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
};
typedef struct _OPH_SHOWGRID_operator_handle OPH_SHOWGRID_operator_handle;

#endif				//__OPH_SHOWGRID_OPERATOR_H
