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

#ifndef __OPH_CREATECONTAINER_OPERATOR_H
#define __OPH_CREATECONTAINER_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator OPH_CREATECONTAINER. 
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param container_output Name of the output container to create (if it doesn't exists)
 * \param cwd Absolute path where the container is created
 * \param user Name of the user that wants to create the container
 * \param id_output_container ID of the output container used/created
 * \param number_of_dimensions Number of input dimensions
 * \param dimension_name Array of dimension names
 * \param dimension_type Array of data types of dimensions
 * \param id_dimension_hierarchy Array of id concept hierarchies of dimensions
 * \param compressed If the data array has to be compressed (1) or not (0)
 * \param id_vocabulary ID of the vocabulary used for metadata
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 * \param base_time Base time in case of time dimension
 * \param units Units of dimension time
 * \param calendar Calendar associated to a time dimension
 * \param month_lengths Month lengths of each year
 * \param leap_year Value of the first leap year
 * \param leap_month Value of the leap month
 */
struct _OPH_CREATECONTAINER_operator_handle {
	ophidiadb oDB;
	char *container_output;
	char *user;
	int id_output_container;
	int number_of_dimensions;
	char **dimension_name;
	char **dimension_type;
	char *cwd;
	int *id_dimension_hierarchy;
	int compressed;
	int id_vocabulary;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	char *base_time;
	char *units;
	char *calendar;
	char *month_lengths;
	int leap_year;
	int leap_month;
};
typedef struct _OPH_CREATECONTAINER_operator_handle OPH_CREATECONTAINER_operator_handle;

#endif				//__OPH_CREATECONTAINER_OPERATOR_H
