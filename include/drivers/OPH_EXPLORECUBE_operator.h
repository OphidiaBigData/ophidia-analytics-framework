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

#ifndef __OPH_EXPLORECUBE_OPERATOR_H
#define __OPH_EXPLORECUBE_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_subset_library.h"
#include "oph_ioserver_library.h"

#define OPH_EXPLORECUBE_TABLE_SPACING 10
#define OPH_EXPLORECUBE_ID "ID"
#define OPH_EXPLORECUBE_GETSUBARRAY_PLUGIN "oph_get_subarray2('oph_%s','oph_%s',%s,'%s')"

#define OPH_EXPLORECUBE_ISINSUBSET_PLUGIN "mysql.oph_is_in_subset(mysql.oph_id_to_index2(%s,%s),%lld,%lld,%lld)"
#define OPH_EXPLORECUBE_PLUGIN_COMPR "oph_get_subarray3('oph_%s', 'oph_%s', oph_uncompress('', '', %s), %s)"
#define OPH_EXPLORECUBE_PLUGIN_COMPR2 "oph_dump('oph_%s', '', oph_get_subarray3('oph_%s', 'oph_%s', oph_uncompress('', '', %s), %s), '%s')"
#define OPH_EXPLORECUBE_PLUGIN_COMPR3 "oph_dump('oph_%s', '', oph_uncompress('', '', %s), '%s')"
#define OPH_EXPLORECUBE_PLUGIN "oph_get_subarray3('oph_%s', 'oph_%s', %s, %s)"
#define OPH_EXPLORECUBE_PLUGIN2 "oph_dump('oph_%s', '', oph_get_subarray3('oph_%s', 'oph_%s', %s, %s), '%s')"
#define OPH_EXPLORECUBE_PLUGIN3 "oph_dump('oph_%s', '', %s, '%s')"

#define OPH_EXPLORECUBE_DECIMAL "decimal"
#define OPH_EXPLORECUBE_BASE64 "base64"

/**
 * \brief Structure of parameters needed by the operator OPH_EXPLORECUBE. It generate a cube by selecting a subset of measure values based on a subset string related to a dimension.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_dimension ID of the dimension which subset is performed along
 * \param number_of_dim Number of dimension
 * \param show_time Flag setted to 1 if the values of time dimensions have to be shown as string with date and time
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param level Verbose level, used to enable the print of dimension values
 * \param time_filter Flag used in case time filters are expressed as dates
 * \param base64 Flag used in representation of output data
 */
struct _OPH_EXPLORECUBE_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	char *task[OPH_SUBSET_LIB_MAX_DIM];
	char *dim_task[OPH_SUBSET_LIB_MAX_DIM];
	int id_dimension[OPH_SUBSET_LIB_MAX_DIM];
	int number_of_dim;
	char *where_clause;
	char *apply_clause;
	int limit;
	short int show_id;
	short int show_index;
	short int show_time;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int level;
	int time_filter;
	int base64;
};
typedef struct _OPH_EXPLORECUBE_operator_handle OPH_EXPLORECUBE_operator_handle;

#endif				//__OPH_EXPLORECUBE_OPERATOR_H
