/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2022 CMCC Foundation

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

#ifndef __OPH_MERGECUBES2_OPERATOR_H
#define __OPH_MERGECUBES2_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#ifdef OPH_DEBUG_MYSQL
#define OPH_MERGECUBES2_QUERY2_COMPR_MYSQL "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress('','',oph_interlace('oph_%s|oph_%s','oph_%s,oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s))) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_MERGECUBES2_QUERY2_MYSQL "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_interlace('oph_%s|oph_%s','oph_%s,oph_%s',%s.%s,%s.%s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#endif

//Define dynamic query building blocks
#define OPH_MERGECUBES2_QUERY_OPERATION OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s")

#define OPH_MERGECUBES2_QUERY_SELECT  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s")
#define OPH_MERGECUBES2_ARG_SELECT			"frag%d.%s|oph_interlace('%s','%s',%s)"
#define OPH_MERGECUBES2_ARG_SELECT_CMPR			"frag%d.%s|oph_compress('','',oph_interlace('%s','%s',%s))"
#define OPH_MERGECUBES2_ARG_SELECT_TYPE			"oph_%s"
#define OPH_MERGECUBES2_ARG_SELECT_PART			"frag%d.%s"
#define OPH_MERGECUBES2_ARG_SELECT_PART_CMPR		"oph_uncompress('','',frag%d.%s)"
#define OPH_MERGECUBES2_ARG_SELECT_SEPARATOR		","
#define OPH_MERGECUBES2_ARG_SELECT_INTYPE_SEPARATOR	"|"
#define OPH_MERGECUBES2_ARG_SELECT_OUTTYPE_SEPARATOR	","

#define OPH_MERGECUBES2_QUERY_ALIAS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s")

#define OPH_MERGECUBES2_QUERY_FROM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s")
#define OPH_MERGECUBES2_ARG_FROM_PART		"%s.%s"
#define OPH_MERGECUBES2_ARG_FROM_SEPARATOR	"|"

#define OPH_MERGECUBES2_QUERY_FROM_ALIAS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s")
#define OPH_MERGECUBES2_ARG_FROM_ALIAS_PART	"frag%d"
#define OPH_MERGECUBES2_ARG_FROM_ALIAS_SEPARATOR	"|"

#define OPH_MERGECUBES2_QUERY_WHERE OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s")
#define OPH_MERGECUBES2_ARG_WHERE_PART		"frag%d.%s = frag%d.%s"
#define OPH_MERGECUBES2_ARG_WHERE_SEPARATOR	" AND "

/**
 * \brief Structure of parameters needed by the operator OPH_MERGECUBES2. It merges n cubes materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube IDs of the input datacubes to operate on
 * \param id_input_container IDs of the output container to operate on
 * \param id_output_datacube ID of the output datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param input_datacube_num Number of input datacube
 * \param measure_type Array with measure types of input datacube
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param description Free description to be associated with output cube
 * \param dim_name Name of new dimension to be created
 * \param dim_type Type of new dimension to be created
 * \param number Number of replies of the first cube to be considered
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 */
struct _OPH_MERGECUBES2_operator_handle {
	ophidiadb oDB;
	int *id_input_datacube;
	int *id_input_container;
	int id_output_datacube;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	int input_datacube_num;
	char **measure_type;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_user;
	char *description;
	char *dim_name;
	char *dim_type;
	int number;
	short int execute_error;
};
typedef struct _OPH_MERGECUBES2_operator_handle OPH_MERGECUBES2_operator_handle;

#endif				//__OPH_MERGECUBES2_OPERATOR_H
