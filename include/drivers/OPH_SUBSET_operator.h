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

#ifndef __OPH_SUBSET_OPERATOR_H
#define __OPH_SUBSET_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_subset_library.h"
#include "oph_ioserver_library.h"

#define OPH_SUBSET_ISINSUBSET_PLUGIN "mysql.oph_is_in_subset(mysql.oph_id_to_index2(%s,%s),%lld,%lld,%lld)"
#define OPH_SUBSET_PLUGIN_COMPR "oph_get_subarray3('oph_%s','oph_%s',oph_uncompress('','',%s),%s)"
#define OPH_SUBSET_PLUGIN_COMPR2 "oph_compress('','',oph_get_subarray3(%s,oph_uncompress('','',%s),%s))"
#define OPH_SUBSET_PLUGIN2 "oph_get_subarray3(%s,%s,%s)"
#define OPH_SUBSET_PLUGIN "oph_get_subarray3('oph_%s','oph_%s',%s,%s)"

#define OPH_SUBSET_QUERY OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_FUNCTION) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FUNC, "mysql.oph_subset") OPH_IOSERVER_SQ_BLOCK (OPH_IOSERVER_SQ_ARG_ARG, "'fact_in'|%s|\"%s\"|'fact_out'|\"%s\"")

#ifdef OPH_DEBUG_MYSQL
#define OPH_SUBSET_QUERY2_MYSQL "CALL mysql.oph_subset('%s.%s',%d,\"%s\",'%s.%s',\"%s\");"
#endif

#define OPH_SUBSET_QUERY2 OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_FUNCTION) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FUNC, "mysql.oph_subset") OPH_IOSERVER_SQ_BLOCK (OPH_IOSERVER_SQ_ARG_ARG, "'%s.%s'|%d|\"%s\"|'%s.%s'|\"%s\"")

#define OPH_SUBSET_TYPE_INDEX "index"
#define OPH_SUBSET_TYPE_COORD "coord"

/**
 * \brief Structure of parameters needed by the operator OPH_SUBSET. It generate a cube by selecting a subset of measure values based on a subset string related to a dimension.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param tuplexfrag_number Number of tuples within each fragment
 * \param subset Subset string
 * \param id_dimension ID of the dimension which subset is performed along
 * \param measure_type Data type of the measure
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param explicited If the selected dimension is explicit
 * \param dim_size Vector of dimension sizes
 * \param dim_number Size of vector size
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param time_filter Flag used in case time filters are expressed as dates
 * \param description Free description to be associated with output cube
 * \param offset List of offsets used to enlarge subset intervals
 * \param offset_num Number of offsets
 * \param subset_type Flag indicating whether filters are expressed as indexes or values
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 */
struct _OPH_SUBSET_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	int id_output_datacube;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	char *task[OPH_SUBSET_LIB_MAX_DIM];
	char *dim_task[OPH_SUBSET_LIB_MAX_DIM];
	int id_dimension[OPH_SUBSET_LIB_MAX_DIM];
	int compressed;
	int explicited[OPH_SUBSET_LIB_MAX_DIM];
	int *keys;
	int number_of_dim;
	int frags_size;
	char *where_clause;
	char *apply_clause;
	char *apply_clause_type;
	char *grid_name;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_user;
	int time_filter;
	char *description;
	double *offset;
	int offset_num;
	int subset_type;
	short int execute_error;
};
typedef struct _OPH_SUBSET_operator_handle OPH_SUBSET_operator_handle;

#endif				//__OPH_SUBSET_OPERATOR_H
