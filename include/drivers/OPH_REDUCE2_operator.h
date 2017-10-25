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

#ifndef __OPH_REDUCE2_OPERATOR_H
#define __OPH_REDUCE2_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_REDUCE2_QUERY_COMPR OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s|oph_compress('','',oph_reduce3('oph_%s','oph_%s',oph_uncompress('','',%s),'oph_%s',oph_to_bin('','oph_%s','%s'),%lld,%d,%f,%s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in")
#define OPH_REDUCE2_QUERY OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s|oph_reduce3('oph_%s','oph_%s',%s,'oph_%s',oph_to_bin('','oph_%s','%s'),%lld,%d,%f,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in")
#define OPH_REDUCE2_PLUGIN_COMPR "oph_compress('','',oph_reduce3('oph_%s','oph_%s',oph_uncompress('','',%s),'oph_%s',?,%lld,%d,%f,%s))"
#define OPH_REDUCE2_PLUGIN "oph_reduce3('oph_%s','oph_%s',%s,'oph_%s',?,%lld,%d,%f,%s)"

/**
 * \brief Structure of parameters needed by the operator OPH_REDUCE2. It generate a cube by aggregating more values along one or more implicit dimensions.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param operation Aggregation operation
 * \param dimension_name Name of the implicit dimension to be reduced
 * \param dimension_level Code of the new concept level
 * \param measure_type Data type of the measure to be aggregated
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param midnight Flag related to mode for edge aggregation
 * \param id_user ID of submitter
 * \param order Order to be used in case of evaluation of the moments
 * \param description Free description to be associated with output cube
 * \param ms Conventional value for missing values
 */
struct _OPH_REDUCE2_operator_handle {
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
	char *operation;
	int size;
	char *measure_type;
	int compressed;
	char *grid_name;
	char *dimension_name;
	char *dimension_level;
	long long block_size;
	char *sizes;
	int size_num;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int midnight;
	int id_user;
	double order;
	char *description;
	double ms;
};
typedef struct _OPH_REDUCE2_operator_handle OPH_REDUCE2_operator_handle;

#endif				//__OPH_REDUCE2_OPERATOR_H
