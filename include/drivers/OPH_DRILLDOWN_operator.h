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

#ifndef __OPH_DRILLDOWN_OPERATOR_H
#define __OPH_DRILLDOWN_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_DRILLDOWN_PLUGIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_FUNCTION) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FUNC, "mysql.oph_drill_down") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_ARG, "'fact_in'|%d|%d|'oph_%s'|'fact_out'|%d")

#ifdef OPH_DEBUG_MYSQL
#define OPH_DRILLDOWN_PLUGIN2_MYSQL "CALL mysql.oph_drill_down('%s.%s',%d,%d,'oph_%s','%s',%d)"
#endif
#define OPH_DRILLDOWN_PLUGIN2 OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_FUNCTION) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FUNC, "mysql.oph_drill_down") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_ARG, "'%s.%s'|%d|%d|'oph_%s'|'%s'|%d")

/**
 * \brief Structure of parameters needed by the operator OPH_DRILLDOWN. It generate a cube materializing a new datacube with more explicit dimensions and less implicit dimensions
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param size Product of the sizes of explicit dimensions to be transformed into implitic dimensions
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param description Free description to be associated with output cube
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 */
struct _OPH_DRILLDOWN_operator_handle {
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
	int outer_size;
	int inner_size;
	char *measure_type;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_user;
	char *description;
	short int execute_error;
};
typedef struct _OPH_DRILLDOWN_operator_handle OPH_DRILLDOWN_operator_handle;

#endif				//__OPH_DRILLDOWN_OPERATOR_H
