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

#ifndef __OPH_INTERCUBE2_OPERATOR_H
#define __OPH_INTERCUBE2_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_INTERCUBE2_QUERY OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s")
#define OPH_INTERCUBE2_QUERY2 OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s")

#define OPH_INTERCUBE2_OP_SUM "oph_sum"
#define OPH_INTERCUBE2_OP_MUL "oph_mul"
#define OPH_INTERCUBE2_OP_AVG "oph_avg"
#define OPH_INTERCUBE2_OP_MAX "oph_max"
#define OPH_INTERCUBE2_OP_MIN "oph_min"
#define OPH_INTERCUBE2_OP_ARG_MAX "oph_arg_max"
#define OPH_INTERCUBE2_OP_ARG_MIN "oph_arg_min"

/**
 * \brief Structure of parameters needed by the operator OPH_INTERCUBE2. It executes an operation on two cubes materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube IDs of the input datacubes to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_output_datacube ID of the output datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param measure Name of the resulting measure
 * \param operation Type of operation to be executed
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param description Free description to be associated with output cube
 * \param ms Conventional value for missing values
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 * \param cubes Pointer to cubes
 * \param cubes_num Number of input cubes
 */
struct _OPH_INTERCUBE2_operator_handle {
	ophidiadb oDB;
	int *id_input_datacube;
	int id_input_container;
	int id_output_datacube;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	char *measure_type;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *measure;
	char *operation;
	char *sessionid;
	int id_user;
	char *description;
	double ms;
	short int execute_error;
	char **cubes;
	int cubes_num;
	char user_missing_value;
};
typedef struct _OPH_INTERCUBE2_operator_handle OPH_INTERCUBE2_operator_handle;

#endif				//__OPH_INTERCUBE2_OPERATOR_H
