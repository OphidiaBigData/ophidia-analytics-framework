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

#ifndef __OPH_APPLY_OPERATOR_H
#define __OPH_APPLY_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_APPLY_QUERY OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in")

/**
 * \brief Structure of parameters needed by the operator OPH_APPLY. It applies the array-query materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_container ID of the output container to operate on
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param array_operation Query to be executed on the input datacube 
 * \param dimension_operation Query to be executed on the implicit dimension of input datacube 
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param measure Name of output measure
 * \param measure_type Algorithm to set data type of output measure
 * \param dimension_type Algorithm to set dimension data type of output cube
 * \param compressed If the data array has to be compressed (1) or not (0)
 * \param expl_size Size of arrays
 * \param expl_size_update Flag used for post-update of the explicit dimension sizes
 * \param impl_size Size of arrays
 * \param impl_size_update Flag used for post-update of the implicit dimension sizes
 * \param operation_type Operation type: simple, reduce, aggregate
 * \param set_measure_type Flag used in automatic completion of primitive data types
 * \param set_dimension_type Flag used in automatic completion of primitive data types
 * \param check_measure_type Flag used to check the correctness of input and output data types
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param num_reference_to_dim Number of references to keyword 'dimension' in main query
 * \param array_values Pointer to value of main implicit dimension of input cube
 * \param description Free description to be associated with output cube
 */
struct _OPH_APPLY_operator_handle {
	ophidiadb oDB;
	int id_input_container;
	int id_input_datacube;
	int id_output_datacube;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	char *array_operation;
	char *dimension_operation;
	int fragment_number;
	int fragment_id_start_position;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *measure;
	char *measure_type;
	char *dimension_type;
	int compressed;
	int expl_size;
	int expl_size_update;
	int impl_size;
	int impl_size_update;
	int operation_type;
	int set_measure_type;
	int set_dimension_type;
	int check_measure_type;
	char *sessionid;
	int id_user;
	int num_reference_to_dim;
	char *array_values;
	long long array_length;
	char *description;
};
typedef struct _OPH_APPLY_operator_handle OPH_APPLY_operator_handle;

#endif				//__OPH_APPLY_OPERATOR_H
