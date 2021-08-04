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

#ifndef __OPH_RANDCUBE2_OPERATOR_H
#define __OPH_RANDCUBE2_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

/**
 * \brief Structure of parameters needed by the operator OPH_RANDCUBE2. It creates a new datacube filling it with random data
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param container_input Name of the input container used
 * \param cwd Absolute path where the container is 
 * \param user Name of the user that wants to create the datacube
 * \param run Simulate the run of operator to compute distribution parameters
 * \param partition_input Name of the host partition used to store data
 * \param grid_name Name of the grid used to specify dimensions
 * \param id_output_datacube ID of the output datacube created
 * \param id_input_container ID of the output container used/created
 * \param ioserver_type Type of I/O server used
 * \param number_of_exp_dimensions Number of input explicit dimension 
 * \param number_of_imp_dimensions Number of input implicit dimension 
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param host_number Number of host to work on
 * \param fragxdb_number Number of fragments for each database (upper bound)
 * \param tuplexfrag_number Number of tuples for each fragment (upper bound)
 * \param array_length Number of elements to store into a row
 * \param measure Measure name
 * \param measure_type Type of data for the given measure
 * \param dimension_name Array of dimension names
 * \param dimension_size Array of sizes of dimensions
 * \param dimension_level Array of short name hierarchy concept levels
 * \param compressed If the data array has to be compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param description Free description to be associated with output cube
 * \param id_job ID of the job related to the task
 * \param rand_algo Type of algorithm used for generating random values
 * \param nthread Number of pthreads related to each MPI task
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 * \param policy Rule to select hosts where data will be distributed
 */
struct _OPH_RANDCUBE2_operator_handle {
	ophidiadb oDB;
	char *container_input;
	char *cwd;
	char *user;
	int run;
	char *partition_input;
	char *grid_name;
	char *ioserver_type;
	int id_output_datacube;
	int id_input_container;
	int number_of_exp_dimensions;
	int number_of_imp_dimensions;
	int schedule_algo;
	int fragment_number;
	int fragment_first_id;
	int host_number;
	int fragxdb_number;
	int tuplexfrag_number;
	long long array_length;
	char *measure;
	char *measure_type;
	char **dimension_name;
	long long *dimension_size;
	char *dimension_level;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	char *description;
	int id_job;
	char *rand_algo;
	int nthread;
	short int execute_error;
	char policy;
	int id_user;
};
typedef struct _OPH_RANDCUBE2_operator_handle OPH_RANDCUBE2_operator_handle;

#endif				//__OPH_RANDCUBE2_OPERATOR_H
