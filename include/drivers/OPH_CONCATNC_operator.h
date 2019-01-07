/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

#ifndef __OPH_CONCATNC_OPERATOR_H
#define __OPH_CONCATNC_OPERATOR_H

//Operator specific headers
#include "oph_common.h"
#include "oph_ophidiadb_main.h"
#include "oph_nc_library.h"
#include "oph_ioserver_library.h"

#define OPH_CONCATNC_SUBSET_INDEX	"index"
#define OPH_CONCATNC_SUBSET_COORD	"coord"
#define OPH_CONCATNC_DIMENSION_DEFAULT	"auto"

//Only import of measured variables is supported

/**
 * \brief Structure of parameters needed by the operator OPH_CONCATNC. It creates a new datacube concatenating data taken from nc file to a cube
 *
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube used
 * \param nc_file_path Path of the nc file to import
 * \param nc_file_path_orig Original value of nc_file_path
 * \param grid_name Name of the grid used to specify dimensions
 * \param id_output_datacube ID of the output datacube created
 * \param id_input_container ID of the output container used/created
 * \param schedule_algo Number of the distribution algorithm to use
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param ncid ID of the netcdf file read
 * \param user Name of the user calling the import operation
 * \param measure Measure name
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param check_exp_dim If explicit dimensions of cube and nc file have to be compared (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param memory_size Maximum amount of memory available
 * \param description Free description to be associated with output cube
 * \param time_filter Flag used in case time filters are expressed as dates
 * \param dim_offset Offset to be added to dimension values of imported data
 * \param dim_continue If enabled the last value of implicit dimension of input cube is used to evaluate the new values of the dimension.
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 */
struct _OPH_CONCATNC_operator_handle {
	ophidiadb oDB;
	char *nc_file_path;
	char *nc_file_path_orig;
	char *grid_name;
	int id_input_datacube;
	int id_output_datacube;
	int id_input_container;
	int schedule_algo;
	int fragment_number;
	int fragment_id_start_position;
	int total_frag_number;
	char *user;
	int ncid;
	int compressed;
	int id_job;
	char *fragment_ids;
	int check_exp_dim;
	NETCDF_var measure;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_user;
	long long memory_size;
	char *description;
	int time_filter;
	double *dim_offset;
	char dim_continue;
	short int execute_error;
};
typedef struct _OPH_CONCATNC_operator_handle OPH_CONCATNC_operator_handle;

#endif				//__OPH_CONCATNC_OPERATOR_H
