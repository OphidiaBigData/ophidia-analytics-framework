/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#ifndef __OPH_IMPORTFITS_OPERATOR_H
#define __OPH_IMPORTFITS_OPERATOR_H

//Operator specific headers
#include "oph_common.h"
#include "oph_ophidiadb_main.h"
#include "oph_fits_library.h"
#include "oph_ioserver_library.h"

#define OPH_IMPORTFITS_SUBSET_INDEX	    "index"
#define OPH_IMPORTFITS_SUBSET_COORD	    "coord"
#define OPH_IMPORTFITS_DIMENSION_DEFAULT	"auto"



/**
 * \brief Structure of parameters needed by the operator OPH_IMPORTFITS. It creates a new datacube filling it with data taken from fits file
 *
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param container_input Name of the input container used
 * \param create_container Flag indicating if the container has to be created
 * \param cwd Absolute path where the container is
 * \param run Simulate the run of operator to compute distribution parameters
 * \param fits_file_path Path of the fits file to import
 * \param partition_input Name of the host partition used to store data
 * \param grid_name Name of the grid used to specify dimensions - Not used
 * \param ioserver_type Type of I/O server used
 * \param id_output_datacube ID of the output datacube created
 * \param id_input_container ID of the output container used/created
 * \param import_metadata Flag to indicate if metadata has to be imported with data
 * \param check_compliance Flag to indicate if compliance with reference vocabulary has to be checked - Not used
 * \param schedule_algo Number of the distribution algorithm to use
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param host_number Number of host to work on
 * \param fragxdb_number Number of fragments for each database (upper bound)
 * \param tuplexfrag_number Number of tuples for each fragment (upper bound)
 * \param array_length Number of elements to store into a row
 * \param user Name of the user calling the import operation
 * \param measure Measure name
 * \param measure_type Type of data for the given measure
 * \param hdu Number of the HDU considered for importing data
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_vocabulary ID of the vocabulary used for metadata - Not used
 * \param id_dimension_hierarchy Array of id concept hierarchies of dimensions - Not used
 * \param base_time Base time in case of time dimension - Not used
 * \param units Units of dimension time - Not used
 * \param calendar Calendar associated to a time dimension - Not used
 * \param month_lengths Month lengths of each year - Not used
 * \param leap_year Value of the first leap year - Not used
 * \param leap_month Value of the leap month - Not used
 * \param memory_size Maximum amount of memory available
 * \param description Free description to be associated with output cube
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 * \param policy Rule to select hosts where data will be distributed
 */
struct _OPH_IMPORTFITS_operator_handle {
	ophidiadb oDB;
	char *container_input;
	int create_container;
	char *cwd;
	int run;
	char *fits_file_path;
	char *fits_file_path_orig;
	int hdu;		/* Set the data HDU; default is the primary HDU (0) */
	char *partition_input;
	char *grid_name;
	char check_grid;
	char *ioserver_type;
	int id_output_datacube;
	int id_input_container;
	int import_metadata;
	int check_compliance;
	int schedule_algo;
	int fragment_number;
	int fragment_first_id;
	int host_number;
	int fragxdb_number;
	int tuplexfrag_number;
	int array_length;
	int total_frag_number;
	char *user;
	fitsfile *fptr;		/* FITS file pointer, defined in fitsio.h */
	int fitsid;
	FITS_var measure;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_vocabulary;
	int *id_dimension_hierarchy;
/* For fits files variables related to time management are not used */
	char *base_time;
	char *units;
	char *calendar;
	char *month_lengths;
	int leap_year;
	int leap_month;

	long long memory_size;
	char *description;
	short int execute_error;
	char policy;
};
typedef struct _OPH_IMPORTFITS_operator_handle OPH_IMPORTFITS_operator_handle;

#endif				//__OPH_IMPORTFITS_OPERATOR_H
