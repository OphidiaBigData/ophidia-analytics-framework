/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

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

#ifndef __OPH_EXPORTNC_OPERATOR_H
#define __OPH_EXPORTNC_OPERATOR_H

//Operator specific headers
#include "oph_common.h"
#include "oph_ophidiadb_main.h"
#include "oph_nc_library.h"
#include "oph_ioserver_library.h"

#define OPH_EXPORTNC_OUTPUT_FILE_EXT ".nc"
#define OPH_EXPORTNC_OUTPUT_FILE "%s_%d"OPH_EXPORTNC_OUTPUT_FILE_EXT
#define OPH_EXPORTNC_OUTPUT_PATH "%s/"
#define OPH_EXPORTNC_OUTPUT_PATH_SUMMARY OPH_EXPORTNC_OUTPUT_PATH"%s.html"
#define OPH_EXPORTNC_OUTPUT_PATH_SINGLE_FILE OPH_EXPORTNC_OUTPUT_PATH"%s%s"
#define OPH_EXPORTNC_OUTPUT_PATH_MORE_FILES OPH_EXPORTNC_OUTPUT_PATH""OPH_EXPORTNC_OUTPUT_FILE

/**
 * \brief Structure of parameters needed by the operator OPH_EXPORTNC. It export as nc files a datacube_input
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param datacube_input Name of the input datacube to operate on 
 * \param id_input_datacube ID of the datacube to export
 * \param id_input_container ID of the input container related to datacube to export
 * \param output_path Name of the output path in prefix to save files
 * \param output_path_user Name of the output path set by the user
 * \param output_path_user_defined 1 in case of a user-defined output path
 * \param output_link URL to file
 * \param output_name Filename for output files
 * \param export_metadata Flag to indicate if metadata has to be exported with data
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param total_fragment_number Number of total fragments
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param measure Name of the measured variable
 * \param measure_type Type of data retrieved from the input datacube
 * \param cached_flag Set to one if the cuboid has already been exported in the cache
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param force Flag used to force file creation
 * \param misc Flag used to save file in export/misc folder
 * \param is_zarr Flag set in case of Zarr output
 */
struct _OPH_EXPORTNC_operator_handle {
	ophidiadb oDB;
	char *datacube_input;
	int id_input_datacube;
	int id_input_container;
	char *output_path;
	char *output_path_user;
	int output_path_user_defined;
	char *output_link;
	char *output_name;
	int export_metadata;
	int schedule_algo;
	char *fragment_ids;
	int total_fragment_number;
	int fragment_id_start_position;
	char *measure;
	char *measure_type;
	int cached_flag;
	int compressed;
	int num_of_dims;
	NETCDF_dim *dims;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int force;
	int misc;
	char shuffle;
	char deflate;
	char is_zarr;
#ifdef OPH_ZARR
	void *dlh;
#endif
};
typedef struct _OPH_EXPORTNC_operator_handle OPH_EXPORTNC_operator_handle;

#endif				//__OPH_EXPORTNC_OPERATOR_H
