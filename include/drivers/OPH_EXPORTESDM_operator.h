/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#ifndef __OPH_EXPORTESDM_OPERATOR_H
#define __OPH_EXPORTESDM_OPERATOR_H

//Operator specific headers
#include "oph_common.h"
#include "oph_ophidiadb_main.h"
#include "oph_ioserver_library.h"

struct _ESDM_dim {
	int dimid;
	char dimname[OPH_ODB_DIM_DIMENSION_SIZE + 1];
	char dimtype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1];
	int dimsize;
	int dimfkid;
	short int dimophlevel;	//Contains the oph_level of the dimension (explicit and implicit)
	short int dimexplicit;	// 1 if explicit, 0 if implicit dimension
	int dimfklabel;
	char dimunlimited;
};
typedef struct _ESDM_dim ESDM_dim;

/**
 * \brief Structure of parameters needed by the operator OPH_EXPORTESDM. It export as nc files a datacube_input
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param datacube_input Name of the input datacube to operate on 
 * \param id_input_datacube ID of the datacube to export
 * \param id_input_container ID of the input container related to datacube to export
 * \param output_name Filename for output files
 * \param export_metadata Flag to indicate if metadata has to be exported with data
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
 */
struct _OPH_EXPORTESDM_operator_handle {
	ophidiadb oDB;
	char *datacube_input;
	int id_input_datacube;
	int id_input_container;
	char *output_name;
	int export_metadata;
	char *fragment_ids;
	int total_fragment_number;
	int fragment_id_start_position;
	char *measure;
	char *measure_type;
	int cached_flag;
	int compressed;
	int num_of_dims;
	ESDM_dim *dims;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
};
typedef struct _OPH_EXPORTESDM_operator_handle OPH_EXPORTESDM_operator_handle;

#endif				//__OPH_EXPORTESDM_OPERATOR_H
