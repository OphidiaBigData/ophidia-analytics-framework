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

#ifndef __OPH_PUBLISH_OPERATOR_H
#define __OPH_PUBLISH_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_PUBLISH_MAX_DIMENSION 64

#define OPH_PUBLISH_INT -1
#define OPH_PUBLISH_DOUBLE 0
#define OPH_PUBLISH_FLOAT 1
#define OPH_PUBLISH_LONG 2
#define OPH_PUBLISH_TIME 3
#define OPH_PUBLISH_SHORT 4
#define OPH_PUBLISH_BYTE 5

#define OPH_PUBLISH_PUBLISH_MAPS		"MAPS"
#define OPH_PUBLISH_PUBLISH_DATA		"DATA"
#define OPH_PUBLISH_PUBLISH_METADATA		"METADATA"
#define OPH_PUBLISH_PUBLISH_NCLINK		"NCLINK"
#define OPH_PUBLISH_PUBLISH_MAPS_DATA		"MAPS_DATA"
#define OPH_PUBLISH_PUBLISH_MAPS_NCLINK	"MAPS_NCLINK"
#define OPH_PUBLISH_PUBLISH_DATA_NCLINK	"DATA_NCLINK"
#define OPH_PUBLISH_PUBLISH_MAPS_DATA_NCLINK	"MAPS_DATA_NCLINK"

#define OPH_PUBLISH_FILE "%s/page%d.html"
#define OPH_PUBLISH_PAGE "page%d.html"

/**
 * \brief Structure of parameters needed by the operator OPH_PUBLISH. It publishes maps,data,nc-file links of a datacube_input according to a predefined macro
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the datacube to publish
 * \param id_input_container ID of the container with the datacube to publish
 * \param input_path Name of the input path for maps, nc files...
 * \param output_path Name of the output path in prefix to save html files
 * \param output_link URL to the main output HTML page
 * \param map_list List of map filenames to work on
 * \param total_maps_number Total number of maps to publish
 * \param map_num_start_position First map to work on
 * \param map_nums Number of maps per process
 * \param cached_flag Set to one if the cuboid has already been published in the cache
 * \param datacube_input Name of the input datacube to operate on
 * \param schedule_algo Number of the distribution algorithm to use
 * \param publish_metadata Flag to indicate if metadata has to be published with data
 * \param fragment_ids Contains the string of fragment relative index
 * \param total_fragment_number Number of total fragments
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param measure_type Type of data retrieved from the input datacube
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param macro Macro used to select necessary info to be published
 * \param var Name of the variable to work on
 * \param dim_num Number of dimension related to datacube (NOTE: it can be at most equl to OPH_PUBLISH_MAX_DIMENSION)
 * \param dim_sizes Array of dimension sizes
 * \param dim_ids Array of dimension IDs
 * \param dim_oph_types Array of oph types
 * \param dim_fk_ids Array of dimension foreign keys
 * \param show_id Flag setted to 1 if the fragment row ID has to be shown
 * \param show_index Flag setted to 1 if the dimensions ID has to be shown
 * \param show_time Flag setted to 1 if the values of time dimensions have to be shown as string with date and time
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 */
struct _OPH_PUBLISH_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	char *input_path;
	char *output_path;
	char *output_link;
	char **map_list;
	int total_maps_number;
	int map_num_start_position;
	int map_nums;
	int cached_flag;
	char *datacube_input;
	int publish_metadata;
	int schedule_algo;
	char *fragment_ids;
	int total_fragment_number;
	int fragment_id_start_position;
	char *measure_type;
	int compressed;
	char *macro;
	char *var;
	//Used to support dimensions
	int dim_num;
	int *dim_sizes;
	int *dim_ids;
	int *dim_oph_types;
	int *dim_fk_ids;
	int *dim_fk_labels;
	int *dim_types;
	short int show_id;
	short int show_index;
	short int show_time;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
};
typedef struct _OPH_PUBLISH_operator_handle OPH_PUBLISH_operator_handle;


#endif				//__OPH_PUBLISH_OPERATOR_H
