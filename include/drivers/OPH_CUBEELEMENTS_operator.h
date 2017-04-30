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

#ifndef __OPH_CUBEELEMENTS_OPERATOR_H
#define __OPH_CUBEELEMENTS_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_CUBEELEMENTS_COUNT_ALGORITHM          "count"
#define OPH_CUBEELEMENTS_PRODUCT_ALGORITHM        "dim_product"
#define OPH_CUBEELEMENTS_COUNT_ALGORITHM_VALUE    1
#define OPH_CUBEELEMENTS_PRODUCT_ALGORITHM_VALUE  2

/**
 * \brief Structure of parameters needed by the operator OPH_CUBEELEMENTS. It computes the number of elements stored in the input datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube
 * \param id_input_container ID of the input container related to datacube
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param measure_type Type of data retrieved from the input datacube
 * \param partial_count Number of elements counted by a single process
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param first_time_computation If the number of elements hasn't been already computed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 */
struct _OPH_CUBEELEMENTS_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	char *measure_type;
	long long partial_count;
	int compressed;
	int first_time_computation;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
};
typedef struct _OPH_CUBEELEMENTS_operator_handle OPH_CUBEELEMENTS_operator_handle;

#endif				//__OPH_CUBEELEMENTS_OPERATOR_H
