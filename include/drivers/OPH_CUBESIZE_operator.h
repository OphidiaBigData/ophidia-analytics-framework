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

#ifndef __OPH_CUBESIZE_OPERATOR_H
#define __OPH_CUBESIZE_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

/**
 * \brief Structure of parameters needed by the operator OPH_CUBESIZE. It computes the number of elements stored in the input datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube
 * \param id_input_container ID of the input container related to datacube
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param dbms_number Number of dbms that a process has to manage
 * \param dbms_id_start_position First dbms in dbmsinstance set to work on
 * \param frag_on_dbms_number Number of fragments in the DBMS that a process has to manage (it's 0 if the process work on more than one DBMS)
 * \param frag_on_dbms_start_position First fragmente in the fragment per DBMS set to work on (it's 0 if the process work on more than one DBMS)
 * \param partial_size Size computed by a single process
 * \param first_time_computation If the number of elements hasn't been already computed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 */
struct _OPH_CUBESIZE_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_input_container;
	int byte_unit;
	int schedule_algo;
	int dbms_number;
	int dbms_id_start_position;
	int frag_on_dbms_number;
	int frag_on_dbms_start_position;
	long long partial_size;
	int first_time_computation;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
};
typedef struct _OPH_CUBESIZE_operator_handle OPH_CUBESIZE_operator_handle;

#endif				//__OPH_CUBESIZE_OPERATOR_H
