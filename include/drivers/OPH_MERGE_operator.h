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

#ifndef __OPH_MERGE_OPERATOR_H
#define __OPH_MERGE_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

/**
 * \brief Structure of parameters needed by the operator OPH_MERGE. It merges a cube materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_container ID of the output container to operate on
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param merge_number Number of fragments to be merged together
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param input_fragment_ids Contains the string of input fragment relative index
 * \param output_fragment_ids Contains the string of output fragment relative index
 * \param input_fragment_number Number of fragments that a process has to manage in input
 * \param input_fragment_id_start_position First input fragment in the relative index set to work on 
 * \param output_fragment_number Number of fragments that a process has to manage in output
 * \param output_fragment_id_start_position First output fragment in the relative index set to work on
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 */
struct _OPH_MERGE_operator_handle
{
  ophidiadb oDB;
  int id_input_container;
  int id_input_datacube;
  int id_output_datacube;
  int id_output_container;
  int id_job;
  int merge_number;
  int schedule_algo;
  char* output_fragment_ids;
  char* input_fragment_ids;
  int input_fragment_number;
  int input_fragment_id_start_position;
  int output_fragment_number;
  int output_fragment_id_start_position;
  char **objkeys;
  int objkeys_num;
  oph_ioserver_handler *server;
  char *sessionid;
  int id_user;
};
typedef struct _OPH_MERGE_operator_handle OPH_MERGE_operator_handle;

#endif  //__OPH_MERGE_OPERATOR_H
