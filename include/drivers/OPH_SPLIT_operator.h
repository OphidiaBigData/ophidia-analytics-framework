/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

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

#ifndef __OPH_SPLIT_OPERATOR_H
#define __OPH_SPLIT_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#ifdef OPH_DEBUG_MYSQL
#define OPH_SPLIT_QUERY_MYSQL "CREATE TABLE %s (%s INTEGER, %s LONGBLOB) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS (SELECT %s, %s FROM %s ORDER BY %s LIMIT %d, %d)"
#endif

#define OPH_SPLIT_QUERY OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s|%s")OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_ORDER, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "%d|%d")


/**
 * \brief Structure of parameters needed by the operator OPH_SPLIT. It duplicates a cube materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube ID of the input datacube to operate on
 * \param id_output_datacube ID of the input datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param description Free description to be associated with output cube
 * \param nthread Number of posix threads related to each MPI task
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 * \param output_path Folder where output cube has to be saved
 * \param cwd Absolute path where the container is
 * \param folder_id Id of output folder
 */
struct _OPH_SPLIT_operator_handle {
	ophidiadb oDB;
	int id_input_datacube;
	int id_output_datacube;
	int id_input_container;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	int split_number;
	int new_tuplexfragment;
	int new_remainder;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *sessionid;
	int id_user;
	char *description;
	unsigned int nthread;
	short int execute_error;
	char *output_path;
	char *cwd;
	int folder_id;
};
typedef struct _OPH_SPLIT_operator_handle OPH_SPLIT_operator_handle;

#endif				//__OPH_SPLIT_OPERATOR_H
