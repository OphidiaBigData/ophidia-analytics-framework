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

#ifndef __OPH_METADATA_OPERATOR_H
#define __OPH_METADATA_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_METADATA_MODE_INSERT_VALUE    1
#define OPH_METADATA_MODE_READ_VALUE      2
#define OPH_METADATA_MODE_UPDATE_VALUE    3
#define OPH_METADATA_MODE_DELETE_VALUE    4
#define OPH_METADATA_MODE_INSERT "insert"
#define OPH_METADATA_MODE_READ   "read"
#define OPH_METADATA_MODE_UPDATE "update"
#define OPH_METADATA_MODE_DELETE "delete"

/**
 * \brief Structure of parameters needed by the operator OPH_METADATA. It provides CRUD operations on OphidiaDB metadata.
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param mode : set the appropriate operation among these:
                - (1) insert : insertion of new metadata (only 1)
                - (2) read (default) : visualization of the values of all requested metadata
                - (3) update : change the value of specified metadata (only 1)
                - (4) delete : remove the values of all specified metadata
 * \param id_datacube_input : pid of the specific datacube related to requested metadata.
 * \param force : flag used to force update or deletion of functional metadata associated to a vocabulary.
 * \param id_container_input : id of the specific container related to requested metadata.
 * \param metadata_keys : names of the keys identifying requested metadata.
 * \param metadata_keys_num : number of keys identifying requested metadata.
 * \param variable : variable associated to the metadata key in insert mode
 * \param metadata_id : id of the particular metadata instance to interact with.
 * \param metadata_id_str : string representation of metadata id
 * \param metadata_type : type of the to-be-inserted metadata.
 * \param metadata_value : string value to be assigned to specified metadata.
 * \param metadata_variable_filter : optional filter on variable name of returned metadata valid in read/delete mode only.
 * \param metadata_type_filter : optional filter on the type of returned metadata valid in read/delete mode only.
 * \param metadata_value_filter : optional filter on the value of returned metadata valid in read/delete mode only.
 * \param user : username of the user managing metadata.
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 * \param userrole User role
 */
typedef struct _OPH_METADATA_operator_handle {
	ophidiadb oDB;
	int mode;
	int force;
	int id_datacube_input;
	int id_container_input;
	char **metadata_keys;
	int metadata_keys_num;
	char *variable;
	int metadata_id;
	char *metadata_id_str;
	char *metadata_type;
	char *metadata_value;
	char *variable_filter;
	char *metadata_type_filter;
	char *metadata_value_filter;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int userrole;
} OPH_METADATA_operator_handle;

#endif				//__OPH_METADATA_OPERATOR_H
