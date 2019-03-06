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

#ifndef __OPH_SEARCH_OPERATOR_H
#define __OPH_SEARCH_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"


/**
 * \brief Structure of parameters needed by the operator OPH_SEARCH. It provides enhanced search on OphidiaDB metadata.
 * \param oDB : Contains the parameters and the connection to OphidiaDB
 * \param metadata_key_filter : one or more filters on metadata keys.
 * \param metadata_key_filter_num : number of filters on metadata keys.
 * \param metadata_value_filter : one or more filters on metadata values.
 * \param metadata_value_filter_num : number of filters on metadata values.
 * \param path : absolute/relative path used as starting point of the recursive search.
 * \param cwd : current working directory.
 * \param user : username of the user searching metadata.
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 */
struct _OPH_SEARCH_operator_handle {
	ophidiadb oDB;
	char **metadata_key_filter;
	int metadata_key_filter_num;
	char **metadata_value_filter;
	int metadata_value_filter_num;
	char *path;
	char *cwd;
	char *user;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int recursive_search;
};
typedef struct _OPH_SEARCH_operator_handle OPH_SEARCH_operator_handle;

/* OPERATOR MYSQL QUERIES */
#define MYSQL_QUERY_OPH_SEARCH_READ_SUBFOLDERS "SELECT idfolder,foldername FROM folder WHERE idparent=%d"
#define MYSQL_QUERY_OPH_SEARCH_READ_INSTANCES "SELECT idcontainer AS Container,datacube.iddatacube AS Datacube,metadatainstance.label AS 'Key',metadatainstance.value AS Value FROM metadatainstance,datacube WHERE datacube.iddatacube=metadatainstance.iddatacube AND idfolder=%d %s ORDER BY Datacube,'Key',Value"
#ifdef OPH_ODB_MNG
#define MONGODB_QUERY_OPH_SEARCH_READ_INSTANCES "SELECT idcontainer AS Container,datacube.iddatacube AS Datacube,NULL AS 'Key',NULL AS Value FROM datacube WHERE idfolder=%d ORDER BY Datacube"
#endif

#endif				//__OPH_SEARCH_OPERATOR_H
