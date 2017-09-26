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

#define _GNU_SOURCE

#include "oph_driver_procedure_library.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "debug.h"

#include "oph_log_error_codes.h"

#include "oph_datacube_library.h"
#include "oph_ioserver_library.h"
#include "oph_analytics_operator_library.h"

extern int msglevel;

int oph_dproc_delete_data(int id_datacube, int id_container, char *fragment_ids)
{
	if (!id_datacube || !fragment_ids) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int i, j, k;

	oph_ioserver_handler *server = NULL;
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	int datacubexdb_number = 0;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONFIGURATION_FILE);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string_for_deletion(&oDB_slave, id_datacube, fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (!frags.size) {
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	int result = OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (oph_dc_setup_dbms(&server, (dbmss.value[0]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//For each DBMS
	for (i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++) {
		if (oph_dc_connect_to_dbms(server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			//For each dbinstance count the number of stored datacubes
			if (oph_odb_stge_get_number_of_datacube_for_db(&oDB_slave, dbs.value[j].id_db, &datacubexdb_number)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve database instance information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DB_DATACUBE_COUNT_ERROR, (dbs.value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//If the db stores just one datacube then directly drop the dbinstance
			if (datacubexdb_number == 1) {
				//Databse drop
				if (oph_dc_delete_db(server, &(dbs.value[j]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while dropping database.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DROP_DB_ERROR, (dbs.value[j]).db_name);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
				continue;
			}
			//In this case the dbinstance may be already been deleted before
			else if (datacubexdb_number == 0)
				continue;

			if (oph_dc_use_db_of_dbms(server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DB_SELECTION_ERROR, (dbs.value[j]).db_name);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			//For each fragment
			for (k = 0; k < frags.size; k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				//Delete fragment
				if (oph_dc_delete_fragment(server, &(frags.value[k]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while dropping table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DROP_FRAGMENT_ERROR, (frags.value[j]).fragment_name);
					result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					break;
				}
			}
		}
		oph_dc_disconnect_from_dbms(server, &(dbmss.value[i]));
	}
	if (oph_dc_cleanup_dbms(server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_IOPLUGIN_CLEANUP_ERROR, (dbmss.value[0]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	return result;
}

int oph_dproc_clean_odb(ophidiadb * oDB, int id_datacube, int id_container)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	int *id_dbs;
	int size_dbs;

	//Get IDdb related to Datacube
	if (oph_odb_stge_retrieve_dbinstance_id_list_from_datacube(oDB, id_datacube, &id_dbs, &size_dbs)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DB ID\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DB_READ_ERROR);
		free(id_dbs);
		return result;
	}
	//Delete datacube and associated partitions, fragments and cubehasdims
	if (oph_odb_cube_delete_from_datacube_table(oDB, id_datacube)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DATACUBE_DELETE_ERROR);
		free(id_dbs);
		return result;
	}

	int i;
	int *id_datacubes = NULL;
	int size_datacubes;

	//Remove DBs
	for (i = 0; i < size_dbs; i++) {
		//Get IDdatacubes related to DB
		size_datacubes = 0;
		if (oph_odb_cube_retrieve_datacube_id_list_from_dbinstance(oDB, id_dbs[i], &id_datacubes, &size_datacubes)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve datacube ID\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DB_DATACUBE_READ_ERROR, id_dbs[i]);
			if (id_datacubes)
				free(id_datacubes);
			free(id_dbs);
			return result;
		}
		//Delete only empty dbinstances
		if (!size_datacubes) {
			//Delete database instance
			if (oph_odb_stge_delete_from_dbinstance_table(oDB, id_dbs[i])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update database instance table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DB_DELETE_ERROR, id_dbs[i]);
				if (id_datacubes)
					free(id_datacubes);
				free(id_dbs);
				return result;
			}
		}
		if (id_datacubes)
			free(id_datacubes);
		id_datacubes = NULL;
	}

	free(id_dbs);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
