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

#define _GNU_SOURCE

#include "oph_driver_procedure_library.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#include "debug.h"

#include "oph_log_error_codes.h"

#include "oph_datacube_library.h"
#include "oph_ioserver_library.h"
#include "oph_analytics_operator_library.h"

#include <pthread.h>

extern int msglevel;

int oph_dproc_delete_data(int id_datacube, int id_container, char *fragment_ids, int start_position, int row_number, int thread_number)
{
	if (!id_datacube || !fragment_ids || thread_number < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	int res[thread_number];
	int l = 0;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb_thread(&oDB_slave);
	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONFIGURATION_FILE);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONNECTION_ERROR);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	int no_frag = 0;

	//retrieve fragment connection string
	if (oph_odb_stge_fetch_fragment_connection_string_for_deletion(&oDB_slave, id_datacube, fragment_ids, &frags, &dbs, &dbmss)) {
		if (start_position != 0 || row_number != 0) {
			//retrieve db connection string
			if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube, start_position, row_number, &dbs, &dbmss)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
				oph_odb_free_ophidiadb_thread(&oDB_slave);
				mysql_thread_end();
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			} else {
				no_frag = 1;
			}
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
			oph_odb_free_ophidiadb_thread(&oDB_slave);
			mysql_thread_end();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	if (!no_frag && !frags.size) {
		if (start_position != 0 || row_number != 0) {
			//retrieve db connection string
			if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube, start_position, row_number, &dbs, &dbmss)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb_thread(&oDB_slave);
				mysql_thread_end();
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			} else {
				no_frag = 1;
			}
		} else {
			//Early termination
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb_thread(&oDB_slave);
			mysql_thread_end();
			return OPH_ANALYTICS_OPERATOR_SUCCESS;
		}
	}
	//For each dbinstance count the number of stored datacubes (COUNT ALL TOGETHER)
	int *datacubexdb_number = NULL;
	if (!(datacubexdb_number = (int *) calloc(dbs.size, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int *id_dbs = NULL;
	if (!(id_dbs = (int *) calloc(dbs.size, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error allocating memory\n");
		free(datacubexdb_number);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int i = 0;
	for (i = 0; i < dbs.size; i++) {
		id_dbs[i] = dbs.value[i].id_db;
	}

	if (oph_odb_stge_get_number_of_datacube_for_dbs(&oDB_slave, dbs.size, id_dbs, datacubexdb_number)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve database instances information.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to retrieve database instances information.\n");
		oph_odb_stge_free_fragment_list(&frags);
		oph_odb_stge_free_db_list(&dbs);
		oph_odb_stge_free_dbms_list(&dbmss);
		oph_odb_free_ophidiadb_thread(&oDB_slave);
		mysql_thread_end();
		free(datacubexdb_number);
		free(id_dbs);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	free(id_dbs);

	struct _thread_struct {
		unsigned int current_thread;
		unsigned int total_threads;
		int id_container;
		oph_odb_fragment_list *frags;
		oph_odb_db_instance_list *dbs;
		oph_odb_dbms_instance_list *dbmss;
		int *datacubexdb_number;
	};
	typedef struct _thread_struct thread_struct;

	void *exec_thread(void *ts) {

		int l = ((thread_struct *) ts)->current_thread;
		int num_threads = ((thread_struct *) ts)->total_threads;
		int id_container = ((thread_struct *) ts)->id_container;

		oph_odb_fragment_list *frags = ((thread_struct *) ts)->frags;
		oph_odb_db_instance_list *dbs = ((thread_struct *) ts)->dbs;
		oph_odb_dbms_instance_list *dbmss = ((thread_struct *) ts)->dbmss;

		int *datacubexdb_number = ((thread_struct *) ts)->datacubexdb_number;

		oph_ioserver_handler *server = NULL;

		int i, j, k;

		int res = OPH_ANALYTICS_OPERATOR_SUCCESS;

		if (oph_dc_setup_dbms_thread(&(server), (dbmss->value[0]).io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_IOPLUGIN_SETUP_ERROR, (dbmss->value[0]).id_dbms);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		int fragxthread = (int) floor((double) (frags->size / num_threads));
		int remainder = (int) frags->size % num_threads;
		//Compute starting number of fragments deleted by other threads
		unsigned int current_frag_count = l * fragxthread + (l < remainder ? l : remainder);

		//Update number of fragments to be inserted
		if (l < remainder)
			fragxthread += 1;

		int frag_count = 0, db_count = 0;
		int first_dbms, first_db, first_frag = current_frag_count;
		int dbxthread = 0;

		//If number of frags is lower than thread number exit from these threads
		if (first_frag >= frags->size) {
			//Early termination
			oph_dc_cleanup_dbms(server);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_SUCCESS;
			int *ret_val = (int *) malloc(sizeof(int));
			*ret_val = res;
			pthread_exit((void *) ret_val);
		}

		for (first_db = 0; first_db < dbs->size && res == OPH_ANALYTICS_OPERATOR_SUCCESS; first_db++) {
			//Find db associated to fragment
			if (frags->value[current_frag_count].id_db == dbs->value[first_db].id_db)
				break;
		}
		for (first_dbms = 0; first_dbms < dbmss->size && res == OPH_ANALYTICS_OPERATOR_SUCCESS; first_dbms++) {
			//Find dbms associated to db
			if (dbs->value[first_db].id_dbms == dbmss->value[first_dbms].id_dbms)
				break;
		}

		//Count number of DBs related to this thread
		dbxthread = 1;
		for (k = first_frag + 1; (k < frags->size) && (k < first_frag + fragxthread); k++) {
			if (frags->value[k - 1].db_instance != frags->value[k].db_instance)
				dbxthread++;
		}

		//Build array of fragments x DB
		int frag_to_delete[dbxthread];
		frag_to_delete[i = 0] = 1;
		for (k = first_frag + 1; (k < frags->size) && (k < first_frag + fragxthread); k++) {
			if (frags->value[k - 1].db_instance != frags->value[k].db_instance)
				frag_to_delete[++i] = 0;
			frag_to_delete[i]++;
		}

		//For each DBMS
		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			for (i = first_dbms; i < dbmss->size && (db_count < dbxthread); i++) {
				if (oph_dc_connect_to_dbms(server, &(dbmss->value[i]), 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DBMS_CONNECTION_ERROR, (dbmss->value[i]).id_dbms);
					oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
					res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					continue;
				}
				//For each DB
				for (j = first_db; j < dbs->size && (db_count < dbxthread); j++) {
					//Check DB - DBMS Association
					if (dbs->value[j].dbms_instance != &(dbmss->value[i]))
						continue;

					db_count++;

					//If the db stores just one datacube then directly drop the dbinstance
					if (datacubexdb_number[j] == 1) {
						//Database drop
						if (oph_dc_delete_db(server, &(dbs->value[j]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while dropping database.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DROP_DB_ERROR, (dbs->value[j]).db_name);
							res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							continue;
						}
						continue;
					}
					//In this case the dbinstance may have already been deleted before
					else if (datacubexdb_number[j] == 0)
						continue;

					if (!no_frag) {
						if (oph_dc_use_db_of_dbms(server, &(dbmss->value[i]), &(dbs->value[j]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DB_SELECTION_ERROR, (dbs->value[j]).db_name);
							res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							continue;
						}
						//For each fragment
						frag_count = 0;
						for (k = first_frag; (k < frags->size) && (frag_count < frag_to_delete[db_count - 1]); k++) {
							//Check Fragment - DB Association
							if (frags->value[k].db_instance != &(dbs->value[j]))
								continue;

							frag_count++;

							//Delete fragment
							if (oph_dc_delete_fragment(server, &(frags->value[k]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while dropping table.\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DROP_FRAGMENT_ERROR, (frags->value[j]).fragment_name);
								res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
								continue;
							}
						}
					}
				}
				oph_dc_disconnect_from_dbms(server, &(dbmss->value[i]));
			}

			if (res != OPH_ANALYTICS_OPERATOR_SUCCESS) {
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
			}
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			oph_dc_cleanup_dbms(server);
			mysql_thread_end();
		}

		int *ret_val = (int *) malloc(sizeof(int));
		*ret_val = res;
		pthread_exit((void *) ret_val);
	}

	pthread_t threads[thread_number];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	thread_struct ts[thread_number];

	int rc;
	for (l = 0; l < thread_number; l++) {
		ts[l].total_threads = thread_number;
		ts[l].current_thread = l;
		ts[l].id_container = id_container;
		ts[l].frags = &frags;
		ts[l].dbs = &dbs;
		ts[l].dbmss = &dbmss;
		ts[l].datacubexdb_number = datacubexdb_number;

		rc = pthread_create(&threads[l], &attr, exec_thread, (void *) &(ts[l]));
		if (rc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create thread %d: %d.\n", l, rc);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Unable to create thread %d: %d.\n", l, rc);
		}
	}

	pthread_attr_destroy(&attr);
	void *ret_val = NULL;
	for (l = 0; l < thread_number; l++) {
		rc = pthread_join(threads[l], &ret_val);
		res[l] = *((int *) ret_val);
		free(ret_val);
		if (rc) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while joining thread %d: %d.\n", l, rc);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Error while joining thread %d: %d.\n", l, rc);
		}
	}

	free(datacubexdb_number);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_free_ophidiadb_thread(&oDB_slave);
	mysql_thread_end();

	for (l = 0; l < thread_number; l++) {
		if (res[l] != OPH_ANALYTICS_OPERATOR_SUCCESS) {
			return res[l];
		}
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
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
				continue;
			}
		}
		if (id_datacubes)
			free(id_datacubes);
		id_datacubes = NULL;
	}

	free(id_dbs);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
