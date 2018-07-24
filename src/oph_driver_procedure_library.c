/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

	//In multi-thread code mysql_library_init must be called before starting the threads
	if (mysql_library_init(0, NULL, NULL)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "MySQL initialization error\n");
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	struct _thread_struct {
		unsigned int current_thread;
		unsigned int total_threads;
		int id_datacube;
		int id_container;
		char *fragment_ids;
		int start_position;
		int row_number;
	};
	typedef struct _thread_struct thread_struct;

	void *exec_thread(void *ts) {

		int l = ((thread_struct *) ts)->current_thread;
		int num_threads = ((thread_struct *) ts)->total_threads;
		int id_datacube = ((thread_struct *) ts)->id_datacube;
		int id_container = ((thread_struct *) ts)->id_container;
		char *fragment_ids = ((thread_struct *) ts)->fragment_ids;
		int start_position = ((thread_struct *) ts)->start_position;
		int row_number = ((thread_struct *) ts)->row_number;

		oph_odb_fragment_list frags;
		oph_odb_db_instance_list dbs;
		oph_odb_dbms_instance_list dbmss;
		oph_ioserver_handler *server = NULL;

		//Each process has to be connected to a slave ophidiadb
		ophidiadb oDB_slave;
		oph_odb_init_ophidiadb(&oDB_slave);
		int i, j, k;

		int res = OPH_ANALYTICS_OPERATOR_SUCCESS;

		if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONFIGURATION_FILE);
			res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_OPHIDIADB_CONNECTION_ERROR);
				oph_odb_free_ophidiadb_thread(&oDB_slave);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}

		int datacubexdb_number = 0;
		int no_frag = 0;

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			//retrieve fragment connection string
			if (oph_odb_stge_fetch_fragment_connection_string_for_deletion(&oDB_slave, id_datacube, fragment_ids, &frags, &dbs, &dbmss)) {
				if (start_position != 0 || row_number != 0) {
					//retrieve db connection string
					if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube, start_position, row_number, &dbs, &dbmss)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
						oph_odb_free_ophidiadb_thread(&oDB_slave);
						mysql_thread_end();
						res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					} else {
						no_frag = 1;
					}
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
					oph_odb_free_ophidiadb_thread(&oDB_slave);
					mysql_thread_end();
					res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			if (!no_frag && !frags.size) {
				if (start_position != 0 || row_number != 0) {
					//retrieve db connection string
					if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube, start_position, row_number, &dbs, &dbmss)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_CONNECTION_STRINGS_NOT_FOUND);
						oph_odb_free_ophidiadb_thread(&oDB_slave);
						mysql_thread_end();
						res = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					} else {
						no_frag = 1;
					}
				} else {
					//Early termination
					oph_odb_stge_free_fragment_list(&frags);
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					mysql_thread_end();
					res = OPH_ANALYTICS_OPERATOR_SUCCESS;
					int *ret_val = (int *) malloc(sizeof(int));
					*ret_val = res;
					pthread_exit((void *) ret_val);
				}
			}
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			if (oph_dc_setup_dbms(&(server), (dbmss.value[0]).io_server_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb_thread(&oDB_slave);
				mysql_thread_end();
				res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}

		int dbxthread = (int) floor((double) (dbs.size / num_threads));
		int remainder = (int) dbs.size % num_threads;
		//Compute starting number of dbs deleted by other threads
		unsigned int current_db_count = l * dbxthread + (l < remainder ? l : remainder);

		//Update number of fragments to be inserted
		if (l < remainder)
			dbxthread += 1;

		int db_count = 0;
		int first_dbms, first_db = current_db_count;

		//If number of DBS is lower than thread number exit from these threads
		if(first_db >= dbs.size) {
			//Early termination
			oph_dc_cleanup_dbms(server);
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			mysql_thread_end();
			res = OPH_ANALYTICS_OPERATOR_SUCCESS;
			int *ret_val = (int *) malloc(sizeof(int));
			*ret_val = res;
			pthread_exit((void *) ret_val);
		}
		
		for (first_dbms = 0; first_dbms < dbmss.size && res == OPH_ANALYTICS_OPERATOR_SUCCESS; first_dbms++) {
			//Find dbms associated to db
			if (dbs.value[current_db_count].id_dbms == dbmss.value[current_db_count].id_dbms)
				break;
		}

		//For each DBMS
		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			for (i = first_dbms; i < dbmss.size; i++) {
				if (oph_dc_connect_to_dbms(server, &(dbmss.value[i]), 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);
					oph_dc_disconnect_from_dbms(server, &(dbmss.value[i]));
					res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
					continue;
				}

				//For each DB
				for (j = first_db; j < dbs.size && (db_count < dbxthread); j++) {
					//Check DB - DBMS Association
					if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
						continue;

					db_count++;

					//For each dbinstance count the number of stored datacubes
					if (oph_odb_stge_get_number_of_datacube_for_db(&oDB_slave, dbs.value[j].id_db, &datacubexdb_number)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve database instance information.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DB_DATACUBE_COUNT_ERROR, (dbs.value[j]).db_name);
						res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
						continue;
					}
					//If the db stores just one datacube then directly drop the dbinstance
					if (datacubexdb_number == 1) {
						//Database drop
						if (oph_dc_delete_db(server, &(dbs.value[j]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while dropping database.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_OPH_DELETE_DROP_DB_ERROR, (dbs.value[j]).db_name);
							res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							continue;
						}
						continue;
					}
					//In this case the dbinstance may have already been deleted before
					else if (datacubexdb_number == 0)
						continue;

					if (!no_frag) {
						if (oph_dc_use_db_of_dbms(server, &(dbmss.value[i]), &(dbs.value[j]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DB_SELECTION_ERROR, (dbs.value[j]).db_name);
							res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
							continue;
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
								res = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
								continue;
							}
						}
					}
				}
				oph_dc_disconnect_from_dbms(server, &(dbmss.value[i]));
			}

			if (res != OPH_ANALYTICS_OPERATOR_SUCCESS) {
				oph_odb_stge_free_fragment_list(&frags);
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb_thread(&oDB_slave);
				oph_dc_cleanup_dbms(server);
				mysql_thread_end();
			}
		}

		if (res == OPH_ANALYTICS_OPERATOR_SUCCESS) {
			oph_odb_stge_free_fragment_list(&frags);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb_thread(&oDB_slave);
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
		ts[l].id_datacube = id_datacube;
		ts[l].id_container = id_container;
		ts[l].fragment_ids = fragment_ids;
		ts[l].start_position = start_position;
		ts[l].row_number = row_number;

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

	//In multi-thread code mysql_library_end must be called after executing the threads
	mysql_library_end();

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
