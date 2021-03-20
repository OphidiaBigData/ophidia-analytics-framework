/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#ifdef OPH_MYSQL_SUPPORT
#include <errmsg.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "oph_idstring_library.h"
#include "oph_pid_library.h"
#include "oph_datacube_library.h"

#include "debug.h"

#include "oph_input_parameters.h"

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

int msglevel = LOG_DEBUG;

int main(int argc, char **argv)
{
	fprintf(stdout, OPH_VERSION, PACKAGE_VERSION);
	fprintf(stdout, "%s", OPH_DISCLAIMER);

	if (!argc || !argv)
		return 1;

	if (!strcmp(argv[1], "-v")) {
		return 0;
	}

	if (!strcmp(argv[1], "-x")) {
		fprintf(stdout, "%s", OPH_WARRANTY);
		return 0;
	}

	if (!strcmp(argv[1], "-z")) {
		fprintf(stdout, "%s", OPH_CONDITIONS);
		return 0;
	}
	//1 - Set up struct to empty values
	ophidiadb oDB;
	int id_input_container;
	int id_input_datacube;
	int id_output_datacube;
	char *fragment_ids;
	oph_ioserver_handler *server;

	id_input_datacube = 1;
	id_output_datacube = 0;
	id_input_container = 1;
	fragment_ids = NULL;
	server = NULL;

#ifdef OPH_PARALLEL_LOCATION
	char log_prefix[OPH_COMMON_BUFFER_LEN];
	snprintf(log_prefix, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_LOG_PATH_PREFIX, OPH_PARALLEL_LOCATION);
	set_log_prefix(log_prefix);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Set logging directory to '%s'\n", log_prefix);
#endif

	oph_odb_init_ophidiadb(&oDB);

	if (oph_odb_read_ophidiadb_config_file(&oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		return -1;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		return -2;
	}
	//Check if datacube exists (by ID container and datacube)
	int status = 0;

	if ((oph_odb_cube_check_datacube_availability(&oDB, id_input_datacube, 0, &status)) || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
		return -2;
	}
	//For error checking
	char id_string[2][OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
	memset(id_string, 0, sizeof(id_string));
	id_string[0][0] = 0;

	oph_odb_datacube cube;
	oph_odb_cube_init_datacube(&cube);

	//retrieve input datacube
	if (oph_odb_cube_retrieve_datacube(&oDB, id_input_datacube, &cube)) {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
		return -2;
	}
	// Change the container id
	cube.id_container = id_input_container;
	cube.id_source = 0;
	cube.level++;

	//Insert new datacube
	if (oph_odb_cube_insert_into_datacube_partitioned_tables(&oDB, &cube, &id_output_datacube)) {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
		return -2;
	}
	//Copy fragment id relative index set 
	if (!(fragment_ids = (char *) strndup(cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))) {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return -2;
	}
	oph_odb_cube_free_datacube(&cube);

	oph_odb_cubehasdim *cubedims = NULL;
	int number_of_dimensions = 0;
	int last_insertd_id = 0;
	int l;

	//Read old cube - dimension relation rows
	if (oph_odb_cube_retrieve_cubehasdim_list(&oDB, id_input_datacube, &cubedims, &number_of_dimensions)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
		free(cubedims);
		return -2;
	}
	//Write new cube - dimension relation rows
	for (l = 0; l < number_of_dimensions; l++) {
		//Change iddatacube in cubehasdim
		cubedims[l].id_datacube = id_output_datacube;

		if (oph_odb_cube_insert_into_cubehasdim_table(&oDB, &(cubedims[l]), &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert datacube - dimension relations.\n");
			free(cubedims);
			return -2;
		}
	}

	free(cubedims);

	last_insertd_id = 0;

	int i = 0, j, k;

	oph_odb_fragment_list frags;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		return -1;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		oph_odb_free_ophidiadb(&oDB_slave);
		return -2;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_input_datacube, fragment_ids, &frags, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		oph_odb_free_ophidiadb(&oDB_slave);
		return -1;
	}

	char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];

	int frag_count = 0;
	int result = 0;

	if (oph_dc_setup_dbms(&server, (dbmss.value[i]).io_server_type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		result = -2;
	}
	//For each DBMS
	for (i = 0; (i < dbmss.size) && (result == 0); i++) {

		if (oph_dc_connect_to_dbms(server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			result = -2;
		}
		//For each DB
		for (j = 0; (j < dbs.size) && (result == 0); j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				result = -2;
				break;
			}
			//For each fragment
			for (k = 0; (k < frags.size) && (result == 0); k++) {
				//Check Fragment - DB Association
				if (frags.value[k].db_instance != &(dbs.value[j]))
					continue;

				if (oph_dc_generate_fragment_name(NULL, id_output_datacube, 0, (frag_count + 1), &frag_name_out)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
					result = -1;
					break;
				}
				//Duplicate fragment
				if (oph_dc_create_fragment_from_query(server, &(frags.value[k]), frag_name_out, MYSQL_FRAG_MEASURE, 0, 0, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
					result = -1;
					break;
				}
				//Change fragment fields
				frags.value[k].id_datacube = id_output_datacube;
				strncpy(frags.value[k].fragment_name, frag_name_out, OPH_ODB_STGE_FRAG_NAME_SIZE);
				frags.value[k].fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE] = 0;

				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags.value[k]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					result = -1;
					break;
				}
				frag_count++;
			}
		}
		oph_dc_disconnect_from_dbms(server, &(dbmss.value[i]));
	}

	if (oph_dc_cleanup_dbms(server)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		result = -2;
	}

	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	if ((result == 0)) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			return -1;
		}
		if (oph_pid_show_pid(id_input_container, id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			free(tmp_uri);
			return -1;
		}
		free(tmp_uri);
	}

	oph_odb_disconnect_from_ophidiadb(&oDB);
	oph_odb_free_ophidiadb(&oDB);

	if (fragment_ids) {
		free(fragment_ids);
	}

	return 0;
}
