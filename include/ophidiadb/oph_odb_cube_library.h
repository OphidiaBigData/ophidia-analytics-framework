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

#ifndef __OPH_ODB_CUBE_H__
#define __OPH_ODB_CUBE_H__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_cube_query.h"

#define OPH_ODB_CUBE_MEASURE_SIZE 256
#define OPH_ODB_CUBE_MEASURE_TYPE_SIZE 64

#define OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE 2048

#define OPH_ODB_CUBE_OPERATOR_SIZE 256
#define OPH_ODB_CUBE_OPERATION_QUERY_SIZE 2048

#define OPH_ODB_CUBE_SOURCE_URI_SIZE 512

#define OPH_ODB_CUBE_DESCRIPTION_SIZE 2048

/**
 * \brief Structure that contains a task table row
 * \param id_outputcube id of the newly generated datacube
 * \param id_job id of the job executing the task
 * \param operator Name of the operator just applied
 * \param query Query used fo the operation (it may be empty)
 * \param input_cube_number Number of input datacube used for the task
 * \param id_inputcube Vector of input datacube IDs used for the operation
 */
typedef struct {
	int id_outputcube;
	int id_job;
	char operator[OPH_ODB_CUBE_OPERATOR_SIZE + 1];
	char query[OPH_ODB_CUBE_OPERATION_QUERY_SIZE + 1];
	int input_cube_number;
	int *id_inputcube;
} oph_odb_task;

/**
 * \brief Structure that contain all parameters needed by DataCube
 * \param id_datacube id of the DataCube
 * \param hostxdatacube Number of hosts used to contain DataCube
 * \param fragmentxdb Number of fragments for each database (upper bound)
 * \param tuplexfragment Number of tuples for each fragment (upper bound)
 * \param measure Measure name
 * \param measure_type Type of data for the given data
 * \param frag_relative_index_set String with IDs of the fragments related to the datacube
 * \param id_container id of container of the datacube
 * \param id_db Vector of database instances ID used by the datacube
 * \param db_number Number of DB used by the datacube
 * \param compressed Whether the data is stored in a compressed (1) or uncompressed (0) form
 * \param level It rappresents the level of transformation.
 * \param id_source id of the source file. It may be NULL (0).
 */
typedef struct {
	int id_datacube;
	int hostxdatacube;
	int fragmentxdb;
	int tuplexfragment;
	char measure[OPH_ODB_CUBE_MEASURE_SIZE + 1];
	char measure_type[OPH_ODB_CUBE_MEASURE_TYPE_SIZE + 1];
	char frag_relative_index_set[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 1];
	int id_container;
	int *id_db;
	int db_number;
	int compressed;
	int level;
	int id_source;
	char description[OPH_ODB_CUBE_DESCRIPTION_SIZE + 1];
} oph_odb_datacube;



/**
 * \brief Structure that contains a source reference
 * \param id_source id of the source
 * \param uri URI of the source used
 */
typedef struct {
	int id_source;
	char uri[OPH_ODB_CUBE_SOURCE_URI_SIZE + 1];
} oph_odb_source;


/**
 * \brief Structure that contains a cubehasdim table row
 * \param id_cubhasdim Id used in Ophidia DB (output value)
 * \param id_datacube id of the containing datacube
 * \param id_dimensioninst id of the dimension instance
 * \param explicit_dim Type of dimension
 * \param size Length of the dimension (field of dimension_instance table used for read only purpose - inherited from previous version)
 * \param level Level of the dimension
 */
typedef struct {
	int id_cubehasdim;
	int id_datacube;
	int id_dimensioninst;
	int explicit_dim;
	int level;
	int size;
} oph_odb_cubehasdim;


/**
 * \brief Function to initialize datacube structure
 * \param m Pointer to datacube to initialize
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_init_datacube(oph_odb_datacube * m);

/**
 * \brief Function to free datcube instance resources
 * \param m Pointer to datacube to clean
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_free_datacube(oph_odb_datacube * m);

/**
 * \brief Function to count number of dbms instance in which a datacube is stored
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param num_elements Number of DBMS instances
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_get_datacube_dbmsinstance_number(ophidiadb * oDB, int id_datacube, int *num_elements);

/**
 * \brief Function to retrieve datacube size from ophidiadb
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param size Size of the datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_get_datacube_size(ophidiadb * oDB, int id_datacube, long long int *size);

/**
 * \brief Function to retrieve datacube number of elements from ophidiadb
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param num_elements Number of elements of the datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_get_datacube_num_elements(ophidiadb * oDB, int id_datacube, long long int *num_elements);

/**
 * \brief Function to insert datacube number of elements in ophidiadb
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param num_elements Number of elements of the datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_set_datacube_num_elements(ophidiadb * oDB, int id_datacube, long long int num_elements);

/**
 * \brief Function to insert datacube size in ophidiadb
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param size Size of the datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_set_datacube_size(ophidiadb * oDB, int id_datacube, long long int size);

/**
 * \brief Function to retrieve datacube informations from OphidiaDB given the datacube name and container name
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of the datacube to be found
 * \param cube Pointer datacube to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_datacube(ophidiadb * oDB, int id_datacube, oph_odb_datacube * cube);

/**
 * \brief Function to retrieve datacube informations from OphidiaDB given the datacube name and container name
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of the datacube to be found
 * \param cube Pointer datacube to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_datacube_with_ordered_partitions(ophidiadb * oDB, int id_datacube, oph_odb_datacube * cube);

/**
 * \brief Function to retrieve source informations from OphidiaDB given the src id
 * \param oDB Pointer to the OphidiaDB
 * \param id_src ID of the source to be found
 * \param src Pointer to src struct to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_source(ophidiadb * oDB, int id_src, oph_odb_source * src);

/**
 * \brief Function to retrieve datacube additional informations from OphidiaDB. This are the creation date and the description (usually unused in other functions)
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of the datacube to be found
 * \param *createdate Pointer to string to be filled with creation date (it has to be freed)
 * \param *description Pointer to string to be filled with description (it has to be freed)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_find_datacube_additional_info(ophidiadb * oDB, int id_datacube, char **creationdate, char **description);

/**
 * \brief Function to retrieve list of all datacubes generated from input one or all datacubes generating input one
 * \param oDB Pointer to the OphidiaDB
 * \param direction If 0 it retrieves the children datacube, if 1 it retrieves the parents datacube
 * \param id_datacube ID of the datacube to filter on
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_find_datacube_hierarchy(ophidiadb * oDB, int direction, int id_datacube, MYSQL_RES ** information_list);

/**
 * \brief Function to retrieve list of all task filtered by one or more parameters
 * \param oDB Pointer to the OphidiaDB
 * \param folder_id ID of the home folder
 * \param id_datacube ID of the datacube to filter on
 * \param container_name Name of the container to filter on
 * \param opertor Operator name to filer on
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_find_task_list(ophidiadb * oDB, int folder_id, int datacube_id, char *operator, char *container_name, MYSQL_RES ** information_list);

/**
 * \brief Function to delete datacube informations from OphidiaDB
 * \param oDB pointer to the OphidiaDB
 * \param id_datacube ID of the datacube to be deleted
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_delete_from_datacube_table(ophidiadb * oDB, int id_datacube);

/**
 * \brief Function that updates OphidiaDB adding source specified. If the uri is already inserted it will just return the id.
 * \param oDB Pointer to OphidiaDB
 * \param fragment Pointer to source row to be added
 * \param last_insertd_id ID of last inserted source
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_insert_into_source_table(ophidiadb * oDB, oph_odb_source * src, int *last_insertd_id);

/**
 * \brief Function that updates OphidiaDB adding the new task and new hasinput relations
 * \param oDB Pointer to OphidiaDB
 * \param new_task Pointer to taskto be added
 * \param last_insertd_id ID of last inserted task
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_insert_into_task_table(ophidiadb * oDB, oph_odb_task * new_task, int *last_insertd_id);

/**
 * \brief Function to check if a container.datacube exists
 * \param Pointer to OphidiaDB
 * \param uri Uri in PID
 * \param id_container Id of the container
 * \param id_datacube Id of the datacube
 * \param exists Variable that contains 0 if the container.datacube doesn't exists and 1 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_check_if_datacube_not_present_by_pid(ophidiadb * oDB, const char *uri, const int id_container, const int id_datacube, int *exists);

/**
 * \brief Function to check if host/dbms where datacube/fragment is stored are available
 * \param Pointer to OphidiaDB
 * \param id_input Id of the input datacube/fragment
 * \param type If set to 0 (default) it will search on the entire datacube, otherwise only on the fragment
 * \param status Variable that contains 0 if the datacube/fragment isn't available and 1 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_check_datacube_availability(ophidiadb * oDB, int id_input, int type, int *status);

/**
 * \brief Function to retrieve id of a datacubes related to a db
 * \param Pointer to OphidiaDB
 * \param id_db Id of the db
 * \param id_datacubes Pointer to be filled with the ids (it has to be freed)
 * \param size Pointer with the length of id_datacubes
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_datacube_id_list_from_dbinstance(ophidiadb * oDB, int id_db, int **id_datacubes, int *size);

/**
 * \brief Function to retrieve the name of the measure of a datacube
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube Id of the datacube
 * \param measure Name of the measure
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_datacube_measure(ophidiadb * oDB, int id_datacube, char *measure);

/**
 * \brief Function that updates OphidiaDB adding the new cubehasdim relations
 * \param oDB Pointer to OphidiaDB
 * \param cubedim Pointer to relation to be added
 * \param last_insertd_id ID of last inserted relation
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_insert_into_cubehasdim_table(ophidiadb * oDB, oph_odb_cubehasdim * cubedim, int *last_insertd_id);

/**
 * \brief Function that updates OphidiaDB adding the new datacube and new partition relations
 * \param oDB Pointer to OphidiaDB
 * \param cube Pointer to datacube to be added
 * \param last_insertd_id ID of last inserted datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_insert_into_datacube_partitioned_tables(ophidiadb * oDB, oph_odb_datacube * cube, int *last_insertd_id);

/**
 * \brief Function to find datacubes and/or containers from OphidiaDB given optionally a datacube name and/or the container name
 * \param oDB Pointer to the OphidiaDB
 * \param folder_id Id of the home of the session
 * \param container_name Name of the container to be found (may be null)
 * \param id_datacube Id of datacube to be found (may be 0)
 * \param search_on Search only on containers (1), datacubes (2) or both (0)
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_find_containers_datacubes(ophidiadb * oDB, int folder_id, int search_on, char *container_name, int id_datacube, MYSQL_RES ** information_list);

/**
 * \brief Function to retrieve cubehasdim rows from OphidiaDB given the datacube id
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of the datacube to be found
 * \param cubedims Pointer to cubehasdim vector (it has to be freed)
 * \param dim_num Pointer to integer used to store the dimension number;
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_cube_retrieve_cubehasdim_list(ophidiadb * oDB, int id_datacube, oph_odb_cubehasdim ** cubedims, int *dim_num);

int oph_odb_cube_update_tuplexfragment(ophidiadb * oDB, int id_datacube, int tuplexfragment);

int oph_odb_cube_update_level_in_cubehasdim_table(ophidiadb * oDB, int level, int id_cubehasdim);

#endif				/* __OPH_ODB_CUBE_H__ */
