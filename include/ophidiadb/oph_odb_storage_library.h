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

#ifndef __OPH_ODB_STGE_H__
#define __OPH_ODB_STGE_H__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_storage_query.h"

#define OPH_ODB_STGE_LOGIN_SIZE 256
#define OPH_ODB_STGE_PWD_SIZE 256
#define OPH_ODB_STGE_HOST_NAME_SIZE 256
#define OPH_ODB_STGE_DB_NAME_SIZE 256
#define OPH_ODB_STGE_FRAG_NAME_SIZE 256
#define OPH_ODB_STGE_SERVER_NAME_SIZE 256
#define OPH_ODB_STGE_PARTITION_NAME_SIZE 64

#define OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE 1
#define OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST 2
#define OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS 3
#define OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB 4
#define OPH_ODB_STGE_FRAG_LIST_LV_CONT_CUBE_HOST_DBMS_DB_FRAG 5

/**
 * \brief Structure that define a Host machine
 * \param hostname name assigned to the host
 * \param id_host id of the host
 * \param cores Number of cores available on host
 * \param memory Amount of RAM memory available on host
 */
typedef struct {
	char hostname[OPH_ODB_STGE_HOST_NAME_SIZE + 1];
	int id_host;
	int cores;
	int memory;
} oph_odb_host;

/**
 * \brief Structure that define a DBMS instance
 * \param hostname name assigned to the host where DBMS instance is installed
 * \param id_dbms id of dbms
 * \param login login to connect to DBMS instance
 * \param pwd Password to connect to DBMS instance as login
 * \param port port where the DBMS instance
 * \param fs_type If the disk is local (0) or global (1)
 * \param io_server_type String with IO server type
 * \param conn Connection to dbms instance
 */
typedef struct {
	char hostname[OPH_ODB_STGE_HOST_NAME_SIZE + 1];
	int id_dbms;
	char login[OPH_ODB_STGE_LOGIN_SIZE + 1];
	char pwd[OPH_ODB_STGE_PWD_SIZE + 1];
	int port;
	int fs_type;
	char io_server_type[OPH_ODB_STGE_SERVER_NAME_SIZE + 1];
	void *conn;
} oph_odb_dbms_instance;

/**
 * \brief Structure that describe a database instance
 * \param dbm_sinstance Pointer to dbms that manages db
 * \param id_db id assigned to dbinstance
 * \param db_name name of database
 */
typedef struct {
	oph_odb_dbms_instance *dbms_instance;
	int id_dbms;
	int id_db;
	char db_name[OPH_ODB_STGE_DB_NAME_SIZE + 1];
} oph_odb_db_instance;

/**
 * \brief Structure that contains a table instance
 * \param db Pointer to database instance that contain the table
 * \param id_fragment id of fragment
 * \param id_datacube id of the containing datacube
 * \param frag_relative_index relative ID of the fragment (related to the datacube)
 * \param fragment_name Name given to the fragment
 * \param key_start starting id referred to DataCube
 * \param key_end ending id referred to DataCube
 */
typedef struct {
	oph_odb_db_instance *db_instance;
	int id_datacube;
	int id_db;
	int id_fragment;
	int frag_relative_index;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE + 1];
	int key_start;
	int key_end;
} oph_odb_fragment;

typedef struct {
	int id_host;
	int id_dbms;
	int id_db;
	int id_fragment;
	int frag_relative_index;
	int key_start;
	int key_end;
} oph_odb_fragment2;

/**
 * \brief Structure that define a DBMS instance list
 * \param value Pointer to dbms_instance array
 * \param size Length of the list
 */
typedef struct {
	oph_odb_dbms_instance *value;
	int size;
} oph_odb_dbms_instance_list;

/**
 * \brief Structure that define a DB instance list
 * \param value Pointer to db_instance array
 * \param size Length of the list
 */
typedef struct {
	oph_odb_db_instance *value;
	int size;
} oph_odb_db_instance_list;

/**
 * \brief Structure that define a fragment list
 * \param value Pointer to fragment array
 * \param size Length of the list
 */
typedef struct {
	oph_odb_fragment *value;
	int size;
} oph_odb_fragment_list;

typedef struct {
	oph_odb_fragment2 *value;
	int size;
} oph_odb_fragment_list2;

/**
 * \brief Function to initialize fragment list
 * \param m Pointer to fragment list to initialize
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_init_fragment_list(oph_odb_fragment_list * m);
int oph_odb_stge_init_fragment_list2(oph_odb_fragment_list2 * m);

/**
 * \brief Function to free fragment list resources
 * \param m Pointer to fragment list to clean
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_free_fragment_list(oph_odb_fragment_list * m);
int oph_odb_stge_free_fragment_list2(oph_odb_fragment_list2 * m);

/**
 * \brief Function to initialize db list
 * \param m Pointer to db list to initialize
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_init_db_list(oph_odb_db_instance_list * m);

/**
 * \brief Function to free db instance list resources
 * \param m Pointer to db list to clean
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_free_db_list(oph_odb_db_instance_list * m);

/**
 * \brief Function to initialize dbms list
 * \param m Pointer to dbms list to initialize
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_init_dbms_list(oph_odb_dbms_instance_list * m);

/**
 * \brief Function to free dbms instance list resources
 * \param m Pointer to dbms list to clean
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_free_dbms_list(oph_odb_dbms_instance_list * m);

/**
 * \brief Function to retrieve dbms informations from OphidiaDB given the dbms id
 * \param oDB Pointer to the OphidiaDB
 * \param id_dbms Id of the DBMS to be found
 * \param dbms Pointer dbms_instance to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_dbmsinstance(ophidiadb * oDB, int id_dbms, oph_odb_dbms_instance * dbms);

/**
 * \brief Function to retrieve information of the first dbms available from OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param dbms Pointer dbms_instance to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_first_dbmsinstance(ophidiadb * oDB, oph_odb_dbms_instance * dbms);

/**
 * \brief Function to retrieve DB connection string informations from OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param datacube_id ID of datacube to be found
 * \param start_position Number of first row to select
 * \param row_number Number of rows to be selected
 * \param dbs Pointer to db list to be filled
 * \param dbmss Pointer to dbms list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_fetch_db_connection_string(ophidiadb * oDB, int datacube_id, int start_position, int row_number, oph_odb_db_instance_list * dbs, oph_odb_dbms_instance_list * dbmss);

/**
 * \brief Function to retrieve DBMS connection string informations from OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube to be found
 * \param start_position Number of first row to select
 * \param row_number Number of rows to be selected
 * \param dbmss Pointer to dbms list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_fetch_dbms_connection_string(ophidiadb * oDB, int id_datacube, int start_position, int row_number, oph_odb_dbms_instance_list * dbmss);

/**
 * \brief Function to retrieve connection string informations from OphidiaDB given fragment and datacube
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube to be found
 * \param fragrelindexset Set of relative fragment IDs to be found
 * \param frags Pointer to fragment list to be filled
 * \param dbs Pointer to db list to be filled
 * \param dbmss Pointer to dbms list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_fetch_fragment_connection_string(ophidiadb * oDB, int id_datacube, char *fragrelindexset, oph_odb_fragment_list * frags, oph_odb_db_instance_list * dbs,
						  oph_odb_dbms_instance_list * dbmss);

/**
 * \brief Function to retrieve connection string informations from OphidiaDB given fragment and datacube
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube to be found
 * \param fragrelindexset Set of relative fragment IDs to be found
 * \param frags Pointer to fragment list to be filled
 * \param dbs Pointer to db list to be filled
 * \param dbmss Pointer to dbms list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_fetch_fragment_connection_string_for_deletion(ophidiadb * oDB, int id_datacube, char *fragrelindexset, oph_odb_fragment_list * frags, oph_odb_db_instance_list * dbs,
							       oph_odb_dbms_instance_list * dbmss);

/**
 * \brief Function to get default values for host partition and/or file system
 * \param oDB Pointer to the OphidiaDB
 * \param ioserver_type Server type to be used. 
 * \param fs_type Pointer to filesystem type. If set to - 1 it will be updated with the first available. It may be 0 (local disk), 1 (global) or 2 (inmemory)
 * \param hots_partition Pointer to name of the host partition. If set to 'auto' the first available will be used
 * \param hots_number Variable that contains the requested I/O hosts
 * \param dbmsxhots_number Variable that contain the requested DBMS number per host
 * \param exists Variable that contains 0 if the host partition of file system doesn't exists and 1 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_get_default_host_partition_fs(ophidiadb * oDB, int *fs_type, char *ioserver_type, char **host_partition, int host_number, int dbmsxhost_number, int *exist);

/**
 * \brief Function to check if number of host and DBMS in OphidiaDB are available
 * \param oDB Pointer to the OphidiaDB
 * \param ioserver_type Server type to be used. 
 * \param fs_type Filesystem type. It may be 0 (local disk), 1 (global) or 2 (inmemory)
 * \param hots_partition Name of the host partition to be used
 * \param id_user User identifier
 * \param hots_number Variable that contains the requested I/O hosts
 * \param dbmsxhots_number Variable that contain the requested DBMS number per host
 * \param exists Variable that contains 0 if the container.datacube doesn't exists and 1 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_check_number_of_host_dbms(ophidiadb * oDB, int fs_type, char *ioserver_type, char *host_partition, int id_user, int host_number, int dbmsxhost_number, int *exist);

/**
 * \brief Function to count maximum number of host and DBMS available in OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param ioserver_type Server type to be used. 
 * \param fs_type Filesystem type. It may be 0 (local disk), 1 (global) or 2 (inmemory)
 * \param host_partition Name of the host partition to be used
 * \param id_user User indentifier
 * \param hots_number Variable that will contain the number of hosts
 * \param dbmsxhots_number Variable that will contain the number of DBMS per host
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_count_number_of_host_dbms(ophidiadb * oDB, int server_type, char *ioserver_type, char *host_partition, int id_user, int *host_number, int *dbmsxhost_number);

/**
 * \brief Function to retrieve fragment list from OphidiaDB given datacube id
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube partitioned
 * \param frags Pointer to fragment list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_fragment_list(ophidiadb * oDB, int id_datacube, oph_odb_fragment_list * frags);

/**
 * \brief Function to retrieve fragment list from OphidiaDB given datacube id. It also retrieves id of host, dbmsinstance and dbinstance that stores the fragment
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube partitioned
 * \param frags Pointer to fragment list to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_fragment_list2(ophidiadb * oDB, int id_datacube, oph_odb_fragment_list2 * frags);

/**
 * \brief Function to retrieve list of structure information from OphidiaDB given datacube and optionally a db name and/or id_dbms and/or hostname
 * \param oDB Pointer to the OphidiaDB
 * \param level Level of verbosity  3: containers and cuboid, 4: containers,cuboid,host , 5: containers,cuboid,host,dbms , 6: containers,cuboid,host,dbms,db , 7: containers,cuboid,host,dbms,db,fragment
 * \param id_datacube ID of datacube to be found
 * \param hostname Name of the host to filter on (may be null)
 * \param db_name Name of the DB to filter on (may be null)
 * \param id_dbms ID of the DBMS to filter on (may be 0)
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_find_datacube_fragmentation_list(ophidiadb * oDB, int level, int id_datacube, char *hostname, char *db_name, int id_dbms, MYSQL_RES ** information_list);

/**
 * \brief Function to retrieve list of hostname dbms instances information from OphidiaDB given optionally hostname
 * \param oDB Pointer to the OphidiaDB
 * \param level Level of verbosity 1: only host, 2: only dbms instances (of a specific host), 3: host and dbms instances
 * \param hostname Name of the host to filter on (may be null)
 * \param partition_name Name of the partition to filter on (may be null)
 * \param server_type Filter on server_type attribute. It may be 0 (local disk), 1 (global), 2 (inmemory) or 3 (all)
 * \param host_status Status of the host to filter on (may be null)
 * \param dbms_status Status of the dbms instance to filter on (may be null)
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \param id_user Reference to user-defined host partitions
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_find_instances_information(ophidiadb * oDB, int level, char *hostname, char *partition_name, int fs_type, char *ioserver_type, char *host_status, char *dbms_status,
					    MYSQL_RES ** information_list, int id_user);

/**
 * \brief Function to retrieve list of fragments name related to datacube and id_dbms
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of datacube to be found
 * \param id_dbms ID of the DBMS to be found
 * \param start_position Number of first row to select
 * \param row_number Number of rows to be selected
 * \param fragment_name_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_find_fragment_name_list(ophidiadb * oDB, int id_datacube, int id_dbms, int start_position, int row_number, MYSQL_RES ** fragment_list);

/**
 * \brief Function to retrieve fragment related metadata
 * \param oDB Pointer to the OphidiaDB
 * \param frag_name String with the name of the fragment to be found
 * \param frag Pointer fragment structure that will contain the retrieved data
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_fragment(ophidiadb * oDB, char *frag_name, oph_odb_fragment * frag);

/**
 * \brief Function to retrieve db + dbms related metadata
 * \param oDB Pointer to the OphidiaDB
 * \param id_dbinstance ID of the db to be retrieved
 * \param db Pointer to db structure that will contain the retrieved data
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_dbinstance(ophidiadb * oDB, int id_dbinstance, oph_odb_db_instance * db);

/**
 * \brief Function to delete db_istance tuple from OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param id_db ID to the db_instance to be deleted
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_delete_from_dbinstance_table(ophidiadb * oDB, int id_db);

/**
 * \brief Function that updates OphidiaDB adding fragment specified
 * \param oDB Pointer to OphidiaDB
 * \param fragment Pointer to fragment to be added
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_insert_into_fragment_table(ophidiadb * oDB, oph_odb_fragment * fragment);

/**
 * \brief Function to retrieve id of the container of a fragment
 * \param Pointer to OphidiaDB
 * \param frag_name name of the fragment
 * \param id_container ID of the container
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_container_id_from_fragment_name(ophidiadb * oDB, char *frag_name, int *id_container);

/**
 * \brief Function to retrieve id of the fragment from its name (that is unique)
 * \param Pointer to OphidiaDB
 * \param frag_name name of the fragment
 * \param id_fragment id of the fragment retrieved
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_fragment_id(ophidiadb * oDB, char *frag_name, int *id_fragment);

/**
 * \brief Function to retrieve id of a db from its name (that is unique)
 * \param Pointer to OphidiaDB
 * \param db_name name of the database
 * \param id_db ID of the database retrieved
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_dbinstance_id(ophidiadb * oDB, char *db_name, int *id_db);

/**
 * \brief Function to retrieve id of databases related to a datacube
 * \param Pointer to OphidiaDB
 * \param id_datacube Id of the datacube
 * \param id_dbs Pointer to be filled with the ids (it has to be freed)
 * \param size Pointer with the length of id_dbs
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_dbinstance_id_list_from_datacube(ophidiadb * oDB, int id_datacube, int **id_dbs, int *size);

/**
 * \brief Function to retrieve the list of ID of available DBMS instances
 * \param Pointer to OphidiaDB
 * \param ioserver_type I/O Server type to be used.
 * \param fs_type Filesystem type. It may be 0 (local disk), 1 (global) or 2 (inmemory)
 * \param host_partition Name of the host partition to be used
 * \param id_user User identifier
 * \param host_number Variable that contains the requested I/O hosts number
 * \param dbmsxhots_number Variable that contain the requested DBMS number per host
 * \param id_dbmss Pointer to be filled with the ids of the dbms instances (it has to be freed)
 * \param size Pointer with the length of id_dbmss
 * \param id_hosts Pointer to be filled with the ids of the hosts (it has to be freed)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_retrieve_dbmsinstance_id_list(ophidiadb * oDB, int fs_type, char *ioserver_type, char *host_partition, int id_user, int host_number, int dbmsxhost_number, int **id_dbmss, int *size,
					       int **id_hosts);

/**
 * \brief Function to unbook hosts
 * \param Pointer to OphidiaDB
 * \param host_partition Name of the host partition to be used
 * \param id_user User identifier
 * \param host_number Variable that contains the requested I/O hosts number
 * \param id_hosts Pointer to the ids of the hosts (it has to be freed)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_unbook_hosts(ophidiadb * oDB, char *host_partition, int id_user, int host_number, int *id_hosts);

/**
 * \brief Function to retrieve the number of datacubes stored in the database instance
 * \param oDB Pointer to the OphidiaDB
 * \param id_db Identifier of the database instance to check
 * \param datacubexdb_number Variable that will contain the datacube number in the database instance
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_get_number_of_datacube_for_db(ophidiadb * oDB, int id_db, int *datacubexdb_number);

/**
 * \brief Function that updates OphidiaDB adding the new db instances and new partition relations
 * \param oDB Pointer to OphidiaDB
 * \param db Pointer to db_instance
 * \param id_datacube ID of related Datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_stge_insert_into_dbinstance_partitioned_tables(ophidiadb * oDB, oph_odb_db_instance * db, int id_datacube);

int oph_odb_stge_add_hostpartition(ophidiadb * oDB, const char *name, int id_user, char reserved, int hosts, int *id_hostpartition);
int oph_odb_stge_add_all_hosts_to_partition(ophidiadb * oDB, int id_hostpartition, char reserved);
int oph_odb_stge_add_some_hosts_to_partition(ophidiadb * oDB, int id_hostpartition, int host_number, char reserved, int *num_rows);
int oph_odb_stge_add_host_to_partition(ophidiadb * oDB, int id_hostpartition, int id_host, char reserved);
int oph_odb_stge_delete_hostpartition(ophidiadb * oDB, const char *name, int id_user, char reserved, int *num_rows);
int oph_odb_stge_delete_hostpartition_by_id(ophidiadb * oDB, int id_hostpartition);

#endif				/* __OPH_ODB_STGE_H__ */
