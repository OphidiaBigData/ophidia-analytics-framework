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

#ifndef __OPH_DATACUBE_H__
#define __OPH_DATACUBE_H__

#include "config.h"

#include "query/oph_datacube_query.h"
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_DC_SUCCESS		0
#define OPH_DC_NULL_PARAM	1
#define OPH_DC_SERVER_ERROR	2
#define OPH_DC_DATA_ERROR	3

#define OPH_DC_DOUBLE_TYPE	OPH_COMMON_DOUBLE_TYPE
#define OPH_DC_FLOAT_TYPE	OPH_COMMON_FLOAT_TYPE
#define OPH_DC_INT_TYPE	OPH_COMMON_INT_TYPE
#define OPH_DC_LONG_TYPE	OPH_COMMON_LONG_TYPE
#define OPH_DC_SHORT_TYPE	OPH_COMMON_SHORT_TYPE
#define OPH_DC_BYTE_TYPE	OPH_COMMON_BYTE_TYPE
#define OPH_DC_BIT_TYPE	OPH_COMMON_BIT_TYPE

#define OPH_DC_DOUBLE_FLAG	OPH_COMMON_DOUBLE_FLAG
#define OPH_DC_FLOAT_FLAG	OPH_COMMON_FLOAT_FLAG
#define OPH_DC_INT_FLAG	OPH_COMMON_INT_FLAG
#define OPH_DC_LONG_FLAG	OPH_COMMON_LONG_FLAG
#define OPH_DC_SHORT_FLAG	OPH_COMMON_SHORT_FLAG
#define OPH_DC_BYTE_FLAG	OPH_COMMON_BYTE_FLAG
#define OPH_DC_BIT_FLAG	OPH_COMMON_BIT_FLAG

#define OPH_DC_DB_NAME		"db_%s_%d_%d_%d_%d"
#define OPH_DC_FRAG_NAME	"fact_%d_%d_%d"
#define OPH_DC_DB_FRAG_NAME	"%s.fact_%d_%d_%d"

/**
 * \brief Function to initialize I/O server
 * \param server Address of pointer to I/O server structure
 * \param server_type Type of I/O server to initialize
 */
int oph_dc_setup_dbms(oph_ioserver_handler ** server, char *server_type);

/**
 * \brief Function to initialize I/O server in a thread
 * \param server Address of pointer to I/O server structure
 * \param server_type Type of I/O server to initialize
 */
int oph_dc_setup_dbms_thread(oph_ioserver_handler ** server, char *server_type);

/**
 * \brief Function to finalize I/O server
 * \param server Pointer to I/O server structure
 */
int oph_dc_cleanup_dbms(oph_ioserver_handler * server);

/**
 * \brief Function to connect to dbms_instance. It doesn't connect to a DB. WARNING: Call this function before any other function
 * \param server Pointer to I/O server structure
 * \param m Pointer to dbms_instance to connect to
 * \param flag Value for client_flag of connection, it may be 0 if the option is unused
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_connect_to_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, unsigned long flag);

/**
 * \brief Function choose new current db. Call this function to use a database manged by a dbms
 * \param server Pointer to I/O server structure
 * \param m Pointer to dbms_instance that manages db
 * \param m Pointer to db_instance to switch on (may be null)
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_use_db_of_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, oph_odb_db_instance * db);

/**
 * \brief Function to check connect status to the DB. WARNING: Do not call this function (or any other) before calling connect_to_dbms
 * \param server Pointer to I/O server structure
 * \param m Pointer to dbms_instance that manages db
 * \param m Pointer to db_instance to switch on
 * \param flag Value for client_flag of connection in case of reconnection, it may be 0 if not used
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_check_connection_to_db(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, oph_odb_db_instance * db, unsigned long flag);

/** 
 * \brief Function to disconnect from dbms_instance
 * \param server Pointer to I/O server structure
 * \param m Pointer to dbms_instance to disconnect from
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_disconnect_from_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms);

/** 
 * \brief Function to create an empty phisical database
 * \param server Pointer to I/O server structure
 * \param m Pointer to db_instance to create
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_db(oph_ioserver_handler * server, oph_odb_db_instance * m);

/** 
 * \brief Function to delete an empty phisical database (checks if the database is empty)
 * \param server Pointer to I/O server structure
 * \param m Pointer to db_instance to delete
 * \return 0 if successfull, N otherwise
 */
int oph_dc_delete_db(oph_ioserver_handler * server, oph_odb_db_instance * m);

/** 
 * \brief Function to create an empty phisical table
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to create
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_empty_fragment(oph_ioserver_handler * server, oph_odb_fragment * m);

/** 
 * \brief Function to create an empty phisical table
 * \param server Pointer to I/O server structure
 * \param frag_name Name of new fragment
 * \param db_instance Pointer to db descriptor
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_empty_fragment_from_name(oph_ioserver_handler * server, const char *frag_name, oph_odb_db_instance * db_instance);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_param(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						 long long *start_id, char *param, long long param_size);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param block_size Block size used to access n- dimensional array
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_param2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						  long long *start_id, long long *block_size, char *param, long long param_size);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \param num Number of occurreces of the parameter
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_params(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						  long long *start_id, char *param, long long param_size, int num);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param block_size Block size used to access n- dimensional array
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \param num Number of occurreces of the parameter
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_params2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						   long long *start_id, long long *block_size, char *param, long long param_size, int num);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_aggregation(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						       long long *start_id, char *param, long long param_size);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param block_size Block size used to access n- dimensional array
 * \param param Paramater value
 * \param param_size Size in byte of the paramater
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query_with_aggregation2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
							long long *start_id, long long *block_size, char *param, long long param_size);

/** 
 * \brief Function to append new_frag to old_frag
 * \param server Pointer to I/O server structure
 * \param tot_rows Number of rows that will be inserted into the fragment
 * \param exec_flag Flag indicating if it is first frag to be appended (1), last frag (2), the only frag (3), or none of the previous (0) 
 * \param new_frag Pointer to new fragment to be extended
 * \param old_frag Pointer to old fragment to be added
 * \param first_id It will contain the ID of the first row inserted in the fragment
 * \param last_id It will contain the ID of the last row inserted
 * \param exec_query  Pointer containing intermediate query (do not set this field)
 * \param exec_args   Pointer containing intermediate args (do not set this field)
 * \return 0 if successfull, N otherwise
 */
int oph_dc_append_fragment_to_fragment(oph_ioserver_handler * server, unsigned long long tot_rows, short int exec_flag, oph_odb_fragment * new_frag, oph_odb_fragment * old_frag, long long *first_id,
				       long long *last_id, oph_ioserver_query ** exec_query, oph_ioserver_query_arg *** exec_args);

/** 
 * \brief Function to copy new_frag to old_frag and apply a binary primitive on the result
 * \param server Pointer to I/O server structure
 * \param tot_rows Number of rows that will be inserted into the fragment
 * \param old_frag1 Pointer to first input fragment to be extended
 * \param old_frag2 Pointer to second input fragment to be added
 * \param frag_name Name of output fragment
 * \param compressed Flag set in case input cubes are compressed; output cube will be compressed as well
 * \param operation Operation to be applied
 * \param measure_type Data type of input cubes
 * \return 0 if successfull, N otherwise
 */
int oph_dc_copy_and_process_fragment(oph_ioserver_handler * server, unsigned long long tot_rows, oph_odb_fragment * old_frag1, oph_odb_fragment * old_frag2, const char *frag_name, int compressed,
				     const char *operation, const char *measure_type);

/** 
 * \brief Function to populate a phisical table with random values
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to populate
 * \param tuple_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param algorithm Type of algorithm used for random number generation
 * \return 0 if successfull, N otherwise
 */
int oph_dc_populate_fragment_with_rand_data(oph_ioserver_handler * server, oph_odb_fragment * m, int tuple_number, int array_length, char *data_type, int compressed, char *algorithm);

/** 
 * \brief Function to run query to build a table with random values
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to populate
 * \param tuple_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param algorithm Type of algorithm used for random number generation
 * \return 0 if successfull, N otherwise
 */
int oph_dc_populate_fragment_with_rand_data2(oph_ioserver_handler * server, oph_odb_fragment * m, int tuple_number, int array_length, char *data_type, int compressed, char *algorithm);

/** 
 * \brief Function to read a physical table with filtering parameters
 * \param server Pointer to I/O server structure
 * \param frag Pointer to fragment to read
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param compressed If the data is compressed (1) or not (0)
 * \param id_clause Clause used to read dimension IDs
 * \param array_clause Clause used to subset the array
 * \param where_clause Clause used to subset rows
 * \param limit Number of rows to be shown
 * \param raw_format If the data is retreived in raw format not human readable (1) or not (0) 
 * \param frag_rows Pointer to result set to fill (it has to be freed with oph_ioserver_free_result)
 * \return 0 if successfull, N otherwise
 */
int oph_dc_read_fragment_data(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, char *id_clause, char *array_clause, char *where_clause, int limit,
			      int raw_format, oph_ioserver_result ** frag_rows);

/** 
 * \brief Function to count the number of elements in the fragment
 * \param server Pointer to I/O server structure
 * \param frag Pointer to fragment to read
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param compressed If the data is compressed (1) or not (0)
 * \param count Pointer to integer containing the number of elements
 * \return 0 if successfull, N otherwise
 */
int oph_dc_get_total_number_of_elements_in_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, long long *count);

/** 
 * \brief Function to count the number of rows in the fragment
 * \param server Pointer to I/O server structure
 * \param frag Pointer to fragment to read
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param count Pointer to integer containing the number of elements
 * \return 0 if successfull, N otherwise
 */
int oph_dc_get_total_number_of_rows_in_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, long long *count);

/** 
 * \brief Function to count the number of elements in a single row of the fragment
 * \param server Pointer to I/O server structure
 * \param frag Pointer to fragment to read
 * \param data_type Type of data to be inserted INT, FLOAT, DOUBLE (default DOUBLE)
 * \param compressed If the data is compressed (1) or not (0)
 * \param count Pointer to integer containing the number of elements
 * \return 0 if successfull, N otherwise
 */
int oph_dc_get_number_of_elements_in_fragment_row(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, long long *length);

/** 
 * \brief Function to retrive primitives from an instance
 * \param server Pointer to I/O server structure
 * \param dbms Structure containing info of DBMS instance
 * \param frag_name Name of fragment to filter on
 * \param frag_rows List of result retrieved
 * \return 0 if successfull, N otherwise
 */
int oph_dc_get_primitives(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, char *frag_name, oph_ioserver_result ** frag_rows);

/** 
 * \brief Function to compute fragments size
 * \param server Pointer to I/O server structure
 * \param dbms Pointer to dbms_instance that contains the fragments
 * \param frag_name String of fragment names to be computed
 * \param count Pointer to integer containing the fragments size
 * \return 0 if successfull, N otherwise
 */
int oph_dc_get_fragments_size_in_bytes(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, char *frag_name, long long *size);

/** 
 * \brief Function to delete a phisical table
 * \param server Pointer to I/O server structure
 * \param m Pointer to fragment to delete
 * \return 0 if successfull, N otherwise
 */
int oph_dc_delete_fragment(oph_ioserver_handler * server, oph_odb_fragment * m);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param old_frag Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long *start_id);

/** 
 * \brief Function to create a new fragment from old_frag applying the operation query
 * \param server Pointer to I/O server structure
 * \param old_frag Pointer to fragment to copy
 * \param new_frag_name Name of new fragment to create (NULL is admitted)
 * \param operation Composition of plugins to be applied to measure or Generic query or Stored procedure and related parameters
 * \param where Where clause (NULL is admitted)
 * \param aggregate_number Number of elements to be aggregated (default value is 1, NULL is admitted)
 * \param start_id Initial value of id_dim to be used
 * \param block_size Block size used to access n- dimensional array
 * \return 0 if successfull, N otherwise
 */
int oph_dc_create_fragment_from_query2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long *start_id,
				       long long *block_size);

/**
 * \brief Function to generate a new fragment name 
 * \param db_name Name of the db instance where the fragment is created (it may be NULL)
 * \param id_datacube ID of the new datacube created
 * \param proc_rank Rank of the process creating the fragment
 * \param frag_number Progressive number of fragment related to the process
 * \param frag_name Pointer to an empty buffer to be filled with the fragment name
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_generate_fragment_name(char *db_name, int id_datacube, int proc_rank, int frag_number, char (*frag_name)[OPH_ODB_STGE_FRAG_NAME_SIZE]);

/**
 * \brief Function to generate a new database instance name 
 * \param odb_name Name of the OphidiaDB where the db instance is created
 * \param id_datacube ID of the new datacube created
 * \param id_dbms ID of the DBMS where the db instance is created
 * \param proc_rank Rank of the process creating the db
 * \param frag_number Progressive number of db related to the process
 * \param frag_name Pointer to an empty buffer to be filled with the db instance name
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_generate_db_name(char *odb_name, int id_datacube, int id_dbms, int proc_rank, int db_number, char (*db_name)[OPH_ODB_STGE_DB_NAME_SIZE]);

/**
 * \brief Function to check measure data type
 * \param input_type Data type to be checked
 * \return 0 if successfull, -1 otherwise
 */
int oph_dc_check_data_type(char *input_type);

#endif				/* __OPH_DATACUBE_H__ */
