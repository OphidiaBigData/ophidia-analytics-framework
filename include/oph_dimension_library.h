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

#ifndef __OPH_DIMENSION_H__
#define __OPH_DIMENSION_H__

#include "query/oph_dimension_query.h"
#include "oph_common.h"
#include "oph_ophidiadb_main.h"

#include <time.h>

#define OPH_DIM_TABLE_NAME_MACRO 	"container_%d"
#define OPH_DIM_TABLE_LABEL_MACRO	"container_%d_label"

#define OPH_DIM_SUCCESS 		0
#define OPH_DIM_NULL_PARAM 		1
#define OPH_DIM_MYSQL_ERROR		2
#define OPH_DIM_DATA_ERROR 		3
#define OPH_DIM_TIME_PARSING_ERROR	4
#define OPH_DIM_SYSTEM_ERROR	5

#define OPH_DIM_DOUBLE_TYPE		OPH_COMMON_DOUBLE_TYPE
#define OPH_DIM_FLOAT_TYPE 		OPH_COMMON_FLOAT_TYPE
#define OPH_DIM_BYTE_TYPE 		OPH_COMMON_BYTE_TYPE
#define OPH_DIM_SHORT_TYPE 		OPH_COMMON_SHORT_TYPE
#define OPH_DIM_INT_TYPE 		OPH_COMMON_INT_TYPE
#define OPH_DIM_LONG_TYPE 		OPH_COMMON_LONG_TYPE

#define OPH_DIM_DOUBLE_FLAG		OPH_COMMON_DOUBLE_FLAG
#define OPH_DIM_FLOAT_FLAG		OPH_COMMON_FLOAT_FLAG
#define OPH_DIM_BYTE_FLAG		OPH_COMMON_BYTE_FLAG
#define OPH_DIM_SHORT_FLAG		OPH_COMMON_SHORT_FLAG
#define OPH_DIM_INT_FLAG		OPH_COMMON_INT_FLAG
#define OPH_DIM_LONG_FLAG		OPH_COMMON_LONG_FLAG

#define OPH_DIM_BUFFER_LEN 		256
#define OPH_DIM_PATH_LEN 		1000

#define OPH_CONF_DIMDB_NAME		"DIMDB_NAME"
#define OPH_CONF_DIMDB_HOST		"DIMDB_HOST"
#define OPH_CONF_DIMDB_PORT		"DIMDB_PORT"
#define OPH_CONF_DIMDB_LOGIN	"DIMDB_LOGIN"
#define OPH_CONF_DIMDB_PWD		"DIMDB_PWD"

#define OPH_DIM_SUBSET_SEPARATOR1	","
#define OPH_DIM_SUBSET_SEPARATOR	OPH_DIM_SUBSET_SEPARATOR1"_"
#define OPH_DIM_SUBSET_SEPARATOR2	':'

/**
 * \brief Function to read dimension info from configuration file
 * \param db Pointer to allocated db_instance structure
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_load_dim_dbinstance(oph_odb_db_instance * db);

/**
 * \brief Function to connect to dbms_instance. It doesn't connect to a DB. WARNING: Call this function before any other function
 * \param dbms Pointer to dbms_instance to connect to
 * \param flag Value for client_flag of mysql_real_connect(), it may be 0 if the option is unused
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_connect_to_dbms(oph_odb_dbms_instance * dbms, unsigned long flag);

/**
 * \brief Function choose new current db. Call this function to use a database manged by a dbms
 * \param dbms1 Pointer to dbms_instance that manages db
 * \param dbms2 Pointer to db_instance to switch on (may be null)
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_use_db_of_dbms(oph_odb_dbms_instance * dbms1, oph_odb_db_instance * dbms2);

/**
 * \brief Function to check connect status to the DB. WARNING: Do not call this function (or any other) before calling connect_to_dbms
 * \param dbms Pointer to dbms_instance that manages db
 * \param db Pointer to db_instance to switch on
 * \param flag Value for client_flag of mysql_real_connect() in case of reconnection, it may be 0 if not used
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_check_connection_to_db(oph_odb_dbms_instance * dbms, oph_odb_db_instance * db, unsigned long flag);

/**
 * \brief Function to disconnect from dbms_instance
 * \param dbms Pointer to dbms_instance to disconnect from
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_disconnect_from_dbms(oph_odb_dbms_instance * dbms);

/**
 * \brief Function to create an empty dimension database
 * \param db Pointer to db_instance to create
 * \return 0 if successfull, N otherwise
 */
int oph_dim_create_db(oph_odb_db_instance * db);

/**
 * \brief Function to delete an empty dimension database (checks if the database is empty)
 * \param db Pointer to db_instance to delete
 * \return 0 if successfull, N otherwise
 */
int oph_dim_delete_db(oph_odb_db_instance * db);

/**
 * \brief Function to create an empty dimension table
 * \param db Pointer to db_instance used for new table
 * \param dimension_table_name Name of new dimension table
 * \return 0 if successfull, N otherwise
 */
int oph_dim_create_empty_table(oph_odb_db_instance * db, char *dimension_table_name);

/**
 * \brief Function to retrieve a dimension row from a dimension table
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of dimension table
 * \param dimension_id ID of the dimension to be found
 * \param dim_row Binary dimension array row to be found
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_retrieve_dimension(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char **dim_row);

/**
 * \brief Function that updates a dimension table adding new dimensions (if it not exists)
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dim_type Type of dimension to be inserted
 * \param dim_size Length of dimension to be inserted
 * \param dim_row Binary dimension array row to be added
 * \param dimension_id Id of last inserted dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_insert_into_dimension_table(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *dim_row, int *dimension_id);

/**
 * \brief Function that updates a dimension table adding new dimensions (if it not exists)
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dim_type Type of dimension to be inserted
 * \param dim_size Length of dimension to be inserted
 * \param query Query to be executed on dim_row before inserting the data
 * \param dim_row Binary dimension array row to be added
 * \param dimension_id Id of last inserted dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_insert_into_dimension_table_from_query(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *query, char *dim_row, int *dimension_id);

/**
 * \brief Function that check if the values of a dimension correspond to those of another dimension
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dim_type Type of dimension to be compared
 * \param dim_size Length of dimension to be compared
 * \param dim_row Binary dimension array row to be compared
 * \param dimension_id ID of the dimension to be found
 * \param match Flag indicating if the output of the comparison
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_compare_dimension(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *dim_row, int dimension_id, int *match);

/**
 * \brief Function that check if the values of a dimension correspond to those of another dimension
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dim_type Type of dimension to be compared
 * \param dim_size Length of dimension to be compared
 * \param apply_clause Filtering clause on the array
 * \param dim_row Binary dimension array row to be compared
 * \param dimension_id ID of the dimension to be found
 * \param match Flag indicating if the output of the comparison
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_compare_dimension2(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *apply_clause, char *dim_row, int dimension_id, int *match);

/**
 * \brief Function to check if a dimension table exists
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param exist_flag It is set to 1 if the table exists otherwise it is set to 0
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_check_if_dimension_table_exists(oph_odb_db_instance * db, char *dimension_table_name, int *exist_flag);

/**
 * \brief Function to populate a dimension table with random values [1;100)
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dimension_type Type of dimension to be inserted
 * \param dimension_size Length of dimension to be inserted
 * \param dimension_id Id of last inserted dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_insert_into_dimension_table_rand_data(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, long long dim_size, int *dimension_id);

/**
 * \brief Function to read a dimension table with filtering parameters
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dimension_id Id of the row to be read
 * \param array_clause Clause used to handle the array
 * \param compressed Select if data are compressed
 * \param dim_row Binary dimension array row to be read
 * \return 0 if successfull, N otherwise
 */
int oph_dim_read_dimension_data(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char *array_clause, int compressed, char **dim_row);


/**
 * \brief Function to read a dimension table data as strings
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dimension_id Id of the row to be read
 * \param dimension_type Type of dimension to be read
 * \param compressed Select if data are compressed
 * \param dim_row Binary dimension array row to be read
 * \return 0 if successfull, N otherwise
 */
int oph_dim_read_dimension(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, int dimension_id, int compressed, char **dim_row);

/**
 * \brief Function to read a dimension table with filtering parameters
 * \param db Pointer to db_instance used for dimension table
 * \param dimension_table_name Name of new dimension table
 * \param dimension_id Id of the row to be read
 * \param apply_clause Filtering clause on the array
 * \param compressed Select if data are compressed
 * \param dim_row In: Binary vector of indexes; Out: binary dimension array row to be read
 * \param dim_type Type of dimension to be extracted
 * \return 0 if successfull, N otherwise
 */
int oph_dim_read_dimension_filtered_data(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char *apply_clause, int compressed, char **dim_row, char *data_type,
					 long long dim_size);

/**
 * \brief Function to delete a dimension table
 * \param db Pointer to db_instance used for new table
 * \param dimension_table_name Name of dimension table
 * \return 0 if successfull, N otherwise
 */
int oph_dim_delete_table(oph_odb_db_instance * db, char *dimension_table_name);

/**
 * \brief Function to free db instance
 * \param db Pointer to db_instance used for dimension
 * \return 0 if successfull, N otherwise
 */
int oph_dim_unload_dim_dbinstance(oph_odb_db_instance * db);

/**
 * \brief Function to check dimensions data type
 * \param input_type Data type to be checked
 * \param size Pointer to a variable which the data size will be 
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_check_data_type(char *input_type, int *size);

/**
 * \brief Function to evaluate the reduction group for time dimensions
 * \param dim_row Dimension array
 * \param kk Index of dimension value to be classified
 * \param dim Dimension metadata
 * \param concept_level_out Frequency to be obtained
 * \param tm_prev Pointer to an element of target group (in/out param)
 * \param midnight Flag related to mode for edge aggregation
 * \param evaluate_centroid If 1 then dim_row[kk] will be replaced with the centroid of standard interval which the element belongs to
 * \param res Output flag
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_is_in_time_group_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, char concept_level_out, struct tm *tm_prev, int midnight, int evaluate_centroid, int *res);

/**
 * \brief Function to evaluate the centroid using mean of original values
 * \param dim_row Dimension array
 * \param dimension_type Dimension data type
 * \param first Index of the first element of the set which the centroid refers to
 * \param last Index of the last element of the set which the centroid refers to
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_update_value(char *dim_row, const char *dimension_type, unsigned int first, unsigned int last);

/**
 * \brief Function to evaluate the time subset string from a season subset string
 * \param subset_string Subset string to be parsed
 * \param dim Dimension metadata
 * \param output_string Char vector to be used for output subset string; it must be already allocated
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_parse_season_subset(const char *subset_string, oph_odb_dimension * dim, char *output_string, char *data, unsigned long long data_size);

/**
 * \brief Function to evaluate the traditional subset string from a time subset string
 * \param subset_string Subset string to be parsed
 * \param dim Dimension metadata
 * \param output_string Char vector to be used for output subset string; it must be already allocated
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_parse_time_subset(const char *subset_string, oph_odb_dimension * dim, char *output_string);

/**
 * \brief Function to extract the datetime string associated to a time dimension value
 * \param dim_row Dimension array
 * \param kk Index of dimension value to be classified
 * \param dim Dimension metadata
 * \param tm_time Time struct to save output; it must be already allocated
 * \param base_time Base time (in sec) evaluated internally (it could be NULL)
 * \param raw_value Time value in seconds (it could be NULL)
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_get_time_value_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, struct tm *tm_time, long long *base_time, long long *raw_value);

/**
 * \brief Function to extract the datetime string associated to a time dimension value
 * \param dim_row Dimension array
 * \param kk Index of dimension value to be classified
 * \param dim Dimension metadata
 * \param raw_value Time value in seconds
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_set_time_value_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, long long raw_value);

/**
 * \brief Function to extract the datetime string associated to a time dimension value
 * \param dim_row Dimension array
 * \param kk Index of dimension value to be classified
 * \param dim Dimension metadata
 * \param output_string Char vector to be used for output subset string; it must be already allocated
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_get_time_string_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, char *output_string);

/**
 * \brief Function that updates a dimension table adding new dimensions (if it not exists)
 * \param db Pointer to db_instance used for dimension table
 * \param from_dimension_table_name Name of input dimension table
 * \param to_dimension_table_name Name of output dimension table
 * \param dimension_id Id of dimension to be copied; it will contain the id of dimension in new table at the end of the function
 * \return 0 if successfull, -1 otherwise
 */
int oph_dim_copy_into_dimension_table(oph_odb_db_instance * db, char *from_dimension_table_name, char *to_dimension_table_name, int *dimension_id);

#endif
