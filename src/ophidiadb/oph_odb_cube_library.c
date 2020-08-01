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

#include "oph_odb_cube_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <mysql.h>
#include "debug.h"

#include "oph_pid_library.h"
#include "oph_odb_metadata_library.h"

extern int msglevel;

int oph_odb_cube_init_datacube(oph_odb_datacube * cube)
{
	if (!cube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	cube->id_db = NULL;
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_free_datacube(oph_odb_datacube * cube)
{
	if (!cube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (cube->id_db) {
		free(cube->id_db);
		cube->id_db = NULL;
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_get_datacube_dbmsinstance_number(ophidiadb * oDB, int id_datacube, int *num_elements)
{
	if (!oDB || !id_datacube || !num_elements) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_DBMS_NUMBER, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		if (row[0])
			*num_elements = strtoll(row[0], NULL, 10);
		else
			*num_elements = 0;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_get_datacube_size(ophidiadb * oDB, int id_datacube, long long int *size)
{
	if (!oDB || !id_datacube || !size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_SIZE, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		if (row[0])
			*size = strtoll(row[0], NULL, 10);
		else
			*size = 0;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_get_datacube_num_elements(ophidiadb * oDB, int id_datacube, long long int *num_elements)
{
	if (!oDB || !id_datacube || !num_elements) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_ELEMENTS, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		if (row[0])
			*num_elements = strtoll(row[0], NULL, 10);
		else
			*num_elements = 0;
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_set_datacube_num_elements(ophidiadb * oDB, int id_datacube, long long int num_elements)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_DATACUBE_ELEMENTS, num_elements, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_set_datacube_size(ophidiadb * oDB, int id_datacube, long long int size)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_DATACUBE_SIZE, size, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_set_folder(ophidiadb * oDB, int id_datacube, int id_folder)
{
	if (!oDB || !id_datacube || !id_folder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_DATACUBE_FOLDER, id_folder, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_retrieve_source(ophidiadb * oDB, int id_src, oph_odb_source * src)
{
	if (!oDB || !src || !id_src) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_SOURCE, id_src);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		src->id_source = id_src;
		memset(&(src->uri), 0, OPH_ODB_CUBE_SOURCE_URI_SIZE + 1);
		strncpy(src->uri, row[0], OPH_ODB_CUBE_SOURCE_URI_SIZE);
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int _oph_odb_cube_retrieve_datacube(ophidiadb * oDB, int id_datacube, oph_odb_datacube * cube, int partition_mode)
{
	if (!oDB || !id_datacube || !cube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_CUBE, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 12) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		cube->id_datacube = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		cube->id_container = (row[1] ? (int) strtol(row[1], NULL, 10) : 0);
		cube->hostxdatacube = (row[2] ? (int) strtol(row[2], NULL, 10) : 0);
		cube->fragmentxdb = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		cube->tuplexfragment = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		memset(&(cube->measure), 0, OPH_ODB_CUBE_MEASURE_SIZE + 1);
		strncpy(cube->measure, row[5], OPH_ODB_CUBE_MEASURE_SIZE);
		memset(&(cube->measure_type), 0, OPH_ODB_CUBE_MEASURE_TYPE_SIZE + 1);
		strncpy(cube->measure_type, row[6], OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
		memset(&(cube->frag_relative_index_set), 0, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE + 1);
		strncpy(cube->frag_relative_index_set, row[7], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		cube->compressed = (row[8] ? (int) strtol(row[8], NULL, 10) : 0);
		cube->level = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
		cube->id_source = (row[10] ? (int) strtol(row[10], NULL, 10) : 0);
		cube->id_folder = (row[11] ? (int) strtol(row[11], NULL, 10) : 0);
	}
	mysql_free_result(res);

	//partitioned table data
	n = snprintf(query, MYSQL_BUFLEN, partition_mode ? MYSQL_QUERY_CUBE_RETRIEVE_PART : MYSQL_QUERY_CUBE_RETRIEVE_PART2, cube->id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	res = mysql_store_result(oDB->conn);

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}


	cube->db_number = mysql_num_rows(res);
	if (!(cube->id_db = (int *) malloc(cube->db_number * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i = 0;
	while ((row = mysql_fetch_row(res)) != NULL) {
		cube->id_db[i] = (int) strtol(row[0], NULL, 10);
		i++;
	}

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_retrieve_datacube(ophidiadb * oDB, int id_datacube, oph_odb_datacube * cube)
{
	return _oph_odb_cube_retrieve_datacube(oDB, id_datacube, cube, 1);
}

int oph_odb_cube_retrieve_datacube_with_ordered_partitions(ophidiadb * oDB, int id_datacube, oph_odb_datacube * cube)
{
	return _oph_odb_cube_retrieve_datacube(oDB, id_datacube, cube, 0);
}

int oph_odb_cube_find_datacube_additional_info(ophidiadb * oDB, int id_datacube, char **creationdate, char **description)
{
	if (!oDB || !id_datacube || !creationdate || !description) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_CUBE_ADDITIONAL_INFO, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_num_rows(res) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		if (row[0])
			if (!(*creationdate = (char *) strndup(row[0], strlen(row[0])))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				mysql_free_result(res);
				return OPH_ODB_MEMORY_ERROR;
			}
		if (row[1])
			if (!(*description = (char *) strndup(row[1], strlen(row[1])))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				mysql_free_result(res);
				return OPH_ODB_MEMORY_ERROR;
			}
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_retrieve_cubehasdim_list(ophidiadb * oDB, int id_datacube, oph_odb_cubehasdim ** cubedims, int *dim_num)
{
	if (!oDB || !id_datacube || !cubedims || !dim_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_CUBEHASDIM, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_field_count(oDB->conn) != 6) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int number_of_dims = mysql_num_rows(res);
	if (!(number_of_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}

	if (!(*cubedims = (oph_odb_cubehasdim *) malloc(number_of_dims * sizeof(oph_odb_cubehasdim)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i = 0;
	*dim_num = number_of_dims;
	while ((row = mysql_fetch_row(res)) != NULL) {
		(*cubedims)[i].id_cubehasdim = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		(*cubedims)[i].id_dimensioninst = (row[1] ? (int) strtol(row[1], NULL, 10) : 0);
		(*cubedims)[i].id_datacube = (row[2] ? (int) strtol(row[2], NULL, 10) : 0);
		(*cubedims)[i].explicit_dim = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		(*cubedims)[i].level = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		(*cubedims)[i].size = (row[5] ? (int) strtol(row[5], NULL, 10) : 0);
		i++;
	}
	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_find_datacube_hierarchy(ophidiadb * oDB, int direction, int id_datacube, MYSQL_RES ** information_list)
{
	(*information_list) = NULL;

	if (!oDB || !information_list || !id_datacube || direction < 0 || direction > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	int n;
	char query[MYSQL_BUFLEN];

	if (!direction)
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_CHILDREN, id_datacube);
	else
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_PARENTS, id_datacube);

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Execute query
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	*information_list = mysql_store_result(oDB->conn);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_find_task_list(ophidiadb * oDB, int folder_id, int datacube_id, char *operator, MYSQL_RES ** information_list)
{
	(*information_list) = NULL;

	if (!oDB || !information_list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;
	char where_clause[MYSQL_BUFLEN];

	//Set values to correct number
	int p1 = datacube_id;
	char *p2 = operator;

	n = 0;
	where_clause[0] = 0;

	if (p1 || p2)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (p1)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "(i.iddatacube = %d OR o.iddatacube = %d) ", p1, p1);
	if (p2 && p1)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "AND ");
	if (p2)
		n += snprintf(where_clause + n, MYSQL_BUFLEN, "operation LIKE '%s' ", p2);

	n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_TASK_LIST, folder_id, where_clause);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		//free(buffer);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Execute query
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		//free(buffer);
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	*information_list = mysql_store_result(oDB->conn);
	//free(buffer);
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_insert_into_source_table(ophidiadb * oDB, oph_odb_source * src, int *last_insertd_id)
{
	if (!oDB || !src || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*last_insertd_id = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;

	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_SOURCE, src->uri);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {

		pmesg(LOG_DEBUG, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));

		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_SOURCE_ID, src->uri);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}

		if (mysql_query(oDB->conn, insertQuery)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}

		MYSQL_RES *res;
		MYSQL_ROW row;
		res = mysql_store_result(oDB->conn);
		if (mysql_num_rows(res) == 1 && mysql_field_count(oDB->conn) == 1) {
			row = mysql_fetch_row(res);
			*last_insertd_id = (int) strtol(row[0], NULL, 10);
		}
		mysql_free_result(res);

		if (!*last_insertd_id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
			return OPH_ODB_TOO_MANY_ROWS;
		}

	} else if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_insert_into_datacube_partitioned_tables(ophidiadb * oDB, oph_odb_datacube * cube, int *last_insertd_id)
{
	if (!oDB || !cube || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;
	if (cube->id_source) {
		if (strlen(cube->description))
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_2, cube->id_folder, cube->id_container, cube->hostxdatacube, cube->fragmentxdb,
				     cube->tuplexfragment, cube->measure, cube->measure_type, cube->frag_relative_index_set, cube->compressed, cube->level, cube->id_source, cube->description);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE, cube->id_folder, cube->id_container, cube->hostxdatacube, cube->fragmentxdb,
				     cube->tuplexfragment, cube->measure, cube->measure_type, cube->frag_relative_index_set, cube->compressed, cube->level, cube->id_source);
	} else {
		if (strlen(cube->description))
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_3, cube->id_folder, cube->id_container, cube->hostxdatacube, cube->fragmentxdb,
				     cube->tuplexfragment, cube->measure, cube->measure_type, cube->frag_relative_index_set, cube->compressed, cube->level, cube->description);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_1, cube->id_folder, cube->id_container, cube->hostxdatacube, cube->fragmentxdb,
				     cube->tuplexfragment, cube->measure, cube->measure_type, cube->frag_relative_index_set, cube->compressed, cube->level);
	}
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(cube->id_datacube = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
	*last_insertd_id = cube->id_datacube;

	//If datacube has to update partition table
	if (cube->id_db) {
		int i;
		for (i = 0; i < cube->db_number; i++) {
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_PART, cube->id_db[i], cube->id_datacube);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}
			if (oph_odb_query_ophidiadb(oDB, insertQuery)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
		}
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_insert_into_task_table(ophidiadb * oDB, oph_odb_task * new_task, int *last_insertd_id)
{
	if (!oDB || !new_task || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n, i, j;
	char new_query[OPH_ODB_CUBE_OPERATION_QUERY_SIZE];

	if (!new_task->input_cube_number)
		new_task->input_cube_number = 1;

	if (new_task->query[0] != 0) {
		//Escape ' char with \'
		j = 0;
		for (i = 0; i < OPH_ODB_CUBE_OPERATION_QUERY_SIZE; i++) {
			if (new_task->query[i] == '\'') {
				new_query[j++] = '\\';
				new_query[j] = new_task->query[i];
			} else
				new_query[j] = new_task->query[i];
			j++;
			if (j >= OPH_ODB_CUBE_OPERATION_QUERY_SIZE - 1)
				break;
		}
		new_query[OPH_ODB_CUBE_OPERATION_QUERY_SIZE - 1] = 0;
	}
	if (new_task->id_job) {
		if (new_task->query[0] != 0)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_2_job, new_task->id_outputcube, new_task->operator, new_query, new_task->id_job,
				     new_task->input_cube_number);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_1_job, new_task->id_outputcube, new_task->operator, new_task->id_job,
				     new_task->input_cube_number);
	} else {
		if (new_task->query[0] != 0)
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_2, new_task->id_outputcube, new_task->operator, new_query, new_task->input_cube_number);
		else
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_1, new_task->id_outputcube, new_task->operator, new_task->input_cube_number);

	}

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
	//If task has to update hasinput table
	if (new_task->id_inputcube) {
		int i;
		for (i = 0; i < new_task->input_cube_number; i++) {
			n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_HASINPUT, new_task->id_inputcube[i], *last_insertd_id);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}

			if (mysql_query(oDB->conn, insertQuery)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
		}
	}
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_insert_into_cubehasdim_table(ophidiadb * oDB, oph_odb_cubehasdim * cubedim, int *last_insertd_id)
{
	if (!oDB || !cubedim || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*last_insertd_id = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBEHASDIM, cubedim->id_dimensioninst, cubedim->id_datacube, cubedim->explicit_dim, cubedim->level);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted cubehasdim id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	cubedim->id_cubehasdim = *last_insertd_id;

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_update_level_in_cubehasdim_table(ophidiadb * oDB, int level, int id_cubehasdim)
{
	if (!oDB || !id_cubehasdim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char updateQuery[MYSQL_BUFLEN];
	int n = snprintf(updateQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_LEVEL_OF_CUBEHASDIM, level, id_cubehasdim);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, updateQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_delete_from_datacube_table(ophidiadb * oDB, int id_datacube)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];
	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_DELETE_OPHIDIADB_CUBE, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (oph_odb_query_ophidiadb(oDB, deleteQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_check_if_datacube_not_present_by_pid(ophidiadb * oDB, const char *uri, const int id_container, const int id_datacube, int *exists)
{
	*exists = 0;

	if (!oDB || !uri || !id_container || !id_datacube || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		if (tmp_uri)
			free(tmp_uri);
		return OPH_ODB_ERROR;
	}
	if (strncmp(uri, tmp_uri, strlen(tmp_uri)) || strncmp(uri, tmp_uri, strlen(uri))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Input URI isn't correct.\n");
		free(tmp_uri);
		return OPH_ODB_ERROR;
	}
	free(tmp_uri);

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_CHECK_CUBE_PID, id_container, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	res = mysql_store_result(oDB->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found in query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	mysql_free_result(res);

	*exists = 1;
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_check_datacube_availability(ophidiadb * oDB, int id_input, int type, int *status)
{
	if (!oDB || !id_input || !status) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*status = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n;
	if (type)
		n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_CHECK_DATACUBE_STORAGE_STATUS_2, id_input);
	else
		n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_CHECK_DATACUBE_STORAGE_STATUS, id_input);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	res = mysql_store_result(oDB->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found in query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}
	MYSQL_ROW row = mysql_fetch_row(res);
	*status = ((int) strtol(row[0], NULL, 10) > 0 ? 0 : 1);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_retrieve_datacube_id_list_from_dbinstance(ophidiadb * oDB, int id_db, int **id_datacubes, int *size)
{
	if (!oDB || !id_datacubes || !size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_PARTITIONED_DATACUBE, id_db);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	int rows;
	rows = mysql_num_rows(res);

	if (!(*id_datacubes = (int *) malloc(rows * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int i = 0;
	*size = rows;
	while ((row = mysql_fetch_row(res))) {
		(*id_datacubes)[i] = (int) strtol(row[0], NULL, 10);
		i++;
	}

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_retrieve_datacube_measure(ophidiadb * oDB, int id_datacube, char *measure)
{
	if (!oDB || !id_datacube || !measure) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		//logging(LOG_ERROR, __FILE__, __LINE__,0,"Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_RETRIEVE_CUBE_MEASURE, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		//logging(LOG_ERROR, __FILE__, __LINE__,0,"Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		//logging(LOG_ERROR, __FILE__, __LINE__,0,"MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		//logging(LOG_ERROR, __FILE__, __LINE__,0,"No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		//logging(LOG_ERROR, __FILE__, __LINE__,0,"Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	row = mysql_fetch_row(res);
	snprintf(measure, OPH_COMMON_BUFFER_LEN, "%s", row[0]);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_update_tuplexfragment(ophidiadb * oDB, int id_datacube, int tuplexfragment)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];
	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TUPLEXFRAGMENT, tuplexfragment, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, deleteQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_cube_order_by(ophidiadb * oDB, int order, int *id_datacube, int id_datacube_num)
{
	if (!oDB || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (!order || (id_datacube_num < 2))
		return OPH_ODB_SUCCESS;

	int n = 0, cc, nn = 0;
	size_t max_size = id_datacube_num * (OPH_COMMON_MAX_INT_LENGHT + 2);
	char cube_list[max_size];
	for (cc = 0; cc < id_datacube_num; ++cc)
		nn += snprintf(cube_list + nn, max_size - nn, "%s%d", cc ? "," : "", id_datacube[2 * cc + 1]);

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	switch (order) {
		case 1:
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_ORDER_CUBE_BY_SOURCE, cube_list);
			break;
		default:
			return OPH_ODB_SUCCESS;
	}
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	MYSQL_ROW row;
	res = mysql_store_result(oDB->conn);

	if (mysql_field_count(oDB->conn) != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query.\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int num_rows = mysql_num_rows(res);
	if (num_rows != id_datacube_num) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Cubes cannot be ordered by source.\n");
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}

	cc = 0;
	while ((row = mysql_fetch_row(res))) {
		id_datacube[2 * cc] = (int) strtol(row[0], NULL, 10);
		id_datacube[2 * cc + 1] = (int) strtol(row[1], NULL, 10);
		cc++;
	}

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}
