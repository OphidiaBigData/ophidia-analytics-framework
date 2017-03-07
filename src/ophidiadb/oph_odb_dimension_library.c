/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#include "oph_odb_dimension_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <mysql.h>
#include "debug.h"

#include "oph_odb_metadata_library.h"
#include "oph_hierarchy_library.h"

extern int msglevel;

int oph_odb_dim_retrieve_full_dimension_info(ophidiadb * oDB, int id_dimensioninst, oph_odb_dimension * dim, oph_odb_dimension_instance * dim_inst, oph_odb_dimension_grid * dim_grid,
					     oph_odb_hierarchy * hier, int id_datacube)
{
	if (!oDB || !id_dimensioninst || !dim || !dim_inst || !dim_grid || !hier) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_FULL_DIMENSION, id_dimensioninst);
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
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No/more than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 21) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		hier->id_hierarchy = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		memset(&(hier->hierarchy_name), 0, OPH_ODB_DIM_HIERARCHY_SIZE + 1);
		if (row[1])
			strncpy(hier->hierarchy_name, row[1], OPH_ODB_DIM_HIERARCHY_SIZE);
		memset(&(hier->filename), 0, OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE + 1);
		if (row[2])
			strncpy(hier->filename, row[2], OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE);
		dim->id_dimension = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		dim->id_container = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		memset(&(dim->dimension_name), 0, OPH_ODB_DIM_DIMENSION_SIZE + 1);
		if (row[5])
			strncpy(dim->dimension_name, row[5], OPH_ODB_DIM_DIMENSION_SIZE);
		memset(&(dim->dimension_type), 0, OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1);
		dim->id_hierarchy = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		dim_inst->id_dimensioninst = (row[7] ? (int) strtol(row[7], NULL, 10) : 0);
		dim_inst->id_dimension = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		dim_inst->size = (row[8] ? (int) strtol(row[8], NULL, 10) : 0);
		dim_inst->fk_id_dimension_index = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
		dim_inst->concept_level = row[10] ? (char) row[10][0] : OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
		dim_inst->fk_id_dimension_label = (row[11] ? (int) strtol(row[11], NULL, 10) : 0);
		if (row[6]) {
			if (dim_inst->fk_id_dimension_label)
				strncpy(dim->dimension_type, row[6], OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
			else
				strncpy(dim->dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE);	// A reduced dimension is handled by indexes
		}
		dim_inst->id_grid = dim_grid->id_grid = row[12] ? (int) strtol(row[12], NULL, 10) : 0;
		dim_inst->unlimited = row[20] ? (char) row[20][0] : 0;
		memset(&(dim_grid->grid_name), 0, OPH_ODB_DIM_GRID_SIZE + 1);
		if (row[13])
			strncpy(dim_grid->grid_name, row[13], OPH_ODB_DIM_GRID_SIZE);
		memset(&(dim->base_time), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[14])
			strncpy(dim->base_time, row[14], OPH_ODB_DIM_TIME_SIZE);
		memset(&(dim->units), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[15])
			strncpy(dim->units, row[15], OPH_ODB_DIM_TIME_SIZE);
		memset(&(dim->calendar), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[16])
			strncpy(dim->calendar, row[16], OPH_ODB_DIM_TIME_SIZE);
		memset(&query, 0, MYSQL_BUFLEN);
		if (row[17]) {
			int i = 0;
			char *tmp = NULL, *save_pointer = NULL;
			strncpy(query, row[17], OPH_ODB_DIM_TIME_SIZE);
			while ((tmp = strtok_r(tmp ? NULL : query, ",", &save_pointer)) && (i < OPH_ODB_DIM_MONTH_NUMBER))
				dim->month_lengths[i++] = (int) strtol(tmp, NULL, 10);
			while (i < OPH_ODB_DIM_MONTH_NUMBER)
				dim->month_lengths[i++] = OPH_ODB_DIM_DAY_NUMBER;
		}
		dim->leap_year = (row[18] ? (int) strtol(row[18], NULL, 10) : 0);
		dim->leap_month = (row[19] ? (int) strtol(row[18], NULL, 10) : 2);
	}
	mysql_free_result(res);

	// Metadata support to get correct values of time metadata
	if (id_datacube) {
		int ll = 0;
		if (oph_odb_meta_check_for_time_dimension(oDB, id_datacube, dim->dimension_name, &ll)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (ll) {
			if (strcmp(hier->hierarchy_name, OPH_COMMON_TIME_HIERARCHY))	// Override hierarchy name!!! This should be done even in ophDB, cretaing anew dimension with the same dimensioninstance
			{
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Override hierarchy name\n");
				if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_TIME_HIERARCHY, &hier->id_hierarchy)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
					return OPH_ODB_MYSQL_ERROR;
				}
				if (oph_odb_dim_retrieve_hierarchy(oDB, hier->id_hierarchy, hier)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
					return OPH_ODB_MYSQL_ERROR;
				}
				dim->id_hierarchy = hier->id_hierarchy;
			}

			char *value;
			if (oph_odb_meta_get(oDB, id_datacube, NULL, OPH_ODB_TIME_FREQUENCY, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				int exists = 0;
				char filename[MYSQL_BUFLEN];
				snprintf(filename, MYSQL_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier->filename);
				if (*hier->filename && oph_hier_check_concept_level_long(filename, value, &exists, &dim_inst->concept_level)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in convert long name: %s\n", value);
					free(value);
					return OPH_ODB_MYSQL_ERROR;
				}
				if (!exists)
					dim_inst->concept_level = *value;
				free(value);
			}
			if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_UNITS, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				if (!strncasecmp(value, OPH_DIM_TIME_UNITS_SECONDS, ll = strlen(OPH_DIM_TIME_UNITS_SECONDS)) ||
				    !strncasecmp(value, OPH_DIM_TIME_UNITS_HOURS, ll = strlen(OPH_DIM_TIME_UNITS_HOURS)) ||
				    !strncasecmp(value, OPH_DIM_TIME_UNITS_DAYS, ll = strlen(OPH_DIM_TIME_UNITS_DAYS))) {
					strncpy(dim->units, value, ll);
					dim->units[ll] = 0;
					strcpy(dim->base_time, value + ll + strlen(OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR) + 2);
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
					return OPH_ODB_MYSQL_ERROR;
				}
				free(value);
			}
			if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_CALENDAR, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				strcpy(dim->calendar, value);
				free(value);
			}
			if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_YEAR, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				dim->leap_year = (int) strtol(value, NULL, 10);
				free(value);
			}
			if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_MONTH, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				dim->leap_month = (int) strtol(value, NULL, 10);
				free(value);
			}
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension(ophidiadb * oDB, int id_dimension, oph_odb_dimension * dim, int id_datacube)
{
	if (!oDB || !dim || !id_dimension) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char hierarchy_name[OPH_ODB_DIM_HIERARCHY_SIZE + 1];
	char query[MYSQL_BUFLEN];
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSION, id_dimension);
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
		dim->id_dimension = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		dim->id_container = (row[1] ? (int) strtol(row[1], NULL, 10) : 0);
		memset(&(dim->dimension_name), 0, OPH_ODB_DIM_DIMENSION_SIZE + 1);
		strncpy(dim->dimension_name, row[2], OPH_ODB_DIM_DIMENSION_SIZE);
		memset(&(dim->dimension_type), 0, OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1);
		strncpy(dim->dimension_type, row[3], OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
		dim->id_hierarchy = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		memset(&(dim->base_time), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[5])
			strncpy(dim->base_time, row[5], OPH_ODB_DIM_TIME_SIZE);
		memset(&(dim->units), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[6])
			strncpy(dim->units, row[6], OPH_ODB_DIM_TIME_SIZE);
		memset(&(dim->calendar), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[7])
			strncpy(dim->calendar, row[7], OPH_ODB_DIM_TIME_SIZE);
		memset(&query, 0, MYSQL_BUFLEN);
		if (row[8]) {
			int i = 0;
			char *tmp = NULL, *save_pointer = NULL;
			strncpy(query, row[8], OPH_ODB_DIM_TIME_SIZE);
			while ((tmp = strtok_r(tmp ? NULL : query, ",", &save_pointer)) && (i < OPH_ODB_DIM_MONTH_NUMBER))
				dim->month_lengths[i++] = (int) strtol(tmp, NULL, 10);
			while (i < OPH_ODB_DIM_MONTH_NUMBER)
				dim->month_lengths[i++] = OPH_ODB_DIM_DAY_NUMBER;
		}
		dim->leap_year = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
		dim->leap_month = (row[10] ? (int) strtol(row[10], NULL, 10) : 2);

		memset(hierarchy_name, 0, OPH_ODB_DIM_HIERARCHY_SIZE + 1);
		if (row[11])
			strncpy(hierarchy_name, row[11], OPH_ODB_DIM_HIERARCHY_SIZE);
	}
	mysql_free_result(res);

	// Metadata support to get correct values of time metadata
	if (id_datacube) {
		if (strcmp(hierarchy_name, OPH_COMMON_TIME_HIERARCHY))	// Override hierarchy name!!! This should be done even in ophidiaDB, cretaing a new dimension with the same dimensioninstance
		{
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Override hierarchy name\n");
			if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_TIME_HIERARCHY, &dim->id_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}

		}

		char *value;
		if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_UNITS, NULL, &value)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (value) {
			int ll;
			if (!strncasecmp(value, OPH_DIM_TIME_UNITS_SECONDS, ll = strlen(OPH_DIM_TIME_UNITS_SECONDS)) ||
			    !strncasecmp(value, OPH_DIM_TIME_UNITS_HOURS, ll = strlen(OPH_DIM_TIME_UNITS_HOURS)) ||
			    !strncasecmp(value, OPH_DIM_TIME_UNITS_DAYS, ll = strlen(OPH_DIM_TIME_UNITS_DAYS))) {
				strncpy(dim->units, value, ll);
				dim->units[ll] = 0;
				strcpy(dim->base_time, value + ll + strlen(OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR) + 2);
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			free(value);
		}
		if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_CALENDAR, NULL, &value)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (value) {
			strcpy(dim->calendar, value);
			free(value);
		}
		if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_YEAR, NULL, &value)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (value) {
			dim->leap_year = (int) strtol(value, NULL, 10);
			free(value);
		}
		if (oph_odb_meta_get(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_MONTH, NULL, &value)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (value) {
			dim->leap_month = (int) strtol(value, NULL, 10);
			free(value);
		}
	}

	return OPH_ODB_SUCCESS;
}

//Note it retrieves grid associated to the specified container
int oph_odb_dim_retrieve_grid_id(ophidiadb * oDB, char *gridname, int id_container, int *id_grid)
{
	if (!oDB || !gridname || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_GRID_ID, gridname, id_container);
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

	unsigned int row_number = mysql_num_rows(res);
	if (!row_number) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "No row found by query\n");
		*id_grid = 0;
		mysql_free_result(res);
		return OPH_ODB_SUCCESS;
	}
	if (row_number > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "More than one row found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (mysql_field_count(oDB->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL)
		*id_grid = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_hierarchy(ophidiadb * oDB, int id_hierarchy, oph_odb_hierarchy * hier)
{
	if (!oDB || !hier || !id_hierarchy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY, id_hierarchy);
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
		hier->id_hierarchy = id_hierarchy;
		memset(&(hier->hierarchy_name), 0, OPH_ODB_DIM_HIERARCHY_SIZE + 1);
		strncpy(hier->hierarchy_name, row[0], OPH_ODB_DIM_HIERARCHY_SIZE);
		memset(&(hier->filename), 0, OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE + 1);
		strncpy(hier->filename, row[1], OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE);
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension_instance_id(ophidiadb * oDB, int id_datacube, char *dimensionname, int *id_dimension)
{
	if (!oDB || !dimensionname || !id_dimension) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_dimension = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_ID, id_datacube, dimensionname);
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

	if ((row = mysql_fetch_row(res)) != NULL)
		*id_dimension = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension_instance_id2(ophidiadb * oDB, int id_datacube, char *dimensionname, int *id_dimension_instance, int *id_dimension, int *id_hierarchy)
{
	if (!oDB || !dimensionname || !id_dimension_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	*id_dimension_instance = 0;
	if (id_dimension)
		*id_dimension = 0;
	if (id_hierarchy)
		*id_hierarchy = 0;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_ID2, id_datacube, dimensionname);
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

	if (mysql_field_count(oDB->conn) != 3) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		*id_dimension_instance = (int) strtol(row[0], NULL, 10);
		if (id_dimension)
			*id_dimension = (int) strtol(row[1], NULL, 10);
		if (id_hierarchy)
			*id_hierarchy = (int) strtol(row[2], NULL, 10);
	}

	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension_instance(ophidiadb * oDB, int id_dimensioninst, oph_odb_dimension_instance * dim_inst, int id_datacube)
{
	if (!oDB || !dim_inst || !id_dimensioninst) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char dimension_name[OPH_ODB_DIM_DIMENSION_SIZE + 1];
	memset(dimension_name, 0, OPH_ODB_DIM_DIMENSION_SIZE + 1);
	char filename[MYSQL_BUFLEN];
	memset(filename, 0, MYSQL_BUFLEN);
	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE, id_dimensioninst);
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

	if (mysql_field_count(oDB->conn) != 10) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if ((row = mysql_fetch_row(res)) != NULL) {
		dim_inst->id_dimensioninst = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		dim_inst->id_dimension = (row[1] ? (int) strtol(row[1], NULL, 10) : 0);
		dim_inst->id_grid = (row[2] ? (int) strtol(row[2], NULL, 10) : 0);
		dim_inst->size = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		dim_inst->fk_id_dimension_index = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		dim_inst->concept_level = (char) row[5][0];
		dim_inst->fk_id_dimension_label = (row[6] ? (int) strtol(row[6], NULL, 10) : 0);
		dim_inst->unlimited = row[9] ? (char) row[9][0] : 0;
		if (row[7])
			strncpy(dimension_name, row[7], OPH_ODB_DIM_DIMENSION_SIZE);
		if (row[8])
			snprintf(filename, MYSQL_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, row[8]);
	}
	mysql_free_result(res);

	// Metadata support to get correct values of time metadata
	if (id_datacube) {
		int ll = 0;
		if (oph_odb_meta_check_for_time_dimension(oDB, id_datacube, dimension_name, &ll)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (ll) {
			char *value;
			if (oph_odb_meta_get(oDB, id_datacube, NULL, OPH_ODB_TIME_FREQUENCY, NULL, &value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (value) {
				int exists = 0;
				if (*filename && oph_hier_check_concept_level_long(filename, value, &exists, &dim_inst->concept_level)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in convert long name: %s\n", value);
					free(value);
					return OPH_ODB_MYSQL_ERROR;
				}
				if (!exists)
					dim_inst->concept_level = *value;
				free(value);
			}
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension_list_from_container(ophidiadb * oDB, int id_container, oph_odb_dimension ** dims, int *dim_num)
{
	if (!oDB || !id_container || !dims || !dim_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_CONTAINER_DIMENSIONS, id_container);
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

	if (mysql_field_count(oDB->conn) != 11) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int number_of_dims = mysql_num_rows(res);
	if (!(number_of_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (!(*dims = (oph_odb_dimension *) malloc(number_of_dims * sizeof(oph_odb_dimension)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i = 0;
	*dim_num = number_of_dims;
	while ((row = mysql_fetch_row(res)) != NULL) {
		(*dims)[i].id_dimension = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		(*dims)[i].id_container = (row[1] ? (int) strtol(row[1], NULL, 10) : 0);
		memset(&((*dims)[i].dimension_name), 0, OPH_ODB_DIM_DIMENSION_SIZE + 1);
		strncpy((*dims)[i].dimension_name, row[2], OPH_ODB_DIM_DIMENSION_SIZE);
		memset(&((*dims)[i].dimension_type), 0, OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1);
		strncpy((*dims)[i].dimension_type, row[3], OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
		(*dims)[i].id_hierarchy = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		memset(&((*dims)[i].base_time), 0, OPH_ODB_DIM_TIME_SIZE);
		if (row[5])
			strncpy((*dims)[i].base_time, row[5], OPH_ODB_DIM_TIME_SIZE + 1);
		memset(&((*dims)[i].units), 0, OPH_ODB_DIM_TIME_SIZE);
		if (row[6])
			strncpy((*dims)[i].units, row[6], OPH_ODB_DIM_TIME_SIZE + 1);
		memset(&((*dims)[i].calendar), 0, OPH_ODB_DIM_TIME_SIZE);
		if (row[7])
			strncpy((*dims)[i].calendar, row[7], OPH_ODB_DIM_TIME_SIZE + 1);
		memset(&query, 0, MYSQL_BUFLEN);
		if (row[8]) {
			int j = 0;
			char *tmp = NULL, *save_pointer = NULL;
			strncpy(query, row[8], OPH_ODB_DIM_TIME_SIZE);
			while ((tmp = strtok_r(tmp ? NULL : query, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
				(*dims)[i].month_lengths[j++] = (int) strtol(tmp, NULL, 10);
			while (j < OPH_ODB_DIM_MONTH_NUMBER)
				(*dims)[i].month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
		}
		(*dims)[i].leap_year = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
		(*dims)[i].leap_month = (row[10] ? (int) strtol(row[10], NULL, 10) : 2);
		i++;
	}
	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_dimension_list_from_grid_in_container(ophidiadb * oDB, char *grid_name, int id_container, oph_odb_dimension ** dims, oph_odb_dimension_instance ** dim_insts, int *dim_num)
{
	if (!oDB || !grid_name || !dims || !dim_insts || !dim_num || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSIONS_FROM_GRID_IN_CONTAINER, grid_name, id_container);
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

	if (mysql_field_count(oDB->conn) != 16) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	int number_of_dims = mysql_num_rows(res);
	if (!(number_of_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query\n");
		mysql_free_result(res);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	if (!(*dims = (oph_odb_dimension *) malloc(number_of_dims * sizeof(oph_odb_dimension)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}
	if (!(*dim_insts = (oph_odb_dimension_instance *) malloc(number_of_dims * sizeof(oph_odb_dimension_instance)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		mysql_free_result(res);
		return OPH_ODB_MEMORY_ERROR;
	}

	int i = 0;
	*dim_num = number_of_dims;
	while ((row = mysql_fetch_row(res)) != NULL) {
		(*dims)[i].id_dimension = (int) strtol(row[0], NULL, 10);
		(*dims)[i].id_container = id_container;
		memset(&((*dims)[i].dimension_name), 0, OPH_ODB_DIM_DIMENSION_SIZE + 1);
		strncpy((*dims)[i].dimension_name, row[1], OPH_ODB_DIM_DIMENSION_SIZE);
		memset(&((*dims)[i].dimension_type), 0, OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1);
		strncpy((*dims)[i].dimension_type, row[2], OPH_ODB_DIM_DIMENSION_TYPE_SIZE);
		(*dims)[i].id_hierarchy = (row[3] ? (int) strtol(row[3], NULL, 10) : 0);
		(*dim_insts)[i].id_dimensioninst = (row[4] ? (int) strtol(row[4], NULL, 10) : 0);
		(*dim_insts)[i].id_dimension = (row[0] ? (int) strtol(row[0], NULL, 10) : 0);
		(*dim_insts)[i].id_grid = (row[5] ? (int) strtol(row[5], NULL, 10) : 0);
		(*dim_insts)[i].size = (row[6] ? (int) strtol(row[6], NULL, 10) : 0);
		(*dim_insts)[i].fk_id_dimension_index = (row[7] ? (int) strtol(row[7], NULL, 10) : 0);
		(*dim_insts)[i].concept_level = (char) row[8][0];
		(*dim_insts)[i].fk_id_dimension_label = (row[9] ? (int) strtol(row[9], NULL, 10) : 0);
		memset(&((*dims)[i].base_time), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[10])
			strncpy((*dims)[i].base_time, row[10], OPH_ODB_DIM_TIME_SIZE);
		memset(&((*dims)[i].units), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[11])
			strncpy((*dims)[i].units, row[11], OPH_ODB_DIM_TIME_SIZE);
		memset(&((*dims)[i].calendar), 0, OPH_ODB_DIM_TIME_SIZE + 1);
		if (row[12])
			strncpy((*dims)[i].calendar, row[12], OPH_ODB_DIM_TIME_SIZE);
		memset(&query, 0, MYSQL_BUFLEN);
		if (row[13]) {
			int j = 0;
			char *tmp = NULL, *save_pointer = NULL;
			strncpy(query, row[13], OPH_ODB_DIM_TIME_SIZE);
			while ((tmp = strtok_r(tmp ? NULL : query, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
				(*dims)[i].month_lengths[j++] = (int) strtol(tmp, NULL, 10);
			while (j < OPH_ODB_DIM_MONTH_NUMBER)
				(*dims)[i].month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
		}
		(*dims)[i].leap_year = (row[14] ? (int) strtol(row[14], NULL, 10) : 0);
		(*dims)[i].leap_month = (row[15] ? (int) strtol(row[15], NULL, 10) : 2);
		i++;
	}
	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

// Warning: support for time metadata is not present in the following function
int oph_odb_dim_find_dimensions_features(ophidiadb * oDB, int id_datacube, MYSQL_RES ** frag_rows, int *dim_num)
{
	if (!oDB || !id_datacube || !frag_rows || !dim_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];

	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSIONS_FEATURES, id_datacube);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	*frag_rows = mysql_store_result(oDB->conn);

	if (mysql_field_count(oDB->conn) != OPH_DIM_STREAM_ELEMENTS) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(*frag_rows);
		return OPH_ODB_TOO_MANY_ROWS;
	}

	*dim_num = mysql_num_rows(*frag_rows);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_find_hierarchy_list(ophidiadb * oDB, MYSQL_RES ** information_list)
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

	n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY_LIST);
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

int oph_odb_dim_find_container_grid_list(ophidiadb * oDB, int id_container, MYSQL_RES ** information_list)
{
	(*information_list) = NULL;

	if (!oDB || !information_list || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;

	n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_GRID_LIST, id_container);
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

int oph_odb_dim_insert_into_grid_table(ophidiadb * oDB, oph_odb_dimension_grid * grid, int *last_insertd_id)
{
	if (!oDB || !grid || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;
	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_GRID_ID2, grid->grid_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(oDB->conn);

	if ((num_rows = mysql_num_rows(res))) {
		if (mysql_field_count(oDB->conn) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Too many fields found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}
		int id_grid = 0;

		if ((row = mysql_fetch_row(res))) {
			id_grid = (int) strtol(row[0], NULL, 10);
		}
		mysql_free_result(res);
		*last_insertd_id = id_grid;
		return OPH_ODB_SUCCESS;
	}
	mysql_free_result(res);


	n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_GRID, grid->grid_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted container id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_insert_into_dimension_table(ophidiadb * oDB, oph_odb_dimension * dim, int *last_insertd_id, int id_datacube)
{
	if (!oDB || !dim || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	int n;
	char insertQuery[MYSQL_BUFLEN];

	if (strlen(dim->units) && strlen(dim->calendar)) {
		int i;
		char month_lengths[OPH_ODB_DIM_TIME_SIZE];
		for (i = n = 0; i < OPH_ODB_DIM_MONTH_NUMBER; ++i)
			n += snprintf(month_lengths + n, OPH_ODB_DIM_TIME_SIZE - n, "%s%d", i ? "," : "", dim->month_lengths[i]);
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA, dim->id_container, dim->dimension_name, dim->dimension_type, dim->id_hierarchy,
			     dim->base_time, dim->units, dim->calendar, month_lengths, dim->leap_year, dim->leap_month);
	} else
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION, dim->id_container, dim->dimension_name, dim->dimension_type, dim->id_hierarchy);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted dimension id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
	// Metadata support to put correct values of time metadata
	if (id_datacube) {
		int ll = 0;
		if (oph_odb_meta_check_for_time_dimension(oDB, id_datacube, dim->dimension_name, &ll)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (ll) {
			char value[OPH_ODB_DIM_TIME_SIZE];
			snprintf(value, OPH_ODB_DIM_TIME_SIZE, "%s %s %s", dim->units, OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR, dim->base_time);
			if (oph_odb_meta_put(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_UNITS, 0, value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			if (oph_odb_meta_put(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_CALENDAR, 0, dim->calendar)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			snprintf(value, OPH_ODB_DIM_TIME_SIZE, "%d", dim->leap_year);
			if (oph_odb_meta_put(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_YEAR, 0, value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
			snprintf(value, OPH_ODB_DIM_TIME_SIZE, "%d", dim->leap_month);
			if (oph_odb_meta_put(oDB, id_datacube, dim->dimension_name, OPH_ODB_TIME_LEAP_MONTH, 0, value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}
		}
	}

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_insert_into_dimensioninstance_table(ophidiadb * oDB, oph_odb_dimension_instance * dim_inst, int *last_insertd_id, int id_datacube, const char *dimension_name, const char *frequency)
{
	if (!oDB || !dim_inst || !last_insertd_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char insertQuery[MYSQL_BUFLEN];
	int n;
	if (dim_inst->id_grid)
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_INST, dim_inst->id_dimension, dim_inst->size, dim_inst->fk_id_dimension_index, dim_inst->id_grid,
			     dim_inst->concept_level, dim_inst->unlimited, dim_inst->fk_id_dimension_label);
	else
		n = snprintf(insertQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_INST_2, dim_inst->id_dimension, dim_inst->size, dim_inst->fk_id_dimension_index,
			     dim_inst->concept_level, dim_inst->unlimited, dim_inst->fk_id_dimension_label);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	if (mysql_query(oDB->conn, insertQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	if (!(*last_insertd_id = mysql_insert_id(oDB->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted dimension id\n");
		return OPH_ODB_TOO_MANY_ROWS;
	}
	// Metadata support to put correct values of time metadata
	if (id_datacube && dimension_name && frequency) {
		int ll = 0;
		if (oph_odb_meta_check_for_time_dimension(oDB, id_datacube, dimension_name, &ll)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (ll && oph_odb_meta_put(oDB, id_datacube, NULL, OPH_ODB_TIME_FREQUENCY, 0, frequency)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
	}

	return OPH_ODB_SUCCESS;
}

//NOTE: It deletes grids used exclusively by this container
int oph_odb_dim_delete_from_grid_table(ophidiadb * oDB, int id_container)
{
	if (!oDB || !id_container) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char deleteQuery[MYSQL_BUFLEN];
	int n = snprintf(deleteQuery, MYSQL_BUFLEN, MYSQL_DELETE_OPHIDIADB_GRID, id_container);
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

int oph_odb_dim_retrieve_hierarchy_id(ophidiadb * oDB, char *hierarchy_name, int *id_hierarchy)
{
	if (!oDB || !hierarchy_name || !id_hierarchy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_RETRIEVE_HIERARCHY_ID, hierarchy_name);
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

	row = mysql_fetch_row(res);
	*id_hierarchy = (int) strtol(row[0], NULL, 10);

	mysql_free_result(res);
	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_retrieve_hierarchy_from_dimension_of_datacube(ophidiadb * oDB, int datacube_id, const char *dimension_name, oph_odb_hierarchy * hier, char *concept_level, int *dimension_instance_id)
{
	if (!oDB || !dimension_name || !hier) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}
	if (concept_level)
		*concept_level = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	int ll = 0;
	if (oph_odb_meta_check_for_time_dimension(oDB, datacube_id, dimension_name, &ll)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	int n;
	MYSQL_RES *res = NULL;
	MYSQL_ROW row;

	if (ll)			// Metadata support to get correct values of time metadata
	{
		// Override hierarchy name!!! This should be done even in ophDB, creating anew dimension with the same dimensioninstance
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Override hierarchy name\n");
		if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_TIME_HIERARCHY, &hier->id_hierarchy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		if (oph_odb_dim_retrieve_hierarchy(oDB, hier->id_hierarchy, hier)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}

		char *concept_level_ = NULL, *units_ = NULL, *calendar_ = NULL, *leap_year_ = NULL, *leap_month_ = NULL;	// *month_lengths_ = NULL;
		if (oph_odb_meta_get(oDB, datacube_id, NULL, OPH_ODB_TIME_FREQUENCY, NULL, &concept_level_)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata read error for key %s\n", OPH_ODB_TIME_FREQUENCY);
			return OPH_ODB_MYSQL_ERROR;
		}
		if (!concept_level_)	// First time
		{
			if (oph_odb_meta_get(oDB, datacube_id, dimension_name, OPH_ODB_TIME_UNITS, NULL, &units_)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata read error for key %s\n", OPH_ODB_TIME_UNITS);
				return OPH_ODB_MYSQL_ERROR;
			}
			if (oph_odb_meta_get(oDB, datacube_id, dimension_name, OPH_ODB_TIME_CALENDAR, NULL, &calendar_)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata read error for key %s\n", OPH_ODB_TIME_CALENDAR);
				return OPH_ODB_MYSQL_ERROR;
			}
			if (oph_odb_meta_get(oDB, datacube_id, dimension_name, OPH_ODB_TIME_LEAP_YEAR, NULL, &leap_year_)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata read error for key %s\n", OPH_ODB_TIME_LEAP_YEAR);
				return OPH_ODB_MYSQL_ERROR;
			}
			if (oph_odb_meta_get(oDB, datacube_id, dimension_name, OPH_ODB_TIME_LEAP_MONTH, NULL, &leap_month_)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata read error for key %s\n", OPH_ODB_TIME_LEAP_MONTH);
				return OPH_ODB_MYSQL_ERROR;
			}
		} else if (concept_level) {
			if ((*concept_level_ == OPH_HIER_MINUTE_SHORT_NAME[0]) || (*concept_level_ == OPH_HIER_MONTH_SHORT_NAME[0])) {
				if (!strncmp(concept_level_, OPH_HIER_MINUTE_LONG_NAME, OPH_HIER_MAX_STRING_LENGTH))
					*concept_level = OPH_HIER_MINUTE_SHORT_NAME[0];
				else if (!strncmp(concept_level_, OPH_HIER_MONTH_LONG_NAME, OPH_HIER_MAX_STRING_LENGTH))
					*concept_level = OPH_HIER_MONTH_SHORT_NAME[0];
			} else
				*concept_level = *concept_level_;
		}

		if (dimension_instance_id || !concept_level_) {
			n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_DATA, datacube_id, dimension_name);
			if (n >= MYSQL_BUFLEN) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
				return OPH_ODB_STR_BUFF_OVERFLOW;
			}

			if (mysql_query(oDB->conn, query)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
				return OPH_ODB_MYSQL_ERROR;
			}

			res = mysql_store_result(oDB->conn);

			if (mysql_num_rows(res) != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if (mysql_field_count(oDB->conn) != 8) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
				mysql_free_result(res);
				return OPH_ODB_TOO_MANY_ROWS;
			}
			if ((row = mysql_fetch_row(res)) != NULL) {
				if (dimension_instance_id)
					*dimension_instance_id = (int) strtol(row[0], NULL, 10);
				if (!concept_level_) {
					if (concept_level && row[1])
						*concept_level = (char) row[1][0];
					if (oph_odb_meta_put(oDB, datacube_id, NULL, OPH_ODB_TIME_FREQUENCY, 0, row[1])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata write error for key %s\n", OPH_ODB_TIME_FREQUENCY);
						return OPH_ODB_MYSQL_ERROR;
					}
					if (!units_ && row[2] && row[3]) {
						char value[OPH_ODB_DIM_TIME_SIZE];
						snprintf(value, OPH_ODB_DIM_TIME_SIZE, "%s %s %s", row[2], OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR, row[3]);
						if (oph_odb_meta_put(oDB, datacube_id, dimension_name, OPH_ODB_TIME_UNITS, 0, value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
							return OPH_ODB_MYSQL_ERROR;
						}
					}
					if (!calendar_ && row[4] && oph_odb_meta_put(oDB, datacube_id, dimension_name, OPH_ODB_TIME_CALENDAR, 0, row[4])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
						return OPH_ODB_MYSQL_ERROR;
					}
					if (!leap_year_ && row[5] && oph_odb_meta_put(oDB, datacube_id, dimension_name, OPH_ODB_TIME_LEAP_YEAR, 0, row[5])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
						return OPH_ODB_MYSQL_ERROR;
					}
					if (!leap_month_ && row[6] && oph_odb_meta_put(oDB, datacube_id, dimension_name, OPH_ODB_TIME_LEAP_MONTH, 0, row[6])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
						return OPH_ODB_MYSQL_ERROR;
					}
				}
			}
		}
		// Freeing
		if (concept_level_)
			free(concept_level_);
		if (units_)
			free(units_);
		if (calendar_)
			free(calendar_);
		if (leap_year_)
			free(leap_year_);
		if (leap_month_)
			free(leap_month_);
	} else {
		n = snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY_FROM_DIMENSION_NAME, datacube_id, dimension_name);
		if (n >= MYSQL_BUFLEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}

		if (mysql_query(oDB->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}

		res = mysql_store_result(oDB->conn);

		if (mysql_num_rows(res) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}

		if (mysql_field_count(oDB->conn) != 5) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
			mysql_free_result(res);
			return OPH_ODB_TOO_MANY_ROWS;
		}

		if ((row = mysql_fetch_row(res)) != NULL) {
			hier->id_hierarchy = (int) strtol(row[0], NULL, 10);
			memset(&(hier->hierarchy_name), 0, OPH_ODB_DIM_HIERARCHY_SIZE + 1);
			strncpy(hier->hierarchy_name, row[1], OPH_ODB_DIM_HIERARCHY_SIZE);
			memset(&(hier->filename), 0, OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE + 1);
			strncpy(hier->filename, row[2], OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE);
			if (concept_level)
				*concept_level = (char) row[3][0];
			if (dimension_instance_id)
				*dimension_instance_id = (int) strtol(row[4], NULL, 10);
		}
	}
	mysql_free_result(res);

	return OPH_ODB_SUCCESS;
}

int oph_odb_dim_set_time_dimension(ophidiadb * oDB, int id_datacube, char *dimension_name)
{
	if (!oDB || !dimension_name || !id_datacube) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	oph_odb_dimension dim;
	if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_TIME_HIERARCHY, &dim.id_hierarchy)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	int id_dimension, id_dimension_instance, id_hierarchy;
	if (oph_odb_dim_retrieve_dimension_instance_id2(oDB, id_datacube, dimension_name, &id_dimension_instance, &id_dimension, &id_hierarchy)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	int ll = 0;
	if (oph_odb_meta_check_for_time_dimension(oDB, id_datacube, dimension_name, &ll)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (!ll) {
		return OPH_ODB_NO_ROW_FOUND;
	}

	oph_odb_hierarchy hier;
	*dim.units = *dim.base_time = *dim.calendar = 0;
	char concept_level = 0;
	if (oph_odb_dim_retrieve_hierarchy(oDB, dim.id_hierarchy, &hier)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	char *value;
	if (oph_odb_meta_get(oDB, id_datacube, NULL, OPH_ODB_TIME_FREQUENCY, NULL, &value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (value) {
		int exists = 0;
		char filename[MYSQL_BUFLEN];
		snprintf(filename, MYSQL_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
		if (*hier.filename && oph_hier_check_concept_level_long(filename, value, &exists, &concept_level)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in convert long name: %s\n", value);
			free(value);
			return OPH_ODB_MYSQL_ERROR;
		}
		if (!exists)
			concept_level = *value;
		free(value);
	}
	if (oph_odb_meta_get(oDB, id_datacube, dimension_name, OPH_ODB_TIME_UNITS, NULL, &value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (value) {
		if (!strncasecmp(value, OPH_DIM_TIME_UNITS_SECONDS, ll = strlen(OPH_DIM_TIME_UNITS_SECONDS)) ||
		    !strncasecmp(value, OPH_DIM_TIME_UNITS_HOURS, ll = strlen(OPH_DIM_TIME_UNITS_HOURS)) || !strncasecmp(value, OPH_DIM_TIME_UNITS_DAYS, ll = strlen(OPH_DIM_TIME_UNITS_DAYS))) {
			strncpy(dim.units, value, ll);
			dim.units[ll] = 0;
			strcpy(dim.base_time, value + ll + strlen(OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR) + 2);
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
			return OPH_ODB_MYSQL_ERROR;
		}
		free(value);
	}
	if (oph_odb_meta_get(oDB, id_datacube, dimension_name, OPH_ODB_TIME_CALENDAR, NULL, &value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (value) {
		strcpy(dim.calendar, value);
		free(value);
	}
	if (oph_odb_meta_get(oDB, id_datacube, dimension_name, OPH_ODB_TIME_LEAP_YEAR, NULL, &value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (value) {
		dim.leap_year = (int) strtol(value, NULL, 10);
		free(value);
	} else
		dim.leap_year = 0;
	if (oph_odb_meta_get(oDB, id_datacube, dimension_name, OPH_ODB_TIME_LEAP_MONTH, NULL, &value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}
	if (value) {
		dim.leap_month = (int) strtol(value, NULL, 10);
		free(value);
	} else
		dim.leap_month = 2;

	char updateQuery[MYSQL_BUFLEN];
	int n =
	    snprintf(updateQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA2, dim.id_hierarchy, dim.base_time, dim.units, dim.calendar, dim.leap_year, dim.leap_month,
		     id_dimension);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	if (mysql_query(oDB->conn, updateQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ODB_MYSQL_ERROR;
	}

	n = snprintf(updateQuery, MYSQL_BUFLEN, MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA3, concept_level, id_dimension_instance);
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

int oph_odb_dim_update_time_dimension(oph_odb_dimension * dim, char **templates, char **values, int nattr)
{
	if (!dim || !templates || !values) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	int i;
	size_t ll;
	for (i = 0; i < nattr; ++i) {
		if (!strcmp(templates[i], OPH_ODB_TIME_UNITS)) {
			if (!strncasecmp(values[i], OPH_DIM_TIME_UNITS_SECONDS, ll = strlen(OPH_DIM_TIME_UNITS_SECONDS)) ||
			    !strncasecmp(values[i], OPH_DIM_TIME_UNITS_HOURS, ll = strlen(OPH_DIM_TIME_UNITS_HOURS)) ||
			    !strncasecmp(values[i], OPH_DIM_TIME_UNITS_DAYS, ll = strlen(OPH_DIM_TIME_UNITS_DAYS))) {
				strncpy(dim->units, values[i], ll);
				dim->units[ll] = 0;
				strcpy(dim->base_time, values[i] + ll + strlen(OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR) + 2);
			}
		} else if (!strcmp(templates[i], OPH_ODB_TIME_CALENDAR)) {
			strcpy(dim->calendar, values[i]);
		} else if (!strcmp(templates[i], OPH_ODB_TIME_LEAP_YEAR)) {
			dim->leap_year = (int) strtol(values[i], NULL, 10);
		} else if (!strcmp(templates[i], OPH_ODB_TIME_LEAP_MONTH)) {
			dim->leap_month = (int) strtol(values[i], NULL, 10);
		}
	}

	return OPH_ODB_SUCCESS;
}
