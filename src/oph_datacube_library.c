/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#include "oph_datacube_library.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <zlib.h>
#include <math.h>

#include <sys/time.h>

#include "oph-lib-binary-io.h"
#include "oph_pid_library.h"
#include "debug.h"

#define OPH_DC_MAX_SIZE 100
#define OPH_DC_MIN_SIZE 50
#define OPH_DC_COMPLEX_DATATYPE_PREFIX "complex_"

extern int msglevel;

char oph_dc_typeof(char *data_type)
{
	if (!data_type)
		return 0;

	char new_type[strlen(data_type)];
	if (strcasestr(data_type, OPH_DC_COMPLEX_DATATYPE_PREFIX)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Complex data types are partially supported: data will be considered as simple\n");
		strcpy(new_type, data_type + strlen(OPH_DC_COMPLEX_DATATYPE_PREFIX));
		data_type = new_type;
	}
	if (!strcasecmp(data_type, OPH_DC_BYTE_TYPE))
		return OPH_DC_BYTE_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_SHORT_TYPE))
		return OPH_DC_SHORT_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_INT_TYPE))
		return OPH_DC_INT_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_LONG_TYPE))
		return OPH_DC_LONG_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_FLOAT_TYPE))
		return OPH_DC_FLOAT_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_DOUBLE_TYPE))
		return OPH_DC_DOUBLE_FLAG;
	else if (!strcasecmp(data_type, OPH_DC_BIT_TYPE))
		return OPH_DC_BIT_FLAG;
	return 0;
}

const char *oph_dc_stringof(char type_flag)
{
	switch (type_flag) {
		case OPH_DC_BYTE_FLAG:
			return OPH_DC_BYTE_TYPE;
		case OPH_DC_SHORT_FLAG:
			return OPH_DC_SHORT_TYPE;
		case OPH_DC_INT_FLAG:
			return OPH_DC_INT_TYPE;
		case OPH_DC_LONG_FLAG:
			return OPH_DC_LONG_TYPE;
		case OPH_DC_FLOAT_FLAG:
			return OPH_DC_FLOAT_TYPE;
		case OPH_DC_DOUBLE_FLAG:
			return OPH_DC_DOUBLE_TYPE;
		case OPH_DC_BIT_FLAG:
			return OPH_DC_BIT_TYPE;
	}
	return 0;
}

int oph_dc_setup_dbms_thread(oph_ioserver_handler ** server, char *server_type)
{
	if (!server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_ioserver_setup(server_type, server, 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup server library\n");
		return OPH_DC_SERVER_ERROR;
	}
	return OPH_DC_SUCCESS;
}

int oph_dc_setup_dbms(oph_ioserver_handler ** server, char *server_type)
{
	/*
	   if (!server) {
	   pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
	   return OPH_DC_NULL_PARAM;
	   }

	   if (oph_ioserver_setup(server_type, server, 0)) {
	   pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup server library\n");
	   return OPH_DC_SERVER_ERROR;
	   }
	 */

	return oph_dc_setup_dbms_thread(server, server_type);
}

int oph_dc_connect_to_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, unsigned long flag)
{
	if (!dbms || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	oph_ioserver_params conn_params;
	conn_params.host = dbms->hostname;
	conn_params.user = dbms->login;
	conn_params.passwd = dbms->pwd;
	conn_params.port = dbms->port;
	conn_params.db_name = NULL;
	conn_params.opt_flag = flag;

	if (oph_ioserver_connect(server, &conn_params)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection error\n");
		oph_dc_disconnect_from_dbms(server, dbms);
		return OPH_DC_SERVER_ERROR;
	}
	return OPH_DC_SUCCESS;
}

int oph_dc_use_db_of_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, oph_odb_db_instance * db)
{
	if (!dbms || !db || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, dbms, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to dbms.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (server->connection) {
		if (oph_ioserver_use_db(server, db->db_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set default database\n");
			return OPH_DC_SERVER_ERROR;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection not established\n");
		return OPH_DC_SERVER_ERROR;
	}
	return OPH_DC_SUCCESS;

}

int oph_dc_check_connection_to_db(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, oph_odb_db_instance * db, unsigned long flag)
{
	if (!dbms || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");
		return OPH_DC_NULL_PARAM;
	}

	if (!(server->connection)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Connection was somehow closed.\n");
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_params conn_params;
	conn_params.host = dbms->hostname;
	conn_params.user = dbms->login;
	conn_params.passwd = dbms->pwd;
	conn_params.port = dbms->port;
	conn_params.db_name = (db ? db->db_name : NULL);
	conn_params.opt_flag = flag;

	if (oph_ioserver_connect(server, &conn_params)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection error\n");
		return OPH_DC_SERVER_ERROR;
	}
	return OPH_DC_SUCCESS;
}


int oph_dc_disconnect_from_dbms(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms)
{
	if (!dbms || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	oph_ioserver_close(server);
	return OPH_DC_SUCCESS;
}

int oph_dc_cleanup_dbms(oph_ioserver_handler * server)
{
	if (!server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	oph_ioserver_cleanup(server);
	return OPH_DC_SUCCESS;
}

int oph_dc_create_db(oph_ioserver_handler * server, oph_odb_db_instance * db)
{
	if (!db || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, db->dbms_instance, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_CREATE_DB, db->db_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_CREATE_DB "\n", db->db_name);
#endif

	char db_creation_query[query_buflen];
	int n = snprintf(db_creation_query, query_buflen, OPH_DC_SQ_CREATE_DB, db->db_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, db_creation_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);
	return OPH_DC_SUCCESS;
}

int oph_dc_delete_db(oph_ioserver_handler * server, oph_odb_db_instance * db)
{
	if (!db || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_DROP_DB, db->db_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_DELETE_DB "\n", db->db_name);
#endif

	//Check if Database is empty and DELETE
	char delete_query[query_buflen];
	int n = snprintf(delete_query, query_buflen, OPH_DC_SQ_DROP_DB, db->db_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, delete_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_create_empty_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag)
{
	if (!frag || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_CREATE_FRAG, frag->fragment_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_CREATE_FRAG "\n", frag->fragment_name);
#endif

	char create_query[query_buflen];
	int n = snprintf(create_query, query_buflen, OPH_DC_SQ_CREATE_FRAG, frag->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, create_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_create_empty_fragment_from_name(oph_ioserver_handler * server, const char *frag_name, oph_odb_db_instance * db_instance)
{
	if (!frag_name || !server || !db_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, db_instance->dbms_instance, db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_CREATE_FRAG, frag_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char create_query[query_buflen];
	int n = snprintf(create_query, query_buflen, OPH_DC_SQ_CREATE_FRAG, frag_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, create_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_delete_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag)
{
	if (!frag || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_DELETE_FRAG, frag->fragment_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_DELETE_FRAG "\n", frag->fragment_name);
#endif

	char delete_query[query_buflen];
	int n = snprintf(delete_query, query_buflen, OPH_DC_SQ_DELETE_FRAG, frag->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, delete_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_create_fragment_from_query(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long *start_id)
{
	return oph_dc_create_fragment_from_query2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL);
}

//Removed multiple statement execution
int oph_dc_create_fragment_from_query2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long *start_id,
				       long long *block_size)
{
	UNUSED(start_id);

	if (!old_frag || !operation || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int n, query_buflen = QUERY_BUFLEN;
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);

	if (new_frag_name == NULL) {

		query_buflen = 1 + snprintf(NULL, 0, operation);
		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];
		n = snprintf(create_query, query_buflen, operation);
		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		oph_ioserver_query *query = NULL;
		if (oph_ioserver_setup_query(server, create_query, 1, NULL, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n", create_query);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n", create_query);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		oph_ioserver_free_query(server, query);

	} else {

		if (where) {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];

		if (where) {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WGB "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size,
					       MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WG "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_W "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name, where);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_GB "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size,
					       MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_G "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}

		}

		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		oph_ioserver_query *query = NULL;
		if (oph_ioserver_setup_query(server, create_query, 1, NULL, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n", create_query);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s',\n", create_query);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	return OPH_DC_SUCCESS;
}

int oph_dc_create_fragment_from_query_with_param(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						 long long *start_id, char *param, long long param_size)
{
	return oph_dc_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size, param ? 1 : 0);
}

int oph_dc_create_fragment_from_query_with_param2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						  long long *start_id, long long *block_size, char *param, long long param_size)
{
	return oph_dc_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, block_size, param, param_size, param ? 1 : 0);
}

int oph_dc_create_fragment_from_query_with_params(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						  long long *start_id, char *param, long long param_size, int num)
{
	return oph_dc_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size, num);
}

//Removed multiple statement execution
int oph_dc_create_fragment_from_query_with_params2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						   long long *start_id, long long *block_size, char *param, long long param_size, int num)
{
	UNUSED(start_id)
	    if (!old_frag || !operation || (!param && param_size) || (param && !param_size) || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + num, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	for (ii = 0; ii < num; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			for (ii = 0; ii < num; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}
	}
	args[num] = NULL;


	int n, nn = !param, query_buflen = QUERY_BUFLEN;

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);

	if (new_frag_name == NULL) {

		query_buflen = 1 + snprintf(NULL, 0, operation);
		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];
		n = snprintf(create_query, query_buflen, operation);
		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < num; ii++) {
			args[ii]->arg_length = param_size;
			args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
			args[ii]->arg_is_null = nn;
			args[ii]->arg = param;
		}

		if (oph_ioserver_setup_query(server, create_query, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			for (ii = 0; ii < num; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			for (ii = 0; ii < num; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < num; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);

	} else {

		if (where) {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];
		if (where) {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WGB "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size,
					       MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WG "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_W "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name, where);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_GB "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size,
					       MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_G "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}
		}

		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < num; ii++) {
			args[ii]->arg_length = param_size;
			args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
			args[ii]->arg_is_null = nn;
			args[ii]->arg = param;
		}

		if (oph_ioserver_setup_query(server, create_query, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			for (ii = 0; ii < num; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query '%s'\n", create_query);
			for (ii = 0; ii < num; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < num; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);

	}

	return OPH_DC_SUCCESS;
}

int oph_dc_create_fragment_from_query_with_aggregation(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
						       long long *start_id, char *param, long long param_size)
{
	return oph_dc_create_fragment_from_query_with_aggregation2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size);
}

//Removed multiple statement execution
int oph_dc_create_fragment_from_query_with_aggregation2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number,
							long long *start_id, long long *block_size, char *param, long long param_size)
{
	UNUSED(start_id)
	    if (!old_frag || !operation || (!param && param_size) || (param && !param_size) || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	if (oph_dc_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	int c_arg = 2, ii;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}
	}
	args[c_arg] = NULL;

	int n, query_buflen = QUERY_BUFLEN;
	if (!param)
		n = 1;

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);

	if (new_frag_name == NULL) {

		query_buflen = 1 + snprintf(NULL, 0, operation);
		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];

		n = snprintf(create_query, query_buflen, operation);
		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		args[0]->arg_length = param_size;
		args[0]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[0]->arg_is_null = n;
		args[0]->arg = param;

		args[1]->arg_length = param_size;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = n;
		args[1]->arg = param;

		if (oph_ioserver_setup_query(server, create_query, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);

	} else {

		if (where) {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_WGB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
								    old_frag->fragment_name, where, MYSQL_FRAG_ID, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, MYSQL_DC_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number,
								    operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_GB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
								    old_frag->fragment_name, MYSQL_FRAG_ID, *block_size);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
								    MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		char create_query[query_buflen];

		if (where) {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WGB2 "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *block_size, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_WGB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
						     old_frag->fragment_name, where, MYSQL_FRAG_ID, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_WG "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, MYSQL_DC_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number,
						     operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_W "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name, where);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		} else {
			if (aggregate_number) {
				if (block_size) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_GB2 "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *block_size, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *block_size);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_GB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE,
						     old_frag->fragment_name, MYSQL_FRAG_ID, *block_size);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN_G "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID,
					       operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID,
						     MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_APPLY_PLUGIN "\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE,
				       old_frag->fragment_name);
#endif
				n = snprintf(create_query, query_buflen, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}
		}

		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC_SERVER_ERROR;
		}

		args[0]->arg_length = param_size;
		args[0]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[0]->arg_is_null = n;
		args[0]->arg = param;

		args[1]->arg_length = param_size;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = n;
		args[1]->arg = param;

		if (oph_ioserver_setup_query(server, create_query, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query '%s'\n", create_query);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query '%s'\n", query);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);

	}

	return OPH_DC_SUCCESS;
}

int _oph_dc_build_rand_row(char *binary, int array_length, char type_flag, char rand_alg)
{

	if (!binary || !array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	int m = 0, res = 0;
	double rand_mes;

	if (type_flag == OPH_DC_BYTE_FLAG) {
		char measure_b;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_b = (char) ceil(((float) rand() / RAND_MAX) * 1000.0);
				res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_b = (char) ceil(rand_mes);
			res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_b = (char) ceil(rand_mes);
				res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_SHORT_FLAG) {
		short measure_s;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_s = (short) ceil(((float) rand() / RAND_MAX) * 1000.0);
				res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_s = (short) ceil(rand_mes);
			res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_s = (short) ceil(rand_mes);
				res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_INT_FLAG) {
		int measure_i;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_i = (int) ceil(((float) rand() / RAND_MAX) * 1000.0);
				res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_i = (int) ceil(rand_mes);
			res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_i = (int) ceil(rand_mes);
				res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_LONG_FLAG) {
		long long measure_l;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_l = (long long) ceil(((double) rand() / RAND_MAX) * 1000.0);
				res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_l = (long long) ceil(rand_mes);
			res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_l = (long long) ceil(rand_mes);
				res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_FLOAT_FLAG) {
		float measure_f;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_f = ((float) rand() / RAND_MAX) * 1000.0;
				res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_f = (float) rand_mes;
			res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_f = (float) rand_mes;
				res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_DOUBLE_FLAG) {
		double measure_d;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_d = ((double) rand() / RAND_MAX) * 1000.0;
				res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((double) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_d = rand_mes;
			res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((double) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_d = rand_mes;
				res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_DC_BIT_FLAG) {
		char measure_c;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				measure_c = (char) ceil(((float) rand() / RAND_MAX) * 1000.0);
				res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		} else {
			rand_mes = ((float) rand() / RAND_MAX) * 40.0 - 5.0;
			measure_c = (char) ceil(rand_mes);
			res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_DC_SERVER_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				rand_mes = rand_mes * 0.9 + 0.1 * (((float) rand() / RAND_MAX) * 40.0 - 5.0);
				measure_c = (char) ceil(rand_mes);
				res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_DC_SERVER_ERROR;
				}
			}
		}
	}

	return OPH_DC_SUCCESS;
}

int oph_dc_populate_fragment_with_rand_data(oph_ioserver_handler * server, oph_odb_fragment * frag, unsigned long long tuple_number, int array_length, char *data_type, int compressed, char *algorithm)
{
	if (!frag || !data_type || !server || !algorithm) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	char type_flag = oph_dc_typeof(data_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC_DATA_ERROR;
	}

	char rand_alg = 0;
	if (strcmp(algorithm, OPH_COMMON_RAND_ALGO_TEMP) == 0) {
		rand_alg = 1;
	} else if (strcmp(algorithm, OPH_COMMON_RAND_ALGO_DEFAULT) == 0) {
		rand_alg = 0;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading algorithm type\n");
		return OPH_DC_DATA_ERROR;
	}

	unsigned long long sizeof_var = 0;

	if (type_flag == OPH_DC_BYTE_FLAG)
		sizeof_var = array_length * sizeof(char);
	else if (type_flag == OPH_DC_SHORT_FLAG)
		sizeof_var = array_length * sizeof(short);
	else if (type_flag == OPH_DC_INT_FLAG)
		sizeof_var = array_length * sizeof(int);
	else if (type_flag == OPH_DC_LONG_FLAG)
		sizeof_var = array_length * sizeof(long long);
	else if (type_flag == OPH_DC_FLOAT_FLAG)
		sizeof_var = array_length * sizeof(float);
	else if (type_flag == OPH_DC_DOUBLE_FLAG)
		sizeof_var = array_length * sizeof(double);
	else if (type_flag == OPH_DC_BIT_FLAG) {
		sizeof_var = array_length * sizeof(char) / 8;
		if (array_length % 8)
			sizeof_var++;
		array_length = sizeof_var;	// a bit array correspond to a char array with 1/8 elements
	}
	//Compute number of tuples per insert (regular case)
	unsigned long long regular_rows = 0, regular_times = 0, remainder_rows = 0;

	unsigned long long block_size = 512 * 1024;	//Maximum size that could be transfered
	unsigned long long block_rows = 1000;	//Maximum number of lines that could be transfered

	if (sizeof_var >= block_size) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuple_number;
		remainder_rows = 0;
	} else if (tuple_number * sizeof_var <= block_size) {
		//Do single insert
		if (tuple_number <= block_rows) {
			regular_rows = tuple_number;
			regular_times = 1;
			remainder_rows = 0;
		} else {
			regular_rows = block_rows;
			regular_times = (int) tuple_number / regular_rows;
			remainder_rows = (int) tuple_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((int) block_size / sizeof_var >= block_rows ? block_rows : (int) block_size / sizeof_var);
		regular_times = (int) tuple_number / regular_rows;
		remainder_rows = (int) tuple_number % regular_rows;
	}

	//Alloc query String
	long long query_size = 0;
	char *insert_query = (remainder_rows > 0 ? OPH_DC_SQ_MULTI_INSERT_FRAG : OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL);
	query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) - 1 + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	unsigned long long i, j, k;
	int n = snprintf(query_string, query_size, insert_query, frag->fragment_name) - 1;
	if (compressed) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
		}
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
		}
	}
	query_string[n - 1] = ';';
	query_string[n] = 0;

	unsigned long long *idDim = (unsigned long long *) calloc(regular_rows, sizeof(unsigned long long));
	if (!(idDim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		return OPH_DC_DATA_ERROR;
	}
	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_DC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	else if (type_flag == OPH_DC_BIT_FLAG)
		res = oph_iob_bin_array_create_c(&binary, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		return OPH_DC_DATA_ERROR;
	}

	unsigned long long c_arg = regular_rows * 2;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		return OPH_DC_DATA_ERROR;
	}

	for (i = 0; i < c_arg; i++) {
		args[i] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (i = 0; i < c_arg; i++)
				if (args[i])
					free(args[i]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}
	}
	args[c_arg] = NULL;


	for (i = 0; i < regular_rows; i++) {
		args[2 * i]->arg_length = sizeof(unsigned long long);
		args[2 * i]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[2 * i]->arg_is_null = 0;
		args[2 * i]->arg = (unsigned long long *) (&(idDim[i]));

		args[2 * i + 1]->arg_length = sizeof_var;
		args[2 * i + 1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[2 * i + 1]->arg_is_null = 0;
		args[2 * i + 1]->arg = (char *) (binary + sizeof_var * i);
		idDim[i] = frag->key_start + i;
	}

	if (oph_ioserver_setup_query(server, query_string, regular_times, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(query_string);
		free(idDim);
		free(binary);
		for (i = 0; i < c_arg; i++)
			if (args[i])
				free(args[i]);
		free(args);
		return OPH_DC_DATA_ERROR;
	}

	struct timeval time;
	gettimeofday(&time, NULL);
	srand(time.tv_sec * 1000000 + time.tv_usec);

	//Fill array
	for (k = 0; k < regular_times; k++) {
		for (j = 0; j < regular_rows; j++) {
			if (_oph_dc_build_rand_row(binary + j * sizeof_var, array_length, type_flag, rand_alg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				free(query_string);
				free(idDim);
				free(binary);
				for (i = 0; i < c_arg; i++)
					if (args[i])
						free(args[i]);
				free(args);
				oph_ioserver_free_query(server, query);
				return OPH_DC_SERVER_ERROR;
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (i = 0; i < c_arg; i++)
				if (args[i])
					free(args[i]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}
		//Increase idDim
		for (i = 0; i < regular_rows; i++) {
			idDim[i] += regular_rows;
		}
	}

	oph_ioserver_free_query(server, query);
	free(query_string);
	query_string = NULL;

	if (remainder_rows > 0) {

		query_size =
		    snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL,
			     frag->fragment_name) - 1 + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
		query_string = (char *) malloc(query_size * sizeof(char));
		if (!(query_string)) {
			free(query_string);
			free(idDim);
			free(binary);
			for (i = 0; i < c_arg; i++)
				if (args[i])
					free(args[i]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}

		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1;
		if (compressed) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
			}
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
			}
		}
		query_string[n - 1] = ';';
		query_string[n] = 0;

		for (i = remainder_rows * 2; i < c_arg; i++) {
			if (args[i]) {
				free(args[i]);
				args[i] = NULL;
			}
		}

		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (i = 0; i < c_arg; i++)
				if (args[i])
					free(args[i]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}

		for (j = 0; j < remainder_rows; j++) {
			if (_oph_dc_build_rand_row(binary + j * sizeof_var, array_length, type_flag, rand_alg)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				free(query_string);
				free(idDim);
				free(binary);
				for (i = 0; i < c_arg; i++)
					if (args[i])
						free(args[i]);
				free(args);
				oph_ioserver_free_query(server, query);
				return OPH_DC_SERVER_ERROR;
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (i = 0; i < c_arg; i++)
				if (args[i])
					free(args[i]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC_SERVER_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	if (query_string)
		free(query_string);
	free(idDim);
	free(binary);
	for (i = 0; i < c_arg; i++)
		if (args[i])
			free(args[i]);
	free(args);

	return OPH_DC_SUCCESS;
}

int oph_dc_populate_fragment_with_rand_data2(oph_ioserver_handler * server, oph_odb_fragment * frag, unsigned long long tuple_number, int array_length, char *data_type, int compressed,
					     char *algorithm)
{
	if (!frag || !data_type || !server || !algorithm) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	char type_flag = oph_dc_typeof(data_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC_DATA_ERROR;
	}

	if (strcmp(algorithm, OPH_COMMON_RAND_ALGO_TEMP) != 0 && strcmp(algorithm, OPH_COMMON_RAND_ALGO_DEFAULT) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading algorithm type\n");
		return OPH_DC_DATA_ERROR;
	}
	//Alloc query String
	long long query_size = 0;
	char *insert_query = OPH_DC_SQ_CREATE_FRAG_FROM_RAND;
	query_size =
	    snprintf(NULL, 0, insert_query, frag->fragment_name, compressed ? OPH_IOSERVER_SQ_VAL_YES : OPH_IOSERVER_SQ_VAL_NO, data_type, tuple_number, frag->key_start, array_length, algorithm) + 1;
	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	int n =
	    snprintf(query_string, query_size, insert_query, frag->fragment_name, compressed ? OPH_IOSERVER_SQ_VAL_YES : OPH_IOSERVER_SQ_VAL_NO, data_type, tuple_number, frag->key_start, array_length,
		     algorithm);
	if (n >= query_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		free(query_string);
		return OPH_DC_DATA_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, query_string, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		free(query_string);
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		free(query_string);
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);
	free(query_string);

	return OPH_DC_SUCCESS;
}

int oph_dc_append_fragment_to_fragment(oph_ioserver_handler * input_server, oph_ioserver_handler * output_server, unsigned long long tot_rows, short int exec_flag, oph_odb_fragment * new_frag,
				       oph_odb_fragment * old_frag, long long *first_id, long long *last_id, oph_ioserver_query ** exec_query, oph_ioserver_query_arg *** exec_args)
{
	if (!new_frag || !old_frag || !first_id || !last_id || !input_server || !output_server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	//Check if connection to input and output fragment are set
	if (oph_dc_check_connection_to_db(output_server, new_frag->db_instance->dbms_instance, new_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}
	if (oph_dc_check_connection_to_db(input_server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	unsigned long long sizeof_var = 0;
	*first_id = 0;
	*last_id = 0;

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG, old_frag->fragment_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Read input fragment
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG "\n", old_frag->fragment_name);
#endif

	char read_query[query_buflen];
	int n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG, old_frag->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(input_server, read_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(input_server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(input_server, query);
		return OPH_DC_SERVER_ERROR;
	}
	oph_ioserver_free_query(input_server, query);
	query = NULL;

	// Init res 
	oph_ioserver_result *old_result = NULL;

	if (oph_ioserver_get_result(input_server, &old_result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(input_server, old_result);
		return OPH_DC_SERVER_ERROR;
	}
	if (old_result->num_fields != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(input_server, old_result);
		return OPH_DC_SERVER_ERROR;
	}

	unsigned long long l, rows = old_result->num_rows;
	sizeof_var = old_result->max_field_length[old_result->num_fields - 1];
	//Get max row length of input table
	if (!sizeof_var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragment is empty\n");
		oph_ioserver_free_result(input_server, old_result);
		return OPH_DC_SERVER_ERROR;
	}

	unsigned long long actual_size = 0;
	unsigned long long *id_dim = NULL;
	char *binary = NULL;
	oph_ioserver_query_arg **args = NULL;
	int c_arg = 2, ii, remake = 0;

	if ((exec_flag == 0) || (exec_flag == 2)) {
		//Last or middle
		if (!*exec_args) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");
			oph_ioserver_free_result(input_server, old_result);
			if (*exec_query)
				oph_ioserver_free_query(output_server, *exec_query);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC_SERVER_ERROR;
		}
		if (!*exec_query) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");
			if (*exec_args) {
				for (ii = 0; ii < c_arg; ii++) {
					if ((*exec_args)[ii]) {
						if ((*exec_args)[ii]->arg)
							free((*exec_args)[ii]->arg);
						free((*exec_args)[ii]);
					}
				}
				free(args);
			}
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC_SERVER_ERROR;
		}

		query = *exec_query;
		args = *exec_args;

		if (!args[0]->arg || !args[1]->arg) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");
			oph_ioserver_free_result(input_server, old_result);
			oph_ioserver_free_query(output_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC_SERVER_ERROR;
		}

		id_dim = args[0]->arg;
		binary = args[1]->arg;

		if (sizeof_var > args[1]->arg_length) {
			if (*exec_query)
				oph_ioserver_free_query(output_server, *exec_query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			args = NULL;
			remake = 1;
		}
	}
	if ((exec_flag == 1) || (exec_flag == 3) || remake) {
		*exec_args = NULL;
		*exec_query = NULL;

		//If first or only execution
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_INSERT_FRAG "\n", new_frag->fragment_name);
#endif

		query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_INSERT_FRAG, new_frag->fragment_name);
		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			oph_ioserver_free_result(input_server, old_result);
			return OPH_DC_SERVER_ERROR;
		}

		char insert_query[query_buflen];
		n = snprintf(insert_query, query_buflen, OPH_DC_SQ_INSERT_FRAG, new_frag->fragment_name);
		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			oph_ioserver_free_result(input_server, old_result);
			return OPH_DC_SERVER_ERROR;
		}

		binary = (char *) calloc(sizeof_var, sizeof(char));
		if (!binary) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
			oph_ioserver_free_result(input_server, old_result);
			return OPH_DC_DATA_ERROR;
		}

		id_dim = (unsigned long long *) calloc(1, sizeof(unsigned long long));
		if (!id_dim) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
			free(binary);
			oph_ioserver_free_result(input_server, old_result);
			return OPH_DC_DATA_ERROR;
		}

		query = NULL;
		args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
		if (!(args)) {
			free(binary);
			free(id_dim);
			oph_ioserver_free_result(input_server, old_result);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_DC_DATA_ERROR;
		}

		for (ii = 0; ii < c_arg; ii++) {
			args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
			if (!args[ii]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
				free(binary);
				free(id_dim);
				oph_ioserver_free_result(input_server, old_result);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				return OPH_DC_DATA_ERROR;
			}
		}
		args[c_arg] = NULL;


		args[0]->arg_length = sizeof(unsigned long long);
		args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[0]->arg_is_null = 0;
		args[0]->arg = (unsigned long long *) (id_dim);

		args[1]->arg_length = sizeof_var;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = 0;
		args[1]->arg = (char *) (binary);


		if (oph_ioserver_setup_query(output_server, insert_query, tot_rows, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			oph_ioserver_free_result(input_server, old_result);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			return OPH_DC_SERVER_ERROR;
		}
		//If first execute
		if ((exec_flag == 1 && tot_rows > 1) || remake) {
			*exec_args = args;
			*exec_query = query;
		}
	}

	oph_ioserver_row *curr_row = NULL;
	long long tmp_first_id = 0, tmp_last_id = 0;
	for (l = 0; l < rows; l++) {
		//Read data     
		if (oph_ioserver_fetch_row(input_server, old_result, &curr_row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
			oph_ioserver_free_result(input_server, old_result);
			oph_ioserver_free_query(output_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC_SERVER_ERROR;
		}

		actual_size = curr_row->field_lengths[1];
		*id_dim = (unsigned long long) strtoll(curr_row->row[0], NULL, 10);
		if (l == 0)
			tmp_first_id = *id_dim;
		tmp_last_id = *id_dim;

		memset(binary, 0, sizeof_var);
		memcpy(binary, curr_row->row[1], actual_size);

		if (oph_ioserver_execute_query(output_server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			oph_ioserver_free_result(input_server, old_result);
			oph_ioserver_free_query(output_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC_SERVER_ERROR;
		}
	}

	oph_ioserver_free_result(input_server, old_result);
	if ((exec_flag == 2) || (exec_flag == 3)) {
		//If last or only one - clean 
		for (ii = 0; ii < c_arg; ii++) {
			if (args[ii]) {
				if (args[ii]->arg)
					free(args[ii]->arg);
				free(args[ii]);
			}
		}
		free(args);
		oph_ioserver_free_query(output_server, query);
		*exec_args = NULL;
		*exec_query = NULL;
	}

	*first_id = tmp_first_id;
	*last_id = tmp_last_id;

	return OPH_DC_SUCCESS;
}

int oph_dc_copy_and_process_fragment2(int cubes_num, oph_ioserver_handler ** servers, unsigned long long tot_rows, oph_odb_fragment ** old_frags, const char *frag_name, int compressed,
				      const char *operation, const char *measure_type, const char *missingvalue)
{
	if (!cubes_num || !old_frags || !frag_name || !servers || !operation || !measure_type || !missingvalue) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	unsigned long long sizeof_var = 0;

	// Init res 
	oph_ioserver_result *old_result[cubes_num];

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);

	int ll, query_buflen, n;
	oph_ioserver_query *query = NULL;

	for (ll = 0; ll < cubes_num; ll++) {
		old_result[ll] = NULL;

		//Check if connection to input fragments are set
		if (oph_dc_check_connection_to_db(servers[ll], old_frags[ll]->db_instance->dbms_instance, old_frags[ll]->db_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
			return OPH_DC_SERVER_ERROR;
		}

		query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frags[ll]->fragment_name);
		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}
		//Read input fragment
		char read_query[query_buflen];
		n = snprintf(read_query, query_buflen, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frags[ll]->fragment_name);
		if (n >= query_buflen) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_ODB_STR_BUFF_OVERFLOW;
		}

		if (oph_ioserver_setup_query(servers[ll], read_query, 1, NULL, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
			return OPH_DC_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(servers[ll], query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
			oph_ioserver_free_query(servers[ll], query);
			return OPH_DC_SERVER_ERROR;
		}
		oph_ioserver_free_query(servers[ll], query);

		if (oph_ioserver_get_result(servers[ll], &old_result[ll])) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
			for (; ll >= 0; ll--)
				oph_ioserver_free_result(servers[ll], old_result[ll]);
			return OPH_DC_SERVER_ERROR;
		}
		if (old_result[ll]->num_fields != 2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
			for (; ll >= 0; ll--)
				oph_ioserver_free_result(servers[ll], old_result[ll]);
			return OPH_DC_SERVER_ERROR;
		}

		if (!ll) {
			sizeof_var = old_result[0]->max_field_length[old_result[0]->num_fields - 1];
			//Get max row length of input table
			if (!sizeof_var) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragment is empty\n");
				oph_ioserver_free_result(servers[0], old_result[0]);
				return OPH_DC_SERVER_ERROR;
			}
		} else {
			if (sizeof_var != old_result[ll]->max_field_length[old_result[ll]->num_fields - 1]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable\n");
				for (; ll >= 0; ll--)
					oph_ioserver_free_result(servers[ll], old_result[ll]);
				return OPH_DC_SERVER_ERROR;
			}
		}
	}

	unsigned long long actual_size = 0;
	int c_arg = 1 + cubes_num, ii;

	char _datatype[QUERY_BUFLEN], _datatypes[QUERY_BUFLEN];
	char _measure[QUERY_BUFLEN], _measures[QUERY_BUFLEN];
	*_datatypes = *_measures = 0;
	for (ii = 0; ii < cubes_num; ii++) {
		snprintf(_datatype, OPH_COMMON_BUFFER_LEN, "%soph_%s", ii ? "|" : "", measure_type);
		strncat(_datatypes, _datatype, QUERY_BUFLEN - strlen(_datatypes));
		snprintf(_measure, OPH_COMMON_BUFFER_LEN, "%s?", ii ? "," : "");
		strncat(_measures, _measure, QUERY_BUFLEN - strlen(_measures));
	}

	query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_INSERT_FRAG3, frag_name, compressed ? "oph_compress(" : "", _datatypes, measure_type, _measures, operation, missingvalue, compressed ? ")" : "");
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//If first or only execution
	char insert_query[query_buflen];
	n = snprintf(insert_query, query_buflen, OPH_DC_SQ_INSERT_FRAG3, frag_name, compressed ? "oph_compress(" : "", _datatypes, measure_type, _measures, operation, missingvalue,
		     compressed ? ")" : "");
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		for (ll = 0; ll < cubes_num; ll++)
			oph_ioserver_free_result(servers[ll], old_result[ll]);
		return OPH_DC_SERVER_ERROR;
	}

	char *binary[cubes_num];
	for (ll = 0; ll < cubes_num; ll++) {
		binary[ll] = (char *) calloc(sizeof_var, sizeof(char));
		if (!binary[ll]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
			for (ll--; ll >= 0; ll--)
				free(binary[ll]);
			for (ll = 0; ll < cubes_num; ll++)
				oph_ioserver_free_result(servers[ll], old_result[ll]);
			return OPH_DC_DATA_ERROR;
		}
	}

	unsigned long long *id_dim = (unsigned long long *) calloc(1, sizeof(unsigned long long));
	if (!id_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		for (ll = 0; ll < cubes_num; ll++)
			free(binary[ll]);
		for (ll = 0; ll < cubes_num; ll++)
			oph_ioserver_free_result(servers[ll], old_result[ll]);
		return OPH_DC_DATA_ERROR;
	}

	query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		for (ll = 0; ll < cubes_num; ll++)
			free(binary[ll]);
		free(id_dim);
		for (ll = 0; ll < cubes_num; ll++)
			oph_ioserver_free_result(servers[ll], old_result[ll]);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			for (ll = 0; ll < cubes_num; ll++)
				free(binary[ll]);
			free(id_dim);
			for (ll = 0; ll < cubes_num; ll++)
				oph_ioserver_free_result(servers[ll], old_result[ll]);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}
	}
	args[c_arg] = NULL;

	args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;
	args[0]->arg = (unsigned long long *) (id_dim);

	for (ll = 1; ll <= cubes_num; ll++) {
		args[ll]->arg_length = sizeof_var;
		args[ll]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[ll]->arg_is_null = 0;
		args[ll]->arg = binary[ll - 1];
	}

	oph_ioserver_handler *first_server = servers[0];

	if (oph_ioserver_setup_query(first_server, insert_query, tot_rows, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		for (ll = 0; ll < cubes_num; ll++)
			oph_ioserver_free_result(servers[ll], old_result[ll]);
		for (ii = 0; ii < c_arg; ii++) {
			if (args[ii]) {
				if (args[ii]->arg)
					free(args[ii]->arg);
				free(args[ii]);
			}
		}
		free(args);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row[cubes_num];

	unsigned long long l, rows = old_result[0]->num_rows;
	for (l = 0; l < rows; l++) {

		//Read data
		for (ll = 0; ll < cubes_num; ll++) {
			curr_row[ll] = NULL;
			if (oph_ioserver_fetch_row(servers[ll], old_result[ll], curr_row + ll)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
				for (ll = 0; ll < cubes_num; ll++)
					oph_ioserver_free_result(servers[ll], old_result[ll]);
				oph_ioserver_free_query(first_server, query);
				for (ii = 0; ii < c_arg; ii++) {
					if (args[ii]) {
						if (args[ii]->arg)
							free(args[ii]->arg);
						free(args[ii]);
					}
				}
				free(args);
				return OPH_DC_SERVER_ERROR;
			}
		}

		*id_dim = (unsigned long long) strtoll(curr_row[0]->row[0], NULL, 10);
		for (ll = 0; ll < cubes_num; ll++) {
			actual_size = curr_row[ll]->field_lengths[1];
			memset(binary[ll], 0, sizeof_var);
			memcpy(binary[ll], curr_row[ll]->row[1], actual_size);
		}

		if (oph_ioserver_execute_query(first_server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			for (ll = 0; ll < cubes_num; ll++)
				oph_ioserver_free_result(servers[ll], old_result[ll]);
			oph_ioserver_free_query(first_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			return OPH_DC_SERVER_ERROR;
		}
	}

	for (ll = 0; ll < cubes_num; ll++)
		oph_ioserver_free_result(servers[ll], old_result[ll]);

	//If last or only one - clean 
	for (ii = 0; ii < c_arg; ii++) {
		if (args[ii]) {
			if (args[ii]->arg)
				free(args[ii]->arg);
			free(args[ii]);
		}
	}
	free(args);
	oph_ioserver_free_query(first_server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_copy_and_process_fragment(oph_ioserver_handler * first_server, oph_ioserver_handler * second_server, unsigned long long tot_rows, oph_odb_fragment * old_frag1, oph_odb_fragment * old_frag2,
				     const char *frag_name, int compressed, const char *operation, const char *measure_type)
{
	if (!old_frag1 || !old_frag2 || !frag_name || !first_server || !second_server || !operation || !measure_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	unsigned long long sizeof_var = 0;

	// Init res 
	oph_ioserver_result *old_result1 = NULL;
	oph_ioserver_result *old_result2 = NULL;

	//Check if connection to input fragments are set
	if (oph_dc_check_connection_to_db(first_server, old_frag1->db_instance->dbms_instance, old_frag1->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag1->fragment_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Read input fragment
	char read_query[query_buflen];
	int n = snprintf(read_query, query_buflen, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag1->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(first_server, read_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(first_server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(first_server, query);
		return OPH_DC_SERVER_ERROR;
	}
	oph_ioserver_free_query(first_server, query);

	if (oph_ioserver_get_result(first_server, &old_result1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}
	if (old_result1->num_fields != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}

	unsigned long long l, rows = old_result1->num_rows;
	sizeof_var = old_result1->max_field_length[old_result1->num_fields - 1];
	//Get max row length of input table
	if (!sizeof_var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragment is empty\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_dc_check_connection_to_db(second_server, old_frag2->db_instance->dbms_instance, old_frag2->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}

	query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag2->fragment_name);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//Read input fragment
	char read_query2[query_buflen];
	n = snprintf(read_query2, query_buflen, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag2->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}

	query = NULL;
	if (oph_ioserver_setup_query(second_server, read_query2, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		oph_ioserver_free_result(first_server, old_result1);
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(second_server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_query(second_server, query);
		return OPH_DC_SERVER_ERROR;
	}
	oph_ioserver_free_query(second_server, query);

	if (oph_ioserver_get_result(second_server, &old_result2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_SERVER_ERROR;
	}
	if (old_result2->num_fields != 2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_SERVER_ERROR;
	}

	if (sizeof_var != old_result2->max_field_length[old_result2->num_fields - 1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_SERVER_ERROR;
	}

	unsigned long long actual_size = 0;
	int c_arg = 3, ii;

	query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_INSERT_COMPRESSED_FRAG2 : OPH_DC_SQ_INSERT_FRAG2, frag_name, operation, measure_type, measure_type, measure_type);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}
	//If first or only execution
	char insert_query[query_buflen];
	n = snprintf(insert_query, query_buflen, compressed ? OPH_DC_SQ_INSERT_COMPRESSED_FRAG2 : OPH_DC_SQ_INSERT_FRAG2, frag_name, operation, measure_type, measure_type, measure_type);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_SERVER_ERROR;
	}

	char *binary1 = (char *) calloc(sizeof_var, sizeof(char));
	if (!binary1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_DATA_ERROR;
	}
	char *binary2 = (char *) calloc(sizeof_var, sizeof(char));
	if (!binary2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		free(binary1);
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_DATA_ERROR;
	}

	unsigned long long *id_dim = (unsigned long long *) calloc(1, sizeof(unsigned long long));
	if (!id_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		free(binary1);
		free(binary2);
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		return OPH_DC_DATA_ERROR;
	}

	query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		free(binary1);
		free(binary2);
		free(id_dim);
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC_DATA_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(binary1);
			free(binary2);
			free(id_dim);
			oph_ioserver_free_result(first_server, old_result1);
			oph_ioserver_free_result(second_server, old_result2);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_DC_DATA_ERROR;
		}
	}
	args[c_arg] = NULL;

	args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;
	args[0]->arg = (unsigned long long *) (id_dim);

	args[1]->arg_length = sizeof_var;
	args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[1]->arg_is_null = 0;
	args[1]->arg = (char *) (binary1);

	args[2]->arg_length = sizeof_var;
	args[2]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[2]->arg_is_null = 0;
	args[2]->arg = (char *) (binary2);

	if (oph_ioserver_setup_query(first_server, insert_query, tot_rows, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		oph_ioserver_free_result(first_server, old_result1);
		oph_ioserver_free_result(second_server, old_result2);
		for (ii = 0; ii < c_arg; ii++) {
			if (args[ii]) {
				if (args[ii]->arg)
					free(args[ii]->arg);
				free(args[ii]);
			}
		}
		free(args);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row1 = NULL;
	oph_ioserver_row *curr_row2 = NULL;
	for (l = 0; l < rows; l++) {
		//Read data
		if (oph_ioserver_fetch_row(first_server, old_result1, &curr_row1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
			oph_ioserver_free_result(first_server, old_result1);
			oph_ioserver_free_result(second_server, old_result2);
			oph_ioserver_free_query(first_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			return OPH_DC_SERVER_ERROR;
		}
		if (oph_ioserver_fetch_row(second_server, old_result2, &curr_row2)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
			oph_ioserver_free_result(first_server, old_result1);
			oph_ioserver_free_result(second_server, old_result2);
			oph_ioserver_free_query(first_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			return OPH_DC_SERVER_ERROR;
		}

		*id_dim = (unsigned long long) strtoll(curr_row1->row[0], NULL, 10);

		actual_size = curr_row1->field_lengths[1];
		memset(binary1, 0, sizeof_var);
		memcpy(binary1, curr_row1->row[1], actual_size);

		actual_size = curr_row2->field_lengths[1];
		memset(binary2, 0, sizeof_var);
		memcpy(binary2, curr_row2->row[1], actual_size);

		if (oph_ioserver_execute_query(first_server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			oph_ioserver_free_result(first_server, old_result1);
			oph_ioserver_free_result(second_server, old_result2);
			oph_ioserver_free_query(first_server, query);
			for (ii = 0; ii < c_arg; ii++) {
				if (args[ii]) {
					if (args[ii]->arg)
						free(args[ii]->arg);
					free(args[ii]);
				}
			}
			free(args);
			return OPH_DC_SERVER_ERROR;
		}
	}

	oph_ioserver_free_result(first_server, old_result1);
	oph_ioserver_free_result(second_server, old_result2);

	//If last or only one - clean 
	for (ii = 0; ii < c_arg; ii++) {
		if (args[ii]) {
			if (args[ii]->arg)
				free(args[ii]->arg);
			free(args[ii]);
		}
	}
	free(args);
	oph_ioserver_free_query(first_server, query);

	return OPH_DC_SUCCESS;
}

int oph_dc_read_fragment_data(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, char *id_clause, char *array_clause, char *where_clause, int limit,
			      int raw_format, oph_ioserver_result ** frag_rows)
{
	if (!frag || !frag_rows || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}
	(*frag_rows) = NULL;
	//Check if connection to input and output fragment are set

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	char type_flag = '\0';
	int n, query_buflen = QUERY_BUFLEN;

	char *array_part = NULL, *read_query = NULL;

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);

	if (!raw_format) {
		//Select right data type
		type_flag = oph_dc_typeof(data_type);
		if (!type_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
			return OPH_DC_DATA_ERROR;
		}
#ifdef OPH_DEBUG_MYSQL
		char where_part_sql[QUERY_BUFLEN];
		char limit_part_sql[QUERY_BUFLEN];
#endif

		//Set up where part of the query
		char *where_part = NULL;
		if (!(where_part = (char *) calloc((where_clause ? strlen(where_clause) + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "")) : 0) + 1, sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_DC_DATA_ERROR;
		}
		if (where_clause) {
#ifdef OPH_DEBUG_MYSQL
			n = snprintf(where_part_sql, strlen(where_clause) + 1 + 7, " WHERE %s", where_clause);
#endif
			n = snprintf(where_part, strlen(where_clause) + 1 + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "")), OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s"),
				     where_clause);
		} else {
#ifdef OPH_DEBUG_MYSQL
			where_part_sql[0] = 0;
#endif
			where_part[0] = 0;
			n = 0;
		}
		//Set up limit part of the query
		char *limit_part = NULL;
		if (!(limit_part = (char *) calloc((limit ? (floor(log10(abs(limit))) + 1) + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "")) : 0) + 2, sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(where_part);
			return OPH_DC_DATA_ERROR;
		}
		if (limit) {
#ifdef OPH_DEBUG_MYSQL
			n = snprintf(limit_part_sql, (floor(log10(abs(limit))) + 1) + 1 + 8, " LIMIT %d;", limit);
#endif
			n = snprintf(limit_part, (floor(log10(abs(limit))) + 1) + 1 + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "")),
				     OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "%d"), limit);
		} else {
#ifdef OPH_DEBUG_MYSQL
			n = snprintf(limit_part_sql, 1 + 1, ";");
#endif
			limit_part[0] = '\0';
		}

		//Set up array part of the query
		if (!(array_part = (char *) calloc(array_clause ? strlen(array_clause) + OPH_DC_MAX_SIZE : OPH_DC_MIN_SIZE, sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(limit_part);
			free(where_part);
			return OPH_DC_DATA_ERROR;
		}

		if (type_flag == OPH_DC_BIT_FLAG) {
			if (array_clause)
				snprintf(array_part, strlen(array_clause) + OPH_DC_MAX_SIZE, "(oph_bit_subarray2('','',%s,'%s'))", (compressed ? "oph_uncompress('','',measure)" : "measure"),
					 array_clause);
			else
				snprintf(array_part, OPH_DC_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)" : "measure"));
			if (id_clause) {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_FRAG_SPECIAL3, MYSQL_FRAG_ID, id_clause, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			} else {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_FRAG_SPECIAL4, MYSQL_FRAG_ID, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			}
		} else {
			if (array_clause) {
				char ttype[QUERY_BUFLEN];
				snprintf(ttype, QUERY_BUFLEN, "%s", oph_dc_stringof(type_flag));
				snprintf(array_part, strlen(array_clause) + OPH_DC_MAX_SIZE, "(oph_get_subarray2('OPH_%s','OPH_%s',%s,'%s'))", ttype, ttype,
					 (compressed ? "oph_uncompress('','',measure)" : "measure"), array_clause);
			} else
				snprintf(array_part, OPH_DC_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)" : "measure"));
			if (id_clause) {
				query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_FRAG_SPECIAL1, MYSQL_FRAG_ID, id_clause, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), array_part,
							    frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			} else {
				query_buflen =
				    1 + snprintf(NULL, 0, OPH_DC_SQ_READ_FRAG_SPECIAL2, MYSQL_FRAG_ID, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), array_part, frag->fragment_name,
						 where_part, MYSQL_FRAG_ID, limit_part);
			}
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		read_query = (char *) malloc(query_buflen * sizeof(char));

		if (type_flag == OPH_DC_BIT_FLAG) {
			if (array_clause)
				n = snprintf(array_part, strlen(array_clause) + OPH_DC_MAX_SIZE, "(oph_bit_subarray2('','',%s,'%s'))", (compressed ? "oph_uncompress('','',measure)" : "measure"),
					     array_clause);
			else
				n = snprintf(array_part, OPH_DC_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)" : "measure"));
			if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_FRAG_SPECIAL3 "\n", MYSQL_FRAG_ID, id_clause, array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_FRAG_SPECIAL3, MYSQL_FRAG_ID, id_clause, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_FRAG_SPECIAL4 "\n", MYSQL_FRAG_ID, array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_FRAG_SPECIAL4, MYSQL_FRAG_ID, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			}
		} else {
			if (array_clause) {
				char ttype[QUERY_BUFLEN];
				snprintf(ttype, QUERY_BUFLEN, "%s", oph_dc_stringof(type_flag));
				n = snprintf(array_part, strlen(array_clause) + OPH_DC_MAX_SIZE, "(oph_get_subarray2('OPH_%s','OPH_%s',%s,'%s'))", ttype, ttype,
					     (compressed ? "oph_uncompress('','',measure)" : "measure"), array_clause);
			} else
				n = snprintf(array_part, OPH_DC_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)" : "measure"));
			if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_FRAG_SPECIAL1 "\n", MYSQL_FRAG_ID, id_clause, oph_dc_stringof(type_flag), array_part, frag->fragment_name, where_part_sql,
				       MYSQL_FRAG_ID, limit_part_sql);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_FRAG_SPECIAL1, MYSQL_FRAG_ID, id_clause, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), array_part,
					     frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_FRAG_SPECIAL2 "\n", MYSQL_FRAG_ID, oph_dc_stringof(type_flag), array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID,
				       limit_part_sql);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_FRAG_SPECIAL2, MYSQL_FRAG_ID, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), array_part,
					     frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			}
		}
		free(array_part);
		free(limit_part);
		free(where_part);

	} else if (!array_clause)	// Real raw
	{

		if (data_type && !strcasecmp(data_type, OPH_DC_BIT_TYPE)) {
			query_buflen =
			    1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG2 "\n", compressed ? "oph_bit_export('','OPH_INT',oph_uncompress('','',measure))" : "oph_bit_export('','OPH_INT',measure)",
					 frag->fragment_name);
		} else {
			query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, frag->fragment_name);
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		read_query = (char *) malloc(query_buflen * sizeof(char));

		if (data_type && !strcasecmp(data_type, OPH_DC_BIT_TYPE)) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG2 "\n", compressed ? "oph_bit_export('','OPH_INT',oph_uncompress('','',measure))" : "oph_bit_export('','OPH_INT',measure)",
			       frag->fragment_name);
#endif
			n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG2 "\n",
				     compressed ? "oph_bit_export('','OPH_INT',oph_uncompress('','',measure))" : "oph_bit_export('','OPH_INT',measure)", frag->fragment_name);
		} else {
			if (compressed) {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG, frag->fragment_name);
			} else {
#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG "\n", frag->fragment_name);
#endif
				n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG, frag->fragment_name);
			}
		}
	} else			// Raw adapted to inspect cube
	{
		if (limit) {
			if (where_clause) {
				if (id_clause) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_WLI, id_clause, array_clause, frag->fragment_name, where_clause, limit);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_WL, array_clause, frag->fragment_name, where_clause, limit);
				}
			} else {
				if (id_clause) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_LI, id_clause, array_clause, frag->fragment_name, limit);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_L, array_clause, frag->fragment_name, limit);
				}
			}
		} else {
			if (where_clause) {
				if (id_clause) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_WI, id_clause, array_clause, frag->fragment_name, where_clause);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_W, array_clause, frag->fragment_name, where_clause);
				}
			} else {
				if (id_clause) {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3_I, id_clause, array_clause, frag->fragment_name);
				} else {
					query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_READ_RAW_FRAG3, array_clause, frag->fragment_name);
				}
			}
		}

		if (query_buflen >= max_size) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
			return OPH_DC_SERVER_ERROR;
		}

		read_query = (char *) malloc(query_buflen * sizeof(char));

		if (limit) {
			if (where_clause) {
				if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_WLI "\n", id_clause, array_clause, frag->fragment_name, where_clause, limit);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_WLI, id_clause, array_clause, frag->fragment_name, where_clause, limit);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_WL "\n", array_clause, frag->fragment_name, where_clause, limit);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_WL, array_clause, frag->fragment_name, where_clause, limit);
				}
			} else {
				if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_LI "\n", id_clause, array_clause, frag->fragment_name, limit);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_LI, id_clause, array_clause, frag->fragment_name, limit);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_L "\n", array_clause, frag->fragment_name, limit);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_L, array_clause, frag->fragment_name, limit);
				}
			}
		} else {
			if (where_clause) {
				if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_WI "\n", id_clause, array_clause, frag->fragment_name, where_clause);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_WI, id_clause, array_clause, frag->fragment_name, where_clause);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_W "\n", array_clause, frag->fragment_name, where_clause);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_W, array_clause, frag->fragment_name, where_clause);
				}
			} else {
				if (id_clause) {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3_I "\n", id_clause, array_clause, frag->fragment_name);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3_I, id_clause, array_clause, frag->fragment_name);
				} else {
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: " MYSQL_DC_READ_RAW_FRAG3 "\n", array_clause, frag->fragment_name);
#endif
					n = snprintf(read_query, query_buflen, OPH_DC_SQ_READ_RAW_FRAG3, array_clause, frag->fragment_name);
				}
			}
		}
	}

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		free(read_query);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, read_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n", read_query);
		return OPH_DC_SERVER_ERROR;
	}
	free(read_query);

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n", read_query);
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	if (oph_ioserver_get_result(server, frag_rows)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, *frag_rows);
		return OPH_DC_SERVER_ERROR;
	}

	return OPH_DC_SUCCESS;
}

int oph_dc_get_total_number_of_elements_in_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, long long *count)
{
	if (!frag || !data_type || !count || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int n, query_buflen = QUERY_BUFLEN;
	char type_flag = oph_dc_typeof(data_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC_DATA_ERROR;
	}

	if (type_flag == OPH_DC_BIT_FLAG) {
		query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG : OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG, frag->fragment_name);
	} else {
		query_buflen =
		    1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG : OPH_DC_SQ_COUNT_ELEMENTS_FRAG, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag),
				 frag->fragment_name);
	}

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char select_query[query_buflen];

	if (type_flag == OPH_DC_BIT_FLAG) {
		if (compressed) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG "\n", frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG, frag->fragment_name);
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_BIT_ELEMENTS_FRAG "\n", frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG, frag->fragment_name);
		}
	} else {
		if (compressed) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_COMPRESSED_ELEMENTS_FRAG "\n", oph_dc_stringof(type_flag), frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), frag->fragment_name);
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_ELEMENTS_FRAG "\n", oph_dc_stringof(type_flag), frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_ELEMENTS_FRAG, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), frag->fragment_name);
		}
	}
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, select_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if (oph_ioserver_get_result(server, &result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_fields != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if (oph_ioserver_fetch_row(server, result, &curr_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (curr_row->row[0])
		*count = strtoll(curr_row->row[0], NULL, 10);
	else
		*count = 0;

	oph_ioserver_free_result(server, result);
	return OPH_DC_SUCCESS;
}

int oph_dc_get_total_number_of_rows_in_fragment(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, long long *count)
{
	if (!frag || !data_type || !count || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_COUNT_ROWS_FRAG, frag->fragment_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char select_query[query_buflen];
	int n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_ROWS_FRAG, frag->fragment_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, select_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if (oph_ioserver_get_result(server, &result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_fields != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if (oph_ioserver_fetch_row(server, result, &curr_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (curr_row->row[0])
		*count = strtoll(curr_row->row[0], NULL, 10);
	else
		*count = 0;

	oph_ioserver_free_result(server, result);
	return OPH_DC_SUCCESS;
}

int oph_dc_get_number_of_elements_in_fragment_row(oph_ioserver_handler * server, oph_odb_fragment * frag, char *data_type, int compressed, long long *length)
{
	if (!frag || !data_type || !length || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int n, query_buflen = QUERY_BUFLEN;
	char type_flag = oph_dc_typeof(data_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC_DATA_ERROR;
	}

	if (type_flag == OPH_DC_BIT_FLAG) {
		query_buflen = 1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG_ROW : OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG_ROW, frag->fragment_name);
	} else {
		query_buflen =
		    1 + snprintf(NULL, 0, compressed ? OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG_ROW : OPH_DC_SQ_COUNT_ELEMENTS_FRAG_ROW, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag),
				 frag->fragment_name);
	}

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char select_query[query_buflen];

	if (type_flag == OPH_DC_BIT_FLAG) {
		if (compressed) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG_ROW "\n", frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG_ROW, frag->fragment_name);
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_BIT_ELEMENTS_FRAG_ROW "\n", frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG_ROW, frag->fragment_name);
		}
	} else {
		if (compressed) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_COMPRESSED_ELEMENTS_FRAG_ROW "\n", oph_dc_stringof(type_flag), frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG_ROW, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), frag->fragment_name);
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_COUNT_ELEMENTS_FRAG_ROW "\n", oph_dc_stringof(type_flag), frag->fragment_name);
#endif
			n = snprintf(select_query, query_buflen, OPH_DC_SQ_COUNT_ELEMENTS_FRAG_ROW, oph_dc_stringof(type_flag), oph_dc_stringof(type_flag), frag->fragment_name);
		}
	}

	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, select_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if (oph_ioserver_get_result(server, &result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_fields != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if (oph_ioserver_fetch_row(server, result, &curr_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (curr_row->row[0])
		*length = strtoll(curr_row->row[0], NULL, 10);
	else
		*length = 0;

	oph_ioserver_free_result(server, result);
	return OPH_DC_SUCCESS;
}

int oph_dc_get_fragments_size_in_bytes(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, char *frag_name, long long *size)
{
	if (!dbms || !frag_name || !size || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, dbms, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: " MYSQL_DC_SIZE_ELEMENTS_FRAG "\n", frag_name);
#endif

	int query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_SIZE_ELEMENTS_FRAG, frag_name);
	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char select_query[query_buflen];
	int n = snprintf(select_query, query_buflen, OPH_DC_SQ_SIZE_ELEMENTS_FRAG, frag_name);
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, select_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n", select_query);
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if (oph_ioserver_get_result(server, &result)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (result->num_fields != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if (oph_ioserver_fetch_row(server, result, &curr_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC_SERVER_ERROR;
	}
	if (curr_row->row[0])
		*size = strtoll(curr_row->row[0], NULL, 10);
	else
		*size = 0;

	oph_ioserver_free_result(server, result);
	return OPH_DC_SUCCESS;
}

int oph_dc_get_primitives(oph_ioserver_handler * server, oph_odb_dbms_instance * dbms, char *frag_name, oph_ioserver_result ** frag_rows)
{
	if (!dbms || !frag_rows || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	if (oph_dc_check_connection_to_db(server, dbms, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC_SERVER_ERROR;
	}

	int n = 0, query_buflen = QUERY_BUFLEN;

	if (!frag_name) {
		query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST);
	} else {
		query_buflen = 1 + snprintf(NULL, 0, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST_W, frag_name);
	}

	long long max_size = QUERY_BUFLEN;
	oph_pid_get_buffer_size(&max_size);
	if (query_buflen >= max_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer size (%ld bytes) is too small.\n", max_size);
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	char select_query[query_buflen];

	if (!frag_name) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_QUERY_RETRIEVE_PRIMITIVES_LIST "\n");
#endif
		n = snprintf(select_query, query_buflen, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST);
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_QUERY_RETRIEVE_PRIMITIVES_LIST_W "\n", frag_name);
#endif
		n = snprintf(select_query, query_buflen, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST_W, frag_name);
	}
	if (n >= query_buflen) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ODB_STR_BUFF_OVERFLOW;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, select_query, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	if (oph_ioserver_get_result(server, frag_rows)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, *frag_rows);
		return OPH_DC_SERVER_ERROR;
	}

	return OPH_DC_SUCCESS;
}

int oph_dc_generate_fragment_name(char *db_name, int id_datacube, int proc_rank, int frag_number, char (*frag_name)[OPH_ODB_STGE_FRAG_NAME_SIZE])
{
	if (!frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	int n = 0;
	if (db_name)
		n = snprintf(*frag_name, OPH_ODB_STGE_FRAG_NAME_SIZE, OPH_DC_DB_FRAG_NAME, db_name, id_datacube, proc_rank, frag_number);
	else
		n = snprintf(*frag_name, OPH_ODB_STGE_FRAG_NAME_SIZE, OPH_DC_FRAG_NAME, id_datacube, proc_rank, frag_number);

	if (n >= OPH_ODB_STGE_FRAG_NAME_SIZE) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
		return OPH_DC_DATA_ERROR;
	} else
		return OPH_DC_SUCCESS;
}

int oph_dc_generate_db_name(char *odb_name, int id_datacube, int id_dbms, int proc_rank, int db_number, char (*db_name)[OPH_ODB_STGE_DB_NAME_SIZE])
{
	if (!db_name || !odb_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	int n = 0;
	n = snprintf(*db_name, OPH_ODB_STGE_DB_NAME_SIZE, OPH_DC_DB_NAME, odb_name, id_datacube, id_dbms, proc_rank, db_number);
	if (n >= OPH_ODB_STGE_FRAG_NAME_SIZE) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
		return OPH_DC_DATA_ERROR;
	} else
		return OPH_DC_SUCCESS;
}

int oph_dc_check_data_type(char *input_type)
{
	if (!input_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC_NULL_PARAM;
	}

	char new_type[strlen(input_type)];
	if (strcasestr(input_type, OPH_DC_COMPLEX_DATATYPE_PREFIX)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Complex data types are partially supported: data will be considered as simple\n");
		sprintf(new_type, "oph_%s", input_type + strlen(OPH_DC_COMPLEX_DATATYPE_PREFIX));
		input_type = new_type;
	}

	if (!strncmp(input_type, OPH_DC_DOUBLE_TYPE, sizeof(OPH_DC_DOUBLE_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_FLOAT_TYPE, sizeof(OPH_DC_FLOAT_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_BYTE_TYPE, sizeof(OPH_DC_INT_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_SHORT_TYPE, sizeof(OPH_DC_LONG_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_INT_TYPE, sizeof(OPH_DC_INT_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_LONG_TYPE, sizeof(OPH_DC_LONG_TYPE)))
		return OPH_DC_SUCCESS;
	if (!strncmp(input_type, OPH_DC_BIT_TYPE, sizeof(OPH_DC_BIT_TYPE)))
		return OPH_DC_SUCCESS;

	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
	return OPH_DC_DATA_ERROR;
}
