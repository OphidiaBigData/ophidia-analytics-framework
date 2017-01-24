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

#include "oph_datacube2_library.h"

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
#include "debug.h"

#define OPH_DC2_MAX_SIZE 100 
#define OPH_DC2_MIN_SIZE 50 

extern int msglevel;

char oph_dc2_typeof(char* data_type)
{
	if(!strcasecmp(data_type,OPH_DC2_BYTE_TYPE)) return OPH_DC2_BYTE_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_SHORT_TYPE)) return OPH_DC2_SHORT_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_INT_TYPE)) return OPH_DC2_INT_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_LONG_TYPE)) return OPH_DC2_LONG_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_FLOAT_TYPE)) return OPH_DC2_FLOAT_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_DOUBLE_TYPE)) return OPH_DC2_DOUBLE_FLAG;
	else if(!strcasecmp(data_type,OPH_DC2_BIT_TYPE)) return OPH_DC2_BIT_FLAG;
	return 0;
}

const char* oph_dc2_stringof(char type_flag)
{
	switch (type_flag)
	{
		case OPH_DC2_BYTE_FLAG: return OPH_DC2_BYTE_TYPE;
		case OPH_DC2_SHORT_FLAG: return OPH_DC2_SHORT_TYPE;
		case OPH_DC2_INT_FLAG: return OPH_DC2_INT_TYPE;
		case OPH_DC2_LONG_FLAG: return OPH_DC2_LONG_TYPE;
		case OPH_DC2_FLOAT_FLAG: return OPH_DC2_FLOAT_TYPE;
		case OPH_DC2_DOUBLE_FLAG: return OPH_DC2_DOUBLE_TYPE;
		case OPH_DC2_BIT_FLAG: return OPH_DC2_BIT_TYPE;
	}
	return 0;
}

int oph_dc2_setup_dbms(oph_ioserver_handler **server, char *server_type)
{
	if(!server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
  
  if(oph_ioserver_setup(server_type, server)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup server library\n");
    return OPH_DC2_SERVER_ERROR;
  }
  return OPH_DC2_SUCCESS;
}

int oph_dc2_connect_to_dbms(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms, unsigned long flag)
{
	if(!dbms || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	dbms->conn = NULL;
  oph_ioserver_params conn_params;
  conn_params.host = dbms->hostname;
  conn_params.user = dbms->login;
  conn_params.passwd = dbms->pwd;
  conn_params.port = dbms->port;
  conn_params.db_name = NULL;
  conn_params.opt_flag = flag;
  
  if (oph_ioserver_connect(server, &conn_params, &dbms->conn))
  {
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection error\n");
    oph_dc2_disconnect_from_dbms(server, dbms);
    return OPH_DC2_SERVER_ERROR;
  }
  return OPH_DC2_SUCCESS;
}

int oph_dc2_use_db_of_dbms(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms, oph_odb_db_instance *db)
{
	if(!dbms || !db || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, dbms, NULL, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to dbms.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if(dbms->conn){
		if(oph_ioserver_use_db(server, db->db_name, dbms->conn))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set default database\n");
			return OPH_DC2_SERVER_ERROR;
		}
	}
	else{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection not established\n");
		return OPH_DC2_SERVER_ERROR;
	}
	return OPH_DC2_SUCCESS;

}

int oph_dc2_check_connection_to_db(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms, oph_odb_db_instance *db, unsigned long flag)
{
	if(!dbms || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if(!(dbms->conn)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Connection was somehow closed.\n");
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_params conn_params;
  conn_params.host = dbms->hostname;
  conn_params.user = dbms->login;
  conn_params.passwd = dbms->pwd;
  conn_params.port = dbms->port;
  conn_params.db_name = (db ? db->db_name : NULL);
  conn_params.opt_flag = flag;

  if (oph_ioserver_connect(server, &conn_params, &dbms->conn))
  {
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Server connection error\n");
    return OPH_DC2_SERVER_ERROR;
  }
	return OPH_DC2_SUCCESS;
}

int oph_dc2_disconnect_from_dbms(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms)
{
	if(!dbms || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	oph_ioserver_close(server, dbms->conn);
  return OPH_DC2_SUCCESS;
}

int oph_dc2_cleanup_dbms(oph_ioserver_handler *server)
{
	if(!server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
  
  oph_ioserver_cleanup(server);
  return OPH_DC2_SUCCESS;
}

int oph_dc2_create_db(oph_ioserver_handler *server, oph_odb_db_instance *db)
{
	if(!db || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if(oph_dc2_check_connection_to_db(server, db->dbms_instance,NULL, 0)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

	char db_creation_query[QUERY_BUFLEN];

#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: "MYSQL_DC_CREATE_DB"\n", db->db_name);
#endif
	int n = snprintf(db_creation_query,QUERY_BUFLEN, OPH_DC_SQ_CREATE_DB, db->db_name);
	if(n >= QUERY_BUFLEN){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, db->dbms_instance->conn, db_creation_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, db->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);
	return OPH_DC2_SUCCESS;
}

int oph_dc2_delete_db(oph_ioserver_handler *server, oph_odb_db_instance *db)
{
	if(!db || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, db->dbms_instance, db, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	//Check if Database is empty and DELETE
	char delete_query[QUERY_BUFLEN];
	int n;
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: "MYSQL_DC_DELETE_DB"\n", db->db_name);
#endif
	n = snprintf(delete_query,QUERY_BUFLEN, OPH_DC_SQ_DROP_DB, db->db_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, db->dbms_instance->conn, delete_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, db->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_create_empty_fragment(oph_ioserver_handler *server, oph_odb_fragment *frag)
{
	if(!frag || !server){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC2_NULL_PARAM;
	}
	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}


	char create_query[QUERY_BUFLEN];
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: "MYSQL_DC_CREATE_FRAG"\n", frag->fragment_name);
#endif
	int n = snprintf(create_query,QUERY_BUFLEN, OPH_DC_SQ_CREATE_FRAG, frag->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, create_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_create_empty_fragment_from_name(oph_ioserver_handler *server, const char* frag_name, oph_odb_db_instance *db_instance)
{
	if(!frag_name || !server || !db_instance){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC2_NULL_PARAM;
	}
	if( oph_dc2_check_connection_to_db(server, db_instance->dbms_instance, db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}


	char create_query[QUERY_BUFLEN];
	int n = snprintf(create_query,QUERY_BUFLEN, OPH_DC_SQ_CREATE_FRAG, frag_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, db_instance->dbms_instance->conn, create_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_delete_fragment(oph_ioserver_handler *server, oph_odb_fragment *frag)
{
	if(!frag || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char delete_query[QUERY_BUFLEN];
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: "MYSQL_DC_DELETE_FRAG"\n", frag->fragment_name);
#endif
	int n = snprintf(delete_query, QUERY_BUFLEN, OPH_DC_SQ_DELETE_FRAG, frag->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, delete_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_create_fragment_from_query(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id)
{
	return oph_dc2_create_fragment_from_query2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL);
}

//Removed multiple statement execution
int oph_dc2_create_fragment_from_query2(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, long long *block_size)
{
	UNUSED(start_id)

	if(!old_frag || !operation || !server){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC2_NULL_PARAM;
	}
	if( oph_dc2_check_connection_to_db(server, old_frag->db_instance->dbms_instance,old_frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char create_query[QUERY_BUFLEN];
	int n;

	if(new_frag_name == NULL){
		n = snprintf(create_query, QUERY_BUFLEN, operation);
		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

    oph_ioserver_query *query = NULL;
		if (oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, NULL, &query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n",create_query);
			return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n",create_query);
      oph_ioserver_free_query(server, query);
			return OPH_DC2_SERVER_ERROR;
		}

    oph_ioserver_free_query(server, query);
	}
	else{

		if (where)
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL
				  	printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WGB"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
 					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WG"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					 n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_W"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
#endif
			 n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		}
		else
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL
 					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_GB"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL   
 					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_G"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL   
 				printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN"\n",new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
#endif
				n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
			}

		}

		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

    oph_ioserver_query *query = NULL;
		if (oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, NULL, &query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n",create_query);
			return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s',\n",create_query);
      oph_ioserver_free_query(server, query);
			return OPH_DC2_SERVER_ERROR;
		}

    oph_ioserver_free_query(server, query);
  }

	return OPH_DC2_SUCCESS;
}

int oph_dc2_create_fragment_from_query_with_param(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, char* param, long long param_size)
{
	return oph_dc2_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size, 1);
}

int oph_dc2_create_fragment_from_query_with_param2(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, long long *block_size, char* param, long long param_size)
{
	return oph_dc2_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, block_size, param, param_size, 1);
}

int oph_dc2_create_fragment_from_query_with_params(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, char* param, long long param_size, int num)
{
	return oph_dc2_create_fragment_from_query_with_params2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size, num);
}

//Removed multiple statement execution
int oph_dc2_create_fragment_from_query_with_params2(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, long long *block_size, char* param, long long param_size, int num)
{
	UNUSED(start_id)

	if(!old_frag || !operation || (!param && param_size) || (param && !param_size) || !server)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC2_NULL_PARAM;
	}
	if (num<1)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "At least an occurrance of parameter has to be found\n");
		return OPH_DC2_DATA_ERROR;
	}
	if( oph_dc2_check_connection_to_db(server, old_frag->db_instance->dbms_instance,old_frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	int ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **)calloc(1+num,sizeof(oph_ioserver_query_arg*)); 
	if(!(args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC2_DATA_ERROR;
	}  

	for(ii = 0; ii < num; ii++){
		args[ii] = (oph_ioserver_query_arg *)calloc(1, sizeof(oph_ioserver_query_arg));
		if(!args[ii]){
			  pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
			free(args);
			return OPH_DC2_DATA_ERROR;
		} 
	}
	args[num] = NULL;


	int n, nn = !param;

	char create_query[QUERY_BUFLEN];
	if(new_frag_name == NULL)
	{
		n = snprintf(create_query, QUERY_BUFLEN, operation);
		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

		for(ii = 0; ii < num; ii++)
		{
			args[ii]->arg_length = param_size;
			args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
			args[ii]->arg_is_null = nn;
			args[ii]->arg = param;
		}

		if(oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, args, &query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC2_SERVER_ERROR;
		}

		for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
	}
	else
	{

		if (where)
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL   
 					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WGB"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_WGB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WG"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					 n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL   
				printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_W"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
#endif
				n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		}
		else
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_GB"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_GB, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_G"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN"\n",new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);		
			}
		}

		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

		for(ii = 0; ii < num; ii++)
		{
			args[ii]->arg_length = param_size;
			args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
			args[ii]->arg_is_null = nn;
			args[ii]->arg = param;
		}

		if(oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, args, &query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query '%s'\n",create_query);
			for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_DC2_SERVER_ERROR;
		}

		for(ii = 0; ii < num; ii++) if(args[ii]) free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);

	}

	return OPH_DC2_SUCCESS;
}

int oph_dc2_create_fragment_from_query_with_aggregation(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, char* param, long long param_size)
{
	return oph_dc2_create_fragment_from_query_with_aggregation2(server, old_frag, new_frag_name, operation, where, aggregate_number, start_id, NULL, param, param_size);
}

//Removed multiple statement execution
int oph_dc2_create_fragment_from_query_with_aggregation2(oph_ioserver_handler *server, oph_odb_fragment *old_frag, char *new_frag_name, char *operation, char *where, long long *aggregate_number, long long* start_id, long long *block_size, char* param, long long param_size)
{
	UNUSED(start_id)

	if(!old_frag || !operation || (!param && param_size) || (param && !param_size) || !server)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DC2_NULL_PARAM;
	}
	if( oph_dc2_check_connection_to_db(server, old_frag->db_instance->dbms_instance,old_frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}
	
  oph_ioserver_query *query = NULL;
  int c_arg = 3, ii;
  oph_ioserver_query_arg **args = (oph_ioserver_query_arg **)calloc(c_arg,sizeof(oph_ioserver_query_arg*)); 
	if(!(args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC2_DATA_ERROR;
	}  

  for(ii = 0; ii < c_arg -1; ii++){
    args[ii] = (oph_ioserver_query_arg *)calloc(1, sizeof(oph_ioserver_query_arg));
	  if(!args[ii]){
		  pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      return OPH_DC2_DATA_ERROR;
	  } 
  }
  args[c_arg -1] = NULL;

	int n;
	if (!param) n=1;

	char create_query[QUERY_BUFLEN];
	if(new_frag_name == NULL)
	{
		n = snprintf(create_query, QUERY_BUFLEN, operation);
		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

    args[0]->arg_length = param_size;
		args[0]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[0]->arg_is_null = n;    
		args[0]->arg = param;

    args[1]->arg_length = param_size;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = n;    
		args[1]->arg = param;

    if(oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, args, &query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      oph_ioserver_free_query(server, query);
      return OPH_DC2_SERVER_ERROR;
		}

    for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
    free(args);
    oph_ioserver_free_query(server, query);
	}
	else{

		if (where)
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WGB2"\n",new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *block_size);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_WGB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_WG"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, MYSQL_DC_APPLY_PLUGIN_WG, new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL   
				printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_W"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
#endif
				n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_W, new_frag_name, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, where);
			}
		}
		else
		{
			if (aggregate_number)
			{
				if (block_size){
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_GB2"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *block_size, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *block_size);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_GB2, new_frag_name, MYSQL_FRAG_ID, *block_size, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *block_size);
				}
				else{
#ifdef OPH_DEBUG_MYSQL   
					printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN_G"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, *aggregate_number, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
#endif
					n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN_G, new_frag_name, MYSQL_FRAG_ID, *aggregate_number, operation, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, old_frag->fragment_name, MYSQL_FRAG_ID, *aggregate_number);
				}
			}
			else{
#ifdef OPH_DEBUG_MYSQL   
				printf("ORIGINAL QUERY: "MYSQL_DC_APPLY_PLUGIN"\n", new_frag_name, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);
#endif
				n = snprintf(create_query, QUERY_BUFLEN, OPH_DC_SQ_APPLY_PLUGIN, new_frag_name, MYSQL_FRAG_ID, operation, MYSQL_FRAG_MEASURE, old_frag->fragment_name);		
			}
		}

		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			return OPH_DC2_SERVER_ERROR;
		}

    args[0]->arg_length = param_size;
		args[0]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[0]->arg_is_null = n;    
		args[0]->arg = param;

    args[1]->arg_length = param_size;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = n;    
		args[1]->arg = param;

    if(oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, create_query, 1, args, &query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query '%s'\n", create_query);
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      return OPH_DC2_SERVER_ERROR;
		}

		if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query '%s'\n", query);
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      oph_ioserver_free_query(server, query);
      return OPH_DC2_SERVER_ERROR;
		}

    for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
    free(args);
    oph_ioserver_free_query(server, query);

	}

	return OPH_DC2_SUCCESS;
}

int oph_dc2_populate_fragment_with_rand_data(oph_ioserver_handler *server, oph_odb_fragment *frag, int tuple_number, int array_length, char *data_type, int compressed)
{
	if(!frag || !data_type || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance,frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char type_flag = oph_dc2_typeof(data_type);
	if (!type_flag)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC2_DATA_ERROR;
	}

	char insert_query[QUERY_BUFLEN];
	int n;

	if(compressed == 1){
#ifdef OPH_DEBUG_MYSQL
  	printf("ORIGINAL QUERY: "MYSQL_DC_INSERT_COMPRESSED_FRAG"\n", frag->fragment_name);
#endif
	  n = snprintf(insert_query,QUERY_BUFLEN, OPH_DC_SQ_INSERT_COMPRESSED_FRAG, frag->fragment_name);
  }
	else{
#ifdef OPH_DEBUG_MYSQL
  	printf("ORIGINAL QUERY: "MYSQL_DC_INSERT_FRAG"\n", frag->fragment_name);
#endif
	  n = snprintf(insert_query,QUERY_BUFLEN, OPH_DC_SQ_INSERT_FRAG, frag->fragment_name);
  }
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
    return OPH_DC2_SERVER_ERROR;
	}

	unsigned long long sizeof_var = 0;

	char measure_b;
	short measure_s;
	int measure_i;
	long long measure_l;
	float measure_f;
	double measure_d;
	char measure_c;

	unsigned long long idDim = 0;

	if(type_flag == OPH_DC2_BYTE_FLAG)
		sizeof_var = array_length*sizeof(char);
	else if(type_flag == OPH_DC2_SHORT_FLAG)
		sizeof_var = array_length*sizeof(short);
	else if(type_flag == OPH_DC2_INT_FLAG)
		sizeof_var = array_length*sizeof(int);
	else if(type_flag == OPH_DC2_LONG_FLAG)
		sizeof_var = array_length*sizeof(long long);
	else if(type_flag == OPH_DC2_FLOAT_FLAG)
		sizeof_var = array_length*sizeof(float);
	else if(type_flag == OPH_DC2_DOUBLE_FLAG)
		sizeof_var = array_length*sizeof(double);
	else if(type_flag == OPH_DC2_BIT_FLAG)
	{
		sizeof_var = array_length*sizeof(char)/8;
		if (array_length%8) sizeof_var++;
		array_length = sizeof_var; // a bit array correspond to a char array with 1/8 elements
	}

	//Create binary array
	char* binary = 0;
	int res = 0;

	if(type_flag == OPH_DC2_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length);
	else if(type_flag == OPH_DC2_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length);
	else if(type_flag == OPH_DC2_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length);
	else if(type_flag == OPH_DC2_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length);
	else if(type_flag == OPH_DC2_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length);
	else if(type_flag == OPH_DC2_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length);
	else if(type_flag == OPH_DC2_BIT_FLAG)
		res = oph_iob_bin_array_create_c(&binary, array_length);

	if(res){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		return OPH_DC2_SERVER_ERROR;
	}


  oph_ioserver_query *query = NULL;
  int c_arg = 3, ii;
  oph_ioserver_query_arg **args = (oph_ioserver_query_arg **)calloc(c_arg,sizeof(oph_ioserver_query_arg*)); 
	if(!(args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(binary);
		return OPH_DC2_DATA_ERROR;
	}  

  for(ii = 0; ii < c_arg -1; ii++){
    args[ii] = (oph_ioserver_query_arg *)calloc(1, sizeof(oph_ioserver_query_arg));
	  if(!args[ii]){
		  pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
			free(binary);
      return OPH_DC2_DATA_ERROR;
	  } 
  }
  args[c_arg -1] = NULL;


  args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;    

  args[1]->arg_length = sizeof_var;
	args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[1]->arg_is_null = 0;  

	args[1]->arg = (char*)(binary);
	args[0]->arg = (unsigned long long*)(&idDim);

	idDim = frag->key_start;

  if(oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, insert_query, tuple_number, args, &query)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary);
    for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
    free(args);
    return OPH_DC2_SERVER_ERROR;
	}

	int l,m;
	struct timeval time;
	gettimeofday(&time, NULL);
	srand (time.tv_sec*1000000 + time.tv_usec);
	for(l=0; l < tuple_number; l++){
		
		//Fill array				
		for(m = 0; m < array_length; m++){
			if(type_flag == OPH_DC2_BYTE_FLAG){
				measure_b = (((float)rand()/RAND_MAX)*100.0);
				res = oph_iob_bin_array_add_b(binary, &measure_b, (long long)m);
			}
			else if(type_flag == OPH_DC2_SHORT_FLAG){
				measure_s = (((float)rand()/RAND_MAX)*1000.0);
				res = oph_iob_bin_array_add_s(binary, &measure_s, (long long)m);
			}
			else if(type_flag == OPH_DC2_INT_FLAG){
				measure_i = (((double)rand()/RAND_MAX)*1000.0);
				res = oph_iob_bin_array_add_i(binary, &measure_i, (long long)m);
			}
			else if(type_flag == OPH_DC2_LONG_FLAG){
				measure_l = (((double)rand()/RAND_MAX)*1000.0);
				res = oph_iob_bin_array_add_l(binary, &measure_l, (long long)m);
			}
			else if(type_flag == OPH_DC2_FLOAT_FLAG){
				measure_f = ((float)rand()/RAND_MAX)*1000.0;
				res = oph_iob_bin_array_add_f(binary, &measure_f, (long long)m);
			}
			else if(type_flag == OPH_DC2_DOUBLE_FLAG){
				measure_d =  ((double)rand()/RAND_MAX)*1000.0;
				res = oph_iob_bin_array_add_d(binary, &measure_d, (long long)m);
			}
			else if(type_flag == OPH_DC2_BIT_FLAG){
				measure_c =  ((char)rand()/RAND_MAX)*1000.0;
				res = oph_iob_bin_array_add_c(binary, &measure_c, (long long)m);
			}

			if(res){
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				free(binary);
        for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
        free(args);
        oph_ioserver_free_query(server, query);
				return OPH_DC2_SERVER_ERROR;
			}
		}

		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
	    free(binary);
      for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
      free(args);
      oph_ioserver_free_query(server, query);
      return OPH_DC2_SERVER_ERROR;
		}

		idDim++;
	}
	free(binary);
  for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
  free(args);
  oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_append_fragment_to_fragment(oph_ioserver_handler *server, unsigned long long tot_rows, short int exec_flag, oph_odb_fragment *new_frag, oph_odb_fragment *old_frag, long long *first_id, long long *last_id, oph_ioserver_query **exec_query, oph_ioserver_query_arg ***exec_args)
{
	if(!new_frag || !old_frag || !first_id || !last_id || !server)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	//Check if connection to input and output fragment are set
	if( oph_dc2_check_connection_to_db(server, new_frag->db_instance->dbms_instance,new_frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}
	if( oph_dc2_check_connection_to_db(server, old_frag->db_instance->dbms_instance,old_frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char read_query[QUERY_BUFLEN];
	unsigned long long sizeof_var = 0;
	*first_id = 0;
	*last_id = 0;		
	
	int n;
	//Read input fragment
#ifdef OPH_DEBUG_MYSQL
	printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG"\n", old_frag->fragment_name);
#endif
  	n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG, old_frag->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, old_frag->db_instance->dbms_instance->conn, read_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, old_frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}
	oph_ioserver_free_query(server, query);
		
	// Init res 
	oph_ioserver_result *old_result = NULL;

	if(oph_ioserver_get_result(server, old_frag->db_instance->dbms_instance->conn,  &old_result))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, old_result);
		return OPH_DC2_SERVER_ERROR;
	}
	if(old_result->num_fields != 2)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, old_result);
		return OPH_DC2_SERVER_ERROR;
	}

	unsigned long long l, rows = old_result->num_rows;
	sizeof_var = old_result->max_field_length[old_result->num_fields-1];
	//Get max row length of input table
	if(!sizeof_var)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragment is empty\n");
		oph_ioserver_free_result(server, old_result);
		return OPH_DC2_SERVER_ERROR;
	}

	char insert_query[QUERY_BUFLEN];
	unsigned long long actual_size = 0;
	unsigned long long *id_dim = NULL;
	char* binary = NULL;	
	oph_ioserver_query_arg **args = NULL;
	int c_arg = 3, ii, remake=0;

	if((exec_flag == 0) || (exec_flag == 2))
	{
		//Last or middle
		if(!*exec_args)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");		
			oph_ioserver_free_result(server, old_result);
			if(*exec_query) oph_ioserver_free_query(server, *exec_query);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC2_SERVER_ERROR;
		}
		if(!*exec_query)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");		
			if(*exec_args)
			{
				for(ii = 0; ii < c_arg -1; ii++)
				{     
					if((*exec_args)[ii])
					{
						if((*exec_args)[ii]->arg) free((*exec_args)[ii]->arg);
						free((*exec_args)[ii]); 
					} 
				} 
				free(args);
			}
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC2_SERVER_ERROR;
		}

		query = *exec_query;
		args = *exec_args;

		if(!args[0]->arg || !args[1]->arg )
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute query\n");		
			oph_ioserver_free_result(server, old_result);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii])
				{
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC2_SERVER_ERROR;
		}

		id_dim = args[0]->arg;
		binary = args[1]->arg;

		if (sizeof_var > args[1]->arg_length)
		{
			if(*exec_query) oph_ioserver_free_query(server, *exec_query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii])
				{
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			args = NULL;
			remake=1;
		}
	}
	if ((exec_flag == 1) || (exec_flag == 3) || remake)
	{
		*exec_args = NULL;
		*exec_query = NULL;

		//If first or only execution
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: "MYSQL_DC_INSERT_FRAG"\n", new_frag->fragment_name);
#endif
		n = snprintf(insert_query, QUERY_BUFLEN,  OPH_DC_SQ_INSERT_FRAG, new_frag->fragment_name);
		if(n >= QUERY_BUFLEN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
			oph_ioserver_free_result(server, old_result);
			return OPH_DC2_SERVER_ERROR;
		}

		binary = (char*)calloc(sizeof_var, sizeof(char));
		if(!binary)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
			oph_ioserver_free_result(server, old_result);
			return OPH_DC2_DATA_ERROR;
		}

		id_dim = (unsigned long long*)calloc(1, sizeof(unsigned long long));
		if(!id_dim)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
			free(binary);
			oph_ioserver_free_result(server, old_result);
			return OPH_DC2_DATA_ERROR;
		}

		query = NULL;
		args = (oph_ioserver_query_arg **)calloc(c_arg,sizeof(oph_ioserver_query_arg*)); 
		if(!(args))
		{
			free(binary);
			free(id_dim);
			oph_ioserver_free_result(server, old_result);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_DC2_DATA_ERROR;
		}  

		for(ii = 0; ii < c_arg -1; ii++)
		{
			args[ii] = (oph_ioserver_query_arg *)calloc(1, sizeof(oph_ioserver_query_arg));
			if(!args[ii])
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
				free(binary);
				free(id_dim);
				oph_ioserver_free_result(server, old_result);
				for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
				free(args);
				return OPH_DC2_DATA_ERROR;
			} 
		}
		args[c_arg -1] = NULL;


		args[0]->arg_length = sizeof(unsigned long long);
		args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[0]->arg_is_null = 0;
		args[0]->arg = (unsigned long long*)(id_dim);    

		args[1]->arg_length = sizeof_var;
		args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[1]->arg_is_null = 0;  
		args[1]->arg = (char*)(binary);


		if(oph_ioserver_setup_query(server, new_frag->db_instance->dbms_instance->conn, insert_query, tot_rows, args, &query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			oph_ioserver_free_result(server, old_result);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii])
				{
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				}
			} 
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}

		//If first execute
		if((exec_flag == 1 && tot_rows > 1) || remake)
		{
			*exec_args = args;
			*exec_query = query;
		}
	}

	oph_ioserver_row *curr_row = NULL;
	long long tmp_first_id = 0, tmp_last_id = 0;
	for(l=0; l < rows; l++)
	{
		//Read data	
		if(oph_ioserver_fetch_row(server, old_result, &curr_row))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
			oph_ioserver_free_result(server, old_result);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii]){
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC2_SERVER_ERROR;
		}

		actual_size = curr_row->field_lengths[1];
		*id_dim = (unsigned long long)strtoll(curr_row->row[0], NULL, 10);
		if (l == 0) tmp_first_id = *id_dim;
		tmp_last_id = *id_dim;

		memset(binary, 0, sizeof_var);
		memcpy(binary, curr_row->row[1], actual_size);

		if (oph_ioserver_execute_query(server, new_frag->db_instance->dbms_instance->conn, query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			oph_ioserver_free_result(server, old_result);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii])
				{
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			*exec_args = NULL;
			*exec_query = NULL;
			return OPH_DC2_SERVER_ERROR;
		}
	}

	oph_ioserver_free_result(server, old_result);
	if((exec_flag == 2) || (exec_flag == 3))
	{
		//If last or only one - clean 
		for(ii = 0; ii < c_arg -1; ii++)
		{     
			if(args[ii])
			{
				if(args[ii]->arg) free(args[ii]->arg);
				free(args[ii]); 
			} 
		} 
		free(args);
		oph_ioserver_free_query(server, query);
		*exec_args = NULL;
		*exec_query = NULL;
	}

	*first_id = tmp_first_id;
	*last_id = tmp_last_id;

	return OPH_DC2_SUCCESS;
}

int oph_dc2_copy_and_process_fragment(oph_ioserver_handler *server, unsigned long long tot_rows, oph_odb_fragment *old_frag1, oph_odb_fragment *old_frag2, const char* frag_name, int compressed, const char* operation, const char* measure_type)
{
	if(!old_frag1 || !old_frag2 || !frag_name || !server || !operation || !measure_type)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	oph_odb_fragment *new_frag = old_frag1;

	char read_query[QUERY_BUFLEN];
	unsigned long long sizeof_var = 0;	
	int n;

	// Init res 
	oph_ioserver_result *old_result1 = NULL;
	oph_ioserver_result *old_result2 = NULL;

	//Check if connection to input fragments are set
	if( oph_dc2_check_connection_to_db(server, old_frag1->db_instance->dbms_instance, old_frag1->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	//Read input fragment
  	n =  snprintf(read_query, QUERY_BUFLEN, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag1->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, old_frag1->db_instance->dbms_instance->conn, read_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, old_frag1->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}
	oph_ioserver_free_query(server, query);

	if(oph_ioserver_get_result(server, old_frag1->db_instance->dbms_instance->conn, &old_result1))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}
	if(old_result1->num_fields != 2)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}

	unsigned long long l, rows = old_result1->num_rows;
	sizeof_var = old_result1->max_field_length[old_result1->num_fields-1];
	//Get max row length of input table
	if(!sizeof_var)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragment is empty\n");
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}

	if( oph_dc2_check_connection_to_db(server, old_frag2->db_instance->dbms_instance, old_frag2->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}

	//Read input fragment
  	n =  snprintf(read_query, QUERY_BUFLEN, compressed ? OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG : OPH_DC_SQ_READ_RAW_FRAG, old_frag2->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}

	query = NULL;
	if (oph_ioserver_setup_query(server, old_frag2->db_instance->dbms_instance->conn, read_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		oph_ioserver_free_result(server, old_result1);
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, old_frag2->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_result(server, old_result1);
    		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}
	oph_ioserver_free_query(server, query);

	if(oph_ioserver_get_result(server, old_frag2->db_instance->dbms_instance->conn, &old_result2))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_SERVER_ERROR;
	}
	if(old_result2->num_fields != 2)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");
		oph_ioserver_free_result(server, old_result1);	
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_SERVER_ERROR;
	}

	if(sizeof_var != old_result2->max_field_length[old_result2->num_fields-1])
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable\n");
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_SERVER_ERROR;
	}

	char insert_query[QUERY_BUFLEN];
	unsigned long long actual_size = 0;
	int c_arg = 4, ii;

	//If first or only execution
	n = snprintf(insert_query, QUERY_BUFLEN, compressed ? OPH_DC_SQ_INSERT_COMPRESSED_FRAG2 : OPH_DC_SQ_INSERT_FRAG2, frag_name, operation, measure_type, measure_type, measure_type);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_SERVER_ERROR;
	}

	char *binary1 = (char*)calloc(sizeof_var, sizeof(char));
	if(!binary1)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_DATA_ERROR;
	}
	char *binary2 = (char*)calloc(sizeof_var, sizeof(char));
	if(!binary2)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		free(binary1);
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_DATA_ERROR;
	}

	unsigned long long *id_dim = (unsigned long long*)calloc(1, sizeof(unsigned long long));
	if(!id_dim)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate data buffers\n");
		free(binary1);
		free(binary2);
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		return OPH_DC2_DATA_ERROR;
	}

	query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **)calloc(c_arg,sizeof(oph_ioserver_query_arg*)); 
	if(!(args))
	{
		free(binary1);
		free(binary2);
		free(id_dim);
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_DC2_DATA_ERROR;
	}  

	for(ii = 0; ii < c_arg -1; ii++)
	{
		args[ii] = (oph_ioserver_query_arg *)calloc(1, sizeof(oph_ioserver_query_arg));
		if(!args[ii])
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(binary1);
			free(binary2);
			free(id_dim);
			oph_ioserver_free_result(server, old_result1);
			oph_ioserver_free_result(server, old_result2);
			for(ii = 0; ii < c_arg -1; ii++) if(args[ii]) free(args[ii]);
			free(args);
			return OPH_DC2_DATA_ERROR;
		} 
	}
	args[c_arg -1] = NULL;

	args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;
	args[0]->arg = (unsigned long long*)(id_dim);    

	args[1]->arg_length = sizeof_var;
	args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[1]->arg_is_null = 0;  
	args[1]->arg = (char*)(binary1);

	args[2]->arg_length = sizeof_var;
	args[2]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[2]->arg_is_null = 0;  
	args[2]->arg = (char*)(binary2);

	if(oph_ioserver_setup_query(server, new_frag->db_instance->dbms_instance->conn, insert_query, tot_rows, args, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		oph_ioserver_free_result(server, old_result1);
		oph_ioserver_free_result(server, old_result2);
		for(ii = 0; ii < c_arg -1; ii++)
		{     
			if(args[ii])
			{
				if(args[ii]->arg) free(args[ii]->arg);
				free(args[ii]); 
			}
		} 
		free(args);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row1 = NULL;
	oph_ioserver_row *curr_row2 = NULL;
	for(l=0; l < rows; l++)
	{
		//Read data
		if(oph_ioserver_fetch_row(server, old_result1, &curr_row1))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
			oph_ioserver_free_result(server, old_result1);
			oph_ioserver_free_result(server, old_result2);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii]){
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}
		if(oph_ioserver_fetch_row(server, old_result2, &curr_row2))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
			oph_ioserver_free_result(server, old_result1);
			oph_ioserver_free_result(server, old_result2);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii]){
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}

		*id_dim = (unsigned long long)strtoll(curr_row1->row[0], NULL, 10);

		actual_size = curr_row1->field_lengths[1];
		memset(binary1, 0, sizeof_var);
		memcpy(binary1, curr_row1->row[1], actual_size);

		actual_size = curr_row2->field_lengths[1];
		memset(binary2, 0, sizeof_var);
		memcpy(binary2, curr_row2->row[1], actual_size);

		if (oph_ioserver_execute_query(server, new_frag->db_instance->dbms_instance->conn, query))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			oph_ioserver_free_result(server, old_result1);
			oph_ioserver_free_result(server, old_result2);
			oph_ioserver_free_query(server, query);
			for(ii = 0; ii < c_arg -1; ii++)
			{     
				if(args[ii])
				{
					if(args[ii]->arg) free(args[ii]->arg);
					free(args[ii]); 
				} 
			} 
			free(args);
			return OPH_DC2_SERVER_ERROR;
		}
	}

	oph_ioserver_free_result(server, old_result1);
	oph_ioserver_free_result(server, old_result2);

	//If last or only one - clean 
	for(ii = 0; ii < c_arg -1; ii++)
	{     
		if(args[ii])
		{
			if(args[ii]->arg) free(args[ii]->arg);
			free(args[ii]); 
		} 
	} 
	free(args);
	oph_ioserver_free_query(server, query);

	return OPH_DC2_SUCCESS;
}

int oph_dc2_read_fragment_data(oph_ioserver_handler *server, oph_odb_fragment *frag, char *data_type, int compressed, char* id_clause, char* array_clause, char* where_clause, int limit, int raw_format, oph_ioserver_result **frag_rows)
{
	if(!frag || !frag_rows|| !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
	(*frag_rows) = NULL;
	//Check if connection to input and output fragment are set

	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance,frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char type_flag = '\0';
	int n;

	char read_query[QUERY_BUFLEN];
	char *array_part = NULL;

	if( !raw_format )
	{
		//Select right data type
		type_flag = oph_dc2_typeof(data_type);
		if (!type_flag)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
			return OPH_DC2_DATA_ERROR;
		}

#ifdef OPH_DEBUG_MYSQL
    char where_part_sql[QUERY_BUFLEN];
		char limit_part_sql[QUERY_BUFLEN];
#endif

		//Set up where part of the query
		char *where_part = NULL;
		if(!(where_part = (char *) calloc ((where_clause ? strlen(where_clause)+strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "")): 0)+1, sizeof (char)))){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_DC2_DATA_ERROR;
		}
		if(where_clause){
#ifdef OPH_DEBUG_MYSQL
			n =  snprintf(where_part_sql, strlen(where_clause)+1 + 7, " WHERE %s", where_clause);
#endif
			n =  snprintf(where_part, strlen(where_clause)+1 + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "")), OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s"), where_clause);
    }
		else{
#ifdef OPH_DEBUG_MYSQL
			where_part_sql[0] = 0;
#endif
			where_part[0] = 0;
			n =  0;	
		}
		//Set up limit part of the query
		char *limit_part = NULL;
		if(!(limit_part = (char *) calloc ((limit ? (floor(log10(abs(limit))) + 1)+strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "")): 0)+2, sizeof (char)))){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(where_part);
			return OPH_DC2_DATA_ERROR;
		}
		if(limit){
#ifdef OPH_DEBUG_MYSQL
			n =  snprintf(limit_part_sql, (floor(log10(abs(limit))) + 1)+1 + 8, " LIMIT %d;", limit);
#endif
			n =  snprintf(limit_part, (floor(log10(abs(limit))) + 1)+1 + strlen(OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "")), OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_LIMIT, "%d"), limit);
    }
		else{
#ifdef OPH_DEBUG_MYSQL
			n =  snprintf(limit_part_sql, 1+1, ";");
#endif
			limit_part[0] = '\0';	
    }

		//Set up array part of the query
		if(!(array_part = (char *) calloc (array_clause ? strlen(array_clause)+OPH_DC2_MAX_SIZE : OPH_DC2_MIN_SIZE, sizeof (char)))){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(limit_part);
			free(where_part);
			return OPH_DC2_DATA_ERROR;
		}

		if (type_flag == OPH_DC2_BIT_FLAG)
		{
			if(array_clause)
				n =  snprintf(array_part, strlen(array_clause)+OPH_DC2_MAX_SIZE, "(oph_bit_subarray2('','',%s,'%s'))", (compressed ? "oph_uncompress('','',measure)": "measure"), array_clause);
			else
				n =  snprintf(array_part, OPH_DC2_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)": "measure"));	
			if (id_clause){
#ifdef OPH_DEBUG_MYSQL
        printf("ORIGINAL QUERY: "MYSQL_DC_READ_FRAG_SPECIAL3"\n", MYSQL_FRAG_ID, id_clause, array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
        n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_FRAG_SPECIAL3, MYSQL_FRAG_ID, id_clause, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
      }
			else{
#ifdef OPH_DEBUG_MYSQL
       printf("ORIGINAL QUERY: "MYSQL_DC_READ_FRAG_SPECIAL4"\n", MYSQL_FRAG_ID, array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
       n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_FRAG_SPECIAL4, MYSQL_FRAG_ID, array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
      }
		}
		else
		{
			if(array_clause)
			{
				char ttype[QUERY_BUFLEN];
				snprintf(ttype,QUERY_BUFLEN,"%s",oph_dc2_stringof(type_flag));			
				n =  snprintf(array_part, strlen(array_clause)+OPH_DC2_MAX_SIZE, "(oph_get_subarray2('OPH_%s','OPH_%s',%s,'%s'))", ttype, ttype, (compressed ? "oph_uncompress('','',measure)": "measure"), array_clause);
			}
			else n =  snprintf(array_part, OPH_DC2_MIN_SIZE, "%s", (compressed ? "oph_uncompress('','',measure)": "measure"));	
			if (id_clause)
			{
#ifdef OPH_DEBUG_MYSQL
        printf("ORIGINAL QUERY: "MYSQL_DC_READ_FRAG_SPECIAL1"\n", MYSQL_FRAG_ID, id_clause, oph_dc2_stringof(type_flag), array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
        n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_FRAG_SPECIAL1, MYSQL_FRAG_ID, id_clause, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			}
			else
			{
#ifdef OPH_DEBUG_MYSQL
        printf( "ORIGINAL QUERY: "MYSQL_DC_READ_FRAG_SPECIAL2"\n", MYSQL_FRAG_ID, oph_dc2_stringof(type_flag), array_part, frag->fragment_name, where_part_sql, MYSQL_FRAG_ID, limit_part_sql);
#endif
        n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_FRAG_SPECIAL2, MYSQL_FRAG_ID, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), array_part, frag->fragment_name, where_part, MYSQL_FRAG_ID, limit_part);
			}
		}
		free(array_part);
		free(limit_part);
		free(where_part);
	}
	else if (!array_clause) // Real raw
	{
		if (data_type && !strcasecmp(data_type,  OPH_DC2_BIT_TYPE)) {
			#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG2"\n", compressed ? "oph_bit_export('','OPH_INT',oph_uncompress('','',measure))" : "oph_bit_export('','OPH_INT',measure)", frag->fragment_name);
			#endif
			n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG2"\n", compressed ? "oph_bit_export('','OPH_INT',oph_uncompress('','',measure))" : "oph_bit_export('','OPH_INT',measure)", frag->fragment_name);
		}
		else {
			if(compressed){
				#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_COMPRESSED_FRAG"\n", frag->fragment_name);
				#endif
				n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_COMPRESSED_FRAG, frag->fragment_name);
			}
			else{
				#ifdef OPH_DEBUG_MYSQL
				printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG"\n", frag->fragment_name);
				#endif
				n =  snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG, frag->fragment_name);
			}
		}	
	}
	else // Raw adapted to inspect cube
	{
		if (limit)
		{
			if (where_clause){
        if(id_clause){
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_WLI "\n", id_clause, array_clause, frag->fragment_name, where_clause, limit);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_WLI, id_clause, array_clause, frag->fragment_name, where_clause, limit);
         }
        else{
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_WL "\n", array_clause, frag->fragment_name, where_clause, limit);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_WL, array_clause, frag->fragment_name, where_clause, limit);
        }
			}
			else{
        if(id_clause){
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_LI "\n", id_clause, array_clause, frag->fragment_name, limit);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_LI, id_clause, array_clause, frag->fragment_name, limit);
         }
        else{
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_L "\n", array_clause, frag->fragment_name, limit);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_L, array_clause, frag->fragment_name, limit);
        }
      }
		}
		else
		{
			if (where_clause){
        if(id_clause){
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_WI"\n", id_clause, array_clause, frag->fragment_name, where_clause);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_WI, id_clause, array_clause, frag->fragment_name, where_clause);
        }
        else{
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_W"\n", array_clause, frag->fragment_name, where_clause);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_W, array_clause, frag->fragment_name, where_clause);
        }
			}
      else{
        if(id_clause){
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3_I"\n", id_clause, array_clause, frag->fragment_name);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3_I, id_clause, array_clause, frag->fragment_name);
        }
        else{
#ifdef OPH_DEBUG_MYSQL
          printf("ORIGINAL QUERY: "MYSQL_DC_READ_RAW_FRAG3"\n", array_clause, frag->fragment_name);
#endif
          n = snprintf(read_query, QUERY_BUFLEN, OPH_DC_SQ_READ_RAW_FRAG3, array_clause, frag->fragment_name);
        }
      }
		}
	}

	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, read_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query '%s'\n",read_query);
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n",read_query);
		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	if(oph_ioserver_get_result(server, frag->db_instance->dbms_instance->conn, frag_rows)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, *frag_rows);
		return OPH_DC2_SERVER_ERROR;
	}

	return OPH_DC2_SUCCESS;
}

int oph_dc2_get_total_number_of_elements_in_fragment(oph_ioserver_handler *server, oph_odb_fragment *frag, char *data_type, int compressed, long long *count)
{
	if(!frag || !data_type || !count|| !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance,frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	int n;
	char type_flag = oph_dc2_typeof(data_type);
	if (!type_flag)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC2_DATA_ERROR;
	}

	char select_query[QUERY_BUFLEN];

	if (type_flag == OPH_DC2_BIT_FLAG)
	{
		if(compressed == 1){
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG"\n", frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG, frag->fragment_name);
		}
    else{
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_BIT_ELEMENTS_FRAG"\n", frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG, frag->fragment_name);
    }
	}
	else
	{
		if(compressed == 1){
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_COMPRESSED_ELEMENTS_FRAG"\n", oph_dc2_stringof(type_flag), frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), frag->fragment_name);
    }
		else{
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_ELEMENTS_FRAG"\n", oph_dc2_stringof(type_flag), frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_ELEMENTS_FRAG, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), frag->fragment_name);
    }
	}
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, select_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if(oph_ioserver_get_result(server, frag->db_instance->dbms_instance->conn,  &result)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if(result->num_rows != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if(result->num_fields != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if(oph_ioserver_fetch_row(server, result, &curr_row)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if (curr_row->row[0]) *count = strtoll(curr_row->row[0], NULL, 10);
	else *count=0;

	oph_ioserver_free_result(server, result);
	return OPH_DC2_SUCCESS;
}

int oph_dc2_get_total_number_of_rows_in_fragment(oph_ioserver_handler *server, oph_odb_fragment *frag, char *data_type, long long *count)
{
	if(!frag || !data_type || !count|| !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance,frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	int n;

	char select_query[QUERY_BUFLEN];
	n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_ROWS_FRAG, frag->fragment_name);
	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, select_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);

	// Init res 
	oph_ioserver_result *result = NULL;

	if(oph_ioserver_get_result(server, frag->db_instance->dbms_instance->conn,  &result)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if(result->num_rows != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if(result->num_fields != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}

	oph_ioserver_row *curr_row = NULL;
	if(oph_ioserver_fetch_row(server, result, &curr_row)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
	if (curr_row->row[0]) *count = strtoll(curr_row->row[0], NULL, 10);
	else *count=0;

	oph_ioserver_free_result(server, result);
	return OPH_DC2_SUCCESS;
}

int oph_dc2_get_number_of_elements_in_fragment_row(oph_ioserver_handler *server, oph_odb_fragment *frag, char *data_type, int compressed, long long *length)
{
	if(!frag || !data_type || !length || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance,frag->db_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	int n;
	char type_flag = oph_dc2_typeof(data_type);
	if (!type_flag)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DC2_DATA_ERROR;
	}

	char select_query[QUERY_BUFLEN];

	if (type_flag == OPH_DC2_BIT_FLAG)
	{
		if(compressed == 1){
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG_ROW"\n", frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_COMPRESSED_BIT_ELEMENTS_FRAG_ROW, frag->fragment_name);
		}
    else{
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_BIT_ELEMENTS_FRAG_ROW"\n", frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_BIT_ELEMENTS_FRAG_ROW, frag->fragment_name);
     }
	}
	else
	{
		if(compressed == 1){
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_COMPRESSED_ELEMENTS_FRAG_ROW"\n", oph_dc2_stringof(type_flag), frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_COMPRESSED_ELEMENTS_FRAG_ROW, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), frag->fragment_name);
		}
    else{
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_COUNT_ELEMENTS_FRAG_ROW"\n", oph_dc2_stringof(type_flag), frag->fragment_name);
#endif
			n =  snprintf(select_query, QUERY_BUFLEN, OPH_DC_SQ_COUNT_ELEMENTS_FRAG_ROW, oph_dc2_stringof(type_flag), oph_dc2_stringof(type_flag), frag->fragment_name);
    }
	}

	if(n >= QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, select_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

  // Init res 
  oph_ioserver_result *result = NULL;

  if(oph_ioserver_get_result(server, frag->db_instance->dbms_instance->conn,  &result)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
    oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
  }
  if(result->num_rows != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
  if(result->num_fields != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_row *curr_row = NULL;
	if(oph_ioserver_fetch_row(server, result, &curr_row)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
  }
	if (curr_row->row[0]) *length = strtoll(curr_row->row[0], NULL, 10);
	else *length=0;

	oph_ioserver_free_result(server, result);
	return OPH_DC2_SUCCESS;
}

int oph_dc2_get_fragments_size_in_bytes(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms, char *frag_name, long long *size)
{
	if(!dbms || !frag_name || !size || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, dbms, NULL, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}


	char select_query[10*QUERY_BUFLEN];	
#ifdef OPH_DEBUG_MYSQL
      printf("ORIGINAL QUERY: "MYSQL_DC_SIZE_ELEMENTS_FRAG"\n", frag_name);
#endif
	int n =  snprintf(select_query, 10*QUERY_BUFLEN, OPH_DC_SQ_SIZE_ELEMENTS_FRAG, frag_name);
	if(n >= 10*QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, dbms->conn, select_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, dbms->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation '%s'\n",select_query);
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

	// Init res 
  oph_ioserver_result *result = NULL;

  if(oph_ioserver_get_result(server, dbms->conn,  &result)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
    oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
  }
  if(result->num_rows != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}
  if(result->num_fields != 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_row *curr_row = NULL;
	if(oph_ioserver_fetch_row(server, result, &curr_row)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");		
		oph_ioserver_free_result(server, result);
		return OPH_DC2_SERVER_ERROR;
  }
	if (curr_row->row[0]) *size = strtoll(curr_row->row[0], NULL, 10);
	else *size = 0;	

	oph_ioserver_free_result(server, result);
	return OPH_DC2_SUCCESS;
}

int oph_dc2_get_primitives(oph_ioserver_handler *server, oph_odb_dbms_instance *dbms, char *frag_name, oph_ioserver_result **frag_rows)
{
	if(!dbms || !frag_rows || !server){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	if( oph_dc2_check_connection_to_db(server, dbms, NULL, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	char select_query[10*QUERY_BUFLEN];
  int n = 0;
  if(!frag_name){
#ifdef OPH_DEBUG_MYSQL
    printf("ORIGINAL QUERY: "MYSQL_QUERY_RETRIEVE_PRIMITIVES_LIST"\n");
#endif
    n = snprintf(select_query, 10*QUERY_BUFLEN, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST);    
  }
  else {
#ifdef OPH_DEBUG_MYSQL
    printf("ORIGINAL QUERY: "MYSQL_QUERY_RETRIEVE_PRIMITIVES_LIST_W"\n", frag_name);
#endif
     n = snprintf(select_query, 10*QUERY_BUFLEN, OPH_DC_SQ_RETRIEVE_PRIMITIVES_LIST_W, frag_name);
  }	
	if(n >= 10*QUERY_BUFLEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
	    return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, dbms->conn, select_query, 1, NULL, &query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		return OPH_DC2_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, dbms->conn, query))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
    oph_ioserver_free_query(server, query);
		return OPH_DC2_SERVER_ERROR;
	}

  oph_ioserver_free_query(server, query);

	// Init res 
  if(oph_ioserver_get_result(server, dbms->conn, frag_rows)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to store result.\n");
    oph_ioserver_free_result(server, *frag_rows);
		return OPH_DC2_SERVER_ERROR;
  }

	return OPH_DC2_SUCCESS;
}

int oph_dc2_generate_fragment_name(char *db_name, int id_datacube, int proc_rank, int frag_number, char (*frag_name)[OPH_ODB_STGE_FRAG_NAME_SIZE]){
	if(!frag_name)
	{	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}

	int n = 0;
	if(db_name)
		n = snprintf(*frag_name, OPH_ODB_STGE_FRAG_NAME_SIZE, OPH_DC2_DB_FRAG_NAME, db_name, id_datacube, proc_rank, frag_number);
	else
		n = snprintf(*frag_name, OPH_ODB_STGE_FRAG_NAME_SIZE, OPH_DC2_FRAG_NAME, id_datacube, proc_rank, frag_number);

	if(n >= OPH_ODB_STGE_FRAG_NAME_SIZE)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
		return OPH_DC2_DATA_ERROR;
	}
	else
		return OPH_DC2_SUCCESS;
}

int oph_dc2_generate_db_name(char *odb_name, int id_datacube, int id_dbms, int proc_rank, int db_number, char (*db_name)[OPH_ODB_STGE_DB_NAME_SIZE]){
	if(!db_name || !odb_name)
	{	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
	
	int n = 0;
	n = snprintf(*db_name, OPH_ODB_STGE_DB_NAME_SIZE, OPH_DC2_DB_NAME, odb_name, id_datacube, id_dbms,  proc_rank, db_number);
	if(n >= OPH_ODB_STGE_FRAG_NAME_SIZE)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
		return OPH_DC2_DATA_ERROR;
	}
	else
		return OPH_DC2_SUCCESS;
}

int oph_dc2_check_data_type(char *input_type)
{
	if(!input_type)
	{	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");		
		return OPH_DC2_NULL_PARAM;
	}
	
	if(!strncmp(input_type, OPH_DC2_DOUBLE_TYPE, sizeof(OPH_DC2_DOUBLE_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_FLOAT_TYPE, sizeof(OPH_DC2_FLOAT_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_BYTE_TYPE, sizeof(OPH_DC2_INT_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_SHORT_TYPE, sizeof(OPH_DC2_LONG_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_INT_TYPE, sizeof(OPH_DC2_INT_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_LONG_TYPE, sizeof(OPH_DC2_LONG_TYPE))) return OPH_DC2_SUCCESS;
	if(!strncmp(input_type, OPH_DC2_BIT_TYPE, sizeof(OPH_DC2_BIT_TYPE))) return OPH_DC2_SUCCESS;
	
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
	return OPH_DC2_DATA_ERROR;
}

