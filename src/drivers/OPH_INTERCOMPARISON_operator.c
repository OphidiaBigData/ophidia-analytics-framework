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

#include "drivers/OPH_INTERCOMPARISON_operator.h"
#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube2_library.h"

int env_set (HASHTBL *task_tbl, oph_operator_struct *handle)
{
  if (!handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  if (!task_tbl){
	  pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
      return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
  }

  if (handle->operator_handle){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
    return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
  }

  if (!(handle->operator_handle = (OPH_INTERCOMPARISON_operator_handle *) calloc (1, sizeof (OPH_INTERCOMPARISON_operator_handle)))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_HANDLE );
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  //1 - Set up struct to empty values
  unsigned int i;
  for (i=0; i<2; ++i) ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[i] = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_job = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids = NULL;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type = NULL;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys = NULL;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys_num = -1;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server = NULL;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid = NULL;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_user = 0;
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description = NULL;

  char *datacube_in[2];
  char *value;

  // retrieve objkeys
  value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(oph_tp_parse_multiple_value_param(value, &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys, &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys_num)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
    oph_tp_free_multiple_value_param_list(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys_num);
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  // retrieve sessionid
  value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  //3 - Fill struct with the correct data
  value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
  datacube_in[0] = value;
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT );
	
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT_2);
  datacube_in[1] = value;
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT_2);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT_2 );
	
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  //For error checking
  int id_datacube_in[4] = { 0, 0, 0, 0 };

  value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  char *username = value;

  if(handle->proc_rank == 0)
  {
  		//Only master process has to initialize and open connection to management OphidiaDB
	  ophidiadb *oDB = &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->oDB;
	  oph_odb_init_ophidiadb(oDB);	

	  if(oph_odb_read_ophidiadb_config_file(oDB)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_OPHIDIADB_CONFIGURATION_FILE );
		
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	  }

	  if( oph_odb_connect_to_ophidiadb(oDB)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_INTERCOMPARISON_OPHIDIADB_CONNECTION_ERROR );
	
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	  }

	//Check if datacube exists (by ID container and datacube)
	  int exists = 0;
	  int status = 0;
	  char *uri = NULL;
	  char *uri2 = NULL;
	  int folder_id = 0;
	  int permission = 0;
	  int container2 = 0;
	  if(oph_pid_parse_pid(datacube_in[0], &id_datacube_in[2], &id_datacube_in[0], &uri)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_PID_ERROR, datacube_in[0] );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
	  else if(oph_pid_parse_pid(datacube_in[1], &container2, &id_datacube_in[1], &uri2)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_PID_ERROR, datacube_in[1] );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
	  else if((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[2], id_datacube_in[0], &exists)) || !exists){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_NO_INPUT_DATACUBE, datacube_in[0] );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
	  else if((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri2, container2, id_datacube_in[1], &exists)) || !exists){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_NO_INPUT_DATACUBE, datacube_in[0] );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
	  else
	  {
		  if((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, datacube_in[0]);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
			id_datacube_in[2] = 0;
		  }
		  else if((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[1], 0, &status)) || !status){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_EXPORTNC_DATACUBE_AVAILABILITY_ERROR, datacube_in[1]);
			id_datacube_in[0] = 0;
			id_datacube_in[1] = 0;
			id_datacube_in[2] = 0;
		  }
	  }
	if(uri) free(uri);
	uri = NULL;
	if(uri2) free(uri2);
	uri2 = NULL;
	if(id_datacube_in[0])
	{
	  if((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[2], 1, &folder_id))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_FOLDER_ERROR, datacube_in[0] );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
	  else if((oph_odb_fs_check_folder_session(folder_id, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid, oDB, &permission)) || !permission){
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_PERMISSION_ERROR, username );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
		id_datacube_in[2] = 0;
	  }
  }
	  if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_user)))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_USER_ID_ERROR );
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	  }

	id_datacube_in[3] = id_datacube_in[2];
	if (id_datacube_in[2])
	{
		value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
		if(!value){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT );
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value,OPH_COMMON_DEFAULT_EMPTY_VALUE,OPH_TP_TASKLEN))
		{
			if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, 0, &id_datacube_in[3]))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container or it is hidden\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value );
				id_datacube_in[0] = 0;
				id_datacube_in[1] = 0;
				id_datacube_in[2] = 0;
			}
		}
	}

  }

  //Broadcast to all other processes the fragment relative index 	
  MPI_Bcast(id_datacube_in,4,MPI_INT,0,MPI_COMM_WORLD);

  //Check if sequential part has been completed
  for (i=0; i<2; ++i)
  {
	  if (id_datacube_in[i] == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");		
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_NO_INPUT_DATACUBE, datacube_in[i] );
		
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	  }
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[i] = id_datacube_in[i];
  }

  if (id_datacube_in[2] == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");		
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_NO_INPUT_CONTAINER, datacube_in[0] );
		
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container = id_datacube_in[2];
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container = id_datacube_in[3];

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
    logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM );
	
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->schedule_algo = (int)strtol(value, NULL, 10);

  value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
  if(!value)
	((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_job = 0;	
  else
	((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_job = (int)strtol(value, NULL, 10);

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
	logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if (strncmp(value,OPH_COMMON_DEFAULT_EMPTY_VALUE,OPH_TP_TASKLEN)){
	if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description = (char *) strndup (value, OPH_TP_TASKLEN))){
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[2], OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "description" );
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init (oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NULL_OPERATOR_HANDLE );	
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  int pointer, stream_max_size=4+OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE+2*sizeof(int)+OPH_ODB_CUBE_MEASURE_TYPE_SIZE;
  char stream[stream_max_size];
  memset(stream, 0, sizeof(stream));
  *stream = 0;
  char *id_string[3], *data_type;
  pointer=0; id_string[0]=stream+pointer;
  pointer+=1+OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE; id_string[1]=stream+pointer;
  pointer+=1+sizeof(int); id_string[2]=stream+pointer;
  pointer+=1+sizeof(int); data_type=stream+pointer;

  if(handle->proc_rank == 0){
	  ophidiadb *oDB = &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->oDB;
	  oph_odb_datacube cube, cube2;
	  oph_odb_cube_init_datacube(&cube);

	  int datacube_id1 = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[0];
	  int datacube_id2 = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[1];

	  //retrieve input datacube
	  if(oph_odb_cube_retrieve_datacube(oDB, datacube_id1, &cube)){
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube1\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_READ_ERROR, "first" );		
		goto __OPH_EXIT_1;
	  }
	  if(oph_odb_cube_retrieve_datacube(oDB, datacube_id2, &cube2)){
		oph_odb_cube_free_datacube(&cube2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube2\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_READ_ERROR, "second" );	
		goto __OPH_EXIT_1;
	  }

	// Checking fragmentation structure
	if ((cube.hostxdatacube!=cube2.hostxdatacube) || (cube.dbmsxhost!=cube2.dbmsxhost) || (cube.dbxdbms!=cube2.dbxdbms) || (cube.fragmentxdb!=cube2.fragmentxdb) || (cube.tuplexfragment!=cube2.tuplexfragment))
	{
		oph_odb_cube_free_datacube(&cube);
		oph_odb_cube_free_datacube(&cube2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube fragmentation structures are not comparable\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "fragmentation structures" );	
		goto __OPH_EXIT_1;
	}
	// Checking fragment indexes
	if (strcmp(cube.frag_relative_index_set,cube2.frag_relative_index_set))
	{
		oph_odb_cube_free_datacube(&cube);
		oph_odb_cube_free_datacube(&cube2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube relative index sets are not comparable\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "relative index sets" );	
		goto __OPH_EXIT_1;
	}
	// Checking data types
	if (strcmp(cube.measure_type,cube2.measure_type))
	{
		oph_odb_cube_free_datacube(&cube);
		oph_odb_cube_free_datacube(&cube2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube types are not comparable\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "measure types" );	
		goto __OPH_EXIT_1;
	}
	if (cube.compressed != cube2.compressed)
	{
		oph_odb_cube_free_datacube(&cube);
		oph_odb_cube_free_datacube(&cube2);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube are compressed differently\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "compression methods" );	
		goto __OPH_EXIT_1;
	}
	  oph_odb_cube_free_datacube(&cube2);


	  // Change the container id
	  cube.id_container = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container;

	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed = cube.compressed;

	  //Copy fragment id relative index set	
	  if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE)))
	  {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "fragment ids" );		
		goto __OPH_EXIT_1;
	  }

	//Copy measure_type relative index set
	if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type = (char *) strndup (cube.measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE)))
	{
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "measure type" );		
		goto __OPH_EXIT_1;
	}

  	  oph_odb_cubehasdim *cubedims = NULL, *cubedims2 = NULL;
	  int number_of_dimensions = 0, number_of_dimensions2 = 0;
	  int last_insertd_id = 0;
	  int l;

	  //Read old cube - dimension relation rows
	  if(oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id1, &cubedims, &number_of_dimensions)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube1 - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_CUBEHASDIM_READ_ERROR, "first" );		
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		goto __OPH_EXIT_1;
	  }
	  if(oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id2, &cubedims2, &number_of_dimensions2)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube2 - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_CUBEHASDIM_READ_ERROR, "second" );	
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		free(cubedims2);
		goto __OPH_EXIT_1;
	  }

	  // Dimension comparison
	  if (number_of_dimensions != number_of_dimensions2)
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube2 - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "number of dimensions" );
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		free(cubedims2);
		goto __OPH_EXIT_1;
	  }
	  for (l=0; l<number_of_dimensions; l++)
		if ((cubedims[l].size!=cubedims2[l].size) ||
			(cubedims[l].explicit_dim!=cubedims2[l].explicit_dim) ||
			(cubedims[l].level!=cubedims2[l].level)) break;
	  if (l<number_of_dimensions)
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube dimensions are not comparable.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "dimensions" );
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		free(cubedims2);
		goto __OPH_EXIT_1;
	  }
	  free(cubedims2);

	  //New fields
	  cube.id_source = 0;
	  cube.level++;
	  if (((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description) snprintf(cube.description,OPH_ODB_CUBE_DESCRIPTION_SIZE,"%s",((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description);
	  else *cube.description = 0;

	  //Insert new datacube
	  if(oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube))){
		oph_odb_cube_free_datacube(&cube);
		free(cubedims);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_INSERT_ERROR );		
		goto __OPH_EXIT_1;	 
	  }
	  oph_odb_cube_free_datacube(&cube);

	// Copy the dimension in case of output has to be saved in a new container
	if (((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container != ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container)
	{
		oph_odb_dimension dim[number_of_dimensions];
		oph_odb_dimension_instance dim_inst[number_of_dimensions];
		for (l=0;l<number_of_dimensions;++l)
		{
			if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id1))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);    				
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id1))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);    				
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}

		oph_odb_db_instance db_;
		oph_odb_db_instance *db = &db_;
		if (oph_dim_load_dim_dbinstance(db))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_LOAD_ERROR );
			oph_dim_unload_dim_dbinstance(db);	
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db->dbms_instance, 0))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_CONNECT_ERROR );
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1; 
		}
		if (oph_dim_use_db_of_dbms(db->dbms_instance, db))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_USE_DB_ERROR );
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			goto __OPH_EXIT_1; 
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_NAME_MACRO,((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container);
		snprintf(label_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_LABEL_MACRO,((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container);
		char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(o_index_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_NAME_MACRO,((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container);
		snprintf(o_label_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_LABEL_MACRO,((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container);

		char* dim_row, *cl_value;
		int compressed = 0; 
		for (l=0; l<number_of_dimensions; ++l)
		{
			if (!dim_inst[l].fk_id_dimension_index)
			{
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
				break;
			}

			dim_row=0;
			if (dim_inst[l].size)
			{
				// Load input labels
				dim_row = NULL;
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, MYSQL_DIMENSION, compressed, &dim_row) || !dim_row)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (dim_inst[l].fk_id_dimension_label)
				{
					if (oph_dim_read_dimension_filtered_data(db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, MYSQL_DIMENSION, compressed, &dim_row, dim[l].dimension_type, dim_inst[l].size) || !dim_row)
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);    				
						if (dim_row) free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						goto __OPH_EXIT_1;
					}
				}
				else strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE); // A reduced dimension is handled by indexes
				// Store output labels
				if (oph_dim_insert_into_dimension_table(db, o_label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_label)))
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (dim_row) free(dim_row);

				// Set indexes
				snprintf(operation, OPH_COMMON_BUFFER_LEN, MYSQL_DIM_INDEX_ARRAY, OPH_DIM_INDEX_DATA_TYPE, 1, dim_inst[l].size);
				dim_row = NULL;
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, compressed, &dim_row) || !dim_row)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_READ_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				// Store output indexes
				if (oph_dim_insert_into_dimension_table(db, o_index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index)))
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_ROW_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
				if (dim_row) free(dim_row);
			}
			else dim_inst[l].fk_id_dimension_index = dim_inst[l].fk_id_dimension_label = 0;

			dim_inst[l].id_grid = 0; 
			cl_value = NULL;
			if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube, dim[l].dimension_name, cl_value))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_DIM_INSTANCE_STORE_ERROR);				
				if (cl_value) free(cl_value);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (cl_value) free(cl_value);
		}
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
	}

	  //Write new cube - dimension relation rows
	  for(l = 0; l < number_of_dimensions ; l++){
		//Change iddatacube in cubehasdim
		cubedims[l].id_datacube = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube;

		if(oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_CUBEHASDIM_INSERT_ERROR );
			free(cubedims);
			goto __OPH_EXIT_1;
		}
	  }
	  free(cubedims);

	  if (oph_odb_meta_copy_from_cube_to_cube(oDB, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[0], ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_user))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR );
		goto __OPH_EXIT_1;
	  }

	  last_insertd_id = 0;
	  //Insert new task REMEBER TO MEMSET query to 0 IF NOT NEEDED
	  oph_odb_task new_task;
	  new_task.id_outputcube = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube;
  	  memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
	  new_task.id_job = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_job;
	  strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
	  if (((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed) snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_INTERCOMPARISON_QUERY_COMPR, MYSQL_FRAG_ID, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_ID);
	  else snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_INTERCOMPARISON_QUERY, MYSQL_FRAG_ID, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_ID);
	  new_task.input_cube_number = 2;
	  if(!(new_task.id_inputcube = (int*)malloc(new_task.input_cube_number*sizeof(int))))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_STRUCT, "task" );
		goto __OPH_EXIT_1;
	  }	  
	  new_task.id_inputcube[0] = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[0];
	  new_task.id_inputcube[1] = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[1];

	  if(oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
	    logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_TASK_INSERT_ERROR, new_task.operator);
		free(new_task.id_inputcube);
		goto __OPH_EXIT_1;
	  } 
	  free(new_task.id_inputcube);
	  
	  strncpy(id_string[0], ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
	  memcpy(id_string[1], &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube, sizeof(int));
	  memcpy(id_string[2], &((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed, sizeof(int));

	  strncpy(data_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
  }
__OPH_EXIT_1:
  //Broadcast to all other processes the fragment relative index 	
  MPI_Bcast(stream,stream_max_size,MPI_CHAR,0,MPI_COMM_WORLD);
  if (*stream == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");		
	    logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  if(handle->proc_rank != 0){
	  if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	  }
	  if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type = (char *) strndup (data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "measure type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	  }
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube = *((int*)id_string[1]);
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed = *((int*)id_string[2]);
  }
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NULL_OPERATOR_HANDLE);
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  int id_number;
  char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

  //Get total number of fragment IDs
  if(oph_ids_count_number_of_ids(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids, &id_number)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_RETREIVE_IDS_ERROR); 
    return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  //All processes compute the fragment number to work on
  int div_result = (id_number)/(handle->proc_number);
  int div_remainder = (id_number)%(handle->proc_number);

  //Every process must process at least divResult
  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_number = div_result;	

  if(div_remainder != 0)
  {
	//Only some certain processes must process an additional part
	if (handle->proc_rank/div_remainder == 0)
		((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_number++;
  }

  int i;
  //Compute fragment IDs starting position
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_number == 0){
	// In case number of process is higher than fragment number
    ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position = -1;	
  }
  else{
	((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position = 0;	
	for(i = handle->proc_rank-1; i >= 0; i--)
	{
		if(div_remainder != 0)
			((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position += (div_result + (i/div_remainder == 0 ? 1 : 0) );
		else
			((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position += div_result;
	}
	if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position >= id_number) ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position = -1;	
  }

  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank!= 0)
	return OPH_ANALYTICS_OPERATOR_SUCCESS;

  //Partition fragment relative index string
  char *new_ptr = new_id_string;
  if(oph_ids_get_substring_from_string(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_number, &new_ptr)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");			
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_ID_STRING_SPLIT_ERROR); 
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  free(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids);
  if(!(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_MEMORY_ERROR_INPUT, "fragment ids");    
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NULL_OPERATOR_HANDLE);    
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank!= 0)
	return OPH_ANALYTICS_OPERATOR_SUCCESS;

  int i, j, k;

  int id_datacube_out = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube; 
  int id_datacube_in1 = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[0];
  int id_datacube_in2 = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_datacube[1];
  int compressed = ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->compressed;

  oph_odb_fragment_list frags,frags2;
  oph_odb_db_instance_list dbs,dbs2;
  oph_odb_dbms_instance_list dbmss,dbmss2;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);	

  if(oph_odb_read_ophidiadb_config_file(&oDB_slave)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_OPHIDIADB_CONFIGURATION_FILE );
	
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  if( oph_odb_connect_to_ophidiadb(&oDB_slave)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_OPHIDIADB_CONNECTION_ERROR );
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
  }

  //retrieve connection string
  if(oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in1, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_CONNECTION_STRINGS_NOT_FOUND, "first");    
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }
  if(oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in2, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids, &frags2, &dbs2, &dbmss2)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_CONNECTION_STRINGS_NOT_FOUND, "second");    
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  if ((dbmss.size!=dbmss2.size) || (dbs.size!=dbs2.size) || (frags.size!=frags2.size))
  {
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Datacube structures are not comparable\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DATACUBE_COMPARISON_ERROR, "structures" );
	oph_odb_stge_free_fragment_list(&frags);
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_stge_free_fragment_list(&frags2);
	oph_odb_stge_free_db_list(&dbs2);
	oph_odb_stge_free_dbms_list(&dbmss2);
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  char operation[OPH_COMMON_BUFFER_LEN];
  char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
  int n, result = OPH_ANALYTICS_OPERATOR_SUCCESS, frag_count = 0;
	

  if(oph_dc2_setup_dbms(&(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server), (dbmss.value[0]).io_server_type))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);    
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

  // This implementation assume a perfect correspondence between datacube structures

  //For each DBMS
  for(i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); i++)
  {
	// Current implementation considers data exchange within the same dbms, databases could be different
	if (dbmss.value[i].id_dbms != dbmss2.value[i].id_dbms)
  	{
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Connot compare datacube in different dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DIFFERENT_DBMS_ERROR);
		result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		break;
  	}

	if(oph_dc2_connect_to_dbms(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]), 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	//For each DB
	for(j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++)
	{
		//Check DB - DBMS Association
		if(dbs.value[j].dbms_instance != &(dbmss.value[i])) continue;

		if(oph_dc2_use_db_of_dbms(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j])))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DB_SELECTION_ERROR, (dbs.value[j]).db_name);
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			break;
		}

		//Check DB - DBMS Association
		if(dbs2.value[j].dbms_instance != &(dbmss2.value[i])) // continue;
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Databases are not comparable.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_DIFFERENT_DB_ERROR);
			result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			break;
		}


		//For each fragment
		for(k = 0; (k < frags.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++)
		{
			//Check Fragment - DB Association
			if(frags.value[k].db_instance != &(dbs.value[j])) continue;

			if(frags2.value[k].db_instance != &(dbs2.value[j])) // continue;
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Fragments are not comparable.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_FRAGMENT_COMPARISON_ERROR);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if(oph_dc2_generate_fragment_name(dbs.value[j].db_name, id_datacube_out, handle->proc_rank, (frag_count+1), &frag_name_out))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

#ifdef OPH_DEBUG_MYSQL
			if (compressed) printf("ORIGINAL QUERY: "OPH_INTERCOMPARISON_QUERY2_COMPR_MYSQL "\n", frag_name_out, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, frags.value[k].fragment_name, MYSQL_FRAG_ID, MYSQL_FRAG_ID, frags.value[k].fragment_name, MYSQL_FRAG_MEASURE, frags2.value[k].fragment_name, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, frags.value[k].db_instance->db_name, frags.value[k].fragment_name, frags2.value[k].db_instance->db_name, frags2.value[k].fragment_name, frags.value[k].fragment_name, MYSQL_FRAG_ID, frags2.value[k].fragment_name, MYSQL_FRAG_ID);
			else printf("ORIGINAL QUERY: "OPH_INTERCOMPARISON_QUERY2_MYSQL "\n", frag_name_out, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, frags.value[k].fragment_name, MYSQL_FRAG_ID, MYSQL_FRAG_ID, frags.value[k].fragment_name, MYSQL_FRAG_MEASURE, frags2.value[k].fragment_name, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, MYSQL_FRAG_MEASURE, frags.value[k].db_instance->db_name, frags.value[k].fragment_name, frags2.value[k].db_instance->db_name, frags2.value[k].fragment_name, frags.value[k].fragment_name, MYSQL_FRAG_ID, frags2.value[k].fragment_name, MYSQL_FRAG_ID);
#endif

			if (compressed) n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_INTERCOMPARISON_QUERY2_COMPR, frag_name_out, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_ID, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, OPH_INTERCOMPARISON_FRAG2, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, frags.value[k].db_instance->db_name, frags.value[k].fragment_name, frags2.value[k].db_instance->db_name, frags2.value[k].fragment_name, OPH_INTERCOMPARISON_FRAG1, OPH_INTERCOMPARISON_FRAG2, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_ID, OPH_INTERCOMPARISON_FRAG2, MYSQL_FRAG_ID);
			else n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_INTERCOMPARISON_QUERY2, frag_name_out, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_ID, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_MEASURE, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type, OPH_INTERCOMPARISON_FRAG2, MYSQL_FRAG_MEASURE, MYSQL_FRAG_ID, MYSQL_FRAG_MEASURE, frags.value[k].db_instance->db_name, frags.value[k].fragment_name, frags2.value[k].db_instance->db_name, frags2.value[k].fragment_name, OPH_INTERCOMPARISON_FRAG1, OPH_INTERCOMPARISON_FRAG2, OPH_INTERCOMPARISON_FRAG1, MYSQL_FRAG_ID, OPH_INTERCOMPARISON_FRAG2, MYSQL_FRAG_ID);
			if(n >= OPH_COMMON_BUFFER_LEN)
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation); 
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			//INTERCOMPARISON fragment
			if(oph_dc2_create_fragment_from_query(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server, &(frags.value[k]), NULL, operation, 0, 0, 0))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NEW_FRAG_ERROR, frag_name_out);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}

			//Change fragment fields
			frags.value[k].id_datacube = id_datacube_out;
			strncpy(frags.value[k].fragment_name, 1+strchr(frag_name_out,'.'), OPH_ODB_STGE_FRAG_NAME_SIZE);

			//Insert new fragment
			if(oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags.value[k])))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_FRAGMENT_INSERT_ERROR, frag_name_out);
				result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				break;
			}
			frag_count++;
		}
	}
	oph_dc2_disconnect_from_dbms(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]));
  }

	if(oph_dc2_cleanup_dbms(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->server))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_IOPLUGIN_CLEANUP_ERROR, (dbmss.value[0]).id_dbms);    
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

  oph_odb_free_ophidiadb(&oDB_slave);
  oph_odb_stge_free_fragment_list(&frags);
  oph_odb_stge_free_db_list(&dbs);
  oph_odb_stge_free_dbms_list(&dbmss);
  oph_odb_stge_free_fragment_list(&frags2);
  oph_odb_stge_free_db_list(&dbs2);
  oph_odb_stge_free_dbms_list(&dbmss2);

  if(handle->proc_rank == 0 && (result == OPH_ANALYTICS_OPERATOR_SUCCESS))
  {
	  //Master process print output datacube PID
    char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri) ){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_PID_URI_ERROR );
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if(oph_pid_show_pid(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube, tmp_uri)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_PID_SHOW_ERROR );
		free(tmp_uri);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char jsonbuf[OPH_COMMON_BUFFER_LEN];
	memset(jsonbuf,0,OPH_COMMON_BUFFER_LEN);
	snprintf(jsonbuf,OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_container, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube);

	// ADD OUTPUT PID TO JSON AS TEXT
	if (oph_json_is_objkey_printable(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys,((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys_num,OPH_JSON_OBJKEY_INTERCOMPARISON)) {
		if (oph_json_add_text(handle->operator_json,OPH_JSON_OBJKEY_INTERCOMPARISON,"Output Cube",jsonbuf)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING,__FILE__,__LINE__, OPH_GENERIC_CONTAINER_ID,"ADD TEXT error\n");
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	// ADD OUTPUT PID TO NOTIFICATION STRING
	char tmp_string[OPH_COMMON_BUFFER_LEN];
	snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_DATACUBE_INPUT, jsonbuf);
	if (handle->output_string)
	{
		strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN-strlen(tmp_string));
		free(handle->output_string);
	}
	handle->output_string = strdup(tmp_string);

	free(tmp_uri);
  }

  if (!handle->proc_rank && (result != OPH_ANALYTICS_OPERATOR_SUCCESS)) oph_odb_cube_delete_from_datacube_table(&((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->oDB, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_output_datacube);

  return result;
}

int task_reduce(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NULL_OPERATOR_HANDLE); 
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_INTERCOMPARISON_NULL_OPERATOR_HANDLE); 
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
  //If NULL return success; it's already free
  if (!handle || !handle->operator_handle)
    return OPH_ANALYTICS_OPERATOR_SUCCESS;    
 
  //Only master process has to close and release connection to management OphidiaDB
  if(handle->proc_rank == 0){
	  oph_odb_disconnect_from_ophidiadb(&((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->oDB);
	  oph_odb_free_ophidiadb(&((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->oDB);
  }
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids){
	  free((char *)((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids); 
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->fragment_ids = NULL;
  }
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type){
	  free((char *)((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type); 
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->measure_type = NULL;
  }
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys){
      oph_tp_free_multiple_value_param_list(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys, ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys_num);
      ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->objkeys = NULL;
  }
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid){
	  free((char *)((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid);
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->sessionid = NULL;
  }
  if(((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description){
	  free((char *)((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description);
	  ((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle)->description = NULL;
  }
  free((OPH_INTERCOMPARISON_operator_handle*)handle->operator_handle);
  handle->operator_handle = NULL;
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

