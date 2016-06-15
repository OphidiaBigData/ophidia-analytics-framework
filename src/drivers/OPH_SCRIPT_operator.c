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

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "drivers/OPH_SCRIPT_operator.h"

#include <errno.h>
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mpi.h>

#include "oph_analytics_operator_library.h"
#include "oph_task_parser_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_framework_paths.h"
#include "oph_pid_library.h"
#include "oph_directory_library.h"

#define MAX_OUT_LEN 500*1024

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

  if (!(handle->operator_handle = (OPH_SCRIPT_operator_handle *) calloc (1, sizeof (OPH_SCRIPT_operator_handle)))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_HANDLE );
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  //1 - Set up struct to empty values
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args_num = -1;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys_num = -1;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id = NULL;
  ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id = NULL;

  //3 - Fill struct with the correct data
  char *value;

  // retrieve objkeys
  value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(oph_tp_parse_multiple_value_param(value, &((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys, &((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys_num)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
    oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys_num);
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCRIPT);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCRIPT);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_SCRIPT);
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "script" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_ARGS);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_ARGS);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_ARGS );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(oph_tp_parse_multiple_value_param(value, &((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args, &((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args_num)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
    oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args, ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args_num);
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDOUT);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDOUT);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDOUT);
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "stdout" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_STDERR);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STDERR);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_STDERR);
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "stderr" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }
  
  char session_code[OPH_COMMON_BUFFER_LEN];
  oph_pid_get_session_code(hashtbl_get(task_tbl,OPH_ARG_SESSIONID),session_code);
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_code = (char *) strdup (session_code))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
 	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session code" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  char buff[OPH_COMMON_BUFFER_LEN];
  snprintf(buff,OPH_COMMON_BUFFER_LEN,OPH_FRAMEWORK_MISCELLANEA_FILES_PATH,oph_pid_path()?oph_pid_path():OPH_PREFIX_CLUSTER,session_code);
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path = (char *) strdup (buff))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session path" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }
  snprintf(buff,OPH_COMMON_BUFFER_LEN,OPH_FRAMEWORK_MISCELLANEA_FILES_PATH,oph_pid_uri(),session_code);
  if(!(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url = (char *) strdup (buff))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
 	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, "session url" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID);
  if(value && !(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id = (char *) strdup (value))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
 	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_WORKFLOWID );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_ARG_MARKERID);
  if(value && !(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id = (char *) strdup (value))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
 	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_MEMORY_ERROR_INPUT, OPH_ARG_MARKERID );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_NULL_OPERATOR_HANDLE );
	return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

	//Create dir if not exist
  struct stat st;
	if(stat(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path,&st)){
		if(oph_dir_r_mkdir(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create dir %s\n", ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path);
			logging(LOG_ERROR, __FILE__, __LINE__, 0, OPH_LOG_GENERIC_DIR_CREATION_ERROR, ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path );
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	} //If dir already exists then exit

	//If script is not a regular file then error
	if(stat(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script,&st)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "%s: no such file\n", ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script);
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "%s: no such file\n", ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script );
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char *ptr = realpath(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script, NULL);
	if(!ptr){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to resolve path\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Unable to resolve path\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if(!strncmp(ptr,"/bin/",5) || !strncmp(ptr,"/sbin/",6) || !strncmp(ptr,"/usr/bin/",9) || !strncmp(ptr,"/usr/sbin/",10)){
		free(ptr);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Permission denied\n");
		logging(LOG_ERROR, __FILE__, __LINE__, 0, "Permission denied\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	free(ptr);

  int error=0,n=0,i;
  char command[OPH_COMMON_BUFFER_LEN];

  memset(command,0,OPH_COMMON_BUFFER_LEN);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"OPH_SCRIPT_SESSION_PATH='%s' ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"OPH_SCRIPT_SESSION_URL='%s' ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"OPH_SCRIPT_SESSION_CODE='%s' ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_code);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"OPH_SCRIPT_WORKFLOW_ID=%s ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id ? ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id : 0);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"OPH_SCRIPT_MARKER_ID=%s ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id ? ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id : 0);
  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"%s ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script);

  for (i = 0; i < ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args_num; i++) {
	  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"%s ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args[i]);
  }

  if (strcmp(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir,"stdout")) {
	  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"1>>%s ",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir);
  }
  if (strcmp(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir,"stderr")) {
	  n += snprintf(command+n,OPH_COMMON_BUFFER_LEN-n,"2>>%s",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir);
  }

  // Dynamic creation of the folders
	char dirname[OPH_COMMON_BUFFER_LEN];
	snprintf(dirname,OPH_COMMON_BUFFER_LEN,"%s/%s",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path,((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id);
	struct stat ss;
	if (stat(dirname,&ss) && (errno==ENOENT))
	{
		int i;
		for (i=0;dirname[i];++i)
		{
			if (dirname[i]=='/')
			{
				dirname[i]=0;
				mkdir(dirname, 0755);
				dirname[i]='/';
			}
		}
		mkdir(dirname, 0755);
	}

  char system_output[MAX_OUT_LEN];
  memset(system_output,0,MAX_OUT_LEN);
  char line[OPH_COMMON_BUFFER_LEN];
  snprintf(system_output, MAX_OUT_LEN, "Command: %s\n\n",command);

  int s=0;
  FILE* fp = popen(command,"r");
  if (!fp) error = -1;
  else
  {
	while (fgets(line, OPH_COMMON_BUFFER_LEN, fp)) if ((s=MAX_OUT_LEN-strlen(system_output))>1) strncat(system_output,line,s);
	error = pclose(fp);
  }

	// ADD COMMAND TO JSON AS TEXT
	if ((s=oph_json_is_objkey_printable(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys,((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys_num,OPH_JSON_OBJKEY_SCRIPT)))
	{
		if (oph_json_add_text(handle->operator_json,OPH_JSON_OBJKEY_SCRIPT,"Launched command",system_output))
		{
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING,__FILE__,__LINE__, OPH_GENERIC_CONTAINER_ID,"ADD TEXT error\n");
		}
		if (((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id) snprintf(system_output,MAX_OUT_LEN,"%s/%s",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url,((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id);
		else snprintf(system_output,MAX_OUT_LEN,"%s",((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url);
		if (oph_json_add_text(handle->operator_json,OPH_JSON_OBJKEY_SCRIPT,"Output URL",system_output))
		{
			pmesg(LOG_WARNING, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING,__FILE__,__LINE__, OPH_GENERIC_CONTAINER_ID,"ADD TEXT error\n");
		}
	}

	// ADD OUTPUT PID TO NOTIFICATION STRING
	char tmp_string[OPH_COMMON_BUFFER_LEN];
	snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_LINK, s ? system_output : ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url);
	if (handle->output_string)
	{
		strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN-strlen(tmp_string));
		free(handle->output_string);
	}
	handle->output_string = strdup(tmp_string);

  if(error == -1){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "System command failed\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_COMMAND_FAILED );
	return OPH_ANALYTICS_OPERATOR_COMMAND_ERROR;
  }
  else if (WEXITSTATUS(error) == 127){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "System command cannot be executed\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_COMMAND_NOT_EXECUTED );
	return OPH_ANALYTICS_OPERATOR_COMMAND_ERROR;  
  }
  else if(WEXITSTATUS(error) != 0){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Command failed with code %d\n",WEXITSTATUS(error));
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Command failed with code %d\n",WEXITSTATUS(error) );
	return OPH_ANALYTICS_OPERATOR_COMMAND_ERROR;
  }
  else return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_SCRIPT_NULL_OPERATOR_HANDLE );
	return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
  //If NULL return success; it's already free
  if (!handle || !handle->operator_handle)
    return OPH_ANALYTICS_OPERATOR_SUCCESS;    
 
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args){
      oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args, ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args_num);
      ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->args = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->err_redir = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->out_redir = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->script = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_path = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_url = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_code){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_code);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->session_code = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->workflow_id = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id){
    free((char *)((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id);
    ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->marker_id = NULL;
  }
  if(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys){
      oph_tp_free_multiple_value_param_list(((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys, ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys_num);
      ((OPH_SCRIPT_operator_handle*)handle->operator_handle)->objkeys = NULL;
  }
  free((OPH_SCRIPT_operator_handle*)handle->operator_handle);
  handle->operator_handle = NULL;
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
