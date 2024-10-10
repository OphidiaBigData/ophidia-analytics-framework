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

#include "oph_analytics_framework.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "oph_ophidiadb_main.h"

#include "oph_log_error_codes.h"
#include "oph_task_parser_library.h"
#include "oph_analytics_operator_library.h"
#include "oph_framework_paths.h"
#include "oph_input_parameters.h"
#include "oph_json_library.h"
#include "oph_pid_library.h"

#include "debug.h"
#include <mpi.h>

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
#include "clients/taketime.h"
#endif

#include "oph_gsoap/oph_soap.h"
#include "oph_gsoap/oph_server_error.h"

extern int msglevel;

int oph_save_json_response(const char *output_json, const char *output_path, const char *output_name)
{
	if (!output_json || !output_path || !output_name)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char filename[OPH_COMMON_BUFFER_LEN];
	snprintf(filename, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_JSON_GENERIC_PATH, output_path, output_name);
	FILE *fp = fopen(filename, "w");
	if (!fp)
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	fprintf(fp, "%s", output_json);
	fclose(fp);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int oph_af_write_json(oph_json *oper_json, char **jstring, char *backtrace, const char *session_id, const char *marker_id)
{
	if (!oper_json)
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

#ifndef OPH_STANDALONE_MODE
	if (oper_json->source && oper_json->source->values && oper_json->source->values[0] && oper_json->source->values[2]) {
		char *oph_web_server = NULL, *tmp2;
		if (oph_pid_get_uri(&oph_web_server) || !oph_web_server) {
			oph_json_free(oper_json);
			if (oph_web_server)
				free(oph_web_server);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading session prefix.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in reading session prefix.\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		unsigned int length = strlen(oph_web_server);
		if (length >= OPH_COMMON_BUFFER_LEN) {
			oph_json_free(oper_json);
			free(oph_web_server);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "System parameter is too long.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "System parameter is too long.\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (strncmp(oper_json->source->values[0], oph_web_server, length)) {
			free(oph_web_server);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid session '%s'.\n", oper_json->source->values[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Invalid session '%s'.\n", oper_json->source->values[0]);
			oph_json_free(oper_json);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		free(oph_web_server);
		while ((*(oper_json->source->values[0] + length)) == '/')
			length++;
		char tmp[OPH_COMMON_BUFFER_LEN], *savepointer = NULL;
		snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s", oper_json->source->values[0] + length);
		tmp2 = strtok_r(tmp, "/", &savepointer);
		if (!tmp2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid session '%s'.\n", oper_json->source->values[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Invalid session '%s'.\n", oper_json->source->values[0]);
			oph_json_free(oper_json);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		tmp2 = strtok_r(NULL, "/", &savepointer);
		if (!tmp2) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid session '%s'.\n", oper_json->source->values[0]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Invalid session '%s'.\n", oper_json->source->values[0]);
			oph_json_free(oper_json);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		free(oper_json->source->values[0]);
		oper_json->source->values[0] = strdup(tmp2);	// Now it contains the session code
		if (!oper_json->source->values[0]) {
			oph_json_free(oper_json);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating session code.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating session code.\n");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	if (backtrace) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
		if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "Backtrace", backtrace)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Error in saving backtrace.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error in saving backtrace.\n");
		}
#endif
		free(backtrace);
		set_log_backtrace(NULL);
	}

	char session_code[OPH_COMMON_BUFFER_LEN];
	*session_code = 0;
	if (oper_json->source && oper_json->source->values && oper_json->source->values[0])
		strcpy(session_code, oper_json->source->values[0]);
	else if (session_id)
		oph_pid_get_session_code(session_id, session_code);

	char marker_code[OPH_COMMON_BUFFER_LEN];
	*marker_code = 0;
	if (oper_json->source && oper_json->source->values && oper_json->source->values[2])
		strcpy(marker_code, oper_json->source->values[2]);
	else if (marker_id)
		strcpy(marker_code, marker_id);

	char filename[OPH_COMMON_BUFFER_LEN];
	snprintf(filename, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_JSON_PATH, oph_pid_path()? oph_pid_path() : OPH_PREFIX_CLUSTER, session_code, marker_code);
	if (_oph_json_to_json_file(oper_json, filename, jstring)) {
		oph_json_free(oper_json);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "JSON file creation failed.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "JSON file creation failed.\n");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#endif

	oph_json_free(oper_json);

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int oph_af_create_job(ophidiadb *oDB, char *task_string, HASHTBL *task_tbl, int *id_job)
{
	int id_session;

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter task table\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char *username = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!username) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	char *sessionid = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!sessionid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
#ifndef OPH_STANDALONE_MODE
	char *markerid = hashtbl_get(task_tbl, OPH_ARG_MARKERID);
#else
	char *markerid = "0";
#endif
	if (!markerid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_ARG_MARKERID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_MARKERID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "'%s' '%s'\n", sessionid, markerid);

	int res;
	int id_folder = 1;
	if ((res = oph_odb_job_retrieve_session_id(oDB, sessionid, &id_session))) {
		if (res != OPH_ODB_NO_ROW_FOUND) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve session id\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_READ_ERROR, "'idsession'");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		// A new entry has to be created in table 'session'
		if (oph_odb_job_update_session_table(oDB, sessionid, username, id_folder, &id_session)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a new entry in table 'session'\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_WRITE_ERROR, "new session");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
#ifndef OPH_STANDALONE_MODE
	if ((res = oph_odb_job_retrieve_job_id(oDB, sessionid, markerid, id_job))) {
		if (res != OPH_ODB_NO_ROW_FOUND) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve job id\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_READ_ERROR, "idjob");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
#endif
		// A new entry has to be created in table 'job'
		if (oph_odb_job_update_job_table(oDB, markerid, task_string, OPH_ODB_JOB_STATUS_UNKNOWN_STR, username, id_session, id_job, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create a new entry in table 'job'\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_WRITE_ERROR, "new job");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
#ifndef OPH_STANDALONE_MODE
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Found a job with the same identifier\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_READ_JOB_ERROR);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int _oph_af_execute_framework(oph_operator_struct *handle, char *task_string, int task_number, int task_rank)
{
	int res = 0, idjob = -1;
	ophidiadb oDB;
	oph_json *oper_json = NULL;

#ifndef OPH_STANDALONE_MODE
/* gSOAP data */
	xsd__int response;
	struct soap soap;
	oph_soap_data data;
	short have_soap = 0;
#endif
	char error_message[OPH_COMMON_BUFFER_LEN];
	char notify_sessionid[OPH_TP_TASKLEN];
#ifndef OPH_STANDALONE_MODE
	char notify_jobid[OPH_TP_TASKLEN];
	char notify_parent_jobid[OPH_TP_TASKLEN];
	char notify_task_index[OPH_TP_TASKLEN];
	char notify_light_task_index[OPH_TP_TASKLEN];
	char notify_cube[OPH_TP_TASKLEN];
	char notify_cwd[OPH_TP_TASKLEN];
	char notify_message[OPH_COMMON_BUFFER_LEN];
	char notify_save[OPH_COMMON_BUFFER_LEN];
	*notify_jobid = 0;
#else
	*notify_sessionid = 0;
#endif

#ifdef OPH_TIME_DEBUG_1
	/*Partial Exec time evaluate */ struct timeval stime, etime, ttime;

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	/*Partial Exec time evaluate */ struct timeval stime, etime, ttime;
	gettimeofday(&stime, NULL);
#endif

	char *backtrace = NULL;
	if (!task_rank)
		set_log_backtrace(&backtrace);

#ifdef OPH_STANDALONE_MODE
	/* If we are using stand-alone mode, replace server-side arguments with fill values */
	snprintf(task_string + strlen(task_string), strlen("cwd=/standalone;userrole=admin;") + 1, "%s", "cwd=/standalone;userrole=admin;");
#endif

	//Initialize environment
	oph_operator_struct_initializer(task_number, task_rank, handle);
#ifdef OPH_STANDALONE_MODE
	handle->soap_data = NULL;
#else
	handle->soap_data = &data;
#endif

	oph_tp_start_xml_parser();

	char marker_id[OPH_TP_TASKLEN];
	// Pre-parsing for SessionCode and Markerid
	if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_MARKERID, marker_id))
		*marker_id = 0;

	if (!task_rank) {
#ifndef OPH_STANDALONE_MODE
		if (!oph_tp_find_param_in_task_string(task_string, OPH_ARG_IDJOB, notify_jobid))
			idjob = (int) strtol(notify_jobid, NULL, 10);
		else
			strcpy(notify_jobid, "0");
		if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_PARENTID, notify_parent_jobid))
			strcpy(notify_parent_jobid, "0");
		if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_TASKINDEX, notify_task_index))
			strcpy(notify_task_index, "-1");
		if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index))
			strcpy(notify_light_task_index, "-1");
		if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_SESSIONID, notify_sessionid))
			*notify_sessionid = 0;
		if (oph_tp_find_param_in_task_string(task_string, OPH_IN_PARAM_DATACUBE_INPUT, notify_cube))
			*notify_cube = 0;
		if (oph_tp_find_param_in_task_string(task_string, OPH_IN_PARAM_CWD, notify_cwd))
			*notify_cwd = 0;
		if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_SAVE, notify_save))
			strcpy(notify_save, OPH_COMMON_YES_VALUE);

		if (oph_soap_init(&soap, &data))
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to send SOAP notification.\n");
		else
			have_soap = 1;
#endif
		if (oph_json_alloc(&oper_json)) {
			oph_json_free(oper_json);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "JSON alloc error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "JSON alloc error\n");
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	//In multi-thread code mysql_library_init must be called before starting any threads
	if (mysql_library_init(0, NULL, NULL)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
		oph_tp_end_xml_parser();

		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "MySQL initialization error%s\n", "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, "MySQL initialization error%s\n", backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

	HASHTBL *task_tbl = NULL;
	if (oph_tp_task_params_parser(task_string, &task_tbl)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to process input parameters\n");
		oph_tp_end_xml_parser();
		if (task_tbl)
			hashtbl_destroy(task_tbl);
		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_PROCESS_INPUT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_PROCESS_INPUT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifndef OPH_STANDALONE_MODE
	hashtbl_insert(task_tbl, OPH_ARG_IDJOB, notify_jobid);
#endif

	char tmp_value[OPH_TP_TASKLEN];
	*tmp_value = 0;
#ifndef OPH_STANDALONE_MODE
	//Find params in task string
	if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_WORKFLOWID, tmp_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing %s input parameter\n", OPH_ARG_WORKFLOWID);
		oph_tp_end_xml_parser();
		hashtbl_destroy(task_tbl);
		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	hashtbl_insert(task_tbl, OPH_ARG_WORKFLOWID, tmp_value);


	if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_MARKERID, tmp_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing %s input parameter\n", OPH_ARG_MARKERID);
		oph_tp_end_xml_parser();
		hashtbl_destroy(task_tbl);
		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	hashtbl_insert(task_tbl, OPH_ARG_MARKERID, tmp_value);
#endif
	if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_USERNAME, tmp_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing %s input parameter\n", OPH_ARG_USERNAME);
		hashtbl_destroy(task_tbl);
		oph_tp_end_xml_parser();
		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	hashtbl_insert(task_tbl, OPH_ARG_USERNAME, tmp_value);

	if (oph_tp_find_param_in_task_string(task_string, OPH_ARG_USERROLE, tmp_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing %s input parameter\n", OPH_ARG_USERROLE);
		hashtbl_destroy(task_tbl);
		oph_tp_end_xml_parser();
		if (!task_rank) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_MISSING_INPUT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	hashtbl_insert(task_tbl, OPH_ARG_USERROLE, tmp_value);

	if (!task_rank) {
#ifndef OPH_STANDALONE_MODE
		//Init JSON START
		if (oph_json_set_source(oper_json, "oph", "Ophidia", NULL, "Ophidia Data Source", hashtbl_get(task_tbl, OPH_ARG_USERNAME))) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_SET_SOURCE, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_SET_SOURCE, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "SET SOURCE error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "SET SOURCE error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_json_add_source_detail(oper_json, "Session Code", hashtbl_get(task_tbl, OPH_ARG_SESSIONID))) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_SESSION_CODE, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_SESSION_CODE, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD SOURCE DETAIL error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD SOURCE DETAIL error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char tmpbuf[OPH_COMMON_BUFFER_LEN];	// Used partially for workflow and marker ids
		snprintf(tmpbuf, OPH_COMMON_MAX_LONG_LENGHT, "%ld", strtol((const char *) hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID), NULL, 10));
		if (oph_json_add_source_detail(oper_json, "Workflow", tmpbuf)) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_WORKFLOWID, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_WORKFLOWID, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD SOURCE DETAIL error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD SOURCE DETAIL error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		snprintf(tmpbuf, OPH_COMMON_MAX_LONG_LENGHT, "%ld", strtol((const char *) hashtbl_get(task_tbl, OPH_ARG_MARKERID), NULL, 10));
		if (oph_json_add_source_detail(oper_json, "Marker", tmpbuf)) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_MARKERID, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_MARKERID, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD SOURCE DETAIL error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD SOURCE DETAIL error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		snprintf(tmpbuf, OPH_COMMON_BUFFER_LEN, "%s?%s#%s", (char *) hashtbl_get(task_tbl, OPH_ARG_SESSIONID), (char *) hashtbl_get(task_tbl, OPH_ARG_WORKFLOWID),
			 (char *) hashtbl_get(task_tbl, OPH_ARG_MARKERID));
		if (oph_json_add_source_detail(oper_json, "JobID", tmpbuf)) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_JOBID, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_JOBID, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD SOURCE DETAIL error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD SOURCE DETAIL error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_json_add_consumer(oper_json, hashtbl_get(task_tbl, OPH_ARG_USERNAME))) {
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_CONSUMER, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_ADD_CONSUMER, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD CONSUMER error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD CONSUMER error\n");
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Init JSON END
#endif

		oph_odb_init_ophidiadb(&oDB);
		if (oph_odb_read_ophidiadb_config_file(&oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_CONFIGURATION_FILE);
			oph_tp_end_xml_parser();
			oph_odb_free_ophidiadb(&oDB);
			hashtbl_destroy(task_tbl);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_READ_CONFIGURATION, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_READ_CONFIGURATION, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_odb_connect_to_ophidiadb(&oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_OPHIDIADB_CONNECTION_ERROR);
			oph_tp_end_xml_parser();
			oph_odb_free_ophidiadb(&oDB);
			hashtbl_destroy(task_tbl);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_CONNECT, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_CONNECT, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ARG_IDJOB,
					 notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
			mysql_library_end();
			oph_pid_free();
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		if (idjob)
			oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_RUNNING);

#ifdef OPH_STANDALONE_MODE
		/* If we are using stand-alone mode, replace server-side arguments with fill values */
		hashtbl_remove(task_tbl, OPH_ARG_USERROLE);
		hashtbl_insert(task_tbl, OPH_ARG_USERROLE, "admin");
		hashtbl_remove(task_tbl, OPH_IN_PARAM_CWD);
		hashtbl_insert(task_tbl, OPH_IN_PARAM_CWD, "/standalone");
		hashtbl_remove(task_tbl, OPH_ARG_SESSIONID);
		hashtbl_insert(task_tbl, OPH_ARG_SESSIONID, "/session/standalone/experiment");

		int folder_id = 0;
		if (oph_odb_fs_path_parsing("", "/standalone", &folder_id, NULL, &oDB)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Path /standalone doesn't exists\n");
			if (oph_odb_fs_insert_into_folder_table(&oDB, 1, "standalone", &folder_id)) {
				//Add standalone folder always under root
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create standalone folder.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to create standalone folder.\n");
				oph_tp_end_xml_parser();
				oph_odb_free_ophidiadb(&oDB);
				hashtbl_destroy(task_tbl);
				snprintf(error_message, OPH_COMMON_BUFFER_LEN, "Unable to create standalone folder.\n");
				mysql_library_end();
				oph_pid_free();
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
		}
#endif

		if (idjob < 0) {
			if (oph_af_create_job(&oDB, task_string, task_tbl, &idjob)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to save job parameters into OphidiaDB. Check access parameters.\n");
				oph_tp_end_xml_parser();
				oph_odb_free_ophidiadb(&oDB);
				hashtbl_destroy(task_tbl);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
				snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_WRITE_JOB, "");
#else
				snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNABLE_TO_WRITE_JOB, backtrace);
#endif
				if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
					oph_json_free(oper_json);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				} else
					oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
				if (have_soap) {
					snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_RUNNING_ERROR,
						 OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX,
						 notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
					if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
						pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
					else if (response)
						pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
					oph_soap_cleanup(&soap, &data);
				}
/* gSOAP notification end */
#endif
				mysql_library_end();
				oph_pid_free();
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//Insert idjob in hash table
			snprintf(tmp_value, sizeof(tmp_value), "%d", idjob);
			hashtbl_insert(task_tbl, OPH_ARG_IDJOB, tmp_value);
		}
		if (idjob)
			oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_START);

#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
		if (have_soap) {
			snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_START, OPH_ARG_IDJOB, notify_jobid,
				 OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid,
				 OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
			if (oph_notify(&soap, &data, notify_message, NULL, &response))
				pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
			else if (response)
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
		}
/* gSOAP notification end */
#endif
	}
#ifdef BENCHMARK
	struct timeval stime_bench, etime_bench, ttime_bench;

	if (task_rank == 0)
		gettimeofday(&stime_bench, NULL);
#endif

	if (!task_rank)
		handle->operator_json = oper_json;

#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Load task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Load task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_SET_ENV);

	//Initialize all processes handles
	if ((res = oph_set_env(task_tbl, handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Process initilization failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_SET_ENV_ERROR;
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		hashtbl_destroy(task_tbl);
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_SET_ENV_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_SET_ENV_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_SET_ENV_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	hashtbl_destroy(task_tbl);

#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Set env:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Set env:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_INIT);

	//Perform task initializazion procedures (optional)
	if ((res = oph_init_task(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Task initilization failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_INIT_ERROR;
		oph_destroy_task(handle);
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_INIT_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_INIT_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_INIT_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Init task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Init task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_DISTRIBUTE);

	//Perform workload distribution activities (optional)
	if ((res = oph_distribute_task(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Task distribution failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR;
		oph_destroy_task(handle);
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_DISTRIBUTE_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_DISTRIBUTE_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Distribute task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Distribute task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_EXECUTE);

	//Perform distributive part of task
	if ((res = oph_execute_task(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Task execution failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_EXECUTE_ERROR;
		oph_destroy_task(handle);
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_EXECUTE_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_EXECUTE_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_EXECUTE_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Execute task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Execute task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_REDUCE);

	//Perform reduction of results
	if ((res = oph_reduce_task(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Task reduction failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_REDUCE_ERROR;
		oph_destroy_task(handle);
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_REDUCE_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_REDUCE_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_REDUCE_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Reduce task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Reduce task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_DESTROY);

	//Reset task specific initialization procedures
	if ((res = oph_destroy_task(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Task destroy failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_DESTROY_ERROR;
		oph_unset_env(handle);
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_DESTROY_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_DESTROY_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_DESTROY_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS,
					 handle->output_code, OPH_ARG_IDJOB, notify_jobid, OPH_ARG_PARENTID, notify_parent_jobid,
					 OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid, OPH_ARG_MARKERID, marker_id,
					 OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Destroy task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Destroy task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

	if (!task_rank && idjob)
		oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_UNSET_ENV);

	//Release task and dynamic library resources
	if ((res = oph_unset_env(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Process deinit failed [Code: %d]!\n", res);
		if (!handle->output_code)
			handle->output_code = OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR;
		oph_tp_end_xml_parser();
		if (handle->dlh)
			oph_exit_task();
		if (!task_rank) {
			if (idjob)
				oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR);
			oph_odb_free_ophidiadb(&oDB);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2)
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNSET_ENV_ERROR, "");
#else
			snprintf(error_message, OPH_COMMON_BUFFER_LEN, OPH_LOG_ANALITICS_OPERATOR_UNSET_ENV_ERROR, backtrace);
#endif
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "ERROR", error_message)) {
				oph_json_free(oper_json);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			} else
				oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id);
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
			if (have_soap) {
				snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, handle->output_code, OPH_ARG_IDJOB, notify_jobid,
					 OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID,
					 notify_sessionid, OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
				if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
					pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
				else if (response)
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
				oph_soap_cleanup(&soap, &data);
			}
/* gSOAP notification end */
#endif
		}
		mysql_library_end();
		oph_pid_free();
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Unset env:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}

	if (task_rank == 0)
		gettimeofday(&stime, NULL);
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Unset env:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);

	gettimeofday(&stime, NULL);
#endif

#ifdef BENCHMARK
	gettimeofday(&etime_bench, NULL);
	timeval_subtract(&ttime_bench, &etime_bench, &stime_bench);
	char exec_time[OPH_COMMON_BUFFER_LEN];
	snprintf(exec_time, OPH_COMMON_BUFFER_LEN, "%d,%06d sec", (int) ttime_bench.tv_sec, (int) ttime_bench.tv_usec);
#endif

	//Release environment resources
	oph_tp_end_xml_parser();
	if (handle->dlh)
		oph_exit_task();

	int return_code = 0;
	if (!task_rank) {
		if (idjob)
			oph_odb_job_set_job_status(&oDB, idjob, OPH_ODB_JOB_STATUS_COMPLETED);
		oph_odb_free_ophidiadb(&oDB);

		if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_STATUS, "SUCCESS", NULL)) {
			oph_json_free(oper_json);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			return_code = -1;
		}
#ifdef BENCHMARK
		else if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_EXEC_TIME, "Execution Time", exec_time)) {
			oph_json_free(oper_json);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
			return_code = -1;
		}
#endif
		else if (oph_af_write_json(oper_json, &handle->output_json, backtrace, notify_sessionid, marker_id))
			return_code = -1;
	}
#ifndef OPH_STANDALONE_MODE
/* gSOAP notification start */
	MPI_Barrier(MPI_COMM_WORLD);	//Barrier synchronization before successful notification
	if (!task_rank && have_soap) {
		snprintf(notify_message, OPH_COMMON_BUFFER_LEN, "%s=%d;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;", OPH_ARG_STATUS, OPH_ODB_JOB_STATUS_COMPLETED, OPH_ARG_IDJOB, notify_jobid,
			 OPH_ARG_PARENTID, notify_parent_jobid, OPH_ARG_TASKINDEX, notify_task_index, OPH_ARG_LIGHTTASKINDEX, notify_light_task_index, OPH_ARG_SESSIONID, notify_sessionid,
			 OPH_ARG_MARKERID, marker_id, OPH_ARG_SAVE, notify_save);
		if (handle->output_string) {
			strncat(notify_message, handle->output_string, OPH_TP_TASKLEN - strlen(notify_message));

			snprintf(notify_jobid, OPH_TP_TASKLEN, "%s=", OPH_IN_PARAM_DATACUBE_INPUT);
			if (strstr(handle->output_string, notify_jobid))
				*notify_cube = 0;
			snprintf(notify_jobid, OPH_TP_TASKLEN, "%s=", OPH_IN_PARAM_CWD);
			if (strstr(handle->output_string, notify_jobid))
				*notify_cwd = 0;
		}
		if (strlen(notify_cube)) {
			snprintf(notify_jobid, OPH_TP_TASKLEN, "%s=%s;", OPH_IN_PARAM_DATACUBE_INPUT, notify_cube);
			strncat(notify_message, notify_jobid, OPH_TP_TASKLEN - strlen(notify_message));
		}
		if (strlen(notify_cwd) && strlen(notify_sessionid)) {
			char *new_folder_to_be_printed = NULL;
			if (!oph_pid_drop_session_prefix(notify_cwd, notify_sessionid, &new_folder_to_be_printed)) {
				snprintf(notify_jobid, OPH_TP_TASKLEN, "%s=%s;", OPH_IN_PARAM_CWD, new_folder_to_be_printed);
				strncat(notify_message, notify_jobid, OPH_TP_TASKLEN - strlen(notify_message));
			}
			if (new_folder_to_be_printed)
				free(new_folder_to_be_printed);
		}
		if (oph_notify(&soap, &data, notify_message, handle->output_json, &response))
			pmesg(LOG_WARNING, __FILE__, __LINE__, "SOAP connection refused.\n");
		else if (response)
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Error %d in sending SOAP notification.\n", response);
		oph_soap_cleanup(&soap, &data);
	}
/* gSOAP notification end */
#endif

	//In multi-thread code mysql_library_end must be called after executing all the threads
	mysql_library_end();
	oph_pid_free();

	if (return_code)
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;

#ifdef OPH_TIME_DEBUG_1
	MPI_Barrier(MPI_COMM_WORLD);
	if (task_rank == 0) {
		gettimeofday(&etime, NULL);
		timeval_subtract(&ttime, &etime, &stime);
		printf("Proc %d: Unload task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
	}
#elif OPH_TIME_DEBUG_2
	gettimeofday(&etime, NULL);
	timeval_subtract(&ttime, &etime, &stime);
	printf("Proc %d: Unload task:\t Time %d,%06d sec\n", task_rank, (int) ttime.tv_sec, (int) ttime.tv_usec);
#endif

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int oph_af_execute_framework(char *task_string, int task_number, int task_rank)
{
	oph_operator_struct handle;
	int result = _oph_af_execute_framework(&handle, task_string, task_number, task_rank);

#ifndef OPH_STANDALONE_MODE
	oph_save_json_response(handle.output_json, handle.output_path, handle.output_name);
#endif
	if (handle.output_string) {
		free(handle.output_string);
		handle.output_string = NULL;
	}
	if (handle.output_json) {
		free(handle.output_json);
		handle.output_json = NULL;
	}
	if (handle.output_path) {
		free(handle.output_path);
		handle.output_path = NULL;
	}
	if (handle.output_name) {
		free(handle.output_name);
		handle.output_name = NULL;
	}
	return result;
}
