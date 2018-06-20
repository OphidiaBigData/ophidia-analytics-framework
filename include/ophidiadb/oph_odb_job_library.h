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

#ifndef __OPH_ODB_JOB_LIBRARY__
#define __OPH_ODB_JOB_LIBRARY__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_job_query.h"

#define OPH_ODB_JOB_STATUS_UNKNOWN_STR "OPH_STATUS_UNKNOWN"

#define OPH_ODB_JOB_STATUS_PENDING_STR "OPH_STATUS_PENDING"
#define OPH_ODB_JOB_STATUS_WAIT_STR "OPH_STATUS_WAIT"
#define OPH_ODB_JOB_STATUS_RUNNING_STR "OPH_STATUS_RUNNING"
#define OPH_ODB_JOB_STATUS_START_STR "OPH_STATUS_START"
#define OPH_ODB_JOB_STATUS_SET_ENV_STR "OPH_STATUS_SET_ENV"
#define OPH_ODB_JOB_STATUS_INIT_STR "OPH_STATUS_INIT"
#define OPH_ODB_JOB_STATUS_DISTRIBUTE_STR "OPH_STATUS_DISTRIBUTE"
#define OPH_ODB_JOB_STATUS_EXECUTE_STR "OPH_STATUS_EXECUTE"
#define OPH_ODB_JOB_STATUS_REDUCE_STR "OPH_STATUS_REDUCE"
#define OPH_ODB_JOB_STATUS_DESTROY_STR "OPH_STATUS_DESTROY"
#define OPH_ODB_JOB_STATUS_UNSET_ENV_STR "OPH_STATUS_UNSET_ENV"
#define OPH_ODB_JOB_STATUS_COMPLETED_STR "OPH_STATUS_COMPLETED"

#define OPH_ODB_JOB_STATUS_ERROR_STR "OPH_STATUS_ERROR"
#define OPH_ODB_JOB_STATUS_PENDING_ERROR_STR "OPH_STATUS_PENDING_ERROR"
#define OPH_ODB_JOB_STATUS_RUNNING_ERROR_STR "OPH_STATUS_RUNNING_ERROR"
#define OPH_ODB_JOB_STATUS_START_ERROR_STR "OPH_STATUS_START_ERROR"
#define OPH_ODB_JOB_STATUS_SET_ENV_ERROR_STR "OPH_STATUS_SET_ENV_ERROR"
#define OPH_ODB_JOB_STATUS_INIT_ERROR_STR "OPH_STATUS_INIT_ERROR"
#define OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR_STR "OPH_STATUS_DISTRIBUTE_ERROR"
#define OPH_ODB_JOB_STATUS_EXECUTE_ERROR_STR "OPH_STATUS_EXECUTE_ERROR"
#define OPH_ODB_JOB_STATUS_REDUCE_ERROR_STR "OPH_STATUS_REDUCE_ERROR"
#define OPH_ODB_JOB_STATUS_DESTROY_ERROR_STR "OPH_STATUS_DESTROY_ERROR"
#define OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR_STR "OPH_STATUS_UNSET_ENV_ERROR"

#define OPH_ODB_JOB_STATUS_SKIPPED_STR "OPH_STATUS_SKIPPED"
#define OPH_ODB_JOB_STATUS_ABORTED_STR "OPH_STATUS_ABORTED"
#define OPH_ODB_JOB_STATUS_UNSELECTED_STR "OPH_STATUS_UNSELECTED"
#define OPH_ODB_JOB_STATUS_EXPIRED_STR "OPH_STATUS_EXPIRED"

#define OPH_ODB_JOB_STATUS_CLOSED_STR "OPH_STATUS_CLOSED"

typedef enum { OPH_ODB_JOB_STATUS_UNKNOWN, OPH_ODB_JOB_STATUS_PENDING, OPH_ODB_JOB_STATUS_WAIT, OPH_ODB_JOB_STATUS_RUNNING, OPH_ODB_JOB_STATUS_START, OPH_ODB_JOB_STATUS_SET_ENV,
	OPH_ODB_JOB_STATUS_INIT, OPH_ODB_JOB_STATUS_DISTRIBUTE, OPH_ODB_JOB_STATUS_EXECUTE, OPH_ODB_JOB_STATUS_REDUCE, OPH_ODB_JOB_STATUS_DESTROY, OPH_ODB_JOB_STATUS_UNSET_ENV,
	OPH_ODB_JOB_STATUS_COMPLETED, OPH_ODB_JOB_STATUS_ERROR, OPH_ODB_JOB_STATUS_PENDING_ERROR, OPH_ODB_JOB_STATUS_RUNNING_ERROR, OPH_ODB_JOB_STATUS_START_ERROR,
	OPH_ODB_JOB_STATUS_SET_ENV_ERROR, OPH_ODB_JOB_STATUS_INIT_ERROR, OPH_ODB_JOB_STATUS_DISTRIBUTE_ERROR, OPH_ODB_JOB_STATUS_EXECUTE_ERROR, OPH_ODB_JOB_STATUS_REDUCE_ERROR,
	OPH_ODB_JOB_STATUS_DESTROY_ERROR, OPH_ODB_JOB_STATUS_UNSET_ENV_ERROR, OPH_ODB_JOB_STATUS_SKIPPED, OPH_ODB_JOB_STATUS_ABORTED, OPH_ODB_JOB_STATUS_UNSELECTED, OPH_ODB_JOB_STATUS_EXPIRED,
	OPH_ODB_JOB_STATUS_CLOSED
} oph_odb_job_status;

/**
 * \brief Function to set the job status
 * \param oDB Pointer to OphidiaDB
 * \param id_job Id of job being updated
 * \param status Status to be set
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_job_set_job_status(ophidiadb * oDB, int id_job, oph_odb_job_status status);

/**
 * \brief Function to retrieve the ID of a session
 * \param oDB Pointer to OphidiaDB
 * \param sessionid String representing the session URL
 * \param id_session ID of the session
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_job_retrieve_session_id(ophidiadb * oDB, char *sessionid, int *id_session);

/**
 * \brief Function to retrieve the ID of a job
 * \param oDB Pointer to OphidiaDB
 * \param sessionid String representing the session URL
 * \param markerid String representing the markerID
 * \param id_job ID of the job
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_job_retrieve_job_id(ophidiadb * oDB, char *sessionid, char *markerid, int *id_job);

/**
 * \brief Function to update the session table
 * \param oDB Pointer to OphidiaDB
 * \param sessionid String representing the session URL
 * \param username Name of a user
 * \param id_folder ID of the session folder
 * \param id_session ID of the new session
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_job_update_session_table(ophidiadb * oDB, char *sessionid, char *username, int id_folder, int *id_session);

/**
 * \brief Function to update the job table
 * \param oDB Pointer to OphidiaDB
 * \param markerid String representing the markerID
 * \param task_string String representing the submitted task
 * \param status Status of the task
 * \param username Name of a user
 * \param id_session ID of the session
 * \param id_job ID of the new job
 * \param parentid ID of the parent job
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_job_update_job_table(ophidiadb * oDB, char *markerid, char *task_string, char *status, char *username, int id_session, int *id_job, char *parentid);

#endif				/* __OPH_ODB_JOB_LIBRARY__ */
