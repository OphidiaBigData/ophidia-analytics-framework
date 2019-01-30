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

#ifndef __OPH_LOGGINGBK_OPERATOR_H
#define __OPH_LOGGINGBK_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_LOGGINGBK_DEFAULT_NLINES 100

/* OPERATOR MYSQL QUERIES */
//Limit filters
#define MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL "SELECT * FROM ("
//Filter queries
#define MYSQL_QUERY_OPH_LOGGINGBK_PARENT_ID "AND (jobaccounting.idparent LIKE '%s')"

#define MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL1 "1"	// "(session.label LIKE '%%' OR session.label IS NULL)" // TODO: check current validity
#define MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL2 "session.label LIKE '%s'"
#define MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL3 "session.label IS NULL"

#define MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS " AND session.sessionid LIKE '%s' AND %s AND session.creationdate >= '%s' AND session.creationdate <= '%s' AND jobaccounting.workflowid LIKE '%s' AND jobaccounting.markerid LIKE '%s' AND jobaccounting.creationdate >= '%s' AND jobaccounting.creationdate <= '%s' AND jobaccounting.status LIKE '%s' AND jobaccounting.submissionstring LIKE '%s' AND ((jobaccounting.timestart >= '%s' AND jobaccounting.timestart <= '%s') OR (jobaccounting.timestart IS NULL)) AND ((jobaccounting.timeend >= '%s' AND jobaccounting.timeend <= '%s') OR (jobaccounting.timeend IS NULL)) %s"

#define MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_NO_JOBS   " AND session.sessionid LIKE '%s' AND %s AND session.creationdate >= '%s' AND session.creationdate <= '%s'"

//Select queries
#define MYSQL_QUERY_OPH_LOGGINGBK_00000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT DISTINCT session.sessionid AS Session_ID,session.label AS Session_Label FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_NO_JOBS" ORDER BY session.sessionid DESC LIMIT %d)tmp ORDER BY tmp.Session_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01001 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01010 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01011 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01100 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01101 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01110 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_01111 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02001 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.creationdate DESC, jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_Submission_Date, tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02010 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02011 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.creationdate DESC, jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_Submission_Date, tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02100 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02101 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.creationdate DESC, jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_Submission_Date, tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02110 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_02111 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.creationdate DESC, jobaccounting.idjob DESC,jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date,tmp.Job_Submission_Date, tmp.Job_ID,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_10000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT DISTINCT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_NO_JOBS" ORDER BY session.creationdate DESC LIMIT %d)tmp ORDER BY tmp.Session_Creation_Date ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11001 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11010 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11011 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11100 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11101 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11110 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_11111 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12000 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12001 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID,tmp.Job_Submission_Date, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12010 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12011 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID,tmp.Job_Submission_Date, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12100 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12101 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID,tmp.Job_Submission_Date, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12110 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_Submission_Date, tmp.Job_ID, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"

#define MYSQL_QUERY_OPH_LOGGINGBK_12111 MYSQL_QUERY_OPH_LOGGINGBK_SELECT_ALL"SELECT session.sessionid AS Session_ID,session.label AS Session_Label,session.creationdate AS Session_Creation_Date,jobaccounting.idjob AS Job_ID,jobaccounting.idparent AS Parent_Job_ID,jobaccounting.workflowid AS Workflow_ID,jobaccounting.markerid AS Marker_ID,jobaccounting.creationdate AS Job_Submission_Date,jobaccounting.status AS Job_Status,jobaccounting.timestart AS Job_Start_Date,jobaccounting.timeend AS Job_End_Date,jobaccounting.submissionstring AS Job_Submission_String FROM user,jobaccounting,session WHERE user.iduser=jobaccounting.iduser AND jobaccounting.idsession=session.idsession AND user.username LIKE '%s'"MYSQL_QUERY_OPH_LOGGINGBK_FILTERS_WITH_JOBS" ORDER BY jobaccounting.timeend DESC,jobaccounting.timestart DESC,jobaccounting.creationdate DESC, jobaccounting.idjob DESC, session.creationdate DESC, jobaccounting.markerid DESC LIMIT %d)tmp ORDER BY tmp.Job_End_Date,tmp.Job_Start_Date, tmp.Job_ID,tmp.Job_Submission_Date, tmp.Session_Creation_Date,tmp.Marker_ID ASC;"


#define OPH_LOGGINGBK_MASK_LEN 3
#define OPH_LOGGINGBK_DATE_LEN 19
#define OPH_LOGGINGBK_DATE_DELIMITER ","
#define OPH_LOGGINGBK_DATE_START_DEF "1900-01-01 00:00:00"
#define OPH_LOGGINGBK_DATE_END_DEF "2100-01-01 00:00:00"

#define OPH_LOGGINGBK_SUBMISSION_STRING "Job_Submission_String"
#define OPH_LOGGINGBK_PARENT_JOB_ID     "Parent_Job_ID"
#define OPH_LOGGINGBK_JOB_ID            "Job_ID"
#define OPH_LOGGINGBK_MARKER_ID         "Marker_ID"
#define OPH_LOGGINGBK_WORKFLOW_ID       "Workflow_ID"
#define OPH_LOGGINGBK_SUBMISSION_STRLEN 21
#define OPH_LOGGINGBK_PARENT_JOB_STRLEN 13
#define OPH_LOGGINGBK_JOB_STRLEN        6
#define OPH_LOGGINGBK_MIN_WIDTH         100


/**
 * \brief Structure of parameters needed by the operator OPH_LOGGINGBK. It shows info about jobs and sessions (logging bookkeeping).
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param session: 0 (sessionid (+ session label) - default) or 1 (sessionid (+ session label) + session creation date)
 * \param marker: 0 (nothing - default) or 1 (job id (+ parent job id) + workflow id + marker id) or 2 (job id (+ parent job id) + workflow id + marker id + job submission date)
 * \param mask: 3-digit mask, considered if marker!=0, according to the following conventions:
                1st digit: 0 (nothing - default) or 1 (job status)
                2nd digit: 0 (nothing - default) or 1 (submission string)
                3rd digit: 0 (nothing - default) or 1 (job start and end date)
                mask examples:
                000 - nothing (default)
                100 - job status
                010 - submission string
                001 - job start and end date
                110 - job status + submission string
 * \param session_filter: filter on a particular sessionID
 * \param session_label_filter: filter on a particular session label
 * \param session_creation_date1: filter on session's creation date (session_creation_date1 <= date <= session_creation_date2)
                                    possible dates:
                                    yyyy
                                    yyyy-mm
                                    yyyy-mm-dd
                                    yyyy-mm-dd hh
                                    yyyy-mm-dd hh:mm
                                    yyyy-mm-dd hh:mm:ss
                                    default values:
                                    date1   1900-01-01 00:00:00
                                    date2   2100-01-01 00:00:00
                                    example:
                                    "1900-01-01 00:00:00,2100-01-01 00:00:00"
 * \param session_creation_date2: filter on session's creation date (session_creation_date1 <= date <= session_creation_date2)
 * \param workflow_id_filter: filter on a particular workflow ID.
 * \param marker_id_filter: filter on a particular markerID
 * \param parent_job_id_filter: filter on a particular parent job ID
 * \param job_submission_date1: filter on job submission date as with session_creation_date1
 * \param job_submission_date2: filter on job submission date as with session_creation_date2
 * \param job_status: filter on job status
 * \param submission_string_filter: filter on submission string
 * \param job_start_date1: filter on job's start date as with session_creation_date1
 * \param job_start_date2: filter on job's start date as with session_creation_date2
 * \param job_end_date1: filter on job's end date as with session_creation_date1
 * \param job_end_date2: filter on job's end date as with session_creation_date2
 * \param user: username of the user running the operator
 * \param nlines Number of lines to be displayed from the end of the log.
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_LOGGINGBK_operator_handle {
	ophidiadb oDB;
	int session;
	int marker;
	char mask[OPH_LOGGINGBK_MASK_LEN + 1];
	char *session_filter;
	char *session_label_filter;
	char session_creation_date1[OPH_LOGGINGBK_DATE_LEN + 1];
	char session_creation_date2[OPH_LOGGINGBK_DATE_LEN + 1];
	char *workflow_id_filter;
	char *marker_id_filter;
	char *parent_job_id_filter;
	char job_submission_date1[OPH_LOGGINGBK_DATE_LEN + 1];
	char job_submission_date2[OPH_LOGGINGBK_DATE_LEN + 1];
	char *job_status;
	char *submission_string_filter;
	char job_start_date1[OPH_LOGGINGBK_DATE_LEN + 1];
	char job_start_date2[OPH_LOGGINGBK_DATE_LEN + 1];
	char job_end_date1[OPH_LOGGINGBK_DATE_LEN + 1];
	char job_end_date2[OPH_LOGGINGBK_DATE_LEN + 1];
	char *user;
	int nlines;
	char **objkeys;
	int objkeys_num;
} OPH_LOGGINGBK_operator_handle;

#endif				//__OPH_LOGGINGBK_OPERATOR_H
