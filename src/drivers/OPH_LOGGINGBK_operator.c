/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

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
#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include "drivers/OPH_LOGGINGBK_operator.h"
#include "oph_analytics_operator_library.h"
#include "oph_json_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

int retrieve_logging_info(ophidiadb * oDB, OPH_LOGGINGBK_operator_handle * handle, unsigned long **field_lengths, int *num_fields, MYSQL_RES ** logging_info)
{
	*logging_info = NULL;
	*field_lengths = NULL;

	if (!oDB || !handle || !field_lengths || !logging_info || !num_fields) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	if (oph_odb_check_connection_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_ODB_MYSQL_ERROR;
	}

	char query[MYSQL_BUFLEN];
	char parent_clause[OPH_COMMON_BUFFER_LEN];
	int nlines = handle->nlines;
	if (handle->parent_job_id_filter)
		snprintf(parent_clause, OPH_COMMON_BUFFER_LEN, MYSQL_QUERY_OPH_LOGGINGBK_PARENT_ID, handle->parent_job_id_filter);

	//Fill the right query according to session,marker and mask with all filters
	switch (handle->session) {
		case 0:
			switch (handle->marker) {
				case 0:
					snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_00000, handle->user, handle->session_filter, handle->session_label_filter,
						 handle->session_creation_date1, handle->session_creation_date2, nlines);
					break;
				case 1:
					switch (handle->mask[0]) {
						case '0':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01000, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01001, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01010, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01011, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						case '1':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01100, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01101, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01110, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_01111, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid first mask value %c.\n", handle->mask[0]);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					break;
				case 2:
					switch (handle->mask[0]) {
						case '0':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02000, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02001, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02010, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02011, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						case '1':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02100, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02101, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02110, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_02111, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid first mask value %c.\n", handle->mask[0]);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid marker value %d.\n", handle->marker);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}
			break;
		case 1:
			switch (handle->marker) {
				case 0:
					snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_10000, handle->user, handle->session_filter, handle->session_label_filter,
						 handle->session_creation_date1, handle->session_creation_date2, nlines);
					break;
				case 1:
					switch (handle->mask[0]) {
						case '0':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11000, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11001, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11010, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11011, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						case '1':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11100, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11101, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11110, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_11111, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid first mask value %c.\n", handle->mask[0]);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					break;
				case 2:
					switch (handle->mask[0]) {
						case '0':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12000, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12001, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12010, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12011, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						case '1':
							switch (handle->mask[1]) {
								case '0':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12100, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12101, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								case '1':
									switch (handle->mask[2]) {
										case '0':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12110, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										case '1':
											snprintf(query, MYSQL_BUFLEN, MYSQL_QUERY_OPH_LOGGINGBK_12111, handle->user, handle->session_filter,
												 handle->session_label_filter, handle->session_creation_date1, handle->session_creation_date2,
												 handle->workflow_id_filter, handle->marker_id_filter, handle->job_submission_date1,
												 handle->job_submission_date2, handle->job_status, handle->submission_string_filter,
												 handle->job_start_date1, handle->job_start_date2, handle->job_end_date1, handle->job_end_date2,
												 (handle->parent_job_id_filter ? parent_clause : ""), nlines);
											break;
										default:
											pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid third mask value %c.\n", handle->mask[2]);
											return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
									}
									break;
								default:
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid second mask value %c.\n", handle->mask[1]);
									return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
							}
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid first mask value %c.\n", handle->mask[0]);
							return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid marker value %d.\n", handle->marker);
					return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
			}
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid session value %d.\n", handle->session);
			return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}
	//End of query processing

	//Execute query
	if (mysql_query(oDB->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(oDB->conn));
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	// Init res
	*logging_info = mysql_store_result(oDB->conn);

	int i;
	MYSQL_FIELD *fields;
	*num_fields = mysql_num_fields(*logging_info);
	*field_lengths = (unsigned long *) malloc((*num_fields) * sizeof(unsigned long));
	if (!(*field_lengths)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	fields = mysql_fetch_fields(*logging_info);

	for (i = 0; i < *num_fields; i++) {
		(*field_lengths)[i] = fields[i].max_length;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_TASK_TABLE);
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_HANDLE_ALREADY_INITIALIZED);
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_LOGGINGBK_operator_handle *) calloc(1, sizeof(OPH_LOGGINGBK_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session = -1;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker = -1;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->nlines = OPH_LOGGINGBK_DEFAULT_NLINES;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	ophidiadb *oDB = &((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->oDB;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_init_ophidiadb(oDB);

	if (oph_odb_read_ophidiadb_config_file(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_OPHIDIADB_CONFIGURATION_FILE_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(oDB)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_OPHIDIADB_CONNECTION_ERROR_NO_CONTAINER);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//3 - Fill struct with the correct data

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, &((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SESSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SESSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MARKER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MARKER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MARKER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MASK);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MASK);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MASK);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->mask, OPH_LOGGINGBK_MASK_LEN + 1, "%s", value);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SESSION_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SESSION_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SESSION_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SESSION_LABEL_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SESSION_LABEL_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SESSION_LABEL_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0 && strcasecmp(value, "null") != 0) {
		char tmp_session_label_filter[OPH_COMMON_BUFFER_LEN];
		memset(tmp_session_label_filter, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(tmp_session_label_filter, OPH_COMMON_BUFFER_LEN, MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL2, value);
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter = (char *) strdup(tmp_session_label_filter))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_LABEL_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else if (strcmp(value, OPH_COMMON_ALL_FILTER) == 0) {
		char tmp_session_label_filter[OPH_COMMON_BUFFER_LEN];
		memset(tmp_session_label_filter, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(tmp_session_label_filter, OPH_COMMON_BUFFER_LEN, MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL1);
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter = (char *) strdup(tmp_session_label_filter))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_LABEL_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		char tmp_session_label_filter[OPH_COMMON_BUFFER_LEN];
		memset(tmp_session_label_filter, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(tmp_session_label_filter, OPH_COMMON_BUFFER_LEN, MYSQL_QUERY_OPH_LOGGINGBK_SESSION_LABEL3);
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter = (char *) strdup(tmp_session_label_filter))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_LABEL_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SESSION_CREATION_DATE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SESSION_CREATION_DATE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SESSION_CREATION_DATE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *valuecopy, *cursor;
	valuecopy = (char *) strdup(value);
	if (!valuecopy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SESSION_CREATION_DATE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	char *savepointer = NULL;
	cursor = strtok_r(valuecopy, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
	if (valuecopy[0] == ',') {
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_START_DEF);
	} else {
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		cursor = strtok_r(NULL, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_creation_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
	}
	free(valuecopy);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_WORKFLOW_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_WORKFLOW_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_WORKFLOW_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_WORKFLOW_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_WORKFLOW_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MARKER_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MARKER_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MARKER_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_MARKER_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_MARKER_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PARENT_ID_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PARENT_ID_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_PARENT_ID_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_PARENT_ID_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter = NULL;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_JOB_SUBMISSION_DATE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_JOB_SUBMISSION_DATE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_JOB_SUBMISSION_DATE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	valuecopy = (char *) strdup(value);
	if (!valuecopy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_JOB_SUBMISSION_DATE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	cursor = strtok_r(valuecopy, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
	if (valuecopy[0] == ',') {
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_START_DEF);
	} else {
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		cursor = strtok_r(NULL, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_submission_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
	}
	free(valuecopy);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_JOB_STATUS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_JOB_STATUS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_JOB_STATUS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_JOB_STATUS);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_JOB_STATUS);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBMISSION_STRING_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBMISSION_STRING_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SUBMISSION_STRING_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter = (char *) strdup(value))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SUBMISSION_STRING_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	} else {
		if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter = (char *) strdup("%"))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_SUBMISSION_STRING_FILTER);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_JOB_START_DATE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_JOB_START_DATE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_JOB_START_DATE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	valuecopy = (char *) strdup(value);
	if (!valuecopy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_JOB_START_DATE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	cursor = strtok_r(valuecopy, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
	if (valuecopy[0] == ',') {
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_START_DEF);
	} else {
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		cursor = strtok_r(NULL, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_start_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
	}
	free(valuecopy);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_JOB_END_DATE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_JOB_END_DATE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_JOB_END_DATE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	valuecopy = (char *) strdup(value);
	if (!valuecopy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_IN_PARAM_JOB_END_DATE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	cursor = strtok_r(valuecopy, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
	if (valuecopy[0] == ',') {
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_START_DEF);
	} else {
		snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date1, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		cursor = strtok_r(NULL, OPH_LOGGINGBK_DATE_DELIMITER, &savepointer);
		if (!cursor) {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", OPH_LOGGINGBK_DATE_END_DEF);
		} else {
			snprintf(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_end_date2, OPH_LOGGINGBK_DATE_LEN + 1, "%s", cursor);
		}
	}
	free(valuecopy);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LOG_LINES_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LOG_LINES_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, "NO-CONTAINER", OPH_IN_PARAM_LOG_LINES_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->nlines = (int) strtol(value, NULL, 10);
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->nlines <= 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "nlines must be >0\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_BAD_LINES_NUMBER, OPH_IN_PARAM_LOG_LINES_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->user = (char *) strdup(value))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_MEMORY_ERROR_INPUT, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_INFO_START);

	ophidiadb *oDB = &((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->oDB;
	MYSQL_RES *logging_info = NULL;
	int num_fields;
	unsigned long *field_lengths = NULL;
	int num_rows = 0;

	//retrieve logging info
	if (retrieve_logging_info(oDB, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle), &field_lengths, &num_fields, &logging_info)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve logging info\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_RETRIEVE_LOGGING_ERROR);
		mysql_free_result(logging_info);
		if (field_lengths) {
			free(field_lengths);
			field_lengths = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Empty set
	if (!(num_rows = mysql_num_rows(logging_info))) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query\n");
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NO_ROWS_FOUND);
		mysql_free_result(logging_info);
		if (field_lengths) {
			free(field_lengths);
			field_lengths = NULL;
		}
		return OPH_ANALYTICS_OPERATOR_SUCCESS;
	}

	MYSQL_ROW row;
	int i, j, k, len;
	int total_len = 1;
	MYSQL_FIELD *fields;

	fields = mysql_fetch_fields(logging_info);

	int submission_string_flag = 0;

	//Build row_separator
	for (i = 0; i < num_fields; i++) {
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_SUBMISSION_STRING, strlen(OPH_LOGGINGBK_SUBMISSION_STRING))) {
			submission_string_flag = 1;
			continue;
		}
		len = (field_lengths[i] > fields[i].name_length) ? field_lengths[i] : fields[i].name_length;
		total_len += (len + 3);
	}

	char *row_separator;
	if (submission_string_flag && (total_len < OPH_LOGGINGBK_MIN_WIDTH)) {
		row_separator = (char *) malloc((OPH_LOGGINGBK_MIN_WIDTH + 1) * sizeof(char));
		memset(row_separator, 0, OPH_LOGGINGBK_MIN_WIDTH + 1);
	} else {
		row_separator = (char *) malloc((total_len + 1) * sizeof(char));
		memset(row_separator, 0, total_len + 1);
	}
	int *field_new_len = (int *) malloc(num_fields * sizeof(int));
	memset(field_new_len, 0, num_fields * sizeof(int));
	double proportion_factor = (double) OPH_LOGGINGBK_MIN_WIDTH / total_len;

	int count = 0;
	//Print row_separator into string and compute field lengths through proportion
	row_separator[count++] = '+';
	for (i = 0; i < num_fields; i++) {
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_SUBMISSION_STRING, strlen(OPH_LOGGINGBK_SUBMISSION_STRING)))
			continue;
		row_separator[count++] = '-';
		len = (field_lengths[i] > fields[i].name_length) ? field_lengths[i] : fields[i].name_length;
		if (submission_string_flag && (total_len < OPH_LOGGINGBK_MIN_WIDTH)) {
			field_new_len[i] = (int) ((len + 3) * proportion_factor) - 3;
		} else
			field_new_len[i] = len;
		for (j = 0; j < field_new_len[i]; j++) {
			row_separator[count++] = '-';
		}
		row_separator[count++] = '-';
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_JOB_ID, strlen(OPH_LOGGINGBK_JOB_ID)))
			row_separator[count++] = '-';
		else
			row_separator[count++] = '+';
	}

	//If total lenght is less than minimum allowed width, extend last field
	if (submission_string_flag && (total_len < OPH_LOGGINGBK_MIN_WIDTH)) {
		count = 1;
		for (i = 0; i < num_fields - 1; i++) {
			count += (field_new_len[i] + 3);
		}
		field_new_len[i - 1] += (OPH_LOGGINGBK_MIN_WIDTH - count);
		//Extend row_separator
		row_separator[OPH_LOGGINGBK_MIN_WIDTH - 1] = '+';
		for (j = 1; j <= (OPH_LOGGINGBK_MIN_WIDTH - count); j++) {
			row_separator[OPH_LOGGINGBK_MIN_WIDTH - j - 1] = '-';
		}
		total_len = OPH_LOGGINGBK_MIN_WIDTH;
	}
	//ALLOC FOR JSON START
	char **keys = NULL;
	char **fieldtypes = NULL;
	if (oph_json_is_objkey_printable
	    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
		keys = (char **) malloc(sizeof(char *) * num_fields);
		fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
	}
	//ALLOC FOR JSON END

	printf("%s\n", row_separator);
	//Build header
	printf("|");
	char tmp_header[OPH_COMMON_BUFFER_LEN];
	for (i = 0; i < num_fields; i++) {
		//SET VALUES FOR JSON START
		if (oph_json_is_objkey_printable
		    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
			if (keys)
				keys[i] = strdup(fields[i].name);
			if (fieldtypes) {
				if (!strcmp(fields[i].name, OPH_LOGGINGBK_JOB_ID) || !strcmp(fields[i].name, OPH_LOGGINGBK_MARKER_ID) || !strcmp(fields[i].name, OPH_LOGGINGBK_PARENT_JOB_ID)
				    || !strcmp(fields[i].name, OPH_LOGGINGBK_WORKFLOW_ID))
					fieldtypes[i] = strdup(OPH_JSON_INT);
				else
					fieldtypes[i] = strdup(OPH_JSON_STRING);
			}
		}
		//SET VALUES FOR JSON END
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_SUBMISSION_STRING, strlen(OPH_LOGGINGBK_SUBMISSION_STRING)))
			continue;
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_PARENT_JOB_ID, strlen(OPH_LOGGINGBK_PARENT_JOB_ID)))
			continue;
		if (!strncmp(fields[i].name, OPH_LOGGINGBK_JOB_ID, strlen(OPH_LOGGINGBK_JOB_ID)) && !strncmp(fields[i + 1].name, OPH_LOGGINGBK_PARENT_JOB_ID, strlen(OPH_LOGGINGBK_PARENT_JOB_ID))) {
			snprintf(tmp_header, OPH_COMMON_BUFFER_LEN, "%s (%s)", fields[i].name, fields[i + 1].name);
			printf(" %-*s |", field_new_len[i] + field_new_len[i + 1] + 3, tmp_header);
		} else
			printf(" %-*s |", field_new_len[i], fields[i].name);
	}
	printf("\n%s\n", row_separator);

	//ADD TO JSON START
	if (oph_json_is_objkey_printable
	    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
		if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_LOGGINGBK, "", NULL, keys, num_fields, fieldtypes, num_fields)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
			//FREE JSON VALUES START
			if (keys) {
				for (i = 0; i < num_fields; i++) {
					if (keys[i]) {
						free(keys[i]);
						keys[i] = NULL;
					}
				}
				free(keys);
				keys = NULL;
			}
			if (fieldtypes) {
				for (i = 0; i < num_fields; i++) {
					if (fieldtypes[i]) {
						free(fieldtypes[i]);
						fieldtypes[i] = NULL;
					}
				}
				free(fieldtypes);
				fieldtypes = NULL;
			}
			//FREE JSON VALUES END
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	//ADD TO JSON END

	//FREE JSON VALUES START
	if (oph_json_is_objkey_printable
	    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
		if (keys) {
			for (i = 0; i < num_fields; i++) {
				if (keys[i]) {
					free(keys[i]);
					keys[i] = NULL;
				}
			}
			free(keys);
			keys = NULL;
		}
		if (fieldtypes) {
			for (i = 0; i < num_fields; i++) {
				if (fieldtypes[i]) {
					free(fieldtypes[i]);
					fieldtypes[i] = NULL;
				}
			}
			free(fieldtypes);
			fieldtypes = NULL;
		}
	}
	//FREE JSON VALUES END

	//For each ROW
	char *tmp;
	int row_length = total_len - 4 - OPH_LOGGINGBK_SUBMISSION_STRLEN - 3;
	char *tmp_buffer = (char *) malloc((row_length + 1) * sizeof(char));

	//ALLOC FOR JSON START
	char **my_row = NULL;
	if (oph_json_is_objkey_printable
	    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
		my_row = (char **) malloc(sizeof(char *) * num_fields);
		if (!my_row) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
			free(tmp_buffer);
			free(row_separator);
			free(field_new_len);
			mysql_free_result(logging_info);
			if (field_lengths) {
				free(field_lengths);
				field_lengths = NULL;
			}
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}
	//ALLOC FOR JSON END

	while ((row = mysql_fetch_row(logging_info))) {
		//SET VALUES FOR JSON START
		if (oph_json_is_objkey_printable
		    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
			for (i = 0; i < num_fields; i++) {
				if (row[i])
					my_row[i] = strdup(row[i]);
				else
					my_row[i] = strdup("");
				if (!my_row[i]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Error allocating memory\n");
					free(tmp_buffer);
					free(row_separator);
					free(field_new_len);
					mysql_free_result(logging_info);
					if (field_lengths) {
						free(field_lengths);
						field_lengths = NULL;
					}
					int j;
					for (j = 0; j < i; j++)
						free(my_row[j]);
					free(my_row);
					return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				}
			}
		}
		//SET VALUES FOR JSON END
		//ADD ROW TO JSON START
		if (oph_json_is_objkey_printable
		    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
			if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_LOGGINGBK, my_row)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
				free(tmp_buffer);
				free(row_separator);
				free(field_new_len);
				mysql_free_result(logging_info);
				if (field_lengths) {
					free(field_lengths);
					field_lengths = NULL;
				}
				int j;
				for (j = 0; j < num_fields; j++)
					free(my_row[j]);
				free(my_row);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		//ADD ROW TO JSON END

		//FREE JSON VALUES START
		if (oph_json_is_objkey_printable
		    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
			for (i = 0; i < num_fields; i++)
				free(my_row[i]);
		}
		//FREE JSON VALUES END

		for (i = 0; i < num_fields; i++) {
			if (!strncmp(fields[i].name, OPH_LOGGINGBK_SUBMISSION_STRING, strlen(OPH_LOGGINGBK_SUBMISSION_STRING))) {
				printf("\n%s\n", row_separator);

				if (!row[i]) {
					printf("| %-*s | %-*s |\n", OPH_LOGGINGBK_SUBMISSION_STRLEN, OPH_LOGGINGBK_SUBMISSION_STRING, row_length, "-");
				} else {
					len = strlen(row[i]);
					tmp = row[i];
					for (k = 0; k < len; k += row_length) {
						memset(tmp_buffer, 0, row_length + 1);
						strncpy(tmp_buffer, tmp, row_length);
						tmp += row_length;
						if (k == 0)
							printf("| %-*s | %-*s |\n", OPH_LOGGINGBK_SUBMISSION_STRLEN, OPH_LOGGINGBK_SUBMISSION_STRING, row_length, tmp_buffer);
						else
							printf("| %-*s | %-*s |\n", OPH_LOGGINGBK_SUBMISSION_STRLEN, " ", row_length, tmp_buffer);
					}
				}

			} else {
				if (!strncmp(fields[i].name, OPH_LOGGINGBK_PARENT_JOB_ID, strlen(OPH_LOGGINGBK_PARENT_JOB_ID)))
					continue;
				if (!i)
					printf("|");
				if (!strncmp(fields[i].name, OPH_LOGGINGBK_JOB_ID, strlen(OPH_LOGGINGBK_JOB_ID))
				    && !strncmp(fields[i + 1].name, OPH_LOGGINGBK_PARENT_JOB_ID, strlen(OPH_LOGGINGBK_PARENT_JOB_ID))) {
					if (row[i + 1])
						snprintf(tmp_header, OPH_COMMON_BUFFER_LEN, "%s (%s)", row[i] ? row[i] : "-", row[i + 1]);
					else
						snprintf(tmp_header, OPH_COMMON_BUFFER_LEN, "%s", row[i] ? row[i] : "-");
					printf(" %-*s |", field_new_len[i] + field_new_len[i + 1] + 3, tmp_header);
				} else
					printf(" %-*s |", field_new_len[i], row[i] ? row[i] : "-");
				if (i == num_fields - 1)
					printf("\n");
			}

		}
		printf("%s\n", row_separator);
	}
	//FREE JSON VALUES START
	if (oph_json_is_objkey_printable
	    (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_LOGGINGBK)) {
		free(my_row);
	}
	//FREE JSON VALUES END
	free(tmp_buffer);
	free(row_separator);
	free(field_new_len);

	mysql_free_result(logging_info);
	if (field_lengths) {
		free(field_lengths);
		field_lengths = NULL;
	}

	logging(LOG_INFO, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_INFO_END);
	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_LOGGINGBK_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	oph_odb_free_ophidiadb(&((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->oDB);

	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->job_status = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_label_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->workflow_id_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->marker_id_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->parent_job_id_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->submission_string_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->session_filter = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->user);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys, ((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_LOGGINGBK_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}

	free((OPH_LOGGINGBK_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
