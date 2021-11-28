/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#ifndef __OPH_INPUT_PARAMETERS_H
#define __OPH_INPUT_PARAMETERS_H

#define OPH_IN_PARAM_OPERATOR_NAME				"operator"
#define OPH_IN_PARAM_CONTAINER_INPUT				"container"
#define OPH_IN_PARAM_CONTAINER_PID				"container_pid"
#define OPH_IN_PARAM_DATACUBE_INPUT				"cube"
#define OPH_IN_PARAM_DATACUBE_INPUT_2				"cube2"
#define OPH_IN_PARAM_DATACUBE_MULTI_INPUT			"cubes"
#define OPH_IN_PARAM_SCHEDULE_ALGORITHM				"schedule"
#define OPH_IN_PARAM_COMPRESSION				"compressed"
#define OPH_IN_PARAM_SLEEP_TIME					"time"
#define OPH_IN_PARAM_PRINTF_TEXT				"text"
#define OPH_IN_PARAM_APPLY_QUERY				"query"
#define OPH_IN_PARAM_APPLY_DIM_QUERY				"dim_query"
#define OPH_IN_PARAM_HOST_NUMBER				"nhost"
#define OPH_IN_PARAM_DBMS_NUMBER				"ndbms"
#define OPH_IN_PARAM_DB_NUMBER					"ndb"
#define OPH_IN_PARAM_FRAGMENENT_NUMBER				"nfrag"
#define OPH_IN_PARAM_TUPLE_NUMBER				"ntuple"
#define OPH_IN_PARAM_ARRAY_LENGTH				"array_length"
#define OPH_IN_PARAM_MEASURE_NAME				"measure"
#define OPH_IN_PARAM_MEASURE_TYPE				"measure_type"
#define OPH_IN_PARAM_CHECK_TYPE					"check_type"
#define OPH_IN_PARAM_AUTH_FILE_PATH				"auth_path"
#define OPH_IN_PARAM_SRC_FILE_PATH				"src_path"
#define OPH_IN_PARAM_DST_FILE_PATH				"dst_path"
#define OPH_IN_PARAM_DIMENSION_NUMBER				"ndim"
#define OPH_IN_PARAM_DIMENSION_NAME				"dim"
#define OPH_IN_PARAM_DIMENSION_TYPE				"dim_type"
#define OPH_IN_PARAM_DIMENSION_SIZE				"dim_size"
#define OPH_IN_PARAM_DIMENSION_LEVEL				"concept_level"
#define OPH_IN_PARAM_EI_DIMENSION_LEVEL				"dim_level"
#define OPH_IN_PARAM_EI_DIMENSION_TYPE				"dim_array"
#define OPH_IN_PARAM_REDUCTION_OPERATION			"operation"
#define OPH_IN_PARAM_REDUCTION_SIZE				"group_size"
#define OPH_IN_PARAM_CUBEIO_HIERARCHY_DIRECTION			"branch"
#define OPH_IN_PARAM_SPLIT_IN_FRAGMENTS				"nsplit"
#define OPH_IN_PARAM_MERGE_ON_FRAGMENTS				"nmerge"
#define OPH_IN_PARAM_FRAGMENT_INPUT				"frag"
#define OPH_IN_PARAM_VISUALIZZATION_LEVEL			"level"
#define OPH_IN_PARAM_CONTAINER_NAME_FILTER			"container_filter"
#define OPH_IN_PARAM_DATACUBE_NAME_FILTER			"cube_filter"
#define OPH_IN_PARAM_OPERATOR_NAME_FILTER			"operator_filter"
#define OPH_IN_PARAM_HOSTNAME_FILTER				"host_filter"
#define OPH_IN_PARAM_DBMS_ID_FILTER				"dbms_filter"
#define OPH_IN_PARAM_DB_NAME_FILTER				"db_filter"
#define OPH_IN_PARAM_IOSERVER_TYPE_FILTER			"ioserver_filter"
#define OPH_IN_PARAM_LIMIT_FILTER				"limit_filter"
#define OPH_IN_PARAM_ROW_FILTER					"row_filter"
#define OPH_IN_PARAM_ARRAY_FILTER				"array_filter"
#define OPH_IN_PARAM_SUBSET_FILTER				"subset_filter"
#define OPH_IN_PARAM_SUBSET_DIMENSIONS				"subset_dims"
#define OPH_IN_PARAM_TIME_FILTER				"time_filter"
#define OPH_IN_PARAM_EXPLICIT_DIMENSION_NUMBER			"exp_ndim"
#define OPH_IN_PARAM_IMPLICIT_DIMENSION_NUMBER			"imp_ndim"
#define OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME			"exp_dim"
#define OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME			"imp_dim"
#define OPH_IN_PARAM_EXPLICIT_DIMENSION_LEVEL			"exp_dim_level"
#define OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL		"exp_concept_level"
#define OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL		"imp_concept_level"
#define OPH_IN_PARAM_IMPLICIT_PERMUTATION_ORDER			"dim_pos"
#define OPH_IN_PARAM_PUBLISH2_CONTENT				"content"
#define OPH_IN_PARAM_PRIMITIVE_NAME_FILTER			"primitive_filter"
#define OPH_IN_PARAM_PRIMITIVE_RETURN_TYPE_FILTER		"return_type"
#define OPH_IN_PARAM_PRIMITIVE_TYPE_FILTER			"primitive_type"
#define OPH_IN_PARAM_LOGGING_TYPE				"log_type"
#define OPH_IN_PARAM_CONTAINER_ID_FILTER			"container_id"
#define OPH_IN_PARAM_LOG_LINES_NUMBER				"nlines"
#define OPH_IN_PARAM_SHOW_ID					"show_id"
#define OPH_IN_PARAM_SHOW_INDEX					"show_index"
#define OPH_IN_PARAM_SHOW_TIME					"show_time"
#define OPH_IN_PARAM_CWD					"cwd"
#define OPH_IN_PARAM_CDD					"cdd"
#define OPH_IN_PARAM_HIDDEN					"hidden"
#define OPH_IN_PARAM_DELETE_TYPE				"delete_type"
#define OPH_IN_PARAM_HOST_STATUS				"host_status"
#define OPH_IN_PARAM_RECURSIVE_SEARCH				"recursive"
#define OPH_IN_PARAM_PARTITION_NAME				"host_partition"
#define OPH_IN_PARAM_IOSERVER_TYPE				"ioserver"
#define OPH_IN_PARAM_IMPORTDIM_GRID_NAME			"grid"
#define OPH_IN_PARAM_CHECK_GRID					"check_grid"
#define OPH_IN_PARAM_VOCABULARY					"vocabulary"
#define OPH_IN_PARAM_IMPORT_METADATA				"import_metadata"
#define OPH_IN_PARAM_EXPORT_METADATA				"export_metadata"
#define OPH_IN_PARAM_SESSION					"session_level"
#define OPH_IN_PARAM_MARKER					"job_level"
#define OPH_IN_PARAM_MASK					"mask"
#define OPH_IN_PARAM_SESSION_FILTER				"session_filter"
#define OPH_IN_PARAM_SESSION_LABEL_FILTER			"session_label_filter"
#define OPH_IN_PARAM_SESSION_CREATION_DATE			"session_creation_filter"
#define OPH_IN_PARAM_MARKER_ID_FILTER				"markerid_filter"
#define OPH_IN_PARAM_WORKFLOW_ID_FILTER				"workflowid_filter"
#define OPH_IN_PARAM_JOB_SUBMISSION_DATE			"job_creation_filter"
#define OPH_IN_PARAM_JOB_STATUS					"job_status_filter"
#define OPH_IN_PARAM_JOB_START_DATE				"job_start_filter"
#define OPH_IN_PARAM_JOB_END_DATE				"job_end_filter"
#define OPH_IN_PARAM_PARENT_ID_FILTER				"parent_job_filter"
#define OPH_IN_PARAM_FUNCTION_NAME				"function"
#define OPH_IN_PARAM_FUNCTION_VERSION				"function_version"
#define OPH_IN_PARAM_FUNCTION_TYPE				"function_type"
#define OPH_IN_PARAM_HIERARCHY_NAME				"hierarchy"
#define OPH_IN_PARAM_HIERARCHY_VERSION				"hierarchy_version"
#define OPH_IN_PARAM_MODE					"mode"
#define OPH_IN_PARAM_METADATA_KEY				"metadata_key"
#define OPH_IN_PARAM_METADATA_ID				"metadata_id"
#define OPH_IN_PARAM_METADATA_TYPE				"metadata_type"
#define OPH_IN_PARAM_METADATA_VARIABLE				"variable"
#define OPH_IN_PARAM_METADATA_VARIABLE_FILTER	"variable_filter"
#define OPH_IN_PARAM_METADATA_VALUE				"metadata_value"
#define OPH_IN_PARAM_METADATA_TYPE_FILTER			"metadata_type_filter"
#define OPH_IN_PARAM_METADATA_VALUE_FILTER			"metadata_value_filter"
#define OPH_IN_PARAM_METADATA_KEY_FILTER			"metadata_key_filter"
#define OPH_IN_PARAM_PATH					"path"
#define OPH_IN_PARAM_DATA_PATH					"dpath"
#define OPH_IN_PARAM_OBJKEY_FILTER				"objkey_filter"
#define OPH_IN_PARAM_SUBSET_FILTER_TYPE				"subset_type"
#define OPH_IN_PARAM_OUTPUT_PATH				"output_path"
#define OPH_IN_PARAM_OUTPUT_NAME				"output_name"
#define OPH_IN_PARAM_FITS_HDU                                   "hdu"
#define OPH_IN_PARAM_LINK					"link"
#define OPH_IN_PARAM_BYTE_UNIT					"byte_unit"
#define OPH_IN_PARAM_ALGORITHM					"algorithm"
#define OPH_IN_PARAM_MEASURE_FILTER				"measure_filter"
#define OPH_IN_PARAM_LEVEL_FILTER				"ntransform"
#define OPH_IN_PARAM_SRC_FILTER					"src_filter"
#define OPH_IN_PARAM_SIMULATE_RUN				"run"
#define OPH_IN_PARAM_CHECK_EXP_DIM				"check_exp_dim"
#define OPH_IN_PARAM_SUBMISSION_STRING_FILTER			"submission_string_filter"
#define OPH_IN_PARAM_SINGLE_FILE				"single_file"
#define OPH_IN_PARAM_BASE_TIME					"base_time"
#define OPH_IN_PARAM_UNITS					"units"
#define OPH_IN_PARAM_CALENDAR					"calendar"
#define OPH_IN_PARAM_MONTH_LENGTHS				"month_lengths"
#define OPH_IN_PARAM_LEAP_YEAR					"leap_year"
#define OPH_IN_PARAM_LEAP_MONTH					"leap_month"
#define OPH_IN_PARAM_MIDNIGHT					"midnight"
#define OPH_IN_PARAM_CHECK_COMPLIANCE				"check_compliance"
#define OPH_IN_PARAM_FORCE					"force"
#define OPH_IN_PARAM_IMPLICIT_POINTS				"imp_num_points"
#define OPH_IN_PARAM_WAVELET					"wavelet"
#define OPH_IN_PARAM_WAVELET_RATIO				"wavelet_ratio"
#define OPH_IN_PARAM_WAVELET_COEFF				"wavelet_coeff"
#define OPH_IN_PARAM_STATS					"show_stats"
#define OPH_IN_PARAM_FIT					"show_fit"
#define OPH_IN_PARAM_ORDER					"order"
#define OPH_IN_PARAM_OFFSET					"offset"
#define OPH_IN_PARAM_MISC					"misc"
#define OPH_IN_PARAM_DESCRIPTION				"description"
#define OPH_IN_PARAM_BASE64					"base64"
#define OPH_IN_PARAM_LIST					"list"
#define OPH_IN_PARAM_MISSINGVALUE				"missingvalue"
#define OPH_IN_PARAM_DIM_OFFSET					"dim_offset"
#define OPH_IN_PARAM_DIM_CONTINUE				"dim_continue"
#define OPH_IN_PARAM_HOLD_VALUES				"hold_values"
#define OPH_IN_PARAM_NUMBER					"number"
#define OPH_IN_PARAM_FILE					"file"
#define OPH_IN_PARAM_DEPTH					"depth"
#define OPH_IN_PARAM_REALPATH					"realpath"
#define OPH_IN_PARAM_ON_REDUCE					"on_reduce"
#define OPH_IN_PARAM_ACTION					"action"
#define OPH_IN_PARAM_POLICY					"policy"
#define OPH_IN_PARAM_COMMAND				"command"

#define OPH_IN_PARAM_INPUT					"input"
#define OPH_IN_PARAM_OUTPUT					"output"

#define OPH_IN_PARAM_SCRIPT					"script"
#define OPH_IN_PARAM_ARGS					"args"
#define OPH_IN_PARAM_STDOUT					"stdout"
#define OPH_IN_PARAM_STDERR					"stderr"
#define OPH_IN_PARAM_SPACE					"space"
#define OPH_IN_PARAM_SKIP_OUTPUT			"skip_output"
#define OPH_IN_PARAM_MULTIPLE_OUTPUT		"multiple_output"

#define OPH_ARG_NTHREAD						"nthreads"
#define OPH_ARG_USERNAME					"username"
#define OPH_ARG_SESSIONID					"sessionid"
#define OPH_ARG_WORKFLOWID					"workflowid"
#define OPH_ARG_MARKERID					"markerid"
#define OPH_ARG_IDJOB						"jobid"
#define OPH_ARG_PARENTID					"parentid"
#define OPH_ARG_TASKINDEX					"taskindex"
#define OPH_ARG_LIGHTTASKINDEX					"lighttaskindex"
#define OPH_ARG_STATUS						"status"
#define OPH_ARG_USERROLE					"userrole"
#define OPH_ARG_SAVE						"save"

#endif				//__OPH_INPUT_PARAMETERS_H
