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

#ifndef __OPH_IOSERVER_SUBMISSION_QUERY_H
#define __OPH_IOSERVER_SUBMISSION_QUERY_H


#define OPH_IOSERVER_SQ_LEN				QUERY_BUFLEN

#define OPH_IOSERVER_SQ_BLOCK(A, B) A"="B";"

//*****************Submission query syntax***************//

#define OPH_IOSERVER_SQ_VALUE_SEPARATOR '='
#define OPH_IOSERVER_SQ_PARAM_SEPARATOR ';'
#define OPH_IOSERVER_SQ_MULTI_VALUE_SEPARATOR '|'
#define OPH_IOSERVER_SQ_STRING_DELIMITER '\''

//*****************Query operation***************//

#define OPH_IOSERVER_SQ_OPERATION "operation"
#define OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT "create_frag_select"
#define OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT_FILE "create_frag_select_file"
#define OPH_IOSERVER_SQ_OP_CREATE_FRAG "create_frag"
#define OPH_IOSERVER_SQ_OP_DROP_FRAG "drop_frag"
#define OPH_IOSERVER_SQ_OP_CREATE_DB "create_database"
#define OPH_IOSERVER_SQ_OP_DROP_DB "drop_database"
#define OPH_IOSERVER_SQ_OP_INSERT "insert"
#define OPH_IOSERVER_SQ_OP_MULTI_INSERT "multi_insert"
#define OPH_IOSERVER_SQ_OP_FILE_IMPORT 	"file_import"
#define OPH_IOSERVER_SQ_OP_INSERT_SELECT "insert_select"
#define OPH_IOSERVER_SQ_OP_RAND_IMPORT "random_import"
#define OPH_IOSERVER_SQ_OP_SELECT "select"
#define OPH_IOSERVER_SQ_OP_FUNCTION "function"

//*****************Query arguments***************//

#define OPH_IOSERVER_SQ_ARG_FINAL_STATEMENT "final_statement"
#define OPH_IOSERVER_SQ_ARG_FRAG "frag_name"
#define OPH_IOSERVER_SQ_ARG_COLUMN_NAME "column_name"
#define OPH_IOSERVER_SQ_ARG_COLUMN_TYPE "column_type"
#define OPH_IOSERVER_SQ_ARG_FIELD "field"
#define OPH_IOSERVER_SQ_ARG_FIELD_ALIAS "select_alias"
#define OPH_IOSERVER_SQ_ARG_FROM "from"
#define OPH_IOSERVER_SQ_ARG_FROM_ALIAS "from_alias"
#define OPH_IOSERVER_SQ_ARG_DB "db_name"
#define OPH_IOSERVER_SQ_ARG_GROUP "group"
#define OPH_IOSERVER_SQ_ARG_WHEREL "where_left"
#define OPH_IOSERVER_SQ_ARG_WHEREC "where_cond"
#define OPH_IOSERVER_SQ_ARG_WHERER "where_right"
#define OPH_IOSERVER_SQ_ARG_WHERE "where"
#define OPH_IOSERVER_SQ_ARG_ORDER "order"
#define OPH_IOSERVER_SQ_ARG_ORDER_DIR "order_dir"
#define OPH_IOSERVER_SQ_ARG_LIMIT "limit"
#define OPH_IOSERVER_SQ_ARG_VALUE "value"
#define OPH_IOSERVER_SQ_ARG_FUNC "func_name"
#define OPH_IOSERVER_SQ_ARG_ARG "arg"
#define OPH_IOSERVER_SQ_ARG_SEQUENTIAL  "sequential_id"
#define OPH_IOSERVER_SQ_ARG_PATH  	  	"src_path"
#define OPH_IOSERVER_SQ_ARG_MEASURE  	  "measure"
#define OPH_IOSERVER_SQ_ARG_COMPRESSED  "compressed"
#define OPH_IOSERVER_SQ_ARG_NROW  	  	"nrows"
#define OPH_IOSERVER_SQ_ARG_ROW_START   "row_start"
#define OPH_IOSERVER_SQ_ARG_DIM_TYPE    "dim_type"
#define OPH_IOSERVER_SQ_ARG_DIM_INDEX   "dim_index"
#define OPH_IOSERVER_SQ_ARG_DIM_START   "dim_start"
#define OPH_IOSERVER_SQ_ARG_DIM_END     "dim_end"
#define OPH_IOSERVER_SQ_ARG_MEASURE_TYPE    "measure_type"
#define OPH_IOSERVER_SQ_ARG_ARRAY_LEN 	"array_len"
#define OPH_IOSERVER_SQ_ARG_ALGORITHM 	"algorithm"

//*****************Query values***************//

#define OPH_IOSERVER_SQ_VAL_YES "yes"
#define OPH_IOSERVER_SQ_VAL_NO 	"no"

//*****************Keywords***************//

#define OPH_IOSERVER_SQ_KW_TABLE_SIZE "@tot_table_size"
#define OPH_IOSERVER_SQ_KW_INFO_SYSTEM "@info_system"
#define OPH_IOSERVER_SQ_KW_INFO_SYSTEM_TABLE "@info_system_table"
#define OPH_IOSERVER_SQ_KW_FUNCTION_FIELDS "@function_fields"
#define OPH_IOSERVER_SQ_KW_FUNCTION_TABLE "@function_table"
#define OPH_IOSERVER_SQ_KW_FILE "@file"

#endif				//__OPH_IOSERVER_SUBMISSION_QUERY_H
