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

#ifndef __OPH_COMMON_H
#define __OPH_COMMON_H

#define UNUSED(x)				{(void)(x);}

//Macro used for strncmp and strncasecmp
#define STRLEN_MAX(a,b)				(strlen(a) > strlen(b) ? strlen(a) : strlen(b))

#define QUERY_BUFLEN				1048576

//MYSQL DEFINES
#define MYSQL_BUFLEN				65536

#define MYSQL_FRAG_ID				"id_dim"
#define MYSQL_FRAG_MEASURE			"measure"
#define MYSQL_DIMENSION				"dimension"

//Framework common defines and macros
#define OPH_COMMON_BUFFER_LEN			4096

#define OPH_COMMON_TRUE				1
#define OPH_COMMON_FALSE			0
#define OPH_COMMON_NAN				"NAN"

#define OPH_COMMON_DEFAULT_CONCEPT_LEVEL 	"c"
#define OPH_COMMON_BASE_CONCEPT_LEVEL 		'c'
#define OPH_COMMON_ALL_CONCEPT_LEVEL 		'A'
#define OPH_COMMON_DEFAULT_EMPTY_VALUE 		"-"
#define OPH_COMMON_DEFAULT_GRID 			OPH_COMMON_DEFAULT_EMPTY_VALUE
#define OPH_COMMON_DEFAULT_HIERARCHY 		"oph_base"

#define OPH_COMMON_CONCEPT_LEVEL_UNKNOWN	'-'

#define OPH_COMMON_FULL_REDUCED_DIM		"ALL"
#define OPH_COMMON_MAX_BYTE_LENGHT		4
#define OPH_COMMON_MAX_SHORT_LENGHT 	8
#define OPH_COMMON_MAX_INT_LENGHT 		16
#define OPH_COMMON_MAX_LONG_LENGHT 		24
#define OPH_COMMON_MAX_FLOAT_LENGHT 	48
#define OPH_COMMON_MAX_DOUBLE_LENGHT	64
#define OPH_COMMON_MAX_DATE_LENGHT		20

#define OPH_COMMON_BYTE_TYPE 			"byte"
#define OPH_COMMON_SHORT_TYPE 			"short"
#define OPH_COMMON_INT_TYPE 			"int"
#define OPH_COMMON_LONG_TYPE 			"long"
#define OPH_COMMON_FLOAT_TYPE 			"float"
#define OPH_COMMON_DOUBLE_TYPE 			"double"
#define OPH_COMMON_STRING_TYPE 			"string"
#define OPH_COMMON_BIT_TYPE				"bit"
#define OPH_COMMON_TYPE_SIZE			8
#define OPH_COMMON_DEFAULT_TYPE			OPH_COMMON_DOUBLE_TYPE

#define OPH_COMMON_BYTE_FLAG			'c'
#define OPH_COMMON_SHORT_FLAG			's'
#define OPH_COMMON_INT_FLAG				'i'
#define OPH_COMMON_LONG_FLAG			'l'
#define OPH_COMMON_FLOAT_FLAG			'f'
#define OPH_COMMON_DOUBLE_FLAG			'd'
#define OPH_COMMON_BIT_FLAG				'b'

#define OPH_COMMON_MB_UNIT				"MB"

#define OPH_COMMON_NCL_EXTENSION		".ncl"
#define OPH_COMMON_NC_EXTENSION			".nc"

#define OPH_COMMON_CURRENT_DIR			"."
#define OPH_COMMON_PARENT_DIR			".."

#define OPH_COMMON_LATEST_VERSION		"latest"

#define OPH_COMMON_IO_FS_DEFAULT		"auto"
#define OPH_COMMON_IO_FS_LOCAL 			"local"
#define OPH_COMMON_IO_FS_GLOBAL 		"global"
#define OPH_COMMON_IO_FS_DEFAULT_TYPE 		-1
#define OPH_COMMON_IO_FS_LOCAL_TYPE 		0
#define OPH_COMMON_IO_FS_GLOBAL_TYPE 		1

#define OPH_COMMON_HOSTPARTITION_DEFAULT	"auto"


#define OPH_COMMON_METADATA_TYPE_TEXT		"text"

#define OPH_COMMON_ALL_FILTER			"all"
#define OPH_COMMON_NONE_FILTER			"none"
#define OPH_COMMON_MAX_VALUE			"max"
#define OPH_COMMON_YES_VALUE			"yes"
#define OPH_COMMON_NO_VALUE				"no"
#define OPH_COMMON_NULL_VALUE			"null"
#define OPH_COMMON_AUTO_VALUE			"auto"
#define OPH_COMMON_GLOBAL_VALUE			"global"
#define OPH_COMMON_ONLY_VALUE			"only"

#define OPH_COMMON_UP_STATUS			"up"
#define OPH_COMMON_DOWN_STATUS			"down"

#define OPH_COMMON_ADMIN_USERNAME		"admin"

#define OPH_COMMON_TIME_HIERARCHY		"oph_time"

#define OPH_COMMON_RAND_ALGO_TEMP		"temperatures"
#define OPH_COMMON_RAND_ALGO_DEFAULT	"default"

#define OPH_COMMON_POLICY_RR			"rr"
#define OPH_COMMON_POLICY_PORT			"port"

// User roles
#define OPH_ROLE_NULL_STR			"-----"
#define OPH_ROLE_READ_STR			"read"
#define OPH_ROLE_WRITE_STR			"write"
#define OPH_ROLE_EXECUTE_STR			"execute"
#define OPH_ROLE_ADMIN_STR			"admin"
#define OPH_ROLE_OWNER_STR			"owner"
// Role values
#define OPH_ROLE_NONE				0
#define OPH_ROLE_READ				1
#define OPH_ROLE_WRITE				2
#define OPH_ROLE_EXECUTE			4
#define OPH_ROLE_ADMIN				8
#define OPH_ROLE_OWNER				16
// Dependening values
#define OPH_ROLE_WRITER				(OPH_ROLE_READ+OPH_ROLE_WRITE)
#define OPH_ROLE_ADMINISTRATOR		(OPH_ROLE_ADMIN+OPH_ROLE_EXECUTE+OPH_ROLE_WRITER)
#define OPH_ROLE_ALL				(OPH_ROLE_OWNER+OPH_ROLE_ADMINISTRATOR)
// Role bit positions
#define OPH_ROLE_READ_POS			0
#define OPH_ROLE_WRITE_POS			1
#define OPH_ROLE_EXECUTE_POS		2
#define OPH_ROLE_ADMIN_POS			3
#define OPH_ROLE_OWNER_POS			4
// Check for role presence
#define IS_OPH_ROLE_PRESENT(userrole,oph_role_pos)	((userrole) & (1<<(oph_role_pos)))

#define OPH_COMMON_FILLVALUE		"_FillValue"

#endif				//__OPH_COMMON_H
