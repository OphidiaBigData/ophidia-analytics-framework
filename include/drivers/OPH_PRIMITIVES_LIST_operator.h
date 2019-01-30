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

#ifndef __OPH_PRIMITIVES_LIST_OPERATOR_H
#define __OPH_PRIMITIVES_LIST_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_PRIMITIVES_LIST_HELP_MESSAGE "\nTo get additional information about a primitive defined above use the following:\n OPH_TERM: oph_man function_type=primitive;function=PRIMITIVENAME; \n SUBMISSION STRING: \"operator=oph_man;function_type=primitive;function=PRIMITIVENAME;\"\n"

/**
 * \brief Structure of parameters needed by the operator OPH_PRIMITIVES_LIST. It shows information about active UDF plugins
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param level Level of verbosity 1: only function's name, 2: name and return type, 3: name,return type,dylib , 4: name,return type,dylib,type, 5: name,return type,dylib,type,id_dbms
 * \param id_dbms ID of DBMS instance to filter on
 * \param func_ret Function return type to filter on (all | array | number)
 * \param func_type Function type to filter on (all | simple | aggregate)
 * \param name_filter Function name (or part of name with no wildcards) to filter on (no regex)
 * \param server Pointer to I/O server handler
 * \param limit Limit the number of displayed results
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_PRIMITIVES_LIST_operator_handle {
	ophidiadb oDB;
	int level;
	int id_dbms;
	char *func_ret;
	char *func_type;
	char *name_filter;
	int limit;
	oph_ioserver_handler *server;
	char **objkeys;
	int objkeys_num;
} OPH_PRIMITIVES_LIST_operator_handle;


#endif				//__PRIMITIVES_LIST_OPERATOR_H
