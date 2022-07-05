/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2022 CMCC Foundation

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

#ifndef __OPH_OPERATORS_LIST_OPERATOR_H
#define __OPH_OPERATORS_LIST_OPERATOR_H

#include "oph_common.h"

#define OPH_OPERATORS_LIST_HELP_MESSAGE "\nTo get additional information about an operator defined above use the following:\n OPH_TERM: oph_man function_type=operator;function=OPERATORNAME; \n SUBMISSION STRING: \"operator=oph_man;function_type=operator;function=OPERATORNAME;\"\n"

/**
 * \brief Structure of parameters needed by the operator OPERATORS_LIST. It shows a list of active operators
 * \param name_filter Operator name (or part of name with no wildcards) to filter on (no regex, case insensitive)
 * \param limit Limit the number of displayed results
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_OPERATORS_LIST_operator_handle {
	char *name_filter;
	int limit;
	char **objkeys;
	int objkeys_num;
} OPH_OPERATORS_LIST_operator_handle;

#endif				//__OPERATORSLIST_OPERATOR_H
