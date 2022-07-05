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

#ifndef __OPH_MAN_OPERATOR_H
#define __OPH_MAN_OPERATOR_H

#include "oph_common.h"

/**
 * \brief Structure of parameters needed by the operator MAN. It shows info of an operator.
 * \param operator_name Operator name
 * \param operator_version Operator version
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_MAN_operator_handle {
	char *function_name;
	char *function_version;
	short int function_type_code;
	char **objkeys;
	int objkeys_num;
} OPH_MAN_operator_handle;

#endif				//__OPH_MAN_OPERATOR_H
