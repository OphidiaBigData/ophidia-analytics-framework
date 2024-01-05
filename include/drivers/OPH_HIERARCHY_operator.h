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

#ifndef __OPH_HIERARCHY_OPERATOR_H
#define __OPH_HIERARCHY_OPERATOR_H

#include "oph_ophidiadb_main.h"
#include "oph_common.h"

#define OPH_HIERARCHY_HELP_MESSAGE "\nTo get additional information about a hierarchy defined above use the following:\n OPH_TERM: oph_hierarchy hierarchy=HIERARCHYNAME;\n SUBMISSION STRING: \"operator=oph_hierarchy;hierarchy=HIERARCHYNAME;\"\n"

/**
 * \brief Structure of parameters needed by the operator OPH_HIERARCHY. It shows info of a hierarchy.
 * \param hierachy_name Hierarchy name
 * \param hierachy_version Version of hierarchy
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
typedef struct _OPH_HIERARCHY_operator_handle {
	char *hierarchy_name;
	char *hierarchy_version;
	char **objkeys;
	int objkeys_num;
} OPH_HIERARCHY_operator_handle;

#endif				//__OPH_HIERARCHY_OPERATOR_H
