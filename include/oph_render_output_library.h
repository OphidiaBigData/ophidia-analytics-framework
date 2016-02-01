/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#ifndef __OPH_RENDER_OUTPUT_LIBRARY__
#define __OPH_RENDER_OUTPUT_LIBRARY__

#include "oph_json_library.h"

#define OPH_RENDER_OUTPUT_RENDER_OK           0
#define OPH_RENDER_OUTPUT_RENDER_INVALID_XML  1
#define OPH_RENDER_OUTPUT_RENDER_ERROR        2

//table dimensions for MAN (OPERATOR/PRIMITIVE)
#define OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH 150 //must be even
#define OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH 24
#define OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH 13
#define OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH 4
#define OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH 4
#define OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH 40
#define OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH 9
#define OPH_RENDER_OUTPUT_MAN_ARG_SPACING 20
#define OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH - OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH \
                        - OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH - OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH - OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH \
                        - OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH - OPH_RENDER_OUTPUT_MAN_ARG_SPACING)

#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH 20
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH 16
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH 7
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH 7
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH 9
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH 9
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH 15
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH 9
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_SPACING 26
#define OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH \
                        - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH \
                        - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH -OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH \
                        - OPH_RENDER_OUTPUT_MAN_MULTI_ARG_SPACING)

//table dimensions for HIERARCHY
#define OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH 130 //must be even
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH 30
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH 12
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH 22
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH 20
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SPACING 14
#define OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH (OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH - OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH - OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH \
                        - OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH - OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH - OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SPACING)

/**
 * \brief Function to render an XML file
 * \param xmlfilename XML filename
 * \param function_type_code Type of function between primitive (1), operator (2) and hierarchy (3)
 * \param oper_json Pointer to OPH_JSON structure
 * \param objkeys Operator admitted objkeys
 * \param objkeys_num Number of objkeys
 * \return 0 if successfull, -1 otherwise
 */
int renderXML(const char *xmlfilename, short int function_type_code, oph_json *oper_json, char **objkeys, int objkeys_num);

#endif /* __OPH_RENDER_OUTPUT_LIBRARY__ */
