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

#ifndef __OPH_JSON_MULTIGRID_H__
#define __OPH_JSON_MULTIGRID_H__

#include "oph_json_common.h"

/***********OPH_JSON_OBJ_MULTIGRID STRUCTURE***********/

/**
 * \brief Structure that defines the OBJCLASS multidimgrid
 * \param title Title
 * \param description Multidimgrid description
 * \param rowkeys Explicit dimension names
 * \param rowkeys_num Number of explicit dimensions
 * \param rowfieldtypes Explicit dimension datatypes
 * \param rowfieldtypes_num Number of datatypes for explicit dimensions
 * \param rowvalues Explicit dimension values
 * \param rowvalues_num1 Number of rows
 * \param rowvalues_num2 Number of explicit dimensions
 * \param colkeys Implicit dimension names
 * \param colkeys_num Number of implicit dimensions
 * \param colfieldtypes Implicit dimension datatypes
 * \param colfieldtypes_num Number of datatypes for implicit dimensions
 * \param colvalues Implicit dimension values
 * \param colvalues_num1 Number of array elements
 * \param colvalues_num2 Number of implicit dimensions
 * \param measurename Measure name
 * \param measuretype Measure type
 * \param measurevalues Measure values
 * \param measurevalues_num1 Number of rows
 * \param measurevalues_num2 Number of array elements
 */
typedef struct _oph_json_obj_multigrid {
	char *title;
	char *description;
	char **rowkeys;
	unsigned int rowkeys_num;
	char **rowfieldtypes;
	unsigned int rowfieldtypes_num;
	char ***rowvalues;
	unsigned int rowvalues_num1;
	unsigned int rowvalues_num2;
	char **colkeys;
	unsigned int colkeys_num;
	char **colfieldtypes;
	unsigned int colfieldtypes_num;
	char ***colvalues;
	unsigned int colvalues_num1;
	unsigned int colvalues_num2;
	char *measurename;
	char *measuretype;
	char ***measurevalues;
	unsigned int measurevalues_num1;
	unsigned int measurevalues_num2;
} oph_json_obj_multigrid;

/***********OPH_JSON_OBJ_MULTIGRID FUNCTIONS***********/

/**
 * \brief Function to add a multigrid object to OPH_JSON
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying objects (required)
 * \param title A title (required)
 * \param description A description string or NULL
 * \param rowkeys Names of explicit dimensions (required)
 * \param rowkeys_num Number of explicit dimensions (required)
 * \param rowfieldtypes Datatypes for explicit dimensions (required)
 * \param rowfieldtypes_num Number of datatypes for explicit dimensions (required)
 * \param colkeys Names of implicit dimensions (required)
 * \param colkeys_num Number of implicit dimensions (required)
 * \param colfieldtypes Datatypes for implicit dimensions (required)
 * \param colfieldtypes_num Number of datatypes for implicit dimensions (required)
 * \param colvalues Matrix of size "number of array elements x number of implicit dimensions" with implicit dimensions values (required)
 * \param colvalues_num Number of array elements (required)
 * \param measurename Measure name (required)
 * \param measuretype Measure type (required)
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_multigrid(oph_json *json, const char *objkey, const char *title, const char *description, char **rowkeys, int rowkeys_num, char **rowfieldtypes, int rowfieldtypes_num, char **colkeys, int colkeys_num, char **colfieldtypes, int colfieldtypes_num, char ***colvalues, int colvalues_num, const char *measurename, const char *measuretype);

/**
 * \brief Function to add a row to a multigrid object in OPH_JSON
 * \param json Pointer to an OPH_JSON object
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying the object
 * \param rowvalues The values of explicit dimensions for the new row
 * \param measurevalues The row of measures to be inserted
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_multigrid_row(oph_json *json, const char *objkey, char **rowvalues, char **measurevalues);

/***********OPH_JSON_OBJ_MULTIGRID INTERNAL FUNCTIONS***********/

// Free a multigrid object contents
int oph_json_free_multigrid(oph_json_obj_multigrid *obj);

#endif
