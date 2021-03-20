/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#ifndef __OPH_JSON_GRID_H__
#define __OPH_JSON_GRID_H__

#include "oph_json_common.h"

/***********OPH_JSON_OBJ_GRID STRUCTURE***********/

/**
 * \brief Structure that defines the OBJCLASS grid
 * \param title Title
 * \param description Grid description
 * \param keys Names of grid columns
 * \param keys_num Number of columns
 * \param fieldtypes Datatypes for each column
 * \param fieldtypes_num Number of datatypes
 * \param values Matrix of values
 * \param values_num1 Number of rows
 * \param values_num2 Number of columns
 */
typedef struct _oph_json_obj_grid {
	char *title;
	char *description;
	char **keys;
	unsigned int keys_num;
	char **fieldtypes;
	unsigned int fieldtypes_num;
	char ***values;
	unsigned int values_num1;
	unsigned int values_num2;
} oph_json_obj_grid;

/***********OPH_JSON_OBJ_GRID FUNCTIONS***********/

/**
 * \brief Function to add a grid object to OPH_JSON
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying objects (required)
 * \param title A title (required)
 * \param description A description string or NULL
 * \param keys Array of column names (required)
 * \param keys_num Number of columns (required)
 * \param fieldtypes Datatypes for each column (required)
 * \param fieldtypes_num Number of columns (required)
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_grid(oph_json * json, const char *objkey, const char *title, const char *description, char **keys, int keys_num, char **fieldtypes, int fieldtypes_num);

/**
 * \brief Function to add a row to a grid object in OPH_JSON
 * \param json Pointer to an OPH_JSON object
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying the object
 * \param values The row to be inserted
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_grid_row(oph_json * json, const char *objkey, char **values);

/***********OPH_JSON_OBJ_GRID INTERNAL FUNCTIONS***********/

// Free a grid object contents
int oph_json_free_grid(oph_json_obj_grid * obj);

#endif
