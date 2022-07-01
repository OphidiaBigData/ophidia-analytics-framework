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

#ifndef __OPH_JSON_TEXT_H__
#define __OPH_JSON_TEXT_H__

#include "oph_json_common.h"

/***********OPH_JSON_OBJ_TEXT STRUCTURE***********/

/**
 * \brief Structure that defines the OBJCLASS text
 * \param title Title
 * \param message Text content
 */
typedef struct _oph_json_obj_text {
	char *title;
	char *message;
} oph_json_obj_text;

/***********OPH_JSON_OBJ_TEXT FUNCTIONS***********/

/**
 * \brief Function to add a text object to OPH_JSON
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying objects (required)
 * \param title A title (required)
 * \param message A text message or NULL
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_text(oph_json * json, const char *objkey, const char *title, const char *message);

/***********OPH_JSON_OBJ_TEXT INTERNAL FUNCTIONS***********/

// Free a text object contents
int oph_json_free_text(oph_json_obj_text * obj);

#endif
