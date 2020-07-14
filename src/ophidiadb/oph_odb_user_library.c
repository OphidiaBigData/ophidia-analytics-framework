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

#include "oph_odb_user_library.h"
#define _GNU_SOURCE

#include <string.h>

#include "debug.h"

extern int msglevel;

int oph_odb_user_retrieve_user_id(ophidiadb * oDB, char *username, int *id_user)
{
	if (!oDB || !username || !id_user) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ODB_NULL_PARAM;
	}

	*id_user = (int) strtol(username, NULL, 10);

	return OPH_ODB_SUCCESS;
}
