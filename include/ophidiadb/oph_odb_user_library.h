/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

#ifndef __OPH_ODB_USER_H__
#define __OPH_ODB_USER_H__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_user_query.h"

/**
 * \brief Function to retrieve the ID of a user
 * \param oDB Pointer to OphidiaDB
 * \param username Name of the user
 * \param id_user ID of the user
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_user_retrieve_user_id(ophidiadb * oDB, char *username, int *id_user);

#endif				/* __OPH_ODB_META_H__ */
