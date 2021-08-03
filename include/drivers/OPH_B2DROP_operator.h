/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#ifndef __OPH_B2DROP_OPERATOR_H
#define __OPH_B2DROP_OPERATOR_H

#include "oph_common.h"

#define OPH_B2DROP_ACTION_PUT	"put"
#define OPH_B2DROP_ACTION_GET	"get"
#define OPH_B2DROP_ACTION_PUT_N	0
#define OPH_B2DROP_ACTION_GET_N	1

/**
 * \brief Structure of parameters needed by the operator OPH_B2DROP. It uploads a file on B2DROP service.
 * \param src_file_path Path of the file to upload
 * \param dst_file_path Path on B2DROP where the file is uploaded
 * \param auth_file_path Path of the authentication file used
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 */
struct _OPH_B2DROP_operator_handle {
	char *src_file_path;
	char *dst_file_path;
	char *auth_file_path;
	char action;
	char **objkeys;
	int objkeys_num;
};
typedef struct _OPH_B2DROP_operator_handle OPH_B2DROP_operator_handle;

#endif				//__OPH_B2DROP_OPERATOR_H
