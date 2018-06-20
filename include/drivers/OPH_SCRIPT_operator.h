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

#ifndef __OPH_SCRIPT_OPERATOR_H
#define __OPH_SCRIPT_OPERATOR_H

#include "oph_common.h"

#define OPH_SCRIPT_NOP ":"

/**
 * \brief Structure of parameters needed by the operator OPH_SCRIPT. It executes a script
 * \param script Script file to be executed
 * \param args List of arguments to be passed to the script
 * \param args_num Number of script arguments
 * \param out_redir Filename where stdout is redirected
 * \param err_redir Filename where stderr is redirected
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param session_path Path relative to current session.
 * \param session_url Url relative to current session.
 * \param workflow_id Workflow ID.
 * \param marker_id Marker ID.
 * \param list Flag used to list available scripts.
 * \param user User that has submitted the task
 */
struct _OPH_SCRIPT_operator_handle {
	char *script;
	char **args;
	int args_num;
	char *out_redir;
	char *err_redir;
	char **objkeys;
	int objkeys_num;
	char *session_path;
	char *session_url;
	char *session_code;
	char *workflow_id;
	char *marker_id;
	int list;
	char *user;
};
typedef struct _OPH_SCRIPT_operator_handle OPH_SCRIPT_operator_handle;

#endif				//__OPH_SCRIPT_OPERATOR_H
