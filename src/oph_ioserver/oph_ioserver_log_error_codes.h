/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#ifndef __OPH_IOSERVER_LOG_ERROR_CODES_H
#define __OPH_IOSERVER_LOG_ERROR_CODES_H

#define OPH_IOSERVER_LOG_NO_SERVER         "none"

/*IOSERVER LOG ERRORS*/
#define OPH_IOSERVER_LOG_NULL_HANDLE       "Null Handle\n"
#define OPH_IOSERVER_LOG_NULL_INPUT_PARAM  "Null input parameter\n"
#define OPH_IOSERVER_LOG_INVALID_SERVER    "IO Server name not valid\n"
#define OPH_IOSERVER_LOG_SERVER_PARSE_ERROR "IO Server name parsing error\n"
#define OPH_IOSERVER_LOG_DLINIT_ERROR       "lt_dlinit error: %s\n"
#define OPH_IOSERVER_LOG_LIB_NOT_FOUND     "IO server library not found\n"
#define OPH_IOSERVER_LOG_DLOPEN_ERROR      "lt_dlopen error: %s\n"
#define OPH_IOSERVER_LOG_LOAD_FUNC_ERROR   "Unable to load IO server function %s\n"
#define OPH_IOSERVER_LOG_LOAD_SERV_ERROR   "IO Server not properly loaded\n"
#define OPH_IOSERVER_LOG_DLCLOSE_ERROR     "lt_dlclose error: %s\n"
#define OPH_IOSERVER_LOG_RELEASE_RES_ERROR "Unable to release resources\n"
#define OPH_IOSERVER_LOG_DLEXIT_ERROR      "Error while executing lt_dlexit %s\n"
#define OPH_IOSERVER_LOG_FILE_NOT_FOUND    "IO server file not found %s\n"
#define OPH_IOSERVER_LOG_READ_LINE_ERROR   "Unable to read file line\n"
#define OPH_IOSERVER_LOG_MEMORY_ERROR      "Memory allocation error\n"

#define OPH_IOSERVER_LOG_VALID_ERROR       "Submission query not valid\n"


#endif				//__OPH_IOSERVER_LOG_ERROR_CODES_H
