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

#ifndef __OPH_DRIVER_PROC_H__
#define __OPH_DRIVER_PROC_H__

#include "config.h"

#include "oph_ophidiadb_main.h"
#include "oph_common.h"

/**
 * \brief Procedure used to delete all fragments associated to a datacube. Parallelism is applied at the level of DBs.
 * \param id_datacube Id of the datacube
 * \param id_container Id of the container where the datacube belongs
 * \param fragment_ids Contains the string of fragment relative index
 * \param start_position DB list starting position (in case of import-type operations)
 * \param row_number DB list number of rows (in case of import-type operations)
 * \param thread_number Number of posix threads to be used for delete procedure
 * \return 0 if successfull, -1 otherwise
 */
int oph_dproc_delete_data(int id_datacube, int id_container, char *fragment_ids, int start_position, int row_number, int thread_number);

/**
 * \brief Procedure to remove all datacube information from OphidiaDB
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_datacube Id of the datacube
 * \param id_container Id of the container where the datacube belongs
 * \return 0 if successfull, -1 otherwise
 */
int oph_dproc_clean_odb(ophidiadb * oDB, int id_datacube, int id_container);

#endif				/* __OPH_DRIVER_PROC_H__ */
