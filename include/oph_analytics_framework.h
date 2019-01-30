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

#ifndef __OPH_ANALYTICS_FRAMEWORK_H
#define __OPH_ANALYTICS_FRAMEWORK_H

/**
 * \brief Function to launch ophidia analytics framework
 * \param task_string Input parameters string
 * \param task_number Number of total task executed
 * \param task_rank Rank of the current task
 * \return 0 if successfull, -1 otherwise
 */
int oph_af_execute_framework(char *task_string, int task_number, int task_rank);

#endif				//__OPH_ANALYTICS_FRAMEWORK_H
