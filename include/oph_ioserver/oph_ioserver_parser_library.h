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

#ifndef __OPH_IOSERVER_PARSER_H
#define __OPH_IOSERVER_PARSER_H

#include "oph_common.h"
#include "hashtbl.h"

/**
 * \brief               Function to count number of arguments into submission query
 * \param server_name   Name of I/O server plugin using the function (used for logging)
 * \param query_string  Submission query to parse
 * \param num_param     Pointer to be filled with number of arguments
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_count_params_query_string(const char *server_name, const char *query_string, int *num_param);

/**
 * \brief               Function to validate submission query
 * \param server_name   Name of I/O server plugin using the function (used for logging)
 * \param query_string  Submission query to validate
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_validate_query_string(const char *server_name, const char *query_string);

/**
 * \brief               Function to load all arguments into an hash table
 * \param server_name   Name of I/O server plugin using the function (used for logging)
 * \param query_string  Submission query to load
 * \param hashtbl       Hash table to be loaded
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_load_query_string_params(const char *server_name, const char *query_string, HASHTBL * hashtbl);

/**
 * \brief               Function parse and split multiple-value arguments. WARNING: it modified the input "values" string 
 * \param server_name   Name of I/O server plugin using the function (used for logging)
 * \param values        Values to be splitted
 * \param value_list    Array of pointer to each value
 * \param value_num     Number of values splitted
 * \return              0 if successfull, non-0 otherwise
 */
int oph_ioserver_parse_multivalue_arg(const char *server_name, char *values, char ***value_list, int *value_num);

#endif				//__OPH_IOSERVER_PARSER_H
