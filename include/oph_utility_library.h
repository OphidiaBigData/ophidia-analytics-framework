/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#ifndef __OPH_UTILITY_H
#define __OPH_UTILITY_H

#include "oph_common.h"

#include <stdlib.h>

#define OPH_UTL_ERROR   1
#define OPH_UTL_SUCCESS 0

#define OPH_UTL_UNIT_SIZE      3
#define OPH_UTL_KB_UNIT        "KB"
#define OPH_UTL_KB_UNIT_VALUE  1
#define OPH_UTL_MB_UNIT        "MB"
#define OPH_UTL_MB_UNIT_VALUE  2
#define OPH_UTL_GB_UNIT        "GB"
#define OPH_UTL_GB_UNIT_VALUE  3
#define OPH_UTL_TB_UNIT        "TB"
#define OPH_UTL_TB_UNIT_VALUE  4
#define OPH_UTL_PB_UNIT        "PB"
#define OPH_UTL_PB_UNIT_VALUE  5

#define OPH_UTL_KB_SIZE     1024.0
#define OPH_UTL_MB_SIZE     1048576.0
#define OPH_UTL_GB_SIZE     1073741824.0
#define OPH_UTL_TB_SIZE     1099511627776.0
#define OPH_UTL_PB_SIZE     1125899906842624.0

/** 
 * \brief Function get size of array from its length and type
 * \param oph_type String with the type used in the array
 * \param array_length Length of the array
 * \param size Size of array to be computed
 * \return 0 if successfull, N otherwise
 */
int oph_utl_get_array_size(char *oph_type, long long array_length, long long *size);

/** 
 * \brief Function to shorten path string
 * \param max_size Maximum number of chars to be considered
 * \param start_size Strating point in the string
 * \param path Path to shorten
 * \return 0 if successfull, N otherwise
 */
int oph_utl_short_folder(int max_size, int start_size, char *path);

/** 
 * \brief Function to compute size automatically setting size unit
 * \param byte_size Size in byte
 * \param computed_size Size computed in the specified unit
 * \param byte_unit Size unit computed 
 * \return 0 if successfull, N otherwise
 */
int oph_utl_auto_compute_size(long long byte_size, double *computed_size, int *byte_unit);

/** 
 * \brief Function to compute size in the specified size unit
 * \param byte_size Size in byte
 * \param byte_unit Size unit
 * \param computed_size Size computed in the specified unit
 * \return 0 if successfull, N otherwise
 */
int oph_utl_compute_size(long long byte_size, int byte_unit, double *computed_size);

/** 
 * \brief Function to convert size unit into string
 * \param unit_value Size unit value
 * \param unit_str Size unit string converted
 * \return 0 if successfull, N otherwise
 */
int oph_utl_unit_to_str(int unit_value, char (*unit_str)[OPH_UTL_UNIT_SIZE]);

/** 
 * \brief Function to convert size unit string into size unit value
 * \param unit_str Size unit string
 * \param unit_value Size unit value converted
 * \return 0 if successfull, N otherwise
 */
int oph_utl_unit_to_value(char unit_str[OPH_UTL_UNIT_SIZE], int *unit_value);

/** 
 * \brief Function to encode binary data using base64 code
 * \param data_buf Input buffer with data to be coded
 * \param dataLength Input buffer length
 * \param result Output buffer
 * \param resultSize Output buffer maximum size
 * \return 0 if successfull, N otherwise
 */
int oph_utl_base64encode(const void *data_buf, size_t dataLength, char *result, size_t resultSize);

/** 
 * \brief Function to decode base64 data into binary data
 * \param data_buf Input buffer terminating with a '\0'
 * \param result Pointer to decoded data; it has to be freed
 * \return 0 if successfull, N otherwise
 */
int oph_utl_base64decode(const char *data_buf, char **result);

#endif				//__OPH_UTILITY_H
