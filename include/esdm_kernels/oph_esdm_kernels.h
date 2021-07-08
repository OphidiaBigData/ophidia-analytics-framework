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

#ifndef __OPH_ESDM_READ_STREAM_H
#define __OPH_ESDM_READ_STREAM_H

#include <esdm.h>

#define OPH_ESDM_FUNCTION_STREAM "stream"
#define OPH_ESDM_FUNCTION_MAX "max"
#define OPH_ESDM_FUNCTION_MIN "min"
#define OPH_ESDM_FUNCTION_AVG "avg"

typedef struct _stream_data_t {
	char *operation;
	void *buff;
	char valid;
	double value;
	uint64_t number;
	void *fill_value;
} stream_data_t;

void *stream_func(esdm_dataspace_t * space, void *buff, void *user_ptr, void *esdm_fill_value);
void reduce_func(esdm_dataspace_t * space, void *user_ptr, void *stream_func_out);

#endif				//__OPH_ESDM_READ_STREAM_H
