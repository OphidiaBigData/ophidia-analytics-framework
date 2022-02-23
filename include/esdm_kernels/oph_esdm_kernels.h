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

#define OPH_ESDM_SEPARATOR ","

#define OPH_ESDM_FUNCTION_NOP "nop"
#define OPH_ESDM_FUNCTION_STREAM "stream"

#define OPH_ESDM_FUNCTION_MAX "max"
#define OPH_ESDM_FUNCTION_MIN "min"
#define OPH_ESDM_FUNCTION_AVG "avg"
#define OPH_ESDM_FUNCTION_SUM "sum"
#define OPH_ESDM_FUNCTION_STD "std"
#define OPH_ESDM_FUNCTION_VAR "var"

#define OPH_ESDM_FUNCTION_SUM_SCALAR "sum_scalar"
#define OPH_ESDM_FUNCTION_MUL_SCALAR "mul_scalar"

#define OPH_ESDM_FUNCTION_ABS "abs"
#define OPH_ESDM_FUNCTION_SQR "sqr"
#define OPH_ESDM_FUNCTION_SQRT "sqrt"
#define OPH_ESDM_FUNCTION_CEIL "ceil"
#define OPH_ESDM_FUNCTION_FLOOR "floor"
#define OPH_ESDM_FUNCTION_ROUND "round"
#define OPH_ESDM_FUNCTION_INT "int"
#define OPH_ESDM_FUNCTION_NINT "nint"

#define OPH_ESDM_FUNCTION_POW "pow"
#define OPH_ESDM_FUNCTION_EXP "exp"
#define OPH_ESDM_FUNCTION_LOG "log"
#define OPH_ESDM_FUNCTION_LOG10 "log10"

#define OPH_ESDM_FUNCTION_SIN "sin"
#define OPH_ESDM_FUNCTION_COS "cos"
#define OPH_ESDM_FUNCTION_TAN "tan"
#define OPH_ESDM_FUNCTION_ASIN "asin"
#define OPH_ESDM_FUNCTION_ACOS "acos"
#define OPH_ESDM_FUNCTION_ATAN "atan"
#define OPH_ESDM_FUNCTION_SINH "sinh"
#define OPH_ESDM_FUNCTION_COSH "cosh"
#define OPH_ESDM_FUNCTION_TANH "tanh"

#define OPH_ESDM_FUNCTION_RECI "reci"
#define OPH_ESDM_FUNCTION_NOT "not"

typedef struct _oph_esdm_stream_data_t {
	char *operation;
	char *args;
	void *buff;
	char valid;
	double value;
	double value2;
	uint64_t number;
	void *fill_value;
} oph_esdm_stream_data_t;

int oph_esdm_is_a_reduce_func(const char *operation);
void *oph_esdm_stream_func(esdm_dataspace_t * space, void *buff, void *user_ptr, void *esdm_fill_value);
void oph_esdm_reduce_func(esdm_dataspace_t * space, void *user_ptr, void *stream_func_out);

#endif				//__OPH_ESDM_READ_STREAM_H
