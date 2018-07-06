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

#ifndef __OPH_SUBSET_H__
#define __OPH_SUBSET_H__

#include "oph_common.h"

#define OPH_SUBSET_LIB_MAX_STRING_LENGTH	1024
#define OPH_SUBSET_LIB_MAX_TYPE_LENGTH		64

#define OPH_SUBSET_LIB_OK			0
#define OPH_SUBSET_LIB_DATA_ERR			1
#define OPH_SUBSET_LIB_NULL_POINTER_ERR		2
#define OPH_SUBSET_LIB_SYSTEM_ERR		3

#define OPH_SUBSET_LIB_PARAM_SEPARATOR		":"
#define OPH_SUBSET_LIB_SUBSET_SEPARATOR		","
#define OPH_SUBSET_LIB_PARAM_BEGIN		"begin"
#define OPH_SUBSET_LIB_PARAM_END		"end"
#define OPH_SUBSET_LIB_MAX_DIM			10

#define OPH_SUBSET_LIB_BYTE_TYPE		OPH_COMMON_BYTE_TYPE
#define OPH_SUBSET_LIB_SHORT_TYPE		OPH_COMMON_SHORT_TYPE
#define OPH_SUBSET_LIB_INT_TYPE			OPH_COMMON_INT_TYPE
#define OPH_SUBSET_LIB_LONG_TYPE		OPH_COMMON_LONG_TYPE
#define OPH_SUBSET_LIB_DOUBLE_TYPE		OPH_COMMON_DOUBLE_TYPE
#define OPH_SUBSET_LIB_FLOAT_TYPE		OPH_COMMON_FLOAT_TYPE

typedef enum { OPH_SUBSET_LIB_SINGLE, OPH_SUBSET_LIB_INTERVAL, OPH_SUBSET_LIB_STRIDE } oph_subset_type;

typedef struct {
	oph_subset_type *type;
	double *start;
	double *end;
	unsigned int number;	// Number of intervals
} oph_subset_double;


typedef struct {
	oph_subset_type *type;
	unsigned long long *start;
	unsigned long long *end;
	unsigned long long *stride;
	unsigned long long *count;
	unsigned long long total;
	unsigned int number;	// Number of intervals
} oph_subset;			// List of subsets in the form <start>:<stride>:<max>

// Initialization of struct oph_subset
int oph_subset_init(oph_subset ** subset);

// Translate non-null-terminated string into an oph_subset struct. Set 'max' to 0 to avoid truncation to 'max' elements
int oph_subset_parse(const char *cond, unsigned long long len, oph_subset * subset, unsigned long long max);

// Evaluate the size of an oph_subset struct over an multi-dimensional array. Use "sizes=NULL" if the dimension is one only.
int oph_subset_size(oph_subset * subset, unsigned long long initial_size, unsigned long long *final_size, unsigned long long *sizes, int size_number);
int oph_subset_size2(oph_subset * subset, unsigned long long initial_size, unsigned long long *final_size, unsigned long long block_size, unsigned long long max);

// Freeing the struct oph_subset
int oph_subset_free(oph_subset * subset);

int oph_subset_vector_free(oph_subset ** subset_struct, int num);

// Return 1 if an index/id is a subset
int oph_subset_index_is_in_subset_block(unsigned long long index, unsigned long long start, unsigned long long stride, unsigned long long end);
int oph_subset_index_is_in_subset(unsigned long long index, oph_subset * subset);
int oph_subset_id_is_in_subset(unsigned long long id, oph_subset * subset, unsigned long long *sizes, int size_number);
int oph_subset_id_is_in_subset2(unsigned long long id, oph_subset * subset, unsigned long long block_size, unsigned long long max);

// Translate id to index given the sizes of quicker dimensions
unsigned long long oph_subset_id_to_index(unsigned long long id, unsigned long long *sizes, int n);
unsigned long long oph_subset_id_to_index2(unsigned long long id, unsigned long long block_size, unsigned long long max);

// Translate values to index for subset string
int oph_subset_value_to_index(const char *in_cond, char *data, unsigned long long data_size, char *data_type, double offset, char *out_cond, oph_subset ** out_subset);

#endif				/* __OPH_SUBSET_H__ */
