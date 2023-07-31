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

#include "oph_subset_library.h"

#include <string.h>
#include <stdlib.h>

#include "debug.h"

extern int msglevel;

void _oph_subset_double_free(oph_subset_double * subset)
{
	if (subset->type) {
		free(subset->type);
		subset->type = 0;
	}
	if (subset->start) {
		free(subset->start);
		subset->start = 0;
	}
	if (subset->end) {
		free(subset->end);
		subset->end = 0;
	}
}


int oph_subset_vector_free(oph_subset ** subset_struct, int num)
{
	int i;
	if (subset_struct)
		for (i = 0; i < num; ++i)
			if (subset_struct[i]) {
				oph_subset_free(subset_struct[i]);
				subset_struct[i] = NULL;
			}
	return OPH_SUBSET_LIB_OK;
}

int oph_subset_double_init(oph_subset_double ** subset)
{
	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}
	*subset = (oph_subset_double *) malloc(sizeof(oph_subset_double));
	if (!(*subset)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in allocating oph_subset\n");
		return OPH_SUBSET_LIB_SYSTEM_ERR;
	}
	return OPH_SUBSET_LIB_OK;
}

int oph_subset_double_parse(const char *cond, unsigned long long len, oph_subset_double * subset, double min, double max, double offset)
{
	char *result, *result2, temp0[1 + len], temp1[1 + len], temp2[1 + len], *next, *temp, *savepointer = NULL;
	unsigned int number;

	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}

	subset->number = 0;
	strncpy(temp0, cond, len);
	temp0[len] = '\0';

	strcpy(temp2, temp0);
	while (strtok_r(subset->number ? NULL : temp0, OPH_SUBSET_LIB_SUBSET_SEPARATOR, &savepointer))
		subset->number++;

	if (!subset->number)
		return OPH_SUBSET_LIB_DATA_ERR;

	int retval = OPH_SUBSET_LIB_OK, i = 0;
	double dtemp;

	subset->type = (oph_subset_type *) malloc(subset->number * sizeof(oph_subset_type));
	subset->start = (double *) malloc(subset->number * sizeof(double));
	subset->end = (double *) malloc(subset->number * sizeof(double));

	next = temp2;
	result = strchr(temp2, OPH_SUBSET_LIB_SUBSET_SEPARATOR[0]);

	while (next && (retval == OPH_SUBSET_LIB_OK)) {
		if (result) {
			result[0] = '\0';
			temp = result + 1;
		} else
			temp = 0;
		result = next;
		next = temp;

		number = 0;
		strcpy(temp1, result);
		result2 = strtok_r(temp1, OPH_SUBSET_LIB_PARAM_SEPARATOR, &savepointer);
		while (result2 && (retval == OPH_SUBSET_LIB_OK)) {
			switch (number) {
				case 0:
					if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_BEGIN, strlen(OPH_SUBSET_LIB_PARAM_BEGIN))) {
						if (min)
							subset->end[i] = min;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_BEGIN);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_END, strlen(OPH_SUBSET_LIB_PARAM_END))) {
						if (max)
							subset->end[i] = max;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_END);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else
						subset->end[i] = strtod(result2, NULL);
					subset->start[i] = subset->end[i];
					subset->type[i] = OPH_SUBSET_LIB_SINGLE;
					if (offset) {
						subset->start[i] -= offset;
						subset->end[i] += offset;
						if (min) {
							if (subset->start[i] < min)
								subset->start[i] = min;
							if (subset->end[i] < min)
								subset->end[i] = min;
						}
						if (max) {
							if (subset->start[i] > max)
								subset->start[i] = max;
							if (subset->end[i] > max)
								subset->end[i] = max;
						}
						if (subset->start[i] != subset->end[i])
							subset->type[i] = OPH_SUBSET_LIB_INTERVAL;
					}
					break;
				case 1:
					if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_BEGIN, strlen(OPH_SUBSET_LIB_PARAM_BEGIN))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_BEGIN);
						retval = OPH_SUBSET_LIB_DATA_ERR;
					} else if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_END, strlen(OPH_SUBSET_LIB_PARAM_END))) {
						if (max)
							subset->end[i] = max;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_END);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else
						subset->end[i] = strtod(result2, NULL);
					subset->type[i] = OPH_SUBSET_LIB_INTERVAL;
					if (offset) {
						subset->end[i] += offset;
						if (min && (subset->end[i] < min))
							subset->end[i] = min;
						if (max && (subset->end[i] > max))
							subset->end[i] = max;
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input data: too many '%s' in subset\n", OPH_SUBSET_LIB_PARAM_SEPARATOR);
					retval = OPH_SUBSET_LIB_DATA_ERR;
			}
			number++;
			result2 = strtok_r(NULL, OPH_SUBSET_LIB_PARAM_SEPARATOR, &savepointer);
		}
		if (retval != OPH_SUBSET_LIB_OK)
			break;

		if (!number) {
			subset->type[i] = OPH_SUBSET_LIB_INTERVAL;
			subset->start[i] = 1;
			if (max)
				subset->end[i] = max;
			else
				subset->end[i] = subset->start[i];
		}
		if (subset->start[i] > subset->end[i]) {
			dtemp = subset->start[i];
			subset->start[i] = subset->end[i];
			subset->end[i] = dtemp;
		}
		++i;
		if (next)
			result = strchr(next, OPH_SUBSET_LIB_SUBSET_SEPARATOR[0]);
	}

	if (retval != OPH_SUBSET_LIB_OK)
		_oph_subset_double_free(subset);

	return retval;
}

void _oph_subset_free(oph_subset * subset)
{
	if (subset->type) {
		free(subset->type);
		subset->type = 0;
	}
	if (subset->start) {
		free(subset->start);
		subset->start = 0;
	}
	if (subset->end) {
		free(subset->end);
		subset->end = 0;
	}
	if (subset->stride) {
		free(subset->stride);
		subset->stride = 0;
	}
	if (subset->count) {
		free(subset->count);
		subset->count = 0;
	}
}

int oph_subset_init(oph_subset ** subset)
{
	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}
	*subset = (oph_subset *) malloc(sizeof(oph_subset));
	if (!(*subset)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in allocating oph_subset\n");
		return OPH_SUBSET_LIB_SYSTEM_ERR;
	}
	return OPH_SUBSET_LIB_OK;
}

int oph_subset_parse(const char *cond, unsigned long long len, oph_subset * subset, unsigned long long max)
{
	char *result, *result2, temp0[1 + len], temp1[1 + len], temp2[1 + len], *next, *temp, *savepointer = NULL;
	unsigned int number;

	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}

	subset->number = 0;
	strncpy(temp0, cond, len);
	temp0[len] = '\0';

	strcpy(temp2, temp0);
	while (strtok_r(subset->number ? NULL : temp0, OPH_SUBSET_LIB_SUBSET_SEPARATOR, &savepointer))
		subset->number++;

	if (!subset->number)
		return OPH_SUBSET_LIB_DATA_ERR;

	int retval = OPH_SUBSET_LIB_OK, i = 0;

	subset->type = (oph_subset_type *) malloc(subset->number * sizeof(oph_subset_type));
	subset->start = (unsigned long long *) malloc(subset->number * sizeof(unsigned long long));
	subset->end = (unsigned long long *) malloc(subset->number * sizeof(unsigned long long));
	subset->stride = (unsigned long long *) malloc(subset->number * sizeof(unsigned long long));
	subset->count = (unsigned long long *) malloc(subset->number * sizeof(unsigned long long));
	subset->total = 0;

	next = temp2;
	result = strchr(temp2, OPH_SUBSET_LIB_SUBSET_SEPARATOR[0]);

	while (next && (retval == OPH_SUBSET_LIB_OK)) {
		if (result) {
			result[0] = '\0';
			temp = result + 1;
		} else
			temp = 0;
		result = next;
		next = temp;

		number = 0;
		strcpy(temp1, result);
		result2 = strtok_r(temp1, OPH_SUBSET_LIB_PARAM_SEPARATOR, &savepointer);
		while (result2 && (retval == OPH_SUBSET_LIB_OK)) {
			switch (number) {
				case 0:
					if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_END, strlen(OPH_SUBSET_LIB_PARAM_END))) {
						if (max)
							subset->end[i] = max;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_END);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else
						subset->end[i] = strtol(result2, NULL, 10);
					subset->start[i] = subset->end[i];
					subset->stride[i] = 1;
					subset->type[i] = OPH_SUBSET_LIB_SINGLE;
					break;
				case 1:
					if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_END, strlen(OPH_SUBSET_LIB_PARAM_END))) {
						if (max)
							subset->end[i] = max;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_END);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else
						subset->end[i] = strtol(result2, NULL, 10);
					subset->type[i] = OPH_SUBSET_LIB_INTERVAL;
					break;
				case 2:
					subset->stride[i] = subset->end[i];
					if (!strncasecmp(result2, OPH_SUBSET_LIB_PARAM_END, strlen(OPH_SUBSET_LIB_PARAM_END))) {
						if (max)
							subset->end[i] = max;
						else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Clause '%s' cannot be used in this context\n", OPH_SUBSET_LIB_PARAM_END);
							retval = OPH_SUBSET_LIB_DATA_ERR;
						}
					} else
						subset->end[i] = strtol(result2, NULL, 10);
					if (subset->stride[i] > 1)
						subset->type[i] = OPH_SUBSET_LIB_STRIDE;
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input data: too many '%s' in subset\n", OPH_SUBSET_LIB_PARAM_SEPARATOR);
					retval = OPH_SUBSET_LIB_DATA_ERR;
			}
			number++;
			result2 = strtok_r(0, OPH_SUBSET_LIB_PARAM_SEPARATOR, &savepointer);
		}
		if (retval != OPH_SUBSET_LIB_OK)
			break;

		if (!number) {
			subset->type[i] = OPH_SUBSET_LIB_INTERVAL;
			subset->start[i] = subset->stride[i] = 1;
			if (max)
				subset->end[i] = max;
			else
				subset->end[i] = subset->start[i];
		}

		if ((subset->stride[i] <= 0) || (subset->start[i] <= 0) || (subset->start[i] > subset->end[i]) || (max && (subset->end[i] > max))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input data: 'start', 'stop' or 'step' parameters are not correctly set\n");
			retval = OPH_SUBSET_LIB_DATA_ERR;
			break;
		}
		subset->count[i] = 1 + (subset->end[i] - subset->start[i]) / subset->stride[i];
		subset->total += subset->count[i];
		++i;
		if (next)
			result = strchr(next, OPH_SUBSET_LIB_SUBSET_SEPARATOR[0]);
	}

	if (retval != OPH_SUBSET_LIB_OK)
		_oph_subset_free(subset);

	return retval;
}

int oph_subset_size(oph_subset * subset, unsigned long long initial_size, unsigned long long *final_size, unsigned long long *sizes, int size_number)
{
	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}

	*final_size = 0;
	unsigned long long j;

	if (!sizes || !size_number) {
		for (j = 1; j <= initial_size; ++j)	// It is not C-like indexing
			if (oph_subset_index_is_in_subset(j, subset))
				(*final_size)++;
	} else {
		for (j = 1; j <= initial_size; ++j)	// It is not C-like indexing
			if (oph_subset_id_is_in_subset(j, subset, sizes, size_number))
				(*final_size)++;
	}

	return OPH_SUBSET_LIB_OK;
}

int oph_subset_size2(oph_subset * subset, unsigned long long initial_size, unsigned long long *final_size, unsigned long long block_size, unsigned long long max)
{
	if (!subset) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}

	*final_size = 0;
	unsigned long long j;

	for (j = 1; j <= initial_size; ++j)	// It is not C-like indexing
		if (oph_subset_id_is_in_subset2(j, subset, block_size, max))
			(*final_size)++;

	return OPH_SUBSET_LIB_OK;
}

int oph_subset_index_is_in_subset(unsigned long long index, oph_subset * subset)
{
	unsigned int i;
	for (i = 0; i < subset->number; ++i)
		if (oph_subset_index_is_in_subset_block(index, subset->start[i], subset->stride[i], subset->end[i]))
			break;
	return (i < subset->number);
}

int oph_subset_id_is_in_subset(unsigned long long id, oph_subset * subset, unsigned long long *sizes, int size_number)
{
	return oph_subset_index_is_in_subset(oph_subset_id_to_index(id, sizes, size_number), subset);
}

int oph_subset_id_is_in_subset2(unsigned long long id, oph_subset * subset, unsigned long long block_size, unsigned long long max)
{
	return oph_subset_index_is_in_subset(oph_subset_id_to_index2(id, block_size, max), subset);
}

int oph_subset_index_is_in_subset_block(unsigned long long index, unsigned long long start, unsigned long long stride, unsigned long long end)
{
	return (!((index - start) % stride) && (index >= start) && (index <= end));
}

unsigned long long oph_subset_id_to_index(unsigned long long id, unsigned long long *sizes, int n)
{
	int i;
	unsigned long long index = 0;
	id--;			// It is not C-like indexing
	for (i = 0; i < n; ++i) {
		index = id % sizes[i];
		id = (id - index) / sizes[i];
	}
	return index + 1;	// It is not C-like indexing
}

unsigned long long oph_subset_id_to_index2(unsigned long long id, unsigned long long block_size, unsigned long long max)
{
	return 1 + (((id - 1) / block_size) % max);
}

int oph_subset_free(oph_subset * subset)
{
	if (subset) {
		_oph_subset_free(subset);
		free(subset);
	}
	return OPH_SUBSET_LIB_OK;
}

int oph_subset_double_free(oph_subset_double * subset)
{
	if (subset) {
		_oph_subset_double_free(subset);
		free(subset);
	}
	return OPH_SUBSET_LIB_OK;
}

int oph_subset_value_to_index(const char *in_cond, char *data, unsigned long long data_size, char *data_type, double offset, char *out_cond, oph_subset ** out_subset)
{
	if (!in_cond || !data || !data_size || !data_type || !out_cond) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_SUBSET_LIB_NULL_POINTER_ERR;
	}

	out_cond[0] = 0;	// Already allocated
	if (out_subset)
		*out_subset = NULL;

	double min, max;
	if (!strncasecmp(data_type, OPH_SUBSET_LIB_DOUBLE_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = *((double *) data);
		max = *(((double *) data) + data_size - 1);
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_FLOAT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = (double) (*((float *) data));
		max = (double) (*(((float *) data) + data_size - 1));
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_INT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = (double) (*((int *) data));
		max = (double) (*(((int *) data) + data_size - 1));
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_LONG_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = (double) (*((long long *) data));
		max = (double) (*(((long long *) data) + data_size - 1));
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_SHORT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = (double) (*((short *) data));
		max = (double) (*(((short *) data) + data_size - 1));
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_BYTE_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		min = (double) (*((char *) data));
		max = (double) (*(((char *) data) + data_size - 1));
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown data type\n");
		return OPH_SUBSET_LIB_DATA_ERR;
	}

	oph_subset_double *subset_double;
	if (oph_subset_double_init(&subset_double))
		return OPH_SUBSET_LIB_SYSTEM_ERR;
	if (oph_subset_double_parse(in_cond, strlen(in_cond), subset_double, min, max, offset))
		return OPH_SUBSET_LIB_DATA_ERR;

	oph_subset *subset;
	oph_subset_init(&subset);
	subset->number = subset_double->number;
	subset->type = (oph_subset_type *) calloc(subset->number, sizeof(oph_subset_type));
	subset->start = (unsigned long long *) calloc(subset->number, sizeof(unsigned long long));
	subset->end = (unsigned long long *) calloc(subset->number, sizeof(unsigned long long));
	subset->stride = (unsigned long long *) calloc(subset->number, sizeof(unsigned long long));
	subset->count = (unsigned long long *) calloc(subset->number, sizeof(unsigned long long));
	subset->total = 0;

	int i, j;
	for (i = 0; i < (int) subset->number; ++i)
		subset->stride[i] = 1;

	if (!strncasecmp(data_type, OPH_SUBSET_LIB_DOUBLE_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((double *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((double *) data + j) - subset_double->start[i] < subset_double->start[i] - *((double *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((double *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0) || ((j < (int) data_size - 1) && (*((double *) data + j) - subset_double->start[i] > subset_double->start[i] - *((double *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;	// The actual value is beetween j-1 (-inf if negative) and j (+inf if equal to data_size)
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((double *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((double *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((double *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((double *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((double *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((double *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((double *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((double *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_FLOAT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((float *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((float *) data + j) - subset_double->start[i] < subset_double->start[i] - *((float *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((float *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0) || ((j < (int) data_size - 1) && (*((float *) data + j) - subset_double->start[i] > subset_double->start[i] - *((float *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((float *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((float *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((float *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((float *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((float *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((float *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((float *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((float *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_BYTE_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((char *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((char *) data + j) - subset_double->start[i] < subset_double->start[i] - *((char *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((char *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0) || ((j < (int) data_size - 1) && (*((char *) data + j) - subset_double->start[i] > subset_double->start[i] - *((char *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((char *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((char *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((char *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((char *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((char *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((char *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((char *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((char *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_SHORT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((short *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((short *) data + j) - subset_double->start[i] < subset_double->start[i] - *((short *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((short *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0) || ((j < (int) data_size - 1) && (*((short *) data + j) - subset_double->start[i] > subset_double->start[i] - *((short *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((short *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((short *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((short *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((short *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((short *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((short *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((short *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((short *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_INT_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((int *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((int *) data + j) - subset_double->start[i] < subset_double->start[i] - *((int *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((int *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0) || ((j < (int) data_size - 1) && (*((int *) data + j) - subset_double->start[i] > subset_double->start[i] - *((int *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((int *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((int *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((int *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((int *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((int *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((int *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((int *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((int *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else if (!strncasecmp(data_type, OPH_SUBSET_LIB_LONG_TYPE, OPH_SUBSET_LIB_MAX_TYPE_LENGTH)) {
		for (i = 0; i < (int) subset->number; ++i) {
			subset->type[i] = subset_double->type[i];
			switch (subset_double->type[i]) {
				case OPH_SUBSET_LIB_SINGLE:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((long long *) data + j) >= subset_double->start[i])
								break;
						if ((j < (int) data_size) && (!j || (*((long long *) data + j) - subset_double->start[i] < subset_double->start[i] - *((long long *) data + j - 1))))
							j++;	// Non 'C'-like indexing
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((long long *) data + j) >= subset_double->start[i])
								break;
						if ((j < 0)
						    || ((j < (int) data_size - 1) && (*((long long *) data + j) - subset_double->start[i] > subset_double->start[i] - *((long long *) data + j + 1))))
							j++;
						j++;	// Non 'C'-like indexing
					}
					subset->start[i] = subset->end[i] = j;
					break;
				case OPH_SUBSET_LIB_INTERVAL:
					if (min < max) {
						for (j = 0; j < (int) data_size; ++j)
							if (*((long long *) data + j) >= subset_double->start[i])
								break;
						if (j == (int) data_size) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if (!j && (subset_double->end[i] < *((int *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->start[i] = j + 1;	// Non 'C'-like indexing
							if (*((long long *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->end[i] = subset->start[i];
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j++; j < (int) data_size; ++j)
								if (*((long long *) data + j) > subset_double->end[i])
									break;
							subset->end[i] = j;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					} else {
						for (j = data_size - 1; j >= 0; --j)
							if (*((long long *) data + j) >= subset_double->start[i])
								break;
						if (j < 0) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too high\n", subset_double->start[i]);
							continue;
						} else if ((j == (int) data_size - 1) && (subset_double->end[i] < *((long long *) data + j))) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset interval is out of dimension range: %f is too low\n", subset_double->end[i]);
							continue;
						} else {
							subset->end[i] = j + 1;	// Non 'C'-like indexing
							if (*((long long *) data + j) > subset_double->end[i])	// Empty set
							{
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Subset cube is empty\n");
								subset->start[i] = subset->end[i];	// Non 'C'-like indexing
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
								continue;
							}
							for (j--; j >= 0; --j)
								if (*((long long *) data + j) > subset_double->end[i])
									break;
							subset->start[i] = j + 2;	// Non 'C'-like indexing
							if (subset->start[i] == subset->end[i])
								subset_double->type[i] = OPH_SUBSET_LIB_SINGLE;
						}
					}
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
					oph_subset_free(subset);
					oph_subset_double_free(subset_double);
					return OPH_SUBSET_LIB_SYSTEM_ERR;
			}
			subset->count[i] = 1 + (subset->end[i] - subset->start[i]);
			subset->total += subset->count[i];
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown data type\n");
		oph_subset_free(subset);
		oph_subset_double_free(subset_double);
		return OPH_SUBSET_LIB_DATA_ERR;
	}

	oph_subset_double_free(subset_double);

	if (!subset->total) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Subset cube is empty\n");
		oph_subset_free(subset);
		return OPH_SUBSET_LIB_OK;
	}

	size_t len;
	unsigned int actual_number = 0;
	char buffer[OPH_SUBSET_LIB_MAX_STRING_LENGTH], separator = 0, found_empty = 0;
	for (i = 0; i < (int) subset->number; ++i) {
		if (!subset->count[i]) {
			found_empty = 1;
			if (separator && (i == (int) subset->number - 1) && ((len = strlen(out_cond))))
				out_cond[len - 1] = 0;
			continue;
		}
		switch (subset->type[i]) {
			case OPH_SUBSET_LIB_SINGLE:
				snprintf(buffer, OPH_SUBSET_LIB_MAX_STRING_LENGTH, "%lld", subset->start[i]);
				break;
			case OPH_SUBSET_LIB_INTERVAL:
				snprintf(buffer, OPH_SUBSET_LIB_MAX_STRING_LENGTH, "%lld%s%lld", subset->start[i], OPH_SUBSET_LIB_PARAM_SEPARATOR, subset->end[i]);
				break;
			default:
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected subset type\n");
				oph_subset_free(subset);
				return OPH_SUBSET_LIB_SYSTEM_ERR;
		}
		strncat(out_cond, buffer, OPH_SUBSET_LIB_MAX_STRING_LENGTH);
		if (i < (int) subset->number - 1) {
			strncat(out_cond, OPH_SUBSET_LIB_SUBSET_SEPARATOR, OPH_SUBSET_LIB_MAX_STRING_LENGTH);
			separator = 1;
		}
		actual_number++;
	}

	if (!out_subset)
		oph_subset_free(subset);
	else {
		if (found_empty)	// Remake subset to be exported
		{
			oph_subset *subset2;
			oph_subset_init(&subset2);
			subset2->number = actual_number;
			subset2->type = (oph_subset_type *) malloc(subset2->number * sizeof(oph_subset_type));
			subset2->start = (unsigned long long *) malloc(subset2->number * sizeof(unsigned long long));
			subset2->end = (unsigned long long *) malloc(subset2->number * sizeof(unsigned long long));
			subset2->stride = (unsigned long long *) malloc(subset2->number * sizeof(unsigned long long));
			subset2->count = (unsigned long long *) malloc(subset2->number * sizeof(unsigned long long));
			subset2->total = subset->total;
			for (i = j = 0; i < (int) subset->number; ++i)
				if (subset->count[i]) {
					subset2->type[j] = subset->type[i];
					subset2->start[j] = subset->start[i];
					subset2->end[j] = subset->end[i];
					subset2->stride[j] = subset->stride[i];
					subset2->count[j] = subset->count[i];
					j++;
				}
			oph_subset_free(subset);
			*out_subset = subset2;
		} else
			*out_subset = subset;
	}

	pmesg(LOG_INFO, __FILE__, __LINE__, "Subset string is '%s' when expressed as indexes\n", out_cond);

	return OPH_SUBSET_LIB_OK;
}
