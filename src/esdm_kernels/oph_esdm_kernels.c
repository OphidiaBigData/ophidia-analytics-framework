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

#include <stdlib.h>
#include <math.h>

#include "oph_esdm_kernels.h"

#define UNUSED(x) {(void)(x);}

typedef struct _oph_esdm_stream_data_out_t {
	double value;
	double value2;
	uint64_t number;
} oph_esdm_stream_data_out_t;

int oph_esdm_is_a_reduce_func(const char *operation)
{
	if (!operation)
		return 0;

	if (!strcmp(operation, OPH_ESDM_FUNCTION_MAX))
		return 1;
	if (!strcmp(operation, OPH_ESDM_FUNCTION_MIN))
		return 2;
	if (!strcmp(operation, OPH_ESDM_FUNCTION_AVG))
		return 3;
	if (!strcmp(operation, OPH_ESDM_FUNCTION_SUM))
		return 4;
	if (!strcmp(operation, OPH_ESDM_FUNCTION_STD))
		return 5;
	if (!strcmp(operation, OPH_ESDM_FUNCTION_VAR))
		return 6;

	return 0;
}

void *oph_esdm_stream_func(esdm_dataspace_t * space, void *buff, void *user_ptr, void *esdm_fill_value)
{
	UNUSED(esdm_fill_value);

	if (!space || !buff || !user_ptr)
		return NULL;

	oph_esdm_stream_data_t *stream_data = (oph_esdm_stream_data_t *) user_ptr;
	if (!stream_data->operation)
		return NULL;

	char *args = stream_data->args ? strdup(stream_data->args) : NULL;	// Copy for strtok

	int64_t i, idx, ndims = esdm_dataspace_get_dims(space);
	int64_t const *s = esdm_dataspace_get_size(space);
	//int64_t const *si = esdm_dataspace_get_offset(space);
	int64_t ci[ndims], ei[ndims];
	for (i = 0; i < ndims; ++i) {
		ci[i] = 0;	// + si[i]
		ei[i] = s[i];	// + si[i]
	}

	uint64_t k = 1, n = esdm_dataspace_element_count(space);
	esdm_type_t type = esdm_dataspace_get_type(space);
	void *fill_value = stream_data->fill_value;
	oph_esdm_stream_data_out_t *tmp = NULL;

	if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NOP) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STREAM)) {

		// TODO: copy only the data related to the dataspace
		memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->value = 0;
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_VAR)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->value = 0;
		tmp->value2 = 0;
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM_SCALAR)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MUL_SCALAR)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ABS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? fabs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? fabs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SQRT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SQR)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_CEIL)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_FLOOR) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_INT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ROUND) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NINT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_POW)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_EXP)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_LOG)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_LOG10)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_COS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_TAN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ASIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ACOS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ATAN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SINH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_COSH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_TANH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_RECI)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NOT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	}

	if (args)
		free(args);

	return tmp;
}

void oph_esdm_reduce_func(esdm_dataspace_t * space, void *user_ptr, void *stream_func_out)
{
	oph_esdm_stream_data_out_t *tmp = (oph_esdm_stream_data_out_t *) stream_func_out;

	do {

		if (!space || !user_ptr || (tmp && !tmp->number))
			break;

		esdm_type_t type = esdm_dataspace_get_type(space);
		oph_esdm_stream_data_t *stream_data = (oph_esdm_stream_data_t *) user_ptr;
		if (!stream_data->operation)
			break;

		if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

			if (!tmp)
				break;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) tmp->value;
				if (stream_data->valid) {
					char pre = *(char *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(char));
				} else {
					memcpy(stream_data->buff, &v, sizeof(char));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) tmp->value;
				if (stream_data->valid) {
					short pre = *(short *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(short));
				} else {
					memcpy(stream_data->buff, &v, sizeof(short));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) tmp->value;
				if (stream_data->valid) {
					int pre = *(int *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(int));
				} else {
					memcpy(stream_data->buff, &v, sizeof(int));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) tmp->value;
				if (stream_data->valid) {
					long long pre = *(long long *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(long long));
				} else {
					memcpy(stream_data->buff, &v, sizeof(long long));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) tmp->value;
				if (stream_data->valid) {
					float pre = *(float *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(float));
				} else {
					memcpy(stream_data->buff, &v, sizeof(float));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) tmp->value;
				if (stream_data->valid) {
					double pre = *(double *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(double));
				} else {
					memcpy(stream_data->buff, &v, sizeof(double));
					stream_data->valid = 1;
				}

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MIN)) {

			if (!tmp)
				break;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) tmp->value;
				if (stream_data->valid) {
					char pre = *(char *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(char));
				} else {
					memcpy(stream_data->buff, &v, sizeof(char));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) tmp->value;
				if (stream_data->valid) {
					short pre = *(short *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(short));
				} else {
					memcpy(stream_data->buff, &v, sizeof(short));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) tmp->value;
				if (stream_data->valid) {
					int pre = *(int *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(int));
				} else {
					memcpy(stream_data->buff, &v, sizeof(int));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) tmp->value;
				if (stream_data->valid) {
					long long pre = *(long long *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(long long));
				} else {
					memcpy(stream_data->buff, &v, sizeof(long long));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) tmp->value;
				if (stream_data->valid) {
					float pre = *(float *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(float));
				} else {
					memcpy(stream_data->buff, &v, sizeof(float));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) tmp->value;
				if (stream_data->valid) {
					double pre = *(double *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(double));
				} else {
					memcpy(stream_data->buff, &v, sizeof(double));
					stream_data->valid = 1;
				}

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM)) {

			if (!tmp)
				break;

			if (!stream_data->valid) {
				stream_data->valid = 1;
				stream_data->value = 0;
				stream_data->number = 0;
			}
			stream_data->value += tmp->value;
			if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG))
				stream_data->number += tmp->number;
			else
				stream_data->number = 1;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(char));

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(short));

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(int));

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(long long));

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(float));

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(double));

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_VAR)) {

			if (!tmp)
				break;

			if (!stream_data->valid) {
				stream_data->valid = 1;
				stream_data->value = 0;
				stream_data->value2 = 0;
				stream_data->number = 0;
			}
			stream_data->value += tmp->value;
			stream_data->value2 += tmp->value2;
			stream_data->number += tmp->number;

			double result = (stream_data->value2 - stream_data->value * stream_data->value / stream_data->number) / stream_data->number;
			if (stream_data->number > 1)
				result *= stream_data->number / (stream_data->number - 1.0);
			if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD))
				result = sqrt(result);

			if (type == SMD_DTYPE_INT8) {

				char v = (char) result;
				memcpy(stream_data->buff, &v, sizeof(char));

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) result;
				memcpy(stream_data->buff, &v, sizeof(short));

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) result;
				memcpy(stream_data->buff, &v, sizeof(int));

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) result;
				memcpy(stream_data->buff, &v, sizeof(long long));

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) result;
				memcpy(stream_data->buff, &v, sizeof(float));

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) result;
				memcpy(stream_data->buff, &v, sizeof(double));

			}
		}

	} while (0);

	if (stream_func_out)
		free(stream_func_out);
}
