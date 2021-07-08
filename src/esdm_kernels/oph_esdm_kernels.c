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

#include "oph_esdm_kernels.h"

#define UNUSED(x) {(void)(x);}

typedef struct _stream_data_out_t {
	double value;
	uint64_t number;
} stream_data_out_t;

void *stream_func(esdm_dataspace_t * space, void *buff, void *user_ptr, void *esdm_fill_value)
{
	UNUSED(esdm_fill_value);

	if (!space || !buff || !user_ptr)
		return NULL;

	stream_data_t *stream_data = (stream_data_t *) user_ptr;
	if (!stream_data->operation)
		return NULL;

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
	stream_data_out_t *tmp = NULL;

	if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STREAM)) {

		// TODO: copy only the data related to the dataspace
		memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

		tmp = (stream_data_out_t *) malloc(sizeof(stream_data_out_t));
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
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MIN)) {

		tmp = (stream_data_out_t *) malloc(sizeof(stream_data_out_t));
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
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG)) {

		tmp = (stream_data_out_t *) malloc(sizeof(stream_data_out_t));
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
			return NULL;
		}
	}

	return tmp;
}

void reduce_func(esdm_dataspace_t * space, void *user_ptr, void *stream_func_out)
{
	stream_data_out_t *tmp = (stream_data_out_t *) stream_func_out;

	do {

		if (!space || !user_ptr || !tmp->number)
			break;

		esdm_type_t type = esdm_dataspace_get_type(space);
		stream_data_t *stream_data = (stream_data_t *) user_ptr;
		if (!stream_data->operation || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STREAM))
			break;

		if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

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

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG)) {

			if (!stream_data->valid) {
				stream_data->valid = 1;
				stream_data->value = 0;
				stream_data->number = 0;
			}
			stream_data->value += tmp->value;
			stream_data->number += tmp->number;

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
		}

	} while (0);

	if (stream_func_out)
		free(stream_func_out);
}
