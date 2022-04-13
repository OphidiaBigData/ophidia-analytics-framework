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

#include "oph_esdm_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <zlib.h>
#include <math.h>
#include <ctype.h>

#include "oph_dimension_library.h"
#include "oph-lib-binary-io.h"
#include "debug.h"

#include "oph_log_error_codes.h"

#include "oph_esdm_kernels.h"

#define OPH_ESDM_MEMORY_BLOCK 1048576
#define OPH_ESDM_BLOCK_SIZE 524288	// Maximum size that could be transfered
#define OPH_ESDM_BLOCK_ROWS 1000	// Maximum number of lines that could be transfered

#define OPH_ESDM_CONCAT_ROW ",?"
#define OPH_ESDM_CONCAT_COMPRESSED_ROW ",oph_uncompress('','',?)"
#define OPH_ESDM_CONCAT_TYPE "oph_"
#define OPH_ESDM_CONCAT_PLUGIN "oph_concat2('%s','oph_%s',measure%s)"
#define OPH_ESDM_CONCAT_PLUGIN_COMPR "oph_compress('','',oph_concat2('%s','oph_%s',oph_uncompress('','',measure)%s))"
#define OPH_ESDM_CONCAT_WHERE "id_dim >= %lld AND id_dim <= %lld"

#define OPH_ESDM_CONCAT_FIELD "oph_concat2('oph_%s|oph_%s','oph_%s', %s.measure, %s.measure)"
#define OPH_ESDM_CONCAT_FIELD_COMPR "oph_compress('','', oph_concat2('oph_%s|oph_%s','oph_%s', oph_uncompress('','', %s.measure), %s.measure))"
#define OPH_ESDM_CONCAT_WHERE_FILE "%s.id_dim = %s.id_dim"

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
#include "clients/taketime.h"
static int timeval_add(res, x, y)
struct timeval *res, *x, *y;
{
	res->tv_sec = x->tv_sec + y->tv_sec;
	res->tv_usec = x->tv_usec + y->tv_usec;
	while (res->tv_usec > MILLION) {
		res->tv_usec -= MILLION;
		res->tv_sec++;
	}
	return 0;
}
#endif

int _oph_esdm_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, int64_t ** id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (int64_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_esdm_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else
		*(id[i]) = (int64_t) (residual + 1);
	return OPH_ESDM_SUCCESS;
}

int oph_esdm_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, int64_t ** id)
{
	if (n > 0) {
		int i;
		unsigned long total = 1;
		for (i = 0; i < n; ++i)
			total *= sizemax[i];
		_oph_esdm_get_dimension_id(ID - 1, total, sizemax, id, 0, n);
	}
	return OPH_ESDM_SUCCESS;
}

int _oph_esdm_get_next_id(int64_t * id, int64_t * sizemax, int i, int n)
{
	if (i < 0)
		return 1;	// Overflow
	(id[i])++;
	if (id[i] >= sizemax[i]) {
		id[i] = 0;
		return _oph_esdm_get_next_id(id, sizemax, i - 1, n);
	}
	return OPH_ESDM_SUCCESS;
}

int oph_esdm_get_next_id(int64_t * id, int64_t * sizemax, int n)
{
	return _oph_esdm_get_next_id(id, sizemax, n - 1, n);
}

int oph_esdm_get_esdm_type(char *in_c_type, esdm_type_t * type_nc)
{
	if (!strcasecmp(in_c_type, OPH_COMMON_BYTE_TYPE)) {
		*type_nc = SMD_DTYPE_INT8;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_SHORT_TYPE)) {
		*type_nc = SMD_DTYPE_INT16;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_INT_TYPE)) {
		*type_nc = SMD_DTYPE_INT32;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_LONG_TYPE)) {
		*type_nc = SMD_DTYPE_INT64;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_FLOAT_TYPE)) {
		*type_nc = SMD_DTYPE_FLOAT;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_DOUBLE_TYPE)) {
		*type_nc = SMD_DTYPE_DOUBLE;
		return OPH_ESDM_SUCCESS;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_BIT_TYPE)) {
		*type_nc = SMD_DTYPE_INT8;
		return OPH_ESDM_SUCCESS;
	}
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type '%s' not supported\n", in_c_type);
	return OPH_ESDM_ERROR;
}

int oph_esdm_set_esdm_type(char *out_c_type, esdm_type_t type_nc)
{
	if (type_nc == SMD_DTYPE_INT8)
		strcpy(out_c_type, OPH_COMMON_BYTE_TYPE);
	else if (type_nc == SMD_DTYPE_INT16)
		strcpy(out_c_type, OPH_COMMON_SHORT_TYPE);
	else if (type_nc == SMD_DTYPE_INT32)
		strcpy(out_c_type, OPH_COMMON_INT_TYPE);
	else if (type_nc == SMD_DTYPE_INT64)
		strcpy(out_c_type, OPH_COMMON_LONG_TYPE);
	else if (type_nc == SMD_DTYPE_FLOAT)
		strcpy(out_c_type, OPH_COMMON_FLOAT_TYPE);
	else if (type_nc == SMD_DTYPE_DOUBLE)
		strcpy(out_c_type, OPH_COMMON_DOUBLE_TYPE);
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not supported\n");
		return OPH_ESDM_ERROR;
	}

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_compare_types(int id_container, esdm_type_t var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE])
{
	if (!var_type || !dim_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}
	if (var_type == SMD_DTYPE_INT8) {
		if (strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else if (var_type == SMD_DTYPE_INT16) {
		if (strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else if (var_type == SMD_DTYPE_INT32) {
		if (strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else if (var_type == SMD_DTYPE_INT64) {
		if (strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else if (var_type == SMD_DTYPE_FLOAT) {
		if (strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else if (var_type == SMD_DTYPE_DOUBLE) {
		if (strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return OPH_ESDM_ERROR;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		return OPH_ESDM_ERROR;
	}
	return OPH_ESDM_SUCCESS;
}

int oph_esdm_update_dim_with_esdm_metadata(ophidiadb * oDB, oph_odb_dimension * time_dim, int id_vocabulary, int id_container_out, ESDM_var * measure)
{
	MYSQL_RES *key_list = NULL;
	MYSQL_ROW row = NULL;

	int num_rows = 0;
	if (oph_odb_meta_find_metadatakey_list(oDB, id_vocabulary, &key_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive key list\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_READ_KEY_LIST);
		if (key_list)
			mysql_free_result(key_list);
		return OPH_ESDM_ERROR;
	}
	num_rows = mysql_num_rows(key_list);

	if (num_rows)		// The vocabulary is not empty
	{
		int i, num_attr = 0;
		char *key, *variable, *template;
		char value[OPH_COMMON_BUFFER_LEN], svalue[OPH_COMMON_BUFFER_LEN];
		smd_basic_type_t xtype;
		esdm_dataset_t *dataset = NULL;

		char **keys = (char **) calloc(num_rows, sizeof(char *));
		if (!keys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate key list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_READ_KEY_LIST);
			mysql_free_result(key_list);
			return OPH_ESDM_ERROR;
		}
		char **values = (char **) calloc(num_rows, sizeof(char *));
		if (!values) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate key list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_READ_KEY_LIST);
			mysql_free_result(key_list);
			free(keys);
			return OPH_ESDM_ERROR;
		}

		smd_attr_t *md = NULL, *attribute = NULL;

		while ((row = mysql_fetch_row(key_list))) {
			if (row[4])	// If the attribute is required and a template is given
			{
				key = row[1];
				variable = row[2];
				template = row[4];

				memset(value, 0, OPH_COMMON_BUFFER_LEN);
				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);
				dataset = NULL;
				attribute = NULL;
				xtype = SMD_TYPE_UNKNOWN;

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Try to load '%s:%s'\n", variable ? variable : "", key);

				if (variable) {
					if (esdm_dataset_open(measure->container, variable, ESDM_MODE_FLAG_READ, &dataset)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering metadata of variable '%s'\n", variable);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_ATTRIBUTE_ERROR);
						break;
					}
					if (esdm_dataset_get_attributes(dataset, &md)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering metadata of variable '%s'\n", variable);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_ATTRIBUTE_ERROR);
						esdm_dataset_close(dataset);
						break;
					}
					if ((attribute = smd_attr_get_child_by_name(md, key)))
						xtype = smd_attr_get_type(attribute);
				} else {
					if (esdm_container_get_attributes(measure->container, &md)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering metadata of the container\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_ATTRIBUTE_ERROR);
						break;
					}
					if ((attribute = smd_attr_get_child_by_name(md, key)))
						xtype = smd_attr_get_type(attribute);
				}

				if (!attribute) {
					if (dataset)
						esdm_dataset_close(dataset);
					continue;
				}

				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

				switch (xtype) {
					case SMD_TYPE_CHAR:{
							char value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%c", value);
							break;
						}
					case SMD_TYPE_INT8:{
							int8_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT8:{
							uint8_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT16:{
							int16_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT16:{
							uint16_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT32:{
							int32_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT32:{
							uint32_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT64:{
							int64_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (long long) value);
							break;
						}
					case SMD_TYPE_UINT64:{
							uint64_t value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (unsigned long long) value);
							break;
						}
					case SMD_TYPE_FLOAT:{
							float value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_DOUBLE:{
							double value;
							smd_attr_copy_value(attribute, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_STRING:
					case SMD_TYPE_ARRAY:
						strncpy(svalue, (char *) smd_attr_get_value(attribute),
							1 + attribute->type->size < OPH_COMMON_BUFFER_LEN ? attribute->type->size : OPH_COMMON_BUFFER_LEN - 1);
						break;
					default:;
				}

				if (dataset)
					esdm_dataset_close(dataset);

				if (!strlen(svalue))
					continue;

				keys[num_attr] = strdup(template);
				values[num_attr++] = strdup(svalue);
			}
		}
		if (row) {
			mysql_free_result(key_list);
			for (i = 0; i < num_attr; ++i) {
				if (keys[i])
					free(keys[i]);
				if (values[i])
					free(values[i]);
			}
			free(keys);
			free(values);
			return OPH_ESDM_ERROR;
		}

		if (oph_odb_dim_update_time_dimension(time_dim, keys, values, num_attr)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension metadata\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_UPDATE_TIME_ERROR);
			mysql_free_result(key_list);
			for (i = 0; i < num_attr; ++i) {
				if (keys[i])
					free(keys[i]);
				if (values[i])
					free(values[i]);
			}
			free(keys);
			free(values);
			return OPH_ESDM_ERROR;
		}

		for (i = 0; i < num_attr; ++i) {
			if (keys[i])
				free(keys[i]);
			if (values[i])
				free(values[i]);
		}
		free(keys);
		free(values);
	}

	mysql_free_result(key_list);

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_get_dim_array(int id_container, esdm_dataset_t * dataset, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, int start_index, int end_index, char **dim_array)
{
	if (!dim_type || !dim_size || !dim_array) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter %d\n", dim_id);
		return OPH_ESDM_ERROR;
	}
	if (!dataset)
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Return an array of integers\n");
	*dim_array = NULL;

	//Assume that the coordinate variable related to a dimension depends by one dimension only (Ex. lat(lat) not lat(x,y))  
	int64_t start[1];
	int64_t count[1];
	start[0] = start_index;
	count[0] = 1;
	if (start_index < end_index)
		count[0] = 1 + end_index - start_index;
	else if (start_index > end_index) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported order for indexes\n");
		return OPH_ESDM_ERROR;
	}

	void *binary_dim = NULL;
	if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(char));
	else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(short));
	else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(int));
	else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(float));
	else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(double));
	else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
		binary_dim = (void *) malloc(dim_size * sizeof(long long));
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		return OPH_ESDM_ERROR;
	}
	if (!binary_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Memory error\n");
		return OPH_ESDM_ERROR;
	}

	if (dataset && (dim_id >= 0)) {

		esdm_type_t type_nc = SMD_DTYPE_UNKNOWN;
		if (oph_esdm_get_esdm_type((char *) dim_type, &type_nc)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return OPH_ESDM_ERROR;
		}

		esdm_dataspace_t *subspace = NULL;
		if ((esdm_dataspace_create_full(1, count, start, type_nc, &subspace))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return OPH_ESDM_ERROR;
		}

		if ((esdm_read(dataset, binary_dim, subspace))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return OPH_ESDM_ERROR;
		}

	} else {

		int value;
		if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			char *f = (char *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (char) value;
		} else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			short *f = (short *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (short) value;
		} else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			int *f = (int *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (int) value;
		} else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			float *f = (float *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (float) value;
		} else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			double *f = (double *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (double) value;
		} else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
			long long *f = (long long *) binary_dim;
			for (value = start_index; value <= end_index; ++value, ++f)
				*f = (long long) value;
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
			return OPH_ESDM_ERROR;
		}

	}

	*dim_array = (char *) binary_dim;

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_index_by_value(int id_container, ESDM_var * measure, int dim_id, esdm_type_t dim_type, int dim_size, char *value, int want_start, double offset, int *valorder, int *coord_index,
			    char out_of_bound)
{
	if (!dim_size || !value || !coord_index || !valorder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	void *binary_dim = NULL;
	int nearest_point = want_start > 0;
	//I need to evaluate the order of the dimension values: ascending or descending
	int order = 1;		//Default is ascending
	int exact_value = 0;	//If I find the exact value
	int i, end_dim_size = dim_size - 1;

	if (dim_type == SMD_DTYPE_INT8)
		binary_dim = (void *) malloc(sizeof(char) * dim_size);
	else if (dim_type == SMD_DTYPE_INT16)
		binary_dim = (void *) malloc(sizeof(short) * dim_size);
	else if (dim_type == SMD_DTYPE_INT32)
		binary_dim = (void *) malloc(sizeof(int) * dim_size);
	else if (dim_type == SMD_DTYPE_INT64)
		binary_dim = (void *) malloc(sizeof(long long) * dim_size);
	else if (dim_type == SMD_DTYPE_FLOAT)
		binary_dim = (void *) malloc(sizeof(float) * dim_size);
	else if (dim_type == SMD_DTYPE_DOUBLE)
		binary_dim = (void *) malloc(sizeof(double) * dim_size);
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not supported\n");
		return OPH_ESDM_ERROR;
	}
	if (!binary_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
		return OPH_ESDM_ERROR;
	}

	int64_t const *size = esdm_dataset_get_actual_size(measure->dim_dataset[dim_id]);
	for (i = 0; i < measure->dim_dspace[dim_id]->dims; i++)
		if (!measure->dim_dspace[dim_id]->size[i])
			measure->dim_dspace[dim_id]->size[i] = size[i];

	if (esdm_read(measure->dim_dataset[dim_id], binary_dim, measure->dim_dspace[dim_id])) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
		free(binary_dim);
		return OPH_ESDM_ERROR;
	}

	if (dim_type == SMD_DTYPE_INT8) {

		char *array_val = binary_dim, value_ = (char) (strtol(value, (char **) NULL, 10));
		if (offset)
			value_ += (char) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_BYTE){
	else if (dim_type == SMD_DTYPE_INT16) {

		short *array_val = binary_dim, value_ = (short) (strtol(value, (char **) NULL, 10));
		if (offset)
			value_ += (short) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_SHORT){
	else if (dim_type == SMD_DTYPE_INT32) {

		int *array_val = binary_dim, value_ = (int) (strtol(value, (char **) NULL, 10));
		if (offset)
			value_ += (int) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %d out of the boundaries [%d, %d]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_INT){
	else if (dim_type == SMD_DTYPE_INT64) {

		long long *array_val = binary_dim, value_ = (long long) (strtoll(value, (char **) NULL, 10));
		if (offset)
			value_ += (long long) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %lld out of the boundaries [%lld, %lld]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %lld out of the boundaries [%lld, %lld]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_INT64){
	else if (dim_type == SMD_DTYPE_FLOAT) {

		float *array_val = binary_dim, value_ = (float) (strtof(value, NULL));
		if (offset)
			value_ += (float) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %f out of the boundaries [%f, %f]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %f out of the boundaries [%f, %f]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_FLOAT){
	else if (dim_type == SMD_DTYPE_DOUBLE) {

		double *array_val = binary_dim, value_ = (double) (strtod(value, NULL));
		if (offset)
			value_ += want_start ? -offset : offset;
		//Check order of dimension values
		if (array_val[0] > array_val[end_dim_size]) {	//Descending
			order = 0;
			if (out_of_bound && (value_ <= array_val[0]) && (value_ >= array_val[end_dim_size]))
				out_of_bound = 0;
		} else if (out_of_bound && (value_ >= array_val[0]) && (value_ <= array_val[end_dim_size]))
			out_of_bound = 0;
		if (out_of_bound) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Value %f out of the boundaries [%f, %f]\n", value_, array_val[0], array_val[end_dim_size]);
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Value %f out of the boundaries [%f, %f]\n", value_, array_val[0], array_val[end_dim_size]);
			free(binary_dim);
			return OPH_ESDM_BOUND_ERROR;
		}
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_) {
						exact_value = 1;
					}
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i > 0) && ((i == dim_size) || (value_ - array_val[i - 1] < array_val[i] - value_)))
						i--;
				} else if (want_start) {
					if (i == dim_size)
						i--;
				} else if (i)
					i--;
			}
		}		//End if(order)
		else {
			//Descending
			for (i = dim_size - 1; i >= 0; i--) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (!exact_value) {
				if (nearest_point) {
					if ((i < dim_size - 1) && ((i < 0) || (value_ - array_val[i + 1] < array_val[i] - value_)))
						i++;
				} else if (want_start) {
					if (i < 0)
						i = 0;
				} else if (i < dim_size - 1)
					i++;
			}
		}		//End else if(order)
	}			//End if(dim_type == NC_DOUBLE){
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		free(binary_dim);
		return OPH_ESDM_ERROR;
	}

	free(binary_dim);
	*coord_index = i;
	*valorder = order;
	return OPH_ESDM_SUCCESS;
}

int oph_esdm_check_subset_string(char *curfilter, int i, ESDM_var * measure, int is_index, double offset, char out_of_bound)
{
	int ii, error = 0;
	char *endfilter = strchr(curfilter, OPH_DIM_SUBSET_SEPARATOR2);
	if (!endfilter && !offset) {
		//Only single point
		//Check curfilter
		if (strlen(curfilter) < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			return OPH_ESDM_ERROR;
		}
		if (is_index) {
			//Input filter is index
			for (ii = 0; ii < (int) strlen(curfilter); ii++) {
				if (!isdigit(curfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer values allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
					return OPH_ESDM_ERROR;
				}
			}
			measure->dims_start_index[i] = (int) (strtol(curfilter, (char **) NULL, 10));
			measure->dims_end_index[i] = measure->dims_start_index[i];
		} else {
			//Input filter is value
			for (ii = 0; ii < (int) strlen(curfilter); ii++) {
				if (ii == 0) {
					if (!isdigit(curfilter[ii]) && curfilter[ii] != '-') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter: %s\n", curfilter);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				} else {
					if (!isdigit(curfilter[ii]) && curfilter[ii] != '.') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter: %s\n", curfilter);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				}
			}
			//End of checking filter

			// Load dimension metadata is needed
			if (!measure->dim_dataset[i])
				if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
					return OPH_ESDM_ERROR;
				}
			if (!measure->dim_dspace[i])
				if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
					return OPH_ESDM_ERROR;
				}
			if (measure->dim_dspace[i]->dims != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return OPH_ESDM_ERROR;
			}

			int coord_index = -1;
			int want_start = 1;	//Single point, it is the same
			int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if ((error = oph_esdm_index_by_value
			     (OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], curfilter, want_start, 0, &order, &coord_index, out_of_bound))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
				return error;
			}
			//Value not found
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return OPH_ESDM_BOUND_ERROR;
			}

			measure->dims_start_index[i] = coord_index;
			measure->dims_end_index[i] = measure->dims_start_index[i];
		}
	} else {
		//Start and end point
		char *startfilter = curfilter;
		if (endfilter) {
			*endfilter = '\0';
			endfilter++;
		} else
			endfilter = startfilter;

		if (strlen(startfilter) < 1 || strlen(endfilter) < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			return OPH_ESDM_ERROR;
		}
		if (is_index) {
			//Input filter is index         
			for (ii = 0; ii < (int) strlen(startfilter); ii++) {
				if (!isdigit(startfilter[ii]) && (startfilter[ii] != '.') && (startfilter[ii] != '-')) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
					return OPH_ESDM_ERROR;
				}
			}

			for (ii = 0; ii < (int) strlen(endfilter); ii++) {
				if (!isdigit(endfilter[ii]) && (endfilter[ii] != '.') && (endfilter[ii] != '-')) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
					return OPH_ESDM_ERROR;
				}
			}
			measure->dims_start_index[i] = (int) (strtol(startfilter, (char **) NULL, 10));
			measure->dims_end_index[i] = (int) (strtol(endfilter, (char **) NULL, 10));
		} else {
			//Input filter is a value
			for (ii = 0; ii < (int) strlen(startfilter); ii++) {
				if (ii == 0) {
					if (!isdigit(startfilter[ii]) && (startfilter[ii] != '-')) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				} else {
					if (!isdigit(startfilter[ii]) && (startfilter[ii] != '.')) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				}
			}
			for (ii = 0; ii < (int) strlen(endfilter); ii++) {
				if (ii == 0) {
					if (!isdigit(endfilter[ii]) && (endfilter[ii] != '-')) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				} else {
					if (!isdigit(endfilter[ii]) && (endfilter[ii] != '.')) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
						return OPH_ESDM_ERROR;
					}
				}
			}
			//End of checking filter

			// Load dimension metadata is needed
			if (!measure->dim_dataset[i])
				if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
					return OPH_ESDM_ERROR;
				}
			if (!measure->dim_dspace[i])
				if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
					return OPH_ESDM_ERROR;
				}
			if (measure->dim_dspace[i]->dims != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return OPH_ESDM_ERROR;
			}

			int coord_index = -1;
			int want_start = -1;
			int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if ((error = oph_esdm_index_by_value
			     (OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], startfilter, want_start, offset, &order, &coord_index, out_of_bound))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING, "");
				return error;
			}
			//Value too big
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return OPH_ESDM_BOUND_ERROR;
			}
			measure->dims_start_index[i] = coord_index;

			coord_index = -1;
			want_start = 0;
			order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if ((error = oph_esdm_index_by_value
			     (OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], endfilter, want_start, offset, &order, &coord_index, out_of_bound))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return error;
			}
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				return OPH_ESDM_BOUND_ERROR;
			}
			//oph_esdm_index_by value returns the index I need considering the order of the dimension values (ascending/descending)
			measure->dims_end_index[i] = coord_index;
			//Descending order; I need to swap start and end index
			if (!order) {
				int temp_ind = measure->dims_start_index[i];
				measure->dims_start_index[i] = measure->dims_end_index[i];
				measure->dims_end_index[i] = temp_ind;
			}

		}
	}

	return OPH_ESDM_SUCCESS;
}

int _oph_esdm_cache_to_buffer(short int tot_dim_number, short int curr_dim, unsigned int *counters, unsigned int *limits, unsigned int *products, long long *index, char *binary_cache,
			      char *binary_insert, size_t sizeof_var)
{
	int i = 0;
	long long addr = 0;

	if (tot_dim_number > curr_dim) {

		if (curr_dim != 0)
			counters[curr_dim] = 0;
		while (counters[curr_dim] < limits[curr_dim]) {
			_oph_esdm_cache_to_buffer(tot_dim_number, curr_dim + 1, counters, limits, products, index, binary_cache, binary_insert, sizeof_var);
			counters[curr_dim]++;
		}
		return OPH_ESDM_SUCCESS;
	}

	if (tot_dim_number == curr_dim) {
		addr = 0;
		for (i = 0; i < tot_dim_number; i++) {
			addr += counters[i] * products[i];
		}

		memcpy(binary_insert + (*index) * sizeof_var, binary_cache + addr * sizeof_var, sizeof_var);
		(*index)++;
		return OPH_ESDM_SUCCESS;
	}

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var)
{
	long long index = 0;
	return _oph_esdm_cache_to_buffer(tot_dim_number, 0, counters, limits, products, &index, binary_cache, binary_insert, sizeof_var);
}

int oph_esdm_populate_fragment2(oph_ioserver_handler * server, oph_odb_fragment * frag, int tuplexfrag_number, int array_length, int compressed, ESDM_var * measure)
{
	if (!frag || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_ESDM_ERROR;
	}

	char type_flag = '\0';
	long long sizeof_var = 0;
	esdm_type_t type_nc = measure->dspace->type;
	if (type_nc == SMD_DTYPE_INT8) {
		type_flag = OPH_ESDM_BYTE_FLAG;
		sizeof_var = (array_length) * sizeof(char);
	} else if (type_nc == SMD_DTYPE_INT16) {
		type_flag = OPH_ESDM_SHORT_FLAG;
		sizeof_var = (array_length) * sizeof(short);
	} else if (type_nc == SMD_DTYPE_INT32) {
		type_flag = OPH_ESDM_INT_FLAG;
		sizeof_var = (array_length) * sizeof(int);
	} else if (type_nc == SMD_DTYPE_INT64) {
		type_flag = OPH_ESDM_LONG_FLAG;
		sizeof_var = (array_length) * sizeof(long long);
	} else if (type_nc == SMD_DTYPE_FLOAT) {
		type_flag = OPH_ESDM_FLOAT_FLAG;
		sizeof_var = (array_length) * sizeof(float);
	} else {
		type_flag = OPH_ESDM_DOUBLE_FLAG;
		sizeof_var = (array_length) * sizeof(double);
	}

	//Compute number of tuples per insert (regular case)
	unsigned long long regular_rows = 0, regular_times = 0, remainder_rows = 0, jj = 0, l;

	if (sizeof_var >= OPH_ESDM_BLOCK_SIZE) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
	} else if (tuplexfrag_number * sizeof_var <= OPH_ESDM_BLOCK_SIZE) {
		//Do single insert
		if (tuplexfrag_number <= OPH_ESDM_BLOCK_ROWS) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
		} else {
			regular_rows = OPH_ESDM_BLOCK_ROWS;
			regular_times = (long long) tuplexfrag_number / regular_rows;
			remainder_rows = (long long) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var) >= OPH_ESDM_BLOCK_ROWS ? OPH_ESDM_BLOCK_ROWS : (long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var));
		regular_times = (long long) tuplexfrag_number / regular_rows;
		remainder_rows = (long long) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	char *insert_query = (remainder_rows > 0 ? OPH_DC_SQ_MULTI_INSERT_FRAG : OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL);
	long long query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows;

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ESDM_ERROR;
	}

	int n = snprintf(query_string, query_size, insert_query, frag->fragment_name) - 1;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		for (jj = 0; jj < regular_rows; jj++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
		}
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
		for (jj = 0; jj < regular_rows; jj++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
		}
	}
	query_string[n - 1] = ';';
	query_string[n] = 0;

	unsigned long long *idDim = (unsigned long long *) calloc(regular_rows, sizeof(unsigned long long));
	if (!(idDim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		return OPH_ESDM_ERROR;
	}
	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		return OPH_ESDM_ERROR;
	}

	unsigned long long c_arg = regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		return OPH_ESDM_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_ESDM_ERROR;
		}
	}
	args[c_arg] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[2 * ii]->arg_length = sizeof(unsigned long long);
		args[2 * ii]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[2 * ii]->arg_is_null = 0;
		args[2 * ii]->arg = (unsigned long long *) (idDim + ii);
		args[2 * ii + 1]->arg_length = sizeof_var;
		args[2 * ii + 1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[2 * ii + 1]->arg_is_null = 0;
		args[2 * ii + 1]->arg = (char *) (binary + sizeof_var * ii);
		idDim[ii] = frag->key_start + ii;
	}

	if (oph_ioserver_setup_query(server, query_string, regular_times, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_ESDM_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	int64_t *start = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	int64_t *count = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	//Sort start in base of oph_level of explicit dimension
	int64_t **start_pointer = (int64_t **) malloc((measure->nexp) * sizeof(int64_t *));
	//Set count
	short int i;
	for (i = 0; i < measure->ndims; i++) {
		//Explicit
		if (measure->dims_type[i]) {
			count[i] = 1;
		} else {
			//Implicit
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				count[i] = 1;
			else
				count[i] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			start[i] = measure->dims_start_index[i];
		}
	}
	//Check
	char check_for_reduce_func = !oph_esdm_is_a_reduce_func(measure->operation);
	int total = 1;
	if (check_for_reduce_func) {
		for (i = 0; i < measure->ndims; i++)
			if (!measure->dims_type[i])
				total *= count[i];
	}

	if (total != array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	short int flag = 0;
	short int curr_lev;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		flag = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				//Modified to allow subsetting
				if (measure->dims_start_index[i] == measure->dims_end_index[i])
					sizemax[curr_lev - 1] = 1;
				else
					sizemax[curr_lev - 1] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				start_pointer[curr_lev - 1] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			free(binary);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}


	int j = 0;

	//Flag set to 0 if implicit dimensions are not in the order specified in the file
	short int imp_dim_ordered = 1;
	//Check if implicit are ordered
	curr_lev = 0;
	for (i = 0; i < measure->ndims; i++) {
		if (measure->dims_type[i] == 0) {
			if (measure->dims_oph_level[i] < curr_lev) {
				imp_dim_ordered = 0;
				break;
			}
			curr_lev = measure->dims_oph_level[i];
		}
	}

	//Create tmp binary array
	char *binary_tmp = NULL;

	int tmp_index = 0;
	unsigned int *counters = NULL;
	unsigned int *products = NULL;
	unsigned int *limits = NULL;

	//Create a binary array to store the tmp row
	if (!imp_dim_ordered) {
		if (type_flag == OPH_ESDM_BYTE_FLAG)
			res = oph_iob_bin_array_create_b(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_SHORT_FLAG)
			res = oph_iob_bin_array_create_s(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_INT_FLAG)
			res = oph_iob_bin_array_create_i(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_LONG_FLAG)
			res = oph_iob_bin_array_create_l(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_FLOAT_FLAG)
			res = oph_iob_bin_array_create_f(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		else
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
			free(binary);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
		//Prepare structures for buffer insert update
		tmp_index = 0;
		counters = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned int));
		int *file_indexes = (int *) malloc((measure->nimp) * sizeof(int));
		products = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		limits = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < measure->ndims; i++) {
			//Implicit
			if (!measure->dims_type[i]) {
				tmp_index = measure->dims_oph_level[i] - 1;	//Start from 0
				counters[tmp_index] = 0;
				products[tmp_index] = 1;
				limits[tmp_index] = check_for_reduce_func ? count[i] : 1;
				file_indexes[tmp_index] = k++;
			}
		}
		//Compute products
		for (k = 0; k < measure->nimp; k++) {
			//Last dimension in file has product 1
			for (i = (file_indexes[k] + 1); i < measure->nimp; i++) {
				flag = 0;
				//For each index following multiply
				for (j = 0; j < measure->nimp; j++) {
					if (file_indexes[j] == i) {
						products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					free(query_string);
					free(idDim);
					free(binary);
					if (binary_tmp)
						free(binary_tmp);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					oph_ioserver_free_query(server, query);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					free(counters);
					free(file_indexes);
					free(products);
					free(limits);
					return OPH_ESDM_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;
	esdm_dataspace_t *subspace = NULL;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type], *pointer = NULL;

	if (esdm_dataset_get_fill_value(measure->dataset, fill_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		free(query_string);
		free(idDim);
		free(binary);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		if (binary_tmp)
			free(binary_tmp);
		if (counters)
			free(counters);
		if (products)
			free(products);
		if (limits)
			free(limits);
		return OPH_ESDM_ERROR;
	}

	for (l = 0; l < regular_times; l++) {

		//Build binary rows
		for (jj = 0; jj < regular_rows; jj++) {
			oph_esdm_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (j = 0; j < measure->ndims; j++) {
					if (start_pointer[i] == &(start[j]))
						*(start_pointer[i]) += measure->dims_start_index[j];
				}
			}

			//Fill array
			subspace = NULL;
			if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create data space\n");
				free(query_string);
				free(idDim);
				free(binary);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (binary_tmp)
					free(binary_tmp);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.args = measure->args;
				stream_data.buff = pointer = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (j = 0; j < array_length; j++, pointer += sizeof_type)
					memcpy(pointer, fill_value, sizeof_type);

				if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
					free(query_string);
					free(idDim);
					free(binary);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					oph_ioserver_free_query(server, query);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					if (binary_tmp)
						free(binary_tmp);
					if (counters)
						free(counters);
					if (products)
						free(products);
					if (limits)
						free(limits);
					return OPH_ESDM_ERROR;
				}

			} else if ((esdm_read(measure->dataset, binary + jj * sizeof_var, subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
				free(query_string);
				free(idDim);
				free(binary);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (binary_tmp)
					free(binary_tmp);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_esdm_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}
		//Increase idDim
		for (ii = 0; ii < regular_rows; ii++) {
			idDim[ii] += regular_rows;
		}
	}

	oph_ioserver_free_query(server, query);
	free(query_string);
	query_string = NULL;

	if (remainder_rows > 0) {
		query_size =
		    snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows;
		query_string = (char *) malloc(query_size * sizeof(char));
		if (!(query_string)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}

		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1;
		if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
			for (jj = 0; jj < remainder_rows; jj++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
			}
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
			for (jj = 0; jj < remainder_rows; jj++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
			}
		}
		query_string[n - 1] = ';';
		query_string[n] = 0;

		for (ii = remainder_rows * 2; ii < c_arg; ii++) {
			if (args[ii]) {
				free(args[ii]);
				args[ii] = NULL;
			}
		}

		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}
		//Build binary rows
		for (jj = 0; jj < remainder_rows; jj++) {
			oph_esdm_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (j = 0; j < measure->ndims; j++) {
					if (start_pointer[i] == &(start[j]))
						*(start_pointer[i]) += measure->dims_start_index[j];
				}
			}

			//Fill array
			subspace = NULL;
			if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create data space\n");
				free(query_string);
				free(idDim);
				free(binary);
				if (binary_tmp)
					free(binary_tmp);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.args = measure->args;
				stream_data.buff = pointer = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (j = 0; j < array_length; j++, pointer += sizeof_type)
					memcpy(pointer, fill_value, sizeof_type);

				if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
					free(query_string);
					free(idDim);
					free(binary);
					if (binary_tmp)
						free(binary_tmp);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					oph_ioserver_free_query(server, query);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					if (counters)
						free(counters);
					if (products)
						free(products);
					if (limits)
						free(limits);
					return OPH_ESDM_ERROR;
				}

			} else if ((esdm_read(measure->dataset, binary + jj * sizeof_var, subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
				free(query_string);
				free(idDim);
				free(binary);
				if (binary_tmp)
					free(binary_tmp);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_esdm_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	free(query_string);
	free(idDim);
	free(binary);
	if (binary_tmp)
		free(binary_tmp);
	for (ii = 0; ii < c_arg; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	free(start);
	free(count);
	free(start_pointer);
	free(sizemax);
	if (counters)
		free(counters);
	if (products)
		free(products);
	if (limits)
		free(limits);
	return OPH_ESDM_SUCCESS;
}

int oph_esdm_populate_fragment3(oph_ioserver_handler * server, oph_odb_fragment * frag, int tuplexfrag_number, int array_length, int compressed, ESDM_var * measure, long long memory_size)
{
	if (!frag || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_ESDM_ERROR;
	}

	char type_flag = '\0';
	long long sizeof_var = 0;
	esdm_type_t type_nc = measure->dspace->type;
	if (type_nc == SMD_DTYPE_INT8) {
		type_flag = OPH_ESDM_BYTE_FLAG;
		sizeof_var = (array_length) * sizeof(char);
	} else if (type_nc == SMD_DTYPE_INT16) {
		type_flag = OPH_ESDM_SHORT_FLAG;
		sizeof_var = (array_length) * sizeof(short);
	} else if (type_nc == SMD_DTYPE_INT32) {
		type_flag = OPH_ESDM_INT_FLAG;
		sizeof_var = (array_length) * sizeof(int);
	} else if (type_nc == SMD_DTYPE_INT64) {
		type_flag = OPH_ESDM_LONG_FLAG;
		sizeof_var = (array_length) * sizeof(long long);
	} else if (type_nc == SMD_DTYPE_FLOAT) {
		type_flag = OPH_ESDM_FLOAT_FLAG;
		sizeof_var = (array_length) * sizeof(float);
	} else {
		type_flag = OPH_ESDM_DOUBLE_FLAG;
		sizeof_var = (array_length) * sizeof(double);
	}

	long long memory_size_mb = memory_size * OPH_ESDM_MEMORY_BLOCK;

	//Flag set to 1 if whole fragment fits in memory
	//Conservative choice. At most we insert the whole fragment, hence we need 2 equal buffers: 1 to read and 1 to write 
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > (long long) (memory_size_mb / 2) ? 0 : 1);

	//pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory set: %lld\n",memory_size_mb);

	//Flag set to 1 if dimension are not in the order specified
	int i;
	short int dimension_ordered = 1;
	unsigned int *weight = (unsigned int *) malloc((measure->ndims) * sizeof(unsigned int));
	for (i = 0; i < measure->ndims; i++) {
		//Assign 0 to explicit dimensions and 1 to implicit
		weight[i] = (measure->dims_type[i] ? 0 : 1);
	}
	//Check if explicit are before implicit
	for (i = 1; i < measure->ndims; i++) {
		if (weight[i] < weight[i - 1]) {
			dimension_ordered = 0;
			break;
		}
	}
	free(weight);

	//Flag set to 1 if explicit dimension is splitted on more fragments, or more than one exp dimensions are in fragment
	int max_exp_lev = measure->nexp;
	short int whole_explicit = 0;
	for (i = 0; i < measure->ndims; i++) {
		//External explicit
		if (measure->dims_type[i] && measure->dims_oph_level[i] == max_exp_lev) {
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				whole_explicit = (1 == tuplexfrag_number ? 1 : 0);
			else
				whole_explicit = ((measure->dims_end_index[i] - measure->dims_start_index[i] + 1) == tuplexfrag_number ? 1 : 0);
			break;
		}
	}

	//If flag is set call old approach, else continue
	if (!whole_fragment || dimension_ordered || !whole_explicit)
		return oph_esdm_populate_fragment2(server, frag, tuplexfrag_number, array_length, compressed, measure);

	//Compute number of tuples per insert (regular case)
	unsigned long long regular_rows = 0, regular_times = 0, remainder_rows = 0, jj = 0, l;

	if (sizeof_var >= OPH_ESDM_BLOCK_SIZE) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
	} else if (tuplexfrag_number * sizeof_var <= OPH_ESDM_BLOCK_SIZE) {
		//Do single insert
		if (tuplexfrag_number <= OPH_ESDM_BLOCK_ROWS) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
		} else {
			regular_rows = OPH_ESDM_BLOCK_ROWS;
			regular_times = (long long) tuplexfrag_number / regular_rows;
			remainder_rows = (long long) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var) >= OPH_ESDM_BLOCK_ROWS ? OPH_ESDM_BLOCK_ROWS : (long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var));
		regular_times = (long long) tuplexfrag_number / regular_rows;
		remainder_rows = (long long) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	char *insert_query = (remainder_rows > 0 ? OPH_DC_SQ_MULTI_INSERT_FRAG : OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL);
	long long query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows;
	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ESDM_ERROR;
	}


	int n = snprintf(query_string, query_size, insert_query, frag->fragment_name) - 1;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		for (jj = 0; jj < regular_rows; jj++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
		}
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
		for (jj = 0; jj < regular_rows; jj++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
		}
	}
	query_string[n - 1] = ';';
	query_string[n] = 0;

	unsigned long long *idDim = (unsigned long long *) calloc(regular_rows, sizeof(unsigned long long));
	if (!(idDim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		return OPH_ESDM_ERROR;
	}
	//Create binary array
	char *binary_cache = 0;
	int res;

	//Create a binary array to store the whole fragment
	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(query_string);
		free(idDim);
		return OPH_ESDM_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(idDim);
		return OPH_ESDM_ERROR;
	}

	unsigned long long c_arg = regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary_cache);
		free(binary_insert);
		free(idDim);
		return OPH_ESDM_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_ESDM_ERROR;
		}
	}
	args[c_arg] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[2 * ii]->arg_length = sizeof(unsigned long long);
		args[2 * ii]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[2 * ii]->arg_is_null = 0;
		args[2 * ii]->arg = (unsigned long long *) (idDim + ii);
		args[2 * ii + 1]->arg_length = sizeof_var;
		args[2 * ii + 1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[2 * ii + 1]->arg_is_null = 0;
		args[2 * ii + 1]->arg = (char *) (binary_insert + sizeof_var * ii);
		idDim[ii] = frag->key_start + ii;
	}

	if (oph_ioserver_setup_query(server, query_string, regular_times, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_ESDM_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	int64_t *start = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	int64_t *count = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	//Sort start in base of oph_level of explicit dimension
	int64_t **start_pointer = (int64_t **) malloc((measure->nexp) * sizeof(int64_t *));

	for (i = 0; i < measure->ndims; i++) {
		//External explicit
		if (measure->dims_type[i] && measure->dims_oph_level[i] != max_exp_lev) {
			count[i] = 1;
		} else {
			//Implicit
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				count[i] = 1;
			else
				count[i] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			start[i] = measure->dims_start_index[i];
		}
	}
	//Check
	char check_for_reduce_func = !oph_esdm_is_a_reduce_func(measure->operation);
	int total = 1;
	for (i = 0; i < measure->ndims; i++)
		if (measure->dims_type[i] || check_for_reduce_func)
			total *= count[i];

	if (total != array_length * tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(idDim);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	short int flag = 0;
	short int curr_lev;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		flag = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				//Modified to allow subsetting
				if (measure->dims_start_index[i] == measure->dims_end_index[i])
					sizemax[curr_lev - 1] = 1;
				else
					sizemax[curr_lev - 1] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				start_pointer[curr_lev - 1] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			free(binary_cache);
			free(binary_insert);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}

	int j = 0;
	oph_esdm_compute_dimension_id(idDim[j], sizemax, measure->nexp, start_pointer);

	for (i = 0; i < measure->nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (j = 0; j < measure->ndims; j++) {
			if (start_pointer[i] == &(start[j])) {
				*(start_pointer[i]) += measure->dims_start_index[j];
			}
		}
	}

	//Fill binary cache
	res = -1;

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
	struct timeval start_read_time, end_read_time, total_read_time;
	struct timeval start_transpose_time, end_transpose_time, intermediate_transpose_time, total_transpose_time;
	struct timeval start_write_time, end_write_time, intermediate_write_time, total_write_time;
	total_transpose_time.tv_usec = 0;
	total_transpose_time.tv_sec = 0;
	total_write_time.tv_usec = 0;
	total_write_time.tv_sec = 0;

	gettimeofday(&start_read_time, NULL);
#endif

	size_t sizeof_type = (int) sizeof_var / array_length;
	esdm_dataspace_t *subspace = NULL;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type], *pointer = NULL;

	if (esdm_dataset_get_fill_value(measure->dataset, fill_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		free(query_string);
		free(idDim);
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}
	//Fill array
	if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
		free(query_string);
		free(idDim);
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	if (measure->operation) {

		// Initialize stream data
		stream_data.operation = measure->operation;
		stream_data.args = measure->args;
		stream_data.buff = pointer = binary_cache;
		stream_data.valid = 0;
		stream_data.fill_value = fill_value;
		for (j = 0; j < total; j++, pointer += sizeof_type)
			memcpy(pointer, fill_value, sizeof_type);

		if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}

	} else if ((esdm_read(measure->dataset, binary_cache, subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
		free(query_string);
		free(idDim);
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}
#if defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", frag->fragment_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	free(start);
	free(start_pointer);
	free(sizemax);

	//Prepare structures for buffer insert update
	int tmp_index = 0;
	unsigned int *counters = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned int));
	int *file_indexes = (int *) malloc((measure->nimp + 1) * sizeof(int));
	unsigned int *products = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned));
	unsigned int *limits = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned));
	int k = 0;

	//Setup arrays for recursive selection
	for (i = 0; i < measure->ndims; i++) {
		//Implicit and internal explicit
		if (!(measure->dims_type[i] && measure->dims_oph_level[i] != max_exp_lev)) {
			//If internal explicit use first position
			if (measure->dims_type[i]) {
				tmp_index = 0;
			} else {
				tmp_index = measure->dims_oph_level[i];
			}
			counters[tmp_index] = 0;
			products[tmp_index] = 1;
			limits[tmp_index] = check_for_reduce_func ? count[i] : 1;
			file_indexes[tmp_index] = k++;
		}
	}

	//Compute products
	for (k = 0; k < (measure->nimp + 1); k++) {
		//Last dimension in file has product 1
		for (i = (file_indexes[k] + 1); i < (measure->nimp + 1); i++) {
			flag = 0;
			//For each index following multiply
			for (j = 0; j < (measure->nimp + 1); j++) {
				if (file_indexes[j] == i) {
					products[k] *= limits[j];
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
				free(binary_cache);
				free(binary_insert);
				free(query_string);
				free(idDim);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(count);
				free(counters);
				free(file_indexes);
				free(products);
				free(limits);
				return OPH_ESDM_ERROR;
			}
		}
	}

	free(count);
	free(file_indexes);

	for (l = 0; l < regular_times; l++) {
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = (l + 1) * regular_rows;
		counters[0] = l * regular_rows;

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_esdm_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_write_time, NULL);
#endif
		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_write_time, NULL);
		timeval_subtract(&intermediate_write_time, &end_write_time, &start_write_time);
		timeval_add(&total_write_time, &total_write_time, &intermediate_write_time);
#endif

		//Increase idDim
		for (ii = 0; ii < regular_rows; ii++) {
			idDim[ii] += regular_rows;
		}
	}
	oph_ioserver_free_query(server, query);
	free(query_string);
	query_string = NULL;

	if (remainder_rows > 0) {
		query_size =
		    snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) + strlen(compressed ? OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW : OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows;
		query_string = (char *) malloc(query_size * sizeof(char));
		if (!(query_string)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}

		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1;
		if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
			for (jj = 0; jj < remainder_rows; jj++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
			}
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
			for (jj = 0; jj < remainder_rows; jj++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_ROW);
			}
		}
		query_string[n - 1] = ';';
		query_string[n] = 0;

		for (ii = remainder_rows * 2; ii < c_arg; ii++) {
			if (args[ii]) {
				free(args[ii]);
				args[ii] = NULL;
			}
		}

		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = regular_times * regular_rows + remainder_rows;
		counters[0] = regular_times * regular_rows;
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_esdm_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_write_time, NULL);
#endif
		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_write_time, NULL);
		timeval_subtract(&intermediate_write_time, &end_write_time, &start_write_time);
		timeval_add(&total_write_time, &total_write_time, &intermediate_write_time);
#endif

		oph_ioserver_free_query(server, query);
	}
#if defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
	printf("Fragment %s:  Total transpose :\t Time %d,%06d sec\n", frag->fragment_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
	printf("Fragment %s:  Total write :\t Time %d,%06d sec\n", frag->fragment_name, (int) total_write_time.tv_sec, (int) total_write_time.tv_usec);
#endif

	free(query_string);
	free(idDim);
	free(binary_cache);
	free(binary_insert);
	for (ii = 0; ii < c_arg; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	free(counters);
	free(products);
	free(limits);

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_populate_fragment5(oph_ioserver_handler * server, oph_odb_fragment * frag, char *nc_file_path, int tuplexfrag_number, int compressed, ESDM_var * measure)
{
	if (!frag || !nc_file_path || !tuplexfrag_number || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_ESDM_ERROR;
	}
	//Flag set to 1 if dimension are in the order specified in the file
	int i;
	int *index = (int *) malloc((measure->ndims) * sizeof(int));
	for (i = 0; i < measure->ndims; i++) {
		//Compute index of actual order in Ophidia
		if (!measure->dims_type[i])
			index[i] = (measure->ndims - measure->nimp) + measure->dims_oph_level[i] - 1;
		else
			index[i] = measure->dims_oph_level[i] - 1;
	}

	//Find most external dimension with size bigger than 1
	int j;
	int most_extern_id = 0;
	for (i = 0; i < measure->nexp; i++) {
		//Find dimension related to index
		for (j = 0; j < measure->ndims; j++) {
			if (i == index[j]) {
				break;
			}
		}

		//External explicit
		if (measure->dims_type[j]) {
			if ((measure->dims_end_index[j] - measure->dims_start_index[j]) > 0) {
				most_extern_id = i;
				break;
			}
		}
	}

	//Check if only most external dimension (bigger than 1) is splitted
	long long curr_rows = 1;
	long long relative_rows = 0;
	short int whole_explicit = 1;
	for (i = measure->ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < measure->ndims; j++) {
			if (i == index[j]) {
				break;
			}
		}

		//External explicit
		if (measure->dims_type[j]) {
			relative_rows = (int) (tuplexfrag_number / curr_rows);
			curr_rows *= (measure->dims_end_index[j] - measure->dims_start_index[j] + 1);
			if (relative_rows < (measure->dims_end_index[j] - measure->dims_start_index[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}
	//If external explicit is not integer
	if ((tuplexfrag_number % curr_rows) != 0)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented\n");
		free(index);
		return OPH_ESDM_ERROR;
	}
	//Alloc query String
	int n1 = 0, n2 = 0, n3 = 0, n4 = 0;
	for (j = 0; j < measure->ndims; j++) {
		n1 += snprintf(NULL, 0, "%hd|", measure->dims_type[j]);
		n2 += snprintf(NULL, 0, "%d|", (int) index[j]);
		n3 += snprintf(NULL, 0, "%d|", measure->dims_start_index[j]);
		n4 += snprintf(NULL, 0, "%d|", measure->dims_end_index[j]);
	}

	char *insert_query = OPH_DC_SQ_CREATE_FRAG_FROM_ESDM;
	int query_size =
	    snprintf(NULL, 0, insert_query, frag->fragment_name, nc_file_path, measure->varname, compressed ? OPH_IOSERVER_SQ_VAL_YES : OPH_IOSERVER_SQ_VAL_NO, tuplexfrag_number, frag->key_start, "",
		     "", "", "", measure->dim_unlim, measure->operation ? measure->operation : OPH_COMMON_NONE_FILTER,
		     measure->args ? measure->args : OPH_COMMON_NONE_FILTER) + (n1 + n2 + n3 + n4 - 4) + 1;

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(index);
		return OPH_ESDM_ERROR;
	}

	char *dims_type_string = (char *) malloc(n1 * sizeof(char));
	if (!(dims_type_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(index);
		return OPH_ESDM_ERROR;
	}
	char *dims_index_string = (char *) malloc(n2 * sizeof(char));
	if (!(dims_index_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(dims_type_string);
		free(index);
		return OPH_ESDM_ERROR;
	}
	char *dims_start_string = (char *) malloc(n3 * sizeof(char));
	if (!(dims_start_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(dims_type_string);
		free(dims_index_string);
		free(index);
		return OPH_ESDM_ERROR;
	}
	char *dims_end_string = (char *) malloc(n4 * sizeof(char));
	if (!(dims_end_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(dims_type_string);
		free(dims_index_string);
		free(dims_start_string);
		free(index);
		return OPH_ESDM_ERROR;
	}
	//Set values into strings
	int m1 = 0, m2 = 0, m3 = 0, m4 = 0;
	for (j = 0; j < measure->ndims; j++) {
		m1 += snprintf(dims_type_string + m1, n1 - m1, "%hd|", measure->dims_type[j]);
		m2 += snprintf(dims_index_string + m2, n2 - m2, "%d|", (int) index[j]);
		m3 += snprintf(dims_start_string + m3, n3 - m3, "%d|", measure->dims_start_index[j]);
		m4 += snprintf(dims_end_string + m4, n4 - m4, "%d|", measure->dims_end_index[j]);
	}
	dims_type_string[m1 - 1] = 0;
	dims_index_string[m2 - 1] = 0;
	dims_start_string[m3 - 1] = 0;
	dims_end_string[m4 - 1] = 0;
	free(index);

	int n = snprintf(query_string, query_size, insert_query, frag->fragment_name, nc_file_path, measure->varname, compressed ? OPH_IOSERVER_SQ_VAL_YES : OPH_IOSERVER_SQ_VAL_NO, tuplexfrag_number,
			 frag->key_start, dims_type_string, dims_index_string, dims_start_string, dims_end_string, measure->dim_unlim, measure->operation ? measure->operation : OPH_COMMON_NONE_FILTER,
			 measure->args ? measure->args : OPH_COMMON_NONE_FILTER);
	if (n >= query_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_ESDM_ERROR;
	}

	free(dims_type_string);
	free(dims_index_string);
	free(dims_start_string);
	free(dims_end_string);

	oph_ioserver_query *query = NULL;
	if (oph_ioserver_setup_query(server, query_string, 1, NULL, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup query.\n");
		free(query_string);
		return OPH_DC_SERVER_ERROR;
	}

	if (oph_ioserver_execute_query(server, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to execute operation.\n");
		free(query_string);
		oph_ioserver_free_query(server, query);
		return OPH_DC_SERVER_ERROR;
	}

	oph_ioserver_free_query(server, query);
	free(query_string);

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_append_fragment_from_esdm(oph_ioserver_handler * server, oph_odb_fragment * old_frag, oph_odb_fragment * new_frag, int compressed, ESDM_var * measure)
{
	if (!old_frag || !new_frag || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_ESDM_ERROR;
	}
	int tuplexfrag_number = old_frag->key_end - old_frag->key_start + 1;

	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	int64_t *start = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	int64_t *count = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	//Sort start in base of oph_level of explicit dimension
	int64_t **start_pointer = (int64_t **) malloc((measure->nexp) * sizeof(int64_t *));
	//Set count
	short int i;
	for (i = 0; i < measure->ndims; i++) {
		//Explicit
		if (measure->dims_type[i]) {
			count[i] = 1;
		} else {
			//Implicit
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				count[i] = 1;
			else
				count[i] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			start[i] = measure->dims_start_index[i];
		}
	}
	char check_for_reduce_func = !oph_esdm_is_a_reduce_func(measure->operation);
	int array_length = 1;
	if (check_for_reduce_func) {
		for (i = 0; i < measure->ndims; i++)
			if (!measure->dims_type[i])
				array_length *= count[i];
	}

	char type_flag = '\0';
	long long sizeof_var = 0;
	esdm_type_t type_nc = measure->dspace->type;
	if (type_nc == SMD_DTYPE_INT8) {
		type_flag = OPH_ESDM_BYTE_FLAG;
		sizeof_var = (array_length) * sizeof(char);
	} else if (type_nc == SMD_DTYPE_INT16) {
		type_flag = OPH_ESDM_SHORT_FLAG;
		sizeof_var = (array_length) * sizeof(short);
	} else if (type_nc == SMD_DTYPE_INT32) {
		type_flag = OPH_ESDM_INT_FLAG;
		sizeof_var = (array_length) * sizeof(int);
	} else if (type_nc == SMD_DTYPE_INT64) {
		type_flag = OPH_ESDM_LONG_FLAG;
		sizeof_var = (array_length) * sizeof(long long);
	} else if (type_nc == SMD_DTYPE_FLOAT) {
		type_flag = OPH_ESDM_FLOAT_FLAG;
		sizeof_var = (array_length) * sizeof(float);
	} else {
		type_flag = OPH_ESDM_DOUBLE_FLAG;
		sizeof_var = (array_length) * sizeof(double);
	}

	//Compute number of tuples per insert (regular case)
	unsigned long long regular_rows = 0, regular_times = 0, remainder_rows = 0, jj, l;

	if (sizeof_var >= OPH_ESDM_BLOCK_SIZE) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
	} else if (tuplexfrag_number * sizeof_var <= OPH_ESDM_BLOCK_SIZE) {
		//Do single insert
		if (tuplexfrag_number <= OPH_ESDM_BLOCK_ROWS) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
		} else {
			regular_rows = OPH_ESDM_BLOCK_ROWS;
			regular_times = (long long) tuplexfrag_number / regular_rows;
			remainder_rows = (long long) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var) >= OPH_ESDM_BLOCK_ROWS ? OPH_ESDM_BLOCK_ROWS : (long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var));
		regular_times = (long long) tuplexfrag_number / regular_rows;
		remainder_rows = (long long) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	int tmp2 = strlen(OPH_ESDM_CONCAT_TYPE);
	char *measure_type = measure->vartype;
	int tmp3 = strlen(measure_type);
	long long input_measure_size = (tmp2 + strlen(measure_type) + 1) * (regular_rows + 1), list_size = 0, plugin_size = 0, where_size = 0, query_size = 0;
	list_size = strlen(compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW) * regular_rows;
	plugin_size = 1 + snprintf(NULL, 0, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, "", measure_type, "") + list_size + input_measure_size;
	where_size = 1 + snprintf(NULL, 0, OPH_ESDM_CONCAT_WHERE, (long long) old_frag->key_end, (long long) old_frag->key_end);
	query_size = 1 + snprintf(NULL, 0, OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, "", old_frag->fragment_name, "") + plugin_size + where_size;

	char input_measure_type[input_measure_size], list_string[list_size + 1], plugin_string[plugin_size + 1], where_string[where_size + 1];
	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	int j = 0, n = 0, nn = 0, tmp = (int) strlen(compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW), res;

	*list_string = 0;
	*input_measure_type = 0;
	strcpy(input_measure_type + nn, OPH_ESDM_CONCAT_TYPE);	// Input data cube
	nn += tmp2;
	strcpy(input_measure_type + nn, measure_type);
	nn += tmp3;
	for (jj = 0; jj < regular_rows; jj++) {
		strcpy(list_string + n, compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW);
		n += tmp;
		strcpy(input_measure_type + nn, "|" OPH_ESDM_CONCAT_TYPE);	// Data from file
		nn += 1 + tmp2;
		strcpy(input_measure_type + nn, measure_type);
		nn += tmp3;
	}

	unsigned long long *idDim = (unsigned long long *) calloc(regular_rows, sizeof(unsigned long long));
	if (!(idDim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}
	//Create binary array
	char *binary = 0;

	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	unsigned long long c_arg = regular_rows, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}
	args[c_arg] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[ii]->arg_length = sizeof_var;
		args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[ii]->arg_is_null = 0;
		args[ii]->arg = (char *) (binary + sizeof_var * ii);
		idDim[ii] = old_frag->key_start + ii;
	}

	short int flag = 0;
	short int curr_lev;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		flag = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				//Modified to allow subsetting
				if (measure->dims_start_index[i] == measure->dims_end_index[i])
					sizemax[curr_lev - 1] = 1;
				else
					sizemax[curr_lev - 1] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				start_pointer[curr_lev - 1] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			free(binary);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}

	//Flag set to 0 if implicit dimensions are not in the order specified in the file
	short int imp_dim_ordered = 1;
	//Check if implicit are ordered
	curr_lev = 0;
	for (i = 0; i < measure->ndims; i++) {
		if (measure->dims_type[i] == 0) {
			if (measure->dims_oph_level[i] < curr_lev) {
				imp_dim_ordered = 0;
				break;
			}
			curr_lev = measure->dims_oph_level[i];
		}
	}

	//Create tmp binary array
	char *binary_tmp = NULL;

	int tmp_index = 0;
	unsigned int *counters = NULL;
	unsigned int *products = NULL;
	unsigned int *limits = NULL;

	//Create a binary array to store the tmp row
	if (!imp_dim_ordered) {
		if (type_flag == OPH_ESDM_BYTE_FLAG)
			res = oph_iob_bin_array_create_b(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_SHORT_FLAG)
			res = oph_iob_bin_array_create_s(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_INT_FLAG)
			res = oph_iob_bin_array_create_i(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_LONG_FLAG)
			res = oph_iob_bin_array_create_l(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_FLOAT_FLAG)
			res = oph_iob_bin_array_create_f(&binary_tmp, array_length);
		else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		else
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
			free(binary);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
		//Prepare structures for buffer insert update
		tmp_index = 0;
		counters = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned int));
		int *file_indexes = (int *) malloc((measure->nimp) * sizeof(int));
		products = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		limits = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < measure->ndims; i++) {
			//Implicit
			if (!measure->dims_type[i]) {
				tmp_index = measure->dims_oph_level[i] - 1;	//Start from 0
				counters[tmp_index] = 0;
				products[tmp_index] = 1;
				limits[tmp_index] = check_for_reduce_func ? count[i] : 1;
				file_indexes[tmp_index] = k++;
			}
		}
		//Compute products
		for (k = 0; k < measure->nimp; k++) {
			//Last dimension in file has product 1
			for (i = (file_indexes[k] + 1); i < measure->nimp; i++) {
				flag = 0;
				//For each index following multiply
				for (j = 0; j < measure->nimp; j++) {
					if (file_indexes[j] == i) {
						products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					free(query_string);
					free(idDim);
					free(binary);
					if (binary_tmp)
						free(binary_tmp);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					free(counters);
					free(file_indexes);
					free(products);
					free(limits);
					return OPH_ESDM_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;
	esdm_dataspace_t *subspace = NULL;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type], *pointer = NULL;

	if (esdm_dataset_get_fill_value(measure->dataset, fill_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		free(query_string);
		free(idDim);
		free(binary);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		if (binary_tmp)
			free(binary_tmp);
		if (counters)
			free(counters);
		if (products)
			free(products);
		if (limits)
			free(limits);
		return OPH_ESDM_ERROR;
	}

	for (l = 0; l < regular_times; l++) {

		snprintf(plugin_string, plugin_size, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, input_measure_type, measure_type, list_string);
		snprintf(where_string, where_size, OPH_ESDM_CONCAT_WHERE, old_frag->key_start + l * regular_rows, old_frag->key_start + (l + 1) * regular_rows - 1);
		snprintf(query_string, query_size, (remainder_rows > 0)
			 || (l < regular_times - 1) ? OPH_DC_SQ_INSERT_SELECT_FRAG : OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, plugin_string, old_frag->fragment_name, where_string);

		query = NULL;
		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}
		//Build binary rows
		for (jj = 0; jj < regular_rows; jj++) {
			oph_esdm_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);
			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (j = 0; j < measure->ndims; j++) {
					if (start_pointer[i] == &(start[j]))
						*(start_pointer[i]) += measure->dims_start_index[j];
				}
			}

			//Fill array
			subspace = NULL;
			if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create data space\n");
				free(query_string);
				free(idDim);
				free(binary);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (binary_tmp)
					free(binary_tmp);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.args = measure->args;
				stream_data.buff = pointer = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (j = 0; j < array_length; j++, pointer += sizeof_type)
					memcpy(pointer, fill_value, sizeof_type);

				if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
					free(query_string);
					free(idDim);
					free(binary);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					oph_ioserver_free_query(server, query);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					if (binary_tmp)
						free(binary_tmp);
					if (counters)
						free(counters);
					if (products)
						free(products);
					if (limits)
						free(limits);
					return OPH_ESDM_ERROR;
				}

			} else if ((esdm_read(measure->dataset, binary + jj * sizeof_var, subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
				free(query_string);
				free(idDim);
				free(binary);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (binary_tmp)
					free(binary_tmp);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_esdm_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_ioserver_free_query(server, query);

		//Increase idDim
		for (ii = 0; ii < regular_rows; ii++) {
			idDim[ii] += regular_rows;
		}
	}

	if (remainder_rows > 0) {
		*list_string = n = 0;
		nn = tmp2 + tmp3;
		for (jj = 0; jj < remainder_rows; jj++) {
			strcpy(list_string + n, compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW);
			n += tmp;
			nn += 1 + tmp2 + tmp3;
		}
		input_measure_type[nn] = 0;

		snprintf(plugin_string, plugin_size, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, input_measure_type, measure_type, list_string);
		snprintf(where_string, where_size, OPH_ESDM_CONCAT_WHERE, old_frag->key_start + l * regular_rows, old_frag->key_start + (l + 1) * regular_rows - 1);
		snprintf(query_string, query_size, OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, plugin_string, old_frag->fragment_name, where_string);

		for (ii = remainder_rows; ii < c_arg; ii++) {
			if (args[ii]) {
				free(args[ii]);
				args[ii] = NULL;
			}
		}

		query = NULL;
		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}
		//Build binary rows
		for (jj = 0; jj < remainder_rows; jj++) {
			oph_esdm_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);
			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (j = 0; j < measure->ndims; j++) {
					if (start_pointer[i] == &(start[j]))
						*(start_pointer[i]) += measure->dims_start_index[j];
				}
			}

			//Fill array
			subspace = NULL;
			if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create data space\n");
				free(query_string);
				free(idDim);
				free(binary);
				if (binary_tmp)
					free(binary_tmp);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.args = measure->args;
				stream_data.buff = pointer = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (j = 0; j < array_length; j++, pointer += sizeof_type)
					memcpy(pointer, fill_value, sizeof_type);

				if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
					free(query_string);
					free(idDim);
					free(binary);
					if (binary_tmp)
						free(binary_tmp);
					for (ii = 0; ii < c_arg; ii++)
						if (args[ii])
							free(args[ii]);
					free(args);
					oph_ioserver_free_query(server, query);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					if (counters)
						free(counters);
					if (products)
						free(products);
					if (limits)
						free(limits);
					return OPH_ESDM_ERROR;
				}

			} else if ((esdm_read(measure->dataset, binary + jj * sizeof_var, subspace))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data\n");
				free(query_string);
				free(idDim);
				free(binary);
				if (binary_tmp)
					free(binary_tmp);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_ESDM_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_esdm_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	free(query_string);
	free(idDim);
	free(binary);
	if (binary_tmp)
		free(binary_tmp);
	for (ii = 0; ii < c_arg; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	free(start);
	free(count);
	free(start_pointer);
	free(sizemax);
	if (counters)
		free(counters);
	if (products)
		free(products);
	if (limits)
		free(limits);

	return OPH_ESDM_SUCCESS;
}

int oph_esdm_append_fragment_from_esdm2(oph_ioserver_handler * server, oph_odb_fragment * old_frag, oph_odb_fragment * new_frag, int compressed, ESDM_var * measure, long long memory_size)
{
	if (!old_frag || !new_frag || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_ESDM_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_ESDM_ERROR;
	}
	int tuplexfrag_number = old_frag->key_end - old_frag->key_start + 1;

	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	int64_t *start = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	int64_t *count = (int64_t *) malloc((measure->ndims) * sizeof(int64_t));
	//Sort start in base of oph_level of explicit dimension
	int64_t **start_pointer = (int64_t **) malloc((measure->nexp) * sizeof(int64_t *));
	//Set count
	int max_exp_lev = measure->nexp;
	short int i;
	for (i = 0; i < measure->ndims; i++) {
		//External explicit
		if (measure->dims_type[i] && measure->dims_oph_level[i] != max_exp_lev) {
			count[i] = 1;
		} else {
			//Implicit
			count[i] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			start[i] = measure->dims_start_index[i];
		}
	}
	int array_length = 1;
	for (i = 0; i < measure->ndims; i++)
		if (!measure->dims_type[i])
			array_length *= count[i];

	char type_flag = '\0';
	long long sizeof_var = 0;
	esdm_type_t type_nc = measure->dspace->type;
	if (type_nc == SMD_DTYPE_INT8) {
		type_flag = OPH_ESDM_BYTE_FLAG;
		sizeof_var = (array_length) * sizeof(char);
	} else if (type_nc == SMD_DTYPE_INT16) {
		type_flag = OPH_ESDM_SHORT_FLAG;
		sizeof_var = (array_length) * sizeof(short);
	} else if (type_nc == SMD_DTYPE_INT32) {
		type_flag = OPH_ESDM_INT_FLAG;
		sizeof_var = (array_length) * sizeof(int);
	} else if (type_nc == SMD_DTYPE_INT64) {
		type_flag = OPH_ESDM_LONG_FLAG;
		sizeof_var = (array_length) * sizeof(long long);
	} else if (type_nc == SMD_DTYPE_FLOAT) {
		type_flag = OPH_ESDM_FLOAT_FLAG;
		sizeof_var = (array_length) * sizeof(float);
	} else {
		type_flag = OPH_ESDM_DOUBLE_FLAG;
		sizeof_var = (array_length) * sizeof(double);
	}

	long long memory_size_mb = memory_size * OPH_ESDM_MEMORY_BLOCK;

	//Flag set to 1 if whole fragment fits in memory
	//Conservative choice. At most we insert the whole fragment, hence we need 2 equal buffers: 1 to read and 1 to write 
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > (long long) (memory_size_mb / 2) ? 0 : 1);

	//pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory set: %lld\n",memory_size_mb);

	//Flag set to 1 if dimension are not in the order specified
	short int dimension_ordered = 1;
	unsigned int *weight = (unsigned int *) malloc((measure->ndims) * sizeof(unsigned int));
	for (i = 0; i < measure->ndims; i++) {
		//Assign 0 to explicit dimensions and 1 to implicit
		weight[i] = (measure->dims_type[i] ? 0 : 1);
	}
	//Check if explicit are before implicit
	for (i = 1; i < measure->ndims; i++) {
		if (weight[i] < weight[i - 1]) {
			dimension_ordered = 0;
			break;
		}
	}
	free(weight);

	//Flag set to 1 if explicit dimension is splitted on more fragments, or more than one exp dimensions are in fragment
	short int whole_explicit = 0;
	for (i = 0; i < measure->ndims; i++) {
		//External explicit
		if (measure->dims_type[i] && measure->dims_oph_level[i] == max_exp_lev) {
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				whole_explicit = (1 == tuplexfrag_number ? 1 : 0);
			else
				whole_explicit = ((measure->dims_end_index[i] - measure->dims_start_index[i] + 1) == tuplexfrag_number ? 1 : 0);
			break;
		}
	}

	//If flag is set call old approach, else continue
	if (!whole_fragment || dimension_ordered || !whole_explicit) {
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return oph_esdm_append_fragment_from_esdm(server, old_frag, new_frag, compressed, measure);
	}
	//Compute number of tuples per insert (regular case)
	unsigned long long regular_rows = 0, regular_times = 0, remainder_rows = 0, jj, l;

	if (sizeof_var >= OPH_ESDM_BLOCK_SIZE) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
	} else if (tuplexfrag_number * sizeof_var <= OPH_ESDM_BLOCK_SIZE) {
		//Do single insert
		if (tuplexfrag_number <= OPH_ESDM_BLOCK_ROWS) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
		} else {
			regular_rows = OPH_ESDM_BLOCK_ROWS;
			regular_times = (long long) tuplexfrag_number / regular_rows;
			remainder_rows = (long long) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var) >= OPH_ESDM_BLOCK_ROWS ? OPH_ESDM_BLOCK_ROWS : (long long) (OPH_ESDM_BLOCK_SIZE / sizeof_var));
		regular_times = (long long) tuplexfrag_number / regular_rows;
		remainder_rows = (long long) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	int tmp2 = strlen(OPH_ESDM_CONCAT_TYPE);
	char *measure_type = measure->vartype;
	int tmp3 = strlen(measure_type);
	long long input_measure_size = (tmp2 + strlen(measure_type) + 1) * (regular_rows + 1), list_size = 0, plugin_size = 0, where_size = 0, query_size = 0;
	list_size = strlen(compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW) * regular_rows;
	plugin_size = 1 + snprintf(NULL, 0, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, "", measure_type, "") + list_size + input_measure_size;
	where_size = 1 + snprintf(NULL, 0, OPH_ESDM_CONCAT_WHERE, (long long) old_frag->key_end, (long long) old_frag->key_end);
	query_size = 1 + snprintf(NULL, 0, OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, "", old_frag->fragment_name, "") + plugin_size + where_size;

	char input_measure_type[input_measure_size], list_string[list_size + 1], plugin_string[plugin_size + 1], where_string[where_size + 1];
	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	int j = 0, n = 0, nn = 0, tmp = (int) strlen(compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW), res;

	*list_string = 0;
	*input_measure_type = 0;
	strcpy(input_measure_type + nn, OPH_ESDM_CONCAT_TYPE);	// Input data cube
	nn += tmp2;
	strcpy(input_measure_type + nn, measure_type);
	nn += tmp3;
	for (jj = 0; jj < regular_rows; jj++) {
		strcpy(list_string + n, compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW);
		n += tmp;
		strcpy(input_measure_type + nn, "|" OPH_ESDM_CONCAT_TYPE);	// Data from file
		nn += 1 + tmp2;
		strcpy(input_measure_type + nn, measure_type);
		nn += tmp3;
	}

	//Create binary array
	char *binary_cache = 0;

	//Create a binary array to store the whole fragment
	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(query_string);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (type_flag == OPH_ESDM_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_ESDM_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	unsigned long long c_arg = regular_rows, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary_cache);
		free(binary_insert);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	for (ii = 0; ii < c_arg; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}
	args[c_arg] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[ii]->arg_length = sizeof_var;
		args[ii]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[ii]->arg_is_null = 0;
		args[ii]->arg = (char *) (binary_insert + sizeof_var * ii);
	}

	//Check
	char check_for_reduce_func = !oph_esdm_is_a_reduce_func(measure->operation);
	int total = 1;
	for (i = 0; i < measure->ndims; i++)
		if (measure->dims_type[i] || check_for_reduce_func)
			total *= count[i];

	if (total != array_length * tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	short int flag = 0;
	short int curr_lev;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		flag = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				sizemax[curr_lev - 1] = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
				start_pointer[curr_lev - 1] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			free(binary_cache);
			free(binary_insert);
			free(query_string);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}
	}

	oph_esdm_compute_dimension_id(old_frag->key_start, sizemax, measure->nexp, start_pointer);

	for (i = 0; i < measure->nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (j = 0; j < measure->ndims; j++) {
			if (start_pointer[i] == &(start[j])) {
				*(start_pointer[i]) += measure->dims_start_index[j];
			}
		}
	}

	//Fill binary cache
	res = -1;

	size_t sizeof_type = (int) sizeof_var / array_length;
	esdm_dataspace_t *subspace = NULL;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type], *pointer = NULL;

	if (esdm_dataset_get_fill_value(measure->dataset, fill_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}
	//Fill array
	if ((esdm_dataspace_create_full(measure->ndims, count, start, type_nc, &subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	if (measure->operation) {

		// Initialize stream data
		stream_data.operation = measure->operation;
		stream_data.args = measure->args;
		stream_data.buff = pointer = binary_cache;
		stream_data.valid = 0;
		stream_data.fill_value = fill_value;
		for (j = 0; j < total; j++, pointer += sizeof_type)
			memcpy(pointer, fill_value, sizeof_type);

		if ((esdm_read_stream(measure->dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
			free(binary_cache);
			free(binary_insert);
			free(query_string);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_ESDM_ERROR;
		}

	} else if ((esdm_read(measure->dataset, binary_cache, subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to write data\n");
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		for (ii = 0; ii < c_arg; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_ESDM_ERROR;
	}

	free(start);
	free(start_pointer);
	free(sizemax);

	//Prepare structures for buffer insert update
	int tmp_index = 0;
	unsigned int *counters = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned int));
	int *file_indexes = (int *) malloc((measure->nimp + 1) * sizeof(int));
	unsigned int *products = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned));
	unsigned int *limits = (unsigned int *) malloc((measure->nimp + 1) * sizeof(unsigned));
	int k = 0;

	//Setup arrays for recursive selection
	for (i = 0; i < measure->ndims; i++) {
		//Implicit and internal explicit
		if (!(measure->dims_type[i] && measure->dims_oph_level[i] != max_exp_lev)) {
			//If internal explicit use first position
			if (measure->dims_type[i]) {
				tmp_index = 0;
			} else {
				tmp_index = measure->dims_oph_level[i];
			}
			counters[tmp_index] = 0;
			products[tmp_index] = 1;
			limits[tmp_index] = check_for_reduce_func ? count[i] : 1;
			file_indexes[tmp_index] = k++;
		}
	}
	//Compute products
	for (k = 0; k < (measure->nimp + 1); k++) {
		//Last dimension in file has product 1
		for (i = (file_indexes[k] + 1); i < (measure->nimp + 1); i++) {
			flag = 0;
			//For each index following multiply
			for (j = 0; j < (measure->nimp + 1); j++) {
				if (file_indexes[j] == i) {
					products[k] *= limits[j];
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
				free(binary_cache);
				free(binary_insert);
				free(query_string);
				for (ii = 0; ii < c_arg; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				free(count);
				free(counters);
				free(file_indexes);
				free(products);
				free(limits);
				return OPH_ESDM_ERROR;
			}
		}
	}

	free(count);
	free(file_indexes);

	snprintf(plugin_string, plugin_size, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, input_measure_type, measure_type, list_string);

	for (l = 0; l < regular_times; l++) {
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = (l + 1) * regular_rows;
		counters[0] = l * regular_rows;

		snprintf(where_string, where_size, OPH_ESDM_CONCAT_WHERE, old_frag->key_start + l * regular_rows, old_frag->key_start + (l + 1) * regular_rows - 1);
		snprintf(query_string, query_size, (remainder_rows > 0)
			 || (l < regular_times - 1) ? OPH_DC_SQ_INSERT_SELECT_FRAG : OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, plugin_string, old_frag->fragment_name, where_string);

		query = NULL;
		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_esdm_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	if (remainder_rows > 0) {
		*list_string = n = 0;
		nn = tmp2 + tmp3;
		for (jj = 0; jj < remainder_rows; jj++) {
			strcpy(list_string + n, compressed ? OPH_ESDM_CONCAT_COMPRESSED_ROW : OPH_ESDM_CONCAT_ROW);
			n += tmp;
			nn += 1 + tmp2 + tmp3;
		}
		input_measure_type[nn] = 0;

		snprintf(plugin_string, plugin_size, compressed ? OPH_ESDM_CONCAT_PLUGIN_COMPR : OPH_ESDM_CONCAT_PLUGIN, input_measure_type, measure_type, list_string);
		snprintf(where_string, where_size, OPH_ESDM_CONCAT_WHERE, old_frag->key_start + l * regular_rows, old_frag->key_start + (l + 1) * regular_rows - 1);
		snprintf(query_string, query_size, OPH_DC_SQ_INSERT_SELECT_FRAG_FINAL, new_frag->fragment_name, plugin_string, old_frag->fragment_name, where_string);

		for (ii = remainder_rows; ii < c_arg; ii++) {
			if (args[ii]) {
				free(args[ii]);
				args[ii] = NULL;
			}
		}

		query = NULL;
		if (oph_ioserver_setup_query(server, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = regular_times * regular_rows + remainder_rows;
		counters[0] = regular_times * regular_rows;
		oph_esdm_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);

		if (oph_ioserver_execute_query(server, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_ESDM_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	free(query_string);
	free(binary_cache);
	free(binary_insert);
	for (ii = 0; ii < c_arg; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	free(counters);
	free(products);
	free(limits);

	return OPH_ESDM_SUCCESS;
}
