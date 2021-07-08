/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include <math.h>
#include <ctype.h>

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_IMPORTESDM_operator.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_hierarchy_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"
#include "oph_driver_procedure_library.h"
#include "oph_datacube_library.h"
#include "oph-lib-binary-io.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#include "oph_esdm_kernels.h"

#define OPH_ESDM_DOUBLE_FLAG	OPH_COMMON_DOUBLE_FLAG
#define OPH_ESDM_FLOAT_FLAG		OPH_COMMON_FLOAT_FLAG
#define OPH_ESDM_INT_FLAG		OPH_COMMON_INT_FLAG
#define OPH_ESDM_LONG_FLAG		OPH_COMMON_LONG_FLAG
#define OPH_ESDM_SHORT_FLAG		OPH_COMMON_SHORT_FLAG
#define OPH_ESDM_BYTE_FLAG		OPH_COMMON_BYTE_FLAG

#define OPH_ESDM_MEMORY_BLOCK 1048576
#define OPH_ESDM_BLOCK_SIZE 524288	// Maximum size that could be transfered
#define OPH_ESDM_BLOCK_ROWS 1000	// Maximum number of lines that could be transfered

#define _NC_DIMS "_nc_dims"
#define _NC_SIZES "_nc_sizes"

int _oph_esdm_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, int64_t ** id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (int64_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_esdm_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else {
		*(id[i]) = (size_t) (residual + 1);
	}
	return 0;
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
	return 0;
}

int oph_esdm_get_esdm_type(char *in_c_type, esdm_type_t * type_nc)
{
	if (!strcasecmp(in_c_type, OPH_COMMON_BYTE_TYPE)) {
		*type_nc = SMD_DTYPE_INT8;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_SHORT_TYPE)) {
		*type_nc = SMD_DTYPE_INT16;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_INT_TYPE)) {
		*type_nc = SMD_DTYPE_INT32;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_LONG_TYPE)) {
		*type_nc = SMD_DTYPE_INT64;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_FLOAT_TYPE)) {
		*type_nc = SMD_DTYPE_FLOAT;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_DOUBLE_TYPE)) {
		*type_nc = SMD_DTYPE_DOUBLE;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_COMMON_BIT_TYPE)) {
		*type_nc = SMD_DTYPE_INT8;
		return 0;
	}
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type '%s' not supported\n", in_c_type);
	return -1;
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
		return -1;
	}

	return 0;
}

size_t _oph_sizeof(char *in_c_type)
{
	if (!strcasecmp(in_c_type, OPH_COMMON_BYTE_TYPE))
		return sizeof(char);
	if (!strcasecmp(in_c_type, OPH_COMMON_SHORT_TYPE))
		return sizeof(short);
	if (!strcasecmp(in_c_type, OPH_COMMON_INT_TYPE))
		return sizeof(int);
	if (!strcasecmp(in_c_type, OPH_COMMON_LONG_TYPE))
		return sizeof(long long);
	if (!strcasecmp(in_c_type, OPH_COMMON_FLOAT_TYPE))
		return sizeof(float);
	if (!strcasecmp(in_c_type, OPH_COMMON_DOUBLE_TYPE))
		return sizeof(double);
	if (!strcasecmp(in_c_type, OPH_COMMON_BIT_TYPE))
		return sizeof(char);
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type '%s' not supported\n", in_c_type);
	return 0;
}

int oph_esdm_compare_types(int id_container, esdm_type_t var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE])
{
	if (!var_type || !dim_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return -1;
	}
	if (var_type == SMD_DTYPE_INT8) {
		if (strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else if (var_type == SMD_DTYPE_INT16) {
		if (strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else if (var_type == SMD_DTYPE_INT32) {
		if (strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else if (var_type == SMD_DTYPE_INT64) {
		if (strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else if (var_type == SMD_DTYPE_FLOAT) {
		if (strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else if (var_type == SMD_DTYPE_DOUBLE) {
		if (strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			return -1;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		return -1;
	}
	return 0;
}

int update_dim_with_esdm_metadata(ophidiadb * oDB, oph_odb_dimension * time_dim, int id_vocabulary, int id_container_out, ESDM_var * measure)
{
	MYSQL_RES *key_list = NULL;
	MYSQL_ROW row = NULL;

	int num_rows = 0;
	if (oph_odb_meta_find_metadatakey_list(oDB, id_vocabulary, &key_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive key list\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_READ_KEY_LIST);
		if (key_list)
			mysql_free_result(key_list);
		return -1;
	}
	num_rows = mysql_num_rows(key_list);

	if (num_rows)		// The vocabulary is not empty
	{
		int i, num_attr = 0;
		char *key, *variable, *template;
		char value[OPH_COMMON_BUFFER_LEN], svalue[OPH_COMMON_BUFFER_LEN];
		smd_basic_type_t xtype;

		char **keys = (char **) calloc(num_rows, sizeof(char *));
		if (!keys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate key list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_READ_KEY_LIST);
			mysql_free_result(key_list);
			return -1;
		}
		char **values = (char **) calloc(num_rows, sizeof(char *));
		if (!values) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate key list\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_READ_KEY_LIST);
			mysql_free_result(key_list);
			free(keys);
			return -1;
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

				if (variable) {
					esdm_dataset_t *dataset = NULL;
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
					attribute = smd_attr_get_child_by_name(md, key);
					xtype = smd_attr_get_type(attribute);
					if (!xtype)
						strcpy(value, smd_attr_get_value(attribute));
					esdm_dataset_close(dataset);
				} else {
					if (esdm_container_get_attributes(measure->container, &md)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering metadata of the container\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_ATTRIBUTE_ERROR);
						break;
					}
					attribute = smd_attr_get_child_by_name(md, key);
					xtype = smd_attr_get_type(attribute);
					if (!xtype)
						strcpy(value, smd_attr_get_value(attribute));
				}

				if (!xtype)
					continue;

				switch (xtype) {
					case SMD_TYPE_INT8:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((char *) value));
						break;
					case SMD_TYPE_UINT8:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned char *) value));
						break;
					case SMD_TYPE_INT16:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((short *) value));
						break;
					case SMD_TYPE_UINT16:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned short *) value));
						break;
					case SMD_TYPE_INT32:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((int *) value));
						break;
					case SMD_TYPE_UINT32:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned int *) value));
						break;
					case SMD_TYPE_INT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((long long *) value));
						break;
					case SMD_TYPE_UINT64:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", *((unsigned long long *) value));
						break;
					case SMD_TYPE_FLOAT:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((float *) value));
						break;
					case SMD_TYPE_DOUBLE:
						snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", *((double *) value));
						break;
					default:
						strcpy(svalue, value);
				}

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
			return -1;
		}

		if (oph_odb_dim_update_time_dimension(time_dim, keys, values, num_attr)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension metadata\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTNC_UPDATE_TIME_ERROR);
			mysql_free_result(key_list);
			for (i = 0; i < num_attr; ++i) {
				if (keys[i])
					free(keys[i]);
				if (values[i])
					free(values[i]);
			}
			free(keys);
			free(values);
			return -1;
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

	return 0;
}

int oph_esdm_get_dim_array(int id_container, esdm_dataset_t * dataset, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, int start_index, int end_index, char **dim_array)
{
	if (!dataset || !dim_type || !dim_size || !dim_array) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return -1;
	}
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
		return -1;
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
		return -1;
	}
	if (!binary_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Memory error\n");
		return -1;
	}

	if (dim_id >= 0) {

		esdm_type_t type_nc = SMD_DTYPE_UNKNOWN;
		if (oph_esdm_get_esdm_type((char *) dim_type, &type_nc)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return -1;
		}

		esdm_dataspace_t *subspace = NULL;
		if ((esdm_dataspace_create_full(1, count, start, type_nc, &subspace))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return -1;
		}

		if ((esdm_read(dataset, binary_dim, subspace))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			free(binary_dim);
			return -1;
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
			return -1;
		}

	}

	*dim_array = (char *) binary_dim;

	return 0;
}

int oph_esdm_index_by_value(int id_container, ESDM_var * measure, int dim_id, esdm_type_t dim_type, int dim_size, char *value, int want_start, double offset, int *valorder, int *coord_index)
{
	if (!dim_size || !value || !coord_index || !valorder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return -1;
	}

	void *binary_dim = NULL;
	int nearest_point = want_start > 0;
	//I need to evaluate the order of the dimension values: ascending or descending
	int order = 1;		//Default is ascending
	int exact_value = 0;	//If I find the exact value
	int i;

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
		return -1;
	}
	if (!binary_dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
		return -1;
	}

	if (esdm_read(measure->dim_dataset[dim_id], binary_dim, measure->dim_dspace[dim_id])) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
		free(binary_dim);
		return -1;
	}

	if (dim_type == SMD_DTYPE_INT8) {

		char *array_val = binary_dim, value_ = (char) (strtol(value, (char **) NULL, 10));
		if (offset)
			value_ += (char) (want_start ? -offset : offset);
		//Check order of dimension values
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
			if (exact_value) {
				// Nothing to do
				// i is the index I need
			} else {
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
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (exact_value) {
			} else {
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
			if (exact_value) {
			} else {
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
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
		if (order) {
			//Ascending
			for (i = 0; i < dim_size; i++) {
				if (array_val[i] >= value_) {
					if (array_val[i] == value_)
						exact_value = 1;
					break;
				}
			}
			if (exact_value) {
			} else {
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
			if (exact_value) {
			} else {
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
		if (array_val[0] > array_val[dim_size - 1])	//Descending
			order = 0;
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
			if (exact_value) {
			} else {
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
			if (exact_value) {
			} else {
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
		return -1;
	}

	free(binary_dim);
	*coord_index = i;
	*valorder = order;
	return 0;
}

int check_subset_string(char *curfilter, int i, ESDM_var * measure, int is_index, double offset)
{
	int ii;
	char *endfilter = strchr(curfilter, OPH_DIM_SUBSET_SEPARATOR2);
	if (!endfilter && !offset) {
		//Only single point
		//Check curfilter
		if (strlen(curfilter) < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			return -1;
		}
		if (is_index) {
			//Input filter is index
			for (ii = 0; ii < (int) strlen(curfilter); ii++) {
				if (!isdigit(curfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer values allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					return -1;
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
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				} else {
					if (!isdigit(curfilter[ii]) && curfilter[ii] != '.') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter: %s\n", curfilter);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				}
			}
			//End of checking filter

			// Load dimension metadata is needed
			if (!measure->dim_dataset[i])
				if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
					return -1;
				}
			if (!measure->dim_dspace[i])
				if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
					return -1;
				}
			if (measure->dim_dspace[i]->dims != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
			}

			int coord_index = -1;
			int want_start = 1;	//Single point, it is the same
			int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if (oph_esdm_index_by_value(OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], curfilter, want_start, 0, &order, &coord_index)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
				return -1;
			}
			//Value not found
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
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
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
			return -1;
		}
		if (is_index) {
			//Input filter is index         
			for (ii = 0; ii < (int) strlen(startfilter); ii++) {
				if (!isdigit(startfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					return -1;
				}
			}

			for (ii = 0; ii < (int) strlen(endfilter); ii++) {
				if (!isdigit(endfilter[ii])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
					return -1;
				}
			}
			measure->dims_start_index[i] = (int) (strtol(startfilter, (char **) NULL, 10));
			measure->dims_end_index[i] = (int) (strtol(endfilter, (char **) NULL, 10));
		} else {
			//Input filter is a value
			for (ii = 0; ii < (int) strlen(startfilter); ii++) {
				if (ii == 0) {
					if (!isdigit(startfilter[ii]) && startfilter[ii] != '-') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				} else {
					if (!isdigit(startfilter[ii]) && startfilter[ii] != '.') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				}
			}
			for (ii = 0; ii < (int) strlen(endfilter); ii++) {
				if (ii == 0) {
					if (!isdigit(endfilter[ii]) && endfilter[ii] != '-') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				} else {
					if (!isdigit(endfilter[ii]) && endfilter[ii] != '.') {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
						return -1;
					}
				}
			}
			//End of checking filter

			// Load dimension metadata is needed
			if (!measure->dim_dataset[i])
				if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
					return -1;
				}
			if (!measure->dim_dspace[i])
				if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
					return -1;
				}
			if (measure->dim_dspace[i]->dims != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
			}

			int coord_index = -1;
			int want_start = -1;
			int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if (oph_esdm_index_by_value(OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], startfilter, want_start, offset, &order, &coord_index)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING, "");
				return -1;
			}
			//Value too big
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
			}
			measure->dims_start_index[i] = coord_index;

			coord_index = -1;
			want_start = 0;
			order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
			//Extract index of the point given the dimension value
			if (oph_esdm_index_by_value(OPH_GENERIC_CONTAINER_ID, measure, i, measure->dim_dspace[i]->type, measure->dims_length[i], endfilter, want_start, offset, &order, &coord_index)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
			}
			if (coord_index >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTNC_INVALID_INPUT_STRING);
				return -1;
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

	return 0;
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
		return 0;
	}

	if (tot_dim_number == curr_dim) {
		addr = 0;
		for (i = 0; i < tot_dim_number; i++) {
			addr += counters[i] * products[i];
		}

		memcpy(binary_insert + (*index) * sizeof_var, binary_cache + addr * sizeof_var, sizeof_var);
		(*index)++;
		return 0;
	}

	return 0;
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
		return -1;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return -1;
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
		return -1;
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
		return -1;
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
		return -1;
	}

	unsigned long long c_arg = regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(1 + c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		return -1;
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
			return -1;
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
		return -1;
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
	int total = 1;
	for (i = 0; i < measure->ndims; i++)
		if (!measure->dims_type[i] && (!measure->operation || !oph_esdm_is_a_reduce_func(measure->operation)))
			total *= count[i];

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
		return -1;
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
			return -1;
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
			return -1;
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
				limits[tmp_index] = count[i];
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
					return -1;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;
	esdm_dataspace_t *subspace;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type];

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
		return -1;
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
				return -1;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.buff = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (ii = 0; ii < sizeof_type; ii++)
					memcpy(stream_data.buff, fill_value, sizeof_type);

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
					return -1;
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
				return -1;
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
			return -1;
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
			return -1;
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
			return -1;
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
				return -1;
			}

			if (measure->operation) {

				// Initialize stream data
				stream_data.operation = measure->operation;
				stream_data.buff = binary + jj * sizeof_var;
				stream_data.valid = 0;
				stream_data.fill_value = fill_value;
				for (ii = 0; ii < sizeof_type; ii++)
					memcpy(stream_data.buff, fill_value, sizeof_type);

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
					return -1;
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
				return -1;
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
			return -1;
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
	return 0;
}

int oph_esdm_populate_fragment3(oph_ioserver_handler * server, oph_odb_fragment * frag, int tuplexfrag_number, int array_length, int compressed, ESDM_var * measure, long long memory_size)
{
	if (!frag || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return -1;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return -1;
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
		return -1;
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
		return -1;
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
		return -1;
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
		return -1;
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
		return -1;
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
			return -1;
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
		return -1;
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
	int total = 1;
	for (i = 0; i < measure->ndims; i++)
		if (measure->dims_type[i] || !measure->operation || !oph_esdm_is_a_reduce_func(measure->operation))
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
		return -1;
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
			return -1;
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

	//Fill array
	esdm_dataspace_t *subspace = NULL;
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
		return -1;
	}

	if ((esdm_read(measure->dataset, binary_cache, subspace))) {
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
		return -1;
	}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
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
			limits[tmp_index] = count[i];
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
				return -1;
			}
		}
	}

	free(count);
	free(file_indexes);

	size_t sizeof_type = (int) sizeof_var / array_length;

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
			return -1;
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
			return -1;
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
			return -1;
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
			return -1;
		}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_write_time, NULL);
		timeval_subtract(&intermediate_write_time, &end_write_time, &start_write_time);
		timeval_add(&total_write_time, &total_write_time, &intermediate_write_time);
#endif

		oph_ioserver_free_query(server, query);
	}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
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

	return 0;
}

int env_set(HASHTBL * task_tbl, oph_operator_struct * handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!task_tbl) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
		return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
	}

	if (handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
		return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
	}

	if (!(handle->operator_handle = (OPH_IMPORTESDM_operator_handle *) calloc(1, sizeof(OPH_IMPORTESDM_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->create_container = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_grid = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->compressed = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_compliance = 0;
	ESDM_var *nc_measure = &(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure);
	nc_measure->dims_name = NULL;
	nc_measure->dims_id = NULL;
	nc_measure->dims_unlim = NULL;
	nc_measure->dims_length = NULL;
	nc_measure->dims_type = NULL;
	nc_measure->dims_oph_level = NULL;
	nc_measure->dims_start_index = NULL;
	nc_measure->dims_end_index = NULL;
	nc_measure->dims_concept_level = NULL;
	nc_measure->dataset = NULL;
	nc_measure->dspace = NULL;
	nc_measure->dim_dataset = NULL;
	nc_measure->dim_dspace = NULL;
	nc_measure->operation = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run = 1;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_year = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_month = 2;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->memory_size = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description = NULL;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->time_filter = 1;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->execute_error = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->policy = 0;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation = NULL;

	char *value;

	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param
	    (value, &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys, &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	// retrieve sessionid
	value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	char *container_name = (!hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT) ? "NO-CONTAINER" : hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT));

	//3 - Fill struct with the correct data

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_POLICY);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strcmp(value, OPH_COMMON_POLICY_PORT))
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->policy = 1;
	else if (strcmp(value, OPH_COMMON_POLICY_RR)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong input parameter %s\n", OPH_IN_PARAM_POLICY);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_RANDCUBE_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_POLICY);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_BASE_TIME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_BASE_TIME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_BASE_TIME);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_UNITS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_UNITS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_UNITS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CALENDAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CALENDAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_CALENDAR);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MONTH_LENGTHS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MONTH_LENGTHS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, OPH_IN_PARAM_MONTH_LENGTHS);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_YEAR);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_YEAR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_YEAR);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_year = (int) strtol(value, NULL, 10);
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_LEAP_MONTH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LEAP_MONTH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_LEAP_MONTH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_month = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description = (char *) strndup(value, OPH_TP_TASKLEN))) {
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT, "description");
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_TIME_FILTER);
	if (value && !strcmp(value, OPH_COMMON_NO_VALUE))
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->time_filter = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORT_METADATA);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORT_METADATA);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORT_METADATA);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig = strdup(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CONTAINER_INPUT);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->create_container = 1;
		char *pointer = strrchr(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, '/');
		while (pointer && !strlen(pointer)) {
			*pointer = 0;
			pointer = strrchr(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, '/');
		}
		container_name = pointer ? pointer + 1 : ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path;
	} else
		container_name = value;
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input = (char *) strndup(container_name, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "container output name");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_ARG_USERNAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "username");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (strstr(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, "esdm://", 7)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong ESDM object\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wrong ESDM object\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char *tmp = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path;
	value = strstr(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, "//") + 2;
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path = strdup(value);
	free(tmp);

	if (oph_pid_get_memory_size(&(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->memory_size))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_OPHIDIADB_CONFIGURATION_FILE, container_name);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CWD);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CWD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CWD);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "cwd");

		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SCHEDULE_ALGORITHM);

		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_HOST_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HOST_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HOST_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number == 0)
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number = -1;	// All the host of the partition

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_FRAGMENENT_NUMBER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FRAGMENENT_NUMBER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_FRAGMENENT_NUMBER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number = (int) strtol(value, NULL, 10);
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number == 0)
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number = -1;	// The maximum number of fragments

	//Additional check (all distrib arguments must be bigger than 0 or at least -1 if default values are given)
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number == 0 || ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAG_PARAMS_ERROR, container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_FRAG_PARAMS_ERROR, container_name);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	ESDM_var *measure = ((ESDM_var *) & (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure));
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_MEASURE_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	strncpy(measure->varname, value, OPH_ODB_DIM_DIMENSION_SIZE);
	measure->varname[OPH_ODB_DIM_DIMENSION_SIZE] = 0;

	int i;
	char **exp_dim_names = NULL;
	char **imp_dim_names = NULL;
	char **exp_dim_clevels = NULL;
	char **imp_dim_clevels = NULL;
	int exp_number_of_dim_names = 0;
	int imp_number_of_dim_names = 0;
	int imp_number_of_dim_clevels = 0;
	int number_of_dim_clevels = 0;

	//Open netcdf file
	int j = 0;
	esdm_status ret = esdm_init();
	if (ret) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ESDM cannot be initialized\n");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if ((ret = esdm_container_open(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, ESDM_MODE_FLAG_READ, &measure->container))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM object '%s': %s\n", ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path, measure->varname);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NC_OPEN_ERROR_NO_CONTAINER, container_name, "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	//Extract measured variable information
	if ((ret = esdm_dataset_open(measure->container, measure->varname, ESDM_MODE_FLAG_READ, &measure->dataset))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	esdm_dataset_t *dataset = measure->dataset;

	if ((ret = esdm_dataset_get_dataspace(dataset, &measure->dspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	esdm_dataspace_t *dspace = measure->dspace;

	if (oph_esdm_set_esdm_type(measure->vartype, dspace->type)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", "");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NC_INC_VAR_ERROR_NO_CONTAINER, container_name, "");
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ndims = dspace->dims;
	char *tmp_concept_levels = NULL;

	if (ndims) {

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		if (strncmp(value, OPH_IMPORTESDM_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTESDM_DIMENSION_DEFAULT, strlen(OPH_IMPORTESDM_DIMENSION_DEFAULT))) {
			//If implicit is differen't from auto use standard approach
			if (oph_tp_parse_multiple_value_param(value, &imp_dim_names, &imp_number_of_dim_names)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			measure->nimp = imp_number_of_dim_names;

			if (measure->nimp > ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			if (!value) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (strncmp(value, OPH_IMPORTESDM_DIMENSION_DEFAULT, strlen(value)) || strncmp(value, OPH_IMPORTESDM_DIMENSION_DEFAULT, strlen(OPH_IMPORTESDM_DIMENSION_DEFAULT))) {
				//Explicit is not auto, use standard approach
				if (oph_tp_parse_multiple_value_param(value, &exp_dim_names, &exp_number_of_dim_names)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				measure->nexp = exp_number_of_dim_names;
			} else {
				//Use optimized approach with drilldown
				measure->nexp = ndims - measure->nimp;
				exp_dim_names = NULL;
				exp_number_of_dim_names = 0;
			}
			measure->ndims = measure->nexp + measure->nimp;
		} else {
			//Implicit dimension is auto, import as NetCDF file order
			measure->nimp = 1;
			measure->nexp = ndims - 1;
			measure->ndims = ndims;
			exp_dim_names = imp_dim_names = NULL;
			exp_number_of_dim_names = imp_number_of_dim_names = 0;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_EXPLICIT_DIMENSION_CONCEPT_LEVEL);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!(tmp_concept_levels = (char *) malloc(measure->ndims * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "Tmp concpet levels");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(tmp_concept_levels, 0, measure->ndims * sizeof(char));
		if (strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(OPH_COMMON_DEFAULT_CONCEPT_LEVEL))) {
			if (oph_tp_parse_multiple_value_param(value, &exp_dim_clevels, &number_of_dim_clevels)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (number_of_dim_clevels != measure->nexp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			for (i = 0; i < measure->nexp; i++) {
				if ((exp_dim_clevels[i][0] == OPH_HIER_MINUTE_SHORT_NAME[0]) || (exp_dim_clevels[i][0] == OPH_HIER_MONTH_SHORT_NAME[0])) {
					if (!strncmp(exp_dim_clevels[i], OPH_HIER_MINUTE_LONG_NAME, strlen(exp_dim_clevels[i])))
						tmp_concept_levels[i] = OPH_HIER_MINUTE_SHORT_NAME[0];
					else
						tmp_concept_levels[i] = OPH_HIER_MONTH_SHORT_NAME[0];
				} else
					tmp_concept_levels[i] = exp_dim_clevels[i][0];
				if (tmp_concept_levels[i] == OPH_COMMON_ALL_CONCEPT_LEVEL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", OPH_COMMON_ALL_CONCEPT_LEVEL);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_BAD2_PARAMETER, "dimension level", OPH_COMMON_ALL_CONCEPT_LEVEL);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					if (tmp_concept_levels)
						free(tmp_concept_levels);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			oph_tp_free_multiple_value_param_list(exp_dim_clevels, number_of_dim_clevels);
		}
		//Default levels
		else {
			for (i = 0; i < measure->nexp; i++)
				tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPLICIT_DIMENSION_CONCEPT_LEVEL);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_CONCEPT_LEVEL, strlen(OPH_COMMON_DEFAULT_CONCEPT_LEVEL))) {
			if (oph_tp_parse_multiple_value_param(value, &imp_dim_clevels, &imp_number_of_dim_clevels)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			if (imp_number_of_dim_clevels != measure->nimp) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

			for (i = measure->nexp; i < measure->ndims; i++) {
				if ((imp_dim_clevels[i - measure->nexp][0] == OPH_HIER_MINUTE_SHORT_NAME[0]) || (imp_dim_clevels[i - measure->nexp][0] == OPH_HIER_MONTH_SHORT_NAME[0])) {
					if (!strncmp(imp_dim_clevels[i - measure->nexp], OPH_HIER_MINUTE_LONG_NAME, strlen(imp_dim_clevels[i - measure->nexp])))
						tmp_concept_levels[i] = OPH_HIER_MINUTE_SHORT_NAME[0];
					else
						tmp_concept_levels[i] = OPH_HIER_MONTH_SHORT_NAME[0];
				} else
					tmp_concept_levels[i] = imp_dim_clevels[i - measure->nexp][0];
				if (tmp_concept_levels[i] == OPH_COMMON_ALL_CONCEPT_LEVEL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c'\n", OPH_COMMON_ALL_CONCEPT_LEVEL);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_BAD2_PARAMETER, "dimension level", OPH_COMMON_ALL_CONCEPT_LEVEL);
					oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
					oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
					oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
					if (tmp_concept_levels)
						free(tmp_concept_levels);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
			oph_tp_free_multiple_value_param_list(imp_dim_clevels, imp_number_of_dim_clevels);
		}
		//Default levels
		else {
			for (i = measure->nexp; i < measure->ndims; i++)
				tmp_concept_levels[i] = OPH_COMMON_BASE_CONCEPT_LEVEL;
		}

		if (ndims != measure->ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_WRONG_DIM_NUMBER_NO_CONTAINER, container_name, ndims);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
/*
		if (!(measure->dims_name = (char **) malloc(measure->ndims * sizeof(char *)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_name");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(measure->dims_name, 0, measure->ndims * sizeof(char *));
*/
		if (!(measure->dims_length = (size_t *) malloc(measure->ndims * sizeof(size_t)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_length");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_unlim = (char *) malloc(measure->ndims * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_unlim");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_type = (short int *) malloc(measure->ndims * sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_type");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_oph_level");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_concept_level = (char *) calloc(measure->ndims, sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_concept_level");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			if (tmp_concept_levels)
				free(tmp_concept_levels);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(measure->dims_concept_level, 0, measure->ndims * sizeof(char));

	} else {

		measure->ndims = ndims;

	}

	//Extract dimension ids following order in the nc file
	if (!(measure->dims_id = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_id");
		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
		if (tmp_concept_levels)
			free(tmp_concept_levels);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	int64_t const *size = esdm_dataset_get_actual_size(dataset);
	measure->dim_dataset = (esdm_dataset_t **) calloc(measure->ndims, sizeof(esdm_dataset_t *));
	measure->dim_dspace = (esdm_dataspace_t **) calloc(measure->ndims, sizeof(esdm_dataspace_t *));

	//Extract dimensions information and check names provided by task string
	char *dimname = NULL;
	short int flag = 0;
	for (i = 0; i < ndims; i++) {
		measure->dims_unlim[i] = !dspace->size[i];
		//measure->dims_name[i] = (char *) malloc((OPH_ODB_DIM_DIMENSION_SIZE + 1) * sizeof(char));
		measure->dims_length[i] = dspace->size[i] ? dspace->size[i] : size[i];
	}
	ret = esdm_dataset_get_name_dims(dataset, &measure->dims_name);

	int level = 1;
	int m2u[measure->ndims ? measure->ndims : 1];
	m2u[0] = 0;
	if (exp_dim_names != NULL) {
		for (i = 0; i < measure->nexp; i++) {
			flag = 0;
			dimname = exp_dim_names[i];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					m2u[i] = j;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname,
					measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_oph_level[j] = level++;
			measure->dims_type[j] = 1;
			measure->dims_concept_level[j] = tmp_concept_levels[i];
		}
	} else {
		if (imp_dim_names != NULL) {
			int k = 0;
			for (i = 0; i < measure->ndims; i++) {
				flag = 1;
				for (j = 0; j < measure->nimp; j++) {
					dimname = imp_dim_names[j];
					if (!strcmp(dimname, measure->dims_name[i])) {
						//Found implicit dimension
						flag = 0;
						break;
					}
				}
				if (flag) {
					m2u[k] = i;
					measure->dims_oph_level[i] = level++;
					measure->dims_type[i] = 1;
					measure->dims_concept_level[i] = tmp_concept_levels[k];
					k++;
				}
			}
		} else {
			//Use order in nc file
			for (i = 0; i < measure->nexp; i++) {
				m2u[i] = i;

				measure->dims_oph_level[i] = level++;
				measure->dims_type[i] = 1;
				measure->dims_concept_level[i] = tmp_concept_levels[i];
			}
		}
	}

	level = 1;
	if (imp_dim_names != NULL) {
		for (i = measure->nexp; i < measure->ndims; i++) {
			flag = 0;
			dimname = imp_dim_names[i - measure->nexp];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					m2u[i] = j;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname,
					measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				if (tmp_concept_levels)
					free(tmp_concept_levels);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_concept_level[j] = tmp_concept_levels[i];
			measure->dims_type[j] = 0;
			measure->dims_oph_level[j] = level++;
		}
	} else {
		//Use order in nc file
		for (i = measure->nexp; i < measure->ndims; i++) {
			m2u[i] = i;
			measure->dims_concept_level[i] = tmp_concept_levels[i];
			measure->dims_type[i] = 0;
			measure->dims_oph_level[i] = level++;
		}
	}

	oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
	oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
	if (tmp_concept_levels)
		free(tmp_concept_levels);

//ADDED TO MANAGE SUBSETTED IMPORT

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_OFFSET);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char **s_offset = NULL;
	int s_offset_num = 0;
	if (oph_tp_parse_multiple_value_param(value, &s_offset, &s_offset_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	double *offset = NULL;
	if (s_offset_num > 0) {
		offset = (double *) calloc(s_offset_num, sizeof(double));
		if (!offset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "offset");
			oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < s_offset_num; ++i)
			offset[i] = (double) strtod(s_offset[i], NULL);
		oph_tp_free_multiple_value_param_list(s_offset, s_offset_num);
	}

	char **sub_dims = 0;
	char **sub_filters = 0;
	char **sub_types = NULL;
	int number_of_sub_dims = 0;
	int number_of_sub_filters = 0;
	int number_of_sub_types = 0;

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int issubset = 0;
	if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
		issubset = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_FILTER);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	short int isfilter = 0;

	if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
		isfilter = 1;
		if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if ((issubset && !isfilter) || (!issubset && isfilter)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

	if (number_of_sub_dims != number_of_sub_filters) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Check dimension names
	for (i = 0; i < number_of_sub_dims; i++) {
		dimname = sub_dims[i];
		for (j = 0; j < ndims; j++)
			if (!strcmp(dimname, measure->dims_name[j]))
				break;
		if (j == ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension '%s' related to variable '%s' in in nc file\n", dimname, measure->varname);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, container_name, dimname, measure->varname);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	//Check the sub_filters strings
	int tf = -1;		// Id of time filter
	for (i = 0; i < number_of_sub_dims; i++) {
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->time_filter && strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR[1])) {
			if (tf >= 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Not more than one time dimension can be considered\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			tf = i;
			dimname = sub_dims[i];
			for (j = 0; j < ndims; j++)
				if (!strcmp(dimname, measure->dims_name[j]))
					break;
		} else if (strchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2) != strrchr(sub_filters[i], OPH_DIM_SUBSET_SEPARATOR2)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided range are not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

		oph_odb_init_ophidiadb(oDB);

		if (oph_odb_read_ophidiadb_config_file(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_OPHIDIADB_CONFIGURATION_FILE, container_name);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (oph_odb_connect_to_ophidiadb(oDB)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_OPHIDIADB_CONNECTION_ERROR, container_name);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_VOCABULARY);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VOCABULARY);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_VOCABULARY);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN)) {
			if ((oph_odb_meta_retrieve_vocabulary_id(oDB, value, &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input vocabulary\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_VOCABULARY_NO_CONTAINER, container_name, value);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
	}

	if (tf >= 0) {
		oph_odb_dimension dim;
		oph_odb_dimension *time_dim = &dim, *tot_dims = NULL;

		int permission = 0, folder_id = 0, container_exists;
		ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

		if (handle->proc_rank == 0) {

			char *cwd = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd;
			char *user = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user;

			//Check if input path exists
			if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_CWD_ERROR, container_name, cwd);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
				//Check if user can work on datacube
				pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DATACUBE_PERMISSION_ERROR, container_name, user);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			//Check if container exists
			if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		int create_container = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->create_container;
		if (container_exists)
			create_container = 0;

		if (create_container) {
			if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata || !handle->proc_rank) {

				strncpy(dim.base_time, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
				dim.base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
				dim.leap_year = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_year;
				dim.leap_month = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_month;

				if (!measure->dim_dataset[j])
					if ((ret = esdm_dataset_open(measure->container, measure->dims_name[j], ESDM_MODE_FLAG_READ, measure->dim_dataset + j))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
						oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
						oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
						if (offset)
							free(offset);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}

				if (!measure->dim_dspace[j])
					if ((ret = esdm_dataset_get_dataspace(measure->dim_dataset[j], measure->dim_dspace + j))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
						oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
						oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
						if (offset)
							free(offset);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}

				if (oph_esdm_set_esdm_type(dim.dimension_type, measure->dim_dspace[j]->type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
					oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
					oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
					if (offset)
						free(offset);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}

				j = 0;
				strncpy(dim.units, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
				dim.units[OPH_ODB_DIM_TIME_SIZE] = 0;
				strncpy(dim.calendar, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
				dim.calendar[OPH_ODB_DIM_TIME_SIZE] = 0;
				char *tmp = NULL, *save_pointer = NULL, month_lengths[1 + OPH_ODB_DIM_TIME_SIZE];
				strncpy(month_lengths, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
				month_lengths[OPH_ODB_DIM_TIME_SIZE] = 0;
				while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
					dim.month_lengths[j++] = (int) strtol(tmp, NULL, 10);
				while (j < OPH_ODB_DIM_MONTH_NUMBER)
					dim.month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
			}

			if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata) {
				size_t packet_size = sizeof(oph_odb_dimension);
				char buffer[packet_size];

				if (handle->proc_rank == 0) {
					ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

					if (update_dim_with_esdm_metadata
					    (oDB, time_dim, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary, OPH_GENERIC_CONTAINER_ID, measure))
						time_dim->id_dimension = 0;
					else
						time_dim->id_dimension = -1;

					memcpy(buffer, time_dim, packet_size);
					MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
				} else {
					MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
					memcpy(time_dim, buffer, packet_size);
				}

				if (!time_dim->id_dimension) {
					oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
					oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
					if (offset)
						free(offset);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
			}
		} else {
			size_t packet_size = sizeof(oph_odb_dimension);
			char buffer[packet_size];

			if (handle->proc_rank == 0) {
				int flag = 1, id_container_out = 0, number_of_dimensions_c = 0;
				while (flag) {

					if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out) && !id_container_out) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_INPUT_CONTAINER_NO_CONTAINER, container_name,
							container_name);
						break;
					}
					if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions .\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIMENSION_READ_ERROR);
						break;
					}

					for (i = 0; i < number_of_dimensions_c; ++i)
						if (!strncmp(tot_dims[i].dimension_name, measure->dims_name[j], OPH_ODB_DIM_DIMENSION_SIZE))
							break;
					if (i >= number_of_dimensions_c) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[j], container_name);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_CONT_ERROR, measure->dims_name[j], container_name);
						break;
					}

					time_dim = tot_dims + i;

					if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata) {
						// Load the vocabulary associated with the container
						if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary
						    && oph_odb_meta_retrieve_vocabulary_id_from_container(oDB, id_container_out,
													  &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown vocabulary\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_VOCABULARY_NO_CONTAINER, container_name, "");
							break;
						}

						if (update_dim_with_esdm_metadata
						    (oDB, time_dim, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary, id_container_out, measure))
							break;
					}

					flag = 0;
				}
				if (flag)
					time_dim->id_dimension = 0;

				memcpy(buffer, time_dim, packet_size);
				MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
			} else {
				MPI_Bcast(buffer, packet_size, MPI_CHAR, 0, MPI_COMM_WORLD);
				memcpy(time_dim, buffer, packet_size);
			}

			if (!time_dim->id_dimension) {
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		long long max_size = QUERY_BUFLEN;
		oph_pid_get_buffer_size(&max_size);
		char temp[max_size];
		if (oph_dim_parse_time_subset(sub_filters[tf], time_dim, temp)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing time values '%s'\n", sub_filters[tf]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			if (offset)
				free(offset);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		free(sub_filters[tf]);
		sub_filters[tf] = strdup(temp);
	}
	//Alloc space for subsetting parameters
	if (!(measure->dims_start_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_start_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_NO_CONTAINER, container_name, "measure dims_end_index");
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		if (offset)
			free(offset);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &sub_types, &number_of_sub_types)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (number_of_sub_dims && (number_of_sub_types > number_of_sub_dims)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
		if (offset)
			free(offset);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
		oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	char is_index[1 + number_of_sub_dims];
	if (number_of_sub_dims) {
		for (i = 0; i < number_of_sub_types; ++i)
			is_index[i] = strncmp(sub_types[i], OPH_IMPORTESDM_SUBSET_COORD, OPH_TP_TASKLEN);
		for (; i < number_of_sub_dims; ++i)
			is_index[i] = number_of_sub_types == 1 ? is_index[0] : 1;
	}
	is_index[number_of_sub_dims] = 0;
	oph_tp_free_multiple_value_param_list(sub_types, number_of_sub_types);

	char *curfilter = NULL;

	//For each dimension, set the dim_start_index and dim_end_index
	for (i = 0; i < ndims; i++) {
		curfilter = NULL;
		for (j = 0; j < number_of_sub_dims; j++) {
			dimname = sub_dims[j];
			if (!strcmp(dimname, measure->dims_name[i])) {
				curfilter = sub_filters[j];
				break;
			}
		}
		if (!curfilter) {
			//Dimension will not be subsetted
			measure->dims_start_index[i] = 0;
			measure->dims_end_index[i] = measure->dims_length[i] - 1;
		} else {
			int ii;
			if ((ii = check_subset_string(curfilter, i, measure, is_index[j], j < s_offset_num ? offset[j] : 0.0))) {
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return ii;
			} else if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
				   || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				if (offset)
					free(offset);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}
	}

	oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
	oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
	if (offset)
		free(offset);

	//Check explicit dimension oph levels (all values in interval [1 - nexp] should be supplied)
	int curr_lev;
	int level_ok;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		level_ok = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				level_ok = 1;
				break;
			}
		}
		if (!level_ok) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Explicit dimension levels aren't correct\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_EXP_DIM_LEVEL_ERROR, container_name);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
	}

	//Set variable size
	measure->varsize = 1;
	for (j = 0; j < measure->ndims; j++) {
		//Modified to allow subsetting
		if (measure->dims_end_index[j] == measure->dims_start_index[j])
			continue;
		measure->varsize *= (measure->dims_end_index[j] - measure->dims_start_index[j]) + 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMPRESSION);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMPRESSION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_COMPRESSION);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->compressed = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SIMULATE_RUN);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SIMULATE_RUN);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_SIMULATE_RUN);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_NO_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run = 0;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_COMPLIANCE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_COMPLIANCE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CHECK_COMPLIANCE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN) == 0) {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_compliance = 1;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_PARTITION_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_PARTITION_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_PARTITION_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->partition_input = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "input partition");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IOSERVER_TYPE);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IOSERVER_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IOSERVER_TYPE);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "I/O server type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_IMPORTDIM_GRID_NAME);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (strcasecmp(value, OPH_COMMON_DEFAULT_GRID) != 0) {
		if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "grid name");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
	}

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_GRID);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_GRID);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_CHECK_GRID);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strncmp(value, OPH_COMMON_YES_VALUE, OPH_TP_TASKLEN))
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_grid = 1;

	if (handle->proc_rank == 0) {
		//Only master process has to initialize and open connection to management OphidiaDB
		ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_HIERARCHY_NAME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_HIERARCHY_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MISSING_INPUT_PARAMETER, container_name, OPH_IN_PARAM_HIERARCHY_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_COMMON_DEFAULT_HIERARCHY, strlen(value)) || strncmp(value, OPH_COMMON_DEFAULT_HIERARCHY, strlen(OPH_COMMON_DEFAULT_HIERARCHY))) {
			char **dim_hierarchy;
			int number_of_dimensions_hierarchy = 0;
			if (oph_tp_parse_multiple_value_param(value, &dim_hierarchy, &number_of_dimensions_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (measure->ndims != number_of_dimensions_hierarchy) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(number_of_dimensions_hierarchy * sizeof(int)))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
				oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			memset(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, number_of_dimensions_hierarchy * sizeof(int));

			int id_hierarchy = 0, found_oph_time = 0;
			for (i = 0; i < number_of_dimensions_hierarchy; i++) {
				//Retrieve hierarchy ID
				if (oph_odb_dim_retrieve_hierarchy_id(oDB, dim_hierarchy[i], &id_hierarchy)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name, dim_hierarchy[i]);
					oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[m2u[i]] = id_hierarchy;
				if (!strcasecmp(dim_hierarchy[i], OPH_COMMON_TIME_HIERARCHY)) {
					if (found_oph_time) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Only one time dimension can be used\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name,
							dim_hierarchy[i]);
						oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
					((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[m2u[i]] *= -1;
					found_oph_time = 1;
				}
			}
			oph_tp_free_multiple_value_param_list(dim_hierarchy, number_of_dimensions_hierarchy);
		}
		//Default hierarchy
		else {
			if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = (int *) malloc(measure->ndims * sizeof(int)))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT_NO_CONTAINER, container_name, "id dimension hierarchy");
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			memset(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy, 0, measure->ndims * sizeof(int));

			//Retrieve hierarchy ID
			int id_hierarchy = 0;
			if (oph_odb_dim_retrieve_hierarchy_id(oDB, OPH_COMMON_DEFAULT_HIERARCHY, &id_hierarchy)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check input hierarchy, or it doesn't exists\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INPUT_HIERARCHY_ERROR_NO_CONTAINER, container_name,
					OPH_COMMON_DEFAULT_HIERARCHY);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			for (i = 0; i < measure->ndims; i++)
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i] = id_hierarchy;
		}
	}

	value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
	if (!value)
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_job = 0;
	else
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_job = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_REDUCTION_OPERATION);
	if (value) {
		if (!(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation = (char *) strndup(value, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT, OPH_IN_PARAM_REDUCTION_OPERATION);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!strcmp(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation, OPH_COMMON_NONE_FILTER)) {
			free(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation);
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation = NULL;
		}
		measure->operation = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NULL_OPERATOR_HANDLE_NO_CONTAINER,
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//For error checking
	int id_datacube[6] = { 0, 0, 0, 0, 0, 0 };

	int i, flush = 1, id_datacube_out = 0, id_container_out = 0;

	//Access data in the netcdf file
	ESDM_var *measure = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure;

	//Find the last explicit dimension checking oph_value
	short int max_lev = 0;
	short int last_dimid = 0;
	for (i = 0; i < measure->ndims; i++) {
		measure->dims_id[i] = i;
		//Consider only explicit dimensions
		if (measure->dims_type[i]) {
			if (measure->dims_oph_level[i] > max_lev) {
				last_dimid = measure->dims_id[i];
				max_lev = measure->dims_oph_level[i];
				if (measure->dims_end_index[i] == measure->dims_start_index[i])
					((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number = 1;
				else
					((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			}
		}
	}

	//Compute total fragment as the number of values of the explicit dimensions excluding the last one
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number = 1;
	for (i = 0; i < measure->ndims; i++) {
		//Consider only explicit dimensions
		if (measure->dims_type[i] && measure->dims_id[i] != last_dimid) {
			if (measure->dims_end_index[i] == measure->dims_start_index[i])
				continue;
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
		}
	}

	// Compute array length
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->array_length = 1;
	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation || !oph_esdm_is_a_reduce_func(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation)) {
		for (i = 0; i < measure->ndims; i++) {
			//Consider only implicit dimensions
			if (!measure->dims_type[i]) {
				if (measure->dims_end_index[i] == measure->dims_start_index[i])
					continue;
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->array_length *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
			}
		}
	}

	int container_exists = 0;
	char *container_name = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input;
	int create_container = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->create_container;

	if (handle->proc_rank == 0) {
		ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

		int i, j;
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
		int last_insertd_id = 0;
		int *host_number = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number;
		int *fragxdb_number = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number;
		char *host_partition = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->partition_input;
		char *ioserver_type = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type;
		int run = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run;

		//Retrieve user id
		char *username = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user;
		int id_user = 0;
		if (oph_odb_user_retrieve_user_id(oDB, username, &id_user)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive user id\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_USER_ID_ERROR);
			goto __OPH_EXIT_1;
		}

	  /********************************
	   *INPUT PARAMETERS CHECK - BEGIN*
	   ********************************/
		int exist_part = 0;
		int nhost = 0;
		int frag_param_error = 0;
		int final_frag_number = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number;

		int max_frag_number = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number * ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number;

		int admissible_frag_number = 0;
		int user_arg_prod = 0;
		int id_host_partition = 0;
		char hidden = 0;

		//If default values are used: select partition
		if (!strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(host_partition))
		    && !strncmp(host_partition, OPH_COMMON_HOSTPARTITION_DEFAULT, strlen(OPH_COMMON_HOSTPARTITION_DEFAULT))) {
			if (oph_odb_stge_get_default_host_partition_fs(oDB, ioserver_type, id_user, &id_host_partition, *host_number > 0 ? *host_number : 1) || !id_host_partition) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts or dbms per host is too big or server type and partition are not available!\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
						((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number, host_partition);
					goto __OPH_EXIT_1;
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts or dbms per host is too big or server type and partition are not available!\n");
					frag_param_error = 1;
				}
			}
		} else {
			if (oph_odb_stge_get_host_partition_by_name(oDB, host_partition, id_user, &id_host_partition, &hidden)) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to load partition '%s'!\n", host_partition);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Failed to load partition '%s'!\n", host_partition);
					goto __OPH_EXIT_1;
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to load partition '%s'!\n", host_partition);
					frag_param_error = 1;
				}
			}
			//Check if are available DBMS and HOST number into specified partition and of server type
			if (*host_number > 0) {
				if ((oph_odb_stge_check_number_of_host_dbms(oDB, ioserver_type, id_host_partition, *host_number, &exist_part)) || !exist_part) {
					if (run) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
							((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number, host_partition);
						goto __OPH_EXIT_1;
					} else {
						//If simulated run then reset values
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
						frag_param_error = 1;
					}
				}
			}
		}

		if (*host_number <= 0) {
			//Check how many DBMS and HOST are available into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ioserver_type, id_host_partition, &nhost) || !nhost) {
				if (run) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or server type and partition are not available!\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name,
						host_partition);
					goto __OPH_EXIT_1;
				} else {
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name,
						host_partition);
					frag_param_error = 1;
				}
			}
		}

		if (*host_number > 0 || *fragxdb_number > 0) {
			//At least one argument is specified
			if (*host_number <= 0) {
				if (*fragxdb_number <= 0) {
					user_arg_prod = (1 * 1);
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER,
								container_name, final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						if (final_frag_number % user_arg_prod != 0) {
							if (run) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								goto __OPH_EXIT_1;
							} else {
								frag_param_error = 1;
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
							}
						} else {
							admissible_frag_number = final_frag_number / user_arg_prod;
							if (admissible_frag_number <= nhost) {
								*host_number = admissible_frag_number;
								*fragxdb_number = 1;
							} else {
								//Get highest divisor for host_number
								int ii = 0;
								for (ii = nhost; ii > 0; ii--) {
									if (admissible_frag_number % ii == 0)
										break;
								}
								*host_number = ii;
								*fragxdb_number = (int) admissible_frag_number / (*host_number);
							}
						}
					}
				} else {
					//If user specified fragxdb_number then check if frag number is lower than product of parameters                       
					user_arg_prod = (1 * (*fragxdb_number));
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER,
								container_name, final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						if (final_frag_number % user_arg_prod != 0) {
							if (run) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								goto __OPH_EXIT_1;
							} else {
								frag_param_error = 1;
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
							}
						} else {
							admissible_frag_number = final_frag_number / user_arg_prod;
							if (admissible_frag_number <= nhost) {
								*host_number = admissible_frag_number;
							} else {
								//Get highest divisor for host_number
								int ii = 0;
								for (ii = nhost; ii > 0; ii--) {
									if (admissible_frag_number % ii == 0)
										break;
								}
								*host_number = ii;
								//Since fragxdb is fixed recompute tuplexfrag
								((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number =
								    (int) ceilf((float) max_frag_number / ((*host_number) * user_arg_prod));
								((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number = ((*host_number) * (*fragxdb_number));
							}
						}
					}
				}
			} else {
				if (*fragxdb_number <= 0) {
					user_arg_prod = ((*host_number) * 1);
					if (final_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER,
								container_name, final_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, final_frag_number);
							frag_param_error = 1;
						}
					} else {
						*fragxdb_number = (int) ceilf((float) final_frag_number / user_arg_prod);
					}
				} else {
					//User has set all parameters - in this case allow further fragmentation
					user_arg_prod = ((*host_number) * (*fragxdb_number));
					if (max_frag_number < user_arg_prod) {
						//If import is executed then return error, else simply return a message
						if (run) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, max_frag_number);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER,
								container_name, max_frag_number);
							goto __OPH_EXIT_1;
						} else {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT3_FAILED_NO_CONTAINER, container_name, max_frag_number);
							frag_param_error = 1;
						}
					} else {
						if (max_frag_number % user_arg_prod != 0) {
							if (run) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
								goto __OPH_EXIT_1;
							} else {
								frag_param_error = 1;
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_FRAGMENTATION_ERROR);
							}
						} else {
							((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number = (int) ceilf((float) max_frag_number / user_arg_prod);
							((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number = ((*host_number) * (*fragxdb_number));
						}
					}
				}
			}
		} else {
			//Default case
			user_arg_prod = 1;
			admissible_frag_number = final_frag_number / user_arg_prod;
			if (admissible_frag_number <= nhost) {
				*host_number = admissible_frag_number;
				*fragxdb_number = 1;
			} else {
				*host_number = nhost;
				*fragxdb_number = (int) ceilf((float) admissible_frag_number / (*host_number));
			}
		}

		if (frag_param_error) {
			//Check how many DBMS and HOST are available into specified partition and of server type
			if (oph_odb_stge_count_number_of_host_dbms(oDB, ioserver_type, id_host_partition, &nhost) || !nhost) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive number of host or server type and partition are not available!\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT2_FAILED_NO_CONTAINER, container_name, host_partition);
				goto __OPH_EXIT_1;
			}

			int ii = 0;
			for (ii = nhost; ii > 0; ii--) {
				if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number % ii == 0)
					break;
			}

			//Simulate simple arguments
			*host_number = ii;
			*fragxdb_number = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number / ii;
		}

		if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run) {
			char message[OPH_COMMON_BUFFER_LEN] = { 0 };
			int len = 0;

			if (frag_param_error)
				printf("Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				printf("Specified parameters are:\n");
			printf("\tNumber of hosts: %d\n", *host_number);
			printf("\tNumber of fragments per database: %d\n", *fragxdb_number);
			printf("\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (frag_param_error)
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters cannot be used with this file!\nAllowed parameters are:\n");
			else
				len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "Specified parameters are:\n");
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of hosts: %d\n", *host_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of fragments per database: %d\n", ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number);
			len += snprintf(message + len, OPH_COMMON_BUFFER_LEN, "\tNumber of tuples per fragment: %d\n", ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number);

			if (oph_json_is_objkey_printable
			    (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num,
			     OPH_JSON_OBJKEY_IMPORTESDM)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTESDM, "Fragmentation parameters", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, "ADD TEXT error\n");
				}
			}
			goto __OPH_EXIT_1;
		}
		//Check if are available DBMS and HOST number into specified partition and of server type
		if ((oph_odb_stge_check_number_of_host_dbms(oDB, ioserver_type, id_host_partition, *host_number, &exist_part)) || !exist_part) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Requested number of hosts is too big or server type and partition are not available!\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_HOST_DBMS_CONSTRAINT_FAILED_NO_CONTAINER, container_name,
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number, host_partition);
			goto __OPH_EXIT_1;
		}

		char *cwd = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd;
		char *user = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user;
		ESDM_var tmp_var;
		int varid;

		int permission = 0;
		int folder_id = 0;
		//Check if input path exists
		if ((oph_odb_fs_path_parsing("", cwd, &folder_id, NULL, oDB))) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Path %s doesn't exists\n", cwd);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_CWD_ERROR, container_name, cwd);
			goto __OPH_EXIT_1;
		}
		if ((oph_odb_fs_check_folder_session(folder_id, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid, oDB, &permission)) || !permission) {
			//Check if user can work on datacube
			pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work in this folder\n", user);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DATACUBE_PERMISSION_ERROR, container_name, user);
			goto __OPH_EXIT_1;
		}
		//Check if container exists
		if ((oph_odb_fs_check_if_container_not_present(oDB, container_name, folder_id, &container_exists))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		if (container_exists)
			create_container = 0;
		if (create_container) {

			if (!oph_odb_fs_is_allowed_name(container_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "%s not allowed for new folders/containers\n", container_name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_NAME_NOT_ALLOWED_ERROR, container_name);
				goto __OPH_EXIT_1;
			}
			//Check if container exists in folder
			int container_unique = 0;
			if ((oph_odb_fs_is_unique(folder_id, container_name, oDB, &container_unique))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check output container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_OUTPUT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			if (!container_unique) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Container '%s' already exists in this path or a folder has the same name\n", container_name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_OUTPUT_CONTAINER_EXIST_ERROR, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			//If it doesn't then create new container and get last id
			oph_odb_container cont;
			strncpy(cont.container_name, container_name, OPH_ODB_FS_CONTAINER_SIZE);
			cont.container_name[OPH_ODB_FS_CONTAINER_SIZE] = 0;
			cont.id_parent = 0;
			cont.id_folder = folder_id;
			cont.operation[0] = 0;
			cont.id_vocabulary = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary;

			if (oph_odb_fs_insert_into_container_table(oDB, &cont, &id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container = id_container_out;

			if (container_exists && oph_odb_fs_add_suffix_to_container_name(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update container table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_INSERT_CONTAINER_ERROR_NO_CONTAINER, container_name, container_name);
				goto __OPH_EXIT_1;
			}
			//Create new dimensions
			oph_odb_dimension dim;
			dim.id_container = id_container_out;
			strncpy(dim.base_time, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time, OPH_ODB_DIM_TIME_SIZE);
			dim.base_time[OPH_ODB_DIM_TIME_SIZE] = 0;
			dim.leap_year = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_year;
			dim.leap_month = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->leap_month;

			for (i = 0; i < measure->ndims; i++) {

				if (!measure->dim_dataset[i])
					if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
						goto __OPH_EXIT_1;
					}

				if (!measure->dim_dspace[i])
					if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
						goto __OPH_EXIT_1;
					}

				if (oph_esdm_set_esdm_type(dim.dimension_type, measure->dim_dspace[i]->type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
					goto __OPH_EXIT_1;
				}
				// Load dimension names
				strncpy(dim.dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE);
				dim.dimension_name[OPH_ODB_DIM_DIMENSION_SIZE] = 0;
				dim.id_hierarchy = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy[i];
				if (dim.id_hierarchy >= 0)
					dim.units[0] = dim.calendar[0] = 0;
				else {
					int j = 0;
					dim.id_hierarchy *= -1;
					strncpy(dim.units, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units, OPH_ODB_DIM_TIME_SIZE);
					dim.units[OPH_ODB_DIM_TIME_SIZE] = 0;
					strncpy(dim.calendar, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar, OPH_ODB_DIM_TIME_SIZE);
					dim.calendar[OPH_ODB_DIM_TIME_SIZE] = 0;
					char *tmp = NULL, *save_pointer = NULL, month_lengths[1 + OPH_ODB_DIM_TIME_SIZE];
					strncpy(month_lengths, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths, OPH_ODB_DIM_TIME_SIZE);
					month_lengths[OPH_ODB_DIM_TIME_SIZE] = 0;
					while ((tmp = strtok_r(tmp ? NULL : month_lengths, ",", &save_pointer)) && (j < OPH_ODB_DIM_MONTH_NUMBER))
						dim.month_lengths[j++] = (int) strtol(tmp, NULL, 10);
					while (j < OPH_ODB_DIM_MONTH_NUMBER)
						dim.month_lengths[j++] = OPH_ODB_DIM_DAY_NUMBER;
				}
				if (oph_odb_dim_insert_into_dimension_table(oDB, &(dim), &last_insertd_id, 0)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert dimension.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INSERT_DIMENSION_ERROR);
					goto __OPH_EXIT_1;
				}
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_create_db(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DB_CREATION);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			char dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			snprintf(dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
			if (oph_dim_create_empty_table(db, dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_CREATION_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				goto __OPH_EXIT_1;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

		} else if (!container_exists) {
			//If it doesn't exist then return an error
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Else retreive container ID and check for dimension table
		if (!create_container && oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, container_name, &id_container_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_INPUT_CONTAINER_NO_CONTAINER, container_name, container_name);
			goto __OPH_EXIT_1;
		}
		//Check vocabulary
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata) {
			if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary
			    && oph_odb_meta_retrieve_vocabulary_id_from_container(oDB, id_container_out, &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown vocabulary\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_NO_VOCABULARY_NO_CONTAINER, container_name, "");
				goto __OPH_EXIT_1;
			}
		}
		//Load dimension table database infos and connect
		oph_odb_db_instance db_;
		oph_odb_db_instance *db_dimension = &db_;
		if (oph_dim_load_dim_dbinstance(db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_LOAD);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_connect_to_dbms(db_dimension->dbms_instance, 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_CONNECT);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (oph_dim_use_db_of_dbms(db_dimension->dbms_instance, db_dimension)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
		snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
		snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);
		int exist_flag = 0;

		if (oph_dim_check_if_dimension_table_exists(db_dimension, index_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_RETREIVE_ERROR, index_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		if (oph_dim_check_if_dimension_table_exists(db_dimension, label_dimension_table_name, &exist_flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving dimension table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_RETREIVE_ERROR, label_dimension_table_name);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (!exist_flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimensions table doesn't exists\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_USE_DB);
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}

		esdm_type_t xtype;
		int id_grid = 0, time_dimension = -1;
		oph_odb_dimension_instance *dim_inst = NULL;
		int dim_inst_num = 0;
		oph_odb_dimension *dims = NULL;
		int *dimvar_ids = malloc(sizeof(int) * measure->ndims);
		if (!dimvar_ids) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating dimvar_ids\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Error allocating dimvar_ids\n");
			oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
			oph_dim_unload_dim_dbinstance(db_dimension);
			goto __OPH_EXIT_1;
		}
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name
		    && !oph_odb_dim_retrieve_grid_id(oDB, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &id_grid) && id_grid) {
			//Check if ophidiadb dimensions are the same of input dimensions

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_grid_in_container
			    (oDB, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name, id_container_out, &dims, &dim_inst, &dim_inst_num)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input grid name not usable! It is already used by another container.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NO_GRID, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				if (dims)
					free(dims);
				if (dim_inst)
					free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;

			}

			if (dim_inst_num != measure->ndims) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension number doesn't match with specified grid dimension number\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INPUT_GRID_DIMENSION_MISMATCH, "number");
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dims);
				free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}

			int match = 1;
			int found_flag;
			char *dim_array = NULL;
			int curdimlength = 1;

			for (i = 0; i < measure->ndims; i++) {
				dimvar_ids[i] = -1;
				//Check if container dimension name, size and concept level matches input dimension params -
				found_flag = 0;
				for (j = 0; j < dim_inst_num; j++) {
					if (!strncmp(dims[j].dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE)) {
						//Modified to allow subsetting
						if (measure->dims_start_index[i] == measure->dims_end_index[i])
							curdimlength = 1;
						else
							curdimlength = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
						if (dim_inst[j].size == curdimlength && dim_inst[j].concept_level == measure->dims_concept_level[i]) {

							if (!measure->dim_dataset[i])
								if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
									logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "");
									oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
									oph_dim_unload_dim_dbinstance(db_dimension);
									free(dims);
									free(dim_inst);
									free(dimvar_ids);
									goto __OPH_EXIT_1;
								}
							if (!measure->dim_dspace[i])
								if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
									logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "");
									oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
									oph_dim_unload_dim_dbinstance(db_dimension);
									free(dims);
									free(dim_inst);
									free(dimvar_ids);
									goto __OPH_EXIT_1;
								}

							dimvar_ids[i] = i;
							if ( /*(dimvar_ids[i] >= 0) && */ oph_esdm_compare_types(id_container_out, measure->dim_dspace[i]->type, dims[j].dimension_type)) {
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension type in NC file doesn't correspond to the one stored in OphidiaDB\n");
								logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TYPE_MISMATCH_ERROR, measure->dims_name[i]);
							}

							if (measure->dim_dspace[i]->dims != 1) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_NOT_ALLOWED);
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}

							dim_array = NULL;
							if (oph_esdm_get_dim_array
							    (id_container_out, measure->dim_dataset[i], dimvar_ids[i], dims[j].dimension_type, dim_inst[j].size, measure->dims_start_index[i],
							     measure->dims_end_index[i], &dim_array)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "");
								oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
								oph_dim_unload_dim_dbinstance(db_dimension);
								free(dims);
								free(dim_inst);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
							}

							if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_grid || (!oph_dim_compare_dimension
																	  (db_dimension, label_dimension_table_name,
																	   dims[j].dimension_type, dim_inst[j].size, dim_array,
																	   dim_inst[j].fk_id_dimension_label, &match)
																	  && !match)) {
								free(dim_array);
								found_flag = 1;
								break;
							}

							free(dim_array);
						}
					}
				}

				if (!found_flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Input dimension '%s' doesn't match with specified container/grid dimensions\n", measure->dims_name[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INPUT_DIMENSION_MISMATCH, measure->dims_name[i]);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dims);
					free(dim_inst);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
			}
		} else if (measure->ndims) {
		 /****************************
	      * BEGIN - IMPORT DIMENSION *
		  ***************************/
			int number_of_dimensions_c = 0, i, j;
			oph_odb_dimension *tot_dims = NULL;

			//Read dimension
			if (oph_odb_dim_retrieve_dimension_list_from_container(oDB, id_container_out, &tot_dims, &number_of_dimensions_c)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimensions.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIMENSION_READ_ERROR);
				if (tot_dims)
					free(tot_dims);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}

			int dimension_array_id = 0;
			char *dim_array = NULL, collapsed = 0;
			int exists = 0;
			char filename[2 * OPH_TP_BUFLEN];
			oph_odb_hierarchy hier;
			dims = (oph_odb_dimension *) malloc(measure->ndims * sizeof(oph_odb_dimension));
			dim_inst = (oph_odb_dimension_instance *) malloc(measure->ndims * sizeof(oph_odb_dimension_instance));
			dim_inst_num = measure->ndims;

			long long kk;
			long long *index_array = NULL;

			//For each input dimension check hierarchy
			for (i = 0; i < measure->ndims; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncasecmp(tot_dims[j].dimension_name, measure->dims_name[i], strlen(tot_dims[j].dimension_name)))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[i],
					      ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				//Find dimension level into hierarchy file
				if (oph_odb_dim_retrieve_hierarchy(oDB, tot_dims[j].id_hierarchy, &hier)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				exist_flag = 0;
				snprintf(filename, 2 * OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, hier.filename);
				if (oph_hier_check_concept_level_short(filename, measure->dims_concept_level[i], &exists)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error retrieving hierarchy\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_HIERARCHY_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (!exists) {
					if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata && (time_dimension < 0)
					    && !strcmp(hier.hierarchy_name, OPH_COMMON_TIME_HIERARCHY))
						time_dimension = i;
					else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to set concept level to '%c' from '%s': check container specifications\n", measure->dims_concept_level[i],
						      filename);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_BAD2_PARAMETER, measure->dims_concept_level[i]);
						free(tot_dims);
						free(dims);
						free(dim_inst);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				}
			}

			oph_odb_dimension_grid new_grid;
			int id_grid = 0, grid_exist = 0;
			if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name) {
				strncpy(new_grid.grid_name, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name, OPH_ODB_DIM_GRID_SIZE);
				new_grid.grid_name[OPH_ODB_DIM_GRID_SIZE] = 0;
				int last_inserted_grid_id = 0;
				if (oph_odb_dim_insert_into_grid_table(oDB, &new_grid, &last_inserted_grid_id, &grid_exist)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert grid in OphidiaDB, or grid already exists.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_GRID_INSERT_ERROR);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (grid_exist) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Grid already exists: dimensions will be not associated to a grid.\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, "Grid already exists: dimensions will be not associated to a grid.\n");
				} else
					id_grid = last_inserted_grid_id;
			}
			//For each input dimension
			for (i = 0; i < measure->ndims; i++) {
				//Find container dimension
				for (j = 0; j < number_of_dimensions_c; j++) {
					if (!strncasecmp(tot_dims[j].dimension_name, measure->dims_name[i], strlen(tot_dims[j].dimension_name)))
						break;
				}
				if (j == number_of_dimensions_c) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension %s not found for container %s\n", measure->dims_name[i],
					      ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_CONT_ERROR, measure->dims_name[i],
						((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}

				varid = i;
				if (!measure->dim_dataset[i])
					if (esdm_dataset_open(measure->container, measure->dims_name[i], ESDM_MODE_FLAG_READ, measure->dim_dataset + i)) {
						if (create_container) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
							free(tot_dims);
							free(dims);
							free(dim_inst);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							free(dimvar_ids);
							goto __OPH_EXIT_1;
						} else {
							varid = -1;
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Fill dimension '%s' with integers\n", measure->dims_name[i]);
							logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, "Fill dimension '%s' with integers\n", measure->dims_name[i]);
						}
					}
				dimvar_ids[i] = varid;

				if (varid >= 0) {
					if (!measure->dim_dspace[i])
						if (esdm_dataset_get_dataspace(measure->dim_dataset[i], measure->dim_dspace + i)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
							free(tot_dims);
							free(dims);
							free(dim_inst);
							oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
							oph_dim_unload_dim_dbinstance(db_dimension);
							free(dimvar_ids);
							goto __OPH_EXIT_1;
						}
					xtype = measure->dim_dspace[i]->type;
				}

				dim_array = NULL;

				//Create entry in dims and dim_insts
				dims[i].id_dimension = tot_dims[j].id_dimension;
				dims[i].id_container = tot_dims[j].id_container;
				dims[i].id_hierarchy = tot_dims[j].id_hierarchy;
				snprintf(dims[i].dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "%s", tot_dims[j].dimension_name);
				snprintf(dims[i].dimension_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE, "%s", tot_dims[j].dimension_type);

				dim_inst[i].id_dimension = tot_dims[j].id_dimension;
				dim_inst[i].fk_id_dimension_index = 0;
				dim_inst[i].fk_id_dimension_label = 0;
				dim_inst[i].id_grid = id_grid;
				dim_inst[i].id_dimensioninst = 0;
				//Modified to allow subsetting
				if (measure->dims_type[i] || !((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation
				    || !oph_esdm_is_a_reduce_func(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation))
					tmp_var.varsize = 1 + measure->dims_end_index[i] - measure->dims_start_index[i];
				else
					tmp_var.varsize = 1;
				dim_inst[i].size = tmp_var.varsize;
				dim_inst[i].concept_level = measure->dims_concept_level[i];	// TODO: see next note
				dim_inst[i].unlimited = measure->dims_unlim[i] ? 1 : 0;

				if ((varid >= 0) && oph_esdm_compare_types(id_container_out, xtype, tot_dims[j].dimension_type)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension type in NC file doesn't correspond to the one stored in OphidiaDB\n");
					logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_TYPE_MISMATCH_ERROR, measure->dims_name[i]);
				}

				collapsed = 0;
				if (measure->dims_type[i] || !((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation
				    || !oph_esdm_is_a_reduce_func(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation)) {
					if (oph_esdm_get_dim_array
					    (id_container_out, measure->dim_dataset[i], varid, tot_dims[j].dimension_type, tmp_var.varsize, measure->dims_start_index[i], measure->dims_end_index[i],
					     &dim_array)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", "");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "");
						free(tot_dims);
						free(dims);
						free(dim_inst);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				} else {
					collapsed = 1;
					dim_inst[i].size = 0;
					dim_inst[i].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
					dim_array = NULL;
				}

				if (!collapsed) {
					if (!collapsed
					    && oph_dim_insert_into_dimension_table(db_dimension, label_dimension_table_name, tot_dims[j].dimension_type, tmp_var.varsize, dim_array,
										   &dimension_array_id)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_ROW_ERROR, tot_dims[j].dimension_name);
						free(tot_dims);
						free(dims);
						free(dim_inst);
						free(dim_array);
						oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
						oph_dim_unload_dim_dbinstance(db_dimension);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
					free(dim_array);
				} else
					dimension_array_id = 0;
				dim_inst[i].fk_id_dimension_label = dimension_array_id;	// Real dimension

				if (!collapsed) {
					index_array = (long long *) malloc(tmp_var.varsize * sizeof(long long));
					for (kk = 0; kk < tmp_var.varsize; ++kk)
						index_array[kk] = 1 + kk;	// Non 'C'-like indexing
				} else
					index_array = NULL;
				if (oph_dim_insert_into_dimension_table
				    (db_dimension, index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, collapsed ? 0 : tmp_var.varsize, collapsed ? NULL : (char *) index_array,
				     &dimension_array_id)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIM_ROW_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					free(index_array);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				if (index_array)
					free(index_array);
				dim_inst[i].fk_id_dimension_index = dimension_array_id;	// Indexes

				if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[i]), &dimension_array_id, 0, NULL, NULL)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new dimension instance row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIMINST_INSERT_ERROR, tot_dims[j].dimension_name);
					free(tot_dims);
					free(dims);
					free(dim_inst);
					oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
					oph_dim_unload_dim_dbinstance(db_dimension);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				dim_inst[i].id_dimensioninst = dimension_array_id;
			}
			free(tot_dims);

			if (id_grid && oph_odb_dim_enable_grid(oDB, id_grid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to enable grid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, "Unable to enable grid\n");
				free(dims);
				free(dim_inst);
				oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
				oph_dim_unload_dim_dbinstance(db_dimension);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
		 /****************************
	      *  END - IMPORT DIMENSION  *
		  ***************************/
		}
		oph_dim_disconnect_from_dbms(db_dimension->dbms_instance);
		oph_dim_unload_dim_dbinstance(db_dimension);
	  /********************************
	   * INPUT PARAMETERS CHECK - END *
	   ********************************/

	  /********************************
	   *  DATACUBE CREATION - BEGIN   *
	   ********************************/
		//Set fragment string
		char *tmp = id_string;
		if (oph_ids_create_new_id_string(&tmp, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, 1, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment ids string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_CREATE_ID_STRING_ERROR);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		oph_odb_datacube cube;
		oph_odb_cube_init_datacube(&cube);

		//Import source name
		oph_odb_source src;
		int id_src = 0;
		strncpy(src.uri, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig, OPH_ODB_CUBE_SOURCE_URI_SIZE);
		src.uri[OPH_ODB_CUBE_SOURCE_URI_SIZE] = 0;
		if (oph_odb_cube_insert_into_source_table(oDB, &src, &id_src)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert source URI\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INSERT_SOURCE_URI_ERROR, src.uri);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		//Set datacube params
		cube.hostxdatacube = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number;
		cube.fragmentxdb = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number;
		cube.tuplexfragment = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		cube.id_container = id_container_out;
		strncpy(cube.measure, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.varname, OPH_ODB_CUBE_MEASURE_SIZE);
		cube.measure[OPH_ODB_CUBE_MEASURE_SIZE] = 0;
		strncpy(cube.measure_type, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.vartype, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
		cube.measure_type[OPH_ODB_CUBE_MEASURE_TYPE_SIZE] = 0;
		strncpy(cube.frag_relative_index_set, id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
		cube.frag_relative_index_set[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE] = 0;
		cube.db_number = cube.hostxdatacube;
		cube.compressed = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->compressed;
		cube.id_db = NULL;
		//New fields
		cube.id_source = id_src;
		cube.level = 0;
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description)
			snprintf(cube.description, OPH_ODB_CUBE_DESCRIPTION_SIZE, "%s", ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description);
		else
			*cube.description = 0;

		//Check dimensions
		oph_odb_cubehasdim *cubedim = (oph_odb_cubehasdim *) malloc(measure->ndims * sizeof(oph_odb_cubehasdim));

		//Check if dimensions exists and insert
		for (i = 0; i < measure->ndims; i++) {
			//Find input dimension with same name of container dimension
			for (j = 0; j < dim_inst_num; j++) {
				if (!strncmp(dims[j].dimension_name, measure->dims_name[i], OPH_ODB_DIM_DIMENSION_SIZE))
					break;
			}
			if (j == dim_inst_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive dimension %s in OphidiaDB.\n", measure->dims_name[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DIMENSION_ODB_ERROR, measure->dims_name[i]);
				oph_odb_cube_free_datacube(&cube);
				free(dims);
				free(dim_inst);
				free(cubedim);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			cubedim[i].id_dimensioninst = dim_inst[j].id_dimensioninst;
			cubedim[i].explicit_dim = measure->dims_type[i];
			cubedim[i].level = measure->dims_oph_level[i];
		}

		//If everything was ok insert cubehasdim relations
		//Insert new datacube
		if (oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &id_datacube_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DATACUBE_INSERT_ERROR);
			oph_odb_cube_free_datacube(&cube);
			free(dims);
			free(dim_inst);
			free(cubedim);
			free(dimvar_ids);
			goto __OPH_EXIT_1;
		}
		oph_odb_cube_free_datacube(&cube);

		for (i = 0; i < measure->ndims; i++) {
			cubedim[i].id_datacube = id_datacube_out;
			if (oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedim[i]), &last_insertd_id)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new datacube - dimension relations.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_CUBEHASDIM_INSERT_ERROR);
				free(dims);
				free(cubedim);
				free(dim_inst);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
		}
		free(dims);
		free(cubedim);
		free(dim_inst);
	  /********************************
	   *   DATACUBE CREATION - END    *
	   ********************************/

	  /********************************
	   *   METADATA IMPORT - BEGIN    *
	   ********************************/
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->import_metadata) {
			//Check vocabulary and metadata key presence
			//NOTE: the type of the key values is fixed to text
			//Retrieve 'text' type id
			char key_type[OPH_COMMON_TYPE_SIZE];
			snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_METADATA_TYPE_TEXT);
			int id_key_type = 0, sid_key_type;
			if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &id_key_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			//Read all vocabulary keys
			MYSQL_RES *key_list = NULL;
			int num_rows = 0;
			if (oph_odb_meta_find_metadatakey_list(oDB, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_vocabulary, &key_list)) {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to retreive key list\n");
				logging(LOG_WARNING, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_READ_KEY_LIST);
				mysql_free_result(key_list);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			num_rows = mysql_num_rows(key_list);

			//Load all keys into a hash table with variable:key form
			HASHTBL *key_tbl = NULL, *required_tbl = NULL;
			if (!(key_tbl = hashtbl_create(num_rows + 1, NULL))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT, "Key hash table");
				mysql_free_result(key_list);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			if (!(required_tbl = hashtbl_create(num_rows + 1, NULL))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_MEMORY_ERROR_INPUT, "Required hash table");
				mysql_free_result(key_list);
				free(dimvar_ids);
				hashtbl_destroy(key_tbl);
				goto __OPH_EXIT_1;
			}

			char key_and_variable[OPH_COMMON_BUFFER_LEN];
			if (num_rows)	// The vocabulary is not empty
			{
				MYSQL_ROW row;
				//For each ROW - insert key into hashtable
				while ((row = mysql_fetch_row(key_list))) {
					memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
					snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", row[2] ? row[2] : "", row[1]);
					hashtbl_insert(key_tbl, key_and_variable, row[0]);
					hashtbl_insert(required_tbl, key_and_variable, row[3]);
				}
			}
			mysql_free_result(key_list);

			//Get global attributes from nc file
			char key[OPH_COMMON_BUFFER_LEN], svalue[OPH_COMMON_BUFFER_LEN];
			char *id_key, *keyptr, is_string;
			int id_metadatainstance;
			keyptr = key;
			int natts = 0;

			smd_attr_t *md, *current;
			smd_basic_type_t xtype;
			if ((esdm_container_get_attributes(measure->container, &md))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of global attributes\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_NATTS_ERROR);
				hashtbl_destroy(key_tbl);
				hashtbl_destroy(required_tbl);
				free(dimvar_ids);
				goto __OPH_EXIT_1;
			}
			natts = md->children;

			//For each global attribute find the corresponding key
			for (i = 0; i < natts; i++) {

				current = smd_attr_get_child(md, i);

				if (!strcmp(current->name, _NC_DIMS) || !strcmp(current->name, _NC_SIZES))	// Skip these special attibutes
					continue;

				// Check for attribute name
				memset(key, 0, OPH_COMMON_BUFFER_LEN);
				strcpy(keyptr, current->name);

				// Check for attribute type
				xtype = smd_attr_get_type(current);
				is_string = (xtype == SMD_TYPE_CHAR) || (xtype == SMD_TYPE_STRING) || (xtype == SMD_TYPE_ARRAY);
				if (!is_string) {
					switch (xtype) {
						case SMD_TYPE_CHAR:
						case SMD_TYPE_INT8:
						case SMD_TYPE_UINT8:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case SMD_TYPE_INT16:
						case SMD_TYPE_UINT16:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case SMD_TYPE_INT32:
						case SMD_TYPE_UINT32:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case SMD_TYPE_INT64:
						case SMD_TYPE_UINT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case SMD_TYPE_FLOAT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
							break;
						case SMD_TYPE_DOUBLE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve metadata key type id\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							goto __OPH_EXIT_1;
					}
					if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve metadata key type id\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				} else
					sid_key_type = id_key_type;

				memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
				snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, ":%s", key);
				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

				//Check if the :key is inside the hashtble
				id_key = hashtbl_get(key_tbl, key_and_variable);

				//Insert key value into OphidiaDB
				switch (xtype) {
					case SMD_TYPE_CHAR:{
							char value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%c", value);
							break;
						}
					case SMD_TYPE_INT8:{
							int8_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT8:{
							uint8_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT16:{
							int16_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT16:{
							uint16_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT32:{
							int32_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT32:{
							uint32_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT64:{
							int64_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (long long) value);
							break;
						}
					case SMD_TYPE_UINT64:{
							uint64_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (unsigned long long) value);
							break;
						}
					case SMD_TYPE_FLOAT:{
							float value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_DOUBLE:{
							double value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_STRING:
					case SMD_TYPE_ARRAY:
						strncpy(svalue, (char *) smd_attr_get_value(current), current->type->size < OPH_COMMON_BUFFER_LEN ? current->type->size : OPH_COMMON_BUFFER_LEN);
						break;
					default:;
				}
				//Insert metadata instance (also manage relation)
				if (oph_odb_meta_insert_into_metadatainstance_manage_tables
				    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, NULL, sid_key_type, id_user, svalue, &id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INSERT_METADATAINSTANCE_ERROR, key, svalue);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				// Drop the metadata out of the hashtable
				if (id_key)
					hashtbl_remove(key_tbl, key_and_variable);
			}

			//Get local attributes from nc file for each dimension variable
			int ii;
			for (ii = 0; ii < measure->ndims; ii++) {
				if (dimvar_ids[ii] < 0)
					continue;

				if (!measure->dim_dataset[ii])
					if ((esdm_dataset_open(measure->container, measure->dims_name[ii], ESDM_MODE_FLAG_READ, measure->dim_dataset + ii))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: type cannot be converted\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_IMPORTESDM_DIM_READ_ERROR, "type cannot be converted");
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
				if ((esdm_dataset_get_attributes(measure->dim_dataset[ii], &md))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of global attributes\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_NATTS_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					free(dimvar_ids);
					goto __OPH_EXIT_1;
				}
				natts = md->children;

				//For each local attribute find the corresponding key
				for (i = 0; i < natts; i++) {

					current = smd_attr_get_child(md, i);

					// Check for attribute name
					memset(key, 0, OPH_COMMON_BUFFER_LEN);
					strcpy(keyptr, current->name);

					// Check for attribute type
					xtype = smd_attr_get_type(current);
					is_string = (xtype == SMD_TYPE_CHAR) || (xtype == SMD_TYPE_STRING) || (xtype == SMD_TYPE_ARRAY);
					if (!is_string) {
						switch (xtype) {
							case SMD_TYPE_CHAR:
							case SMD_TYPE_INT8:
							case SMD_TYPE_UINT8:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
								break;
							case SMD_TYPE_INT16:
							case SMD_TYPE_UINT16:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
								break;
							case SMD_TYPE_INT32:
							case SMD_TYPE_UINT32:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
								break;
							case SMD_TYPE_INT64:
							case SMD_TYPE_UINT64:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
								break;
							case SMD_TYPE_FLOAT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
								break;
							case SMD_TYPE_DOUBLE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
								break;
							default:
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve metadata key type id\n");
								logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
								hashtbl_destroy(key_tbl);
								hashtbl_destroy(required_tbl);
								free(dimvar_ids);
								goto __OPH_EXIT_1;
						}
						if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve metadata key type id '%s'\n", key_type);
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							free(dimvar_ids);
							goto __OPH_EXIT_1;
						}
					} else
						sid_key_type = id_key_type;

					memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
					snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", measure->dims_name[ii], key);
					memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

					//Check if the variable:key is inside the hashtble
					id_key = hashtbl_get(key_tbl, key_and_variable);

					//Insert key value into OphidiaDB
					switch (xtype) {
						case SMD_TYPE_CHAR:{
								char value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%c", value);
								break;
							}
						case SMD_TYPE_INT8:{
								int8_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_UINT8:{
								uint8_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_INT16:{
								int16_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_UINT16:{
								uint16_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_INT32:{
								int32_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_UINT32:{
								uint32_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
								break;
							}
						case SMD_TYPE_INT64:{
								int64_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (long long) value);
								break;
							}
						case SMD_TYPE_UINT64:{
								uint64_t value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (unsigned long long) value);
								break;
							}
						case SMD_TYPE_FLOAT:{
								float value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
								break;
							}
						case SMD_TYPE_DOUBLE:{
								double value;
								smd_attr_copy_value(current, &value);
								snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
								break;
							}
						case SMD_TYPE_STRING:
						case SMD_TYPE_ARRAY:
							strncpy(svalue, (char *) smd_attr_get_value(current),
								current->type->size < OPH_COMMON_BUFFER_LEN ? current->type->size : OPH_COMMON_BUFFER_LEN);
							break;
						default:;
					}

					//Insert metadata instance (also manage relation)
					if (oph_odb_meta_insert_into_metadatainstance_manage_tables
					    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, measure->dims_name[ii], sid_key_type, id_user, svalue, &id_metadatainstance)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INSERT_METADATAINSTANCE_ERROR, key, svalue);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						free(dimvar_ids);
						goto __OPH_EXIT_1;
					}
					// Drop the metadata out of the hashtable
					if (id_key)
						hashtbl_remove(key_tbl, key_and_variable);
				}
			}
			if (dimvar_ids) {
				free(dimvar_ids);
				dimvar_ids = NULL;
			}

			if ((esdm_dataset_get_attributes(measure->dataset, &md))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error recovering number of global attributes\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NC_NATTS_ERROR);
				hashtbl_destroy(key_tbl);
				hashtbl_destroy(required_tbl);
				goto __OPH_EXIT_1;
			}
			natts = md->children;

			//For each local attribute find the corresponding key
			for (i = 0; i < natts; i++) {

				current = smd_attr_get_child(md, i);

				// Check for attribute name
				memset(key, 0, OPH_COMMON_BUFFER_LEN);
				strcpy(keyptr, current->name);

				// Check for attribute type
				xtype = smd_attr_get_type(current);
				is_string = (xtype == SMD_TYPE_CHAR) || (xtype == SMD_TYPE_STRING) || (xtype == SMD_TYPE_ARRAY);
				if (!is_string) {
					switch (xtype) {
						case SMD_TYPE_CHAR:
						case SMD_TYPE_INT8:
						case SMD_TYPE_UINT8:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
							break;
						case SMD_TYPE_INT16:
						case SMD_TYPE_UINT16:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
							break;
						case SMD_TYPE_INT32:
						case SMD_TYPE_UINT32:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
							break;
						case SMD_TYPE_INT64:
						case SMD_TYPE_UINT64:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
							break;
						case SMD_TYPE_FLOAT:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
							break;
						case SMD_TYPE_DOUBLE:
							snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
							break;
						default:
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
							logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
							hashtbl_destroy(key_tbl);
							hashtbl_destroy(required_tbl);
							goto __OPH_EXIT_1;
					}
					if (oph_odb_meta_retrieve_metadatatype_id(oDB, key_type, &sid_key_type)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive metadata key type id\n");
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_METADATATYPE_ID_ERROR);
						hashtbl_destroy(key_tbl);
						hashtbl_destroy(required_tbl);
						goto __OPH_EXIT_1;
					}
				} else
					sid_key_type = id_key_type;

				memset(key_and_variable, 0, OPH_COMMON_BUFFER_LEN);
				snprintf(key_and_variable, OPH_COMMON_BUFFER_LEN, "%s:%s", measure->varname, key);
				memset(svalue, 0, OPH_COMMON_BUFFER_LEN);

				//Check if the variable:key is inside the hashtble
				id_key = hashtbl_get(key_tbl, key_and_variable);

				//Insert key value into OphidiaDB
				switch (xtype) {
					case SMD_TYPE_CHAR:{
							char value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%c", value);
							break;
						}
					case SMD_TYPE_INT8:{
							int8_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT8:{
							uint8_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT16:{
							int16_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT16:{
							uint16_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT32:{
							int32_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_UINT32:{
							uint32_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%d", value);
							break;
						}
					case SMD_TYPE_INT64:{
							int64_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (long long) value);
							break;
						}
					case SMD_TYPE_UINT64:{
							uint64_t value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%lld", (unsigned long long) value);
							break;
						}
					case SMD_TYPE_FLOAT:{
							float value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_DOUBLE:{
							double value;
							smd_attr_copy_value(current, &value);
							snprintf(svalue, OPH_COMMON_BUFFER_LEN, "%f", value);
							break;
						}
					case SMD_TYPE_STRING:
					case SMD_TYPE_ARRAY:
						strncpy(svalue, (char *) smd_attr_get_value(current), current->type->size < OPH_COMMON_BUFFER_LEN ? current->type->size : OPH_COMMON_BUFFER_LEN);
						break;
					default:;
				}

				//Insert metadata instance (also manage relation)
				if (oph_odb_meta_insert_into_metadatainstance_manage_tables
				    (oDB, id_datacube_out, id_key ? (int) strtol(id_key, NULL, 10) : -1, key, measure->varname, sid_key_type, id_user, svalue, &id_metadatainstance)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update metadatainstance table\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_INSERT_METADATAINSTANCE_ERROR, key, svalue);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
				// Drop the metadata out of the hashtable
				if (id_key)
					hashtbl_remove(key_tbl, key_and_variable);
			}

			if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->check_compliance)	// Check if all the mandatory metadata are taken
			{
				short found_a_required_attribute = 0;
				while (!found_a_required_attribute && (id_key = hashtbl_pop_key(key_tbl))) {
					keyptr = hashtbl_get(required_tbl, id_key);
					found_a_required_attribute = keyptr ? (short) strtol(keyptr, NULL, 10) : 0;
					free(id_key);
				}
				if (found_a_required_attribute) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_COMPLIANCE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_COMPLIANCE_ERROR);
					hashtbl_destroy(key_tbl);
					hashtbl_destroy(required_tbl);
					goto __OPH_EXIT_1;
				}
			}

			hashtbl_destroy(key_tbl);
			hashtbl_destroy(required_tbl);

			if (create_container)	// Check for time dimensions
			{
				for (i = 0; i < measure->ndims; i++) {
					ii = oph_odb_dim_set_time_dimension(oDB, id_datacube_out, (char *) measure->dims_name[i]);
					if (!ii)
						break;
					else if (ii != OPH_ODB_NO_ROW_FOUND)	// Time dimension cannot be set
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_UPDATE_TIME_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_UPDATE_TIME_ERROR);
						goto __OPH_EXIT_1;
					}
				}
			} else if ((time_dimension >= 0) && oph_odb_dim_set_time_dimension(oDB, id_datacube_out, (char *) measure->dims_name[time_dimension])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_IMPORTESDM_SET_TIME_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_SET_TIME_ERROR);
				goto __OPH_EXIT_1;
			}
		}
	  /********************************
	   *    METADATA IMPORT - END     *
	   ********************************/

	  /********************************
	   * DB INSTANCE CREATION - BEGIN *
	   ********************************/
		int dbmss_length, host_num;
		dbmss_length = host_num = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number;
		int *id_dbmss = NULL, *id_hosts = NULL;
		//Retreive ID dbms list
		if (oph_odb_stge_retrieve_dbmsinstance_id_list
		    (oDB, ioserver_type, id_host_partition, hidden, host_num, id_datacube_out, &id_dbmss, &id_hosts, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->policy)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve DBMS list.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DBMS_LIST_ERROR);
			if (id_dbmss)
				free(id_dbmss);
			if (id_hosts)
				free(id_hosts);
			goto __OPH_EXIT_1;
		}

		oph_odb_db_instance db;
		oph_odb_dbms_instance dbms;
		char db_name[OPH_ODB_STGE_DB_NAME_SIZE];

		for (j = 0; j < dbmss_length; j++) {
			db.id_dbms = id_dbmss[j];
			//Retreive DBMS params
			if (oph_odb_stge_retrieve_dbmsinstance(oDB, db.id_dbms, &dbms)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive DBMS\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DBMS_ERROR, db.id_dbms);
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}
			db.dbms_instance = &dbms;

			if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server) {
				if (oph_dc_setup_dbms(&(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server), dbms.io_server_type)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_IOPLUGIN_SETUP_ERROR, db.id_dbms);
					free(id_dbmss);
					free(id_hosts);
					goto __OPH_EXIT_1;
				}
			}

			if (oph_dc_connect_to_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms), 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DBMS_CONNECTION_ERROR, dbms.id_dbms);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms));
				free(id_dbmss);
				free(id_hosts);
				goto __OPH_EXIT_1;
			}

			if (oph_dc_generate_db_name(oDB->name, id_datacube_out, db.id_dbms, 0, 1, &db_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of Db instance  name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_STRING_BUFFER_OVERFLOW, "DB instance name", db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			strcpy(db.db_name, db_name);
			if (oph_dc_create_db(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create new db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_NEW_DB_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			//Insert new database instance and partitions
			if (oph_odb_stge_insert_into_dbinstance_partitioned_tables(oDB, &db, id_datacube_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dbinstance table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_DB_INSERT_ERROR, db.db_name);
				free(id_dbmss);
				free(id_hosts);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms));
				goto __OPH_EXIT_1;
			}
			oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbms));
		}
		free(id_dbmss);
		free(id_hosts);
	  /********************************
	   *  DB INSTANCE CREATION - END  *
	   ********************************/

		last_insertd_id = 0;
		oph_odb_task new_task;
		new_task.id_outputcube = id_datacube_out;
		new_task.id_job = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_job;
		memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
		strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
		i = snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_DC_SQ_MULTI_INSERT_FRAG, "frag") - 1;
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->compressed)
			i += snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) - 1;
		else
			i += snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, OPH_DC_SQ_MULTI_INSERT_ROW) - 1;
		snprintf(new_task.query + i, OPH_ODB_CUBE_OPERATION_QUERY_SIZE - i, ";");
		new_task.input_cube_number = 0;
		new_task.id_inputcube = NULL;
		if (oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container_out, OPH_LOG_OPH_IMPORTESDM_TASK_INSERT_ERROR, new_task.operator);
			goto __OPH_EXIT_1;
		}

		id_datacube[0] = id_datacube_out;
		id_datacube[1] = id_container_out;
		id_datacube[2] = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number;
		id_datacube[3] = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number;
		id_datacube[4] = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number;
		id_datacube[5] = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number;

		flush = 0;
	}
      __OPH_EXIT_1:
	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (!handle->proc_rank && flush) {
		while (id_container_out && create_container) {
			ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;

			//Remove also grid related to container dimensions
			if (oph_odb_dim_delete_from_grid_table(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting grid related to container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_GRID_DELETE_ERROR);
				break;
			}
			//Delete container and related dimensions/ dimension instances
			if (oph_odb_fs_delete_from_container_table(oDB, id_container_out)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting input container\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_CONTAINER_DELETE_ERROR);
				break;
			}

			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DIM_LOAD);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DIM_CONNECT);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DIM_USE_DB);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}

			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_NAME_MACRO, id_container_out);
			snprintf(label_dimension_table_name, OPH_COMMON_BUFFER_LEN, OPH_DIM_TABLE_LABEL_MACRO, id_container_out);

			if (oph_dim_delete_table(db, index_dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_DELETE_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}
			if (oph_dim_delete_table(db, label_dimension_table_name)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while deleting dimension table\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DIM_TABLE_DELETE_ERROR);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				break;
			}

			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

			break;
		}
		oph_odb_cube_delete_from_datacube_table(&((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB, id_datacube_out);
	}
	//Broadcast to all other processes the result
	MPI_Bcast(id_datacube, 6, MPI_INT, 0, MPI_COMM_WORLD);

	//Check if sequential part has been completed
	if (!id_datacube[0] || !id_datacube[1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube[1], OPH_LOG_OPH_IMPORTESDM_MASTER_TASK_INIT_FAILED_NO_CONTAINER, container_name);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->execute_error = 1;
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube = id_datacube[0];
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container = id_datacube[1];
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->host_number = id_datacube[2];
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number = id_datacube[3];
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number = id_datacube[4];
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number = id_datacube[5];

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int frag_total_number = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->total_frag_number;

	//All processes compute the fragment number to work on
	int div_result = (frag_total_number) / (handle->proc_number);
	int div_remainder = (frag_total_number) % (handle->proc_number);

	//Every process must process at least divResult
	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number = div_result;

	if (div_remainder != 0) {
		//Only some certain processes must process an additional part
		if (handle->proc_rank / div_remainder == 0)
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number++;
	}

	int i;
	//Compute fragment IDs starting position
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number == 0) {
		// In case number of process is higher than fragment number
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	} else {
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id = 0;
		for (i = handle->proc_rank - 1; i >= 0; i--) {
			if (div_remainder != 0)
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id += (div_result + (i / div_remainder == 0 ? 1 : 0));
			else
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id += div_result;
		}
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id >= frag_total_number)
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id = -1;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id < 0 && handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->execute_error = 1;

	int i, j, k;
	int id_datacube_out = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube;
	oph_odb_db_instance_list dbs;
	oph_odb_dbms_instance_list dbmss;

	//Compute DB list starting position and number of rows
	int start_position =
	    (int) floor((double) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id / ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number);
	int row_number =
	    (int) ceil((double) (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id + ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number) /
		       ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

	//Each process has to be connected to a slave ophidiadb
	ophidiadb oDB_slave;
	oph_odb_init_ophidiadb(&oDB_slave);

	if (oph_odb_read_ophidiadb_config_file(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_OPHIDIADB_CONFIGURATION_FILE,
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	if (oph_odb_connect_to_ophidiadb(&oDB_slave)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_OPHIDIADB_CONNECTION_ERROR,
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	//retrieve connection string
	if (oph_odb_stge_fetch_db_connection_string(&oDB_slave, id_datacube_out, start_position, row_number, &dbs, &dbmss)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_CONNECTION_STRINGS_NOT_FOUND);
		oph_odb_free_ophidiadb(&oDB_slave);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_fragment new_frag;
	char fragment_name[OPH_ODB_STGE_FRAG_NAME_SIZE];

	int frag_to_insert = 0;
	int frag_count = 0;

	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server) {
		if (oph_dc_setup_dbms(&(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server), dbmss.value[0].io_server_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_IOPLUGIN_SETUP_ERROR,
				dbmss.value[0].id_dbms);
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
	}
	//For each DBMS
	for (i = 0; i < dbmss.size; i++) {

		if (oph_dc_connect_to_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DBMS_CONNECTION_ERROR,
				(dbmss.value[i]).id_dbms);
			oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
			oph_odb_stge_free_db_list(&dbs);
			oph_odb_stge_free_dbms_list(&dbmss);
			oph_odb_free_ophidiadb(&oDB_slave);
			return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
		}
		//For each DB
		for (j = 0; j < dbs.size; j++) {
			//Check DB - DBMS Association
			if (dbs.value[j].dbms_instance != &(dbmss.value[i]))
				continue;

			if (oph_dc_use_db_of_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DB_SELECTION_ERROR,
					(dbs.value[j]).db_name);
				oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
				oph_odb_stge_free_db_list(&dbs);
				oph_odb_stge_free_dbms_list(&dbmss);
				oph_odb_free_ophidiadb(&oDB_slave);
				return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			}
			//Compute number of fragments to insert in DB
			frag_to_insert =
			    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number - (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id +
													    frag_count -
													    start_position *
													    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number);

			//For each fragment
			for (k = 0; k < frag_to_insert; k++) {

				//Set new fragment
				new_frag.id_datacube = id_datacube_out;
				new_frag.id_db = dbs.value[j].id_db;
				new_frag.frag_relative_index = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id + frag_count + 1;
				new_frag.key_start =
				    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number + 1;
				new_frag.key_end =
				    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id * ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    frag_count * ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number +
				    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number;
				new_frag.db_instance = &(dbs.value[j]);

				if (oph_dc_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count + 1), &fragment_name)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTESDM_STRING_BUFFER_OVERFLOW, "fragment name", fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				strcpy(new_frag.fragment_name, fragment_name);
				//Create Empty fragment
				if (oph_dc_create_empty_fragment(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while creating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTESDM_FRAGMENT_CREATION_ERROR, new_frag.fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//Populate fragment
				if (oph_esdm_populate_fragment3
				    (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &new_frag, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->tuplexfrag_number,
				     ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->array_length, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->compressed,
				     (ESDM_var *) & (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure),
				     ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->memory_size)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while populating fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTESDM_FRAG_POPULATE_ERROR, new_frag.fragment_name, "");
					oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
				}
				//Insert new fragment
				if (oph_odb_stge_insert_into_fragment_table(&oDB_slave, &new_frag)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTESDM_FRAGMENT_INSERT_ERROR, new_frag.fragment_name);
					oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));
					oph_odb_stge_free_db_list(&dbs);
					oph_odb_stge_free_dbms_list(&dbmss);
					oph_odb_free_ophidiadb(&oDB_slave);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				frag_count++;
				if (frag_count == ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number)
					break;
			}
			start_position++;
		}
		oph_dc_disconnect_from_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server, &(dbmss.value[i]));

	}
	oph_odb_stge_free_db_list(&dbs);
	oph_odb_stge_free_dbms_list(&dbmss);
	oph_odb_free_ophidiadb(&oDB_slave);

	((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->execute_error = 0;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	if (!((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->run)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	short int proc_error = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->execute_error;
	int id_datacube = ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube;
	short int global_error = 0;

	//Reduce results
	MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MAX, MPI_COMM_WORLD);

	if (handle->proc_rank == 0 && global_error == 0) {
		//Master process print output datacube PID
		char *tmp_uri = NULL;
		if (oph_pid_get_uri(&tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_PID_URI_ERROR);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_pid_show_pid
		    (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube, tmp_uri)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_PID_SHOW_ERROR);
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		char jsonbuf[OPH_COMMON_BUFFER_LEN];
		memset(jsonbuf, 0, OPH_COMMON_BUFFER_LEN);
		snprintf(jsonbuf, OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
			 ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_output_datacube);

		// ADD OUTPUT PID TO JSON AS TEXT
		if (oph_json_is_objkey_printable
		    (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num, OPH_JSON_OBJKEY_IMPORTESDM)) {
			if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_IMPORTESDM, "Output Cube", jsonbuf)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
				free(tmp_uri);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		// ADD OUTPUT PID TO NOTIFICATION STRING
		char tmp_string[OPH_COMMON_BUFFER_LEN];
		snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_DATACUBE_INPUT, jsonbuf);
		if (handle->output_string) {
			strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN - strlen(tmp_string));
			free(handle->output_string);
		}
		handle->output_string = strdup(tmp_string);

		free(tmp_uri);
	}

	if (global_error) {
		//For error checking
		char id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
		memset(id_string, 0, sizeof(id_string));

		if (handle->proc_rank == 0) {
			ophidiadb *oDB = &((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB;
			oph_odb_datacube cube;
			oph_odb_cube_init_datacube(&cube);

			//retrieve input datacube
			if (oph_odb_cube_retrieve_datacube(oDB, id_datacube, &cube)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_DATACUBE_READ_ERROR);
			} else {
				//Copy fragment id relative index set 
				strncpy(id_string, cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
			}
			oph_odb_cube_free_datacube(&cube);
		}
		//Broadcast to all other processes the fragment relative index        
		MPI_Bcast(id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

		//Check if sequential part has been completed
		if (id_string[0] == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_OPH_IMPORTESDM_MASTER_TASK_INIT_FAILED);
		} else {
			if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id >= 0 || handle->proc_rank == 0) {
				//Partition fragment relative index string
				char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];
				char *new_ptr = new_id_string;
				if (oph_ids_get_substring_from_string
				    (id_string, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id,
				     ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number, &new_ptr)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
						OPH_LOG_OPH_IMPORTESDM_ID_STRING_SPLIT_ERROR);
				} else {
					//Delete fragments
					int start_position =
					    (int) floor((double) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id /
							((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number);
					int row_number = (int)
					    ceil((double)
						 (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_first_id +
						  ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragment_number) /
						 ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->fragxdb_number) - start_position;

					if (oph_dproc_delete_data
					    (id_datacube, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, new_id_string, start_position, row_number, 1)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete fragments\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container,
							OPH_LOG_OPH_DELETE_DB_READ_ERROR);
					}
				}
			}
		}

		if (handle->output_code)
			proc_error = (short int) handle->output_code;
		else
			proc_error = OPH_ODB_JOB_STATUS_DESTROY_ERROR;
		MPI_Allreduce(&proc_error, &global_error, 1, MPI_SHORT, MPI_MIN, MPI_COMM_WORLD);
		handle->output_code = global_error;

		//Delete from OphidiaDB
		if (handle->proc_rank == 0) {
			oph_dproc_clean_odb(&((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB, id_datacube,
					    ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container);
		}

		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_PROCESS_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_PROCESS_ERROR);

		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	//Only master process has to close and release connection to management OphidiaDB
	if (handle->proc_rank == 0) {
		oph_odb_free_ophidiadb(&((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->oDB);
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->container_input = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->cwd = NULL;
	}

	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server)
		oph_dc_cleanup_dbms(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->server);

	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->ioserver_type = NULL;
	}

	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->user = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->grid_name = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->nc_file_path_orig = NULL;
	}

	ESDM_var *measure = ((ESDM_var *) & (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure));
/*
	if (measure->dims_name) {
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_name[i]) {
				free((char *) measure->dims_name[i]);
				measure->dims_name[i] = NULL;
			}
		}
		free(measure->dims_name);
		measure->dims_name = NULL;
	}
*/
	if (measure->dims_id) {
		free((int *) measure->dims_id);
		measure->dims_id = NULL;
	}
	if (measure->dims_unlim) {
		free((char *) measure->dims_unlim);
		measure->dims_unlim = NULL;
	}
	if (measure->dims_length) {
		free((size_t *) measure->dims_length);
		measure->dims_length = NULL;
	}
	if (measure->dims_type) {
		free((short int *) measure->dims_type);
		measure->dims_type = NULL;
	}
	if (measure->dims_oph_level) {
		free((short int *) measure->dims_oph_level);
		measure->dims_oph_level = NULL;
	}
	if (measure->dims_start_index) {
		free((int *) measure->dims_start_index);
		measure->dims_start_index = NULL;
	}
	if (measure->dims_end_index) {
		free((int *) measure->dims_end_index);
		measure->dims_end_index = NULL;
	}
	if (measure->dims_concept_level) {
		free((char *) measure->dims_concept_level);
		measure->dims_concept_level = NULL;
	}

	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->partition_input) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->partition_input);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->partition_input = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy) {
		free((int *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->id_dimension_hierarchy = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys, ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->sessionid = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->base_time = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->units = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->calendar = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->month_lengths = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->description = NULL;
	}
	if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation) {
		free((char *) ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation);
		((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->operation = NULL;
	}

	int i;
	for (i = 0; i < ((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.ndims; ++i) {
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dspace) {
			free(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dspace);
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dspace = NULL;
		}
		if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset) {
			if (((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset[i]) {
				esdm_dataset_close(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset[i]);
				((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset[i] = NULL;
			}
			free(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset);
			((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dim_dataset = NULL;
		}
	}

	esdm_dataset_close(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.dataset);
	esdm_container_close(((OPH_IMPORTESDM_operator_handle *) handle->operator_handle)->measure.container);

	esdm_finalize();

	free((OPH_IMPORTESDM_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
