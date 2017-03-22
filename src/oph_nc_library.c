/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#include "oph_nc_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <zlib.h>
#include <math.h>

#include "oph-lib-binary-io.h"
#include "debug.h"

#include "oph_log_error_codes.h"

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

extern int msglevel;

int _oph_nc_cache_to_buffer(short int tot_dim_number, short int curr_dim, unsigned int *counters, unsigned int *limits, unsigned int *products, long long *index, char *binary_cache,
			    char *binary_insert, size_t sizeof_var)
{
	int i = 0;
	long long addr = 0;


	if (tot_dim_number > curr_dim) {

		if (curr_dim != 0)
			counters[curr_dim] = 0;
		while (counters[curr_dim] < limits[curr_dim]) {
			_oph_nc_cache_to_buffer(tot_dim_number, curr_dim + 1, counters, limits, products, index, binary_cache, binary_insert, sizeof_var);
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

int oph_nc_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var)
{
	long long index = 0;
	return _oph_nc_cache_to_buffer(tot_dim_number, 0, counters, limits, products, &index, binary_cache, binary_insert, sizeof_var);
}

int oph_nc_populate_fragment_from_nc(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure)
{
	if (!frag || !ncid || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	if (oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_NC_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case NC_BYTE:
		case NC_CHAR:
			type_flag = OPH_NC_BYTE_FLAG;
			break;
		case NC_SHORT:
			type_flag = OPH_NC_SHORT_FLAG;
			break;
		case NC_INT:
			type_flag = OPH_NC_INT_FLAG;
			break;
		case NC_INT64:
			type_flag = OPH_NC_LONG_FLAG;
			break;
		case NC_FLOAT:
			type_flag = OPH_NC_FLOAT_FLAG;
			break;
		case NC_DOUBLE:
			type_flag = OPH_NC_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_NC_DOUBLE_FLAG;
	}

	char insert_query[QUERY_BUFLEN];
	int n;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		n = snprintf(insert_query, QUERY_BUFLEN, OPH_DC_SQ_INSERT_COMPRESSED_FRAG, frag->fragment_name);
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_INSERT_FRAG "\n", frag->fragment_name);
#endif
		n = snprintf(insert_query, QUERY_BUFLEN, OPH_DC_SQ_INSERT_FRAG, frag->fragment_name);
	}

	if (n >= QUERY_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_NC_ERROR;
	}

	unsigned long sizeof_var = 0;

	unsigned long long idDim = 0;

	int l;

	if (type_flag == OPH_NC_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_NC_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_NC_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		sizeof_var = (array_length) * sizeof(double);
	else
		sizeof_var = (array_length) * sizeof(double);

	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_NC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length);
	else if (type_flag == OPH_NC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length);
	else if (type_flag == OPH_NC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		return OPH_NC_ERROR;
	}

	int c_arg = 3, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(binary);
		return OPH_NC_ERROR;
	}

	for (ii = 0; ii < c_arg - 1; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_NC_ERROR;
		}

	}
	args[c_arg - 1] = NULL;

	args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;
	args[0]->arg = (unsigned long long *) (&idDim);

	args[1]->arg_length = sizeof_var;
	args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[1]->arg_is_null = 0;
	args[1]->arg = (char *) (binary);
	idDim = frag->key_start;

	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, insert_query, tuplexfrag_number, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_NC_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	size_t *start = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	size_t *count = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc((measure->nexp) * sizeof(size_t *));
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
		if (!measure->dims_type[i])
			total *= count[i];

	if (total != array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_NC_ERROR;
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
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
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
		if (type_flag == OPH_NC_BYTE_FLAG)
			res = oph_iob_bin_array_create_b(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_SHORT_FLAG)
			res = oph_iob_bin_array_create_s(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_INT_FLAG)
			res = oph_iob_bin_array_create_i(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_LONG_FLAG)
			res = oph_iob_bin_array_create_l(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_FLOAT_FLAG)
			res = oph_iob_bin_array_create_f(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_DOUBLE_FLAG)
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		else
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
		}
		//Prepare structures for buffer insert update
		tmp_index = 0;
		counters = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned int));
		int *file_indexes = (int *) malloc((measure->nimp) * sizeof(int));
		products = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		limits = (unsigned int *) malloc((measure->nimp) * sizeof(unsigned));
		int k = 0, j = 0;

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
					free(binary);
					for (ii = 0; ii < c_arg - 1; ii++)
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
					free(counters);
					free(file_indexes);
					free(products);
					free(limits);
					return OPH_NC_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;

	for (l = 0; l < tuplexfrag_number; l++) {
		oph_nc_compute_dimension_id(idDim, sizemax, measure->nexp, start_pointer);

		for (i = 0; i < measure->nexp; i++) {
			*(start_pointer[i]) -= 1;
			for (ii = 0; ii < measure->ndims; ii++) {
				if (start_pointer[i] == &(start[ii]))
					*(start_pointer[i]) += measure->dims_start_index[ii];
			}
		}

		//Fill array
		if (type_flag == OPH_NC_INT_FLAG) {
			if ((res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_BYTE_FLAG) {
			if ((res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_SHORT_FLAG) {
			if ((res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_LONG_FLAG) {
			if ((res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_FLOAT_FLAG) {
			if ((res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
			if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		} else {
			if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) binary))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}
		}

		if (!imp_dim_ordered) {
			//Implicit dimensions are not orderer, hence we must rearrange binary.
			memset(counters, 0, measure->nimp);
			oph_nc_cache_to_buffer(measure->nimp, counters, limits, products, binary, binary_tmp, sizeof_type);
			//Move from tmp to input buffer
			memcpy(binary, binary_tmp, sizeof_var);
		}

		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
		}
		idDim++;
	}
	free(binary);
	for (ii = 0; ii < c_arg - 1; ii++)
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

	return OPH_NC_SUCCESS;
}

int oph_nc_populate_fragment_from_nc2(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure)
{
	if (!frag || !ncid || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	if (oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_NC_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case NC_BYTE:
		case NC_CHAR:
			type_flag = OPH_NC_BYTE_FLAG;
			break;
		case NC_SHORT:
			type_flag = OPH_NC_SHORT_FLAG;
			break;
		case NC_INT:
			type_flag = OPH_NC_INT_FLAG;
			break;
		case NC_INT64:
			type_flag = OPH_NC_LONG_FLAG;
			break;
		case NC_FLOAT:
			type_flag = OPH_NC_FLOAT_FLAG;
			break;
		case NC_DOUBLE:
			type_flag = OPH_NC_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_NC_DOUBLE_FLAG;
	}

	long long sizeof_var = 0;

	if (type_flag == OPH_NC_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_NC_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_NC_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		sizeof_var = (array_length) * sizeof(double);
	else
		sizeof_var = (array_length) * sizeof(double);

	//Compute number of tuples per insert (regular case)
	long long regular_rows = 0;
	long long regular_times = 0;
	long long remainder_rows = 0;

	long block_size = 512 * 1024;	//Maximum size that could be transfered
	long block_rows = 1000;	//Maximum number of lines that could be transfered

	if (sizeof_var >= block_size) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
		remainder_rows = 0;
	} else if (tuplexfrag_number * sizeof_var <= block_size) {
		//Do single insert
		if (tuplexfrag_number <= block_rows) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
			remainder_rows = 0;
		} else {
			regular_rows = block_rows;
			regular_times = (int) tuplexfrag_number / regular_rows;
			remainder_rows = (int) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((int) block_size / sizeof_var >= block_rows ? block_rows : (int) block_size / sizeof_var);
		regular_times = (int) tuplexfrag_number / regular_rows;
		remainder_rows = (int) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	long long query_size = 0;
	char *insert_query = (remainder_rows > 0 ? OPH_DC_SQ_MULTI_INSERT_FRAG : OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL);
	if (compressed == 1) {
		query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
	} else {
		query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
	}

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_NC_ERROR;
	}

	int j = 0;
	int n;

	n = snprintf(query_string, query_size, insert_query, frag->fragment_name) - 1;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
		}
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
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
		return OPH_NC_ERROR;
	}

	int l;

	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_NC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length * regular_rows);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length * regular_rows);
	else if (type_flag == OPH_NC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length * regular_rows);
	else if (type_flag == OPH_NC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length * regular_rows);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length * regular_rows);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		return OPH_NC_ERROR;
	}

	int c_arg = 1 + regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		return OPH_NC_ERROR;
	}

	for (ii = 0; ii < c_arg - 1; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_NC_ERROR;
		}
	}
	args[c_arg - 1] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[2 * ii]->arg_length = sizeof(unsigned long long);
		args[2 * ii]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[2 * ii]->arg_is_null = 0;
		args[2 * ii]->arg = (unsigned long long *) (&(idDim[ii]));

		args[2 * ii + 1]->arg_length = sizeof_var;
		args[2 * ii + 1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[2 * ii + 1]->arg_is_null = 0;
		args[2 * ii + 1]->arg = (char *) (binary + sizeof_var * ii);
		idDim[ii] = frag->key_start + ii;
	}

	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, query_string, regular_times, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_NC_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	size_t *start = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	size_t *count = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc((measure->nexp) * sizeof(size_t *));
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
		if (!measure->dims_type[i])
			total *= count[i];

	if (total != array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_NC_ERROR;
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
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	}


	int jj = 0;

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
		if (type_flag == OPH_NC_BYTE_FLAG)
			res = oph_iob_bin_array_create_b(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_SHORT_FLAG)
			res = oph_iob_bin_array_create_s(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_INT_FLAG)
			res = oph_iob_bin_array_create_i(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_LONG_FLAG)
			res = oph_iob_bin_array_create_l(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_FLOAT_FLAG)
			res = oph_iob_bin_array_create_f(&binary_tmp, array_length);
		else if (type_flag == OPH_NC_DOUBLE_FLAG)
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		else
			res = oph_iob_bin_array_create_d(&binary_tmp, array_length);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
			free(binary);
			free(query_string);
			free(idDim);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
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
					for (ii = 0; ii < c_arg - 1; ii++)
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
					return OPH_NC_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;

	for (l = 0; l < regular_times; l++) {

		//Build binary rows
		for (jj = 0; jj < regular_rows; jj++) {
			oph_nc_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (ii = 0; ii < measure->ndims; ii++) {
					if (start_pointer[i] == &(start[ii]))
						*(start_pointer[i]) += measure->dims_start_index[ii];
				}
			}

			//Fill array
			res = -1;
			if (type_flag == OPH_NC_INT_FLAG) {
				res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_BYTE_FLAG) {
				res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_SHORT_FLAG) {
				res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_LONG_FLAG) {
				res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_FLOAT_FLAG) {
				res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
				res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + jj * sizeof_var));
			} else {
				res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + jj * sizeof_var));
			}
			if (res != 0) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(query_string);
				free(idDim);
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}

			if (!imp_dim_ordered) {

				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_nc_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}


		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
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
		if (compressed == 1) {
			query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
		} else {
			query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
		}

		query_string = (char *) malloc(query_size * sizeof(char));
		if (!(query_string)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
		}

		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1;
		if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
			}
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
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

		if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
		}
		//Build binary rows
		for (jj = 0; jj < remainder_rows; jj++) {
			oph_nc_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (ii = 0; ii < measure->ndims; ii++) {
					if (start_pointer[i] == &(start[ii]))
						*(start_pointer[i]) += measure->dims_start_index[ii];
				}
			}
			//Fill array
			res = -1;
			if (type_flag == OPH_NC_INT_FLAG) {
				res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_BYTE_FLAG) {
				res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_SHORT_FLAG) {
				res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_LONG_FLAG) {
				res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_FLOAT_FLAG) {
				res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) (binary + jj * sizeof_var));
			} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
				res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + jj * sizeof_var));
			} else {
				res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + jj * sizeof_var));
			}
			if (res != 0) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(query_string);
				free(idDim);
				free(binary);
				if (binary_tmp)
					free(binary_tmp);
				for (ii = 0; ii < c_arg - 1; ii++)
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
				return OPH_NC_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_nc_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
				//Move from tmp to input buffer
				memcpy(binary + jj * sizeof_var, binary_tmp, sizeof_var);
			}
		}


		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary);
			if (binary_tmp)
				free(binary_tmp);
			for (ii = 0; ii < c_arg - 1; ii++)
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
			return OPH_NC_ERROR;
		}

		oph_ioserver_free_query(server, query);
	}

	free(query_string);
	free(idDim);
	free(binary);
	if (binary_tmp)
		free(binary_tmp);
	for (ii = 0; ii < c_arg - 1; ii++)
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
	return OPH_NC_SUCCESS;
}

int oph_nc_populate_fragment_from_nc3(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure,
				      long long memory_size)
{
	if (!frag || !ncid || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	if (oph_dc2_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_NC_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case NC_BYTE:
		case NC_CHAR:
			type_flag = OPH_NC_BYTE_FLAG;
			break;
		case NC_SHORT:
			type_flag = OPH_NC_SHORT_FLAG;
			break;
		case NC_INT:
			type_flag = OPH_NC_INT_FLAG;
			break;
		case NC_INT64:
			type_flag = OPH_NC_LONG_FLAG;
			break;
		case NC_FLOAT:
			type_flag = OPH_NC_FLOAT_FLAG;
			break;
		case NC_DOUBLE:
			type_flag = OPH_NC_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_NC_DOUBLE_FLAG;
	}

	long long sizeof_var = 0;

	if (type_flag == OPH_NC_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_NC_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_NC_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		sizeof_var = (array_length) * sizeof(double);
	else
		sizeof_var = (array_length) * sizeof(double);

	long long memory_size_mb = memory_size * 1024 * 1024;
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
	if (!whole_fragment || dimension_ordered || !whole_explicit) {
		return oph_nc_populate_fragment_from_nc2(server, frag, ncid, tuplexfrag_number, array_length, compressed, measure);
	}
	//Compute number of tuples per insert (regular case)
	long long regular_rows = 0;
	long long regular_times = 0;
	long long remainder_rows = 0;

	long long block_size = 512 * 1024;	//Maximum size that could be transfered
	long long block_rows = 1000;	//Maximum number of lines that could be transfered

	if (sizeof_var >= block_size) {
		//Use the same algorithm
		regular_rows = 1;
		regular_times = tuplexfrag_number;
		remainder_rows = 0;
	} else if (tuplexfrag_number * sizeof_var <= block_size) {
		//Do single insert
		if (tuplexfrag_number <= block_rows) {
			regular_rows = tuplexfrag_number;
			regular_times = 1;
			remainder_rows = 0;
		} else {
			regular_rows = block_rows;
			regular_times = (int) tuplexfrag_number / regular_rows;
			remainder_rows = (int) tuplexfrag_number % regular_rows;
		}
	} else {
		//Compute num rows x insert and remainder
		regular_rows = ((int) block_size / sizeof_var >= block_rows ? block_rows : (int) block_size / sizeof_var);
		regular_times = (int) tuplexfrag_number / regular_rows;
		remainder_rows = (int) tuplexfrag_number % regular_rows;
	}

	//Alloc query String
	long long query_size = 0;
	char *insert_query = (remainder_rows > 0 ? OPH_DC_SQ_MULTI_INSERT_FRAG : OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL);
	if (compressed == 1) {
		query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
	} else {
		query_size = snprintf(NULL, 0, insert_query, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
	}

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_NC_ERROR;
	}

	int j = 0;
	int n;

	n = snprintf(query_string, query_size, insert_query, frag->fragment_name) - 1;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
			strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
			n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
		}
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
		for (j = 0; j < regular_rows; j++) {
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
		return OPH_NC_ERROR;
	}

	int l;

	//Create binary array
	char *binary_cache = 0;
	int res;

	//Create a binary array to store the whole fragment
	if (type_flag == OPH_NC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_NC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_NC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(query_string);
		free(idDim);
		return OPH_NC_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (type_flag == OPH_NC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_NC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_NC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(idDim);
		return OPH_NC_ERROR;
	}

	int c_arg = 1 + regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary_cache);
		free(binary_insert);
		free(idDim);
		return OPH_NC_ERROR;
	}

	for (ii = 0; ii < c_arg - 1; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_NC_ERROR;
		}
	}
	args[c_arg - 1] = NULL;

	for (ii = 0; ii < regular_rows; ii++) {
		args[2 * ii]->arg_length = sizeof(unsigned long long);
		args[2 * ii]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
		args[2 * ii]->arg_is_null = 0;
		args[2 * ii]->arg = (unsigned long long *) (&(idDim[ii]));

		args[2 * ii + 1]->arg_length = sizeof_var;
		args[2 * ii + 1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
		args[2 * ii + 1]->arg_is_null = 0;
		args[2 * ii + 1]->arg = (char *) (binary_insert + sizeof_var * ii);
		idDim[ii] = frag->key_start + ii;
	}

	if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, query_string, regular_times, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_NC_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	size_t *start = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	size_t *count = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc((measure->nexp) * sizeof(size_t *));

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
		total *= count[i];

	if (total != array_length * tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(idDim);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_NC_ERROR;
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
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	}

	int jj = 0;

	oph_nc_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

	for (i = 0; i < measure->nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (ii = 0; ii < measure->ndims; ii++) {
			if (start_pointer[i] == &(start[ii])) {
				*(start_pointer[i]) += measure->dims_start_index[ii];
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

	if (type_flag == OPH_NC_INT_FLAG) {
		res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) (binary_cache));
	} else if (type_flag == OPH_NC_BYTE_FLAG) {
		res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) (binary_cache));
	} else if (type_flag == OPH_NC_SHORT_FLAG) {
		res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) (binary_cache));
	} else if (type_flag == OPH_NC_LONG_FLAG) {
		res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) (binary_cache));
	} else if (type_flag == OPH_NC_FLOAT_FLAG) {
		res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) (binary_cache));
	} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
		res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary_cache));
	} else {
		res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary_cache));
	}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", frag->fragment_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (res != 0) {
		OPH_NC_ERR(res);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		free(query_string);
		free(idDim);
		free(binary_cache);
		free(binary_insert);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		oph_ioserver_free_query(server, query);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_NC_ERROR;
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
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(count);
				free(counters);
				free(file_indexes);
				free(products);
				free(limits);
				return OPH_NC_ERROR;
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
		oph_nc_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_write_time, NULL);
#endif
		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_NC_ERROR;
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
		if (compressed == 1) {
			query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
		} else {
			query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
		}

		query_string = (char *) malloc(query_size * sizeof(char));
		if (!(query_string)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_NC_ERROR;
		}

		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG_FINAL, frag->fragment_name) - 1;
		if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_COMPRESSED_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
				strncpy(query_string + n, OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW, strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW));
				n += strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW);
			}
		} else {
#ifdef OPH_DEBUG_MYSQL
			printf("ORIGINAL QUERY: " MYSQL_DC_MULTI_INSERT_FRAG "\n", frag->fragment_name);
#endif
			for (j = 0; j < remainder_rows; j++) {
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

		if (oph_ioserver_setup_query(server, frag->db_instance->dbms_instance->conn, query_string, 1, args, &query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			free(counters);
			free(products);
			free(limits);
			return OPH_NC_ERROR;
		}
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = regular_times * regular_rows + remainder_rows;
		counters[0] = regular_times * regular_rows;
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_nc_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif

#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_write_time, NULL);
#endif
		if (oph_ioserver_execute_query(server, frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(query_string);
			free(idDim);
			free(binary_cache);
			free(binary_insert);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(counters);
			free(products);
			free(limits);
			return OPH_NC_ERROR;
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
	for (ii = 0; ii < c_arg - 1; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	free(counters);
	free(products);
	free(limits);

	return OPH_NC_SUCCESS;
}

int oph_nc_get_row_from_nc(int ncid, int array_length, NETCDF_var * measure, unsigned long idDim, char **row)
{
	if (!ncid || !array_length || !measure || !row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}
	*row = NULL;

	int res;
	char type_flag = '\0', *binary = NULL;
	switch (measure->vartype) {
		case NC_BYTE:
		case NC_CHAR:
			type_flag = OPH_NC_BYTE_FLAG;
			res = oph_iob_bin_array_create_b(&binary, array_length);
			break;
		case NC_SHORT:
			type_flag = OPH_NC_SHORT_FLAG;
			res = oph_iob_bin_array_create_s(&binary, array_length);
			break;
		case NC_INT:
			type_flag = OPH_NC_INT_FLAG;
			res = oph_iob_bin_array_create_i(&binary, array_length);
			break;
		case NC_INT64:
			type_flag = OPH_NC_LONG_FLAG;
			res = oph_iob_bin_array_create_l(&binary, array_length);
			break;
		case NC_FLOAT:
			type_flag = OPH_NC_FLOAT_FLAG;
			res = oph_iob_bin_array_create_f(&binary, array_length);
			break;
		case NC_DOUBLE:
			type_flag = OPH_NC_DOUBLE_FLAG;
			res = oph_iob_bin_array_create_d(&binary, array_length);
			break;
		default:
			type_flag = OPH_NC_DOUBLE_FLAG;
			res = oph_iob_bin_array_create_d(&binary, array_length);
	}
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		return OPH_NC_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	size_t *start = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	size_t *count = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc((measure->nexp) * sizeof(size_t *));
	//Set count
	short int i, ii;
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
		if (!measure->dims_type[i])
			total *= count[i];
	if (total != array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_NC_ERROR;
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
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	}

	oph_nc_compute_dimension_id(idDim + 1, sizemax, measure->nexp, start_pointer);

	for (i = 0; i < measure->nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (ii = 0; ii < measure->ndims; ii++) {
			if (start_pointer[i] == &(start[ii]))
				*(start_pointer[i]) += measure->dims_start_index[ii];
		}
	}

	//Fill array
	if (type_flag == OPH_NC_INT_FLAG) {
		if ((res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else if (type_flag == OPH_NC_LONG_FLAG) {
		if ((res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else if (type_flag == OPH_NC_BYTE_FLAG) {
		if ((res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else if (type_flag == OPH_NC_SHORT_FLAG) {
		if ((res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else if (type_flag == OPH_NC_FLOAT_FLAG) {
		if ((res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
		if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	} else {
		if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) binary))) {
			OPH_NC_ERR(res);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_NC_ERROR;
		}
	}

	free(start);
	free(count);
	free(start_pointer);
	free(sizemax);

	*row = binary;

	return OPH_NC_SUCCESS;
}

int _oph_nc_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, size_t ** id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (size_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_nc_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else {
		*(id[i]) = (size_t) (residual + 1);
	}
	return 0;
}

int oph_nc_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, size_t ** id)
{
	int i;
	unsigned long total = 1;
	for (i = 0; i < n; ++i)
		total *= sizemax[i];
	_oph_nc_get_dimension_id(ID - 1, total, sizemax, id, 0, n);
	return 0;
}

int oph_nc_get_c_type(nc_type type_nc, char *out_c_type)
{

	switch (type_nc) {
		case NC_BYTE:
		case NC_CHAR:
			strncpy(out_c_type, OPH_NC_BYTE_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case NC_SHORT:
			strncpy(out_c_type, OPH_NC_SHORT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case NC_INT:
			strncpy(out_c_type, OPH_NC_INT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case NC_INT64:
			strncpy(out_c_type, OPH_NC_LONG_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case NC_FLOAT:
			strncpy(out_c_type, OPH_NC_FLOAT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case NC_DOUBLE:
			strncpy(out_c_type, OPH_NC_DOUBLE_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			return -1;
	}
	return 0;

}

int oph_nc_get_nc_type(char *in_c_type, nc_type * type_nc)
{
	if (!strcasecmp(in_c_type, OPH_NC_BYTE_TYPE)) {
		*type_nc = NC_BYTE;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_SHORT_TYPE)) {
		*type_nc = NC_SHORT;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_INT_TYPE)) {
		*type_nc = NC_INT;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_LONG_TYPE)) {
		*type_nc = NC_INT64;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_FLOAT_TYPE)) {
		*type_nc = NC_FLOAT;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_DOUBLE_TYPE)) {
		*type_nc = NC_DOUBLE;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_NC_BIT_TYPE)) {
		*type_nc = NC_INT;
		return 0;
	}
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
	return -1;

}

int _oph_nc_get_next_nc_id(size_t * id, unsigned int *sizemax, int i, int n)
{
	if (i < 0)
		return 1;	// Overflow
	(id[i])++;
	if (id[i] >= sizemax[i]) {
		id[i] = 0;
		return _oph_nc_get_next_nc_id(id, sizemax, i - 1, n);
	}
	return 0;
}

int oph_nc_get_next_nc_id(size_t * id, unsigned int *sizemax, int n)
{
	return _oph_nc_get_next_nc_id(id, sizemax, n - 1, n);
}

int oph_nc_append_fragment_from_nc(oph_ioserver_handler * server, oph_odb_fragment * old_frag, oph_odb_fragment * new_frag, int ncid, int compressed, NETCDF_var * measure)
{
	if (!old_frag || !new_frag || !ncid || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	if (oph_dc2_check_connection_to_db(server, old_frag->db_instance->dbms_instance, old_frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_NC_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case NC_BYTE:
		case NC_CHAR:
			type_flag = OPH_NC_BYTE_FLAG;
			break;
		case NC_SHORT:
			type_flag = OPH_NC_SHORT_FLAG;
			break;
		case NC_INT:
			type_flag = OPH_NC_INT_FLAG;
			break;
		case NC_INT64:
			type_flag = OPH_NC_LONG_FLAG;
			break;
		case NC_FLOAT:
			type_flag = OPH_NC_FLOAT_FLAG;
			break;
		case NC_DOUBLE:
			type_flag = OPH_NC_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_NC_DOUBLE_FLAG;
	}

	char insert_query[QUERY_BUFLEN];
	int n;
	if (compressed == 1) {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_INSERT_COMPRESSED_FRAG "\n", new_frag->fragment_name);
#endif
		n = snprintf(insert_query, QUERY_BUFLEN, OPH_DC_SQ_INSERT_COMPRESSED_FRAG, new_frag->fragment_name);
	} else {
#ifdef OPH_DEBUG_MYSQL
		printf("ORIGINAL QUERY: " MYSQL_DC_INSERT_FRAG "\n", new_frag->fragment_name);
#endif
		n = snprintf(insert_query, QUERY_BUFLEN, OPH_DC_SQ_INSERT_FRAG, new_frag->fragment_name);
	}

	if (n >= QUERY_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_NC_ERROR;
	}
	char c_type[OPH_ODB_CUBE_MEASURE_TYPE_SIZE];
	oph_nc_get_c_type(measure->vartype, c_type);
	oph_ioserver_result *frag_rows;
	short int i;
	if (oph_dc2_read_fragment_data(server, old_frag, c_type, compressed, NULL, NULL, NULL, 0, 1, &frag_rows)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment.\n");
		return OPH_NC_ERROR;
	}

	int num_fields = frag_rows->num_fields;
	int tuplexfrag_number = frag_rows->num_rows;
	unsigned long field_length = 0;
	field_length = frag_rows->max_field_length[num_fields - 1];
	long old_array_length;
	if (type_flag == OPH_NC_BYTE_FLAG)
		old_array_length = (long) field_length / sizeof(char);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		old_array_length = (long) field_length / sizeof(short);
	else if (type_flag == OPH_NC_INT_FLAG)
		old_array_length = (long) field_length / sizeof(int);
	else if (type_flag == OPH_NC_LONG_FLAG)
		old_array_length = (long) field_length / sizeof(long long);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		old_array_length = (long) field_length / sizeof(float);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		old_array_length = (long) field_length / sizeof(double);
	else
		old_array_length = (long) field_length / sizeof(double);

	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the nc file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	size_t *start = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	size_t *count = (size_t *) malloc((measure->ndims) * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc((measure->nexp) * sizeof(size_t *));
	//Set count
	for (i = 0; i < measure->ndims; i++) {
		//Explicit
		if (measure->dims_type[i]) {
			count[i] = 1;
		} else {
			//Implicit
			count[i] = measure->dims_length[i];
			start[i] = 0;
		}
	}
	//Check
	long new_array_length = 1;
	for (i = 0; i < measure->ndims; i++)
		if (!measure->dims_type[i])
			new_array_length *= count[i];

	long array_length = old_array_length + new_array_length;
	unsigned long sizeof_var = 0;

	unsigned long long idDim = 0;

	int l;

	if (type_flag == OPH_NC_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_NC_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_NC_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		sizeof_var = (array_length) * sizeof(double);
	else
		sizeof_var = (array_length) * sizeof(double);

	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_NC_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length);
	else if (type_flag == OPH_NC_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length);
	else if (type_flag == OPH_NC_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length);
	else if (type_flag == OPH_NC_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length);
	else if (type_flag == OPH_NC_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length);
	else if (type_flag == OPH_NC_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length);

	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		oph_ioserver_free_result(server, frag_rows);
		return OPH_NC_ERROR;
	}

	oph_ioserver_query *query = NULL;

	int c_arg = 3, ii;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(binary);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		oph_ioserver_free_result(server, frag_rows);
		return OPH_NC_ERROR;
	}

	for (ii = 0; ii < c_arg - 1; ii++) {
		args[ii] = (oph_ioserver_query_arg *) calloc(1, sizeof(oph_ioserver_query_arg));
		if (!args[ii]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input arguments\n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			oph_ioserver_free_result(server, frag_rows);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			return OPH_NC_ERROR;
		}

	}
	args[c_arg - 1] = NULL;

	args[0]->arg_length = sizeof(unsigned long long);
	args[0]->arg_type = OPH_IOSERVER_TYPE_LONGLONG;
	args[0]->arg_is_null = 0;
	args[0]->arg = (unsigned long long *) (&idDim);

	args[1]->arg_length = sizeof_var;
	args[1]->arg_type = OPH_IOSERVER_TYPE_BLOB;
	args[1]->arg_is_null = 0;
	args[1]->arg = (char *) (binary);
	idDim = new_frag->key_start;

	if (oph_ioserver_setup_query(server, new_frag->db_instance->dbms_instance->conn, insert_query, tuplexfrag_number, args, &query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot setup query\n");
		free(binary);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		oph_ioserver_free_result(server, frag_rows);
		for (ii = 0; ii < c_arg - 1; ii++)
			if (args[ii])
				free(args[ii]);
		free(args);
		return OPH_NC_ERROR;
	}


	short int flag = 0;
	short int curr_lev;
	for (curr_lev = 1; curr_lev <= measure->nexp; curr_lev++) {
		flag = 0;
		//Find dimension with oph_level = curr_lev
		for (i = 0; i < measure->ndims; i++) {
			if (measure->dims_type[i] && measure->dims_oph_level[i] == curr_lev) {
				sizemax[curr_lev - 1] = measure->dims_length[i];
				start_pointer[curr_lev - 1] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			free(binary);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			oph_ioserver_free_result(server, frag_rows);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			return OPH_NC_ERROR;
		}
	}

	oph_ioserver_row *old_row = NULL;

	for (l = 0; l < tuplexfrag_number; l++) {

		//Read data             
		if (oph_ioserver_fetch_row(server, frag_rows, &old_row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
			oph_ioserver_free_result(server, frag_rows);
			return OPH_DC2_SERVER_ERROR;
		}

		if (!old_row->row) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment row\n");
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			oph_ioserver_free_result(server, frag_rows);
			return OPH_NC_ERROR;
		}

		oph_nc_compute_dimension_id(idDim, sizemax, measure->nexp, start_pointer);

		for (i = 0; i < measure->nexp; i++)
			*(start_pointer[i]) -= 1;

		//Fill array
		if (type_flag == OPH_NC_BYTE_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(char));
			if ((res = nc_get_vara_uchar(ncid, measure->varid, start, count, (unsigned char *) (binary + old_array_length * sizeof(char))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_SHORT_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(short));
			if ((res = nc_get_vara_short(ncid, measure->varid, start, count, (short *) (binary + old_array_length * sizeof(short))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_INT_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(int));
			if ((res = nc_get_vara_int(ncid, measure->varid, start, count, (int *) (binary + old_array_length * sizeof(int))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_LONG_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(long long));
			if ((res = nc_get_vara_longlong(ncid, measure->varid, start, count, (long long *) (binary + old_array_length * sizeof(long long))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_FLOAT_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(float));
			if ((res = nc_get_vara_float(ncid, measure->varid, start, count, (float *) (binary + old_array_length * sizeof(float))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else if (type_flag == OPH_NC_DOUBLE_FLAG) {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(double));
			if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + old_array_length * sizeof(double))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		} else {
			memcpy(binary, old_row->row[1], old_array_length * sizeof(double));
			if ((res = nc_get_vara_double(ncid, measure->varid, start, count, (double *) (binary + old_array_length * sizeof(double))))) {
				OPH_NC_ERR(res);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				free(binary);
				for (ii = 0; ii < c_arg - 1; ii++)
					if (args[ii])
						free(args[ii]);
				free(args);
				oph_ioserver_free_query(server, query);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				oph_ioserver_free_result(server, frag_rows);
				return OPH_NC_ERROR;
			}
		}

		if (oph_ioserver_execute_query(server, new_frag->db_instance->dbms_instance->conn, query)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot execute query\n");
			free(binary);
			for (ii = 0; ii < c_arg - 1; ii++)
				if (args[ii])
					free(args[ii]);
			free(args);
			oph_ioserver_free_query(server, query);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			oph_ioserver_free_result(server, frag_rows);
			return OPH_NC_ERROR;
		}
		idDim++;
	}
	free(binary);
	for (ii = 0; ii < c_arg - 1; ii++)
		if (args[ii])
			free(args[ii]);
	free(args);
	oph_ioserver_free_query(server, query);
	free(start);
	free(count);
	free(start_pointer);
	free(sizemax);
	oph_ioserver_free_result(server, frag_rows);

	return OPH_NC_SUCCESS;
}

int oph_nc_get_dim_array2(int id_container, int ncid, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, int start_index, int end_index, char **dim_array)
{
	if (!ncid || !dim_type || !dim_size || !dim_array) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}
	//Assume that the coordinate variable related to a dimension depends by one dimension only (Ex. lat(lat) not lat(x,y))  
	void *binary_dim = NULL;
	int retval = 0;
	size_t start[1];
	size_t count[1];

	start[0] = (size_t) start_index;
	count[0] = 1;

	if (start_index != end_index)
		count[0] = (size_t) (end_index - start_index) + 1;

	if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(char) * dim_size);

		if ((retval = nc_get_vara_uchar(ncid, dim_id, start, count, (unsigned char *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(char) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (char *) binary_dim, sizeof(char) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(short) * dim_size);

		if ((retval = nc_get_vara_short(ncid, dim_id, start, count, (short *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(short) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (short *) binary_dim, sizeof(short) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(int) * dim_size);

		if ((retval = nc_get_vara_int(ncid, dim_id, start, count, (int *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(int) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (int *) binary_dim, sizeof(int) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(float) * dim_size);

		if ((retval = nc_get_vara_float(ncid, dim_id, start, count, (float *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(float) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (float *) binary_dim, sizeof(float) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(double) * dim_size);
		if ((retval = nc_get_vara_double(ncid, dim_id, start, count, (double *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(double) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (double *) binary_dim, sizeof(double) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(long long) * dim_size);

		if ((retval = nc_get_vara_longlong(ncid, dim_id, start, count, (long long *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(long long) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (long long *) binary_dim, sizeof(long long) * dim_size);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		free(binary_dim);
		return OPH_NC_ERROR;
	}
	free(binary_dim);
	return OPH_NC_SUCCESS;
}

int oph_nc_get_dim_array(int id_container, int ncid, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, char **dim_array)
{
	if (!ncid || !dim_type || !dim_size || !dim_array) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	void *binary_dim = NULL;
	int retval = 0;
	if (!strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(char) * dim_size);

		if ((retval = nc_get_var_uchar(ncid, dim_id, (unsigned char *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(char) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (char *) binary_dim, sizeof(char) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(short) * dim_size);

		if ((retval = nc_get_var_short(ncid, dim_id, (short *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(short) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (short *) binary_dim, sizeof(short) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(int) * dim_size);

		if ((retval = nc_get_var_int(ncid, dim_id, (int *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(int) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (int *) binary_dim, sizeof(int) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(float) * dim_size);

		if ((retval = nc_get_var_float(ncid, dim_id, (float *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(float) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (float *) binary_dim, sizeof(float) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(double) * dim_size);

		if ((retval = nc_get_var_double(ncid, dim_id, (double *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(double) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (double *) binary_dim, sizeof(double) * dim_size);
	} else if (!strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
		binary_dim = (void *) malloc(sizeof(long long) * dim_size);

		if ((retval = nc_get_var_longlong(ncid, dim_id, (long long *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		*dim_array = (char *) malloc(sizeof(long long) * dim_size);
		if (!(*dim_array)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
			free(binary_dim);
			return OPH_NC_ERROR;
		}
		memcpy((char *) *dim_array, (long long *) binary_dim, sizeof(long long) * dim_size);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		free(binary_dim);
		return OPH_NC_ERROR;
	}
	free(binary_dim);
	return OPH_NC_SUCCESS;
}

/*
Original meaning of want_start was different: 0 for the end index, <>0 for the start index.
Now it means: 0 for the end index, <0 for the start index, >0 for the nearest index.
*/
int oph_nc_index_by_value(int id_container, int ncid, int dim_id, nc_type dim_type, int dim_size, char *value, int want_start, double offset, int *valorder, int *coord_index)
{
	if (!ncid || !dim_size || !value || !coord_index || !valorder) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	void *binary_dim = NULL;
	int retval = 0, nearest_point = want_start > 0;
	//I need to evaluate the order of the dimension values: ascending or descending
	int order = 1;		//Default is ascending
	int exact_value = 0;	//If I find the exact value
	int i;

	if (dim_type == NC_INT) {
		binary_dim = (void *) malloc(sizeof(int) * dim_size);

		if ((retval = nc_get_var_int(ncid, dim_id, (int *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else if (dim_type == NC_BYTE || dim_type == NC_CHAR) {
		binary_dim = (void *) malloc(sizeof(char) * dim_size);

		if ((retval = nc_get_var_uchar(ncid, dim_id, (unsigned char *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else if (dim_type == NC_SHORT) {
		binary_dim = (void *) malloc(sizeof(short) * dim_size);

		if ((retval = nc_get_var_short(ncid, dim_id, (short *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else if (dim_type == NC_FLOAT) {
		binary_dim = (void *) malloc(sizeof(float) * dim_size);

		if ((retval = nc_get_var_float(ncid, dim_id, (float *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else if (dim_type == NC_DOUBLE) {
		binary_dim = (void *) malloc(sizeof(double) * dim_size);

		if ((retval = nc_get_var_double(ncid, dim_id, (double *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else if (dim_type == NC_INT64) {
		binary_dim = (void *) malloc(sizeof(long long) * dim_size);

		if ((retval = nc_get_var_longlong(ncid, dim_id, (long long *) binary_dim))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
			free(binary_dim);
			return OPH_NC_ERROR;
		}
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
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
		free(binary_dim);
		return OPH_NC_ERROR;
	}
	free(binary_dim);
	*coord_index = i;
	*valorder = order;
	return OPH_NC_SUCCESS;
}

int oph_nc_compare_nc_c_types(int id_container, nc_type var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE])
{
	if (!var_type || !dim_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}

	switch (var_type) {
		case NC_BYTE:
		case NC_CHAR:
			if (strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		case NC_SHORT:
			if (strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		case NC_INT:
			if (strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		case NC_INT64:
			if (strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		case NC_FLOAT:
			if (strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		case NC_DOUBLE:
			if (strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_NC_ERROR;
			}
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_VAR_TYPE_NOT_SUPPORTED, dim_type);
			return OPH_NC_ERROR;
	}
	return OPH_NC_SUCCESS;
}

int oph_nc_get_nc_var(int id_container, const char var_name[OPH_ODB_CUBE_MEASURE_SIZE], int ncid, int max_ndims, NETCDF_var * var)
{
	if (!ncid || !var_name || !max_ndims || !var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_NC_ERROR;
	}
	int retval = 0;

	//Get id from dimension name
	if ((retval = nc_inq_varid(ncid, var_name, &(var->varid)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
		return OPH_NC_ERROR;
	}
	//Get information from id
	if ((retval = nc_inq_vartype(ncid, var->varid, &(var->vartype)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
		return OPH_NC_ERROR;
	}
	if ((retval = nc_inq_varndims(ncid, var->varid, &(var->ndims)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
		return OPH_NC_ERROR;
	}

	if (var->ndims != max_ndims) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_NOT_ALLOWED);
		return OPH_NC_ERROR;
	}

	var->dims_id = malloc(var->ndims * sizeof(int));
	if (!(var->dims_id)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions nc ids");
		return OPH_NC_ERROR;
	}
	if ((retval = nc_inq_vardimid(ncid, var->varid, var->dims_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
		return OPH_NC_ERROR;
	}

	var->dims_length = malloc(var->ndims * sizeof(size_t));
	if (!(var->dims_length)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions nc lengths");
		return OPH_NC_ERROR;
	}

	if ((retval = nc_inq_dimlen(ncid, var->dims_id[0], &(var->dims_length[0])))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, nc_strerror(retval));
		return OPH_NC_ERROR;
	}
	var->varsize = 1 * var->dims_length[0];
	return OPH_NC_SUCCESS;
}
