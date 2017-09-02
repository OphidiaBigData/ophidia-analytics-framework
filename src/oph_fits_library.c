/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#include "oph_fits_library.h"
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

int _oph_fits_cache_to_buffer(short int tot_dim_number, short int curr_dim, unsigned int *counters, unsigned int *limits, unsigned int *products, long long *index, char *binary_cache,
			      char *binary_insert, size_t sizeof_var)
{
	int i = 0;
	long long addr = 0;


	if (tot_dim_number > curr_dim) {

		if (curr_dim != 0)
			counters[curr_dim] = 0;
		while (counters[curr_dim] < limits[curr_dim]) {
			_oph_fits_cache_to_buffer(tot_dim_number, curr_dim + 1, counters, limits, products, index, binary_cache, binary_insert, sizeof_var);
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

int oph_fits_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var)
{
	long long index = 0;
	return _oph_fits_cache_to_buffer(tot_dim_number, 0, counters, limits, products, &index, binary_cache, binary_insert, sizeof_var);
}

int oph_fits_populate_fragment_from_fits2(oph_ioserver_handler * server, oph_odb_fragment * frag, fitsfile * fptr, int tuplexfrag_number, int array_length, int compressed, FITS_var * measure)
{
	if (!frag || !fptr || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_FITS_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_FITS_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case BYTE_IMG:
			//case NC_CHAR:
			type_flag = OPH_FITS_BYTE_FLAG;
			break;
		case SHORT_IMG:
			type_flag = OPH_FITS_SHORT_FLAG;
			break;
		case LONG_IMG:
			type_flag = OPH_FITS_INT_FLAG;
			break;
		case LONGLONG_IMG:
			type_flag = OPH_FITS_LONG_FLAG;
			break;
		case FLOAT_IMG:
			type_flag = OPH_FITS_FLOAT_FLAG;
			break;
		case DOUBLE_IMG:
			type_flag = OPH_FITS_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_FITS_DOUBLE_FLAG;
	}

	long long sizeof_var = 0;

	if (type_flag == OPH_FITS_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_FITS_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_FITS_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_FITS_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_FITS_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_FITS_DOUBLE_FLAG)
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
	if (compressed == 1) {
		query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
	} else {
		query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
	}
	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_FITS_ERROR;
	}

	int j = 0;
	int n;

	n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1;
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
		return OPH_FITS_ERROR;
	}

	int l;

	//Create binary array
	char *binary = 0;
	int res;

	if (type_flag == OPH_FITS_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length * regular_rows);
	else if (type_flag == OPH_FITS_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length * regular_rows);
	else if (type_flag == OPH_FITS_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length * regular_rows);
	else if (type_flag == OPH_FITS_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length * regular_rows);
	else if (type_flag == OPH_FITS_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length * regular_rows);
	else if (type_flag == OPH_FITS_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary);
		free(query_string);
		free(idDim);
		return OPH_FITS_ERROR;
	}

	int c_arg = 1 + regular_rows * 2, ii;
	oph_ioserver_query *query = NULL;
	oph_ioserver_query_arg **args = (oph_ioserver_query_arg **) calloc(c_arg, sizeof(oph_ioserver_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		free(query_string);
		free(binary);
		free(idDim);
		return OPH_FITS_ERROR;
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
			return OPH_FITS_ERROR;
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
		return OPH_FITS_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the fits file
	//sizemax must be sorted in base of the oph_level value
	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	long *start = (long *) malloc((measure->ndims) * sizeof(long));
	long *count = (long *) malloc((measure->ndims) * sizeof(long));
	long *inc = (long *) malloc((measure->ndims) * sizeof(long));	//For FITS files used to control the stride value in subset
	//Sort start in base of oph_level of explicit dimension
	long **start_pointer = (long **) malloc((measure->nexp) * sizeof(long *));
	//Set count
	short int i;
	for (i = 0; i < measure->ndims; i++) {
		//Explicit
		if (measure->dims_type[i]) {
			count[i] = (long) measure->dims_start_index[i] + 1;	//Not zero based
		} else {
			//Implicit
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				count[i] = (long) measure->dims_start_index[i] + 1;
			else
				count[i] = (long) measure->dims_end_index[i] + 1;
			start[i] = (long) measure->dims_start_index[i] + 1;
		}
		inc[i] = 1;	//Stride non supported
	}
	//Check
	long total = 1;
	for (i = 0; i < measure->ndims; i++)
		if (!measure->dims_type[i])
			total *= (count[i] - measure->dims_start_index[i]);

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
		free(inc);
		free(start_pointer);
		free(sizemax);
		return OPH_FITS_ERROR;
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			return OPH_FITS_ERROR;
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
		if (type_flag == OPH_FITS_BYTE_FLAG)
			res = oph_iob_bin_array_create_b(&binary_tmp, array_length);
		else if (type_flag == OPH_FITS_SHORT_FLAG)
			res = oph_iob_bin_array_create_s(&binary_tmp, array_length);
		else if (type_flag == OPH_FITS_INT_FLAG)
			res = oph_iob_bin_array_create_i(&binary_tmp, array_length);
		else if (type_flag == OPH_FITS_LONG_FLAG)
			res = oph_iob_bin_array_create_l(&binary_tmp, array_length);
		else if (type_flag == OPH_FITS_FLOAT_FLAG)
			res = oph_iob_bin_array_create_f(&binary_tmp, array_length);
		else if (type_flag == OPH_FITS_DOUBLE_FLAG)
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			return OPH_FITS_ERROR;
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
					free(inc);
					free(start_pointer);
					free(sizemax);
					free(counters);
					free(file_indexes);
					free(products);
					free(limits);
					return OPH_FITS_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	size_t sizeof_type = (int) sizeof_var / array_length;

	for (l = 0; l < regular_times; l++) {


		//Build binary rows
		for (jj = 0; jj < regular_rows; jj++) {
			oph_fits_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (ii = 0; ii < measure->ndims; ii++) {
					if (start_pointer[i] == &(start[ii])) {
						*(start_pointer[i]) += measure->dims_start_index[ii] + 1;
						count[ii] = start[ii];
					}
				}
			}

			int status = 0;

			//Fill array
			res = -1;
			if (type_flag == OPH_FITS_INT_FLAG) {
				res = fits_read_subset(fptr, TLONG, start, count, inc, NULL, (int *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_BYTE_FLAG) {
				res = fits_read_subset(fptr, TBYTE, start, count, inc, NULL, (unsigned char *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_SHORT_FLAG) {
				fits_read_subset(fptr, TSHORT, start, count, inc, NULL, (short *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_LONG_FLAG) {
				res = fits_read_subset(fptr, TLONGLONG, start, count, inc, NULL, (long long *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_FLOAT_FLAG) {
				res = fits_read_subset(fptr, TFLOAT, start, count, inc, NULL, (float *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_DOUBLE_FLAG) {
				res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary + jj * sizeof_var), NULL, &status);
			} else {
				res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary + jj * sizeof_var), NULL, &status);
			}
			char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code

			if (status != 0) {
				fits_get_errstatus(status, err_text);
				OPH_FITS_ERR(err_text);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling, %s\n", err_text);
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
				free(inc);
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
				return OPH_FITS_ERROR;
			}

			if (!imp_dim_ordered) {

				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_fits_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_FITS_ERROR;
		}
		//Increase idDim
		for (ii = 0; ii < regular_rows; ii++) {
			idDim[ii] += regular_rows;
		}
	}

	oph_ioserver_free_query(server, query);


	if (remainder_rows > 0) {
		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1;
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_FITS_ERROR;
		}
		//Build binary rows
		for (jj = 0; jj < remainder_rows; jj++) {
			oph_fits_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

			for (i = 0; i < measure->nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (ii = 0; ii < measure->ndims; ii++) {
					if (start_pointer[i] == &(start[ii])) {
						*(start_pointer[i]) += measure->dims_start_index[ii] + 1;
						count[ii] = start[ii];
					}
				}
			}
			//Fill array
			res = -1;
			int status = 0;
			char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code

			if (type_flag == OPH_FITS_INT_FLAG) {
				res = fits_read_subset(fptr, TLONG, start, count, inc, NULL, (int *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_BYTE_FLAG) {
				res = fits_read_subset(fptr, TBYTE, start, count, inc, NULL, (unsigned char **) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_SHORT_FLAG) {
				res = fits_read_subset(fptr, TSHORT, start, count, inc, NULL, (short *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_LONG_FLAG) {
				res = fits_read_subset(fptr, TLONGLONG, start, count, inc, NULL, (long long *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_FLOAT_FLAG) {
				res = fits_read_subset(fptr, TFLOAT, start, count, inc, NULL, (float *) (binary + jj * sizeof_var), NULL, &status);
			} else if (type_flag == OPH_FITS_DOUBLE_FLAG) {
				res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary + jj * sizeof_var), NULL, &status);
			} else {
				res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary + jj * sizeof_var), NULL, &status);
			}

			if (status != 0) {
				fits_get_errstatus(status, err_text);
				OPH_FITS_ERR(err_text);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling, %s\n", err_text);
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
				free(inc);
				free(start_pointer);
				free(sizemax);
				if (counters)
					free(counters);
				if (products)
					free(products);
				if (limits)
					free(limits);
				return OPH_FITS_ERROR;
			}

			if (!imp_dim_ordered) {
				//Implicit dimensions are not orderer, hence we must rearrange binary.
				memset(counters, 0, measure->nimp);
				oph_fits_cache_to_buffer(measure->nimp, counters, limits, products, binary + jj * sizeof_var, binary_tmp, sizeof_type);
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			if (counters)
				free(counters);
			if (products)
				free(products);
			if (limits)
				free(limits);
			return OPH_FITS_ERROR;
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
	free(inc);
	free(start_pointer);
	free(sizemax);
	if (counters)
		free(counters);
	if (products)
		free(products);
	if (limits)
		free(limits);
	return OPH_FITS_SUCCESS;
}

int oph_fits_populate_fragment_from_fits3(oph_ioserver_handler * server, oph_odb_fragment * frag, fitsfile * fptr, int tuplexfrag_number, int array_length, int compressed, FITS_var * measure,
					  long long memory_size)
{
	if (!frag || !fptr || !tuplexfrag_number || !array_length || !measure || !server) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_FITS_ERROR;
	}

	if (oph_dc_check_connection_to_db(server, frag->db_instance->dbms_instance, frag->db_instance, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_FITS_ERROR;
	}

	char type_flag = '\0';
	switch (measure->vartype) {
		case BYTE_IMG:
			//case NC_CHAR:
			type_flag = OPH_FITS_BYTE_FLAG;
			break;
		case SHORT_IMG:
			type_flag = OPH_FITS_SHORT_FLAG;
			break;
		case LONG_IMG:
			type_flag = OPH_FITS_INT_FLAG;
			break;
		case LONGLONG_IMG:
			type_flag = OPH_FITS_LONG_FLAG;
			break;
		case FLOAT_IMG:
			type_flag = OPH_FITS_FLOAT_FLAG;
			break;
		case DOUBLE_IMG:
			type_flag = OPH_FITS_DOUBLE_FLAG;
			break;
		default:
			type_flag = OPH_FITS_DOUBLE_FLAG;
	}

	long long sizeof_var = 0;

	if (type_flag == OPH_FITS_BYTE_FLAG)
		sizeof_var = (array_length) * sizeof(char);
	else if (type_flag == OPH_FITS_SHORT_FLAG)
		sizeof_var = (array_length) * sizeof(short);
	else if (type_flag == OPH_FITS_INT_FLAG)
		sizeof_var = (array_length) * sizeof(int);
	else if (type_flag == OPH_FITS_LONG_FLAG)
		sizeof_var = (array_length) * sizeof(long long);
	else if (type_flag == OPH_FITS_FLOAT_FLAG)
		sizeof_var = (array_length) * sizeof(float);
	else if (type_flag == OPH_FITS_DOUBLE_FLAG)
		sizeof_var = (array_length) * sizeof(double);
	else
		sizeof_var = (array_length) * sizeof(double);

	long long memory_size_mb = memory_size * 1024 * 1024;
	//Flag set to 1 if whole fragment fits in memory
	//Conservative choice. At most we insert the whole fragment, hence we need 2 equal buffers: 1 to read and 1 to write 
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > (long long) (memory_size_mb / 2) ? 0 : 1);

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
		return oph_fits_populate_fragment_from_fits2(server, frag, fptr, tuplexfrag_number, array_length, compressed, measure);
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
	if (compressed == 1) {
		query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_COMPRESSED_ROW) * regular_rows + 1;
	} else {
		query_size = snprintf(NULL, 0, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1 + strlen(OPH_DC_SQ_MULTI_INSERT_ROW) * regular_rows + 1;
	}

	char *query_string = (char *) malloc(query_size * sizeof(char));
	if (!(query_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_FITS_ERROR;
	}

	int j = 0;
	int n;

	n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1;
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
		return OPH_FITS_ERROR;
	}

	int l;

	//Create binary array
	char *binary_cache = 0;
	int res;

	//Create a binary array to store the whole fragment
	if (type_flag == OPH_FITS_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_FITS_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_FITS_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_FITS_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_FITS_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
	else if (type_flag == OPH_FITS_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(query_string);
		free(idDim);
		return OPH_FITS_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (type_flag == OPH_FITS_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_FITS_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_FITS_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_FITS_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_FITS_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * regular_rows);
	else if (type_flag == OPH_FITS_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * regular_rows);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		free(binary_cache);
		free(binary_insert);
		free(query_string);
		free(idDim);
		return OPH_FITS_ERROR;
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
		return OPH_FITS_ERROR;
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
			return OPH_FITS_ERROR;
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
		return OPH_FITS_ERROR;
	}
	//idDim controls the start array
	//start and count array must be sorted in base of the real order of dimensions in the fits file
	//sizemax must be sorted in base of the oph_level value
	// start corresponds to the start point - bottom left corner of image
	// count corresponds to the end point - upper right corner of image

	unsigned int *sizemax = (unsigned int *) malloc((measure->nexp) * sizeof(unsigned int));
	long *start = (long *) malloc((measure->ndims) * sizeof(long));
	long *count = (long *) malloc((measure->ndims) * sizeof(long));
	long *inc = (long *) malloc((measure->ndims) * sizeof(long));	//For FITS files used to control the stride value in subset
	//Sort start in base of oph_level of explicit dimension
	long **start_pointer = (long **) malloc((measure->nexp) * sizeof(long *));

	for (i = 0; i < measure->ndims; i++) {
		//External explicit
		if (measure->dims_type[i] && measure->dims_oph_level[i] != max_exp_lev) {
			count[i] = (long) measure->dims_start_index[i] + 1;	//Not zero based
		} else {
			//Implicit
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				count[i] = (long) measure->dims_start_index[i] + 1;
			else
				count[i] = (long) measure->dims_end_index[i] + 1;
			start[i] = (long) measure->dims_start_index[i] + 1;
			inc[i] = 1;	//Stride non supported
		}
	}
	//Check
	long total = 1;
	for (i = 0; i < measure->ndims; i++)
		total *= (count[i] - measure->dims_start_index[i]);
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
		free(inc);
		free(start_pointer);
		free(sizemax);
		return OPH_FITS_ERROR;
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
			free(inc);
			free(start_pointer);
			free(sizemax);
			return OPH_FITS_ERROR;
		}
	}

	int jj = 0;

	oph_fits_compute_dimension_id(idDim[jj], sizemax, measure->nexp, start_pointer);

	for (i = 0; i < measure->nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (ii = 0; ii < measure->ndims; ii++) {
			if (start_pointer[i] == &(start[ii])) {
				*(start_pointer[i]) += measure->dims_start_index[ii] + 1;
				count[ii] = start[ii];
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
	int status = 0;

	if (type_flag == OPH_FITS_INT_FLAG) {
		res = fits_read_subset(fptr, TLONG, start, count, inc, NULL, (int *) (binary_cache), NULL, &status);
	} else if (type_flag == OPH_FITS_BYTE_FLAG) {
		res = fits_read_subset(fptr, TBYTE, start, count, inc, NULL, (unsigned char *) (binary_cache), NULL, &status);
	} else if (type_flag == OPH_FITS_SHORT_FLAG) {
		res = fits_read_subset(fptr, TSHORT, start, count, inc, NULL, (short *) (binary_cache), NULL, &status);
	} else if (type_flag == OPH_FITS_LONG_FLAG) {
		res = fits_read_subset(fptr, TLONGLONG, start, count, inc, NULL, (long long *) (binary_cache), NULL, &status);
	} else if (type_flag == OPH_FITS_FLOAT_FLAG) {
		res = fits_read_subset(fptr, TFLOAT, start, count, inc, NULL, (float *) (binary_cache), NULL, &status);
	} else if (type_flag == OPH_FITS_DOUBLE_FLAG) {
		res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary_cache), NULL, &status);
	} else {
		res = fits_read_subset(fptr, TDOUBLE, start, count, inc, NULL, (double *) (binary_cache), NULL, &status);
	}
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", frag->fragment_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	char err_text[48];	//Descriptive text string (30 char max.) corresponding to a CFITSIO error status code

	if (status != 0) {
		OPH_FITS_ERR(err_text);
		fits_get_errstatus(status, err_text);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling,%s\n", err_text);
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
		free(inc);
		free(start_pointer);
		free(sizemax);
		return OPH_FITS_ERROR;
	}

	free(start);
	free(inc);
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
				return OPH_FITS_ERROR;
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
		oph_fits_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
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
			return OPH_FITS_ERROR;
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

	if (remainder_rows > 0) {
		query = NULL;
		n = snprintf(query_string, query_size, OPH_DC_SQ_MULTI_INSERT_FRAG, frag->fragment_name) - 1;
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
			return OPH_FITS_ERROR;
		}
		//Update counters and limit for explicit internal dimension
		memset(counters, 0, measure->nimp + 1);
		limits[0] = regular_times * regular_rows + remainder_rows;
		counters[0] = regular_times * regular_rows;
#if defined(OPH_TIME_DEBUG_1) || defined(OPH_TIME_DEBUG_2) || defined(BENCHMARK)
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_fits_cache_to_buffer(measure->nimp + 1, counters, limits, products, binary_cache, binary_insert, sizeof_type);
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
			return OPH_FITS_ERROR;
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

	return OPH_FITS_SUCCESS;
}

int _oph_fits_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, long **id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (size_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_fits_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else {
		*(id[i]) = (size_t) (residual + 1);
	}
	return 0;
}

int oph_fits_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, long **id)
{
	int i;
	unsigned long total = 1;
	for (i = 0; i < n; ++i)
		total *= sizemax[i];
	_oph_fits_get_dimension_id(ID - 1, total, sizemax, id, 0, n);
	return 0;
}

int oph_fits_get_c_type(int type_fits, char *out_c_type)
{

	switch (type_fits) {
		case BYTE_IMG:
			//case NC_CHAR:
			strncpy(out_c_type, OPH_FITS_BYTE_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case SHORT_IMG:
			strncpy(out_c_type, OPH_FITS_SHORT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case LONG_IMG:
			strncpy(out_c_type, OPH_FITS_INT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case LONGLONG_IMG:
			strncpy(out_c_type, OPH_FITS_LONG_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case FLOAT_IMG:
			strncpy(out_c_type, OPH_FITS_FLOAT_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		case DOUBLE_IMG:
			strncpy(out_c_type, OPH_FITS_DOUBLE_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
		default:
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Variable type not supported: double used\n");
			strncpy(out_c_type, OPH_FITS_DOUBLE_TYPE, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);
			break;
	}
	return 0;

}

int oph_fits_get_fits_type(char *in_c_type, int *type_fits)
{
	if (!strcasecmp(in_c_type, OPH_FITS_BYTE_TYPE)) {
		*type_fits = BYTE_IMG;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_FITS_SHORT_TYPE)) {
		*type_fits = SHORT_IMG;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_FITS_INT_TYPE)) {
		*type_fits = LONG_IMG;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_FITS_LONG_TYPE)) {
		*type_fits = LONGLONG_IMG;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_FITS_FLOAT_TYPE)) {
		*type_fits = FLOAT_IMG;
		return 0;
	}
	if (!strcasecmp(in_c_type, OPH_FITS_DOUBLE_TYPE)) {
		*type_fits = DOUBLE_IMG;
		return 0;
	}
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Variable type not supported: double used\n");
	*type_fits = DOUBLE_IMG;
	return 0;

}

int _oph_fits_get_next_fits_id(size_t * id, unsigned int *sizemax, int i, int n)
{
	if (i < 0)
		return 1;	// Overflow
	(id[i])++;
	if (id[i] >= sizemax[i]) {
		id[i] = 0;
		return _oph_fits_get_next_fits_id(id, sizemax, i - 1, n);
	}
	return 0;
}

int oph_fits_get_next_fits_id(size_t * id, unsigned int *sizemax, int n)
{
	return _oph_fits_get_next_fits_id(id, sizemax, n - 1, n);
}

int oph_fits_get_dim_array2(int id_container, int dim_size, char **dim_array)
{
	// FITS file has no dimension values; they are sequential long-type ordered numbers
	if (!dim_size || !dim_array) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_FITS_ERROR;
	}
	int i = 0;
	int j = 0;
	*dim_array = (char *) malloc(sizeof(int) * dim_size);
	if (!(*dim_array)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions binary array");
		return OPH_FITS_ERROR;
	}
	for (i = 0; i < dim_size; i++) {
		j++;
		memcpy((char *) ((*dim_array) + i * sizeof(int)), (char *) (&j), sizeof(char) * sizeof(int));	//Set dimension values as 1,..,n
	}
	return OPH_FITS_SUCCESS;
}

int oph_fits_compare_fits_c_types(int id_container, int var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE])
{
	if (!var_type || !dim_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_container, "Null input parameter\n");
		return OPH_FITS_ERROR;
	}

	switch (var_type) {
		case BYTE_IMG:
			//case NC_CHAR:
			if (strncasecmp(OPH_COMMON_BYTE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		case SHORT_IMG:
			if (strncasecmp(OPH_COMMON_SHORT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		case LONG_IMG:
			if (strncasecmp(OPH_COMMON_INT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		case LONGLONG_IMG:
			if (strncasecmp(OPH_COMMON_LONG_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		case FLOAT_IMG:
			if (strncasecmp(OPH_COMMON_FLOAT_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		case DOUBLE_IMG:
			if (strncasecmp(OPH_COMMON_DOUBLE_TYPE, dim_type, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				return OPH_FITS_ERROR;
			}
			break;
		default:
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Variable type not supported: double assumed\n");
			logging(LOG_WARNING, __FILE__, __LINE__, id_container, "Variable type not supported: double assumed\n");
			break;
	}
	return OPH_FITS_SUCCESS;
}

int oph_fits_get_fits_var(int id_container, char *varname, long *dims_length, FITS_var * var, short flag)
{
	if (!varname || !var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_FITS_ERROR;
	}
	// There are no dimension id and var id in fits file.
	// Assume the id as a sequential number for dimensions, and 0 for the measured variable
	//Get information from id
	if (!flag) {
		// Dimension
		// The string NAXIS has length = 5
		char s[16] = { '\0' };
		strncpy(s, varname + 5, 8);
		if (s == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension information");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_DIM_READ_ERROR, "");
			return OPH_FITS_ERROR;
		}
		var->varid = (int) strtol(s, NULL, 10);
		var->vartype = LONG_IMG;
		var->ndims = 1;
		var->dims_id = malloc(var->ndims * sizeof(int));
		if (!(var->dims_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions fits ids");
			return OPH_FITS_ERROR;
		}
		var->dims_id[0] = 1;
		var->dims_length = malloc(var->ndims * sizeof(size_t));
		if (!(var->dims_length)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, id_container, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "dimensions fits lengths");
			return OPH_FITS_ERROR;
		}
		var->dims_length[0] = *dims_length;
	}
	var->varsize = 1 * var->dims_length[0];
	return OPH_FITS_SUCCESS;
}
