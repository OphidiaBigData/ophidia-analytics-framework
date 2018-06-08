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

#define _GNU_SOURCE

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <gsl/gsl_spline.h>
#include <gsl/gsl_wavelet.h>
#include <gsl/gsl_sort.h>
#include <gsl/gsl_statistics.h>
#include <gsl/gsl_fit.h>

#include <math.h>
#include <ctype.h>

#include "oph_analytics_operator_library.h"
#include "drivers/OPH_EXPLORENC_operator.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"

#define OPH_DEFAULT_STATS "00000000000000"
#define OPH_STAT_UNSELECTED '0'
#define OPH_STAT_SELECTED '1'

#define OPH_OPERATION_MAX "max"
#define OPH_OPERATION_MIN "min"
#define OPH_OPERATION_AVG "avg"
#define OPH_OPERATION_SUM "sum"

int oph_explorenc_add_to(double v, const char *operation, double *a, unsigned int *n)
{
	if (!a || !n)
		return 1;
	if (!(*n))
		*a = v;
	else if (!operation || !strcasecmp(operation, OPH_OPERATION_AVG))
		*a = ((*a) * (*n) + v) / (*n + 1.0);	// Default
	else if (!strcasecmp(operation, OPH_OPERATION_MAX)) {
		if (*a < v)
			*a = v;
	} else if (!strcasecmp(operation, OPH_OPERATION_MIN)) {
		if (*a > v)
			*a = v;
	} else if (!strcasecmp(operation, OPH_OPERATION_SUM))
		*a += v;
	else
		return 2;
	(*n)++;
	return 0;
}

const char *oph_stat_name(int k)
{
	switch (k) {
		case 0:
			return "MEAN";
		case 1:
			return "VARIANCE";
		case 2:
			return "STANDARD DEVIATION";
		case 3:
			return "ABSOLUTE DEVIATION";
		case 4:
			return "SKEW";
		case 5:
			return "KURTOSIS";
		case 6:
			return "AUTOCORRELATION";
		case 7:
			return "MAXIMUM";
		case 8:
			return "MINIMUM";
		case 9:
			return "0.05 QUANTILE";
		case 10:
			return "0.25 QUANTILE";
		case 11:
			return "0.5 QUANTILE";
		case 12:
			return "0.75 QUANTILE";
		case 13:
			return "0.95 QUANTILE";
	}
	return "";
}

int oph_gsl_stats_produce(void *in_data, const size_t data_len, nc_type data_type, const char *mask, double *out_data, const size_t out_len)
{
	size_t i = 0, j;
	for (j = 0; j < out_len; j++) {
		while ((mask[i] != OPH_STAT_SELECTED) && (i < strlen(OPH_DEFAULT_STATS)))
			i++;
		if (i == strlen(OPH_DEFAULT_STATS))
			return 1;

		if (data_type == NC_INT) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_int_mean((int *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_int_variance((int *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_int_sd((int *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_int_absdev((int *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_int_skew((int *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_int_kurtosis((int *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_int_lag1_autocorrelation((int *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_int_max((int *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_int_min((int *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_int_quantile_from_sorted_data((int *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_int_quantile_from_sorted_data((int *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_int_quantile_from_sorted_data((int *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_int_quantile_from_sorted_data((int *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_int_quantile_from_sorted_data((int *) in_data, 1, data_len, 0.95);
					break;
			}
		} else if (data_type == NC_SHORT) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_short_mean((short *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_short_variance((short *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_short_sd((short *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_short_absdev((short *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_short_skew((short *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_short_kurtosis((short *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_short_lag1_autocorrelation((short *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_short_max((short *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_short_min((short *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_short_quantile_from_sorted_data((short *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_short_quantile_from_sorted_data((short *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_short_quantile_from_sorted_data((short *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_short_quantile_from_sorted_data((short *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_short_quantile_from_sorted_data((short *) in_data, 1, data_len, 0.95);
					break;
			}
		} else if (data_type == NC_BYTE || data_type == NC_CHAR) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_char_mean((char *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_char_variance((char *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_char_sd((char *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_char_absdev((char *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_char_skew((char *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_char_kurtosis((char *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_char_lag1_autocorrelation((char *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_char_max((char *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_char_min((char *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_char_quantile_from_sorted_data((char *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_char_quantile_from_sorted_data((char *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_char_quantile_from_sorted_data((char *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_char_quantile_from_sorted_data((char *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_char_quantile_from_sorted_data((char *) in_data, 1, data_len, 0.95);
					break;
			}
		} else if (data_type == NC_INT64) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_long_mean((long int *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_long_variance((long int *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_long_sd((long int *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_long_absdev((long int *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_long_skew((long int *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_long_kurtosis((long int *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_long_lag1_autocorrelation((long int *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_long_max((long int *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_long_min((long int *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_long_quantile_from_sorted_data((long int *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_long_quantile_from_sorted_data((long int *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_long_quantile_from_sorted_data((long int *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_long_quantile_from_sorted_data((long int *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_long_quantile_from_sorted_data((long int *) in_data, 1, data_len, 0.95);
					break;
			}
		} else if (data_type == NC_FLOAT) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_float_mean((float *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_float_variance((float *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_float_sd((float *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_float_absdev((float *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_float_skew((float *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_float_kurtosis((float *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_float_lag1_autocorrelation((float *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_float_max((float *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_float_min((float *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_float_quantile_from_sorted_data((float *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_float_quantile_from_sorted_data((float *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_float_quantile_from_sorted_data((float *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_float_quantile_from_sorted_data((float *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_float_quantile_from_sorted_data((float *) in_data, 1, data_len, 0.95);
					break;
			}
		} else if (data_type == NC_DOUBLE) {
			switch (i) {
				case 0:	// 1xxxxxxxxxxxxx : mean
					out_data[j] = gsl_stats_mean((double *) in_data, 1, data_len);
					break;
				case 1:	// x1xxxxxxxxxxxx : variance
					out_data[j] = gsl_stats_variance((double *) in_data, 1, data_len);
					break;
				case 2:	// xx1xxxxxxxxxxx : std dev
					out_data[j] = gsl_stats_sd((double *) in_data, 1, data_len);
					break;
				case 3:	// xxx1xxxxxxxxxx : abs dev
					out_data[j] = gsl_stats_absdev((double *) in_data, 1, data_len);
					break;
				case 4:	// xxxx1xxxxxxxxx : skew
					out_data[j] = gsl_stats_skew((double *) in_data, 1, data_len);
					break;
				case 5:	// xxxxx1xxxxxxxx : kurtosis
					out_data[j] = gsl_stats_kurtosis((double *) in_data, 1, data_len);
					break;
				case 6:	// xxxxxx1xxxxxxx : autocorrelation
					out_data[j] = gsl_stats_lag1_autocorrelation((double *) in_data, 1, data_len);
					break;
				case 7:	// xxxxxxx1xxxxxx : max
					out_data[j] = gsl_stats_max((double *) in_data, 1, data_len);
					break;
				case 8:	// xxxxxxxx1xxxxx : min
					out_data[j] = gsl_stats_min((double *) in_data, 1, data_len);
					break;
				case 9:	// xxxxxxxxx1xxxx : 0.05 quantile
					out_data[j] = gsl_stats_quantile_from_sorted_data((double *) in_data, 1, data_len, 0.05);
					break;
				case 10:	// xxxxxxxxxx1xxx : 0.25 quantile->Q1
					out_data[j] = gsl_stats_quantile_from_sorted_data((double *) in_data, 1, data_len, 0.25);
					break;
				case 11:	// xxxxxxxxxxx1xx : 0.5 quantile->Q2 (median)
					out_data[j] = gsl_stats_quantile_from_sorted_data((double *) in_data, 1, data_len, 0.5);
					break;
				case 12:	// xxxxxxxxxxxx1x : 0.75 quantile->Q3
					out_data[j] = gsl_stats_quantile_from_sorted_data((double *) in_data, 1, data_len, 0.75);
					break;
				case 13:	// xxxxxxxxxxxxx1 : 0.95 quantile
					out_data[j] = gsl_stats_quantile_from_sorted_data((double *) in_data, 1, data_len, 0.95);
					break;
			}
		} else
			return 1;

		i++;
	}

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

	if (!(handle->operator_handle = (OPH_EXPLORENC_operator_handle *) calloc(1, sizeof(OPH_EXPLORENC_operator_handle)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
	//1 - Set up struct to empty values
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	NETCDF_var *nc_measure = &(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->measure);
	nc_measure->dims_name = NULL;
	nc_measure->dims_id = NULL;
	nc_measure->dims_unlim = NULL;
	nc_measure->dims_length = NULL;
	nc_measure->dims_type = NULL;
	nc_measure->dims_oph_level = NULL;
	nc_measure->dims_start_index = NULL;
	nc_measure->dims_end_index = NULL;
	nc_measure->dims_order = NULL;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_fit = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num = -1;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays = NULL;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->number_of_dim = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->imp_num_points = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_coeff = 0;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask = NULL;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation = NULL;
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->offset = 0;

	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	char *value;
	// retrieve objkeys
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (oph_tp_parse_multiple_value_param(value, &((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, &((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
		oph_tp_free_multiple_value_param_list(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//3 - Fill struct with the correct data
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->schedule_algo = (int) strtol(value, NULL, 10);

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level = (int) strtol(value, NULL, 10);
	if ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level < 0) || (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level > 2)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not valid parameter '%s'\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Not valid parameter '%s'\n", OPH_IN_PARAM_VISUALIZZATION_LEVEL);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	//Open netcdf file
	int retval, j;
	value = hashtbl_get(task_tbl, OPH_IN_PARAM_SRC_FILE_PATH);
	if (!value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SRC_FILE_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_SRC_FILE_PATH);
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path = (char *) strndup(value, OPH_TP_TASKLEN))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, "nc file path");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}

	if (strstr(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}
	if (!strstr(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path, "http://")
	    && !strstr(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path, "https://")) {
		char *pointer = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			char tmp[OPH_COMMON_BUFFER_LEN];
			if (*pointer != '/') {
				value = hashtbl_get(task_tbl, OPH_IN_PARAM_CDD);
				if (!value) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter '%s'\n", OPH_IN_PARAM_CDD);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CDD);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (*value != '/') {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Parameter '%s' must begin with '/'\n", value);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Parameter '%s' must begin with '/'\n", value);
					return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
				}
				if (strlen(value) > 1) {
					if (strstr(value, "..")) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "The use of '..' is forbidden\n");
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s/%s", value + 1, pointer);
					free(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path);
					((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path = strdup(tmp);
					pointer = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path;
				}
			}
			if (oph_pid_get_base_src_path(&value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read base src_path\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to read base src_path\n");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			snprintf(tmp, OPH_COMMON_BUFFER_LEN, "%s%s%s", value ? value : "", *pointer != '/' ? "/" : "", pointer);
			free(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path);
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path = strdup(tmp);
			free(value);
		}
	}

	if ((retval = nc_open(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path, NC_NOWRITE, &(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER, "", nc_strerror(retval));
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	int ncid = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid;
	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level) {

		NETCDF_var *measure = ((NETCDF_var *) & (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->measure));
		value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
		if (!value || ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level > 0) && !strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_MEASURE_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		strncpy(measure->varname, value, NC_MAX_NAME);
		measure->varname[NC_MAX_NAME] = 0;

		char **exp_dim_names, **imp_dim_names;
		int i, exp_number_of_dim_names = 0, imp_number_of_dim_names = 0;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
		if (!value || ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level > 0) && !strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_EXPLICIT_DIMENSION_NAME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (oph_tp_parse_multiple_value_param(value, &exp_dim_names, &exp_number_of_dim_names)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		measure->nexp = exp_number_of_dim_names;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
		if (!value || ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level > 0) && !strncmp(value, OPH_COMMON_DEFAULT_EMPTY_VALUE, OPH_TP_TASKLEN))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_IMPLICIT_DIMENSION_NAME);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (oph_tp_parse_multiple_value_param(value, &imp_dim_names, &imp_number_of_dim_names)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		measure->nimp = imp_number_of_dim_names;
		measure->ndims = measure->nexp + measure->nimp;

		//Extract measured variable informations
		if ((retval = nc_inq_varid(ncid, measure->varname, &(measure->varid)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Get information from id
		if ((retval = nc_inq_vartype(ncid, measure->varid, &(measure->vartype)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Check ndims value
		int ndims;
		if ((retval = nc_inq_varndims(ncid, measure->varid, &(ndims)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (ndims != measure->ndims) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong number of dimensions provided in task string\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WRONG_DIM_NUMBER_NO_CONTAINER, "", ndims);
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if (!(measure->dims_name = (char **) malloc(measure->ndims * sizeof(char *)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_name");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		memset(measure->dims_name, 0, measure->ndims * sizeof(char *));

		if (!(measure->dims_length = (size_t *) malloc(measure->ndims * sizeof(size_t)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_length");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_unlim = (char *) malloc(measure->ndims * sizeof(char)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_unlim");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_type = (short int *) malloc(measure->ndims * sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_type");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_oph_level = (short int *) calloc(measure->ndims, sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_oph_level");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if (!(measure->dims_order = (short int *) calloc(measure->ndims, sizeof(short int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_order");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		//Extract dimension ids following order in the nc file
		if (!(measure->dims_id = (int *) malloc(measure->ndims * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_id");
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		if ((retval = nc_inq_vardimid(ncid, measure->varid, measure->dims_id))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		int unlimdimid;
		if ((retval = nc_inq_unlimdim(ncid, &unlimdimid))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
			oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		//Extract dimensions information and check names provided by task string
		char *dimname;
		short int flag = 0;
		for (i = 0; i < ndims; i++) {
			measure->dims_unlim[i] = measure->dims_id[i] == unlimdimid;
			measure->dims_name[i] = (char *) malloc((NC_MAX_NAME + 1) * sizeof(char));
			if ((retval = nc_inq_dim(ncid, measure->dims_id[i], measure->dims_name[i], &measure->dims_length[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

		int level = 1;
		for (i = 0; i < measure->nexp; i++) {
			flag = 0;
			dimname = exp_dim_names[i];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, "", dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_oph_level[j] = level++;
			measure->dims_type[j] = 1;
			measure->dims_order[i] = j;
		}

		level = 1;
		for (i = measure->nexp; i < measure->ndims; i++) {
			flag = 0;
			dimname = imp_dim_names[i - measure->nexp];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, "", dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
				oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			measure->dims_type[j] = 0;
			measure->dims_oph_level[j] = level++;
			measure->dims_order[i] = j;
		}

		oph_tp_free_multiple_value_param_list(exp_dim_names, exp_number_of_dim_names);
		oph_tp_free_multiple_value_param_list(imp_dim_names, imp_number_of_dim_names);

		//ADDED TO MANAGE SUBSETTED IMPORT
		char **sub_dims = 0;
		char **sub_filters = 0;
		int number_of_sub_dims = 0;
		int number_of_sub_filters = 0;
		int is_index = 1;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER_TYPE);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_SUBSET_FILTER_TYPE);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (!strncmp(value, OPH_EXPLORENC_SUBSET_COORD, OPH_TP_TASKLEN)) {
			is_index = 0;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_DIMENSIONS);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_DIMENSIONS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_SUBSET_DIMENSIONS);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		short int issubset = 0;
		if (strncmp(value, OPH_COMMON_NONE_FILTER, OPH_TP_TASKLEN)) {
			issubset = 1;
			if (oph_tp_parse_multiple_value_param(value, &sub_dims, &number_of_sub_dims)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SUBSET_FILTER);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SUBSET_FILTER);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, "", OPH_IN_PARAM_SUBSET_FILTER);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		short int isfilter = 0;
		if (strncmp(value, OPH_COMMON_ALL_FILTER, OPH_TP_TASKLEN)) {
			isfilter = 1;
			if (oph_tp_parse_multiple_value_param(value, &sub_filters, &number_of_sub_filters)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		if ((issubset && !isfilter) || (!issubset && isfilter)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_LIMIT_FILTER);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_LIMIT_FILTER);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_LIMIT_FILTER);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_ALL_FILTER) != 0) {
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit = (int) strtol(value, NULL, 10);
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_ID);
		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_ID);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_ID);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHOW_ID);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id = 1;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_INDEX);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_INDEX);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHOW_INDEX);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index = 1;

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_SHOW_TIME);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SHOW_TIME);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SHOW_TIME);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0)
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time = 1;

		if (number_of_sub_dims != number_of_sub_filters) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Number of multidimensional parameters not corresponding\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MULTIVARIABLE_NUMBER_NOT_CORRESPONDING);
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//Check the sub_filters strings
		for (i = 0; i < number_of_sub_dims; i++) {
			if (strchr(sub_filters[i], ':') != strrchr(sub_filters[i], ':')) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Strided range are not supported\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
		}

		//Alloc space for subsetting parameters
		if (!(measure->dims_start_index = (int *) malloc(measure->ndims * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_start_index");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		if (!(measure->dims_end_index = (int *) malloc(measure->ndims * sizeof(int)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "measure dims_end_index");
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		//Check if the provided dims for subsetting exist
		for (i = 0; i < number_of_sub_dims; i++) {
			flag = 0;
			dimname = sub_dims[i];
			for (j = 0; j < ndims; j++) {
				if (!strcmp(dimname, measure->dims_name[j])) {
					flag = 1;
					break;
				}
			}
			if (!flag) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find dimension %s related to variable %s in in nc file\n", dimname, measure->varname);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIMENSION_VARIABLE_ERROR_NO_CONTAINER, "", dimname, measure->varname);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

		char *curfilter = NULL;
		char *startfilter = NULL;
		char *endfilter = NULL;
		size_t ii;

		NETCDF_var tmp_var;
		tmp_var.dims_id = malloc(NC_MAX_VAR_DIMS * sizeof(int));
		if (!(tmp_var.dims_id)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_INC_VAR_ERROR_NO_CONTAINER, "", nc_strerror(retval));
			oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
			oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		//For each dimension, set the dim_start_index and dim_end_index
		for (i = 0; i < ndims; i++) {
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
				endfilter = strchr(curfilter, ':');
				if (!endfilter) {
					//Only single point
					//Check curfilter
					if (strlen(curfilter) < 1) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
						oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
						oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
						free(tmp_var.dims_id);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					if (is_index) {
						//Input filter is index
						for (ii = 0; ii < strlen(curfilter); ii++) {
							if (!isdigit(curfilter[ii])) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer values allowed)\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
								oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
								oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
								free(tmp_var.dims_id);
								return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							}
						}
						measure->dims_start_index[i] = (int) (strtol(curfilter, (char **) NULL, 10));
						measure->dims_end_index[i] = measure->dims_start_index[i];
					} else {
						//Input filter is value
						for (ii = 0; ii < strlen(curfilter); ii++) {
							if (ii == 0) {
								if (!isdigit(curfilter[ii]) && curfilter[ii] != '-') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							} else {
								if (!isdigit(curfilter[ii]) && curfilter[ii] != '.') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							}
						}
						//End of checking filter

						//Extract the index of the coord based on the value
						if ((retval = nc_inq_varid(ncid, measure->dims_name[i], &(tmp_var.varid)))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						/* Get all the information related to the dimension variable; we don’t need name, since we already know it */
						if ((retval = nc_inq_var(ncid, tmp_var.varid, 0, &(tmp_var.vartype), &(tmp_var.ndims), tmp_var.dims_id, 0))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}

						if (tmp_var.ndims != 1) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}

						int coord_index = -1;
						int want_start = 1;	//Single point, it is the same
						int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
						//Extract index of the point given the dimension value
						if (oph_nc_index_by_value
						    (OPH_GENERIC_CONTAINER_ID, ncid, tmp_var.varid, tmp_var.vartype, measure->dims_length[i], curfilter, want_start, 0, &order, &coord_index)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						//Value not found
						if (coord_index >= (int) measure->dims_length[i]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}

						measure->dims_start_index[i] = coord_index;
						measure->dims_end_index[i] = measure->dims_start_index[i];
					}	//End of if(is_index)
				} else {
					//Start and end point
					*endfilter = '\0';
					startfilter = curfilter;
					endfilter++;
					if (strlen(startfilter) < 1 || strlen(endfilter) < 1) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
						oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
						oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
						free(tmp_var.dims_id);
						return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					}
					if (is_index) {
						//Input filter is index         
						for (ii = 0; ii < strlen(startfilter); ii++) {
							if (!isdigit(startfilter[ii])) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
								oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
								oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
								free(tmp_var.dims_id);
								return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							}
						}

						for (ii = 0; ii < strlen(endfilter); ii++) {
							if (!isdigit(endfilter[ii])) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter (only integer value allowed)\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
								oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
								oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
								free(tmp_var.dims_id);
								return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
							}
						}
						measure->dims_start_index[i] = (int) (strtol(startfilter, (char **) NULL, 10));
						measure->dims_end_index[i] = (int) (strtol(endfilter, (char **) NULL, 10));
					} else {
						//Input filter is a value
						for (ii = 0; ii < strlen(startfilter); ii++) {
							if (ii == 0) {
								if (!isdigit(startfilter[ii]) && startfilter[ii] != '-') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							} else {
								if (!isdigit(startfilter[ii]) && startfilter[ii] != '.') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							}
						}
						for (ii = 0; ii < strlen(endfilter); ii++) {
							if (ii == 0) {
								if (!isdigit(endfilter[ii]) && endfilter[ii] != '-') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							} else {
								if (!isdigit(endfilter[ii]) && endfilter[ii] != '.') {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
									oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
									oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
									free(tmp_var.dims_id);
									return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
								}
							}
						}
						//End of checking filter

						//Extract the index of the coord based on the value
						if ((retval = nc_inq_varid(ncid, measure->dims_name[i], &(tmp_var.varid)))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable informations: %s\n", nc_strerror(retval));
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						/* Get all the information related to the dimension variable; we don’t need name, since we already know it */
						if ((retval = nc_inq_var(ncid, tmp_var.varid, 0, &(tmp_var.vartype), &(tmp_var.ndims), tmp_var.dims_id, 0))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}

						if (tmp_var.ndims != 1) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension variable is multidimensional\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}

						int coord_index = -1;
						int want_start = 1;
						int order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
						//Extract index of the point given the dimension value
						if (oph_nc_index_by_value
						    (OPH_GENERIC_CONTAINER_ID, ncid, tmp_var.varid, tmp_var.vartype, measure->dims_length[i], startfilter, want_start, 0, &order, &coord_index)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING, nc_strerror(retval));
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						//Value too big
						if (coord_index >= (int) measure->dims_length[i]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Values exceed dimensions bound\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						measure->dims_start_index[i] = coord_index;

						coord_index = -1;
						want_start = 0;
						order = 1;	//It will be changed by the following function (1 ascending, 0 descending)
						//Extract index of the point given the dimension value
						if (oph_nc_index_by_value
						    (OPH_GENERIC_CONTAINER_ID, ncid, tmp_var.varid, tmp_var.vartype, measure->dims_length[i], endfilter, want_start, 0, &order, &coord_index)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						if (coord_index >= (int) measure->dims_length[i]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
							oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
							oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
							free(tmp_var.dims_id);
							return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
						}
						//oph_nc_index_by value returns the index I need considering the order of the dimension values (ascending/descending)
						measure->dims_end_index[i] = coord_index;
						//Descending order; I need to swap start and end index
						if (!order) {
							int temp_ind = measure->dims_start_index[i];
							measure->dims_start_index[i] = measure->dims_end_index[i];
							measure->dims_end_index[i] = temp_ind;
						}

					}	//End of searching indexes by values
				}	//End of searching the indexex (by values of indexex)
			}	//End of if(!curfilter)
			curfilter = NULL;
			//Check
			if (measure->dims_start_index[i] < 0 || measure->dims_end_index[i] < 0 || measure->dims_start_index[i] > measure->dims_end_index[i]
			    || measure->dims_start_index[i] >= (int) measure->dims_length[i] || measure->dims_end_index[i] >= (int) measure->dims_length[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid subsetting filter\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INVALID_INPUT_STRING);
				oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
				oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}

		}		//End of loop on each dimension

		free(tmp_var.dims_id);
		oph_tp_free_multiple_value_param_list(sub_dims, number_of_sub_dims);
		oph_tp_free_multiple_value_param_list(sub_filters, number_of_sub_filters);

		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->number_of_dim = number_of_sub_dims;

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
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_EXP_DIM_LEVEL_ERROR, "");
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

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_IMPLICIT_POINTS);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_IMPLICIT_POINTS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_IMPLICIT_POINTS);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->imp_num_points = (int) strtol(value, NULL, 10);
		if ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->imp_num_points > 0) && (imp_number_of_dim_names != 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_WAVELET);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_WAVELET);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_WAVELET);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
			if (imp_number_of_dim_names != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet = 1;
		} else if (strcmp(value, OPH_COMMON_ONLY_VALUE) == 0) {
			if (imp_number_of_dim_names != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet = -1;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_OFFSET);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OFFSET);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OFFSET);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->offset = (double) strtod(value, NULL);

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_WAVELET_RATIO);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_WAVELET_RATIO);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_WAVELET_RATIO);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio = (double) strtod(value, NULL);
		if ((((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio > 0) && (imp_number_of_dim_names != 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_WAVELET_COEFF);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_WAVELET_COEFF);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_WAVELET_COEFF);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
			if (imp_number_of_dim_names != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_coeff = 1;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_STATS);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_STATS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_STATS);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value, OPH_DEFAULT_STATS, OPH_TP_TASKLEN)) {
			if (imp_number_of_dim_names != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			if (!(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask = (char *) strndup(value, OPH_TP_TASKLEN))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, OPH_IN_PARAM_STATS);
				return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_FIT);
		if (!value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_FIT);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPORTNC_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_FIT);
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strcmp(value, OPH_COMMON_YES_VALUE) == 0) {
			if (imp_number_of_dim_names != 1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
			}
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_fit = 1;
		}

		value = hashtbl_get(task_tbl, OPH_IN_PARAM_REDUCTION_OPERATION);
		if (value && (!(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation = (char *) strndup(value, OPH_TP_TASKLEN)))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT_NO_CONTAINER, value, OPH_IN_PARAM_REDUCTION_OPERATION);
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NULL_OPERATOR_HANDLE_NO_CONTAINER, "");
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}
	//Only master process has to continue
	if (handle->proc_rank != 0)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, retval = 0, result = OPH_ANALYTICS_OPERATOR_SUCCESS;
	int is_objkey_printable, iii, jjj = 0, num_fields;
	char jsontmp[OPH_COMMON_BUFFER_LEN];
	char **jsonkeys = NULL;
	char **fieldtypes = NULL;
	char **jsonvalues = NULL;
	int ncid = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid;
	char tmp_value[OPH_COMMON_BUFFER_LEN];
	*tmp_value = 0;

	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level) {

		NETCDF_var *measure = &((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->measure;

		// Compute array_length
		int array_length = 1;
		for (i = 0; i < measure->ndims; i++) {
			//Consider only implicit dimensions
			//Modified to allow subsetting
			if (!measure->dims_type[i]) {
				if (measure->dims_end_index[i] == measure->dims_start_index[i])
					continue;
				array_length *= (measure->dims_end_index[i] - measure->dims_start_index[i]) + 1;
			}
		}

		NETCDF_var tmp_var;
		oph_odb_dimension dims[measure->ndims];
		oph_odb_dimension_instance dim_inst[measure->ndims];
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays = (char **) malloc(measure->ndims * sizeof(char *));
		if (!((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc dimension informations\n");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR, "");
			return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}
		for (i = 0; i < measure->ndims; i++)
			((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays[i] = NULL;

		//For each input dimension
		for (i = 0; i < measure->ndims; i++) {
			tmp_var.dims_id = NULL;
			tmp_var.dims_length = NULL;

			if (oph_nc_get_nc_var(OPH_GENERIC_CONTAINER_ID, measure->dims_name[i], ncid, 1, &tmp_var)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR, nc_strerror(retval));
				if (tmp_var.dims_id)
					free(tmp_var.dims_id);
				if (tmp_var.dims_length)
					free(tmp_var.dims_length);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			free(tmp_var.dims_id);
			free(tmp_var.dims_length);

			//Create entry in dims and dim_insts
			dims[i].id_dimension = i;
			dims[i].id_container = OPH_GENERIC_CONTAINER_ID;
			dims[i].id_hierarchy = 0;
			snprintf(dims[i].dimension_name, OPH_ODB_DIM_DIMENSION_SIZE, "%s", measure->dims_name[i]);
			if (oph_nc_get_c_type(tmp_var.vartype, dims[i].dimension_type)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: type cannot be converted\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR, "type cannot be converted");
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			dim_inst[i].id_dimension = i;
			dim_inst[i].fk_id_dimension_index = 0;
			dim_inst[i].fk_id_dimension_label = 0;
			dim_inst[i].id_grid = 0;
			dim_inst[i].id_dimensioninst = 0;
			//Modified to allow subsetting
			if (measure->dims_start_index[i] == measure->dims_end_index[i])
				tmp_var.varsize = 1;
			else
				tmp_var.varsize = measure->dims_end_index[i] - measure->dims_start_index[i] + 1;
			dim_inst[i].size = tmp_var.varsize;
			dim_inst[i].concept_level = 0;
			dim_inst[i].unlimited = measure->dims_unlim[i];

			//Modified to allow subsetting
			tmp_var.dims_start_index = &(measure->dims_start_index[i]);
			tmp_var.dims_end_index = &(measure->dims_end_index[i]);

			if (oph_nc_get_dim_array2
			    (OPH_GENERIC_CONTAINER_ID, ncid, tmp_var.varid, dims[i].dimension_type, tmp_var.varsize, *(tmp_var.dims_start_index), *(tmp_var.dims_end_index),
			     ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays + i)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read dimension informations: %s\n", nc_strerror(retval));
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR, nc_strerror(retval));
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			/* Specific for standard files */
			if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time) {
				char value[OPH_COMMON_BUFFER_LEN];
				memset(dims[i].calendar, 0, OPH_ODB_DIM_TIME_SIZE);
				memset(value, 0, OPH_COMMON_BUFFER_LEN);
				if (!nc_get_att_text(ncid, tmp_var.varid, "calendar", dims[i].calendar) && !nc_get_att_text(ncid, tmp_var.varid, "units", value)) {
					size_t ll;
					if (!strncasecmp(value, OPH_DIM_TIME_UNITS_SECONDS, ll = strlen(OPH_DIM_TIME_UNITS_SECONDS)) ||
					    !strncasecmp(value, OPH_DIM_TIME_UNITS_HOURS, ll = strlen(OPH_DIM_TIME_UNITS_HOURS)) ||
					    !strncasecmp(value, OPH_DIM_TIME_UNITS_DAYS, ll = strlen(OPH_DIM_TIME_UNITS_DAYS))) {
						strncpy(dims[i].units, value, ll);
						dims[i].units[ll] = 0;
						strcpy(dims[i].base_time, value + ll + strlen(OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR) + 2);
					}
				}
			}
		}

		int number_of_dimensions = measure->ndims;
		int total_number_of_rows = 0, number_of_rows = 0, empty = 1;
		for (i = 0; i < number_of_dimensions; ++i) {
			if (measure->dims_type[i] && dim_inst[i].size) {
				if (!total_number_of_rows)
					total_number_of_rows = 1;
				total_number_of_rows *= dim_inst[i].size;
			}
		}
		if (!total_number_of_rows)
			total_number_of_rows = 1;	// APEX

		char **dim_rows = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays;

		int ii, jj, k, kk, kkk = -1;

		size_t *ppp = NULL;
		double *dim_d = NULL, *dim_dd = NULL, *row_d = NULL, *row_dd = NULL, *new_dim = NULL, *wcoeff = NULL, *wrow = NULL, *wi_dim = NULL, *wi_row = NULL, *accumulator = NULL;
		float *dim_f = NULL;
		int *dim_i = NULL;
		long long *dim_l = NULL;
		short *dim_s = NULL;
		char *dim_b = NULL;
		char **stats_name = NULL;
		int current_limit = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;
		unsigned long long max, block_size = 1, _block_size, index_dim = 0, _index_dim;
		int ed = 0, fr_ed = 0;
		char *dimension_name[number_of_dimensions];
		char *dimension_type[number_of_dimensions];
		char dimension_time[number_of_dimensions];
		for (jj = 0; jj < number_of_dimensions; jj++) {
			dimension_name[jj] = dimension_type[jj] = NULL;
			dimension_time[jj] = 0;
			ii = measure->dims_order[jj];
			if (!measure->dims_type[ii] || dim_inst[ii].size)
				kkk = ii;
			if (!measure->dims_type[ii] || !dim_inst[ii].size)
				continue;
			dimension_name[ed] = dims[ii].dimension_name;
			dimension_type[ed] = dims[ii].dimension_type;
			dimension_time[ed] = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[ii].calendar && strlen(dims[ii].calendar);
			block_size *= dim_inst[ii].size;
			ed++;
		}
		for (jj = 0; jj < number_of_dimensions; jj++) {
			ii = measure->dims_order[jj];
			if (!measure->dims_type[ii] || dim_inst[ii].size)
				continue;
			dimension_name[ed + fr_ed] = dims[ii].dimension_name;
			dimension_type[ed + fr_ed] = dims[ii].dimension_type;
			fr_ed++;
		}

		int show_stats = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask ? 1 : 0, stats_size = 0;
		while (show_stats) {
			char *stats_mask = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask;
			int tot_size = (int) strlen(stats_mask);
			if (tot_size > (int) strlen(OPH_DEFAULT_STATS))
				tot_size = (int) strlen(OPH_DEFAULT_STATS);
			for (k = 0; k < tot_size; ++k)
				if (stats_mask[k] == OPH_STAT_SELECTED)
					stats_size++;
				else if (stats_mask[k] != OPH_STAT_UNSELECTED) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
					result = OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
					break;
				}
			if (!stats_size)
				show_stats = 0;
			else {
				row_dd = (double *) malloc(stats_size * sizeof(double));
				if (!row_dd) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				stats_name = (char **) malloc(stats_size * sizeof(char *));
				for (k = kk = 0; k < tot_size; ++k)
					if (stats_mask[k] == OPH_STAT_SELECTED)
						stats_name[kk++] = strdup(oph_stat_name(k));
			}
			break;
		}

		char *dim_rows_ = NULL;
		if (!result && (kkk >= 0)) {
			size_t size = array_length;
			if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(int);
			else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(long long);
			else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(short);
			else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(char);
			else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(float);
			else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
				size *= sizeof(double);
			else
				size *= sizeof(double);
			if (size)
				dim_rows_ = (char *) malloc(size);
			if (!dim_rows) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
			}
			if (size)
				memcpy(dim_rows_, dim_rows[kkk], size);
		}

		int imp_num_points = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->imp_num_points, array_length_ = array_length, aaa = array_length - 1, pppp = imp_num_points - 1;
		double step, offset = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->offset / 100.0, one_minus_offset = 1.0 - offset;
		gsl_interp_accel *acc = NULL, *acc_wavelet = NULL, *acc_coeff = NULL;
		gsl_spline *spline = NULL, *spline_forward = NULL, *spline_inverse = NULL, *spline_coeff = NULL;
		while (!result && (imp_num_points > 0)) {
			row_d = (double *) malloc(array_length * sizeof(double));
			if (!row_d) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				break;
			}
			new_dim = (double *) malloc(imp_num_points * sizeof(double));
			if (!new_dim) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				break;
			}
			dim_inst[kkk].size = array_length_ = imp_num_points;
			dim_dd = (double *) malloc(array_length * sizeof(double));
			if (!dim_dd) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				break;
			}
			if (kkk < 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_i = (int *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_i[0] > dim_i[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (k = 0; k < array_length; ++k)
					dim_dd[k] = (double) dim_i[k];
				if (imp_num_points > array_length)	// Interpolation
				{
					step = ((double) (dim_i[aaa] - dim_i[0])) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_i[0] + k * step;
					new_dim[k] = dim_i[aaa];
				} else {
					step = (dim_i[aaa] - dim_i[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_i[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(int));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_i = (int *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_i[k] = (int) round(new_dim[k]);
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_s = (short *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_s[0] > dim_s[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (k = 0; k < array_length; ++k)
					dim_dd[k] = (double) dim_s[k];
				if (imp_num_points > array_length)	// Interpolation
				{
					step = ((double) (dim_s[aaa] - dim_s[0])) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_s[0] + k * step;
					new_dim[k] = dim_s[aaa];
				} else {
					step = (dim_s[aaa] - dim_s[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_s[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(short));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_s = (short *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_s[k] = (short) round(new_dim[k]);
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_b = (char *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_b[0] > dim_b[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (k = 0; k < array_length; ++k)
					dim_dd[k] = (double) dim_b[k];
				if (imp_num_points > array_length)	// Interpolation
				{
					step = ((double) (dim_b[aaa] - dim_b[0])) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_b[0] + k * step;
					new_dim[k] = dim_b[aaa];
				} else {
					step = (dim_b[aaa] - dim_b[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_b[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(char));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_b = (char *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_b[k] = (char) round(new_dim[k]);
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_l = (long long *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_l[0] > dim_l[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (k = 0; k < array_length; ++k)
					dim_dd[k] = (double) dim_l[k];
				if (imp_num_points > array_length)	// Interpolation
				{
					step = ((double) (dim_l[aaa] - dim_l[0])) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_l[0] + k * step;
					new_dim[k] = dim_l[aaa];
				} else {
					step = (dim_l[aaa] - dim_l[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_l[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(long long));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_l = (long long *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_l[k] = (long long) round(new_dim[k]);
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_f = (float *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_f[0] > dim_f[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (k = 0; k < array_length; ++k)
					dim_dd[k] = (double) dim_f[k];
				if (imp_num_points > array_length)	// Interpolation
				{
					step = (dim_f[aaa] - dim_f[0]) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_f[0] + k * step;
					new_dim[k] = dim_f[aaa];
				} else {
					step = (dim_f[aaa] - dim_f[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_f[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(float));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_f = (float *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_f[k] = (float) new_dim[k];
			} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
				dim_d = (double *) (dim_rows[kkk]);
				if ((array_length > 1) && (dim_d[0] > dim_d[aaa])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				memcpy(dim_dd, dim_d, array_length * sizeof(double));
				if (imp_num_points > array_length)	// Interpolation
				{
					step = (dim_d[aaa] - dim_d[0]) / pppp;
					for (k = 0; k < pppp; ++k)
						new_dim[k] = dim_d[0] + k * step;
					new_dim[k] = dim_d[aaa];
				} else {
					step = (dim_d[aaa] - dim_d[0]) / imp_num_points;
					for (k = 0; k < imp_num_points; ++k)
						new_dim[k] = dim_d[0] + (offset + k) * step;
				}
				free(dim_rows[kkk]);
				dim_rows[kkk] = (char *) malloc(imp_num_points * sizeof(double));
				if (!dim_rows[kkk]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				dim_d = (double *) (dim_rows[kkk]);
				for (k = 0; k < imp_num_points; ++k)
					dim_d[k] = new_dim[k];
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED, dims[kkk].dimension_type);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			break;
		}

		while (!result && ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_fit) {
			if (!dim_dd) {
				dim_dd = (double *) malloc(array_length * sizeof(double));
				if (!dim_dd) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				if (kkk < 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_i = (int *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_i[0] > dim_i[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < array_length; ++k)
						dim_dd[k] = (double) dim_i[k];
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_l = (long long *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_l[0] > dim_l[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < array_length; ++k)
						dim_dd[k] = (double) dim_l[k];
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_s = (short *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_s[0] > dim_s[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < array_length; ++k)
						dim_dd[k] = (double) dim_s[k];
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_b = (char *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_b[0] > dim_b[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < array_length; ++k)
						dim_dd[k] = (double) dim_b[k];
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_f = (float *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_f[0] > dim_f[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < array_length; ++k)
						dim_dd[k] = (double) dim_f[k];
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_d = (double *) (dim_rows[kkk]);
					if ((array_length > 1) && (dim_d[0] > dim_d[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					memcpy(dim_dd, dim_d, array_length * sizeof(double));
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED, dims[kkk].dimension_type);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}
			if (!row_d) {
				row_d = (double *) malloc(array_length * sizeof(double));
				if (!row_d) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
			}
			break;
		}

		int spline_num_points = array_length, load_dim = 1;
		int wavelet_points = (int) round(array_length * (1 - ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio / 100.0));
		if ((!wavelet_points && (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio < 100))
		    || ((wavelet_points >= array_length) && (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_ratio > 0))) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Wavelet filter has not been applied\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Wavelet filter cannot be applied\n");
		}
		int wavelet = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet, wavelet_coeff =
		    ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->wavelet_coeff, wfilter = wavelet && (wavelet_points > 0) && (wavelet_points < array_length), evaluate_wavelet = wfilter
		    || wavelet_coeff;

		gsl_wavelet *w = NULL;
		gsl_wavelet_workspace *work = NULL;
		while (!result && evaluate_wavelet) {
			// Check length
			double logg = log2((double) array_length);
			spline_num_points = logg > floor(logg) ? (int) exp2(ceil(logg)) : array_length;
			if (spline_num_points > array_length) {
				wavelet_points = (wavelet_points * spline_num_points) / array_length;
				wi_dim = (double *) malloc(spline_num_points * sizeof(double));
				if (!wi_dim) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				wi_row = (double *) malloc(spline_num_points * sizeof(double));
				if (!wi_row) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				if (!dim_dd) {
					dim_dd = (double *) malloc(array_length * sizeof(double));
					if (!dim_dd) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						break;
					}
					load_dim = 1;
				} else
					load_dim = 0;
				if (!dim_rows_) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_IMP_POINT_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_i = (int *) (dim_rows_);
					if ((array_length > 1) && (dim_i[0] > dim_i[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						for (k = 0; k < array_length; ++k)
							dim_dd[k] = (double) dim_i[k];
					if (spline_num_points > 1) {
						step = ((double) (dim_i[aaa] - dim_i[0])) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_i[0] + k * step;
					} else
						wi_dim[0] = (dim_i[0] + dim_i[aaa]) / 2;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_l = (long long *) (dim_rows_);
					if ((array_length > 1) && (dim_l[0] > dim_l[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						for (k = 0; k < array_length; ++k)
							dim_dd[k] = (double) dim_l[k];
					if (spline_num_points > 1) {
						step = ((double) (dim_l[aaa] - dim_l[0])) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_l[0] + k * step;
					} else
						wi_dim[0] = (dim_l[0] + dim_l[aaa]) / 2;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_s = (short *) (dim_rows_);
					if ((array_length > 1) && (dim_s[0] > dim_s[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						for (k = 0; k < array_length; ++k)
							dim_dd[k] = (double) dim_s[k];
					if (spline_num_points > 1) {
						step = ((double) (dim_s[aaa] - dim_s[0])) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_s[0] + k * step;
					} else
						wi_dim[0] = (dim_s[0] + dim_s[aaa]) / 2;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_b = (char *) (dim_rows_);
					if ((array_length > 1) && (dim_b[0] > dim_b[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						for (k = 0; k < array_length; ++k)
							dim_dd[k] = (double) dim_b[k];
					if (spline_num_points > 1) {
						step = ((double) (dim_b[aaa] - dim_b[0])) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_b[0] + k * step;
					} else
						wi_dim[0] = (dim_b[0] + dim_b[aaa]) / 2;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_f = (float *) (dim_rows_);
					if ((array_length > 1) && (dim_f[0] > dim_f[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						for (k = 0; k < array_length; ++k)
							dim_dd[k] = (double) dim_f[k];
					if (spline_num_points > 1) {
						step = (dim_f[aaa] - dim_f[0]) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_f[0] + k * step;
					} else
						wi_dim[0] = (dim_f[0] + dim_f[aaa]) / 2;
				} else if (!strncasecmp(dims[kkk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
					dim_d = (double *) (dim_rows_);
					if ((array_length > 1) && (dim_d[0] > dim_d[aaa])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (load_dim)
						memcpy(dim_dd, dim_d, array_length * sizeof(double));
					if (spline_num_points > 1) {
						step = (dim_d[aaa] - dim_d[0]) / (spline_num_points - 1);
						for (k = 0; k < spline_num_points; ++k)
							wi_dim[k] = dim_d[0] + k * step;
					} else
						wi_dim[0] = (dim_d[0] + dim_d[aaa]) / 2;
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED, dims[kkk].dimension_type);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}
			// Allocate structs
			w = gsl_wavelet_alloc(gsl_wavelet_daubechies, 4);
			if (!w) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				break;
			}
			work = gsl_wavelet_workspace_alloc(spline_num_points);
			if (!work) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
				result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
				break;
			}
			if (wfilter) {
				wcoeff = (double *) malloc(spline_num_points * sizeof(double));
				if (!wcoeff) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
				ppp = (size_t *) malloc(spline_num_points * sizeof(size_t));
				if (!ppp) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
			}
			if (!row_d) {
				row_d = (double *) malloc(array_length * sizeof(double));
				if (!row_d) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
					result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
					break;
				}
			}
			break;
		}
		if (dim_rows_) {
			free(dim_rows_);
			dim_rows_ = NULL;
		}

		size_t row_size = 0;
		int est_num_rows = total_number_of_rows;
		if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit < total_number_of_rows)
			est_num_rows = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;
		char **rows = (char **) calloc(est_num_rows, sizeof(char *));
		if (!rows) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
			result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
		}

		char *row = NULL, *dump_row = NULL;
		is_objkey_printable =
		    oph_json_is_objkey_printable(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
						 OPH_JSON_OBJKEY_EXPLORENC_DATA);
		while (!result && is_objkey_printable && (wavelet >= 0)) {
			num_fields =
			    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id ? 1 : 0) + (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1) * ed +
			    fr_ed + 1;

			// Header
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
				jsonkeys[jjj] = strdup(OPH_EXPLORENC_ID);
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
			}
			for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
				jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
					snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
					jsonkeys[jjj] = strdup(jsontmp);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
			}
			if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
				break;
			for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
				jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
			}
			if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
				break;
			jsonkeys[jjj] = strdup(measure->varname ? measure->varname : "");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
				fieldtypes[jjj] = strdup(OPH_JSON_LONG);
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
			}
			for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
				if (dimension_time[iii])
					fieldtypes[jjj] = strdup(OPH_JSON_STRING);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_INT);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
				else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
					fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
				else
					fieldtypes[jjj] = 0;
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
			}
			if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
				break;
			for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
			}
			if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
				break;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// Array is a string in any case
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DATA, measure->varname ? measure->varname : "", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (iii = 0; iii < num_fields; iii++)
				if (fieldtypes[iii])
					free(fieldtypes[iii]);
			if (fieldtypes)
				free(fieldtypes);

			// Row by row extraction
			while ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (number_of_rows < total_number_of_rows) && (current_limit > 0)) {
				if (row) {
					free(row);
					row = NULL;
				}
				if (oph_nc_get_row_from_nc(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid, array_length, measure, number_of_rows, &row)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (!rows[number_of_rows] && row) {
					if (!row_size)
						switch (measure->vartype) {
							case NC_BYTE:
							case NC_CHAR:
								row_size = array_length * sizeof(char);
								break;
							case NC_SHORT:
								row_size = array_length * sizeof(short);
								break;
							case NC_INT:
								row_size = array_length * sizeof(int);
								break;
							case NC_INT64:
								row_size = array_length * sizeof(long long);
								break;
							case NC_FLOAT:
								row_size = array_length * sizeof(float);
								break;
							case NC_DOUBLE:
								row_size = array_length * sizeof(double);
								break;
							default:
								row_size = array_length * sizeof(double);
						}
					rows[number_of_rows] = (char *) malloc(row_size);
					if (!rows[number_of_rows]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
						result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						break;
					}
					memcpy(rows[number_of_rows], row, row_size);
				}

				jsonvalues = (char **) calloc(num_fields, sizeof(char *));
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj = 0;

				jj = 0;
				_block_size = block_size;
				_index_dim = number_of_rows;
				for (ii = 0; (ii < ed + fr_ed + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
					if (ii) {
						kk = measure->dims_order[ii - 1];

						if (!dim_rows[kk] || !dim_inst[kk].size)
							continue;
						else {
							_block_size /= dim_inst[kk].size;
							index_dim = _index_dim / _block_size;
							_index_dim %= _block_size;

							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
								if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), jsontmp)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
							} else {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED, dims[kk].dimension_type);
								result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
								break;
							}
						}

						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", index_dim + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
					} else if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
						snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", number_of_rows + 1);
						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					jj++;
				}
				for (ii = 0; (ii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
					jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;

				switch (measure->vartype) {
					case NC_BYTE:
					case NC_CHAR:
						dim_b = (char *) row;
						if (imp_num_points > 0)
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_b[k];
						wrow = row_d;
						break;
					case NC_SHORT:
						dim_s = (short *) row;
						if (imp_num_points > 0)
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_i[k];
						wrow = row_d;
						break;
					case NC_INT:
						dim_i = (int *) row;
						if (imp_num_points > 0)
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_i[k];
						wrow = row_d;
						break;
					case NC_INT64:
						dim_l = (long long *) row;
						if (imp_num_points > 0)
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_l[k];
						wrow = row_d;
						break;
					case NC_FLOAT:
						dim_f = (float *) row;
						if (imp_num_points > 0)
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_f[k];
						wrow = row_d;
						break;
					case NC_DOUBLE:
						wrow = (double *) row;
						break;
					default:
						wrow = (double *) row;
				}
				if (imp_num_points > array_length)	// Interpolation
				{
					spline = gsl_spline_alloc(gsl_interp_cspline, array_length);
					if (!spline) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						break;
					}
					acc = gsl_interp_accel_alloc();
					if (!acc) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
						break;
					}
				} else if (imp_num_points > 0)	// Reduction
				{
					unsigned int num[imp_num_points];
					accumulator = (double *) malloc(imp_num_points * sizeof(double));
					for (k = 0; k < imp_num_points; ++k) {
						accumulator[k] = NAN;
						num[k] = 0;
					}
					for (ii = 0, k = 0; ii < array_length; ++ii) {
						while ((k < imp_num_points - 1) && (dim_dd[ii] > (offset * new_dim[k] + one_minus_offset * new_dim[k + 1])))
							k++;
						if (oph_explorenc_add_to(wrow[ii], ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation, accumulator + k, num + k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
				}
				switch (measure->vartype) {
					case NC_INT:
						if (imp_num_points > 0) {
							dim_i = (int *) malloc(imp_num_points * sizeof(int));
							if (!dim_i) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_i[k] = (int) round(gsl_spline_eval(spline, new_dim[k], acc));
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_i[k] = (int) round(accumulator[k]);	// Reduction
						}
						max = array_length_ * (OPH_COMMON_MAX_INT_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_i[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_i)
							free(dim_i);
						break;
					case NC_SHORT:
						if (imp_num_points > 0) {
							dim_s = (short *) malloc(imp_num_points * sizeof(short));
							if (!dim_s) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_s[k] = (short) round(gsl_spline_eval(spline, new_dim[k], acc));
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_s[k] = (short) round(accumulator[k]);	// Reduction
						}
						max = array_length_ * (OPH_COMMON_MAX_SHORT_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_s[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_s)
							free(dim_s);
						break;
					case NC_BYTE:
					case NC_CHAR:
						if (imp_num_points > 0) {
							dim_b = (char *) malloc(imp_num_points * sizeof(char));
							if (!dim_b) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_b[k] = (char) round(gsl_spline_eval(spline, new_dim[k], acc));
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_b[k] = (char) round(accumulator[k]);	// Reduction
						}
						max = array_length_ * (OPH_COMMON_MAX_BYTE_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_b[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_b)
							free(dim_b);
						break;
					case NC_INT64:
						if (imp_num_points > 0) {
							dim_l = (long long *) malloc(imp_num_points * sizeof(long long));
							if (!dim_l) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_l[k] = (long long) round(gsl_spline_eval(spline, new_dim[k], acc));
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_l[k] = (long long) round(accumulator[k]);	// Reduction
						}
						max = array_length_ * (OPH_COMMON_MAX_LONG_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld%s", dim_l[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_l)
							free(dim_l);
						break;
					case NC_FLOAT:
						if (imp_num_points > 0) {
							dim_f = (float *) malloc(imp_num_points * sizeof(float));
							if (!dim_f) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_f[k] = (float) gsl_spline_eval(spline, new_dim[k], acc);
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_f[k] = (float) accumulator[k];	// Reduction
						}
						max = array_length_ * (OPH_COMMON_MAX_FLOAT_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_f[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_f)
							free(dim_f);
						break;
					case NC_DOUBLE:
						if (imp_num_points > 0) {
							dim_d = (double *) malloc(imp_num_points * sizeof(double));
							if (!dim_d) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = gsl_spline_eval(spline, new_dim[k], acc);
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = accumulator[k];	// Reduction
						} else
							dim_d = wrow;
						max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_d)
							free(dim_d);
						break;
					default:
						if (imp_num_points > 0) {
							dim_d = (double *) malloc(imp_num_points * sizeof(double));
							if (!dim_d) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							if (imp_num_points > array_length)	// Interpolation
							{
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = gsl_spline_eval(spline, new_dim[k], acc);
							} else
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = accumulator[k];	// Reduction
						} else
							dim_d = wrow;
						max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
						dump_row = (char *) malloc(max);
						*dump_row = 0;
						for (k = 0; k < array_length_; ++k) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
							if (max - strlen(dump_row) > 1)
								strncat(dump_row, tmp_value, max - strlen(dump_row));
							else
								break;
						}
						if ((imp_num_points > 0) && dim_d)
							free(dim_d);
				}
				if (accumulator) {
					free(accumulator);
					accumulator = NULL;
				}
				if (spline) {
					gsl_spline_free(spline);
					spline = NULL;
				}
				if (acc) {
					gsl_interp_accel_free(acc);
					acc = NULL;
				}
				if (result)
					break;

				jsonvalues[jjj] = strdup(dump_row ? dump_row : "");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DATA, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);
				number_of_rows++;

				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit)
					current_limit--;

				if (dump_row) {
					free(dump_row);
					dump_row = NULL;
				}
				if (empty)
					empty = 0;
			}
			if (dump_row) {
				free(dump_row);
				dump_row = NULL;
			}

			break;
		}

		if (!result && (wavelet >= 0) && empty) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "No rows found by query.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NO_ROWS_FOUND);
		} else {

			while (!result && is_objkey_printable && show_stats && (wavelet >= 0))	// Stats
			{
				number_of_rows = jjj = 0;
				current_limit = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;

				num_fields =
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id ? 1 : 0) +
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1) * ed + fr_ed + stats_size;

				// Header
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					jsonkeys[jjj] = strdup(OPH_EXPLORENC_ID);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
						jsonkeys[jjj] = strdup(jsontmp);
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < stats_size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(stats_name[iii]);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					if (dimension_time[iii])
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_INT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					else
						fieldtypes[jjj] = 0;
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < stats_size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}

				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%s (stats)", measure->varname ? measure->varname : "");
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_STATS, jsontmp, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);

				// Row by row extraction
				while ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (number_of_rows < total_number_of_rows) && (current_limit > 0)) {
					if (rows[number_of_rows]) {
						if (!row)
							row = (char *) malloc(row_size);
						if (!row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(row, rows[number_of_rows], row_size);
					} else {
						if (row) {
							free(row);
							row = NULL;
						}
						if (oph_nc_get_row_from_nc(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid, array_length, measure, number_of_rows, &row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
					if (!rows[number_of_rows] && row) {
						if (!row_size)
							switch (measure->vartype) {
								case NC_BYTE:
								case NC_CHAR:
									row_size = array_length * sizeof(char);
									break;
								case NC_SHORT:
									row_size = array_length * sizeof(short);
									break;
								case NC_INT:
									row_size = array_length * sizeof(int);
									break;
								case NC_INT64:
									row_size = array_length * sizeof(long long);
									break;
								case NC_FLOAT:
									row_size = array_length * sizeof(float);
									break;
								case NC_DOUBLE:
									row_size = array_length * sizeof(double);
									break;
								default:
									row_size = array_length * sizeof(double);
							}
						rows[number_of_rows] = (char *) malloc(row_size);
						if (!rows[number_of_rows]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(rows[number_of_rows], row, row_size);
					}

					jsonvalues = (char **) calloc(num_fields, sizeof(char *));
					if (!jsonvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj = 0;

					jj = 0;
					_block_size = block_size;
					_index_dim = number_of_rows;
					for (ii = 0; (ii < ed + fr_ed + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						if (ii) {
							kk = measure->dims_order[ii - 1];

							if (!dim_rows[kk] || !dim_inst[kk].size)
								continue;
							else {
								_block_size /= dim_inst[kk].size;
								index_dim = _index_dim / _block_size;
								_index_dim %= _block_size;

								if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
									if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), jsontmp)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED,
										dims[kk].dimension_type);
									result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
									break;
								}
							}

							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", index_dim + 1);
								jsonvalues[jjj] = strdup(tmp_value);
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj++;
							}
						} else if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", number_of_rows + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						jj++;
					}
					for (ii = 0; (ii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
						break;

					if (oph_gsl_stats_produce(row, array_length, measure->vartype, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask, row_dd, stats_size)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_STAT_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}

					for (iii = 0; (iii < stats_size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
						snprintf(tmp_value, OPH_COMMON_MAX_DOUBLE_LENGHT + 2, "%.*f", OPH_EXPLORENC_PRECISION, row_dd[iii]);
						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (result)
						break;

					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_STATS, jsonvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					number_of_rows++;

					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit)
						current_limit--;

					if (empty)
						empty = 0;
				}

				break;
			}

			while (!result && is_objkey_printable && ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_fit && (wavelet >= 0))	// Regression
			{
				double c[2], cov00, cov01, cov11, sumsq;

				number_of_rows = jjj = 0;
				current_limit = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;

				num_fields =
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id ? 1 : 0) +
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1) * ed + fr_ed + 1;

				// Header
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					jsonkeys[jjj] = strdup(OPH_EXPLORENC_ID);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
						jsonkeys[jjj] = strdup(jsontmp);
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				jsonkeys[jjj] = strdup(measure->varname ? measure->varname : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					if (dimension_time[iii])
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_INT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					else
						fieldtypes[jjj] = 0;
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// Array is a string in any case
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%s (fitted linearly)", measure->varname ? measure->varname : "");
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_FIT, jsontmp, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);

				// Row by row extraction
				while ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (number_of_rows < total_number_of_rows) && (current_limit > 0)) {
					if (rows[number_of_rows]) {
						if (!row)
							row = (char *) malloc(row_size);
						if (!row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(row, rows[number_of_rows], row_size);
					} else {
						if (row) {
							free(row);
							row = NULL;
						}
						if (oph_nc_get_row_from_nc(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid, array_length, measure, number_of_rows, &row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
					if (!rows[number_of_rows] && row) {
						if (!row_size)
							switch (measure->vartype) {
								case NC_BYTE:
								case NC_CHAR:
									row_size = array_length * sizeof(char);
									break;
								case NC_SHORT:
									row_size = array_length * sizeof(short);
									break;
								case NC_INT:
									row_size = array_length * sizeof(int);
									break;
								case NC_INT64:
									row_size = array_length * sizeof(long long);
									break;
								case NC_FLOAT:
									row_size = array_length * sizeof(float);
									break;
								case NC_DOUBLE:
									row_size = array_length * sizeof(double);
									break;
								default:
									row_size = array_length * sizeof(double);
							}
						rows[number_of_rows] = (char *) malloc(row_size);
						if (!rows[number_of_rows]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(rows[number_of_rows], row, row_size);
					}

					jsonvalues = (char **) calloc(num_fields, sizeof(char *));
					if (!jsonvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj = 0;

					jj = 0;
					_block_size = block_size;
					_index_dim = number_of_rows;
					for (ii = 0; (ii < ed + fr_ed + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						if (ii) {
							kk = measure->dims_order[ii - 1];

							if (!dim_rows[kk] || !dim_inst[kk].size)
								continue;
							else {
								_block_size /= dim_inst[kk].size;
								index_dim = _index_dim / _block_size;
								_index_dim %= _block_size;

								if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
									if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), jsontmp)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED,
										dims[kk].dimension_type);
									result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
									break;
								}
							}

							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", index_dim + 1);
								jsonvalues[jjj] = strdup(tmp_value);
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj++;
							}
						} else if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", number_of_rows + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						jj++;
					}
					for (ii = 0; (ii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
						break;

					switch (measure->vartype) {
						case NC_BYTE:
						case NC_CHAR:
							dim_b = (char *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_b[k];
							wrow = row_d;
							break;
						case NC_SHORT:
							dim_s = (short *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_s[k];
							wrow = row_d;
							break;
						case NC_INT:
							dim_i = (int *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_i[k];
							wrow = row_d;
							break;
						case NC_INT64:
							dim_l = (long long *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_l[k];
							wrow = row_d;
							break;
						case NC_FLOAT:
							dim_f = (float *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_f[k];
							wrow = row_d;
							break;
						case NC_DOUBLE:
							wrow = (double *) row;
							break;
						default:
							wrow = (double *) row;
					}

					if (gsl_fit_linear(dim_dd, 1, wrow, 1, array_length, &c[0], &c[1], &cov00, &cov01, &cov11, &sumsq)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (!imp_num_points)
						for (k = 0; k < array_length; ++k)
							wrow[k] = c[0] + dim_dd[k] * c[1];

					switch (measure->vartype) {
						case NC_INT:
							if (imp_num_points > 0) {
								dim_i = (int *) malloc(imp_num_points * sizeof(int));
								if (!dim_i) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_i[k] = (int) round(c[0] + new_dim[k] * c[1]);
							} else
								for (k = 0; k < array_length_; ++k)
									dim_i[k] = (int) round(wrow[k]);
							max = array_length_ * (OPH_COMMON_MAX_INT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_i[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_i)
								free(dim_i);
							break;
						case NC_SHORT:
							if (imp_num_points > 0) {
								dim_s = (short *) malloc(imp_num_points * sizeof(short));
								if (!dim_s) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_s[k] = (short) round(c[0] + new_dim[k] * c[1]);
							} else
								for (k = 0; k < array_length_; ++k)
									dim_s[k] = (short) round(wrow[k]);
							max = array_length_ * (OPH_COMMON_MAX_SHORT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_s[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_s)
								free(dim_s);
							break;
						case NC_BYTE:
						case NC_CHAR:
							if (imp_num_points > 0) {
								dim_b = (char *) malloc(imp_num_points * sizeof(char));
								if (!dim_b) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_b[k] = (char) round(c[0] + new_dim[k] * c[1]);
							} else
								for (k = 0; k < array_length_; ++k)
									dim_b[k] = (char) round(wrow[k]);
							max = array_length_ * (OPH_COMMON_MAX_BYTE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_b[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_b)
								free(dim_b);
							break;
						case NC_INT64:
							if (imp_num_points > 0) {
								dim_l = (long long *) malloc(imp_num_points * sizeof(long long));
								if (!dim_l) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_l[k] = (long long) round(c[0] + new_dim[k] * c[1]);
							} else
								for (k = 0; k < array_length_; ++k)
									dim_l[k] = (long long) round(wrow[k]);
							max = array_length_ * (OPH_COMMON_MAX_LONG_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld%s", dim_l[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_l)
								free(dim_l);
							break;
						case NC_FLOAT:
							if (imp_num_points > 0) {
								dim_f = (float *) malloc(imp_num_points * sizeof(float));
								if (!dim_f) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_f[k] = (float) (c[0] + new_dim[k] * c[1]);
							} else
								for (k = 0; k < array_length_; ++k)
									dim_f[k] = (float) wrow[k];
							max = array_length_ * (OPH_COMMON_MAX_FLOAT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_f[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_f)
								free(dim_f);
							break;
						case NC_DOUBLE:
							if (imp_num_points > 0) {
								dim_d = (double *) malloc(imp_num_points * sizeof(double));
								if (!dim_d) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_d[k] = c[0] + new_dim[k] * c[1];
							} else
								dim_d = wrow;
							max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_d)
								free(dim_d);
							break;
						default:
							if (imp_num_points > 0) {
								dim_d = (double *) malloc(imp_num_points * sizeof(double));
								if (!dim_d) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_REGRESSION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < array_length_; ++k)
									dim_d[k] = c[0] + new_dim[k] * c[1];
							} else
								dim_d = wrow;
							max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_d)
								free(dim_d);
					}
					if (result)
						break;

					jsonvalues[jjj] = strdup(dump_row ? dump_row : "");
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_FIT, jsonvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					number_of_rows++;

					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit)
						current_limit--;

					if (dump_row) {
						free(dump_row);
						dump_row = NULL;
					}
					if (empty)
						empty = 0;
				}

				if (dump_row) {
					free(dump_row);
					dump_row = NULL;
				}

				break;
			}

			while (!result && is_objkey_printable && wfilter)	// Wavelets
			{
				number_of_rows = jjj = 0;
				current_limit = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;

				num_fields =
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id ? 1 : 0) +
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1) * ed + fr_ed + 1;

				// Header
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					jsonkeys[jjj] = strdup(OPH_EXPLORENC_ID);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
						jsonkeys[jjj] = strdup(jsontmp);
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				jsonkeys[jjj] = strdup(measure->varname ? measure->varname : "");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					if (dimension_time[iii])
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_INT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					else
						fieldtypes[jjj] = 0;
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// Array is a string in any case
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "%s (filtered)", measure->varname ? measure->varname : "");
				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_WAVELET_DATA, jsontmp, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);

				// Row by row extraction
				while ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (number_of_rows < total_number_of_rows) && (current_limit > 0)) {
					if (rows[number_of_rows]) {
						if (!row)
							row = (char *) malloc(row_size);
						if (!row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(row, rows[number_of_rows], row_size);
					} else {
						if (row) {
							free(row);
							row = NULL;
						}
						if (oph_nc_get_row_from_nc(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid, array_length, measure, number_of_rows, &row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
					if (!rows[number_of_rows] && row) {
						if (!row_size)
							switch (measure->vartype) {
								case NC_BYTE:
								case NC_CHAR:
									row_size = array_length * sizeof(char);
									break;
								case NC_SHORT:
									row_size = array_length * sizeof(short);
									break;
								case NC_INT:
									row_size = array_length * sizeof(int);
									break;
								case NC_INT64:
									row_size = array_length * sizeof(long long);
									break;
								case NC_FLOAT:
									row_size = array_length * sizeof(float);
									break;
								case NC_DOUBLE:
									row_size = array_length * sizeof(double);
									break;
								default:
									row_size = array_length * sizeof(double);
							}
						rows[number_of_rows] = (char *) malloc(row_size);
						if (!rows[number_of_rows]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(rows[number_of_rows], row, row_size);
					}

					jsonvalues = (char **) calloc(num_fields, sizeof(char *));
					if (!jsonvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj = 0;

					jj = 0;
					_block_size = block_size;
					_index_dim = number_of_rows;
					for (ii = 0; (ii < ed + fr_ed + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						if (ii) {
							kk = measure->dims_order[ii - 1];

							if (!dim_rows[kk] || !dim_inst[kk].size)
								continue;
							else {
								_block_size /= dim_inst[kk].size;
								index_dim = _index_dim / _block_size;
								_index_dim %= _block_size;

								if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
									if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), jsontmp)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED,
										dims[kk].dimension_type);
									result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
									break;
								}
							}

							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", index_dim + 1);
								jsonvalues[jjj] = strdup(tmp_value);
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj++;
							}
						} else if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", number_of_rows + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						jj++;
					}
					for (ii = 0; (ii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
						break;

					switch (measure->vartype) {
						case NC_BYTE:
						case NC_CHAR:
							dim_b = (char *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_b[k];
							wrow = row_d;
							break;
						case NC_SHORT:
							dim_s = (short *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_s[k];
							wrow = row_d;
							break;
						case NC_INT:
							dim_i = (int *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_i[k];
							wrow = row_d;
							break;
						case NC_INT64:
							dim_l = (long long *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_l[k];
							wrow = row_d;
							break;
						case NC_FLOAT:
							dim_f = (float *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_f[k];
							wrow = row_d;
							break;
						case NC_DOUBLE:
							wrow = (double *) row;
							break;
						default:
							wrow = (double *) row;
					}
					int expand = spline_num_points > array_length;
					if (expand)	// Interpolation to 2-power
					{
						// wrow -> wi_row
						spline_forward = gsl_spline_alloc(gsl_interp_cspline, array_length);
						if (!spline_forward) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						acc_wavelet = gsl_interp_accel_alloc();
						if (!acc_wavelet) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						if (gsl_spline_init(spline_forward, dim_dd, wrow, array_length)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						for (k = 0; k < spline_num_points; ++k)
							wi_row[k] = gsl_spline_eval(spline_forward, wi_dim[k], acc_wavelet);
						if (spline_forward) {
							gsl_spline_free(spline_forward);
							spline_forward = NULL;
						}
						if (acc_wavelet) {
							gsl_interp_accel_free(acc_wavelet);
							acc_wavelet = NULL;
						}
					}
					if (gsl_wavelet_transform_forward(w, expand ? wi_row : wrow, 1, spline_num_points, work)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (k = 0; k < spline_num_points; k++)
						wcoeff[k] = fabs(expand ? wi_row[k] : wrow[k]);
					gsl_sort_index(ppp, wcoeff, 1, spline_num_points);
					for (k = 0; (k + wavelet_points) < spline_num_points; k++)
						if (expand)
							wi_row[ppp[k]] = 0;
						else
							wrow[ppp[k]] = 0;
					if (gsl_wavelet_transform_inverse(w, expand ? wi_row : wrow, 1, spline_num_points, work)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (expand)	// De-interpolation from 2-power
					{
						// wi_row -> wrow
						spline_inverse = gsl_spline_alloc(gsl_interp_cspline, spline_num_points);
						if (!spline_inverse) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						acc_wavelet = gsl_interp_accel_alloc();
						if (!acc_wavelet) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						if (gsl_spline_init(spline_inverse, wi_dim, wi_row, spline_num_points)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						for (k = 0; k < array_length; ++k)
							wrow[k] = gsl_spline_eval(spline_inverse, dim_dd[k], acc_wavelet);
						if (spline_inverse) {
							gsl_spline_free(spline_inverse);
							spline_inverse = NULL;
						}
						if (acc_wavelet) {
							gsl_interp_accel_free(acc_wavelet);
							acc_wavelet = NULL;
						}
					}
					if (imp_num_points > 0) {
						spline = gsl_spline_alloc(gsl_interp_cspline, array_length);
						if (!spline) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						acc = gsl_interp_accel_alloc();
						if (!acc) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
					}
					switch (measure->vartype) {
						case NC_INT:
							for (k = 0; k < array_length_; ++k)
								dim_i[k] = (int) round(wrow[k]);
							if (imp_num_points > 0) {
								dim_i = (int *) malloc(imp_num_points * sizeof(int));
								if (!dim_i) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_i[k] = (int) round(gsl_spline_eval(spline, new_dim[k], acc));
							}
							max = array_length_ * (OPH_COMMON_MAX_INT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_i[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_i)
								free(dim_i);
							break;
						case NC_SHORT:
							for (k = 0; k < array_length_; ++k)
								dim_s[k] = (short) round(wrow[k]);
							if (imp_num_points > 0) {
								dim_s = (short *) malloc(imp_num_points * sizeof(short));
								if (!dim_s) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_s[k] = (short) round(gsl_spline_eval(spline, new_dim[k], acc));
							}
							max = array_length_ * (OPH_COMMON_MAX_SHORT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_s[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_s)
								free(dim_s);
							break;
						case NC_BYTE:
						case NC_CHAR:
							for (k = 0; k < array_length_; ++k)
								dim_b[k] = (char) round(wrow[k]);
							if (imp_num_points > 0) {
								dim_b = (char *) malloc(imp_num_points * sizeof(char));
								if (!dim_b) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_b[k] = (char) round(gsl_spline_eval(spline, new_dim[k], acc));
							}
							max = array_length_ * (OPH_COMMON_MAX_BYTE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d%s", dim_b[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_b)
								free(dim_b);
							break;
						case NC_INT64:
							for (k = 0; k < array_length_; ++k)
								dim_l[k] = (long long) round(wrow[k]);
							if (imp_num_points > 0) {
								dim_l = (long long *) malloc(imp_num_points * sizeof(long long));
								if (!dim_l) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_l[k] = (long long) round(gsl_spline_eval(spline, new_dim[k], acc));
							}
							max = array_length_ * (OPH_COMMON_MAX_LONG_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld%s", dim_l[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_l)
								free(dim_l);
							break;
						case NC_FLOAT:
							for (k = 0; k < array_length_; ++k)
								dim_f[k] = (float) wrow[k];
							if (imp_num_points > 0) {
								dim_f = (float *) malloc(imp_num_points * sizeof(float));
								if (!dim_f) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_f[k] = (float) gsl_spline_eval(spline, new_dim[k], acc);
							}
							max = array_length_ * (OPH_COMMON_MAX_FLOAT_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_f[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_f)
								free(dim_f);
							break;
						case NC_DOUBLE:
							if (imp_num_points > 0) {
								dim_d = (double *) malloc(imp_num_points * sizeof(double));
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = gsl_spline_eval(spline, new_dim[k], acc);
							} else
								dim_d = wrow;
							max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_d)
								free(dim_d);
							break;
						default:
							if (imp_num_points > 0) {
								dim_d = (double *) malloc(imp_num_points * sizeof(double));
								if (gsl_spline_init(spline, dim_dd, wrow, array_length)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								for (k = 0; k < imp_num_points; ++k)
									dim_d[k] = gsl_spline_eval(spline, new_dim[k], acc);
							} else
								dim_d = wrow;
							max = array_length_ * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
							dump_row = (char *) malloc(max);
							*dump_row = 0;
							for (k = 0; k < array_length_; ++k) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, dim_d[k], k < array_length_ - 1 ? ", " : "");
								if (max - strlen(dump_row) > 1)
									strncat(dump_row, tmp_value, max - strlen(dump_row));
								else
									break;
							}
							if ((imp_num_points > 0) && dim_d)
								free(dim_d);
					}
					if (spline) {
						gsl_spline_free(spline);
						spline = NULL;
					}
					if (acc) {
						gsl_interp_accel_free(acc);
						acc = NULL;
					}
					if (result)
						break;

					jsonvalues[jjj] = strdup(dump_row ? dump_row : "");
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_WAVELET_DATA, jsonvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					number_of_rows++;

					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit)
						current_limit--;

					if (dump_row) {
						free(dump_row);
						dump_row = NULL;
					}
					if (empty)
						empty = 0;
				}

				if (dump_row) {
					free(dump_row);
					dump_row = NULL;
				}

				break;
			}

			while (!result && is_objkey_printable && wavelet_coeff) {
				number_of_rows = jjj = 0;
				current_limit = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit;

				num_fields =
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id ? 1 : 0) +
				    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1) * ed + fr_ed + 1;

				// Header
				jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonkeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					jsonkeys[jjj] = strdup(OPH_EXPLORENC_ID);
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[iii] ? dimension_name[iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						snprintf(jsontmp, OPH_COMMON_BUFFER_LEN, "INDEX OF %s", dimension_name[iii] ? dimension_name[iii] : "");
						jsonkeys[jjj] = strdup(jsontmp);
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					jsonkeys[jjj] = strdup(dimension_name[ed + iii] ? dimension_name[ed + iii] : "");
					if (!jsonkeys[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
						for (iii = 0; iii < jjj; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				jsonkeys[jjj] = strdup("Wavelet coefficients");
				if (!jsonkeys[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
					for (iii = 0; iii < jjj; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				jjj = 0;
				fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
				if (!fieldtypes) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
					fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				for (iii = 0; (iii < ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					if (dimension_time[iii])
						fieldtypes[jjj] = strdup(OPH_JSON_STRING);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_INT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
					else if (!strncasecmp(dimension_type[iii], OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
						fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
					else
						fieldtypes[jjj] = 0;
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
						fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				for (iii = 0; (iii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ++iii) {
					fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// A simple 'ALL'
					if (!fieldtypes[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < jjj; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj++;
				}
				if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
					break;
				fieldtypes[jjj] = strdup(OPH_JSON_STRING);	// Array is a string in any case
				if (!fieldtypes[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < jjj; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_WAVELET_COEFF, "Wavelet coefficients", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonkeys[iii])
							free(jsonkeys[iii]);
					if (jsonkeys)
						free(jsonkeys);
					for (iii = 0; iii < num_fields; iii++)
						if (fieldtypes[iii])
							free(fieldtypes[iii]);
					if (fieldtypes)
						free(fieldtypes);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);

				// Row by row extraction
				while ((result == OPH_ANALYTICS_OPERATOR_SUCCESS) && (number_of_rows < total_number_of_rows) && (current_limit > 0)) {
					if (rows[number_of_rows]) {
						if (!row)
							row = (char *) malloc(row_size);
						if (!row) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(row, rows[number_of_rows], row_size);
					} else {
						if (row) {
							free(row);
							row = NULL;
						}
						if (oph_nc_get_row_from_nc(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid, array_length, measure, number_of_rows, &row)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to fetch row\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NC_OPEN_ERROR_NO_CONTAINER);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
					}
					if (!rows[number_of_rows] && row) {
						if (!row_size)
							switch (measure->vartype) {
								case NC_BYTE:
								case NC_CHAR:
									row_size = array_length * sizeof(char);
									break;
								case NC_SHORT:
									row_size = array_length * sizeof(short);
									break;
								case NC_INT:
									row_size = array_length * sizeof(int);
									break;
								case NC_INT64:
									row_size = array_length * sizeof(long long);
									break;
								case NC_FLOAT:
									row_size = array_length * sizeof(float);
									break;
								case NC_DOUBLE:
									row_size = array_length * sizeof(double);
									break;
								default:
									row_size = array_length * sizeof(double);
							}
						rows[number_of_rows] = (char *) malloc(row_size);
						if (!rows[number_of_rows]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_NO_CONTAINER, "", "rows");
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						memcpy(rows[number_of_rows], row, row_size);
					}

					jsonvalues = (char **) calloc(num_fields, sizeof(char *));
					if (!jsonvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					jjj = 0;

					jj = 0;
					_block_size = block_size;
					_index_dim = number_of_rows;
					for (ii = 0; (ii < ed + fr_ed + 1) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						if (ii) {
							kk = measure->dims_order[ii - 1];

							if (!dim_rows[kk] || !dim_inst[kk].size)
								continue;
							else {
								_block_size /= dim_inst[kk].size;
								index_dim = _index_dim / _block_size;
								_index_dim %= _block_size;

								if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
									if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), jsontmp)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
										result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
										break;
									}
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%s", jsontmp);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
								} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
									dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
									snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED,
										dims[kk].dimension_type);
									result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
									break;
								}
							}

							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", index_dim + 1);
								jsonvalues[jjj] = strdup(tmp_value);
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj++;
							}
						} else if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_id) {
							snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", number_of_rows + 1);
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						jj++;
					}
					for (ii = 0; (ii < fr_ed) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); ii++) {
						jsonvalues[jjj] = strdup(OPH_COMMON_FULL_REDUCED_DIM);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
					}
					if (result != OPH_ANALYTICS_OPERATOR_SUCCESS)
						break;

					switch (measure->vartype) {
						case NC_BYTE:
						case NC_CHAR:
							dim_b = (char *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_b[k];
							wrow = row_d;
							break;
						case NC_SHORT:
							dim_s = (short *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_s[k];
							wrow = row_d;
							break;
						case NC_INT:
							dim_i = (int *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_i[k];
							wrow = row_d;
							break;
						case NC_INT64:
							dim_l = (long long *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_l[k];
							wrow = row_d;
							break;
						case NC_FLOAT:
							dim_f = (float *) row;
							for (k = 0; k < array_length; ++k)
								row_d[k] = (double) dim_f[k];
							wrow = row_d;
							break;
						case NC_DOUBLE:
							wrow = (double *) row;
							break;
						default:
							wrow = (double *) row;
					}
					int expand = spline_num_points > array_length;
					if (expand)	// Interpolation to 2-power
					{
						// wrow -> wi_row
						spline_coeff = gsl_spline_alloc(gsl_interp_cspline, array_length);
						if (!spline_coeff) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						acc_coeff = gsl_interp_accel_alloc();
						if (!acc_coeff) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
							break;
						}
						if (gsl_spline_init(spline_coeff, dim_dd, wrow, array_length)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_INTERPOLATION_ERROR);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						for (k = 0; k < spline_num_points; ++k)
							wi_row[k] = gsl_spline_eval(spline_coeff, wi_dim[k], acc_coeff);
						if (spline_coeff) {
							gsl_spline_free(spline_coeff);
							spline_coeff = NULL;
						}
						if (acc_coeff) {
							gsl_interp_accel_free(acc_coeff);
							acc_coeff = NULL;
						}
					}
					if (gsl_wavelet_transform_forward(w, expand ? wi_row : wrow, 1, spline_num_points, work)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_WAVELET_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					max = spline_num_points * (OPH_COMMON_MAX_DOUBLE_LENGHT + 2);
					dump_row = (char *) malloc(max);
					*dump_row = 0;
					for (k = 0; k < spline_num_points; ++k) {
						snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%.*f%s", OPH_EXPLORENC_PRECISION, expand ? wi_row[k] : wrow[k], k < spline_num_points - 1 ? ", " : "");
						if (max - strlen(dump_row) > 1)
							strncat(dump_row, tmp_value, max - strlen(dump_row));
						else
							break;
					}
					if (result)
						break;

					jsonvalues[jjj] = strdup(dump_row ? dump_row : "");
					if (!jsonvalues[jjj]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
						for (iii = 0; iii < jjj; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_WAVELET_COEFF, jsonvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
						logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					number_of_rows++;

					if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->limit)
						current_limit--;

					if (dump_row) {
						free(dump_row);
						dump_row = NULL;
					}
				}

				if (dump_row) {
					free(dump_row);
					dump_row = NULL;
				}

				break;
			}
		}

		if (!result) {
			char message[OPH_COMMON_BUFFER_LEN];
			snprintf(message, OPH_COMMON_BUFFER_LEN, "Selected %d row%s out of %d", number_of_rows, number_of_rows == 1 ? "" : "s", total_number_of_rows);
			if (oph_json_is_objkey_printable
			    (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
			     OPH_JSON_OBJKEY_EXPLORENC_SUMMARY)) {
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_SUMMARY, "Summary", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
		}

		if (!result && (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->level > 1)) {
			is_objkey_printable =
			    oph_json_is_objkey_printable(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
							 OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES);
			if (is_objkey_printable) {
				num_fields = ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index ? 2 : 1;
				int success = 1;

				for (kk = 0; kk < number_of_dimensions; kk++) {
					if (!dim_rows[kk] || !dim_inst[kk].size)
						continue;	// Don't show fully reduced dimensions

					success = 0;
					while (!success) {
						// Header
						jjj = 0;
						jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonkeys) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
							jsonkeys[jjj] = strdup("INDEX");
							if (!jsonkeys[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
								for (iii = 0; iii < jjj; iii++)
									if (jsonkeys[iii])
										free(jsonkeys[iii]);
								if (jsonkeys)
									free(jsonkeys);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						jsonkeys[jjj] = strdup("VALUE");
						if (!jsonkeys[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
							for (iii = 0; iii < jjj; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						jjj = 0;
						fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
						if (!fieldtypes) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
							fieldtypes[jjj] = strdup(OPH_JSON_LONG);
							if (!fieldtypes[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
								for (iii = 0; iii < num_fields; iii++)
									if (jsonkeys[iii])
										free(jsonkeys[iii]);
								if (jsonkeys)
									free(jsonkeys);
								for (iii = 0; iii < jjj; iii++)
									if (fieldtypes[iii])
										free(fieldtypes[iii]);
								if (fieldtypes)
									free(fieldtypes);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}
							jjj++;
						}
						if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar))
							fieldtypes[jjj] = strdup(OPH_JSON_STRING);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_INT);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_LONG);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_SHORT);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_BYTE);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_FLOAT);
						else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
							fieldtypes[jjj] = strdup(OPH_JSON_DOUBLE);
						else
							fieldtypes[jjj] = strdup(OPH_JSON_STRING);
						if (!fieldtypes[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < jjj; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						if (oph_json_add_grid
						    (handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES, dims[kk].dimension_name, NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonkeys[iii])
									free(jsonkeys[iii]);
							if (jsonkeys)
								free(jsonkeys);
							for (iii = 0; iii < num_fields; iii++)
								if (fieldtypes[iii])
									free(fieldtypes[iii]);
							if (fieldtypes)
								free(fieldtypes);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						for (iii = 0; iii < num_fields; iii++)
							if (jsonkeys[iii])
								free(jsonkeys[iii]);
						if (jsonkeys)
							free(jsonkeys);
						for (iii = 0; iii < num_fields; iii++)
							if (fieldtypes[iii])
								free(fieldtypes[iii]);
						if (fieldtypes)
							free(fieldtypes);

						success = 1;
					}
					if (!success)
						break;

					for (ii = 0; ii < dim_inst[kk].size; ii++) {
						success = 0;
						while (!success) {
							jjj = 0;
							jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
							if (!jsonvalues) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}

							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_index) {
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", ii + 1);
								jsonvalues[jjj] = strdup(tmp_value);
								if (!jsonvalues[jjj]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
								jjj++;
							}

							index_dim = ii;
							if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_time && dims[kk].calendar && strlen(dims[kk].calendar)) {
								if (oph_dim_get_time_string_of(dim_rows[kk], index_dim, &(dims[kk]), tmp_value)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in handling time dimension\n");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_DIM_READ_ERROR);
									for (iii = 0; iii < jjj; iii++)
										if (jsonvalues[iii])
											free(jsonvalues[iii]);
									if (jsonvalues)
										free(jsonvalues);
									result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
									break;
								}
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_INT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_i = (int *) (dim_rows[kk] + index_dim * sizeof(int));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_i);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_LONG_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_l = (long long *) (dim_rows[kk] + index_dim * sizeof(long long));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *dim_l);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_SHORT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_s = (short *) (dim_rows[kk] + index_dim * sizeof(short));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_s);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_BYTE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_b = (char *) (dim_rows[kk] + index_dim * sizeof(char));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *dim_b);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_FLOAT_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_f = (float *) (dim_rows[kk] + index_dim * sizeof(float));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_f);
							} else if (!strncasecmp(dims[kk].dimension_type, OPH_COMMON_DOUBLE_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE)) {
								dim_d = (double *) (dim_rows[kk] + index_dim * sizeof(double));
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *dim_d);
							} else {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Type not supported.\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_TYPE_NOT_SUPPORTED, dims[kk].dimension_type);
								result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
								break;
							}
							jsonvalues[jjj] = strdup(tmp_value);
							if (!jsonvalues[jjj]) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
								for (iii = 0; iii < jjj; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}

							if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES, jsonvalues)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
								logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
								for (iii = 0; iii < num_fields; iii++)
									if (jsonvalues[iii])
										free(jsonvalues[iii]);
								if (jsonvalues)
									free(jsonvalues);
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
							}

							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);

							success = 1;
						}
						if (!success)
							break;
					}
					if (!success)
						break;
				}
			}
		}

		if (show_stats) {
			if (row_dd) {
				free(row_dd);
				row_dd = NULL;
			}
			if (stats_name) {
				for (k = 0; k < stats_size; ++k)
					if (stats_name[k])
						free(stats_name[k]);
				free(stats_name);
				stats_name = NULL;
			}
		}
		if (imp_num_points > 0) {
			if (spline) {
				gsl_spline_free(spline);
				spline = NULL;
			}
			if (acc) {
				gsl_interp_accel_free(acc);
				acc = NULL;
			}
			if (dim_dd) {
				free(dim_dd);
				dim_dd = NULL;
			}
			if (row_d) {
				free(row_d);
				row_d = NULL;
			}
			if (new_dim) {
				free(new_dim);
				new_dim = NULL;
			}
			if (accumulator) {
				free(accumulator);
				accumulator = NULL;
			}
		}
		if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->show_fit > 0) {
			if (dim_dd) {
				free(dim_dd);
				dim_dd = NULL;
			}
			if (row_d) {
				free(row_d);
				row_d = NULL;
			}
		}
		if (evaluate_wavelet) {
			if (spline_forward) {
				gsl_spline_free(spline_forward);
				spline_forward = NULL;
			}
			if (spline_inverse) {
				gsl_spline_free(spline_inverse);
				spline_inverse = NULL;
			}
			if (spline_coeff) {
				gsl_spline_free(spline_coeff);
				spline_coeff = NULL;
			}
			if (acc) {
				gsl_interp_accel_free(acc);
				acc = NULL;
			}
			if (acc_wavelet) {
				gsl_interp_accel_free(acc_wavelet);
				acc_wavelet = NULL;
			}
			if (acc_coeff) {
				gsl_interp_accel_free(acc_coeff);
				acc_coeff = NULL;
			}
			if (w) {
				gsl_wavelet_free(w);
				w = NULL;
			}
			if (work) {
				gsl_wavelet_workspace_free(work);
				work = NULL;
			}
			if (wcoeff) {
				free(wcoeff);
				wcoeff = NULL;
			}
			if (ppp) {
				free(ppp);
				ppp = NULL;
			}
			if (wi_dim) {
				free(wi_dim);
				wi_dim = NULL;
			}
			if (wi_row) {
				free(wi_row);
				wi_row = NULL;
			}
			if (dim_dd) {
				free(dim_dd);
				dim_dd = NULL;
			}
			if (row_d) {
				free(row_d);
				row_d = NULL;
			}
		}

		if (rows) {
			for (k = 0; k < est_num_rows; ++k)
				if (rows[k])
					free(rows[k]);
			free(rows);
		}
		if (row) {
			free(row);
			row = NULL;
		}

	} else {

		int success = 1, j, natts, ndims, nvars;
		char key[OPH_COMMON_BUFFER_LEN], key_type[OPH_COMMON_TYPE_SIZE], value[OPH_COMMON_BUFFER_LEN], variable[OPH_COMMON_BUFFER_LEN], **dims = NULL, **vars = NULL;
		nc_type xtype;

		is_objkey_printable = !result && success
		    && oph_json_is_objkey_printable(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
						    OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES);
		while (is_objkey_printable) {

			num_fields = 3;

			// Header
			jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jsonkeys[jjj] = strdup("DIMENSION");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("SIZE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("UNLIMITED");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES, "Dimension list", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (iii = 0; iii < num_fields; iii++)
				if (fieldtypes[iii])
					free(fieldtypes[iii]);
			if (fieldtypes)
				free(fieldtypes);

			// Data
			int recid;
			size_t size;
			if (nc_inq_unlimdim(ncid, &recid)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get unlimited dimension\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get unlimited dimension\n");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if (nc_inq_ndims(ncid, &ndims)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of dimensions\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get number of dimensions\n");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			if (ndims)
				dims = (char **) calloc(ndims, sizeof(char *));

			for (j = 0; j < ndims; j++) {

				if (nc_inq_dim(ncid, j, key, &size)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get dimension name\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get dimension name\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				dims[j] = strdup(key);

				jjj = 0;
				jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jsonvalues[jjj] = strdup(key);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				snprintf(tmp_value, OPH_COMMON_TYPE_SIZE, "%d", (int) size);
				jsonvalues[jjj] = strdup(tmp_value);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonvalues[jjj] = strdup(j == recid ? "yes" : "no");
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DIMVALUES, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);

			}

			if (!success) {
				char message[OPH_COMMON_BUFFER_LEN];
				snprintf(message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve dimension sizes");
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_SUMMARY, "Error", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}

			break;
		}

		is_objkey_printable = !result && success
		    && oph_json_is_objkey_printable(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
						    OPH_JSON_OBJKEY_EXPLORENC_DATA);
		while (is_objkey_printable) {

			num_fields = 3;

			// Header
			jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jsonkeys[jjj] = strdup("VARIABLE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("TYPE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("DIMENSIONS");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DATA, "Variable list", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (iii = 0; iii < num_fields; iii++)
				if (fieldtypes[iii])
					free(fieldtypes[iii]);
			if (fieldtypes)
				free(fieldtypes);

			// Data
			if (nc_inq_nvars(ncid, &nvars)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of dimensions\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get number of dimensions\n");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			if (nvars)
				vars = (char **) calloc(nvars, sizeof(char *));

			for (j = 0; j < nvars; j++) {

				if (nc_inq_varname(ncid, j, variable)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get variable name\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get variable name\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				vars[j] = strdup(variable);
				if (nc_inq_vartype(ncid, j, &xtype)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get variable name\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get variable name\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				switch (xtype) {
					case NC_CHAR:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_METADATA_TYPE_TEXT);
						break;
					case NC_BYTE:
					case NC_UBYTE:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
						break;
					case NC_SHORT:
					case NC_USHORT:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
						break;
					case NC_INT:
					case NC_UINT:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
						break;
					case NC_UINT64:
					case NC_INT64:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
						break;
					case NC_FLOAT:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
						break;
					case NC_DOUBLE:
						snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
						break;
					default:
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get variable type\n");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get variable type\n");
						*key_type = 0;
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
				}
				if (!strlen(key_type))
					break;
				if (nc_inq_varndims(ncid, j, &natts)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of dimensions of variable '%s'\n", vars[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get number of dimension of variable '%s'\n", vars[j]);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				*tmp_value = 0;
				if (natts) {

					int var_dims[natts], n = 0;
					if (nc_inq_vardimid(ncid, j, var_dims)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get dimensions of variable '%s'\n", vars[j]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get dimension of variable '%s'\n", vars[j]);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
					for (i = 0; i < natts; i++)
						n += snprintf(tmp_value + n, OPH_COMMON_BUFFER_LEN - n, "%s%s", i ? "|" : "", dims[var_dims[i]]);
				}


				jjj = 0;
				jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
				if (!jsonvalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jsonvalues[jjj] = strdup(variable);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonvalues[jjj] = strdup(key_type);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				jjj++;
				jsonvalues[jjj] = strdup(tmp_value);
				if (!jsonvalues[jjj]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
					for (iii = 0; iii < jjj; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_DATA, jsonvalues)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
					logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
					for (iii = 0; iii < num_fields; iii++)
						if (jsonvalues[iii])
							free(jsonvalues[iii]);
					if (jsonvalues)
						free(jsonvalues);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				for (iii = 0; iii < num_fields; iii++)
					if (jsonvalues[iii])
						free(jsonvalues[iii]);
				if (jsonvalues)
					free(jsonvalues);

			}

			if (!success) {
				char message[OPH_COMMON_BUFFER_LEN];
				snprintf(message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve variable information");
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_SUMMARY, "Error", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}

			break;
		}

		is_objkey_printable = !result && success
		    && oph_json_is_objkey_printable(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num,
						    OPH_JSON_OBJKEY_EXPLORENC_METADATA);
		while (is_objkey_printable) {

			num_fields = 4;

			// Header
			jjj = 0;
			jsonkeys = (char **) malloc(sizeof(char *) * num_fields);
			if (!jsonkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "keys");
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jsonkeys[jjj] = strdup("VARIABLE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("KEY");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("TYPE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			jsonkeys[jjj] = strdup("VALUE");
			if (!jsonkeys[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "key");
				for (iii = 0; iii < jjj; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			jjj = 0;
			fieldtypes = (char **) malloc(sizeof(char *) * num_fields);
			if (!fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtypes");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			jjj++;
			fieldtypes[jjj] = strdup(OPH_JSON_STRING);
			if (!fieldtypes[jjj]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "fieldtype");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < jjj; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			if (oph_json_add_grid(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_METADATA, "Metadata list", NULL, jsonkeys, num_fields, fieldtypes, num_fields)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID error\n");
				logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID error\n");
				for (iii = 0; iii < num_fields; iii++)
					if (jsonkeys[iii])
						free(jsonkeys[iii]);
				if (jsonkeys)
					free(jsonkeys);
				for (iii = 0; iii < num_fields; iii++)
					if (fieldtypes[iii])
						free(fieldtypes[iii]);
				if (fieldtypes)
					free(fieldtypes);
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			for (iii = 0; iii < num_fields; iii++)
				if (jsonkeys[iii])
					free(jsonkeys[iii]);
			if (jsonkeys)
				free(jsonkeys);
			for (iii = 0; iii < num_fields; iii++)
				if (fieldtypes[iii])
					free(fieldtypes[iii]);
			if (fieldtypes)
				free(fieldtypes);

			// Data
			int varid;
			char variable_attribute;
			for (j = 0; j <= nvars; j++) {

				variable_attribute = j < nvars;
				varid = variable_attribute ? j : NC_GLOBAL;

				natts = 0;
				if (nc_inq_varnatts(ncid, varid, &natts)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of global attributes\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get number of global attributes\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				for (i = 0; i < natts; i++) {

					success = 0;
					while (!success) {

						if (nc_inq_attname(ncid, varid, i, key)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute name\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get attribute name\n");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						if (nc_inq_atttype(ncid, varid, key, &xtype)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute type\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get attribute type\n");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						switch (xtype) {
							case NC_CHAR:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_METADATA_TYPE_TEXT);
								break;
							case NC_BYTE:
							case NC_UBYTE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_BYTE_TYPE);
								break;
							case NC_SHORT:
							case NC_USHORT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_SHORT_TYPE);
								break;
							case NC_INT:
							case NC_UINT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_INT_TYPE);
								break;
							case NC_UINT64:
							case NC_INT64:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_LONG_TYPE);
								break;
							case NC_FLOAT:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_FLOAT_TYPE);
								break;
							case NC_DOUBLE:
								snprintf(key_type, OPH_COMMON_TYPE_SIZE, OPH_COMMON_DOUBLE_TYPE);
								break;
							default:
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute type\n");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get attribute type\n");
								*key_type = 0;
								result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
								break;
						}
						if (!strlen(key_type))
							break;
						memset(value, 0, OPH_COMMON_BUFFER_LEN);
						if (nc_get_att(ncid, varid, (const char *) key, (void *) &value)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get attribute value from file\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Unable to get attribute value from file\n");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						switch (xtype) {
							case NC_BYTE:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((char *) value));
								break;
							case NC_UBYTE:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned char *) value));
								break;
							case NC_SHORT:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((short *) value));
								break;
							case NC_USHORT:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned short *) value));
								break;
							case NC_INT:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((int *) value));
								break;
							case NC_UINT:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%d", *((unsigned int *) value));
								break;
							case NC_UINT64:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *((unsigned long long *) value));
								break;
							case NC_INT64:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%lld", *((long long *) value));
								break;
							case NC_FLOAT:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *((float *) value));
								break;
							case NC_DOUBLE:
								snprintf(tmp_value, OPH_COMMON_BUFFER_LEN, "%f", *((double *) value));
								break;
							default:
								strcpy(tmp_value, value);
						}
						jjj = 0;
						jsonvalues = (char **) malloc(sizeof(char *) * num_fields);
						if (!jsonvalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "values");
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jsonvalues[jjj] = strdup(variable_attribute ? vars[j] : "");
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(key);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(key_type);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}
						jjj++;
						jsonvalues[jjj] = strdup(tmp_value);
						if (!jsonvalues[jjj]) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_MEMORY_ERROR_INPUT, "value");
							for (iii = 0; iii < jjj; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						if (oph_json_add_grid_row(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_METADATA, jsonvalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD GRID ROW error\n");
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD GRID ROW error\n");
							for (iii = 0; iii < num_fields; iii++)
								if (jsonvalues[iii])
									free(jsonvalues[iii]);
							if (jsonvalues)
								free(jsonvalues);
							result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
							break;
						}

						for (iii = 0; iii < num_fields; iii++)
							if (jsonvalues[iii])
								free(jsonvalues[iii]);
						if (jsonvalues)
							free(jsonvalues);

						success = 1;
					}
					if (!success)
						break;
				}
				if (!success)
					break;
			}

			if (!success) {
				char message[OPH_COMMON_BUFFER_LEN];
				snprintf(message, OPH_COMMON_BUFFER_LEN, "Unable to retrieve every metadata");
				if (oph_json_add_text(handle->operator_json, OPH_JSON_OBJKEY_EXPLORENC_SUMMARY, "Error", message)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "ADD TEXT error\n");
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}

			break;
		}

		if (dims) {
			for (j = 0; j < ndims; j++)
				if (dims[j])
					free(dims[j]);
			free(dims);
		}
		if (vars) {
			for (j = 0; j < nvars; j++)
				if (vars[j])
					free(vars[j]);
			free(vars);
		}
	}

	return result;
}

int task_reduce(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct * handle)
{
	if (!handle || !handle->operator_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_EXPLORENC_NULL_OPERATOR_HANDLE);
		return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
	}

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct * handle)
{
	//If NULL return success; it's already free
	if (!handle || !handle->operator_handle)
		return OPH_ANALYTICS_OPERATOR_SUCCESS;

	int i, retval;

	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path) {
		free((char *) ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path);
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->nc_file_path = NULL;
	}
	if ((retval = nc_close(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->ncid)))
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %s\n", nc_strerror(retval));

	NETCDF_var *measure = ((NETCDF_var *) & (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->measure));

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

	if (measure->dims_order) {
		free((char *) measure->dims_order);
		measure->dims_order = NULL;
	}

	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask) {
		free(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask);
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->stats_mask = NULL;
	}
	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays) {
		for (i = 0; i < measure->ndims; i++)
			if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays[i])
				free(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays[i]);
		free(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->dim_arrays);
	}
	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation) {
		free((char *) ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation);
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->operation = NULL;
	}
	if (((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys) {
		oph_tp_free_multiple_value_param_list(((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys, ((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys_num);
		((OPH_EXPLORENC_operator_handle *) handle->operator_handle)->objkeys = NULL;
	}
	free((OPH_EXPLORENC_operator_handle *) handle->operator_handle);
	handle->operator_handle = NULL;

	return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
