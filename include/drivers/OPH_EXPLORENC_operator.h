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

#ifndef __OPH_EXPLORENC_OPERATOR_H
#define __OPH_EXPLORENC_OPERATOR_H

//Operator specific headers
#include "oph_common.h"
#include "oph_ophidiadb_main.h"
#include "oph_nc_library.h"
#include "oph_ioserver_library.h"

#define OPH_EXPLORENC_ID "ID"
#define OPH_EXPLORENC_SUBSET_INDEX	"index"
#define OPH_EXPLORENC_SUBSET_COORD	"coord"
#define OPH_EXPLORENC_PRECISION 10

/**
 * \brief Structure of parameters needed by the operator OPH_EXPLORENC. It creates a new datacube filling it with data taken from nc file
 *
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param nc_file_path Path of the nc file to import
 * \param schedule_algo Number of the distribution algorithm to use
 * \param array_length Number of elements to store into a row
 * \param ncid File descriptor index
 * \param measure Measure name
 * \param number_of_dim Number of dimension
 * \param show_time Flag setted to 1 if the values of time dimensions have to be shown as string with date and time
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param sessionid SessionID
 * \param level Verbose level, used to enable the print of dimension values
 * \param imp_num_points Number of points which measure values must be distribute along by interpolation or reduction
 * \param wavelet Show wavelet filtered data
 * \param wavelet_ratio Fraction of wavelet transform coefficients that are cleared by the filter (percentage)
 * \param wavelet_coeff Show coefficients of wavelet filter
 * \param stats_mask Mask used for GSL stats
 * \param operation Operation to be applied in case of reduction
 * \param offset Offset to be used to set reduction interval bounds
 */
struct _OPH_EXPLORENC_operator_handle {
	ophidiadb oDB;
	char *nc_file_path;
	int schedule_algo;
	int ncid;
	NETCDF_var measure;
	char **dim_arrays;
	int number_of_dim;
	int limit;
	short int show_id;
	short int show_index;
	short int show_time;
	char **objkeys;
	int objkeys_num;
	char *sessionid;
	int level;
	int imp_num_points;
	int wavelet;
	double wavelet_ratio;
	int wavelet_coeff;
	char *stats_mask;
	int show_fit;
	char *operation;
	double offset;
};
typedef struct _OPH_EXPLORENC_operator_handle OPH_EXPLORENC_operator_handle;

#endif				//__OPH_EXPLORENC_OPERATOR_H
