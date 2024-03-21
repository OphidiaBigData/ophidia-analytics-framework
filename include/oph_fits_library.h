/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of


    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __OPH_FITS_UTILITY_H
#define __OPH_FITS_UTILITY_H

//#include "netcdf.h"
#include "fitsio.h"
#include "oph_common.h"
#include "oph_datacube_library.h"
#include "oph_ioserver_library.h"

#define FITS_MAX_VAR_DIMS	99
#define OPH_FITS_BYTE_TYPE	OPH_COMMON_BYTE_TYPE	// related value 8
#define OPH_FITS_SHORT_TYPE	OPH_COMMON_SHORT_TYPE	// related value 16
#define OPH_FITS_INT_TYPE	OPH_COMMON_INT_TYPE	// related value 32
#define OPH_FITS_FLOAT_TYPE	OPH_COMMON_FLOAT_TYPE	// related value -32
#define OPH_FITS_DOUBLE_TYPE	OPH_COMMON_DOUBLE_TYPE	// related value -64
#define OPH_FITS_LONG_TYPE	OPH_COMMON_LONG_TYPE	// related value 64
#define OPH_FITS_BIT_TYPE	OPH_COMMON_BIT_TYPE	// Could be not supported

#define OPH_FITS_DOUBLE_FLAG    OPH_COMMON_DOUBLE_FLAG
#define OPH_FITS_FLOAT_FLAG     OPH_COMMON_FLOAT_FLAG
#define OPH_FITS_INT_FLAG       OPH_COMMON_INT_FLAG
#define OPH_FITS_LONG_FLAG	OPH_COMMON_LONG_FLAG
#define OPH_FITS_SHORT_FLAG	OPH_COMMON_SHORT_FLAG
#define OPH_FITS_BYTE_FLAG	OPH_COMMON_BYTE_FLAG


#define OPH_FITS_ERRCODE  2
// Managed locally
#define OPH_FITS_ERR(e)   {printf("Error: %s\n", e);}

#define OPH_FITS_ERROR 1
#define OPH_FITS_SUCCESS 0

//Structure used by OPH_IMPORTFITS operator
struct _FITS_var {
	int varid;		// FITS file hasn't varid. Use a sequential number
/* FITS file pointer, defined in fitsio.h */
	char varname[63 + 1];
	// Types are :8, 16, 32, 64, -32, -64
	int vartype;
	int varsize;
	int ndims;
	char **dims_name;
	int *dims_id;
	long *dims_length;
	short int *dims_type;	//Contains the type of the dimension (explicit = 1/implicit = 0) as a boolean value
	short int *dims_oph_level;	//Contains the oph_level of the dimensions (explicit and implicit)
	// For allowing subsetting during the import phase
	int *dims_start_index;	//Contains the start index for each dimension; it follows the dims_id array positionally
	int *dims_end_index;	//Contains the end index for each dimension; it follows the dims_id array positionally
	char *dims_concept_level;	//Contains the concept level of the dimensions (depends on hierarchy)
	short int nexp;
	short int nimp;
	short int *dims_order;
	int *oph_dims_id;
};
typedef struct _FITS_var FITS_var;

// Auxiliary functions

/**
 * \brief Internal recursive function used to compute dimension ID from tuple ID
 * \param residual Param used to store residual of computation
 * \param total Param used to store total of computation
 * \param sizemax Array containing the max values of dimension ids
 * \param id Output to be filled with the dimension ids related to tuple ID
 * \param i Dimension index
 * \param n Number of dimensions
 * \return 0 if successfull
 */
int _oph_fits_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, long **id, int i, int n);

/**
 * \brief Function used to compute dimension ID from tuple ID
 * \param ID Tuple ID
 * \param sizemax Array containing the max values of dimension ids
 * \param n Number of dimensions
 * \param id Output to be filled with the dimension ids related to tuple ID
 * \return 0 if successfull
 */
int oph_fits_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, long **id);

/**
 * \brief Populate a fragment with fits data (multi-insert version of previous function)
 * \param server Pointer to I/O server structure
 * \param frag Structure with information about fragment to be filled
 * \param fptr Pointer to fits file
 * \param tuplexfrag_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \return 0 if successfull
 */
int oph_fits_populate_fragment_from_fits2(oph_ioserver_handler * server, oph_odb_fragment * frag, fitsfile * fptr, int tuplexfrag_number, int array_length, int compressed, FITS_var * measure);

/**
 * \brief Populate a fragment with fits data (auto-drilldown version of previous function)
 * \param server Pointer to I/O server structure
 * \param frag Structure with information about fragment to be filled
 * \param fptr Pointer to fits file
 * \param tuplexfrag_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \param memory_size Value of maximum memory available
 * \return 0 if successfull
 */
int oph_fits_populate_fragment_from_fits3(oph_ioserver_handler * server, oph_odb_fragment * frag, fitsfile * fptr, int tuplexfrag_number, int array_length, int compressed, FITS_var * measure,
					  long long memory_size);

/**
 * \brief Return the C type given the fits_type
 * \param type_fits The type_fits to be converted
 * \param out_c_type String to be filled with the corresponding C type
 * \return 0 if successfull, -1 otherwise
 */
int oph_fits_get_c_type(int type_fits, char *out_c_type);

/**
 * \brief Return the type_fits given the C type
 * \param out_c_type String to with the C type to be converted
 * \param type_fits Output to be filled with the corresponding fits type
 * \return 0 if successfull, -1 otherwise
 */
int oph_fits_get_fits_type(char *in_c_type, int *type_fits);

/**
 * \brief Internal recursive function used to compute the subsequent fits dimension id
 * \param id Output with the fits id of the next dimensions
 * \param sizemax Array containing the max values of dimension ids
 * \param i Param used to count number of recursive iterations
 * \param n Number of dimensions
 * \return 0 if successfull, 1 otherwise
 */
int _oph_fits_get_next_fits_id(size_t *id, unsigned int *sizemax, int i, int n);

/**
 * \brief Function used to compute the subsequent fits dimension id
 * \param id Output with the fits id of the next dimensions
 * \param sizemax Array containing the max values of dimension ids
 * \param n Number of dimensions
 * \return 0 if successfull, 1 otherwise
 */
int oph_fits_get_next_fits_id(size_t *id, unsigned int *sizemax, int n);

/**
 * \brief Retrieve a dimension coordinated variable data from a FITS file allowing subsetting
 * \param id_container Id of output container (used by logging function)
 * \param dim_size Size of the dimension to be retrieved
 * \param dim_array Structure containing the dimension data read
 * \return 0 if successfull
 */
int oph_fits_get_dim_array2(int id_container, int dim_size, char **dim_array);

/**
 * \brief Compare fits type with c type
 * \param id_container Id of output container (used by logging function)
 * \param var_type FITS type
 * \param dim_type String with the c type
 * \return 0 if successfull
 */
int oph_fits_compare_fits_c_types(int id_container, int var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE]);

/**
 * \brief Retrieve FITS_var info from file
 * \param id_container Id of output container (used by logging function)
 * \param varname Name of the variable
 * \param dims_length Size of the dimensions
 * \param max_ndims Max number of dimensions allowed for variable
 * \param var Structure containing the variable info read
 * \param flag Flag to distinguish between variables and dimensions
 * \return 0 if successfull
 */
int oph_fits_get_fits_var(int id_container, char *varname, long *dims_length, FITS_var * var, short flag);

#endif				//__OPH_FITS_UTILITY_H
