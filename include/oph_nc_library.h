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

#ifndef __OPH_NC_UTILITY_H
#define __OPH_NC_UTILITY_H

#include "netcdf.h"
#include "oph_common.h"
#include "oph_datacube2_library.h"
#include "oph_ioserver_library.h"

#define OPH_NC_BYTE_TYPE	OPH_COMMON_BYTE_TYPE
#define OPH_NC_SHORT_TYPE	OPH_COMMON_SHORT_TYPE
#define OPH_NC_INT_TYPE		OPH_COMMON_INT_TYPE
#define OPH_NC_FLOAT_TYPE	OPH_COMMON_FLOAT_TYPE
#define OPH_NC_DOUBLE_TYPE	OPH_COMMON_DOUBLE_TYPE
#define OPH_NC_LONG_TYPE	OPH_COMMON_LONG_TYPE
#define OPH_NC_BIT_TYPE		OPH_COMMON_BIT_TYPE

#define OPH_NC_DOUBLE_FLAG      OPH_COMMON_DOUBLE_FLAG
#define OPH_NC_FLOAT_FLAG       OPH_COMMON_FLOAT_FLAG
#define OPH_NC_INT_FLAG         OPH_COMMON_INT_FLAG
#define OPH_NC_LONG_FLAG	OPH_COMMON_LONG_FLAG
#define OPH_NC_SHORT_FLAG	OPH_COMMON_SHORT_FLAG
#define OPH_NC_BYTE_FLAG	OPH_COMMON_BYTE_FLAG

#define OPH_NC_ERRCODE		2
#define OPH_NC_ERR(e)		{printf("Error: %s\n", nc_strerror(e));}

#define OPH_NC_ERROR		1
#define OPH_NC_SUCCESS		0

//Structure used by OPH_EXPORTNC operator
struct _NETCDF_dim {
	int dimid;
	char dimname[NC_MAX_NAME + 1];
	nc_type dimtype;
	int dimsize;
	int dimfkid;
	short int dimophlevel;	//Contains the oph_level of the dimension (explicit and implicit)
	short int dimexplicit;	// 1 if explicit, 0 if implicit dimension
	int dimfklabel;
	char dimunlimited;
};
typedef struct _NETCDF_dim NETCDF_dim;

//Structure used by OPH_IMPORTNC operator
struct _NETCDF_var {
	int varid;
	char varname[NC_MAX_NAME + 1];
	nc_type vartype;
	int varsize;
	int ndims;
	char **dims_name;
	int *dims_id;
	size_t *dims_length;
	short int *dims_type;	//Contains the type of the dimension (explicit = 1/implicit = 0) as a boolean value
	short int *dims_oph_level;	//Contains the oph_level of the dimensions (explicit and implicit)
	// For allowing subsetting during the import phaseù
	int *dims_start_index;	//Contains the start index for each dimension; it follows the dims_id array positionally
	int *dims_end_index;	//Contains the end index for each dimension; it follows the dims_id array positionally
	char *dims_concept_level;	//Contains the concept level of the dimensions (depends on hierarchy)
	//NETCDF_var *expdims;
	short int nexp;
	//NETCDF_coordvar *impdims;
	short int nimp;
	char *dims_unlim;
	short int *dims_order;
	int *oph_dims_id;
};
typedef struct _NETCDF_var NETCDF_var;

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
int _oph_nc_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, size_t ** id, int i, int n);

/**
 * \brief Function used to compute dimension ID from tuple ID
 * \param ID Tuple ID
 * \param sizemax Array containing the max values of dimension ids
 * \param n Number of dimensions
 * \param id Output to be filled with the dimension ids related to tuple ID
 * \return 0 if successfull
 */
int oph_nc_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, size_t ** id);

/**
 * \brief Populate a fragment with nc data
 * \param server Pointer to I/O server structure
 * \param frag Structure with information about fragment to be filled
 * \param ncid Id of nc file
 * \param tuplexfrag_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \return 0 if successfull
 */
int oph_nc_populate_fragment_from_nc(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure);

/**
 * \brief Populate a fragment with nc data (multi-insert version of previous function)
 * \param server Pointer to I/O server structure
 * \param frag Structure with information about fragment to be filled
 * \param ncid Id of nc file
 * \param tuplexfrag_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \return 0 if successfull
 */
int oph_nc_populate_fragment_from_nc2(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure);

/**
 * \brief Populate a fragment with nc data (auto-drilldown version of previous function)
 * \param server Pointer to I/O server structure
 * \param frag Structure with information about fragment to be filled
 * \param ncid Id of nc file
 * \param tuplexfrag_number Number of tuple to insert
 * \param array_length Number of elements to insert in a single row
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \param memory_size Value of maximum memory available
 * \return 0 if successfull
 */
int oph_nc_populate_fragment_from_nc3(oph_ioserver_handler * server, oph_odb_fragment * frag, int ncid, int tuplexfrag_number, int array_length, int compressed, NETCDF_var * measure,
				      long long memory_size);

/**
 * \brief Return the C type given the nc_type
 * \param nc_type The nc_type to be converted
 * \param out_c_type String to be filled with the corresponding C type
 * \return 0 if successfull, -1 otherwise
 */
int oph_nc_get_c_type(nc_type type_nc, char *out_c_type);

/**
 * \brief Return the nc_type given the C type
 * \param out_c_type String to with the C type to be converted
 * \param nc_type Output to be filled with the corresponding nc type
 * \return 0 if successfull, -1 otherwise
 */
int oph_nc_get_nc_type(char *in_c_type, nc_type * type_nc);

/**
 * \brief Internal recursive function used to compute the subsequent nc dimension id
 * \param id Output with the nc id of the next dimensions
 * \param sizemax Array containing the max values of dimension ids
 * \param i Param used to count number of recursive iterations
 * \param n Number of dimensions
 * \return 0 if successfull, 1 otherwise
 */
int _oph_nc_get_next_nc_id(size_t * id, unsigned int *sizemax, int i, int n);

/**
 * \brief Function used to compute the subsequent nc dimension id
 * \param id Output with the nc id of the next dimensions
 * \param sizemax Array containing the max values of dimension ids
 * \param n Number of dimensions
 * \return 0 if successfull, 1 otherwise
 */
int oph_nc_get_next_nc_id(size_t * id, unsigned int *sizemax, int n);

/**
 * \brief Append nc data to a fragment
 * \param server Pointer to I/O server structure
 * \param old_frag Structure with information about the old fragment
 * \param new_frag Structure with information about the new fragment
 * \param ncid Id of nc file
 * \param compressed If the data to insert is compressed (1) or not (0)
 * \param measure Structure containing measure data and information to be stored
 * \return 0 if successfull
 */
int oph_nc_append_fragment_from_nc(oph_ioserver_handler * server, oph_odb_fragment * old_frag, oph_odb_fragment * new_frag, int ncid, int compressed, NETCDF_var * measure);

/**
 * \brief Retrieve a dimension coordinated variable data from a NetCDF file
 * \param id_container Id of output container (used by logging function)
 * \param ncid Id of nc file
 * \param dim_id Id of the dimension variable to be read
 * \param dim_type String with the type of variable to be read
 * \param dim_size Size of the dimension to be retrieved
 * \param dim_array Structure containing the dimension data read
 * \return 0 if successfull
 */
int oph_nc_get_dim_array(int id_container, int ncid, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, char **dim_array);

/**
 * \brief Retrieve a dimension coordinated variable data from a NetCDF file allowing subsetting
 * \param id_container Id of output container (used by logging function)
 * \param ncid Id of nc file
 * \param dim_id Id of the dimension variable to be read
 * \param dim_type String with the type of variable to be read
 * \param dim_size Size of the dimension to be retrieved
 * \param start_index Start index (0 based) of the dimension to consider
 * \param end_index End index (0 based) of the dimension to consider
 * \param dim_array Structure containing the dimension data read
 * \return 0 if successfull
 */
int oph_nc_get_dim_array2(int id_container, int ncid, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, int start_index, int end_index, char **dim_array);

/**
 * \brief Retrieve the index of a coordinated variable using its value from a NetCDF file
 * \param id_container Id of output container (used by logging function)
 * \param ncid Id of nc file
 * \param dim_id Id of the dimension variable to be read
 * \param dim_type Type of variable to be read (nc_type)
 * \param dim_size Size of the dimension to consider
 * \param value String that contains the numeric value
 * \param want_start 1 if the value I'm searching for is the start index, 0 otherwise
 * \param offset Added to bounds of subset intervals to extend them
 * \param order Return value containing 1 if the order of the dimension values is ascending, 0 otherwise
 * \param coord_index Index of the first value greater than "value"
 * \return 0 if successfull
 */
int oph_nc_index_by_value(int id_container, int ncid, int dim_id, nc_type dim_type, int dim_size, char *value, int want_start, double offset, int *order, int *coord_index);

/**
 * \brief Compare nc type with c type
 * \param id_container Id of output container (used by logging function)
 * \param var_type Nc type
 * \param dim_type String with the c type
 * \return 0 if successfull
 */
int oph_nc_compare_nc_c_types(int id_container, nc_type var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE]);

/**
 * \brief Retrieve NETCDF_var info from file
 * \param id_container Id of output container (used by logging function)
 * \param ncid Id of nc file
 * \param var_name Name of the variable to be retrieved
 * \param max_ndims Max number of dimensions allowed for variable
 * \param var Structure containing the variable info read
 * \return 0 if successfull
 */
int oph_nc_get_nc_var(int id_container, const char var_name[OPH_ODB_CUBE_MEASURE_SIZE], int ncid, int max_ndims, NETCDF_var * var);

/**
 * \brief Extract a row from nc file
 * \param ncid Id of nc file
 * \param array_length Number of elements to insert in a single row
 * \param measure Structure containing measure data and information to be stored
 * \param idDim Row index
 * \param row Pointer to row to be extracted; it must be freed
 * \return 0 if successfull
 */
int oph_nc_get_row_from_nc(int ncid, int array_length, NETCDF_var * measure, unsigned long idDim, char **row);

int update_dim_with_nc_metadata(ophidiadb * oDB, oph_odb_dimension * time_dim, int id_vocabulary, int id_container_out, int ncid);

int check_subset_string(char *curfilter, int i, NETCDF_var * measure, int is_index, int ncid, double offset);

#endif				//__OPH_NC_UTILITY_H
