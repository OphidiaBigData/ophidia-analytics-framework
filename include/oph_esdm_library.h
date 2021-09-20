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

#ifndef __OPH_ESDM_UTILITY_H
#define __OPH_ESDM_UTILITY_H

#include <esdm.h>

#include "oph_common.h"
#include "oph_datacube_library.h"
#include "oph_ioserver_library.h"

#ifndef OPH_ESDM_PREFIX
#define OPH_ESDM_PREFIX			"esdm://"
#endif

#define OPH_ESDM_DOUBLE_FLAG	OPH_COMMON_DOUBLE_FLAG
#define OPH_ESDM_FLOAT_FLAG		OPH_COMMON_FLOAT_FLAG
#define OPH_ESDM_INT_FLAG		OPH_COMMON_INT_FLAG
#define OPH_ESDM_LONG_FLAG		OPH_COMMON_LONG_FLAG
#define OPH_ESDM_SHORT_FLAG		OPH_COMMON_SHORT_FLAG
#define OPH_ESDM_BYTE_FLAG		OPH_COMMON_BYTE_FLAG

#define OPH_ESDM_MEMORY_BLOCK	1048576
#define OPH_ESDM_BLOCK_SIZE		524288	// Maximum size that could be transfered
#define OPH_ESDM_BLOCK_ROWS		1000	// Maximum number of lines that could be transfered

#define OPH_ESDM_SKIP_ATTRIBUTES
#define _NC_BOUNDS				"bounds"
#define _NC_PROPERTIES			"_NCProperties"
#define _NC_DIMS				"_nc_dims"
#define _NC_SIZES				"_nc_sizes"

#define OPH_IOSERVER_SQ_OP_ESDM_IMPORT 	"esdm_import"
#define OPH_DC_SQ_CREATE_FRAG_FROM_ESDM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_ESDM_IMPORT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_COLUMN_NAME, "id_dim|measure") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_COLUMN_TYPE, "long|blob") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_PATH, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_MEASURE, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_COMPRESSED, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_NROW, "%d") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_ROW_START, "%d") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_DIM_TYPE, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_DIM_INDEX, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_DIM_START, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_DIM_END, "%s")

struct _ESDM_var {
	esdm_container_t *container;
	esdm_dataset_t *dataset;
	esdm_dataspace_t *dspace;
	esdm_dataset_t **dim_dataset;
	esdm_dataspace_t **dim_dspace;
	char varname[OPH_ODB_DIM_DIMENSION_SIZE + 1];
	char vartype[OPH_ODB_DIM_DIMENSION_TYPE_SIZE + 1];
	int varsize;
	int ndims;
	char const *const *dims_name;
	int *dims_id;
	size_t *dims_length;
	short int *dims_type;	//Contains the type of the dimension (explicit = 1/implicit = 0) as a boolean value
	short int *dims_oph_level;	//Contains the oph_level of the dimensions (explicit and implicit)
	// For allowing subsetting during the import phase
	int *dims_start_index;	//Contains the start index for each dimension; it follows the dims_id array positionally
	int *dims_end_index;	//Contains the end index for each dimension; it follows the dims_id array positionally
	char *dims_concept_level;	//Contains the concept level of the dimensions (depends on hierarchy)
	short int nexp;
	short int nimp;
	char *dims_unlim;
	short int *dims_order;
	int *oph_dims_id;
	char *operation;
	char *args;
};
typedef struct _ESDM_var ESDM_var;

int oph_esdm_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, int64_t ** id);

int oph_esdm_get_next_id(int64_t * id, int64_t * sizemax, int n);

int oph_esdm_get_esdm_type(char *in_c_type, esdm_type_t * type_nc);

int oph_esdm_set_esdm_type(char *out_c_type, esdm_type_t type_nc);

int oph_esdm_compare_types(int id_container, esdm_type_t var_type, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE]);

int oph_esdm_update_dim_with_esdm_metadata(ophidiadb * oDB, oph_odb_dimension * time_dim, int id_vocabulary, int id_container_out, ESDM_var * measure);

int oph_esdm_get_dim_array(int id_container, esdm_dataset_t * dataset, int dim_id, const char dim_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE], int dim_size, int start_index, int end_index,
			   char **dim_array);

int oph_esdm_index_by_value(int id_container, ESDM_var * measure, int dim_id, esdm_type_t dim_type, int dim_size, char *value, int want_start, double offset, int *valorder, int *coord_index,
			    char out_of_bound);

int oph_esdm_check_subset_string(char *curfilter, int i, ESDM_var * measure, int is_index, double offset, char out_of_bound);

int oph_esdm_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var);

int oph_esdm_populate_fragment2(oph_ioserver_handler * server, oph_odb_fragment * frag, int tuplexfrag_number, int array_length, int compressed, ESDM_var * measure);

int oph_esdm_populate_fragment3(oph_ioserver_handler * server, oph_odb_fragment * frag, int tuplexfrag_number, int array_length, int compressed, ESDM_var * measure, long long memory_size);

int oph_esdm_populate_fragment5(oph_ioserver_handler * server, oph_odb_fragment * frag, char *nc_file_path, int tuplexfrag_number, int compressed, ESDM_var * measure);

#endif				//__OPH_ESDM_UTILITY_H
