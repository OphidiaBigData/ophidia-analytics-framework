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

#ifndef __OPH_ODB_DIM_H__
#define __OPH_ODB_DIM_H__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_dimension_query.h"


#define OPH_ODB_DIM_DIMENSION_SIZE 256
#define OPH_ODB_DIM_DIMENSION_TYPE_SIZE 64
#define OPH_ODB_DIM_GRID_SIZE 256
#define OPH_ODB_DIM_HIERARCHY_SIZE 64
#define OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE 256
#define OPH_ODB_DIM_TIME_SIZE 64

#define OPH_ODB_DIM_SECOND_NUMBER 60
#define OPH_ODB_DIM_MINUTE_NUMBER 60
#define OPH_ODB_DIM_HOUR_NUMBER 24
#define OPH_ODB_DIM_DAY_NUMBER 30
#define OPH_ODB_DIM_MONTH_NUMBER 12

#define OPH_ODB_TIME_FREQUENCY "time:frequency"
#define OPH_ODB_TIME_UNITS "time:units"
#define OPH_ODB_TIME_CALENDAR "time:calendar"
#define OPH_ODB_TIME_MONTH_LENGTHS "time:month_lengths"
#define OPH_ODB_TIME_LEAP_YEAR "time:leap_year"
#define OPH_ODB_TIME_LEAP_MONTH "time:leap_month"

#define OPH_DIM_INDEX_DATA_TYPE	OPH_COMMON_LONG_TYPE

#define OPH_DIM_TIME_UNITS_SECONDS "seconds"
#define OPH_DIM_TIME_UNITS_HOURS "hours"
#define OPH_DIM_TIME_UNITS_DAYS "days"

#define OPH_DIM_TIME_CALENDAR_STANDARD "standard"
#define OPH_DIM_TIME_CALENDAR_GREGORIAN "gregorian"
#define OPH_DIM_TIME_CALENDAR_PGREGORIAN "proleptic_gregorian"
#define OPH_DIM_TIME_CALENDAR_JULIAN "julian"
#define OPH_DIM_TIME_CALENDAR_360_DAY "360_day"
#define OPH_DIM_TIME_CALENDAR_NO_LEAP "no_leap"
#define OPH_DIM_TIME_CALENDAR_ALL_LEAP "all_leap"
#define OPH_DIM_TIME_CALENDAR_USER_DEFINED "user_defined"

#define OPH_DIM_TIME_UNITS_BASETIME_SEPARATOR "since"

#define OPH_DIM_TIME_CL_IS_TIME(cl) ((cl!=OPH_COMMON_BASE_CONCEPT_LEVEL) && (cl!=OPH_COMMON_CONCEPT_LEVEL_UNKNOWN))

#define OPH_DIM_STREAM_ELEMENTS 11
#define OPH_DIM_STREAM_LENGTH 64

/**
 * \brief Structure that contains a dimension
 * \param id_dimension id of the dimension
 * \param id_container id of the related container
 * \param dimension_name Name of the dimension
 * \param dimension_type Data type of the dimension
 * \param id_hierarchy ID of concept hierarchy in wich dimension belongs
 * \param base_time Base time used for time dimension
 * \param units Units of dimension time
 * \param calendar Calendar of dimension time
 * \param month_lengths Array of the lengths of each month of a year
 * \param leap_year The first leap year
 * \param leap_month The leap month
*/
typedef struct
{
	int id_dimension;
	int id_container;
	char dimension_name[OPH_ODB_DIM_DIMENSION_SIZE];
	char dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE];
	int id_hierarchy;
	char base_time[OPH_ODB_DIM_TIME_SIZE];
	char units[OPH_ODB_DIM_TIME_SIZE];
	char calendar[OPH_ODB_DIM_TIME_SIZE];
	int month_lengths[OPH_ODB_DIM_MONTH_NUMBER];
	int leap_year;
	int leap_month;
} oph_odb_dimension;

/**
 * \brief Structure that contains a dimension instance
 * \param id_dimensioninst id of the dimension instance
 * \param id_dimension id of the dimension
 * \param id_grid id of the dimension grid
 * \param size Length of the dimension
 * \param fk_id_dimension_index Virtual foreign key to dimension instance array
 * \param concept_level Concept level of dimension instance
 * \param unlimited Flag indicating if the dimension was unlimited in the original file
*/
typedef struct
{
	int id_dimensioninst;
	int id_dimension;
	int id_grid;
	int size;
	int fk_id_dimension_index;
	int fk_id_dimension_label;
	char concept_level;
	char unlimited;
} oph_odb_dimension_instance;

/**
 * \brief Structure that contains a grid
 * \param id_grid id of the grid
 * \param grid_name Name of the grid
 */
typedef struct
{
	int id_grid;
	char grid_name[OPH_ODB_DIM_GRID_SIZE];
} oph_odb_dimension_grid;

/**
 * \brief Structure that contains a hierarchy
 * \param id_hierarchy id of the hierarchy
 * \param hierarchy_name Name of the hierarchy
 * \param filename Filename of XML document that describes the hierarchy
 */
typedef struct
{
	int id_hierarchy;
	char hierarchy_name[OPH_ODB_DIM_HIERARCHY_SIZE];
	char filename[OPH_ODB_DIM_HIERARCHY_FILENAME_SIZE];
} oph_odb_hierarchy;

/**
 * \brief Function to retrieve all the values related to a dimension instance in OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param id_dimensioninst Id of the dimension instance to be found
 * \param dim Pointer to dimension
 * \param dim_insts Pointer to dimension instances struct
 * \param dim_grid Pointer to a grid struct
 * \param hier Pointer to a hierarchy struct
 * \param datacube_id Id of the datacube, used to access time 
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_full_dimension_info(ophidiadb *oDB, int id_dimensioninst, oph_odb_dimension *dim, oph_odb_dimension_instance *dim_inst, oph_odb_dimension_grid *dim_grid, oph_odb_hierarchy *hier, int id_datacube);

/**
 * \brief Function to retrieve dimensions from OphidiaDB given the id_dimension
 * \param oDB Pointer to the OphidiaDB
 * \param id_dimension ID of the dimension to be found
 * \param dim Pointer to dimension structure to be filled
 * \param datacube_id Id of the datacube, used to access time 
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_dimension(ophidiadb *oDB, int id_dimension, oph_odb_dimension *dim, int id_datacube);

/**
 * \brief Function to retrieve dimension instances from OphidiaDB given the id_dimensioninst
 * \param oDB Pointer to the OphidiaDB
 * \param id_dimensioninst ID of the dimension instance to be found
 * \param dim_inst Pointer to dimension_instance structure to be filled
 * \param datacube_id Id of the datacube, used to access time 
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_dimension_instance(ophidiadb *oDB, int id_dimensioninst, oph_odb_dimension_instance *dim_inst, int id_datacube);

/**
 * \brief Function to retrieve grid id from OphidiaDB given its name and the container id
 * \param oDB Pointer to the OphidiaDB
 * \param gridname Name of the grid to be found
 * \param id_container Id of the container related to the grid
 * \param id_grid Pointer to id_grid to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_grid_id(ophidiadb *oDB, char* gridname, int id_container, int* id_grid);

/**
 * \brief Function to retrieve hierarchy from OphidiaDB given the id_hierarchy
 * \param oDB Pointer to the OphidiaDB
 * \param id_hierarchy ID of the dimension to be found
 * \param hier Pointer to hierarchy structure to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_hierarchy(ophidiadb *oDB, int id_hierarchy, oph_odb_hierarchy *hier);

/**
 * \brief Function to retrieve dimension instance id from OphidiaDB given the datacube id and dimension name
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube Id of the datacube to be found
 * \param dimensionname Name of the dimension to be found
 * \param id_dimension_instance Pointer to id_dimension_instance to be filled
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_dimension_instance_id(ophidiadb *oDB, int id_datacube, char* dimensionname, int* id_dimension_instance);

/**
 * \brief Function to retrieve dimension rows OphidiaDB given the container id
 * \param oDB Pointer to the OphidiaDB
 * \param id_container ID of the container to be found
 * \param dims Pointer to dimension vector (it has to be freed)
 * \param dim_num Pointer to integer used to store the dimension number;
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_dimension_list_from_container(ophidiadb *oDB, int id_container, oph_odb_dimension **dims, int *dim_num);

/**
 * \brief Function to retrieve dimension rows and related instances given the grid name and container id
 * \param oDB Pointer to the OphidiaDB
 * \param grid_name Grid name used to retreive dimension
 * \param id_container ID of the container to be found
 * \param dims Pointer to dimension vector (it has to be freed)
 * \param dim_insts Pointer to dimension instances vector (it has to be freed)
 * \param dim_num Pointer to integer used to store the dimension number;
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_dimension_list_from_grid_in_container(ophidiadb *oDB, char *grid_name, int id_container, oph_odb_dimension **dims, oph_odb_dimension_instance **dim_insts, int *dim_num);

/**
 * \brief Function to retrieve all features of dimensions given the datacube id
 * \param oDB Pointer to the OphidiaDB
 * \param id_datacube ID of the datacube
 * \param frag_rows Pointer to MYSQL_RES instance (it has to be freed)
 * \param dim_num Pointer to integer used to store the dimension number;
 * \param datacube_id Id of the datacube, used to access time 
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_find_dimensions_features(ophidiadb *oDB, int id_datacube, MYSQL_RES **frag_rows, int *dim_num);

/**
 * \brief Function to retrieve list of all hierarchies
 * \param oDB Pointer to the OphidiaDB
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_find_hierarchy_list(ophidiadb *oDB, MYSQL_RES **information_list);

/**
 * \brief Function to retrieve list of all grids related to a container
 * \param oDB Pointer to the OphidiaDB
 * \param id_container ID of container to which the grid are related
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_find_container_grid_list(ophidiadb *oDB, int id_container, MYSQL_RES **information_list);

/**
 * \brief Function to delete grid informations from OphidiaDB
 * \param oDB pointer to the OphidiaDB
 * \param id_container ID of the container to whom the grids are related
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_delete_from_grid_table(ophidiadb *oDB, int id_container);

/**
 * \brief Function that updates OphidiaDB adding the new grid
 * \param oDB Pointer to OphidiaDB
 * \param grid Pointer to grid to be added
 * \param last_insertd_id ID of last inserted datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_insert_into_grid_table(ophidiadb *oDB, oph_odb_dimension_grid *grid, int *last_insertd_id);

/**
 * \brief Function that updates OphidiaDB adding the new dimension
 * \param oDB Pointer to OphidiaDB
 * \param cont Pointer to dimension to be added
 * \param last_insertd_id ID of last inserted dimension
 * \param datacube_id Id of the datacube, used to access time 
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_insert_into_dimension_table(ophidiadb *oDB, oph_odb_dimension *dim, int *last_insertd_id, int id_datacube);

/**
 * \brief Function that updates OphidiaDB adding the new dimension instance
 * \param oDB Pointer to OphidiaDB
 * \param cont Pointer to dimension instance to be added
 * \param last_insertd_id ID of last inserted dimension instance
 * \param datacube_id Id of the datacube, used to access time metadata
 * \param dimension_name Dimension name, used to access time metadata
 * \param frequency New value of frequency in case of time dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_insert_into_dimensioninstance_table(ophidiadb *oDB, oph_odb_dimension_instance *dim_inst, int *last_insertd_id, int id_datacube, const char* dimension_name, const char* frequency);

/**
 * \brief Function to retrieve id of a hieararchy from its name (that is unique)
 * \param Pointer to OphidiaDB
 * \param hierarchy_name name of the hieararchy
 * \param id_hierarchy ID of the hierarchy retrieved
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_hierarchy_id(ophidiadb *oDB, char *hierarchy_name, int *id_hierarchy);

/**
 * \brief Function to retrieve hierarchy and current concept level of a given dimension of a datacube
 * \param Pointer to OphidiaDB
 * \param datacube_id Id of the datacube
 * \param dimension_name Name of the dimension
 * \param hier Hierarchy associated with the dimension
 * \param concept_level Current concept level associated with the dimension
 * \param dimension_instance_id Return the dimension instance id associated with the dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_retrieve_hierarchy_from_dimension_of_datacube(ophidiadb *oDB, int datacube_id, const char* dimension_name, oph_odb_hierarchy *hier, char* concept_level, int* dimension_instance_id);

/**
 * \brief Function to assign time hierarchy to a given dimension of a datacube
 * \param Pointer to OphidiaDB
 * \param datacube_id Id of the datacube
 * \param dimension_name Name of the dimension
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_dim_set_time_dimension(ophidiadb *oDB, int id_datacube, char* dimension_name);

#endif /* __OPH_ODB_DIM_H__ */
