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

#ifndef __OPH_ODB_META_H__
#define __OPH_ODB_META_H__

#include <mysql.h>

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_metadata_query.h"

#define OPH_ODB_META_QUERY_LEN (MYSQL_BUFLEN+2*1024*1024)
#define OPH_ODB_META_VALUE_LEN (1*1024*1024)

#define OPH_ODB_META_TEMPLATE_SEPARATOR ':'

/**
 * \brief Function to retrieve list of all keys related to a specific vocabulary
 * \param oDB Pointer to the OphidiaDB
 * \param id_vocabulary ID of the vocabulary to filter on; 0 is acceptable, but result is empty
 * \param information_list Pointer to MYSQL_RES result set (it has to be freed with mysql_free_result)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_find_metadatakey_list(ophidiadb * oDB, int vocabulary_id, MYSQL_RES ** information_list);

/**
 * \brief Function that updates OphidiaDB adding the new metadata instance and new manage relations
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube ID of the datacube to whom the metadata instance is related
 * \param id_metadatakey ID of the metadata key to whom the metadata instance is related or -1
 * \param new_metadatakey New metadata key in case of id_metadatakey = -1
 * \param new_metadatakey_variable New metadata key variable associated to new key
 * \param id_metadatatype ID of the metadata key type
 * \param id_user ID of the user managing the metadata
 * \param value Value of the metadatakey instance
 * \param last_insertd_id ID of last inserted metadatainstance
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_insert_into_metadatainstance_manage_tables(ophidiadb * oDB, const int id_datacube, const int id_metadatakey, const char *new_metadatakey, const char *new_metadatakey_variable,
							    const int id_metadatatype, const int id_user, const char *value, int *last_insertd_id);

/**
 * \brief Function to retrieve id of a vocabulary from its name
 * \param Pointer to OphidiaDB
 * \param vocabulary_name name of the vocabulary
 * \param id_vocabulary ID of the vocabulary
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_retrieve_vocabulary_id(ophidiadb * oDB, char *vocabulary_name, int *id_vocabulary);

/**
 * \brief Function to retrieve id of a vocabulary from a container id
 * \param oDB Pointer to OphidiaDB
 * \param id_container ID of the container
 * \param id_vocabulary ID of the vocabulary
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_retrieve_vocabulary_id_from_container(ophidiadb * oDB, int id_container, int *id_vocabulary);

/**
 * \brief Function to check if metadatainstance in a specified container exists
 * \param oDB Pointer to OphidiaDB
 * \param metadatainstance_id ID of the metadatainstance to be found
 * \param id_datacube Id of the datacube of the metadata instance to be found
 * \param exist It will be 1 if metadatainstance_id exists, 0 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_check_metadatainstance_existance(ophidiadb * oDB, int metadatainstance_id, int id_datacube, int *exists);

/**
 * \brief Function to retrive id of a metadata key
 * \param oDB Pointer to OphidiaDB
 * \param key_label Key to be found
 * \param key_variable Variable the key refers to (lat,lon,time,t2m...)
 * \param id_container Id of the container of the key to be found
 * \param add Flag indicating if anew metadata key has to be added in case the key is not found
 * \param id_metadatakey Id of the metadata key to be found, 0 if not found
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_retrieve_metadatakey_id(ophidiadb * oDB, char *key_label, char *key_variable, int id_container, int add, int *id_metadatakey);

/**
 * \brief Function to retrieve id of a metadatatype from its name (that is unique)
 * \param Pointer to OphidiaDB
 * \param metadatatype_name name of the metadatatype
 * \param id_metadatatype ID of the metadatatype retrieved
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_retrieve_metadatatype_id(ophidiadb * oDB, char *metadatatype_name, int *id_metadatatype);

/**
 * \brief Function to insert a metedata instance
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube Id of the container related to the instance
 * \param id_metadatakey Id of key of instance
 * \param id_metadatatype Id of type of instance
 * \param metadata_value Value of the instance
 * \param last_insertd_id Id of the insterted instance
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_insert_into_metadatainstance_table(ophidiadb * oDB, int id_datacube, int id_metadatakey, int id_metadatatype, char *metadata_value, int *last_insertd_id);

/**
 * \brief Function to insert a new row in manage table
 * \param oDB Pointer to OphidiaDB
 * \param id_metadatainstance Id of the instance related
 * \param id_user Id of the user related
 * \param last_insertd_id Id of the insterted manage row
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_insert_into_manage_table(ophidiadb * oDB, int id_metadatainstance, int id_user);

/**
 * \brief Function to retrieve list of all metadata information
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube Id of the datacube to filter
 * \param metadata_keys Array of metadata keys to filter on
 * \param metadata_keys_num Number of metadata keys to filter on
 * \param id_metadatainstance Id of the instance to filter
 * \param metadata_type Type of metadata to filter on
 * \param metadata_value Value of metadata to filter on
 * \param metadata_list MySQL result structure filled with results (it must be free'd outside the function)
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_find_complete_metadata_list(ophidiadb * oDB, int id_datacube, const char **metadata_keys, int metadata_keys_num, char *id_metadatainstance, char *metadata_type, char *metadata_value,
					     MYSQL_RES ** metadata_list);

/**
 * \brief Function to update the metadatainstance table
 * \param oDB Pointer to OphidiaDB
 * \param id_metadatainstance Id of the metadatainstance to update
 * \param id_datacube Id of the container of metadata instance to update
 * \param metadata_value Metadata instance value to insert
 * \param force Force update of functional metadata associated to vocabulary
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_update_metadatainstance_table(ophidiadb * oDB, int id_metadatainstance, int id_datacube, char *metadata_value, int force);

/**
 * \brief Function to delete a list of metadata instance
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube Id of the container to filter
 * \param metadata_keys Array of metadata keys to filter on
 * \param metadata_keys_num Number of metadata keys to filter on
 * \param id_metadatainstance Id of the instance to filter
 * \param force Force update of functional metadata associated to vocabulary
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_delete_from_metadatainstance_table(ophidiadb * oDB, int id_datacube, const char **metadata_keys, int metadata_keys_num, int id_metadatainstance, int force);

/**
 * \brief Function to copy the list of metadata instances of a cube to another cube
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube_input PID of old datacube
 * \param id_datacube_output PID of new datacube
 * \param id_user Id of the user that copies
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_copy_from_cube_to_cube(ophidiadb * oDB, int id_datacube_input, int id_datacube_output, int id_user);

/**
 * \brief Function to get time metadata identified by template
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube PID of old datacube
 * \param variable Name of the variable related to metadata, NULL for globals
 * \param template Name of the template
 * \param id_metadata_instance Index of the metadata read
 * \param value Value of the metadata
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_get(ophidiadb * oDB, int id_datacube, const char *variable, const char *template, int *id_metadata_instance, char **value);

/**
 * \brief Function to update time metadata identified by template
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube PID of old datacube
 * \param variable Name of the variable to be used in case of the metadata has to be created
 * \param template Name of the template
 * \param id_metadata_instance Index of the metadata to be override
 * \param value Value of the metadata
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_put(ophidiadb * oDB, int id_datacube, const char *variable, const char *template, int id_metadata_instance, const char *value);

/**
 * \brief Count the number of time metadata associated to a given dimension
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube PID of old datacube
 * \param dimenson_name Dimension name
 * \param count Number of metadata found
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_check_for_time_dimension(ophidiadb * oDB, int id_datacube, const char *dimension_name, int *count);

/**
 * \brief Delete the metadata keys specified for a datacube
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube PID of the datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_delete_keys_of_cube(ophidiadb * oDB, int id_datacube);

/**
 * \brief Function to correct the list of metadata keys of a cube having a different measure name
 * \param oDB Pointer to OphidiaDB
 * \param id_datacube PID of the datacube
 * \param old_variable Original value of parameter 'variable', used to find metadata to be changed
 * \param new_variable New value for parameter 'variable'
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_meta_update_metadatakeys(ophidiadb * oDB, int id_datacube, const char *old_variable, const char *new_variable);

#endif				/* __OPH_ODB_META_H__ */
