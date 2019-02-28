/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

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

#ifndef __OPH_ODB_FS_H__
#define __OPH_ODB_FS_H__

/* Project headers */
#include "oph_ophidiadb_library.h"
#include "oph_common.h"
#include "query/oph_odb_filesystem_query.h"

#define OPH_ODB_FS_CURRENT_DIR OPH_COMMON_CURRENT_DIR
#define OPH_ODB_FS_PARENT_DIR OPH_COMMON_PARENT_DIR
#define OPH_ODB_FS_FOLDER_SLASH "/"
#define OPH_ODB_FS_FOLDER_SLASH_CHAR '/'
#define OPH_ODB_FS_CONTAINER_SIZE 256
#define OPH_ODB_FS_CONTAINER_OPERATOR_SIZE 256
#define OPH_ODB_FS_ROOT "/"

#define OPH_ODB_FS_TASK_MULTIPLE_INPUT "MULTI"

#define OPH_ODB_FS_CHARS_NUM 14

#define OPH_ODB_FS_DESCRIPTION_SIZE 2048

/**
 * \brief Structure that contains a container
 * \param id_folder id of the folder
 * \param id_parent id of the dimension (may be 0)
 * \param container_name Name of the container
 * \param operation Describes the operation applied on the container
 */
typedef struct {
	int id_folder;
	int id_parent;
	char container_name[OPH_ODB_FS_CONTAINER_SIZE + 1];
	char operation[OPH_ODB_FS_CONTAINER_OPERATOR_SIZE + 1];
	int id_vocabulary;
	char description[OPH_ODB_FS_DESCRIPTION_SIZE + 1];
} oph_odb_container;

/**
 * \brief Function used to parse/control paths
 * \param inpath Absolute path or relative path with respect to cwd. It can be the string "".
 * \param cwd Absolute path representing the current working directory. It cannot be NULL and must start with "/".
 * \param folder_id Id of the folder indicated by the path "cwd/inpath" (leaf folder).
 * \param output_path Expanded absolute path of the folder indicated by the path "cwd/inpath". If NULL, the function will control the existence of the folder and return only its id.
 * \param oDB Pointer to the OphidiaDB
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_path_parsing(char *inpath, char *cwd, int *folder_id, char **output_path, ophidiadb * oDB);

/**
 * \brief Function used to retreive session home ID
 * \param sessionid SessionID of the session to be checked
 * \param oDB Pointer to the OphidiaDB
 * \param folder_id Pointer to session home ID
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_get_session_home_id(char *sessionid, ophidiadb * oDB, int *folder_id);

/**
 * \brief Function used to check if a folder is within session tree
 * \param folder_id Id of the folder to be checked
 * \param sessionid SessionID of the session to be checked
 * \param oDB Pointer to the OphidiaDB
 * \param status If the folder is within session tree it will be set to 1, otherwise it will be 0.
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_check_folder_session(int folder_id, char *sessionid, ophidiadb * oDB, int *status);

/**
 * \brief Function used to check if a folder is within session tree
 * \param container_id Id of the container to be checked
 * \param sessionid SessionID of the session to be checked
 * \param oDB Pointer to the OphidiaDB
 * \param status If the folder is within session tree it will be set to 1, otherwise it will be 0.
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_check_container_session(int container_id, char *sessionid, ophidiadb * oDB, int *status);

/**
 * \brief Function used to retrieve the folder id of a container
 * \param oDB Pointer to the OphidiaDB
 * \param container_id Id of the container
 * \param folder_id Id of the folder related to the container
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_retrive_container_folder_id(ophidiadb * oDB, int container_id, int *folder_id);

/**
 * \brief Function used to build backward path given the id of the leaf folder
 * \param folder_id Id of the leaf folder
 * \param oDB Pointer to the OphidiaDB
 * \param out_path Path to be built
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_build_path(int folder_id, ophidiadb * oDB, char (*out_path)[MYSQL_BUFLEN]);

/**
 * \brief Function used to split a generic path in dirname and basename (leaf folder/container)
 * \param input Input path
 * \param first_part Output dirname
 * \param last_token Output basename
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_str_last_token(const char *input, char **first_part, char **last_token);

/**
 * \brief Function used to check if name is a visible container located in the folder related to folder_id
 * \param folder_id Id of the container parent folder
 * \param name Name of the container
 * \param oDB Pointer to the OphidiaDB
 * \param answer 1 if visible, 0 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_is_visible_container(int folder_id, char *name, ophidiadb * oDB, int *answer);

/**
 * \brief Function used to check if name is not used by any of the folders or visible containers located in the folder related to folder_id
 * \param folder_id Id of the parent folder
 * \param name Name to check
 * \param oDB Pointer to the OphidiaDB
 * \param answer 1 if unique, 0 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_is_unique(int folderid, char *name, ophidiadb * oDB, int *answer);

/**
 * \brief Function used to check if the folder indicated by folder_id is empty (no subfolders and containers)
 * \param folder_id Id of the folder to check
 * \param oDB Pointer to the OphidiaDB
 * \param answer 1 if empty, 0 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_is_empty_folder(int folderid, ophidiadb * oDB, int *answer);

/**
 * \brief Function used to update name and folder of a container
 * \param container_id Id of the container to be updated
 * \param out_folder_id Id of the output folder used
 * \param out_container_name Name to be given
 * \param oDB Pointer to the OphidiaDB
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_update_container_path_name(ophidiadb * oDB, int in_container_id, int out_folder_id, char *out_container_name);

/**
 * \brief Function used to retrieve filesystem objects (folders, containers and datacubes)
 * \param oDB Pointer to the OphidiaDB
 * \param level 0 - only folders, 1 - folders and containers, 2 - folders, containers and datacubes
 * \param id_folder Id of the current folder
 * \param information_list Output result set
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_find_fs_objects(ophidiadb * oDB, int level, int id_folder, MYSQL_RES ** information_list);

/**
 * \brief Function used to retrieve filesystem objects and also additional info. Output can be filtered on different arguments
 * \param oDB Pointer to the OphidiaDB 
 * \param id_folder Id of the current folder
 * \param measure_filter Optional filter on the name of measure. It can be NULL.
 * \param oper_level Optional filter on operation level. If negative it won't be considered
 * \param src_filter Optional filter on the source file. It can be NULL.
 * \param information_list Output result set
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_find_fs_filtered_objects(ophidiadb * oDB, int id_folder, char *measure_filter, int oper_level, char *src_filter, MYSQL_RES ** information_list);

/**
 * \brief Function used to insert a new folder
 * \param oDB Pointer to the OphidiaDB
 * \param parent_folder_id Id of the parent folder
 * \param child_folder Name of the new folder
 * \param last_insertd_id ID of last inserted row.
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_insert_into_folder_table(ophidiadb * oDB, int parent_folder_id, char *child_folder, int *last_insertd_id);

/**
 * \brief Function used to delete a folder
 * \param oDB Pointer to the OphidiaDB
 * \param folderid Id of the folder to be removed
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_delete_from_folder_table(ophidiadb * oDB, int folderid);

/**
 * \brief Function used to move a folder
 * \param oDB Pointer to the OphidiaDB
 * \param old_folder_id Id of the folder to be moved
 * \param new_parent_folder_id Id of the new parent folder
 * \param new_child_folder Name of the new folder
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_update_folder_table(ophidiadb * oDB, int old_folder_id, int new_parent_folder_id, char *new_child_folder);

/**
 * \brief Function to delete container tuple from OphidiaDB
 * \param oDB Pointer to the OphidiaDB
 * \param id_container ID to the container to be deleted
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_delete_from_container_table(ophidiadb * oDB, int id_container);

/**
 * \brief Function to check if a container is empty
 * \param oDB Pointer to the OphidiaDB
 * \param id_container Id of the container to check
 * \param flag Set to 1 in case the container is empty
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_check_if_container_empty(ophidiadb * oDB, int id_container, int *flag);

/**
 * \brief Function that updates OphidiaDB adding the new container
 * \param oDB Pointer to OphidiaDB
 * \param cont Pointer to container to be added
 * \param last_insertd_id ID of last inserted datacube
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_insert_into_container_table(ophidiadb * oDB, oph_odb_container * cont, int *last_insertd_id);

/**
 * \brief Function to retrieve id of a container from its name
 * \param Pointer to OphidiaDB
 * \param container_name name of the container
 * \param folder_id id of folder where the container should be
 * \param id_container ID of the container
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_retrieve_container_id_from_container_name(ophidiadb * oDB, int folder_id, char *container_name, int *id_container);

/**
 * \brief Function to check if a container doesn't exists
 * \param Pointer to OphidiaDB
 * \param container_name name of the container
 * \param folder_id Id of folder containing the container
 * \param result Variable tha contains 0 if the container doesn't exists and 1 otherwise
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_check_if_container_not_present(ophidiadb * oDB, char *container_name, int folder_id, int *result);

/**
 * \brief Function used to check if name is an admissible name for a filesystem object
 * \param name Name to check
 * \return 1 if yes, 0 if no
 */
int oph_odb_fs_is_allowed_name(char *name);

/**
 * \brief Function used to check if a folder coincides to or has an ascendant that coincides to a folder
 * \param oDB Pointer to the OphidiaDB
 * \param folderid Id of the current folder
 * \param new_parent_folderid Id of a possible parent folder for folder pointed by folderid
 * \param answer 1 if yes, 0 if no
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_has_ascendant_equal_to_folder(ophidiadb * oDB, int folderid, int new_parent_folderid, int *answer);

/**
 * \brief Function used to append container id to container name for uniqueness
 * \param oDB Pointer to the OphidiaDB
 * \param containerid Id of target container
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_add_suffix_to_container_name(ophidiadb * oDB, int containerid);

/**
 * \brief Function used to retrieve container info from container name
 * \param Pointer to OphidiaDB
 * \param container_name name of the container
 * \param folder_id id of folder where the container should be
 * \param id_container ID of the container
 * \param description Pointer to an output string containing the description
 * \param vocabulary Pointer to an output string containing the vocabulary
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_retrieve_container_from_container_name(ophidiadb * oDB, int folder_id, char *container_name, int *id_container, char **description, char **vocabulary);

/**
 * \brief Function used to retrieve container info from container name
 * \param Pointer to OphidiaDB
 * \param container_id Id of the container
 * \param container_name Pointer to container name
 * \param folder_id Id of folder where the container should be
 * \return 0 if successfull, -1 otherwise
 */
int oph_odb_fs_retrieve_container_name_from_container(ophidiadb * oDB, int container_id, char **container_name, int *folder_id);

#endif				/* __OPH_ODB_FS_H__ */
