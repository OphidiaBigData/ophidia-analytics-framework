/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

#ifndef __OPH_DIRECTORY_H
#define __OPH_DIRECTORY_H

#include "oph_common.h"
#include <ftw.h>
#include <sys/stat.h>
#include <sys/types.h>

/** 
 * \brief Enum type used to identify the extension type of the files being searched
 * \var OPH_DIR_NC Variable used to identify nc extension
 * \var OPH_DIR_NCL Variable used to identify ncl extension
 * \var OPH_DIR_ALL Variable used for all extensions
 */
typedef enum {
	OPH_DIR_NC,
	OPH_DIR_NCL,
	OPH_DIR_ALL
} oph_dir_extension;

#define OPH_DIR_NCL_EXTENSION	OPH_COMMON_NCL_EXTENSION
#define OPH_DIR_NC_EXTENSION	OPH_COMMON_NC_EXTENSION
#define OPH_DIR_CURRENT_DIR	OPH_COMMON_CURRENT_DIR
#define OPH_DIR_PARENT_DIR	OPH_COMMON_PARENT_DIR
#define OPH_DIR_BUFFER_LEN	OPH_COMMON_BUFFER_LEN

#define OPH_DIR_ERROR   	1
#define OPH_DIR_SUCCESS 	0


/** 
 * \brief Function to create a new folder
 * \param dir Directory were the new folder is created
 * \return 0 if successfull, N otherwise
 */
int oph_dir_r_mkdir(const char *dir);

/** 
 * \brief Function to count number of files in directory
 * \param dirpath Path of directory
 * \param num Number of files counted in directory
 * \param ext_type Type of file extension to be counted
 * \return 0 if successfull, N otherwise
 */
int oph_dir_get_num_of_files_in_dir(char *dirpath, int *num, oph_dir_extension ext_type);

/** 
 * \brief Function to get the list of file name inside a folder
 * \param dirpath Path of directory
 * \param file_list List of files into the directory
 * \param ext_type Type of file extension to be retrieved
 * \return 0 if successfull, N otherwise
 */
int oph_dir_get_files_in_dir(char *dirpath, char **file_list, oph_dir_extension ext_type);

/** 
 * \brief Function to get a nc file name inside a folder
 * \param dirpath Path of directory
 * \param file_name Name of file into the directory
 * \return 0 if successfull, N otherwise
 */
int oph_dir_get_defnc_in_dir(char *dirpath, char *file_name);

/** 
 * \brief Function used by nftw to delete folders
 * \param fpath is the pathname of the entry 
 * \param sb is a pointer to the stat structure returned by a call to stat
 * \param typeflag is an integer
 * \param ftwbuf File tree walk buffer
 * \return 0 if successfull, N otherwise
 */
int oph_dir_unlink_path(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);

#endif				//__OPH_DIRECTORY_H
