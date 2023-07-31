/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

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

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include "oph_directory_library.h"

#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <stdlib.h>

int oph_dir_r_mkdir(const char *dir)
{
	char tmp[OPH_DIR_BUFFER_LEN];
	char *p = NULL;
	size_t len;
	struct stat st;
	int res;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if (tmp[len - 1] == '/')
		tmp[len - 1] = 0;
	for (p = tmp + 1; *p; p++) {
		if (*p == '/') {
			*p = 0;
			if (stat(tmp, &st)) {
				if ((res = mkdir(tmp, S_IRWXU | S_IXGRP | S_IRGRP | S_IROTH | S_IXOTH)))
					return res;
			}
			*p = '/';
		}
	}
	if (stat(tmp, &st)) {
		if ((res = mkdir(tmp, S_IRWXU | S_IXGRP | S_IRGRP | S_IROTH | S_IXOTH)))
			return res;
	}
	return OPH_DIR_SUCCESS;
}

int oph_dir_get_num_of_files_in_dir(char *dirpath, int *num, oph_dir_extension ext_type)
{
	DIR *dir;
	struct dirent *ent;
	*num = 0;

	if ((dir = opendir(dirpath)) != NULL) {
		while ((ent = readdir(dir)) != NULL) {
			switch (ext_type) {
				case OPH_DIR_NC:
					if (strcasestr(ent->d_name, OPH_DIR_NC_EXTENSION) != NULL)
						*num = *num + 1;
					break;
				case OPH_DIR_NCL:
					if (strcasestr(ent->d_name, OPH_DIR_NCL_EXTENSION) != NULL)
						*num = *num + 1;
					break;
				default:
					if (strcmp(ent->d_name, OPH_DIR_CURRENT_DIR) && strcmp(ent->d_name, OPH_DIR_PARENT_DIR))
						*num = *num + 1;
			}
		}
		closedir(dir);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "could not open directory\n");
		return OPH_DIR_ERROR;
	}

	return OPH_DIR_SUCCESS;
}

int oph_dir_get_files_in_dir(char *dirpath, char **file_list, oph_dir_extension ext_type)
{
	int n;
	struct dirent **ents;

	//versionsort only defined for Linux
	if ((n = scandir(dirpath, &ents, 0, versionsort)) >= 0) {
		int i;
		int j = 0;
		for (i = 0; i < n; i++) {
			switch (ext_type) {
				case OPH_DIR_NC:
					if (strcasestr(ents[i]->d_name, OPH_DIR_NC_EXTENSION) != NULL) {
						snprintf(file_list[j], OPH_DIR_BUFFER_LEN, "%s", ents[i]->d_name);
						j++;
					}
					break;
				case OPH_DIR_NCL:
					if (strcasestr(ents[i]->d_name, OPH_DIR_NCL_EXTENSION) != NULL) {
						snprintf(file_list[j], OPH_DIR_BUFFER_LEN, "%s", ents[i]->d_name);
						j++;
					}
					break;
				default:
					if (strcmp(ents[i]->d_name, OPH_DIR_CURRENT_DIR) && strcmp(ents[i]->d_name, OPH_DIR_PARENT_DIR)) {
						snprintf(file_list[j], OPH_DIR_BUFFER_LEN, "%s", ents[i]->d_name);
						j++;
					}
			}
			free(ents[i]);
		}
		free(ents);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "could not open directory\n");
		return OPH_DIR_ERROR;
	}

	return OPH_DIR_SUCCESS;
}

int oph_dir_get_defnc_in_dir(char *dirpath, char *file_name)
{
	DIR *dir;
	struct dirent *ent;

	if ((dir = opendir(dirpath)) != NULL) {
		while ((ent = readdir(dir)) != NULL) {
			if (strcasestr(ent->d_name, OPH_DIR_NC_EXTENSION) != NULL) {
				snprintf(file_name, OPH_DIR_BUFFER_LEN, "%s", ent->d_name);
				break;
			}
		}
		closedir(dir);
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "could not open directory\n");
		return OPH_DIR_ERROR;
	}

	return OPH_DIR_SUCCESS;
}

int oph_dir_unlink_path(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
	if (!sb || !typeflag || !ftwbuf)
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Null parameter\n");
	int rv = remove(fpath);
	if (rv)
		perror(fpath);
	return rv;
}
