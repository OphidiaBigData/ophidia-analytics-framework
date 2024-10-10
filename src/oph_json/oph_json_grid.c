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

#define _GNU_SOURCE

#include "oph_json_grid.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>

#include "debug.h"
#include "oph_log_error_codes.h"

extern int msglevel;

/***********OPH_JSON_OBJ_GRID INTERNAL FUNCTIONS***********/

// Free a grid object contents
int oph_json_free_grid(oph_json_obj_grid *obj)
{
	if (obj) {
		if (obj->description) {
			free(obj->description);
			obj->description = NULL;
		}
		if (obj->fieldtypes) {
			unsigned int i;
			for (i = 0; i < obj->fieldtypes_num; i++) {
				if (obj->fieldtypes[i]) {
					free(obj->fieldtypes[i]);
					obj->fieldtypes[i] = NULL;
				}
			}
			free(obj->fieldtypes);
			obj->fieldtypes = NULL;
		}
		obj->fieldtypes_num = 0;
		if (obj->keys) {
			unsigned int i;
			for (i = 0; i < obj->keys_num; i++) {
				if (obj->keys[i]) {
					free(obj->keys[i]);
					obj->keys[i] = NULL;
				}
			}
			free(obj->keys);
			obj->keys = NULL;
		}
		obj->keys_num = 0;
		if (obj->title) {
			free(obj->title);
			obj->title = NULL;
		}
		if (obj->values) {
			unsigned int i;
			for (i = 0; i < obj->values_num1; i++) {
				if (obj->values[i]) {
					unsigned int j;
					for (j = 0; j < obj->values_num2; j++) {
						if (obj->values[i][j]) {
							free(obj->values[i][j]);
							obj->values[i][j] = NULL;
						}
					}
					free(obj->values[i]);
					obj->values[i] = NULL;
				}
			}
			free(obj->values);
			obj->values = NULL;
		}
		obj->values_num1 = 0;
		obj->values_num2 = 0;
	}
	return OPH_JSON_SUCCESS;
}

/***********OPH_JSON_OBJ_GRID FUNCTIONS***********/

int oph_json_add_grid(oph_json *json, const char *objkey, const char *title, const char *description, char **keys, int keys_num, char **fieldtypes, int fieldtypes_num)
{
	if (!json || !objkey || !title || !keys || keys_num < 1 || !fieldtypes || fieldtypes_num < 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	if (json->response_num == 0) {
		json->response = (oph_json_response *) malloc(sizeof(oph_json_response));
		if (!json->response) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->response[0].objclass = NULL;
		json->response[0].objcontent = NULL;
		json->response[0].objcontent_num = 0;
		json->response[0].objkey = NULL;

		json->response[0].objclass = (char *) strdup(OPH_JSON_GRID);
		if (!json->response[0].objclass) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response_num++;

		json->response[0].objkey = (char *) strdup(objkey);
		if (!json->response[0].objkey) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
			return OPH_JSON_MEMORY_ERROR;
		}
		if (oph_json_add_responseKey(json, objkey)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response[0].objcontent = malloc(sizeof(oph_json_obj_grid));
		if (!json->response[0].objcontent) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_grid *) json->response[0].objcontent)[0].description = NULL;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes = NULL;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes_num = 0;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].keys = NULL;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].keys_num = 0;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].title = NULL;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].values = NULL;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].values_num1 = 0;
		((oph_json_obj_grid *) json->response[0].objcontent)[0].values_num2 = 0;

		((oph_json_obj_grid *) json->response[0].objcontent)[0].title = (char *) strdup(title);
		if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].title) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response[0].objcontent_num++;

		if (description) {
			((oph_json_obj_grid *) json->response[0].objcontent)[0].description = (char *) strdup(description);
			if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].description) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
				return OPH_JSON_MEMORY_ERROR;
			}
		}

		int k, q;

		((oph_json_obj_grid *) json->response[0].objcontent)[0].keys = (char **) malloc(sizeof(char *) * keys_num);
		if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].keys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < keys_num; k++) {
			for (q = 0; q < k; q++) {
				if (!strcmp(keys[q], keys[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
			}
			((oph_json_obj_grid *) json->response[0].objcontent)[0].keys[k] = (char *) strdup(keys[k]);
			if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].keys[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_grid *) json->response[0].objcontent)[0].keys_num++;
		}

		((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes = (char **) malloc(sizeof(char *) * fieldtypes_num);
		if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < fieldtypes_num; k++) {
			if (!oph_json_is_type_correct(fieldtypes[k])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes[k] = (char *) strdup(fieldtypes[k]);
			if (!((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes_num++;
		}

		if (keys_num != fieldtypes_num || (unsigned int) keys_num != ((oph_json_obj_grid *) json->response[0].objcontent)[0].keys_num
		    || (unsigned int) fieldtypes_num != ((oph_json_obj_grid *) json->response[0].objcontent)[0].fieldtypes_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
			return OPH_JSON_BAD_PARAM_ERROR;
		}

		((oph_json_obj_grid *) json->response[0].objcontent)[0].values_num2 = ((oph_json_obj_grid *) json->response[0].objcontent)[0].keys_num;
	} else {
		unsigned int i;
		int add_frag = 0;
		for (i = 0; i < json->response_num; i++) {
			if (!strcmp(json->response[i].objkey, objkey)) {
				if (!strcmp(json->response[i].objclass, OPH_JSON_GRID)) {
					add_frag = 1;
					break;
				}
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
		if (add_frag) {
			void *tmp = json->response[i].objcontent;
			unsigned int index = json->response[i].objcontent_num;
			json->response[i].objcontent = realloc(json->response[i].objcontent, sizeof(oph_json_obj_grid) * (json->response[i].objcontent_num + 1));
			if (!json->response[i].objcontent) {
				json->response[i].objcontent = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_grid *) json->response[i].objcontent)[index].description = NULL;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes = NULL;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes_num = 0;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].keys = NULL;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].keys_num = 0;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].title = NULL;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].values = NULL;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].values_num1 = 0;
			((oph_json_obj_grid *) json->response[i].objcontent)[index].values_num2 = 0;

			((oph_json_obj_grid *) json->response[i].objcontent)[index].title = (char *) strdup(title);
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[i].objcontent_num++;

			if (description) {
				((oph_json_obj_grid *) json->response[i].objcontent)[index].description = (char *) strdup(description);
				if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			int k, q;

			((oph_json_obj_grid *) json->response[i].objcontent)[index].keys = (char **) malloc(sizeof(char *) * keys_num);
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].keys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < keys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(keys[q], keys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_grid *) json->response[i].objcontent)[index].keys[k] = (char *) strdup(keys[k]);
				if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].keys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_grid *) json->response[i].objcontent)[index].keys_num++;
			}

			((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes = (char **) malloc(sizeof(char *) * fieldtypes_num);
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < fieldtypes_num; k++) {
				if (!oph_json_is_type_correct(fieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes[k] = (char *) strdup(fieldtypes[k]);
				if (!((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes_num++;
			}

			if (keys_num != fieldtypes_num || (unsigned int) keys_num != ((oph_json_obj_grid *) json->response[i].objcontent)[index].keys_num
			    || (unsigned int) fieldtypes_num != ((oph_json_obj_grid *) json->response[i].objcontent)[index].fieldtypes_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
				return OPH_JSON_BAD_PARAM_ERROR;
			}

			((oph_json_obj_grid *) json->response[i].objcontent)[index].values_num2 = ((oph_json_obj_grid *) json->response[i].objcontent)[index].keys_num;
		} else {
			oph_json_response *tmp = json->response;
			unsigned int index = json->response_num;
			json->response = (oph_json_response *) realloc(json->response, sizeof(oph_json_response) * (json->response_num + 1));
			if (!json->response) {
				json->response = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response");
				return OPH_JSON_MEMORY_ERROR;
			}
			json->response[index].objclass = NULL;
			json->response[index].objcontent = NULL;
			json->response[index].objcontent_num = 0;
			json->response[index].objkey = NULL;

			json->response[index].objclass = (char *) strdup(OPH_JSON_GRID);
			if (!json->response[index].objclass) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response_num++;

			json->response[index].objkey = (char *) strdup(objkey);
			if (!json->response[index].objkey) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				return OPH_JSON_MEMORY_ERROR;
			}
			if (oph_json_add_responseKey(json, objkey)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[index].objcontent = malloc(sizeof(oph_json_obj_grid));
			if (!json->response[index].objcontent) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_grid *) json->response[index].objcontent)[0].description = NULL;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes = NULL;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes_num = 0;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].keys = NULL;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].keys_num = 0;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].title = NULL;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].values = NULL;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].values_num1 = 0;
			((oph_json_obj_grid *) json->response[index].objcontent)[0].values_num2 = 0;

			((oph_json_obj_grid *) json->response[index].objcontent)[0].title = (char *) strdup(title);
			if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[index].objcontent_num++;

			if (description) {
				((oph_json_obj_grid *) json->response[index].objcontent)[0].description = (char *) strdup(description);
				if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			int k, q;

			((oph_json_obj_grid *) json->response[index].objcontent)[0].keys = (char **) malloc(sizeof(char *) * keys_num);
			if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].keys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < keys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(keys[q], keys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_grid *) json->response[index].objcontent)[0].keys[k] = (char *) strdup(keys[k]);
				if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].keys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_grid *) json->response[index].objcontent)[0].keys_num++;
			}

			((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes = (char **) malloc(sizeof(char *) * fieldtypes_num);
			if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < fieldtypes_num; k++) {
				if (!oph_json_is_type_correct(fieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "fieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes[k] = (char *) strdup(fieldtypes[k]);
				if (!((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "fieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes_num++;
			}

			if (keys_num != fieldtypes_num || (unsigned int) keys_num != ((oph_json_obj_grid *) json->response[index].objcontent)[0].keys_num
			    || (unsigned int) fieldtypes_num != ((oph_json_obj_grid *) json->response[index].objcontent)[0].fieldtypes_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys_num/fieldtypes_num");
				return OPH_JSON_BAD_PARAM_ERROR;
			}

			((oph_json_obj_grid *) json->response[index].objcontent)[0].values_num2 = ((oph_json_obj_grid *) json->response[index].objcontent)[0].keys_num;
		}
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_add_grid_row(oph_json *json, const char *objkey, char **values)
{
	if (!json || !objkey || !values) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	if (json->response_num == 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "response_num");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "response_num");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	unsigned int i;
	int grid_present = 0;
	for (i = 0; i < json->response_num; i++) {
		if (!strcmp(json->response[i].objkey, objkey)) {
			if (!strcmp(json->response[i].objclass, OPH_JSON_GRID)) {
				grid_present = 1;
				break;
			}
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
	}
	if (grid_present) {
		if (json->response[i].objcontent_num < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		unsigned int index = 0;
		if (((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num1 == 0) {
			((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values = (char ***) malloc(sizeof(char **));
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = 0;
		} else {
			char ***tmp = ((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values;
			((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values =
			    (char ***) realloc(((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values,
					       sizeof(char **) * (((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num1 + 1));
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values) {
				((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = ((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num1;
		}

		unsigned int k;

		((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values[index] =
		    (char **) malloc(sizeof(char *) * (((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num2));
		if (!((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values[index]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values row");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values row");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num1++;

		for (k = 0; k < ((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num2; k++) {
			((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values[index][k] = NULL;
		}
		for (k = 0; k < ((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values_num2; k++) {
			((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values[index][k] = (char *) strdup(values[k]);
			if (!((oph_json_obj_grid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].values[index][k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "value");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "value");
				return OPH_JSON_MEMORY_ERROR;
			}
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	return OPH_JSON_SUCCESS;
}
