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

#define _GNU_SOURCE

#include "oph_json_text.h"

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

/***********OPH_JSON_OBJ_TEXT INTERNAL FUNCTIONS***********/

// Free a text object contents
int oph_json_free_text(oph_json_obj_text * obj)
{
	if (obj) {
		if (obj->title) {
			free(obj->title);
			obj->title = NULL;
		}
		if (obj->message) {
			free(obj->message);
			obj->message = NULL;
		}
	}
	return OPH_JSON_SUCCESS;
}

/***********OPH_JSON_OBJ_TEXT FUNCTIONS***********/

int oph_json_add_text(oph_json * json, const char *objkey, const char *title, const char *message)
{
	if (!json || !objkey || !title) {
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

		json->response[0].objclass = (char *) strdup(OPH_JSON_TEXT);
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

		json->response[0].objcontent = malloc(sizeof(oph_json_obj_text));
		if (!json->response[0].objcontent) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_text *) json->response[0].objcontent)[0].title = NULL;
		((oph_json_obj_text *) json->response[0].objcontent)[0].message = NULL;

		((oph_json_obj_text *) json->response[0].objcontent)[0].title = (char *) strdup(title);
		if (!((oph_json_obj_text *) json->response[0].objcontent)[0].title) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response[0].objcontent_num++;

		if (message) {
			((oph_json_obj_text *) json->response[0].objcontent)[0].message = (char *) strdup(message);
			if (!((oph_json_obj_text *) json->response[0].objcontent)[0].message) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "message");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "message");
				return OPH_JSON_MEMORY_ERROR;
			}
		}
	} else {
		unsigned int i;
		int add_frag = 0;
		for (i = 0; i < json->response_num; i++) {
			if (!strcmp(json->response[i].objkey, objkey)) {
				if (!strcmp(json->response[i].objclass, OPH_JSON_TEXT)) {
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
			json->response[i].objcontent = realloc(json->response[i].objcontent, sizeof(oph_json_obj_text) * (json->response[i].objcontent_num + 1));
			if (!json->response[i].objcontent) {
				json->response[i].objcontent = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_text *) json->response[i].objcontent)[index].title = NULL;
			((oph_json_obj_text *) json->response[i].objcontent)[index].message = NULL;

			((oph_json_obj_text *) json->response[i].objcontent)[index].title = (char *) strdup(title);
			if (!((oph_json_obj_text *) json->response[i].objcontent)[index].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[i].objcontent_num++;

			if (message) {
				((oph_json_obj_text *) json->response[i].objcontent)[index].message = (char *) strdup(message);
				if (!((oph_json_obj_text *) json->response[i].objcontent)[index].message) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "message");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "message");
					return OPH_JSON_MEMORY_ERROR;
				}
			}
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

			json->response[index].objclass = (char *) strdup(OPH_JSON_TEXT);
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

			json->response[index].objcontent = malloc(sizeof(oph_json_obj_text));
			if (!json->response[index].objcontent) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_text *) json->response[index].objcontent)[0].title = NULL;
			((oph_json_obj_text *) json->response[index].objcontent)[0].message = NULL;

			((oph_json_obj_text *) json->response[index].objcontent)[0].title = (char *) strdup(title);
			if (!((oph_json_obj_text *) json->response[index].objcontent)[0].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[index].objcontent_num++;

			if (message) {
				((oph_json_obj_text *) json->response[index].objcontent)[0].message = (char *) strdup(message);
				if (!((oph_json_obj_text *) json->response[index].objcontent)[0].message) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "message");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "message");
					return OPH_JSON_MEMORY_ERROR;
				}
			}
		}
	}

	return OPH_JSON_SUCCESS;
}
