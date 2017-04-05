/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#include "oph_json_common.h"
#include "oph_json_text.h"
#include "oph_json_grid.h"
#include "oph_json_multigrid.h"
#include "oph_json_tree.h"
#include "oph_json_graph.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>

/* Jansson header to manipulate JSONs */
#include <jansson.h>

#include "debug.h"
#include "oph_log_error_codes.h"

extern int msglevel;

/***********OPH_JSON INTERNAL FUNCTIONS***********/

// Check if measure type does exist
int oph_json_is_measuretype_correct(const char *measuretype)
{
	int res;
	if (!strcmp(measuretype, OPH_JSON_INT))
		res = 1;
	else if (!strcmp(measuretype, OPH_JSON_LONG))
		res = 1;
	else if (!strcmp(measuretype, OPH_JSON_SHORT))
		res = 1;
	else if (!strcmp(measuretype, OPH_JSON_BYTE))
		res = 1;
	else if (!strcmp(measuretype, OPH_JSON_FLOAT))
		res = 1;
	else if (!strcmp(measuretype, OPH_JSON_DOUBLE))
		res = 1;
	else
		res = 0;
	return res;
}

// Check if type does exist
int oph_json_is_type_correct(const char *type)
{
	int res;
	if (!strcmp(type, OPH_JSON_INT))
		res = 1;
	else if (!strcmp(type, OPH_JSON_LONG))
		res = 1;
	else if (!strcmp(type, OPH_JSON_SHORT))
		res = 1;
	else if (!strcmp(type, OPH_JSON_BYTE))
		res = 1;
	else if (!strcmp(type, OPH_JSON_FLOAT))
		res = 1;
	else if (!strcmp(type, OPH_JSON_DOUBLE))
		res = 1;
	else if (!strcmp(type, OPH_JSON_STRING))
		res = 1;
	else if (!strcmp(type, OPH_JSON_BLOB))
		res = 1;
	else
		res = 0;
	return res;
}

// Add an objkey to the responseKeyset if new
int oph_json_add_responseKey(oph_json * json, const char *responseKey)
{
	if (!json || !responseKey) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->responseKeyset_num == 0) {
		json->responseKeyset = (char **) malloc(sizeof(char *));
		if (!json->responseKeyset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->responseKeyset[0] = (char *) strdup(responseKey);
		if (!json->responseKeyset[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->responseKeyset_num++;
	} else {
		unsigned int i;
		for (i = 0; i < json->responseKeyset_num; i++) {
			if (!strcmp(json->responseKeyset[i], responseKey)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "responseKey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "responseKey");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
		char **tmp = json->responseKeyset;
		json->responseKeyset = (char **) realloc(json->responseKeyset, sizeof(char *) * (json->responseKeyset_num + 1));
		if (!json->responseKeyset) {
			json->responseKeyset = tmp;
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->responseKeyset[json->responseKeyset_num] = (char *) strdup(responseKey);
		if (!json->responseKeyset[json->responseKeyset_num]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->responseKeyset_num++;
	}
	return OPH_JSON_SUCCESS;
}

// Free consumers
int oph_json_free_consumers(oph_json * json)
{
	if (!json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->consumers) {
		unsigned int i;
		for (i = 0; i < json->consumers_num; i++) {
			if (json->consumers[i]) {
				free(json->consumers[i]);
				json->consumers[i] = NULL;
			}
		}
		free(json->consumers);
		json->consumers = NULL;
	}
	json->consumers_num = 0;
	return OPH_JSON_SUCCESS;
}

// Free responseKeyset
int oph_json_free_responseKeyset(oph_json * json)
{
	if (!json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->responseKeyset) {
		unsigned int i;
		for (i = 0; i < json->responseKeyset_num; i++) {
			if (json->responseKeyset[i]) {
				free(json->responseKeyset[i]);
				json->responseKeyset[i] = NULL;
			}
		}
		free(json->responseKeyset);
		json->responseKeyset = NULL;
	}
	json->responseKeyset_num = 0;
	return OPH_JSON_SUCCESS;
}

// Free source
int oph_json_free_source(oph_json * json)
{
	if (!json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->source) {
		if (json->source->description) {
			free(json->source->description);
			json->source->description = NULL;
		}
		if (json->source->keys) {
			unsigned int i;
			for (i = 0; i < json->source->keys_num; i++) {
				if (json->source->keys[i]) {
					free(json->source->keys[i]);
					json->source->keys[i] = NULL;
				}
			}
			free(json->source->keys);
			json->source->keys = NULL;
		}
		json->source->keys_num = 0;
		if (json->source->producer) {
			free(json->source->producer);
			json->source->producer = NULL;
		}
		if (json->source->srckey) {
			free(json->source->srckey);
			json->source->srckey = NULL;
		}
		if (json->source->srcname) {
			free(json->source->srcname);
			json->source->srcname = NULL;
		}
		if (json->source->srcurl) {
			free(json->source->srcurl);
			json->source->srcurl = NULL;
		}
		if (json->source->values) {
			unsigned int i;
			for (i = 0; i < json->source->values_num; i++) {
				if (json->source->values[i]) {
					free(json->source->values[i]);
					json->source->values[i] = NULL;
				}
			}
			free(json->source->values);
			json->source->values = NULL;
		}
		json->source->values_num = 0;

		free(json->source);
		json->source = NULL;
	}
	return OPH_JSON_SUCCESS;
}

// Free response
int oph_json_free_response(oph_json * json)
{
	if (!json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->response) {
		unsigned int i;
		for (i = 0; i < json->response_num; i++) {
			if (json->response[i].objkey) {
				free(json->response[i].objkey);
				json->response[i].objkey = NULL;
			}
			if (json->response[i].objcontent) {
				unsigned int j;
				if (!strcmp(json->response[i].objclass, OPH_JSON_TEXT)) {
					for (j = 0; j < json->response[i].objcontent_num; j++) {
						oph_json_free_text(&((oph_json_obj_text *) json->response[i].objcontent)[j]);
					}
				} else if (!strcmp(json->response[i].objclass, OPH_JSON_GRID)) {
					for (j = 0; j < json->response[i].objcontent_num; j++) {
						oph_json_free_grid((&((oph_json_obj_grid *) json->response[i].objcontent)[j]));
					}
				} else if (!strcmp(json->response[i].objclass, OPH_JSON_MULTIGRID)) {
					for (j = 0; j < json->response[i].objcontent_num; j++) {
						oph_json_free_multigrid((&((oph_json_obj_multigrid *) json->response[i].objcontent)[j]));
					}
				} else if (!strcmp(json->response[i].objclass, OPH_JSON_TREE)) {
					for (j = 0; j < json->response[i].objcontent_num; j++) {
						oph_json_free_tree((&((oph_json_obj_tree *) json->response[i].objcontent)[j]));
					}
				} else if (!strcmp(json->response[i].objclass, OPH_JSON_DGRAPH) || !strcmp(json->response[i].objclass, OPH_JSON_GRAPH)) {
					for (j = 0; j < json->response[i].objcontent_num; j++) {
						oph_json_free_graph((&((oph_json_obj_graph *) json->response[i].objcontent)[j]));
					}
				}
				free(json->response[i].objcontent);
				json->response[i].objcontent = NULL;
			}
			json->response[i].objcontent_num = 0;
			if (json->response[i].objclass) {
				free(json->response[i].objclass);
				json->response[i].objclass = NULL;
			}
		}
		free(json->response);
		json->response = NULL;
	}
	json->response_num = 0;
	return OPH_JSON_SUCCESS;
}

/***********OPH_JSON FUNCTIONS***********/

int oph_json_alloc(oph_json ** json)
{
	*json = (oph_json *) malloc(sizeof(oph_json));
	if (!*json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "oph_json");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "oph_json");
		return OPH_JSON_MEMORY_ERROR;
	}
	(*json)->consumers = NULL;
	(*json)->consumers_num = 0;
	(*json)->response = NULL;
	(*json)->responseKeyset = NULL;
	(*json)->responseKeyset_num = 0;
	(*json)->response_num = 0;
	(*json)->source = NULL;
	return OPH_JSON_SUCCESS;
}

int oph_json_free(oph_json * json)
{
	if (json) {
		oph_json_free_consumers(json);
		oph_json_free_response(json);
		oph_json_free_responseKeyset(json);
		oph_json_free_source(json);
		free(json);
		json = NULL;
	}
	return OPH_JSON_SUCCESS;
}

int oph_json_add_consumer(oph_json * json, const char *consumer)
{
	if (!json || !consumer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->consumers_num == 0) {
		json->consumers = (char **) malloc(sizeof(char *));
		if (!json->consumers) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->consumers[0] = (char *) strdup(consumer);
		if (!json->consumers[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->consumers_num++;
	} else {
		unsigned int i;
		for (i = 0; i < json->consumers_num; i++) {
			if (!strcmp(json->consumers[i], consumer)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "consumer");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "consumer");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
		char **tmp = json->consumers;
		json->consumers = (char **) realloc(json->consumers, sizeof(char *) * (json->consumers_num + 1));
		if (!json->consumers) {
			json->consumers = tmp;
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->consumers[json->consumers_num] = (char *) strdup(consumer);
		if (!json->consumers[json->consumers_num]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->consumers_num++;
	}
	return OPH_JSON_SUCCESS;
}

int oph_json_set_source(oph_json * json, const char *srckey, const char *srcname, const char *srcurl, const char *description, const char *producer)
{
	if (!json || !srckey || !srcname) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	if (json->source) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "source");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "source");
		return OPH_JSON_BAD_PARAM_ERROR;
	}
	json->source = (oph_json_source *) malloc(sizeof(oph_json_source));
	if (!json->source) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "source");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "source");
		return OPH_JSON_MEMORY_ERROR;
	}
	json->source->description = NULL;
	json->source->keys = NULL;
	json->source->keys_num = 0;
	json->source->producer = NULL;
	json->source->srckey = NULL;
	json->source->srcname = NULL;
	json->source->srcurl = NULL;
	json->source->values = NULL;
	json->source->values_num = 0;

	json->source->srckey = (char *) strdup(srckey);
	if (!json->source->srckey) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srckey");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srckey");
		return OPH_JSON_MEMORY_ERROR;
	}
	json->source->srcname = (char *) strdup(srcname);
	if (!json->source->srcname) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srcname");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srcname");
		return OPH_JSON_MEMORY_ERROR;
	}

	if (srcurl) {
		json->source->srcurl = (char *) strdup(srcurl);
		if (!json->source->srcurl) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srcurl");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srcurl");
			return OPH_JSON_MEMORY_ERROR;
		}
	}
	if (description) {
		json->source->description = (char *) strdup(description);
		if (!json->source->description) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
			return OPH_JSON_MEMORY_ERROR;
		}
	}
	if (producer) {
		json->source->producer = (char *) strdup(producer);
		if (!json->source->producer) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "producer");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "producer");
			return OPH_JSON_MEMORY_ERROR;
		}
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_add_source_detail(oph_json * json, const char *key, const char *value)
{
	if (!json || !key || !value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	if (!json->source) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "source");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "source");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	if (json->source->keys_num == 0) {
		json->source->keys = (char **) malloc(sizeof(char *));
		if (!json->source->keys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->keys[0] = (char *) strdup(key);
		if (!json->source->keys[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->keys_num++;
		json->source->values = (char **) malloc(sizeof(char *));
		if (!json->source->values) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->values[0] = (char *) strdup(value);
		if (!json->source->values[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "value");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "value");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->values_num++;
	} else {
		unsigned int i;
		for (i = 0; i < json->source->keys_num; i++) {
			if (!strcmp(json->source->keys[i], key)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "key");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
		char **tmp = json->source->keys;
		json->source->keys = (char **) realloc(json->source->keys, sizeof(char *) * (json->source->keys_num + 1));
		if (!json->source->keys) {
			json->source->keys = tmp;
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->keys[json->source->keys_num] = (char *) strdup(key);
		if (!json->source->keys[json->source->keys_num]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->keys_num++;
		char **tmp2 = json->source->values;
		json->source->values = (char **) realloc(json->source->values, sizeof(char *) * (json->source->values_num + 1));
		if (!json->source->values) {
			json->source->values = tmp2;
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->values[json->source->values_num] = (char *) strdup(value);
		if (!json->source->values[json->source->values_num]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "value");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "value");
			return OPH_JSON_MEMORY_ERROR;
		}
		json->source->values_num++;
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_to_json_string(oph_json * json, char **jstring)
{
	if (!json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "(NULL parameters)");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	*jstring = NULL;

	/* INIT JSON OBJECT */
	json_t *oph_json = json_object();
	if (!oph_json) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "oph_json");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "oph_json");
		return OPH_JSON_MEMORY_ERROR;
	}

	/* ADD SOURCE */
	if (json->source) {
		json_t *source = json_object();
		if (!source) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "source");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "source");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		if (json_object_set_new(oph_json, "source", source)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "source");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "source");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		// SRCKEY
		if (!json->source->srckey) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "srckey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "srckey");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		if (json_object_set_new(source, "srckey", json_string((const char *) json->source->srckey))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srckey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srckey");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		// SRCNAME
		if (!json->source->srcname) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "srcname");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "srcname");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		if (json_object_set_new(source, "srcname", json_string((const char *) json->source->srcname))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srcname");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srcname");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		// SRCURL
		if (json->source->srcurl) {
			if (json_object_set_new(source, "srcurl", json_string((const char *) json->source->srcurl))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "srcurl");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "srcurl");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
		}
		// DESCRIPTION
		if (json->source->description) {
			if (json_object_set_new(source, "description", json_string((const char *) json->source->description))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
		}
		// PRODUCER
		if (json->source->producer) {
			if (json_object_set_new(source, "producer", json_string((const char *) json->source->producer))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "producer");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "producer");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
		}
		// KEYS & VALUES
		if (json->source->keys_num != json->source->values_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys/values num");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys/values num");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		if (json->source->keys_num != 0) {
			unsigned int i;
			json_t *keys = json_array();
			if (!keys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			if (json_object_set_new(source, "keys", keys)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "keys");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}

			json_t *values = json_array();
			if (!values) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			if (json_object_set_new(source, "values", values)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "values");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "values");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}

			for (i = 0; i < json->source->keys_num; i++) {
				if (json_array_append_new(keys, json_string((const char *) json->source->keys[i]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "key");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "key");
					if (oph_json)
						json_decref(oph_json);
					return OPH_JSON_MEMORY_ERROR;
				}
				if (json_array_append_new(values, json_string((const char *) json->source->values[i]))) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "value");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "value");
					if (oph_json)
						json_decref(oph_json);
					return OPH_JSON_MEMORY_ERROR;
				}
			}
		}
	}

	/* ADD CONSUMERS */
	if (json->consumers_num != 0) {
		unsigned int i;
		json_t *consumers = json_array();
		if (!consumers) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		if (json_object_set_new(oph_json, "consumers", consumers)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumers");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}

		for (i = 0; i < json->consumers_num; i++) {
			if (json_array_append_new(consumers, json_string((const char *) json->consumers[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "consumer");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
		}
	}

	/* ADD RESPONSEKEYSET */
	if (json->responseKeyset_num != 0) {
		unsigned int i;
		json_t *responseKeyset = json_array();
		if (!responseKeyset) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		if (json_object_set_new(oph_json, "responseKeyset", responseKeyset)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKeyset");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}

		for (i = 0; i < json->responseKeyset_num; i++) {
			if (json_array_append_new(responseKeyset, json_string((const char *) json->responseKeyset[i]))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "responseKey");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
		}
	}

	/* ADD RESPONSE */
	if (json->response_num != 0) {
		unsigned int i;
		json_t *response = json_array();
		if (!response) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}
		if (json_object_set_new(oph_json, "response", response)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response");
			if (oph_json)
				json_decref(oph_json);
			return OPH_JSON_MEMORY_ERROR;
		}

		for (i = 0; i < json->response_num; i++) {
			json_t *response_i = json_object();
			if (!response_i) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response_i");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response_i");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			if (json_array_append_new(response, response_i)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "response_i");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "response_i");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			// OBJCLASS
			if (!json->response[i].objclass) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objclass");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objclass");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			if (json_object_set_new(response_i, "objclass", json_string((const char *) json->response[i].objclass))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objclass");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			// OBJKEY
			if (!json->response[i].objkey) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			if (json_object_set_new(response_i, "objkey", json_string((const char *) json->response[i].objkey))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objkey");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}

			/* OBJCONTENT */
			if (json->response[i].objcontent_num == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			json_t *objcontent = json_array();
			if (!objcontent) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}
			if (json_object_set_new(response_i, "objcontent", objcontent)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_MEMORY_ERROR;
			}

			unsigned int j;
			if (!strcmp(json->response[i].objclass, OPH_JSON_TEXT)) {
				/* ADD TEXT */
				for (j = 0; j < json->response[i].objcontent_num; j++) {
					json_t *objcontent_j = json_object();
					if (!objcontent_j) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_array_append_new(objcontent, objcontent_j)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// TITLE
					if (!((oph_json_obj_text *) json->response[i].objcontent)[j].title) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "title", json_string((const char *) ((oph_json_obj_text *) json->response[i].objcontent)[j].title))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// MESSAGE
					if (((oph_json_obj_text *) json->response[i].objcontent)[j].message) {
						if (json_object_set_new(objcontent_j, "message", json_string((const char *) ((oph_json_obj_text *) json->response[i].objcontent)[j].message))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "message");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "message");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}
				}
			} else if (!strcmp(json->response[i].objclass, OPH_JSON_GRID)) {
				/* ADD GRID */
				for (j = 0; j < json->response[i].objcontent_num; j++) {
					json_t *objcontent_j = json_object();
					if (!objcontent_j) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_array_append_new(objcontent, objcontent_j)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// TITLE
					if (!((oph_json_obj_grid *) json->response[i].objcontent)[j].title) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "title", json_string((const char *) ((oph_json_obj_grid *) json->response[i].objcontent)[j].title))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// DESCRIPTION
					if (((oph_json_obj_grid *) json->response[i].objcontent)[j].description) {
						if (json_object_set_new(objcontent_j, "description", json_string((const char *) ((oph_json_obj_grid *) json->response[i].objcontent)[j].description))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}
					// ROWKEYS & ROWFIELDTYPES
					if (((oph_json_obj_grid *) json->response[i].objcontent)[j].keys_num != ((oph_json_obj_grid *) json->response[i].objcontent)[j].fieldtypes_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys/fieldtypes num");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "keys/fieldtypes num");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					unsigned int k;
					json_t *rowkeys = json_array();
					if (!rowkeys) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowkeys", rowkeys)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					json_t *rowfieldtypes = json_array();
					if (!rowfieldtypes) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowfieldtypes", rowfieldtypes)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_grid *) json->response[i].objcontent)[j].keys_num; k++) {
						if (json_array_append_new(rowkeys, json_string((const char *) ((oph_json_obj_grid *) json->response[i].objcontent)[j].keys[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(rowfieldtypes, json_string((const char *) ((oph_json_obj_grid *) json->response[i].objcontent)[j].fieldtypes[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}

					// ROWVALUES
					if (((oph_json_obj_grid *) json->response[i].objcontent)[j].values_num2 != ((oph_json_obj_grid *) json->response[i].objcontent)[j].keys_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "values_num2");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "values_num2");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *rowvalues = json_array();
					if (!rowvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowvalues", rowvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_grid *) json->response[i].objcontent)[j].values_num1; k++) {
						json_t *rowvalues_k = json_array();
						if (!rowvalues_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(rowvalues, rowvalues_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_grid *) json->response[i].objcontent)[j].values_num2; q++) {
							if (json_array_append_new(rowvalues_k, json_string((const char *) ((oph_json_obj_grid *) json->response[i].objcontent)[j].values[k][q]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}
					}
				}
			} else if (!strcmp(json->response[i].objclass, OPH_JSON_MULTIGRID)) {
				/* ADD MULTIGRID */
				for (j = 0; j < json->response[i].objcontent_num; j++) {
					json_t *objcontent_j = json_object();
					if (!objcontent_j) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_array_append_new(objcontent, objcontent_j)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// TITLE
					if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[j].title) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "title", json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].title))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// DESCRIPTION
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].description) {
						if (json_object_set_new
						    (objcontent_j, "description", json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].description))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}
					// MEASURENAME
					if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurename) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurename");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurename");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "measurename", json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurename))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// MEASURETYPE
					if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measuretype) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "measuretype", json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measuretype))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// ROWKEYS & ROWFIELDTYPES
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowkeys_num != ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowfieldtypes_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkeys/rowfieldtypes num");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkeys/rowfieldtypes num");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					unsigned int k;
					json_t *rowkeys = json_array();
					if (!rowkeys) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowkeys", rowkeys)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					json_t *rowfieldtypes = json_array();
					if (!rowfieldtypes) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowfieldtypes", rowfieldtypes)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowkeys_num; k++) {
						if (json_array_append_new(rowkeys, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowkeys[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(rowfieldtypes, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowfieldtypes[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}

					// ROWVALUES
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowvalues_num2 != ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowkeys_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowvalues_num2");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowvalues_num2");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *rowvalues = json_array();
					if (!rowvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rowvalues", rowvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowvalues_num1; k++) {
						json_t *rowvalues_k = json_array();
						if (!rowvalues_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(rowvalues, rowvalues_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowvalues_num2; q++) {
							if (json_array_append_new
							    (rowvalues_k, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowvalues[k][q]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}
					}

					// COLKEYS & COLFIELDTYPES
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colkeys_num != ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colfieldtypes_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkeys/colfieldtypes num");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkeys/colfieldtypes num");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *colkeys = json_array();
					if (!colkeys) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "colkeys", colkeys)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					json_t *colfieldtypes = json_array();
					if (!colfieldtypes) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "colfieldtypes", colfieldtypes)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colkeys_num; k++) {
						if (json_array_append_new(colkeys, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colkeys[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(colfieldtypes, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colfieldtypes[k]))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}

					// COLVALUES
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colvalues_num2 != ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colkeys_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colvalues_num2");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colvalues_num2");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *colvalues = json_array();
					if (!colvalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "colvalues", colvalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colvalues_num1; k++) {
						json_t *colvalues_k = json_array();
						if (!colvalues_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(colvalues, colvalues_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colvalues_num2; q++) {
							if (json_array_append_new
							    (colvalues_k, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colvalues[k][q]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalue_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalue_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}
					}

					// MEASUREVALUES
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurevalues_num2 !=
					    ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].colvalues_num1) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurevalues_num2");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurevalues_num2");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurevalues_num1 !=
					    ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].rowvalues_num1) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurevalues_num1");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measurevalues_num1");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *measurevalues = json_array();
					if (!measurevalues) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "measurevalues", measurevalues)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurevalues_num1; k++) {
						json_t *measurevalues_k = json_array();
						if (!measurevalues_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(measurevalues, measurevalues_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurevalues_num2; q++) {
							if (json_array_append_new
							    (measurevalues_k, json_string((const char *) ((oph_json_obj_multigrid *) json->response[i].objcontent)[j].measurevalues[k][q]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalue_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalue_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}
					}
				}
			} else if (!strcmp(json->response[i].objclass, OPH_JSON_TREE)) {
				/* ADD TREE */
				for (j = 0; j < json->response[i].objcontent_num; j++) {
					json_t *objcontent_j = json_object();
					if (!objcontent_j) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_array_append_new(objcontent, objcontent_j)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// TITLE
					if (!((oph_json_obj_tree *) json->response[i].objcontent)[j].title) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "title", json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].title))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// DESCRIPTION
					if (((oph_json_obj_tree *) json->response[i].objcontent)[j].description) {
						if (json_object_set_new(objcontent_j, "description", json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].description))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}
					// ROOTNODE
					if (!((oph_json_obj_tree *) json->response[i].objcontent)[j].rootnode) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rootnode");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rootnode");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "rootnode", json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].rootnode))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rootnode");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rootnode");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// NODEKEYS
					if (((oph_json_obj_tree *) json->response[i].objcontent)[j].nodekeys_num != 0) {
						if (((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues_num2 != ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodekeys_num) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num2");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num2");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_BAD_PARAM_ERROR;
						}
						unsigned int k;
						json_t *nodekeys = json_array();
						if (!nodekeys) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_object_set_new(objcontent_j, "nodekeys", nodekeys)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						for (k = 0; k < ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodekeys_num; k++) {
							if (json_array_append_new(nodekeys, json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodekeys[k]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}

						// NODEVALUES
						json_t *nodevalues = json_array();
						if (!nodevalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_object_set_new(objcontent_j, "nodevalues", nodevalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						for (k = 0; k < ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues_num1; k++) {
							json_t *nodevalues_k = json_array();
							if (!nodevalues_k) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							if (json_array_append_new(nodevalues, nodevalues_k)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							unsigned int q;
							for (q = 0; q < ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues_num2; q++) {
								if (json_array_append_new
								    (nodevalues_k, json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues[k][q]))) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue_k");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue_k");
									if (oph_json)
										json_decref(oph_json);
									return OPH_JSON_MEMORY_ERROR;
								}
							}
						}
					}
					// NODELINKS
					if (((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues_num1 != 0
					    && ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodevalues_num1 != ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num1/nodelinks_num");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num1/nodelinks_num");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *nodelinks = json_array();
					if (!nodelinks) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "nodelinks", nodelinks)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					unsigned int k;
					for (k = 0; k < ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks_num; k++) {
						json_t *nodelinks_k = json_array();
						if (!nodelinks_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(nodelinks, nodelinks_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks[k].links_num; q++) {
							json_t *nodelinks_kq = json_object();
							if (!nodelinks_kq) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							if (json_array_append_new(nodelinks_k, nodelinks_kq)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							// NODE
							if (!((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks[k].links[q].node) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_BAD_PARAM_ERROR;
							}
							if (json_object_set_new
							    (nodelinks_kq, "node", json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks[k].links[q].node))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "node");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "node");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							// DESCRIPTION
							if (((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks[k].links[q].description) {
								if (json_object_set_new
								    (nodelinks_kq, "description",
								     json_string((const char *) ((oph_json_obj_tree *) json->response[i].objcontent)[j].nodelinks[k].links[q].description))) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
									if (oph_json)
										json_decref(oph_json);
									return OPH_JSON_MEMORY_ERROR;
								}
							}
						}
					}
				}
			} else if (!strcmp(json->response[i].objclass, OPH_JSON_DGRAPH) || !strcmp(json->response[i].objclass, OPH_JSON_GRAPH)) {
				/* ADD (DI)GRAPH */
				for (j = 0; j < json->response[i].objcontent_num; j++) {
					json_t *objcontent_j = json_object();
					if (!objcontent_j) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_array_append_new(objcontent, objcontent_j)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent_j");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// TITLE
					if (!((oph_json_obj_graph *) json->response[i].objcontent)[j].title) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					if (json_object_set_new(objcontent_j, "title", json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].title))) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					// DESCRIPTION
					if (((oph_json_obj_graph *) json->response[i].objcontent)[j].description) {
						if (json_object_set_new(objcontent_j, "description", json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].description))) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
					}
					// NODEKEYS
					if (((oph_json_obj_graph *) json->response[i].objcontent)[j].nodekeys_num != 0) {
						if (((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues_num2 != ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodekeys_num) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num2");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num2");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_BAD_PARAM_ERROR;
						}
						unsigned int k;
						json_t *nodekeys = json_array();
						if (!nodekeys) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_object_set_new(objcontent_j, "nodekeys", nodekeys)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						for (k = 0; k < ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodekeys_num; k++) {
							if (json_array_append_new(nodekeys, json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodekeys[k]))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
						}

						// NODEVALUES
						json_t *nodevalues = json_array();
						if (!nodevalues) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_object_set_new(objcontent_j, "nodevalues", nodevalues)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						for (k = 0; k < ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues_num1; k++) {
							json_t *nodevalues_k = json_array();
							if (!nodevalues_k) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							if (json_array_append_new(nodevalues, nodevalues_k)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues_k");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							unsigned int q;
							for (q = 0; q < ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues_num2; q++) {
								if (json_array_append_new
								    (nodevalues_k, json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues[k][q]))) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue_k");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue_k");
									if (oph_json)
										json_decref(oph_json);
									return OPH_JSON_MEMORY_ERROR;
								}
							}
						}
					}
					// NODELINKS
					if (((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues_num1 != 0
					    && ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodevalues_num1 != ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num1/nodelinks_num");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues_num1/nodelinks_num");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_BAD_PARAM_ERROR;
					}
					json_t *nodelinks = json_array();
					if (!nodelinks) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					if (json_object_set_new(objcontent_j, "nodelinks", nodelinks)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
						if (oph_json)
							json_decref(oph_json);
						return OPH_JSON_MEMORY_ERROR;
					}
					unsigned int k;
					for (k = 0; k < ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks_num; k++) {
						json_t *nodelinks_k = json_array();
						if (!nodelinks_k) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						if (json_array_append_new(nodelinks, nodelinks_k)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_k");
							if (oph_json)
								json_decref(oph_json);
							return OPH_JSON_MEMORY_ERROR;
						}
						unsigned int q;
						for (q = 0; q < ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks[k].links_num; q++) {
							json_t *nodelinks_kq = json_object();
							if (!nodelinks_kq) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							if (json_array_append_new(nodelinks_k, nodelinks_kq)) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks_kq");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							// NODE
							if (!((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks[k].links[q].node) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_BAD_PARAM_ERROR;
							}
							if (json_object_set_new
							    (nodelinks_kq, "node", json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks[k].links[q].node))) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "node");
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "node");
								if (oph_json)
									json_decref(oph_json);
								return OPH_JSON_MEMORY_ERROR;
							}
							// DESCRIPTION
							if (((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks[k].links[q].description) {
								if (json_object_set_new
								    (nodelinks_kq, "description",
								     json_string((const char *) ((oph_json_obj_graph *) json->response[i].objcontent)[j].nodelinks[k].links[q].description))) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
									if (oph_json)
										json_decref(oph_json);
									return OPH_JSON_MEMORY_ERROR;
								}
							}
						}
					}
				}
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objclass");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objclass");
				if (oph_json)
					json_decref(oph_json);
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
	}

	*jstring = json_dumps((const json_t *) oph_json, JSON_INDENT(4));
	if (!(*jstring)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "jstring");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "jstring");
		if (oph_json)
			json_decref(oph_json);
		return OPH_JSON_MEMORY_ERROR;
	}

	/* CLEANUP */
	if (oph_json)
		json_decref(oph_json);

	return OPH_JSON_SUCCESS;
}

int _oph_json_to_json_file(oph_json * json, char *filename, char **jstring)
{
	*jstring = NULL;

	if (oph_json_to_json_string(json, jstring)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "json string");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "json string");
		return OPH_JSON_MEMORY_ERROR;
	}

	if (*jstring) {
		FILE *fp = fopen(filename, "w");
		if (!fp) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_IO_ERROR, filename);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_IO_ERROR, filename);
			free(*jstring);
			*jstring = NULL;
			return OPH_JSON_IO_ERROR;
		}
		fprintf(fp, "%s\n", *jstring);
		fclose(fp);
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_to_json_file(oph_json * json, char *filename)
{
	char *jstring = NULL;
	int res = _oph_json_to_json_file(json, filename, &jstring);
	if (jstring)
		free(jstring);
	return res;
}

int oph_json_is_objkey_printable(char **objkeys, int objkeys_num, const char *objkey)
{
	if (objkeys_num < 1 || !objkeys || !objkey)
		return 1;
	if (!strcmp(objkeys[0], "all"))
		return 1;
	if (!strcmp(objkeys[0], "none"))
		return 0;
	unsigned short int i;
	for (i = 0; i < objkeys_num; i++) {
		if (!strcmp(objkeys[i], objkey))
			return 1;
	}
	return 0;
}
