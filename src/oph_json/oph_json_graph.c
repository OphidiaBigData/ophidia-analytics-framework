/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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

#include "debug.h"
#include "oph_log_error_codes.h"

extern int msglevel;

/***********OPH_JSON_OBJ_GRAPH INTERNAL FUNCTIONS***********/

// Free a (di)graph object contents
int oph_json_free_graph(oph_json_obj_graph * obj)
{
	if (obj) {
		if (obj->description) {
			free(obj->description);
			obj->description = NULL;
		}
		if (obj->nodekeys) {
			unsigned int i;
			for (i = 0; i < obj->nodekeys_num; i++) {
				if (obj->nodekeys[i]) {
					free(obj->nodekeys[i]);
					obj->nodekeys[i] = NULL;
				}
			}
			free(obj->nodekeys);
			obj->nodekeys = NULL;
		}
		obj->nodekeys_num = 0;
		if (obj->nodelinks) {
			unsigned int i;
			for (i = 0; i < obj->nodelinks_num; i++) {
				if (obj->nodelinks[i].links) {
					unsigned int j;
					for (j = 0; j < obj->nodelinks[i].links_num; j++) {
						if (obj->nodelinks[i].links[j].description) {
							free(obj->nodelinks[i].links[j].description);
							obj->nodelinks[i].links[j].description = NULL;
						}
						if (obj->nodelinks[i].links[j].node) {
							free(obj->nodelinks[i].links[j].node);
							obj->nodelinks[i].links[j].node = NULL;
						}
					}
					free(obj->nodelinks[i].links);
					obj->nodelinks[i].links = NULL;
				}
				obj->nodelinks[i].links_num = 0;
			}
			free(obj->nodelinks);
			obj->nodelinks = NULL;
		}
		obj->nodelinks_num = 0;
		if (obj->nodevalues) {
			unsigned int i;
			for (i = 0; i < obj->nodevalues_num1; i++) {
				if (obj->nodevalues[i]) {
					unsigned int j;
					for (j = 0; j < obj->nodevalues_num2; j++) {
						if (obj->nodevalues[i][j]) {
							free(obj->nodevalues[i][j]);
							obj->nodevalues[i][j] = NULL;
						}
					}
					free(obj->nodevalues[i]);
					obj->nodevalues[i] = NULL;
				}
			}
			free(obj->nodevalues);
			obj->nodevalues = NULL;
		}
		obj->nodevalues_num1 = 0;
		obj->nodevalues_num2 = 0;
		if (obj->title) {
			free(obj->title);
			obj->title = NULL;
		}
	}
	return OPH_JSON_SUCCESS;
}

/***********OPH_JSON_OBJ_GRAPH FUNCTIONS***********/

int oph_json_add_graph(oph_json * json, const char *objkey, int is_digraph, const char *title, const char *description, char **nodekeys, int nodekeys_num)
{
	if (!json || !objkey || is_digraph < 0 || is_digraph > 1 || !title || nodekeys_num < 0) {
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

		if (is_digraph) {
			json->response[0].objclass = (char *) strdup(OPH_JSON_DGRAPH);
		} else {
			json->response[0].objclass = (char *) strdup(OPH_JSON_GRAPH);
		}
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

		json->response[0].objcontent = malloc(sizeof(oph_json_obj_graph));
		if (!json->response[0].objcontent) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_graph *) json->response[0].objcontent)[0].description = NULL;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys = NULL;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys_num = 0;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodelinks = NULL;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodelinks_num = 0;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodevalues = NULL;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodevalues_num1 = 0;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].nodevalues_num2 = 0;
		((oph_json_obj_graph *) json->response[0].objcontent)[0].title = NULL;

		((oph_json_obj_graph *) json->response[0].objcontent)[0].title = (char *) strdup(title);
		if (!((oph_json_obj_graph *) json->response[0].objcontent)[0].title) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response[0].objcontent_num++;

		if (description) {
			((oph_json_obj_graph *) json->response[0].objcontent)[0].description = (char *) strdup(description);
			if (!((oph_json_obj_graph *) json->response[0].objcontent)[0].description) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
				return OPH_JSON_MEMORY_ERROR;
			}
		}

		if (nodekeys) {
			int k, q;

			((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys = (char **) malloc(sizeof(char *) * nodekeys_num);
			if (!((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < nodekeys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(nodekeys[q], nodekeys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys[k] = (char *) strdup(nodekeys[k]);
				if (!((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_graph *) json->response[0].objcontent)[0].nodekeys_num++;
			}

			((oph_json_obj_graph *) json->response[0].objcontent)[0].nodevalues_num2 = nodekeys_num;
		}
	} else {
		unsigned int i;
		int add_frag = 0;
		for (i = 0; i < json->response_num; i++) {
			if (!strcmp(json->response[i].objkey, objkey)) {
				if (is_digraph) {
					if (!strcmp(json->response[i].objclass, OPH_JSON_DGRAPH)) {
						add_frag = 1;
						break;
					}
				} else {
					if (!strcmp(json->response[i].objclass, OPH_JSON_GRAPH)) {
						add_frag = 1;
						break;
					}
				}
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}
		if (add_frag) {
			void *tmp = json->response[i].objcontent;
			unsigned int index = json->response[i].objcontent_num;
			json->response[i].objcontent = realloc(json->response[i].objcontent, sizeof(oph_json_obj_graph) * (json->response[i].objcontent_num + 1));
			if (!json->response[i].objcontent) {
				json->response[i].objcontent = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_graph *) json->response[i].objcontent)[index].description = NULL;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys = NULL;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys_num = 0;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodelinks = NULL;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodelinks_num = 0;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodevalues = NULL;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodevalues_num1 = 0;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].nodevalues_num2 = 0;
			((oph_json_obj_graph *) json->response[i].objcontent)[index].title = NULL;

			((oph_json_obj_graph *) json->response[i].objcontent)[index].title = (char *) strdup(title);
			if (!((oph_json_obj_graph *) json->response[i].objcontent)[index].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[i].objcontent_num++;

			if (description) {
				((oph_json_obj_graph *) json->response[i].objcontent)[index].description = (char *) strdup(description);
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[index].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			if (nodekeys) {
				int k, q;

				((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys = (char **) malloc(sizeof(char *) * nodekeys_num);
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
					return OPH_JSON_MEMORY_ERROR;
				}
				for (k = 0; k < nodekeys_num; k++) {
					for (q = 0; q < k; q++) {
						if (!strcmp(nodekeys[q], nodekeys[k])) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
							return OPH_JSON_BAD_PARAM_ERROR;
						}
					}
					((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys[k] = (char *) strdup(nodekeys[k]);
					if (!((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys[k]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
						return OPH_JSON_MEMORY_ERROR;
					}
					((oph_json_obj_graph *) json->response[i].objcontent)[index].nodekeys_num++;
				}

				((oph_json_obj_graph *) json->response[i].objcontent)[index].nodevalues_num2 = nodekeys_num;
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

			if (is_digraph) {
				json->response[index].objclass = (char *) strdup(OPH_JSON_DGRAPH);
			} else {
				json->response[index].objclass = (char *) strdup(OPH_JSON_GRAPH);
			}
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

			json->response[index].objcontent = malloc(sizeof(oph_json_obj_graph));
			if (!json->response[index].objcontent) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_graph *) json->response[index].objcontent)[0].description = NULL;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys = NULL;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys_num = 0;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodelinks = NULL;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodelinks_num = 0;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodevalues = NULL;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodevalues_num1 = 0;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].nodevalues_num2 = 0;
			((oph_json_obj_graph *) json->response[index].objcontent)[0].title = NULL;

			((oph_json_obj_graph *) json->response[index].objcontent)[0].title = (char *) strdup(title);
			if (!((oph_json_obj_graph *) json->response[index].objcontent)[0].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[index].objcontent_num++;

			if (description) {
				((oph_json_obj_graph *) json->response[index].objcontent)[0].description = (char *) strdup(description);
				if (!((oph_json_obj_graph *) json->response[index].objcontent)[0].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			if (nodekeys) {
				int k, q;

				((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys = (char **) malloc(sizeof(char *) * nodekeys_num);
				if (!((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekeys");
					return OPH_JSON_MEMORY_ERROR;
				}
				for (k = 0; k < nodekeys_num; k++) {
					for (q = 0; q < k; q++) {
						if (!strcmp(nodekeys[q], nodekeys[k])) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodekey");
							return OPH_JSON_BAD_PARAM_ERROR;
						}
					}
					((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys[k] = (char *) strdup(nodekeys[k]);
					if (!((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys[k]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodekey");
						return OPH_JSON_MEMORY_ERROR;
					}
					((oph_json_obj_graph *) json->response[index].objcontent)[0].nodekeys_num++;
				}

				((oph_json_obj_graph *) json->response[index].objcontent)[0].nodevalues_num2 = nodekeys_num;
			}
		}
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_add_graph_node(oph_json * json, const char *objkey, char **nodevalues)
{
	if (!json || !objkey) {
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
	int graph_present = 0;
	for (i = 0; i < json->response_num; i++) {
		if (!strcmp(json->response[i].objkey, objkey)) {
			if (!strcmp(json->response[i].objclass, OPH_JSON_DGRAPH) || !strcmp(json->response[i].objclass, OPH_JSON_GRAPH)) {
				graph_present = 1;
				break;
			}
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
	}
	if (graph_present) {
		if (json->response[i].objcontent_num < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		if (nodevalues) {
			if (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num2 == 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			unsigned int index = 0;
			if (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num1 == 0) {
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues = (char ***) malloc(sizeof(char **));
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
					return OPH_JSON_MEMORY_ERROR;
				}
				index = 0;
			} else {
				char ***tmp = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues;
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues =
				    (char ***) realloc(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues,
						       sizeof(char **) * (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num1 + 1));
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues) {
					((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues = tmp;
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues");
					return OPH_JSON_MEMORY_ERROR;
				}
				index = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num1;
			}

			unsigned int k;

			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues[index] =
			    (char **) malloc(sizeof(char *) * (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num2));
			if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues[index]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues row");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalues row");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num1++;

			for (k = 0; k < ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num2; k++) {
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues[index][k] = NULL;
			}
			for (k = 0; k < ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num2; k++) {
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues[index][k] = (char *) strdup(nodevalues[k]);
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues[index][k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodevalue");
					return OPH_JSON_MEMORY_ERROR;
				}
			}
		} else {
			if (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodevalues_num2 != 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "nodevalues");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
		}

		unsigned int index = 0;
		if (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num == 0) {
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks = (oph_json_links *) malloc(sizeof(oph_json_links));
			if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = 0;
		} else {
			oph_json_links *tmp = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks;
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks =
			    (oph_json_links *) realloc(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks,
						       sizeof(oph_json_links) * (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num + 1));
			if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks) {
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "nodelinks");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num;
		}
		((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num++;

		((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[index].links = NULL;
		((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[index].links_num = 0;

	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_add_graph_link(oph_json * json, const char *objkey, int node1, int node2, const char *description)
{
	if (!json || !objkey || node1 < 0 || node2 < 0) {
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
	int graph_present = 0;
	int is_digraph = 0;
	for (i = 0; i < json->response_num; i++) {
		if (!strcmp(json->response[i].objkey, objkey)) {
			if (!strcmp(json->response[i].objclass, OPH_JSON_DGRAPH)) {
				graph_present = 1;
				is_digraph = 1;
				break;
			}
			if (!strcmp(json->response[i].objclass, OPH_JSON_GRAPH)) {
				graph_present = 1;
				is_digraph = 0;
				break;
			}
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
	}
	if (graph_present) {
		if (json->response[i].objcontent_num < 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objcontent_num");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		if (node1 != node2 && ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num >= (unsigned int) (node1 + 1)
		    && ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks_num >= (unsigned int) (node2 + 1)) {
			unsigned int index = 0;
			if (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links_num == 0) {
				if (!is_digraph) {
					char buf[20];
					memset(buf, 0, 20);
					snprintf(buf, 20, "%d", node1);
					unsigned int n;
					for (n = 0; n < ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node2].links_num; n++) {
						if (!strcmp(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node2].links[n].node, buf)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1");
							return OPH_JSON_BAD_PARAM_ERROR;
						}
					}
				}
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links = (oph_json_link *) malloc(sizeof(oph_json_link));
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "links");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "links");
					return OPH_JSON_MEMORY_ERROR;
				}
				index = 0;
			} else {
				if (!is_digraph) {
					char buf[20];
					memset(buf, 0, 20);
					snprintf(buf, 20, "%d", node1);
					unsigned int n;
					for (n = 0; n < ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node2].links_num; n++) {
						if (!strcmp(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node2].links[n].node, buf)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1");
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1");
							return OPH_JSON_BAD_PARAM_ERROR;
						}
					}
				}
				char buf[20];
				memset(buf, 0, 20);
				snprintf(buf, 20, "%d", node2);
				unsigned int n;
				for (n = 0; n < ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links_num; n++) {
					if (!strcmp(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[n].node, buf)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node2");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node2");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				oph_json_link *tmp = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links;
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links =
				    (oph_json_link *) realloc(((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links,
							      sizeof(oph_json_link) *
							      (((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links_num + 1));
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links) {
					((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links = tmp;
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "links");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "links");
					return OPH_JSON_MEMORY_ERROR;
				}
				index = ((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links_num;
			}
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].description = NULL;
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].node = NULL;

			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links_num++;

			char buf[20];
			memset(buf, 0, 20);
			snprintf(buf, 20, "%d", node2);
			((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].node = (char *) strdup(buf);
			if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].node) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "node2");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "node2");
				return OPH_JSON_MEMORY_ERROR;
			}

			if (description) {
				((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].description = (char *) strdup(description);
				if (!((oph_json_obj_graph *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].nodelinks[node1].links[index].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1/node2");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "node1/node2");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "objkey");
		return OPH_JSON_BAD_PARAM_ERROR;
	}

	return OPH_JSON_SUCCESS;
}
