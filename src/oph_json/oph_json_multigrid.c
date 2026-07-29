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

#include "oph_json_multigrid.h"

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

/***********OPH_JSON_OBJ_MULTIGRID INTERNAL FUNCTIONS***********/

// Free a multigrid object contents
int oph_json_free_multigrid(oph_json_obj_multigrid * obj)
{
	if (obj) {
		if (obj->colfieldtypes) {
			unsigned int i;
			for (i = 0; i < obj->colfieldtypes_num; i++) {
				if (obj->colfieldtypes[i]) {
					free(obj->colfieldtypes[i]);
					obj->colfieldtypes[i] = NULL;
				}
			}
			free(obj->colfieldtypes);
			obj->colfieldtypes = NULL;
		}
		obj->colfieldtypes_num = 0;
		if (obj->colkeys) {
			unsigned int i;
			for (i = 0; i < obj->colkeys_num; i++) {
				if (obj->colkeys[i]) {
					free(obj->colkeys[i]);
					obj->colkeys[i] = NULL;
				}
			}
			free(obj->colkeys);
			obj->colkeys = NULL;
		}
		obj->colkeys_num = 0;
		if (obj->colvalues) {
			unsigned int i;
			for (i = 0; i < obj->colvalues_num1; i++) {
				if (obj->colvalues[i]) {
					unsigned int j;
					for (j = 0; j < obj->colvalues_num2; j++) {
						if (obj->colvalues[i][j]) {
							free(obj->colvalues[i][j]);
							obj->colvalues[i][j] = NULL;
						}
					}
					free(obj->colvalues[i]);
					obj->colvalues[i] = NULL;
				}
			}
			free(obj->colvalues);
			obj->colvalues = NULL;
		}
		obj->colvalues_num1 = 0;
		obj->colvalues_num2 = 0;
		if (obj->description) {
			free(obj->description);
			obj->description = NULL;
		}
		if (obj->measurename) {
			free(obj->measurename);
			obj->measurename = NULL;
		}
		if (obj->measuretype) {
			free(obj->measuretype);
			obj->measuretype = NULL;
		}
		if (obj->measurevalues) {
			unsigned int i;
			for (i = 0; i < obj->measurevalues_num1; i++) {
				if (obj->measurevalues[i]) {
					unsigned int j;
					for (j = 0; j < obj->measurevalues_num2; j++) {
						if (obj->measurevalues[i][j]) {
							free(obj->measurevalues[i][j]);
							obj->measurevalues[i][j] = NULL;
						}
					}
					free(obj->measurevalues[i]);
					obj->measurevalues[i] = NULL;
				}
			}
			free(obj->measurevalues);
			obj->measurevalues = NULL;
		}
		obj->measurevalues_num1 = 0;
		obj->measurevalues_num2 = 0;
		if (obj->rowfieldtypes) {
			unsigned int i;
			for (i = 0; i < obj->rowfieldtypes_num; i++) {
				if (obj->rowfieldtypes[i]) {
					free(obj->rowfieldtypes[i]);
					obj->rowfieldtypes[i] = NULL;
				}
			}
			free(obj->rowfieldtypes);
			obj->rowfieldtypes = NULL;
		}
		obj->rowfieldtypes_num = 0;
		if (obj->rowkeys) {
			unsigned int i;
			for (i = 0; i < obj->rowkeys_num; i++) {
				if (obj->rowkeys[i]) {
					free(obj->rowkeys[i]);
					obj->rowkeys[i] = NULL;
				}
			}
			free(obj->rowkeys);
			obj->rowkeys = NULL;
		}
		obj->rowkeys_num = 0;
		if (obj->rowvalues) {
			unsigned int i;
			for (i = 0; i < obj->rowvalues_num1; i++) {
				if (obj->rowvalues[i]) {
					unsigned int j;
					for (j = 0; j < obj->rowvalues_num2; j++) {
						if (obj->rowvalues[i][j]) {
							free(obj->rowvalues[i][j]);
							obj->rowvalues[i][j] = NULL;
						}
					}
					free(obj->rowvalues[i]);
					obj->rowvalues[i] = NULL;
				}
			}
			free(obj->rowvalues);
			obj->rowvalues = NULL;
		}
		obj->rowvalues_num1 = 0;
		obj->rowvalues_num2 = 0;
		if (obj->title) {
			free(obj->title);
			obj->title = NULL;
		}
	}
	return OPH_JSON_SUCCESS;
}

/***********OPH_JSON_OBJ_MULTIGRID FUNCTIONS***********/

int oph_json_add_multigrid(oph_json * json, const char *objkey, const char *title, const char *description, char **rowkeys, int rowkeys_num, char **rowfieldtypes, int rowfieldtypes_num,
			   char **colkeys, int colkeys_num, char **colfieldtypes, int colfieldtypes_num, char ***colvalues, int colvalues_num, const char *measurename, const char *measuretype)
{
	if (!json || !objkey || !title || !rowkeys || rowkeys_num < 1 || !rowfieldtypes || rowfieldtypes_num < 1 || !colkeys || colkeys_num < 1 || !colfieldtypes || colfieldtypes_num < 1 || !colvalues
	    || colvalues_num < 1 || !measurename || !measuretype) {
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

		json->response[0].objclass = (char *) strdup(OPH_JSON_MULTIGRID);
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

		json->response[0].objcontent = malloc(sizeof(oph_json_obj_multigrid));
		if (!json->response[0].objcontent) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes_num = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys_num = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues_num1 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues_num2 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].description = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurename = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measuretype = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurevalues = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurevalues_num1 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurevalues_num2 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes_num = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys_num = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowvalues = NULL;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowvalues_num1 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowvalues_num2 = 0;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].title = NULL;

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].title = (char *) strdup(title);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].title) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
			return OPH_JSON_MEMORY_ERROR;
		}

		json->response[0].objcontent_num++;

		if (description) {
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].description = (char *) strdup(description);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].description) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
				return OPH_JSON_MEMORY_ERROR;
			}
		}

		int k, q;

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys = (char **) malloc(sizeof(char *) * rowkeys_num);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < rowkeys_num; k++) {
			for (q = 0; q < k; q++) {
				if (!strcmp(rowkeys[q], rowkeys[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys[k] = (char *) strdup(rowkeys[k]);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowkeys_num++;
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes = (char **) malloc(sizeof(char *) * rowfieldtypes_num);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < rowfieldtypes_num; k++) {
			if (!oph_json_is_type_correct(rowfieldtypes[k])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes[k] = (char *) strdup(rowfieldtypes[k]);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowfieldtypes_num++;
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys = (char **) malloc(sizeof(char *) * colkeys_num);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < colkeys_num; k++) {
			for (q = 0; q < k; q++) {
				if (!strcmp(colkeys[q], colkeys[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys[k] = (char *) strdup(colkeys[k]);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colkeys_num++;
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes = (char **) malloc(sizeof(char *) * colfieldtypes_num);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < colfieldtypes_num; k++) {
			if (!oph_json_is_type_correct(colfieldtypes[k])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes[k] = (char *) strdup(colfieldtypes[k]);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colfieldtypes_num++;
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues = (char ***) malloc(sizeof(char **) * colvalues_num);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
			return OPH_JSON_MEMORY_ERROR;
		}
		for (k = 0; k < colvalues_num; k++) {
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k] = NULL;
		}
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues_num1 = colvalues_num;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues_num2 = colkeys_num;
		for (k = 0; k < colvalues_num; k++) {
			((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k] = (char **) malloc(sizeof(char *) * colkeys_num);
			if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
				return OPH_JSON_MEMORY_ERROR;
			}

			for (q = 0; q < colkeys_num; q++) {
				((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k][q] = NULL;
			}
			for (q = 0; q < colkeys_num; q++) {
				((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k][q] = (char *) strdup(colvalues[k][q]);
				if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].colvalues[k][q]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
					return OPH_JSON_MEMORY_ERROR;
				}
			}
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurename = (char *) strdup(measurename);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurename) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
			return OPH_JSON_MEMORY_ERROR;
		}

		if (!oph_json_is_measuretype_correct(measuretype)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
			return OPH_JSON_BAD_PARAM_ERROR;
		}
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measuretype = (char *) strdup(measuretype);
		if (!((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measuretype) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
			return OPH_JSON_MEMORY_ERROR;
		}

		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].measurevalues_num2 = colvalues_num;
		((oph_json_obj_multigrid *) json->response[0].objcontent)[0].rowvalues_num2 = rowkeys_num;

	} else {
		unsigned int i;
		int add_frag = 0;
		for (i = 0; i < json->response_num; i++) {
			if (!strcmp(json->response[i].objkey, objkey)) {
				if (!strcmp(json->response[i].objclass, OPH_JSON_MULTIGRID)) {
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
			json->response[i].objcontent = realloc(json->response[i].objcontent, sizeof(oph_json_obj_multigrid) * (json->response[i].objcontent_num + 1));
			if (!json->response[i].objcontent) {
				json->response[i].objcontent = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes_num = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys_num = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].description = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurename = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measuretype = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurevalues = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurevalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurevalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes_num = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys_num = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowvalues = NULL;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowvalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowvalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].title = NULL;

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].title = (char *) strdup(title);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[i].objcontent_num++;

			if (description) {
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].description = (char *) strdup(description);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			int k, q;

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys = (char **) malloc(sizeof(char *) * rowkeys_num);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < rowkeys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(rowkeys[q], rowkeys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys[k] = (char *) strdup(rowkeys[k]);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowkeys_num++;
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes = (char **) malloc(sizeof(char *) * rowfieldtypes_num);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < rowfieldtypes_num; k++) {
				if (!oph_json_is_type_correct(rowfieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes[k] = (char *) strdup(rowfieldtypes[k]);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowfieldtypes_num++;
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys = (char **) malloc(sizeof(char *) * colkeys_num);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colkeys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(colkeys[q], colkeys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys[k] = (char *) strdup(colkeys[k]);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colkeys_num++;
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes = (char **) malloc(sizeof(char *) * colfieldtypes_num);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colfieldtypes_num; k++) {
				if (!oph_json_is_type_correct(colfieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes[k] = (char *) strdup(colfieldtypes[k]);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colfieldtypes_num++;
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues = (char ***) malloc(sizeof(char **) * colvalues_num);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colvalues_num; k++) {
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k] = NULL;
			}
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues_num1 = colvalues_num;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues_num2 = colkeys_num;
			for (k = 0; k < colvalues_num; k++) {
				((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k] = (char **) malloc(sizeof(char *) * colkeys_num);
				if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
					return OPH_JSON_MEMORY_ERROR;
				}

				for (q = 0; q < colkeys_num; q++) {
					((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k][q] = NULL;
				}
				for (q = 0; q < colkeys_num; q++) {
					((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k][q] = (char *) strdup(colvalues[k][q]);
					if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].colvalues[k][q]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
						return OPH_JSON_MEMORY_ERROR;
					}
				}
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurename = (char *) strdup(measurename);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurename) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
				return OPH_JSON_MEMORY_ERROR;
			}

			if (!oph_json_is_measuretype_correct(measuretype)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measuretype = (char *) strdup(measuretype);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measuretype) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
				return OPH_JSON_MEMORY_ERROR;
			}

			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].measurevalues_num2 = colvalues_num;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[index].rowvalues_num2 = rowkeys_num;

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

			json->response[index].objclass = (char *) strdup(OPH_JSON_MULTIGRID);
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

			json->response[index].objcontent = malloc(sizeof(oph_json_obj_multigrid));
			if (!json->response[index].objcontent) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "objcontent");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes_num = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys_num = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].description = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurename = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measuretype = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurevalues = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurevalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurevalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes_num = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys_num = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowvalues = NULL;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowvalues_num1 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowvalues_num2 = 0;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].title = NULL;

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].title = (char *) strdup(title);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].title) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "title");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "title");
				return OPH_JSON_MEMORY_ERROR;
			}

			json->response[index].objcontent_num++;

			if (description) {
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].description = (char *) strdup(description);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].description) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "description");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "description");
					return OPH_JSON_MEMORY_ERROR;
				}
			}

			int k, q;

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys = (char **) malloc(sizeof(char *) * rowkeys_num);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkeys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < rowkeys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(rowkeys[q], rowkeys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowkey");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys[k] = (char *) strdup(rowkeys[k]);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowkey");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowkeys_num++;
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes = (char **) malloc(sizeof(char *) * rowfieldtypes_num);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < rowfieldtypes_num; k++) {
				if (!oph_json_is_type_correct(rowfieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "rowfieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes[k] = (char *) strdup(rowfieldtypes[k]);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowfieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowfieldtypes_num++;
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys = (char **) malloc(sizeof(char *) * colkeys_num);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkeys");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colkeys_num; k++) {
				for (q = 0; q < k; q++) {
					if (!strcmp(colkeys[q], colkeys[k])) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colkey");
						return OPH_JSON_BAD_PARAM_ERROR;
					}
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys[k] = (char *) strdup(colkeys[k]);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colkey");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colkeys_num++;
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes = (char **) malloc(sizeof(char *) * colfieldtypes_num);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtypes");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colfieldtypes_num; k++) {
				if (!oph_json_is_type_correct(colfieldtypes[k])) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "colfieldtype");
					return OPH_JSON_BAD_PARAM_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes[k] = (char *) strdup(colfieldtypes[k]);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colfieldtype");
					return OPH_JSON_MEMORY_ERROR;
				}
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colfieldtypes_num++;
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues = (char ***) malloc(sizeof(char **) * colvalues_num);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			for (k = 0; k < colvalues_num; k++) {
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k] = NULL;
			}
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues_num1 = colvalues_num;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues_num2 = colkeys_num;
			for (k = 0; k < colvalues_num; k++) {
				((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k] = (char **) malloc(sizeof(char *) * colkeys_num);
				if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalues row");
					return OPH_JSON_MEMORY_ERROR;
				}

				for (q = 0; q < colkeys_num; q++) {
					((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k][q] = NULL;
				}
				for (q = 0; q < colkeys_num; q++) {
					((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k][q] = (char *) strdup(colvalues[k][q]);
					if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].colvalues[k][q]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "colvalue");
						return OPH_JSON_MEMORY_ERROR;
					}
				}
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurename = (char *) strdup(measurename);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurename) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurename");
				return OPH_JSON_MEMORY_ERROR;
			}

			if (!oph_json_is_measuretype_correct(measuretype)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_BAD_PARAM_ERROR, "measuretype");
				return OPH_JSON_BAD_PARAM_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measuretype = (char *) strdup(measuretype);
			if (!((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measuretype) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measuretype");
				return OPH_JSON_MEMORY_ERROR;
			}

			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].measurevalues_num2 = colvalues_num;
			((oph_json_obj_multigrid *) json->response[index].objcontent)[0].rowvalues_num2 = rowkeys_num;

		}
	}

	return OPH_JSON_SUCCESS;
}

int oph_json_add_multigrid_row(oph_json * json, const char *objkey, char **rowvalues, char **measurevalues)
{
	if (!json || !objkey || !rowvalues || !measurevalues) {
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
			if (!strcmp(json->response[i].objclass, OPH_JSON_MULTIGRID)) {
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
		if (((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num1 == 0) {
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues = (char ***) malloc(sizeof(char **));
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues = (char ***) malloc(sizeof(char **));
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = 0;
		} else {
			char ***tmp = ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues =
			    (char ***) realloc(((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues,
					       sizeof(char **) * (((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num1 + 1));
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues) {
				((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues = tmp;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			char ***tmp2 = ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues;
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues =
			    (char ***) realloc(((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues,
					       sizeof(char **) * (((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num1 + 1));
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues) {
				((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues = tmp2;
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues");
				return OPH_JSON_MEMORY_ERROR;
			}
			index = ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num1;
		}

		unsigned int k;

		((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues[index] =
		    (char **) malloc(sizeof(char *) * (((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num2));
		if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues[index]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues row");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalues row");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num1++;
		for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num2; k++) {
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues[index][k] = NULL;
		}

		((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues[index] =
		    (char **) malloc(sizeof(char *) * (((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues_num2));
		if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues[index]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues row");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalues row");
			return OPH_JSON_MEMORY_ERROR;
		}
		((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues_num1++;
		for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues_num2; k++) {
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues[index][k] = NULL;
		}

		for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues_num2; k++) {
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues[index][k] = (char *) strdup(measurevalues[k]);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].measurevalues[index][k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "measurevalue");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "measurevalue");
				return OPH_JSON_MEMORY_ERROR;
			}
		}

		for (k = 0; k < ((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues_num2; k++) {
			((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues[index][k] = (char *) strdup(rowvalues[k]);
			if (!((oph_json_obj_multigrid *) json->response[i].objcontent)[json->response[i].objcontent_num - 1].rowvalues[index][k]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_JSON_LOG_MEMORY_ERROR, "rowvalue");
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
