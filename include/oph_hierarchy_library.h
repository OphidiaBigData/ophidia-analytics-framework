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

#ifndef OPH_HIER_CUBOIDXML_H
#define OPH_HIER_CUBOIDXML_H

// include and define

#include <stdio.h>
#include <libxml/tree.h>
#include "oph_common.h"

#define OPH_HIER_MAX_STRING_LENGTH		1024	// chars
#define OPH_HIER_AGGREGATE_OP_SEPARATOR		"|"

// XML names
#define OPH_HIER_STR_HIERARCHY			"hierarchy"
#define OPH_HIER_STR_NAME			"name"
#define OPH_HIER_STR_ATTRIBUTE			"attribute"
#define OPH_HIER_STR_LONG_NAME			"long_name"
#define OPH_HIER_STR_SHORT_NAME			"short_name"
#define OPH_HIER_STR_AGGREGATE_FIELD		"aggregate_field"
#define OPH_HIER_STR_AGGREGATE_SET		"aggregate_set"
#define OPH_HIER_STR_AGGREGATE_OPERATION	"aggregate_op"
#define OPH_HIER_STR_ALL_LONG_NAME		"ALL"
#define OPH_HIER_STR_ALL_SHORT_NAME		"A"

// Time
#define OPH_HIER_MINUTE_LONG_NAME		"minute"
#define OPH_HIER_MINUTE_SHORT_NAME		"m"
#define OPH_HIER_MONTH_LONG_NAME		"month"
#define OPH_HIER_MONTH_SHORT_NAME		"M"

// error codes
#define OPH_HIER_OK				0
#define OPH_HIER_DATA_ERR			1
#define OPH_HIER_IO_ERR				2
#define OPH_HIER_SYSTEM_ERR			3
#define OPH_HIER_XML_ERR			4

// enum and struct

typedef struct {
	xmlDocPtr document;
} oph_hier_document;

typedef struct {
	char **names;
	unsigned int number;
} oph_hier_list;

typedef struct {
	char long_name[OPH_HIER_MAX_STRING_LENGTH];
	char short_name;
	char aggregate_field[OPH_HIER_MAX_STRING_LENGTH];
	unsigned int aggregate_set;
	oph_hier_list *aggregate_operation_list;
} oph_hier_attribute;

typedef struct {
	//char name[OPH_HIER_MAX_STRING_LENGTH];
	oph_hier_attribute **attributes;
	unsigned int attribute_number;
} oph_hier_hierarchy;

//Auxilia
int oph_hier_is_numeric(const char *s);
int oph_hier_basic_control(oph_hier_document * document);
void oph_hier_free_strings(char **names, unsigned int number);

// Prototypes

int oph_hier_open(oph_hier_document ** document_pointer, const char *filename);
int oph_hier_close(oph_hier_document * document);

int oph_hier_validate(oph_hier_document * document);

int oph_hier_get_hierarchy(oph_hier_hierarchy * hierarchy, oph_hier_document * document);

int oph_hier_get_attributes(oph_hier_list ** list, const char *hierarchy, oph_hier_document * document);
int oph_hier_get_attribute(const char *name, oph_hier_attribute * attribute, const char *hierarchy, oph_hier_document * document);

void oph_hier_free_hierarchy(oph_hier_hierarchy * name);
void oph_hier_free_attribute(oph_hier_attribute * name);
void oph_hier_free_list(oph_hier_list * list);

// Main functions

int oph_hier_check_concept_level_long(const char *filename, char *concept_level_long, int *exists, char *short_name);
int oph_hier_check_concept_level_short(const char *filename, char concept_level_short, int *exists);
int oph_hier_retrieve_available_op(const char *filename, char concept_level_in, char concept_level_out, oph_hier_list ** available_op, int *aggregate_set);
int oph_hier_get_concept_level_long(const char *filename, char concept_level_short, char **concept_level_long);

#endif				/* OPH_HIER_CUBOIDXML_H */
