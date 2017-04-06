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

#include "oph_hierarchy_library.h"

#include <string.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/valid.h>
#include <time.h>
#include <ctype.h>

#include "debug.h"


extern int msglevel;

// Auxilia
int oph_hier_is_numeric(const char *s)
{
	if (s == NULL || *s == '\0' || isspace(*s))
		return 0;
	char *p;
	strtod(s, &p);
	return *p == '\0';
}

int oph_hier_basic_control(oph_hier_document * document)
{
	if (!document || !document->document) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Empty document\n");
		return OPH_HIER_DATA_ERR;
	}
	return OPH_HIER_OK;
}

int _oph_hier_order_list(int *ordered_list, xmlNodePtr * nodeTab, int n)
{
	int i, j;
	char temp = OPH_HIER_STR_ALL_SHORT_NAME[0];
	xmlAttrPtr name_attr;
	for (i = 0; i < n; ++i) {
		for (j = 0; j < n; ++j) {
			name_attr = xmlHasProp(nodeTab[j], (xmlChar *) OPH_HIER_STR_SHORT_NAME);
			if (!name_attr || !strlen((char *) name_attr->children->content)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s'\n", OPH_HIER_STR_SHORT_NAME);
				return OPH_HIER_XML_ERR;
			}
			if (temp == *((char *) name_attr->children->content)) {
				ordered_list[i] = j;
				name_attr = xmlHasProp(nodeTab[j], (xmlChar *) OPH_HIER_STR_AGGREGATE_FIELD);
				if (!name_attr || !strlen((char *) name_attr->children->content)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s'\n", OPH_HIER_STR_AGGREGATE_FIELD);
					return OPH_HIER_XML_ERR;
				}
				temp = *((char *) name_attr->children->content);
				break;
			}
		}
		if (j == n) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Attribute values are not correct\n");
			return OPH_HIER_SYSTEM_ERR;
		}
	}
	return OPH_HIER_OK;
}

int _oph_hier_get_attribute(xmlNodePtr node, oph_hier_attribute * attribute)
{
	int result = OPH_HIER_OK;
	xmlAttrPtr name_attr;

	if (result == OPH_HIER_OK) {
		name_attr = xmlHasProp(node, (xmlChar *) OPH_HIER_STR_LONG_NAME);
		if (!name_attr || !strlen((char *) name_attr->children->content)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s'\n", OPH_HIER_STR_LONG_NAME);
			result = OPH_HIER_XML_ERR;
		} else {
			strncpy(attribute->long_name, (char *) name_attr->children->content, OPH_HIER_MAX_STRING_LENGTH);
			attribute->long_name[OPH_HIER_MAX_STRING_LENGTH - 1] = 0;
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Property '%s' for attribute '%s' read\n", OPH_HIER_STR_LONG_NAME, attribute->long_name);
		}
	}

	if (result == OPH_HIER_OK) {
		name_attr = xmlHasProp(node, (xmlChar *) OPH_HIER_STR_SHORT_NAME);
		if (!name_attr || !strlen((char *) name_attr->children->content)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s' for attribute '%s'\n", OPH_HIER_STR_SHORT_NAME, attribute->long_name);
			result = OPH_HIER_XML_ERR;
		} else {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Property '%s' for attribute '%s' read\n", OPH_HIER_STR_SHORT_NAME, attribute->long_name);
			attribute->short_name = *((char *) name_attr->children->content);
		}
	}

	if (result == OPH_HIER_OK) {
		name_attr = xmlHasProp(node, (xmlChar *) OPH_HIER_STR_AGGREGATE_FIELD);
		if (!name_attr || !strlen((char *) name_attr->children->content)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s' for attribute '%s'\n", OPH_HIER_STR_AGGREGATE_FIELD, attribute->long_name);
			result = OPH_HIER_XML_ERR;
		} else {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Property '%s' for attribute '%s' read\n", OPH_HIER_STR_AGGREGATE_FIELD, attribute->long_name);
			strncpy(attribute->aggregate_field, (char *) name_attr->children->content, OPH_HIER_MAX_STRING_LENGTH);
			attribute->aggregate_field[OPH_HIER_MAX_STRING_LENGTH - 1] = 0;
		}
	}

	if (result == OPH_HIER_OK) {
		name_attr = xmlHasProp(node, (xmlChar *) OPH_HIER_STR_AGGREGATE_SET);
		if (!name_attr || !strlen((char *) name_attr->children->content)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s' for attribute '%s'\n", OPH_HIER_STR_AGGREGATE_SET, attribute->long_name);
			result = OPH_HIER_XML_ERR;
		} else if (!oph_hier_is_numeric((char *) name_attr->children->content)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Property '%s' of attribute '%s' must be numeric\n", OPH_HIER_STR_AGGREGATE_SET, attribute->long_name);
			result = OPH_HIER_XML_ERR;
		} else {
			attribute->aggregate_set = strtol((char *) name_attr->children->content, NULL, 10);
			if (!attribute->aggregate_set && (attribute->short_name != OPH_HIER_STR_ALL_SHORT_NAME[0])) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Aggregate set for attribute '%s' shall be positive\n", attribute->long_name);
				result = OPH_HIER_XML_ERR;
			} else
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Property '%s' for attribute '%s' read\n", OPH_HIER_STR_AGGREGATE_SET, attribute->long_name);
		}
	}

	if (result == OPH_HIER_OK) {
		name_attr = xmlHasProp(node, (xmlChar *) OPH_HIER_STR_AGGREGATE_OPERATION);
		if (!name_attr || !strlen((char *) name_attr->children->content))
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Missed property '%s'\n", OPH_HIER_STR_AGGREGATE_OPERATION);
		else {
			char aggregate_op[1 + OPH_HIER_MAX_STRING_LENGTH];
			strncpy(aggregate_op, (char *) name_attr->children->content, OPH_HIER_MAX_STRING_LENGTH);
			aggregate_op[OPH_HIER_MAX_STRING_LENGTH] = 0;
			oph_hier_list *list = attribute->aggregate_operation_list = (oph_hier_list *) malloc(sizeof(oph_hier_list));
			list->names = NULL;
			list->number = 0;
			unsigned int residual_length = strlen(aggregate_op);
			char *savepointer = NULL, *pch = strtok_r(aggregate_op, OPH_HIER_AGGREGATE_OP_SEPARATOR, &savepointer);
			if (!pch) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Missed property '%s' for attribute '%s'\n", OPH_HIER_STR_AGGREGATE_OPERATION, attribute->long_name);
				result = OPH_HIER_XML_ERR;
			} else
				while (pch) {
					residual_length -= strlen(pch);
					list->number++;
					pch = strtok_r(NULL, OPH_HIER_AGGREGATE_OP_SEPARATOR, &savepointer);
					if (pch)
						residual_length--;
				}
			if ((result == OPH_HIER_OK) && residual_length) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Some aggregate operations are not correctly reported for attribute '%s'\n", attribute->long_name);
				result = OPH_HIER_XML_ERR;
			}
			list->names = (char **) malloc(list->number * sizeof(char *));
			list->number = 0;
			strncpy(aggregate_op, (char *) name_attr->children->content, OPH_HIER_MAX_STRING_LENGTH);
			aggregate_op[OPH_HIER_MAX_STRING_LENGTH] = 0;
			pch = strtok_r(aggregate_op, OPH_HIER_AGGREGATE_OP_SEPARATOR, &savepointer);
			while (pch) {
				list->names[list->number++] = strndup(pch, OPH_HIER_MAX_STRING_LENGTH);
				pch = strtok_r(NULL, OPH_HIER_AGGREGATE_OP_SEPARATOR, &savepointer);
			}
		}
	}
	return result;
}

void oph_hier_free_strings(char **names, unsigned int number)
{
	if (names) {
		unsigned int i;
		for (i = 0; i < number; i++)
			if (names[i])
				free(names[i]);
		free(names);
	}
}

void oph_hier_free_list(oph_hier_list * list)
{
	if (list) {
		oph_hier_free_strings(list->names, list->number);
		free(list);
	}
}

// Main

int oph_hier_validate(oph_hier_document * document)
{
	int result;
	if ((result = oph_hier_basic_control(document)) != OPH_HIER_OK)
		return result;
	xmlDtdPtr dtd = xmlParseDTD(NULL, (xmlChar *) OPH_FRAMEWORK_HIERARCHY_DTD_PATH);
	if (!dtd) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "DTD file '%s' not found\n", OPH_FRAMEWORK_HIERARCHY_DTD_PATH);
		return OPH_HIER_IO_ERR;
	}
	xmlValidCtxtPtr cvp = xmlNewValidCtxt();
	if (!cvp) {
		xmlFreeDtd(dtd);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Context is not gettable\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	if (!xmlValidateDtd(cvp, document->document, dtd)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "File is not valid\n");
		result = OPH_HIER_XML_ERR;
	}
	xmlFreeValidCtxt(cvp);
	xmlFreeDtd(dtd);
	return result;
}

int oph_hier_open(oph_hier_document ** document_pointer, const char *filename)
{
	if (!document_pointer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	if (!filename || !strlen(filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Filename not found\n");
		return OPH_HIER_DATA_ERR;
	}
	if (*document_pointer)
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Document is not empty\n");
	xmlDocPtr xml_doc = xmlReadFile(filename, 0, 0);
	if (!xml_doc) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in opening '%s'\n", filename);
		return OPH_HIER_IO_ERR;
	}
	*document_pointer = (oph_hier_document *) malloc(sizeof(oph_hier_document));
	if (!(*document_pointer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocation error\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	(*document_pointer)->document = xml_doc;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Document opened\n");
	return OPH_HIER_OK;
}

int oph_hier_close(oph_hier_document * document)
{
	int result;
	if ((result = oph_hier_basic_control(document)) != OPH_HIER_OK)
		return result;
	xmlFreeDoc(document->document);
	xmlCleanupParser();
	free(document);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Document closed\n");
	return OPH_HIER_OK;
}

int oph_hier_get_hierarchy(oph_hier_hierarchy * hierarchy, oph_hier_document * document)
{
	if (!hierarchy) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	int result;
	if ((result = oph_hier_basic_control(document)) != OPH_HIER_OK)
		return result;
	xmlXPathContextPtr context = xmlXPathNewContext(document->document);
	if (!context) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Context is not gettable\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	char target[OPH_HIER_MAX_STRING_LENGTH];
	sprintf(target, "//%s", OPH_HIER_STR_HIERARCHY);
	xmlXPathObjectPtr object = xmlXPathEvalExpression((xmlChar *) target, context);
	if (!object) {
		xmlXPathFreeContext(context);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "XPath error\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	xmlNodeSetPtr nodeset = object->nodesetval;
	if (!nodeset || !nodeset->nodeNr) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "No hierarchy found\n");
		result = OPH_HIER_DATA_ERR;
	} else if (nodeset->nodeNr > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Malformed file\n");
		result = OPH_HIER_XML_ERR;
	} else {
		hierarchy->attribute_number = 0;
		xmlNodePtr name_node = nodeset->nodeTab[0]->children;
		while (name_node) {
			if (name_node->type == XML_ELEMENT_NODE)
				hierarchy->attribute_number++;
			if (name_node == nodeset->nodeTab[0]->last)
				break;
			else
				name_node = name_node->next;
		}
		if (hierarchy->attribute_number) {
			hierarchy->attributes = (oph_hier_attribute **) malloc(hierarchy->attribute_number * sizeof(oph_hier_attribute *));
			name_node = nodeset->nodeTab[0]->children;
			unsigned int kk, last_kk;
			xmlNodePtr saved_names[hierarchy->attribute_number];
			for (kk = 0; kk < hierarchy->attribute_number; ++kk) {
				hierarchy->attributes[kk] = 0;
				while ((name_node->type != XML_ELEMENT_NODE) && (name_node != nodeset->nodeTab[0]->last))
					name_node = name_node->next;
				if (name_node->type != XML_ELEMENT_NODE) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Internal error\n");
					result = OPH_HIER_SYSTEM_ERR;
					break;
				}
				hierarchy->attributes[kk] = (oph_hier_attribute *) malloc(sizeof(oph_hier_attribute));
				saved_names[kk] = name_node;
				name_node = name_node->next;
			}
			last_kk = kk;
			if (result == OPH_HIER_OK) {
				for (kk = 0; kk < hierarchy->attribute_number; ++kk) {
					if ((result = _oph_hier_get_attribute(saved_names[kk], hierarchy->attributes[kk])) != OPH_HIER_OK)
						break;
				}
			}
			if (result != OPH_HIER_OK)	// Free - roll-back
			{
				for (kk = 0; kk < last_kk; kk++)
					if (hierarchy->attributes[kk])
						free(hierarchy->attributes[kk]);
				free(hierarchy->attributes);
				hierarchy->attributes = 0;
			}
		} else
			hierarchy->attributes = 0;
	}
	xmlXPathFreeObject(object);
	xmlXPathFreeContext(context);
	return result;
}

int oph_hier_get_attributes(oph_hier_list ** list, const char *hierarchy, oph_hier_document * document)
{
	if (!hierarchy || !strlen(hierarchy)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Hierarchy not given\n");
		return OPH_HIER_DATA_ERR;
	}
	if (!list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	int result;
	if ((result = oph_hier_basic_control(document)) != OPH_HIER_OK)
		return result;
	xmlXPathContextPtr context = xmlXPathNewContext(document->document);
	if (!context) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Context is not gettable\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	char target[OPH_HIER_MAX_STRING_LENGTH];
	sprintf(target, "//%s[@%s='%s']/%s", OPH_HIER_STR_HIERARCHY, OPH_HIER_STR_NAME, hierarchy, OPH_HIER_STR_ATTRIBUTE);
	xmlXPathObjectPtr object = xmlXPathEvalExpression((xmlChar *) target, context);
	if (!object) {
		xmlXPathFreeContext(context);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "XPath error\n");
		return OPH_HIER_SYSTEM_ERR;
	}

	*list = (oph_hier_list *) malloc(sizeof(oph_hier_list));

	xmlNodeSetPtr nodeset = object->nodesetval;
	if (!nodeset)
		(*list)->number = 0;
	else
		(*list)->number = nodeset->nodeNr;

	if (!((*list)->number))
		(*list)->names = 0;
	else {
		char **names_ = (*list)->names = (char **) malloc(nodeset->nodeNr * sizeof(char *));
		int ordered_list[nodeset->nodeNr];
		if ((result = _oph_hier_order_list(ordered_list, nodeset->nodeTab, nodeset->nodeNr)) == OPH_HIER_OK) {
			int i;
			for (i = 0; i < nodeset->nodeNr; ++i) {
				if (nodeset->nodeTab[i]) {
					names_[ordered_list[i]] = strndup((char *) nodeset->nodeTab[i]->children->content, OPH_HIER_MAX_STRING_LENGTH);
					if (!names_[ordered_list[i]]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocation error\n");
						result = OPH_HIER_SYSTEM_ERR;
						break;
					}
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "Attribute taken\n");
				} else {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Attribute lost\n");
					result = OPH_HIER_SYSTEM_ERR;
					break;
				}
			}
		}
	}
	xmlXPathFreeObject(object);
	xmlXPathFreeContext(context);
	if (result == OPH_HIER_OK)
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Attributes read\n");
	return result;
}

int oph_hier_get_attribute(const char *name, oph_hier_attribute * attribute, const char *hierarchy, oph_hier_document * document)
{
	if (!name || !strlen(name)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Property '%s' not given\n", OPH_HIER_STR_NAME);
		return OPH_HIER_DATA_ERR;
	}
	if (!hierarchy || !strlen(hierarchy))
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Hierarchy not given\n", OPH_HIER_STR_NAME);
	int result;
	if ((result = oph_hier_basic_control(document)) != OPH_HIER_OK)
		return result;
	xmlXPathContextPtr context = xmlXPathNewContext(document->document);
	if (!context) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Context is not gettable\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	char target[OPH_HIER_MAX_STRING_LENGTH];
	if (hierarchy && (strlen(hierarchy) > 0))
		sprintf(target, "//%s[@%s='%s']/%s[@%s='%s']", OPH_HIER_STR_HIERARCHY, OPH_HIER_STR_NAME, hierarchy, OPH_HIER_STR_ATTRIBUTE, OPH_HIER_STR_LONG_NAME, name);
	else
		sprintf(target, "//%s[@%s()='%s']", OPH_HIER_STR_ATTRIBUTE, OPH_HIER_STR_LONG_NAME, name);
	xmlXPathObjectPtr object = xmlXPathEvalExpression((xmlChar *) target, context);
	if (!object) {
		xmlXPathFreeContext(context);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "XPath error\n");
		return OPH_HIER_SYSTEM_ERR;
	}
	xmlNodeSetPtr nodeset = object->nodesetval;
	if (!nodeset || !nodeset->nodeNr) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No attribute '%s'\n", name);
		result = OPH_HIER_DATA_ERR;
	} else if ((nodeset->nodeNr > 1) && hierarchy && (strlen(hierarchy) > 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Malformed file\n");
		result = OPH_HIER_XML_ERR;
	} else
		result = _oph_hier_get_attribute(nodeset->nodeTab[0], attribute);
	xmlXPathFreeObject(object);
	xmlXPathFreeContext(context);
	if (result == OPH_HIER_OK)
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Attribute '%s' read\n", name);
	return result;
}

void oph_hier_free_hierarchy(oph_hier_hierarchy * name)
{
	if (name) {
		if (name->attributes) {
			unsigned int i;
			for (i = 0; i < name->attribute_number; ++i)
				oph_hier_free_attribute(name->attributes[i]);
			free(name->attributes);
		}
		free(name);
	}
}

void oph_hier_free_attribute(oph_hier_attribute * name)
{
	if (name) {
		oph_hier_free_list(name->aggregate_operation_list);
		free(name);
	}
}

// Main functions

int oph_hier_check_concept_level_long(const char *filename, char *concept_level_long, int *exists, char *short_name)
{
	if (!filename || !concept_level_long || !exists || !short_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	*exists = 0;
	*short_name = 0;

	oph_hier_document *document = NULL;
	if (oph_hier_open(&document, filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' cannot be opened\n", filename);
		return OPH_HIER_IO_ERR;
	}

	if (oph_hier_validate(document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' is not valid\n", filename);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	oph_hier_hierarchy *hierarchy = (oph_hier_hierarchy *) malloc(sizeof(oph_hier_hierarchy));
	if (oph_hier_get_hierarchy(hierarchy, document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Hierarchy cannot be retrieved\n");
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	unsigned int i;
	for (i = 0; i < hierarchy->attribute_number; ++i)
		if (!strncasecmp(hierarchy->attributes[i]->long_name, concept_level_long, OPH_HIER_MAX_STRING_LENGTH)) {
			*exists = 1;
			*short_name = hierarchy->attributes[i]->short_name;
			break;
		}

	oph_hier_free_hierarchy(hierarchy);
	oph_hier_close(document);

	return OPH_HIER_OK;
}

int oph_hier_check_concept_level_short(const char *filename, char concept_level_short, int *exists)
{
	if (!filename || !exists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	*exists = 0;

	oph_hier_document *document = NULL;
	if (oph_hier_open(&document, filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' cannot be opened\n", filename);
		return OPH_HIER_IO_ERR;
	}

	if (oph_hier_validate(document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' is not valid\n", filename);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	oph_hier_hierarchy *hierarchy = (oph_hier_hierarchy *) malloc(sizeof(oph_hier_hierarchy));
	if (oph_hier_get_hierarchy(hierarchy, document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Hierarchy cannot be retrieved\n");
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	unsigned int i;
	for (i = 0; i < hierarchy->attribute_number; ++i)
		if (hierarchy->attributes[i]->short_name == concept_level_short) {
			*exists = 1;
			break;
		}

	oph_hier_free_hierarchy(hierarchy);
	oph_hier_close(document);

	return OPH_HIER_OK;
}

int oph_hier_retrieve_available_op(const char *filename, char concept_level_in, char concept_level_out, oph_hier_list ** available_op, int *aggregate_set)
{
	if (!filename || !available_op || !aggregate_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	*available_op = NULL;
	*aggregate_set = 0;

	oph_hier_document *document = NULL;
	if (oph_hier_open(&document, filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' cannot be opened\n", filename);
		return OPH_HIER_IO_ERR;
	}
	oph_hier_hierarchy *hierarchy = (oph_hier_hierarchy *) malloc(sizeof(oph_hier_hierarchy));
	if (oph_hier_get_hierarchy(hierarchy, document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Hierarchy cannot be retrieved\n");
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	unsigned int i, j, k;
	for (i = 0; i < hierarchy->attribute_number; ++i)
		if (hierarchy->attributes[i]->short_name == concept_level_in)
			break;
	if (i == hierarchy->attribute_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Concept level '%c' not found\n", concept_level_in);
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_DATA_ERR;
	}
	for (j = 0; j < hierarchy->attribute_number; ++j)
		if (hierarchy->attributes[j]->short_name == concept_level_out)
			break;
	if (j == hierarchy->attribute_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Concept level '%c' not found\n", concept_level_out);
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_DATA_ERR;
	}

	if (!i || (i <= j)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data reduction cannot be carried out\n");
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_DATA_ERR;
	}

	oph_hier_list *result = *available_op = (oph_hier_list *) malloc(sizeof(oph_hier_list));
	result->number = hierarchy->attributes[j]->aggregate_operation_list->number;
	result->names = (char **) malloc(result->number * sizeof(char *));
	for (k = 0; k < result->number; ++k)
		result->names[k] = strndup(hierarchy->attributes[j]->aggregate_operation_list->names[k], OPH_HIER_MAX_STRING_LENGTH);

	if (j)
		for (*aggregate_set = 1, --i; i >= j; --i) {
			if ((hierarchy->attributes[i]->short_name != 'w') || (hierarchy->attributes[j]->short_name == 'w'))
				*aggregate_set *= hierarchy->attributes[i]->aggregate_set;
		}

	oph_hier_free_hierarchy(hierarchy);
	oph_hier_close(document);

	return OPH_HIER_OK;
}

int oph_hier_get_concept_level_long(const char *filename, char concept_level_short, char **concept_level_long)
{
	if (!filename || !concept_level_long) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return OPH_HIER_DATA_ERR;
	}
	*concept_level_long = 0;

	oph_hier_document *document = NULL;
	if (oph_hier_open(&document, filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' cannot be opened\n", filename);
		return OPH_HIER_IO_ERR;
	}

	if (oph_hier_validate(document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "File '%s' is not valid\n", filename);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	oph_hier_hierarchy *hierarchy = (oph_hier_hierarchy *) malloc(sizeof(oph_hier_hierarchy));
	if (oph_hier_get_hierarchy(hierarchy, document)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Hierarchy cannot be retrieved\n");
		oph_hier_free_hierarchy(hierarchy);
		oph_hier_close(document);
		return OPH_HIER_XML_ERR;
	}

	unsigned int i;
	for (i = 0; i < hierarchy->attribute_number; ++i)
		if (hierarchy->attributes[i]->short_name == concept_level_short) {
			*concept_level_long = strndup(hierarchy->attributes[i]->long_name, OPH_HIER_MAX_STRING_LENGTH);
			break;
		}

	oph_hier_free_hierarchy(hierarchy);
	oph_hier_close(document);

	return OPH_HIER_OK;
}
