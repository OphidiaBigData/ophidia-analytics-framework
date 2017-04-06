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

#include "oph_render_output_library.h"
#define _GNU_SOURCE

/* Standard C99 headers */
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "debug.h"

#include "oph_task_parser_library.h"
#include "oph_json_library.h"
#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xmlmemory.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

extern int msglevel;

int renderXML(const char *xmlfilename, short int function_type_code, oph_json * oper_json, char **objkeys, int objkeys_num)
{
	/*
	 * this initialize the library and check potential ABI mismatches
	 * between the version it was compiled for and the actual shared
	 * library used.
	 */

	xmlParserCtxtPtr ctxt;
	xmlDocPtr doc;
	xmlNodePtr node;
	xmlChar *content, *content2;
	xmlXPathContextPtr xpathCtx;
	xmlXPathObjectPtr xpathObj;

	if (function_type_code != OPH_TP_XML_PRIMITIVE_TYPE_CODE && function_type_code != OPH_TP_XML_OPERATOR_TYPE_CODE && function_type_code != OPH_TP_XML_HIERARCHY_TYPE_CODE)
		return OPH_RENDER_OUTPUT_RENDER_ERROR;

	/* create a parser context */
	ctxt = xmlNewParserCtxt();
	if (ctxt == NULL) {
		fprintf(stderr, "Failed to allocate parser context\n");
		return OPH_RENDER_OUTPUT_RENDER_ERROR;
	}
	//Create validation context
	xmlValidCtxtPtr ctxt1;
	ctxt1 = xmlNewValidCtxt();
	if (ctxt1 == NULL) {
		fprintf(stderr, "Failed to allocate valid context\n");
		xmlFreeParserCtxt(ctxt);
		return OPH_RENDER_OUTPUT_RENDER_ERROR;
	}
	//Parse the DTD file
	doc = xmlCtxtReadFile(ctxt, xmlfilename, NULL, 0);
	xmlDtdPtr dtd;
	if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
		dtd = xmlParseDTD(NULL, (xmlChar *) OPH_FRAMEWORK_PRIMITIVE_DTD_PATH);
	else if (function_type_code == OPH_TP_XML_OPERATOR_TYPE_CODE)
		dtd = xmlParseDTD(NULL, (xmlChar *) OPH_FRAMEWORK_OPERATOR_DTD_PATH);
	else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE)
		dtd = xmlParseDTD(NULL, (xmlChar *) OPH_FRAMEWORK_HIERARCHY_DTD_PATH);

	if (dtd == NULL) {
		fprintf(stderr, "Failed to parse %s\n", xmlfilename);
		xmlFreeDoc(doc);
		xmlFreeValidCtxt(ctxt1);
		xmlFreeParserCtxt(ctxt);
		return OPH_RENDER_OUTPUT_RENDER_ERROR;
	}
	//Validate document
	if (!xmlValidateDtd(ctxt1, doc, dtd)) {
		fprintf(stderr, "Failed to validate %s\n", xmlfilename);
		xmlFreeDtd(dtd);
		xmlFreeDoc(doc);
		xmlFreeValidCtxt(ctxt1);
		xmlFreeParserCtxt(ctxt);
		return OPH_RENDER_OUTPUT_RENDER_INVALID_XML;
	}
	xmlFreeDtd(dtd);
	xmlFreeValidCtxt(ctxt1);

	/* Create xpath evaluation context */
	xpathCtx = xmlXPathNewContext(doc);
	if (xpathCtx == NULL) {
		fprintf(stderr, "Error: unable to create new XPath context\n");
		xmlFreeDoc(doc);
		xmlFreeParserCtxt(ctxt);
		return OPH_RENDER_OUTPUT_RENDER_ERROR;
	}

	unsigned int i, j = 0;

	/*
	 *
	 * PRINT DESCRIPTION - FUNCTION NAME & VERSION
	 *
	 */

	//print line
	char *header_line = NULL;
	if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE) {
		header_line = malloc(OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH + 3);
		if (!header_line) {
			fprintf(stderr, "Memory error\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		memset(header_line, '\0', OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH + 3);
	} else {
		header_line = malloc(OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH + 3);
		if (!header_line) {
			fprintf(stderr, "Memory error\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		memset(header_line, '\0', OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH + 3);
	}
	header_line[0] = '+';
	if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE) {
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH; i++) {
			header_line[i] = '=';
		}
	} else {
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH; i++) {
			header_line[i] = '=';
		}
	}
	header_line[i] = '+';
	printf("%s\n", header_line);

	//NAME & VERSION start
	printf("|");

	if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
		xpathObj = xmlXPathEvalExpression((const xmlChar *) "/" OPH_TP_XML_PRIMITIVE_TYPE, xpathCtx);
	else if (function_type_code == OPH_TP_XML_OPERATOR_TYPE_CODE)
		xpathObj = xmlXPathEvalExpression((const xmlChar *) "/" OPH_TP_XML_OPERATOR_TYPE, xpathCtx);
	else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE)
		xpathObj = xmlXPathEvalExpression((const xmlChar *) "/" OPH_TP_XML_HIERARCHY_TYPE, xpathCtx);
	if (xpathObj == NULL) {
		fprintf(stderr, "Error: unable to evaluate xpath expression\n");
		xmlXPathFreeContext(xpathCtx);
		xmlFreeDoc(doc);
		xmlFreeParserCtxt(ctxt);
		free(header_line);
		return OPH_RENDER_OUTPUT_RENDER_ERROR;
	}
	node = xpathObj->nodesetval->nodeTab[0];
	content = xmlGetProp(node, (const xmlChar *) "name");
	content2 = xmlGetProp(node, (const xmlChar *) "version");

	//print name + version
	if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE) {
		printf("%*s%s%s%s%*s",
		       (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 + (int) strlen("PRIMITIVE: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
												    (int) strlen((char *) content2) / 2), "PRIMITIVE: ", content, " V: ", content2,
		       (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 - (int) strlen("PRIMITIVE: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
												    (int) strlen((char *) content2) / 2) - (((int) strlen((char *) content)) % 2) -
		       (((int) strlen((char *) content2)) % 2), "");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_FUNCTION)) {
			char tmpbuf[OPH_COMMON_BUFFER_LEN];
			snprintf(tmpbuf, OPH_COMMON_BUFFER_LEN, "%s v%s", content, content2);
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_MAN_FUNCTION, "Function Name and Version", tmpbuf)) {
				fprintf(stderr, "ADD TEXT error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
	} else if (function_type_code == OPH_TP_XML_OPERATOR_TYPE_CODE) {
		printf("%*s%s%s%s%*s",
		       (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 + (int) strlen("OPERATOR: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
												   (int) strlen((char *) content2) / 2), "OPERATOR: ", content, " V: ", content2,
		       (OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 - (int) strlen("OPERATOR: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
												   (int) strlen((char *) content2) / 2) - (((int) strlen((char *) content)) % 2) -
		       (((int) strlen((char *) content2)) % 2), "");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_FUNCTION)) {
			char tmpbuf[OPH_COMMON_BUFFER_LEN];
			snprintf(tmpbuf, OPH_COMMON_BUFFER_LEN, "%s v%s", content, content2);
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_MAN_FUNCTION, "Function Name and Version", tmpbuf)) {
				fprintf(stderr, "ADD TEXT error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
	} else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE) {
		printf("%*s%s%s%s%*s",
		       (OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH / 2 + (int) strlen("HIERARCHY: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
													  (int) strlen((char *) content2) / 2), "HIERARCHY: ", content, " V: ", content2,
		       (OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH / 2 - (int) strlen("HIERARCHY: ") / 2) - ((int) strlen((char *) content) / 2 + (int) strlen((char *) " V: ") / 2 +
													  (int) strlen((char *) content2) / 2) - (((int) strlen((char *) content)) % 2) -
		       (((int) strlen((char *) content2)) % 2), "");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_FUNCTION)) {
			char tmpbuf[OPH_COMMON_BUFFER_LEN];
			snprintf(tmpbuf, OPH_COMMON_BUFFER_LEN, "%s v%s", content, content2);
			if (oph_json_add_text(oper_json, OPH_JSON_OBJKEY_HIERARCHY_FUNCTION, "Function Name and Version", tmpbuf)) {
				fprintf(stderr, "ADD TEXT error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
	}
	printf("|\n");

	xmlFree(content);
	xmlFree(content2);
	xmlXPathFreeObject(xpathObj);
	//NAME & VERSION end

	//print line
	printf("%s\n", header_line);

	/*
	 *
	 * PRINT DESCRIPTION
	 *
	 */

	//INFO start

	if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE) {

		/*
		 * following code is for hierarchies only
		 */

		char inner_header_line[OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH + 3] = { '\0' };

		//print line (between rows)
		char row_line[OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH + 3] = { '\0' };
		row_line[0] = '|';
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH; i++) {
			if (i == 16)
				row_line[i] = '|';
			else
				row_line[i] = '-';
		}
		row_line[i] = '|';

		/*
		 *
		 * PRINT DESCRIPTION - FUNCTION ARGUMENTS
		 *
		 */
		int n;

		//ARGUMENTS start
		//print line
		printf("%s\n", header_line);
		printf("|");
		//print args header
		printf("%*s%*s", OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH / 2 + (int) strlen("CONCEPT LEVELS") / 2, "CONCEPT LEVELS",
		       OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH / 2 - (int) strlen("CONCEPT LEVELS") / 2, "");
		printf("|\n");

		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//attribute", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		if (xpathObj->nodesetval->nodeNr == 0) {
			//print line
			printf("%s\n", header_line);
			printf("| %-*s |\n", OPH_RENDER_OUTPUT_HIERARCHY_TABLE_WIDTH - 2, "No arguments are available");
			//print line
			printf("%s\n\n", header_line);

			xmlXPathFreeObject(xpathObj);
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_OK;
		}
		//print line
		int k = 0;
		inner_header_line[0] = '+';
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH + 2; i++) {
			inner_header_line[i] = '=';
		}
		inner_header_line[i] = '+';
		k = i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		printf("%s\n", inner_header_line);

		k = 0;
		row_line[0] = '|';
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH + 2; i++) {
			row_line[i] = '-';
		}
		row_line[i] = '|';
		k = i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';

		//print args columns
		printf("| %-*s | %-*s | %-*s | %-*s | %-*s |\n",
		       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, "LONG NAME",
		       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, "SHORT NAME",
		       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, "AGGREGATION FIELD",
		       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, "AGGREGATION SET", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH, "AGGREGATION FUNCTIONS");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS)) {
			int num_fields = 5;
			char *keys[5] = { "LONG NAME", "SHORT NAME", "AGGREGATION FIELD", "AGGREGATION SET", "AGGREGATION FUNCTIONS" };
			char *fieldtypes[5] = { "string", "string", "string", "string", "string" };
			if (oph_json_add_grid(oper_json, OPH_JSON_OBJKEY_HIERARCHY_ATTRS, "Hierarchy Attributes", NULL, keys, num_fields, fieldtypes, num_fields)) {
				fprintf(stderr, "ADD GRID error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
		//print line
		printf("%s\n", inner_header_line);

		for (n = 0; n < xpathObj->nodesetval->nodeNr; n++) {

			node = xpathObj->nodesetval->nodeTab[n];

			if (n != 0) {
				//print line
				printf("%s\n", row_line);
			}

			printf("| ");

			char my_row1[OPH_COMMON_BUFFER_LEN];
			char my_row2[OPH_COMMON_BUFFER_LEN];
			char my_row3[OPH_COMMON_BUFFER_LEN];
			char my_row4[OPH_COMMON_BUFFER_LEN];
			char my_row5[OPH_COMMON_BUFFER_LEN];
			char *my_row[5] = { my_row1, my_row2, my_row3, my_row4, my_row5 };

			//LONG NAME
			content = xmlGetProp(node, (const xmlChar *) "long_name");
			printf("%-*s | ", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
				snprintf(my_row[0], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//SHORT NAME
			content = xmlGetProp(node, (const xmlChar *) "short_name");
			printf("%-*s | ", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
				snprintf(my_row[1], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//AGGREGATION FIELD
			content = xmlGetProp(node, (const xmlChar *) "aggregate_field");
			printf("%-*s | ", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
				snprintf(my_row[2], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//AGGREGATION SET
			content = xmlGetProp(node, (const xmlChar *) "aggregate_set");
			printf("%-*s | ", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
				snprintf(my_row[3], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//AGGREGATION OPERATIONS
			content = xmlGetProp(node, (const xmlChar *) "aggregate_op");
			if (!content) {
				printf("%-*s |\n", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH, "");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
					snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", "");
			} else {
				j = 0;
				for (i = 0; i < strlen((char *) content); i++) {
					if (content[i] == '\t') {
						if (j == OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH) {
							printf(" |\n| %-*s | %-*s | %-*s | %-*s | ",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, "", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, "");
							j = 0;
						}
						printf(" ");
						j++;
					} else if (content[i] == '\n') {
						if (i != 0) {
							if (j == OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH) {
								printf(" |\n| %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, "", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, "");
							} else {
								printf("%-*s |\n| %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH - j, "",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, "", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, "");
							}
							j = 0;
						}
						printf(" ");
						j++;
					} else {
						if (j == OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH) {
							printf(" |\n| %-*s | %-*s | %-*s | %-*s | ",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_LNAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_SNAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_FIELD_WIDTH, "", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_SET_WIDTH, "");
							j = 0;
						}
						printf("%c", content[i]);
						j++;
					}
				}
				if (j == OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH) {
					printf(" |\n");
				} else {
					printf("%-*s |\n", OPH_RENDER_OUTPUT_HIERARCHY_ATTR_AGG_OP_WIDTH - j, "");
				}
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS))
					snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
			}

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_HIERARCHY_ATTRS)) {
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_HIERARCHY_ATTRS, my_row)) {
					fprintf(stderr, "ADD GRID ROW error\n");
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
			}
		}
		xmlXPathFreeObject(xpathObj);

		//print line
		printf("%s\n\n", inner_header_line);
		//ARGUMENTS end

		/* Cleanup of XPath context */
		xmlXPathFreeContext(xpathCtx);
		/* free up the resulting document */
		xmlFreeDoc(doc);
		/* free up the parser context */
		xmlFreeParserCtxt(ctxt);
		free(header_line);
		return OPH_RENDER_OUTPUT_RENDER_OK;

	} else {

		/*
		 * following code is for primitives or operators only
		 */

		printf("%s\n", header_line);

		printf("|");
		//print info header
		printf("%*s%*s", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 + (int) strlen("INFORMATION") / 2, "INFORMATION", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 - (int) strlen("INFORMATION") / 2, "");
		printf("|\n");
		//print line
		char inner_header_line[OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH + 3] = { '\0' };
		inner_header_line[0] = '+';
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH; i++) {
			if (i == 16)
				inner_header_line[i] = '+';
			else
				inner_header_line[i] = '=';
		}
		inner_header_line[i] = '+';
		printf("%s\n", inner_header_line);
		//print info columns
		printf("| ");
		printf("%-13s | %-*s ", "INFO TYPE", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18, "INFO VALUE");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
			int num_fields = 2;
			char *keys[2] = { "INFO TYPE", "INFO VALUE" };
			char *fieldtypes[2] = { "string", "string" };
			if (oph_json_add_grid(oper_json, OPH_JSON_OBJKEY_MAN_INFO, "Function Info", NULL, keys, num_fields, fieldtypes, num_fields)) {
				fprintf(stderr, "ADD GRID error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
		printf("|\n");
		//print line
		printf("%s\n", inner_header_line);
		//INFO end

		//ABSTRACT start
		printf("| ");

		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//abstract", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		node = xpathObj->nodesetval->nodeTab[0];
		content = xmlNodeGetContent(node);

		//print abstract
		printf("%-13s | ", "Abstract");
		for (i = 0; i < strlen((char *) content); i++) {
			if (content[i] == '\t') {
				if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
					printf(" |\n| %-13s | ", "");
					j = 0;
				}
				printf(" ");
				j++;
			} else if (content[i] == '\n') {
				if (i != 0) {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
					} else {
						printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
					}
					j = 0;
				}
				printf(" ");
				j++;
			} else {
				if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
					printf(" |\n| %-13s | ", "");
					j = 0;
				}
				printf("%c", content[i]);
				j++;
			}
		}
		if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
			printf(" |\n");
		} else {
			printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
		}

		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
			char *my_row[2] = { "Abstract", (char *) content };
			if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_INFO, my_row)) {
				fprintf(stderr, "ADD GRID ROW error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}

		xmlFree(content);
		xmlXPathFreeObject(xpathObj);

		//print line (between rows)
		char row_line[OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH + 3] = { '\0' };
		row_line[0] = '|';
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH; i++) {
			if (i == 16)
				row_line[i] = '|';
			else
				row_line[i] = '-';
		}
		row_line[i] = '|';
		printf("%s\n", row_line);
		//ABSTRACT end

		//AUTHOR start
		printf("| ");

		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//author", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		node = xpathObj->nodesetval->nodeTab[0];
		content = xmlNodeGetContent(node);

		//print author
		printf("%-13s | ", "Author");
		j = 0;
		for (i = 0; i < strlen((char *) content); i++) {
			if (content[i] == '\t') {
				if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
					printf(" |\n| %-13s | ", "");
					j = 0;
				}
				printf(" ");
				j++;
			} else if (content[i] == '\n') {
				if (i != 0) {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
					} else {
						printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
					}
					j = 0;
				}
				printf(" ");
				j++;
			} else {
				if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
					printf(" |\n| %-13s | ", "");
					j = 0;
				}
				printf("%c", content[i]);
				j++;
			}
		}
		if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
			printf(" |\n");
		} else {
			printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
		}

		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
			char *my_row[2] = { "Author", (char *) content };
			if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_INFO, my_row)) {
				fprintf(stderr, "ADD GRID ROW error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}

		xmlFree(content);
		xmlXPathFreeObject(xpathObj);
		//AUTHOR end

		//CREATION DATE start
		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//creationdate", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		if (xpathObj->nodesetval->nodeNr != 0) {

			//print line (between rows)
			printf("%s\n", row_line);
			printf("| ");

			node = xpathObj->nodesetval->nodeTab[0];
			content = xmlNodeGetContent(node);

			//print creation date
			printf("%-13s | ", "Creation Date");
			j = 0;
			for (i = 0; i < strlen((char *) content); i++) {
				if (content[i] == '\t') {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf(" ");
					j++;
				} else if (content[i] == '\n') {
					if (i != 0) {
						if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
							printf(" |\n| %-13s | ", "");
						} else {
							printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
						}
						j = 0;
					}
					printf(" ");
					j++;
				} else {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf("%c", content[i]);
					j++;
				}
			}
			if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
				printf(" |\n");
			} else {
				printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
			}

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
				char *my_row[2] = { "Creation Date", (char *) content };
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_INFO, my_row)) {
					fprintf(stderr, "ADD GRID ROW error\n");
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
			}

			xmlFree(content);
		}
		xmlXPathFreeObject(xpathObj);
		//CREATION DATE end

		//LICENSE start
		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//license", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		if (xpathObj->nodesetval->nodeNr != 0) {

			//print line (between rows)
			printf("%s\n", row_line);
			printf("| ");

			node = xpathObj->nodesetval->nodeTab[0];
			content = xmlNodeGetContent(node);

			//print license
			printf("%-13s | ", "License");
			j = 0;
			for (i = 0; i < strlen((char *) content); i++) {
				if (content[i] == '\t') {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf(" ");
					j++;
				} else if (content[i] == '\n') {
					if (i != 0) {
						if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
							printf(" |\n| %-13s | ", "");
						} else {
							printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
						}
						j = 0;
					}
					printf(" ");
					j++;
				} else {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf("%c", content[i]);
					j++;
				}
			}
			if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
				printf(" |\n");
			} else {
				printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
			}

			char buffer[OPH_COMMON_BUFFER_LEN];
			int nn = 0;

			nn += snprintf(buffer + nn, OPH_COMMON_BUFFER_LEN - nn, "%s\n", content);

			xmlFree(content);
			content = xmlGetProp(node, (const xmlChar *) "url");

			//print license url
			printf("| %-13s | ", "");
			j = 0;
			for (i = 0; i < strlen((char *) content); i++) {
				if (content[i] == '\t') {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf(" ");
					j++;
				} else if (content[i] == '\n') {
					if (i != 0) {
						if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
							printf(" |\n| %-13s | ", "");
						} else {
							printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
						}
						j = 0;
					}
					printf(" ");
					j++;
				} else {
					if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
						printf(" |\n| %-13s | ", "");
						j = 0;
					}
					printf("%c", content[i]);
					j++;
				}
			}
			if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
				printf(" |\n");
			} else {
				printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
			}

			nn += snprintf(buffer + nn, OPH_COMMON_BUFFER_LEN - nn, "%s", content);

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
				char *my_row[2] = { "License", buffer };
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_INFO, my_row)) {
					fprintf(stderr, "ADD GRID ROW error\n");
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
			}

			xmlFree(content);
		}
		xmlXPathFreeObject(xpathObj);
		//LICENSE end

		//RETURN start
		if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE) {
			xpathObj = xmlXPathEvalExpression((const xmlChar *) "//return", xpathCtx);
			if (xpathObj == NULL) {
				fprintf(stderr, "Error: unable to evaluate xpath expression\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
			if (xpathObj->nodesetval->nodeNr != 0) {

				//print line (between rows)
				printf("%s\n", row_line);
				printf("| ");

				node = xpathObj->nodesetval->nodeTab[0];
				content = xmlGetProp(node, (const xmlChar *) "type");

				//print return type
				printf("%-13s | ", "Return Type");
				j = 0;
				for (i = 0; i < strlen((char *) content); i++) {
					if (content[i] == '\t') {
						if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
							printf(" |\n| %-13s | ", "");
							j = 0;
						}
						printf(" ");
						j++;
					} else if (content[i] == '\n') {
						if (i != 0) {
							if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
								printf(" |\n| %-13s | ", "");
							} else {
								printf("%-*s |\n| %-13s | ", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "", "");
							}
							j = 0;
						}
						printf(" ");
						j++;
					} else {
						if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
							printf(" |\n| %-13s | ", "");
							j = 0;
						}
						printf("%c", content[i]);
						j++;
					}
				}
				if (j == OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18) {
					printf(" |\n");
				} else {
					printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 18 - j, "");
				}

				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_INFO)) {
					char *my_row[2] = { "Return Type", (char *) content };
					if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_INFO, my_row)) {
						fprintf(stderr, "ADD GRID ROW error\n");
						xmlXPathFreeContext(xpathCtx);
						xmlFreeDoc(doc);
						xmlFreeParserCtxt(ctxt);
						free(header_line);
						return OPH_RENDER_OUTPUT_RENDER_ERROR;
					}
				}

				xmlFree(content);

			}
			xmlXPathFreeObject(xpathObj);
		}
		//RETURN end

		//print line
		printf("%s\n\n", inner_header_line);

		/*
		 *
		 * PRINT DESCRIPTION - FUNCTION ARGUMENTS
		 *
		 */
		int n;

		//ARGUMENTS start
		//print line
		printf("%s\n", header_line);
		printf("|");
		//print args header
		printf("%*s%*s", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 + (int) strlen("ARGUMENTS") / 2, "ARGUMENTS", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 - (int) strlen("ARGUMENTS") / 2, "");
		printf("|\n");

		xpathObj = xmlXPathEvalExpression((const xmlChar *) "//args/argument", xpathCtx);
		if (xpathObj == NULL) {
			fprintf(stderr, "Error: unable to evaluate xpath expression\n");
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_ERROR;
		}
		if (xpathObj->nodesetval->nodeNr == 0) {
			//print line
			printf("%s\n", header_line);
			printf("| %-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 2, "No arguments are available");
			//print line
			printf("%s\n\n", header_line);

			xmlXPathFreeObject(xpathObj);
			xmlXPathFreeContext(xpathCtx);
			xmlFreeDoc(doc);
			xmlFreeParserCtxt(ctxt);
			free(header_line);
			return OPH_RENDER_OUTPUT_RENDER_OK;
		}
		//print line
		int k = 0;
		inner_header_line[0] = '+';
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH + 2; i++) {
			inner_header_line[i] = '=';
		}
		inner_header_line[i] = '+';
		k = i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH + 2; i++) {
			inner_header_line[k + i] = '=';
		}
		inner_header_line[k + i] = '+';
		printf("%s\n", inner_header_line);

		k = 0;
		row_line[0] = '|';
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH + 2; i++) {
			row_line[i] = '-';
		}
		row_line[i] = '|';
		k = i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';
		k += i;
		for (i = 1; i <= OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH + 2; i++) {
			row_line[k + i] = '-';
		}
		row_line[k + i] = '|';

		//print args columns
		printf("| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s |\n",
		       OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, "NAME",
		       OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, "TYPE",
		       OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "MANDATORY",
		       OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "DEFAULT",
		       OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "MIN", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "MAX", OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH, "VALUES");
		if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS)) {
			int num_fields = 7;
			char *keys[7] = { "NAME", "TYPE", "MANDATORY", "DEFAULT", "MIN", "MAX", "VALUES" };
			char *fieldtypes[7] = { "string", "string", "string", "string", "string", "string", "string" };
			if (oph_json_add_grid(oper_json, OPH_JSON_OBJKEY_MAN_ARGS, "Function Arguments", NULL, keys, num_fields, fieldtypes, num_fields)) {
				fprintf(stderr, "ADD GRID error\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
		}
		//print line
		printf("%s\n", inner_header_line);

		for (n = 0; n < xpathObj->nodesetval->nodeNr; n++) {

			node = xpathObj->nodesetval->nodeTab[n];

			if (n != 0) {
				//print line
				printf("%s\n", row_line);
			}

			printf("| ");

			char my_row1[OPH_COMMON_BUFFER_LEN];
			char my_row2[OPH_COMMON_BUFFER_LEN];
			char my_row3[OPH_COMMON_BUFFER_LEN];
			char my_row4[OPH_COMMON_BUFFER_LEN];
			char my_row5[OPH_COMMON_BUFFER_LEN];
			char my_row6[OPH_COMMON_BUFFER_LEN];
			char my_row7[OPH_COMMON_BUFFER_LEN];
			char *my_row[7] = { my_row1, my_row2, my_row3, my_row4, my_row5, my_row6, my_row7 };

			//NAME
			content = xmlNodeGetContent(node);
			if (!content) {
				fprintf(stderr, "Error: argument %d is empty\n", n);
				xmlXPathFreeObject(xpathObj);
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
			printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
				snprintf(my_row[0], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//TYPE
			content = xmlGetProp(node, (const xmlChar *) "type");
			printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, (char *) content);
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
				snprintf(my_row[1], OPH_COMMON_BUFFER_LEN, "%s", content);
			xmlFree(content);
			//MANDATORY
			content = xmlGetProp(node, (const xmlChar *) "mandatory");
			if (!content) {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "yes");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[2], OPH_COMMON_BUFFER_LEN, "%s", "yes");
			} else {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[2], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
			}
			//DEFAULT
			content = xmlGetProp(node, (const xmlChar *) "default");
			if (!content) {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[3], OPH_COMMON_BUFFER_LEN, "%s", "");
			} else {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[3], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
			}
			//MINVAL
			content = xmlGetProp(node, (const xmlChar *) "minvalue");
			if (!content) {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", "");
			} else {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
			}
			//MAXVAL
			content = xmlGetProp(node, (const xmlChar *) "maxvalue");
			if (!content) {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[5], OPH_COMMON_BUFFER_LEN, "%s", "");
			} else {
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[5], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
			}
			//VALUES
			content = xmlGetProp(node, (const xmlChar *) "values");
			if (!content) {
				printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH, "");
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[6], OPH_COMMON_BUFFER_LEN, "%s", "");
			} else {
				j = 0;
				for (i = 0; i < strlen((char *) content); i++) {
					if (content[i] == '\t') {
						if (j == OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH) {
							printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
							       OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "");
							j = 0;
						}
						printf(" ");
						j++;
					} else if (content[i] == '\n') {
						if (i != 0) {
							if (j == OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH) {
								printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "");
							} else {
								printf("%-*s |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH - j, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "");
							}
							j = 0;
						}
						printf(" ");
						j++;
					} else {
						if (j == OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH) {
							printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
							       OPH_RENDER_OUTPUT_MAN_ARG_NAME_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_TYPE_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_MANDATORY_WIDTH, "",
							       OPH_RENDER_OUTPUT_MAN_ARG_DEFAULT_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MINVAL_WIDTH, "", OPH_RENDER_OUTPUT_MAN_ARG_MAXVAL_WIDTH, "");
							j = 0;
						}
						printf("%c", content[i]);
						j++;
					}
				}
				if (j == OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH) {
					printf(" |\n");
				} else {
					printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_ARG_VALUES_WIDTH - j, "");
				}

				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS))
					snprintf(my_row[6], OPH_COMMON_BUFFER_LEN, "%s", content);

				xmlFree(content);
			}

			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_ARGS)) {
				if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_ARGS, my_row)) {
					fprintf(stderr, "ADD GRID ROW error\n");
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
			}

		}
		xmlXPathFreeObject(xpathObj);

		//print line
		printf("%s\n\n", inner_header_line);
		//ARGUMENTS end

		//MULTI-ARGUMENTS start
		//print line
		if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE) {
			printf("%s\n", header_line);
			printf("|");
			//print args header
			printf("%*s%*s", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 + (int) strlen("MULTI-ARGUMENTS") / 2, "MULTI-ARGUMENTS",
			       OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH / 2 - (int) strlen("MULTI-ARGUMENTS") / 2, "");
			printf("|\n");

			xpathObj = xmlXPathEvalExpression((const xmlChar *) "//args/multi-argument", xpathCtx);
			if (xpathObj == NULL) {
				fprintf(stderr, "Error: unable to evaluate xpath expression\n");
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_ERROR;
			}
			if (xpathObj->nodesetval->nodeNr == 0) {
				//print line
				printf("%s\n", header_line);
				printf("| %-*s |\n", OPH_RENDER_OUTPUT_MAN_TABLE_WIDTH - 2, "No multiple arguments are available");
				//print line
				printf("%s\n\n", header_line);

				xmlXPathFreeObject(xpathObj);
				xmlXPathFreeContext(xpathCtx);
				xmlFreeDoc(doc);
				xmlFreeParserCtxt(ctxt);
				free(header_line);
				return OPH_RENDER_OUTPUT_RENDER_OK;
			}
			//print line
			printf("%s\n", inner_header_line);
			//print args columns
			printf("| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s |\n",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH, "NAME",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, "TYPE",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "MANDATORY",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "DEFAULT",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "MIN VAL",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "MAX VAL",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "MIN TIMES",
			       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "MAX TIMES", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH, "VALUES");
			if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS)) {
				int num_fields = 9;
				char *keys[9] = { "NAME", "TYPE", "MANDATORY", "DEFAULT", "MIN VAL", "MAX VAL", "MIN TIMES", "MAX TIMES", "VALUES" };
				char *fieldtypes[9] = { "string", "string", "string", "string", "string", "string", "string", "string", "string" };
				if (oph_json_add_grid(oper_json, OPH_JSON_OBJKEY_MAN_MULTIARGS, "Function Multi-Arguments", NULL, keys, num_fields, fieldtypes, num_fields)) {
					fprintf(stderr, "ADD GRID error\n");
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
			}
			//print line
			printf("%s\n", inner_header_line);

			for (n = 0; n < xpathObj->nodesetval->nodeNr; n++) {

				node = xpathObj->nodesetval->nodeTab[n];

				if (n != 0) {
					//print line
					printf("%s\n", row_line);
				}

				printf("| ");

				char my_row1[OPH_COMMON_BUFFER_LEN];
				char my_row2[OPH_COMMON_BUFFER_LEN];
				char my_row3[OPH_COMMON_BUFFER_LEN];
				char my_row4[OPH_COMMON_BUFFER_LEN];
				char my_row5[OPH_COMMON_BUFFER_LEN];
				char my_row6[OPH_COMMON_BUFFER_LEN];
				char my_row7[OPH_COMMON_BUFFER_LEN];
				char my_row8[OPH_COMMON_BUFFER_LEN];
				char my_row9[OPH_COMMON_BUFFER_LEN];
				char *my_row[9] = { my_row1, my_row2, my_row3, my_row4, my_row5, my_row6, my_row7, my_row8, my_row9 };

				//NAME
				content = xmlNodeGetContent(node);
				if (!content) {
					fprintf(stderr, "Error: argument %d is empty\n", n);
					xmlXPathFreeObject(xpathObj);
					xmlXPathFreeContext(xpathCtx);
					xmlFreeDoc(doc);
					xmlFreeParserCtxt(ctxt);
					free(header_line);
					return OPH_RENDER_OUTPUT_RENDER_ERROR;
				}
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
					snprintf(my_row[0], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
				//TYPE
				content = xmlGetProp(node, (const xmlChar *) "type");
				printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, (char *) content);
				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
					snprintf(my_row[1], OPH_COMMON_BUFFER_LEN, "%s", content);
				xmlFree(content);
				//MANDATORY
				content = xmlGetProp(node, (const xmlChar *) "mandatory");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "yes");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[2], OPH_COMMON_BUFFER_LEN, "%s", "yes");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[2], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//DEFAULT
				content = xmlGetProp(node, (const xmlChar *) "default");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[3], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[3], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//MINVAL
				content = xmlGetProp(node, (const xmlChar *) "minvalue");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[4], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//MAXVAL
				content = xmlGetProp(node, (const xmlChar *) "maxvalue");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[5], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[5], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//MINTIME
				content = xmlGetProp(node, (const xmlChar *) "mintimes");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[6], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[6], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//MAXTIME
				content = xmlGetProp(node, (const xmlChar *) "maxtimes");
				if (!content) {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[7], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					printf("%-*s | ", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, (char *) content);
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[7], OPH_COMMON_BUFFER_LEN, "%s", content);
					xmlFree(content);
				}
				//VALUES
				content = xmlGetProp(node, (const xmlChar *) "values");
				if (!content) {
					printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH, "");
					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[8], OPH_COMMON_BUFFER_LEN, "%s", "");
				} else {
					j = 0;
					for (i = 0; i < strlen((char *) content); i++) {
						if (content[i] == '\t') {
							if (j == OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH) {
								printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, "", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, "");
								j = 0;
							}
							printf(" ");
							j++;
						} else if (content[i] == '\n') {
							if (i != 0) {
								if (j == OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH) {
									printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, "", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, "");
								} else {
									printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH - j, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "",
									       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, "", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, "");
								}
								j = 0;
							}
							printf(" ");
							j++;
						} else {
							if (j == OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH) {
								printf(" |\n| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | ",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_NAME_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_TYPE_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MANDATORY_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_DEFAULT_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINVAL_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXVAL_WIDTH, "",
								       OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MINTIME_WIDTH, "", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_MAXTIME_WIDTH, "");
								j = 0;
							}
							printf("%c", content[i]);
							j++;
						}
					}
					if (j == OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH) {
						printf(" |\n");
					} else {
						printf("%-*s |\n", OPH_RENDER_OUTPUT_MAN_MULTI_ARG_VALUES_WIDTH - j, "");
					}

					if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS))
						snprintf(my_row[8], OPH_COMMON_BUFFER_LEN, "%s", content);

					xmlFree(content);
				}

				if (oph_json_is_objkey_printable(objkeys, objkeys_num, OPH_JSON_OBJKEY_MAN_MULTIARGS)) {
					if (oph_json_add_grid_row(oper_json, OPH_JSON_OBJKEY_MAN_MULTIARGS, my_row)) {
						fprintf(stderr, "ADD GRID ROW error\n");
						xmlXPathFreeContext(xpathCtx);
						xmlFreeDoc(doc);
						xmlFreeParserCtxt(ctxt);
						free(header_line);
						return OPH_RENDER_OUTPUT_RENDER_ERROR;
					}
				}

			}
			xmlXPathFreeObject(xpathObj);

			//print line
			printf("%s\n\n", inner_header_line);
		}
		//MULTI-ARGUMENTS end

		/* Cleanup of XPath context */
		xmlXPathFreeContext(xpathCtx);
		/* free up the resulting document */
		xmlFreeDoc(doc);
		/* free up the parser context */
		xmlFreeParserCtxt(ctxt);
		free(header_line);
		return OPH_RENDER_OUTPUT_RENDER_OK;
	}
}
