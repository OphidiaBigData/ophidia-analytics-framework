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

#include "oph_task_parser_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <libxml/parser.h>
#include <libxml/valid.h>
#include <libxml/tree.h>
#include <libxml/xmlstring.h>

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "debug.h"

extern int msglevel;

int oph_tp_retrieve_function_xml_file(const char *function_name, const char *function_version, short int function_type_code, char (*xml_filename)[OPH_TP_BUFLEN])
{
	DIR *dir;
	struct dirent *ent;
	int found = 0;
	char folder[OPH_TP_BUFLEN];
	char full_filename[OPH_TP_BUFLEN];
	struct stat file_stat;

	if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
		snprintf(folder, OPH_TP_BUFLEN, OPH_FRAMEWORK_PRIMITIVE_XML_FOLDER_PATH, OPH_ANALYTICS_LOCATION);
	else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE)
		snprintf(folder, OPH_TP_BUFLEN, OPH_FRAMEWORK_HIERARCHY_XML_FOLDER_PATH, OPH_ANALYTICS_LOCATION);
	else
		snprintf(folder, OPH_TP_BUFLEN, OPH_FRAMEWORK_OPERATOR_XML_FOLDER_PATH, OPH_ANALYTICS_LOCATION);

	if ((dir = opendir(folder)) != NULL) {
		if (function_version == NULL) {	//retrieve latest version
			char format[OPH_TP_BUFLEN];
			char buffer[OPH_TP_BUFLEN];
			unsigned int maxlen = 0;
			int maxversionlen = 0;
			int versionlen = 0;
			int count = 0;
			char *ptr1, *ptr2, *ptr3;
			size_t len;

			if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
				snprintf(*xml_filename, OPH_TP_BUFLEN, "%s_" OPH_TP_XML_PRIMITIVE_TYPE "_", function_name);
			else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE)
				snprintf(*xml_filename, OPH_TP_BUFLEN, "%s_" OPH_TP_XML_HIERARCHY_TYPE "_", function_name);
			else
				snprintf(*xml_filename, OPH_TP_BUFLEN, "%s_" OPH_TP_XML_OPERATOR_TYPE "_", function_name);

			while ((ent = readdir(dir)) != NULL) {

				snprintf(full_filename, OPH_TP_BUFLEN, "%s/%s", folder, ent->d_name);
				lstat(full_filename, &file_stat);

				if (!S_ISLNK(file_stat.st_mode) && !S_ISREG(file_stat.st_mode))
					continue;
				ptr1 = strrchr(ent->d_name, '_');
				ptr2 = strrchr(ent->d_name, OPH_TP_VERSION_SEPARATOR);
				if (!ptr1 || !ptr2) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "invalid filename\n");
					return OPH_TP_TASK_PARSER_ERROR;
				}
				if (strcasestr(ent->d_name, *xml_filename) && !strcmp(ptr2, OPH_TP_VERSION_EXTENSION)) {
					//verify filename
					len = strlen(ent->d_name);
					strncpy(buffer, ent->d_name, len - strlen(ptr1) + 1);
					buffer[len - strlen(ptr1) + 1] = 0;
					if (strcasecmp(*xml_filename, buffer))
						continue;

					versionlen = 0;
					ptr3 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);

					if (strlen(ptr1) - strlen(ptr3) - 1 > maxlen) {
						maxlen = strlen(ptr1) - strlen(ptr3) - 1;
					}
					versionlen++;
					count++;

					while (ptr2 != ptr3) {
						ptr1 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);
						ptr3 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);
						if (!ptr1 || !ptr3) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "invalid filename\n");
							return OPH_TP_TASK_PARSER_ERROR;
						}
						if (strlen(ptr1) - strlen(ptr3) - 1 > maxlen) {
							maxlen = strlen(ptr1) - strlen(ptr3) - 1;
						}
						versionlen++;
					}

					snprintf(format, OPH_TP_BUFLEN, "%%0%dd", maxlen);
					if (versionlen > maxversionlen) {
						maxversionlen = versionlen;
					}
					//At last check file extension it should be exactly xml
					if (strncasecmp(ptr2 + 1, OPH_TP_XML_FILE_EXTENSION, strlen(OPH_TP_XML_FILE_EXTENSION)) || strncasecmp(ptr2 + 1, OPH_TP_XML_FILE_EXTENSION, strlen(ptr2 + 1)))
						continue;

					found = 1;
					break;
				}
			}
			closedir(dir);

			if (!found) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "could not find %s\n", *xml_filename);
				return OPH_TP_TASK_PARSER_ERROR;
			}

			char *field = (char *) malloc((maxlen + 1) * sizeof(char));
			if (!field) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				return OPH_TP_TASK_PARSER_ERROR;
			}

			char **versions = (char **) malloc(count * sizeof(char *));
			if (!versions) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				return OPH_TP_TASK_PARSER_ERROR;
			}
			int i;
			for (i = 0; i < count; i++) {
				versions[i] = (char *) malloc((maxlen * maxversionlen + 1) * sizeof(char));
				if (!versions[i]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					return OPH_TP_TASK_PARSER_ERROR;
				}
			}
			char **versions2 = (char **) malloc(count * sizeof(char *));
			if (!versions2) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
				return OPH_TP_TASK_PARSER_ERROR;
			}
			for (i = 0; i < count; i++) {
				versions2[i] = (char *) malloc(OPH_TP_BUFLEN * sizeof(char));
				if (!versions2[i]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
					return OPH_TP_TASK_PARSER_ERROR;
				}
			}

			//extract version values
			int j = 0;
			int val;
			if ((dir = opendir(folder)) != NULL) {
				while ((ent = readdir(dir)) != NULL) {

					snprintf(full_filename, OPH_TP_BUFLEN, "%s/%s", folder, ent->d_name);
					lstat(full_filename, &file_stat);

					if (!S_ISLNK(file_stat.st_mode) && !S_ISREG(file_stat.st_mode))
						continue;
					ptr1 = strrchr(ent->d_name, '_');
					ptr2 = strrchr(ent->d_name, OPH_TP_VERSION_SEPARATOR);
					if (!ptr1 || !ptr2) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "invalid filename\n");
						return OPH_TP_TASK_PARSER_ERROR;
					}
					if (strcasestr(ent->d_name, *xml_filename) && !strcmp(ptr2, OPH_TP_VERSION_EXTENSION)) {
						//verify filename
						len = strlen(ent->d_name);
						strncpy(buffer, ent->d_name, len - strlen(ptr1) + 1);
						buffer[len - strlen(ptr1) + 1] = 0;
						if (strcasecmp(*xml_filename, buffer))
							continue;

						j++;
						i = 0;
						ptr3 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);

						//copy real filename
						strncpy(versions2[j - 1], ent->d_name, strlen(ent->d_name));
						versions2[j - 1][strlen(ent->d_name)] = 0;

						//extract a single value from version
						strncpy(field, ptr1 + 1, strlen(ptr1) - strlen(ptr3) - 1);
						field[strlen(ptr1) - strlen(ptr3) - 1] = 0;
						val = strtol(field, NULL, 10);
						snprintf(field, maxlen + 1, format, val);
						strncpy(versions[j - 1] + i * maxlen, field, maxlen);

						i++;
						while (i < maxversionlen) {
							if (ptr2 != ptr3) {
								ptr1 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);
								ptr3 = strchr(ptr1 + 1, OPH_TP_VERSION_SEPARATOR);
								if (!ptr1 || !ptr3) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "invalid filename\n");
									return OPH_TP_TASK_PARSER_ERROR;
								}
								//extract a single value from version
								strncpy(field, ptr1 + 1, strlen(ptr1) - strlen(ptr3) - 1);
								field[strlen(ptr1) - strlen(ptr3) - 1] = 0;
								val = strtol(field, NULL, 10);
								snprintf(field, maxlen + 1, format, val);
								strncpy(versions[j - 1] + i * maxlen, field, maxlen);

							} else {
								//consider value=0
								val = 0;
								snprintf(field, maxlen + 1, format, val);
								strncpy(versions[j - 1] + i * maxlen, field, maxlen);

							}
							i++;
						}

						versions[j - 1][maxlen * maxversionlen] = 0;

						if (j == count)
							break;
					}
				}
				closedir(dir);
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "could not open directory\n");
				return OPH_TP_TASK_PARSER_ERROR;
			}

			//find latest version
			int latest_index = 0;
			for (j = 1; j < count; j++) {
				if (strcmp(versions[j], versions[latest_index]) > 0) {
					latest_index = j;
				}
			}

			snprintf(*xml_filename, OPH_TP_BUFLEN, "%s", versions2[latest_index]);

			free(field);
			for (i = 0; i < count; i++) {
				free(versions[i]);
			}
			free(versions);
			for (i = 0; i < count; i++) {
				free(versions2[i]);
			}
			free(versions2);

		} else {
			if (function_type_code == OPH_TP_XML_PRIMITIVE_TYPE_CODE)
				snprintf(*xml_filename, OPH_TP_BUFLEN, OPH_TP_XML_FILE_FORMAT, function_name, OPH_TP_XML_PRIMITIVE_TYPE, function_version);
			else if (function_type_code == OPH_TP_XML_HIERARCHY_TYPE_CODE)
				snprintf(*xml_filename, OPH_TP_BUFLEN, OPH_TP_XML_FILE_FORMAT, function_name, OPH_TP_XML_HIERARCHY_TYPE, function_version);
			else
				snprintf(*xml_filename, OPH_TP_BUFLEN, OPH_TP_XML_FILE_FORMAT, function_name, OPH_TP_XML_OPERATOR_TYPE, function_version);
			while ((ent = readdir(dir)) != NULL) {
				if (!strcasecmp(*xml_filename, ent->d_name)) {
					found = 1;
					snprintf(*xml_filename, OPH_TP_BUFLEN, "%s", ent->d_name);
					break;
				}
			}
			closedir(dir);
			if (!found) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "could not find %s\n", *xml_filename);
				return OPH_TP_TASK_PARSER_ERROR;
			}
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "could not open directory\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}

	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_validate_task_string(const char *task_string)
{
	if (!task_string)
		return OPH_TP_TASK_PARSER_ERROR;

	//Track the last special character
	char last_char = OPH_TP_PARAM_PARAM_SEPARATOR;
	//Track the last char
	char previous_char = 0;
	int first_flag = 1;
	int i;

	int skip_flag = 0, skip_check = 0;
	for (i = 0; task_string[i]; i++) {
		if (skip_flag) {
			if (task_string[i] == OPH_TP_SKIP_SEPARATOR) {
				skip_flag = 0;
				skip_check = 1;
			}
			continue;
		}
		if (skip_check && task_string[i] != OPH_TP_PARAM_PARAM_SEPARATOR)
			return OPH_TP_TASK_PARSER_ERROR;
		switch (task_string[i]) {
			case OPH_TP_SKIP_SEPARATOR:{
					skip_flag = 1;
					break;
				}
			case OPH_TP_PARAM_VALUE_SEPARATOR:{
					if (((last_char != OPH_TP_PARAM_PARAM_SEPARATOR) && (last_char != OPH_TP_PARAM_VALUE_SEPARATOR))
					    || (previous_char == OPH_TP_PARAM_VALUE_SEPARATOR || previous_char == OPH_TP_MULTI_VALUE_SEPARATOR || previous_char == OPH_TP_PARAM_PARAM_SEPARATOR))
						return OPH_TP_TASK_PARSER_ERROR;
					else {
						last_char = OPH_TP_PARAM_VALUE_SEPARATOR;
						first_flag = 0;
					}
					break;
				}
			case OPH_TP_PARAM_PARAM_SEPARATOR:{
					if (first_flag)
						return OPH_TP_TASK_PARSER_ERROR;
					if (last_char == OPH_TP_PARAM_PARAM_SEPARATOR
					    || (previous_char == OPH_TP_PARAM_VALUE_SEPARATOR || previous_char == OPH_TP_MULTI_VALUE_SEPARATOR || previous_char == OPH_TP_PARAM_PARAM_SEPARATOR))
						return OPH_TP_TASK_PARSER_ERROR;
					else
						last_char = OPH_TP_PARAM_PARAM_SEPARATOR;
					break;
				}
			case OPH_TP_MULTI_VALUE_SEPARATOR:{
					if (first_flag)
						return OPH_TP_TASK_PARSER_ERROR;
					if (last_char == OPH_TP_PARAM_PARAM_SEPARATOR
					    || (previous_char == OPH_TP_PARAM_VALUE_SEPARATOR || previous_char == OPH_TP_MULTI_VALUE_SEPARATOR || previous_char == OPH_TP_PARAM_PARAM_SEPARATOR))
						return OPH_TP_TASK_PARSER_ERROR;
					else
						last_char = OPH_TP_MULTI_VALUE_SEPARATOR;
					break;
				}
		}
		previous_char = task_string[i];
		skip_check = 0;
	}
	if (skip_flag || skip_check)
		return OPH_TP_TASK_PARSER_ERROR;
	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_find_param_in_task_string(const char *task_string, const char *param, char *value)
{
	if (!task_string || !param || !value)
		return OPH_TP_TASK_PARSER_ERROR;

	const char *ptr_begin, *ptr_equal, *ptr_end, *start_char, *stop_char;

	ptr_begin = task_string;
	ptr_equal = strchr(task_string, OPH_TP_PARAM_VALUE_SEPARATOR);
	ptr_end = strchr(task_string, OPH_TP_PARAM_PARAM_SEPARATOR);
	while (ptr_end) {
		if (!ptr_begin || !ptr_equal || !ptr_end)
			return OPH_TP_TASK_PARSER_ERROR;

		if (!strncmp(ptr_begin, param, strlen(ptr_begin) - strlen(ptr_equal)) && !strncmp(ptr_begin, param, strlen(param))) {
			start_char = ptr_equal + 1;
			stop_char = ptr_end;
			if (*start_char == OPH_TP_SKIP_SEPARATOR) {
				start_char++;
				stop_char--;
			}
			strncpy(value, start_char, strlen(start_char) - strlen(stop_char));
			value[strlen(start_char) - strlen(stop_char)] = 0;
			return OPH_TP_TASK_PARSER_SUCCESS;
		}
		ptr_begin = ptr_end + 1;
		ptr_equal = strchr(ptr_end + 1, OPH_TP_PARAM_VALUE_SEPARATOR);
		ptr_end = strchr(ptr_end + 1, OPH_TP_PARAM_PARAM_SEPARATOR);
	}
	return OPH_TP_TASK_PARSER_ERROR;
}

int oph_tp_validate_xml_document(xmlDocPtr document)
{
	if (!document)
		return OPH_TP_TASK_PARSER_ERROR;

	//Create validation context
	xmlValidCtxtPtr ctxt;
	ctxt = xmlNewValidCtxt();
	if (ctxt == NULL) {
		return OPH_TP_TASK_PARSER_ERROR;
	}
	//Parse the DTD file
	xmlDtdPtr dtd = xmlParseDTD(NULL, (xmlChar *) OPH_TP_DTD_SCHEMA);
	if (dtd == NULL) {
		xmlFreeValidCtxt(ctxt);
		return OPH_TP_TASK_PARSER_ERROR;
	}
	//Validate document
	if (!xmlValidateDtd(ctxt, document, dtd)) {
		xmlFreeValidCtxt(ctxt);
		xmlFreeDtd(dtd);
		return OPH_TP_TASK_PARSER_ERROR;
	}
	xmlFreeDtd(dtd);
	xmlFreeValidCtxt(ctxt);

	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_match_value_in_xml_value_list(const char *value, const xmlChar *values)
{
	if (!value || !values)
		return OPH_TP_TASK_PARSER_ERROR;

	char *ptr_begin, *ptr_end;

	ptr_begin = (char *) values;
	ptr_end = strchr(ptr_begin, OPH_TP_MULTI_VALUE_SEPARATOR);
	while (ptr_end) {
		if (!ptr_begin || !ptr_end)
			return OPH_TP_TASK_PARSER_ERROR;

		if (!strncmp(ptr_begin, value, strlen(ptr_begin) - strlen(ptr_end)) && !strncmp(ptr_begin, value, strlen(value)))
			return OPH_TP_TASK_PARSER_SUCCESS;

		ptr_begin = ptr_end + 1;
		ptr_end = strchr(ptr_end + 1, OPH_TP_MULTI_VALUE_SEPARATOR);
	}
	//Check last value
	if (!strncmp(ptr_begin, value, strlen(ptr_begin)) && !strncmp(ptr_begin, value, strlen(value)))
		return OPH_TP_TASK_PARSER_SUCCESS;

	return OPH_TP_TASK_PARSER_ERROR;
}

int oph_tp_validate_task_string_param(const char *task_string, xmlNodePtr xml_node, const char *param, char *value)
{
	if (!task_string || !param || !value || !xml_node)
		return OPH_TP_TASK_PARSER_ERROR;

	xmlChar *attribute_type, *attribute_mandatory, *attribute_minvalue, *attribute_maxvalue, *attribute_default, *attribute_values;
	char *tmp_value = strdup(task_string);
	if (!tmp_value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}
	//Find param in task string
	if (oph_tp_find_param_in_task_string(task_string, param, tmp_value)) {

		//Check if the parameter is mandatory
		attribute_mandatory = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_MANDATORY);
		if (attribute_mandatory != NULL && !xmlStrcmp((const xmlChar *) "no", attribute_mandatory)) {
			xmlFree(attribute_mandatory);
			attribute_default = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_DEFAULT);
			if (attribute_default != NULL) {
				strncpy(value, (char *) attribute_default, xmlStrlen(attribute_default));
				value[xmlStrlen(attribute_default)] = 0;
				xmlFree(attribute_default);
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Default value for param '%s' not given\n", param);
				free(tmp_value);
				return OPH_TP_TASK_PARSER_ERROR;
			}
		} else {
			xmlFree(attribute_mandatory);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "The param '%s' is mandatory\n", param);
			free(tmp_value);
			return OPH_TP_TASK_PARSER_ERROR;
		}
	} else {

		//Other checks
		attribute_type = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_TYPE);
		if (attribute_type != NULL) {
			if (!xmlStrcmp(attribute_type, (const xmlChar *) OPH_TP_INT_TYPE)) {
				int numeric_value = (int) strtol(tmp_value, NULL, 10);

				attribute_minvalue = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_MINVALUE);
				attribute_maxvalue = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_MAXVALUE);
				int min_value = 0, max_value = 0;
				if (attribute_minvalue != NULL && attribute_maxvalue != NULL) {
					min_value = (int) strtol((char *) attribute_minvalue, NULL, 10);
					max_value = (int) strtol((char *) attribute_maxvalue, NULL, 10);
					xmlFree(attribute_minvalue);
					xmlFree(attribute_maxvalue);
					if (min_value == max_value) {
						sprintf(tmp_value, "%d", min_value);
						if (min_value < numeric_value) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Param '%s' is changed to the only possible value %d\n", param, min_value);
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Param '%s' is changed to the only possible value %d\n", param, min_value);
						}
					} else {
						if (numeric_value < min_value) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is smaller than minvalue %d\n", param, min_value);
							xmlFree(attribute_type);
							free(tmp_value);
							return OPH_TP_TASK_PARSER_ERROR;
						}
						if (numeric_value > max_value) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is bigger than maxvalue %d\n", param, max_value);
							xmlFree(attribute_type);
							free(tmp_value);
							return OPH_TP_TASK_PARSER_ERROR;
						}
					}
				} else if (attribute_minvalue != NULL) {
					min_value = (int) strtol((char *) attribute_minvalue, NULL, 10);
					xmlFree(attribute_minvalue);
					if (numeric_value < min_value) {
						xmlFree(attribute_type);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is smaller than minvalue %d\n", param, min_value);
						free(tmp_value);
						return OPH_TP_TASK_PARSER_ERROR;
					}
				} else if (attribute_maxvalue != NULL) {
					max_value = (int) strtol((char *) attribute_maxvalue, NULL, 10);
					xmlFree(attribute_maxvalue);
					if (numeric_value > max_value) {
						xmlFree(attribute_type);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is bigger than maxvalue %d\n", param, max_value);
						free(tmp_value);
						return OPH_TP_TASK_PARSER_ERROR;
					}
				}
			} else if (!xmlStrcmp(attribute_type, (const xmlChar *) OPH_TP_REAL_TYPE)) {
				double numeric_value = strtod(tmp_value, NULL);

				attribute_minvalue = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_MINVALUE);
				attribute_maxvalue = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_MAXVALUE);
				double min_value = 0, max_value = 0;
				if (attribute_minvalue != NULL && attribute_maxvalue != NULL) {
					min_value = strtod((char *) attribute_minvalue, NULL);
					max_value = strtod((char *) attribute_maxvalue, NULL);
					xmlFree(attribute_minvalue);
					xmlFree(attribute_maxvalue);
					if (min_value == max_value) {
						sprintf(tmp_value, "%f", min_value);
						if (min_value < numeric_value) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Param '%s' is changed to the only possible value %f\n", param, min_value);
							logging(LOG_WARNING, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Param '%s' is changed to the only possible value %f\n", param, min_value);
						}
					} else {
						if (numeric_value < min_value) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is smaller than minvalue %f\n", param, min_value);
							xmlFree(attribute_type);
							free(tmp_value);
							return OPH_TP_TASK_PARSER_ERROR;
						}
						if (numeric_value > max_value) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is bigger than maxvalue %f\n", param, max_value);
							xmlFree(attribute_type);
							free(tmp_value);
							return OPH_TP_TASK_PARSER_ERROR;
						}
					}
				} else if (attribute_minvalue != NULL) {
					min_value = strtod((char *) attribute_minvalue, NULL);
					xmlFree(attribute_minvalue);
					if (numeric_value < min_value) {
						xmlFree(attribute_type);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is smaller than minvalue %f\n", param, min_value);
						free(tmp_value);
						return OPH_TP_TASK_PARSER_ERROR;
					}
				} else if (attribute_maxvalue != NULL) {
					max_value = strtod((char *) attribute_maxvalue, NULL);
					xmlFree(attribute_maxvalue);
					if (numeric_value > max_value) {
						xmlFree(attribute_type);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' is bigger than maxvalue %f\n", param, max_value);
						free(tmp_value);
						return OPH_TP_TASK_PARSER_ERROR;
					}
				}
			}

			attribute_values = xmlGetProp(xml_node, (const xmlChar *) OPH_TP_XML_ATTRIBUTE_VALUES);
			if (attribute_values != NULL) {
				char **values = NULL;
				int value_number = 0;
				//If tmp_value is  not multiple-field value, check single value
				if (oph_tp_parse_multiple_value_param(tmp_value, &values, &value_number) || (value_number <= 1)) {
					oph_tp_free_multiple_value_param_list(values, value_number);

					//Check if the value is in the set of specified values
					if (oph_tp_match_value_in_xml_value_list(tmp_value, attribute_values)) {
						xmlFree(attribute_type);
						xmlFree(attribute_values);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' value doesn't appear in value list\n", param);
						free(tmp_value);
						return OPH_TP_TASK_PARSER_ERROR;
					}
				} else {
					int i = 0;
					for (i = 0; i < value_number; i++) {
						//Check if the value is in the set of specified values
						if (oph_tp_match_value_in_xml_value_list(values[i], attribute_values)) {
							oph_tp_free_multiple_value_param_list(values, value_number);
							xmlFree(attribute_type);
							xmlFree(attribute_values);
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Param '%s' value doesn't appear in value list\n", param);
							free(tmp_value);
							return OPH_TP_TASK_PARSER_ERROR;
						}
					}
					oph_tp_free_multiple_value_param_list(values, value_number);
				}
				xmlFree(attribute_values);
			}

			strncpy(value, tmp_value, strlen(tmp_value));
			value[strlen(tmp_value)] = 0;
			xmlFree(attribute_type);
		}
	}

	free(tmp_value);

	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_start_xml_parser()
{
	xmlInitParser();
	LIBXML_TEST_VERSION return 0;
}

int oph_tp_end_xml_parser()
{
	xmlCleanupParser();
	return 0;
}

int oph_tp_task_params_parser(char *task_string, HASHTBL **hashtbl)
{
	if (!task_string || !hashtbl)
		return OPH_TP_TASK_PARSER_ERROR;

	//Check if string has correct format
	if (oph_tp_validate_task_string(task_string)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Submission string is not valid!\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}

	char *op, operator[OPH_TP_TASKLEN] = { '\0' };

	//Find operator name in task string
	if (oph_tp_find_param_in_task_string(task_string, OPH_IN_PARAM_OPERATOR_NAME, operator)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find operator name in the string\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}

	xmlDocPtr document;
	xmlNodePtr root, node, subnode;

	//Select the correct XML file
	char path_file[OPH_TP_XML_PATH_LENGTH] = { '\0' };
	char filename[OPH_TP_XML_PATH_LENGTH] = { '\0' };
	char operator_name[OPH_TP_TASKLEN] = { '\0' };
	strcpy(operator_name, operator);
	for (op = operator_name; op && (*op != '\0'); op++)
		*op = toupper((unsigned char) *op);

	if (oph_tp_retrieve_function_xml_file((const char *) operator_name, NULL, OPH_TP_XML_OPERATOR_TYPE_CODE, &filename)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find xml\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}
	snprintf(path_file, sizeof(path_file), OPH_TP_XML_OPERATOR_FILE, OPH_ANALYTICS_LOCATION, filename);

	//Open document
	document = xmlParseFile(path_file);
	if (document == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open XML document %s\n", filename);
		return OPH_TP_TASK_PARSER_ERROR;
	}
	//Validate XML document
	if (oph_tp_validate_xml_document(document)) {
		xmlFreeDoc(document);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to validate XML document %s\n", filename);
		return OPH_TP_TASK_PARSER_ERROR;
	}
	//Read root
	root = xmlDocGetRootElement(document);
	if (root == NULL) {
		xmlFreeDoc(document);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find root of document\n");
		return OPH_TP_TASK_PARSER_ERROR;
	}

	xmlChar *content;

	//Parse till args section
	long number_arguments = 0;
	char *value1 = NULL;
	node = root->children;
	while (node != NULL) {
		if (!xmlStrcmp(node->name, (const xmlChar *) OPH_TP_XML_ARGS)) {
			//Count number of elements
			number_arguments = xmlChildElementCount(node);

			if (!(*hashtbl = hashtbl_create(number_arguments + 1, NULL))) {
				xmlFreeDoc(document);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create hash table\n");
				return OPH_TP_TASK_PARSER_ERROR;
			}
			//For each argument read content and attributes
			subnode = node->xmlChildrenNode;
			while (subnode != NULL) {
				if ((!xmlStrcmp(subnode->name, (const xmlChar *) OPH_TP_XML_ARGUMENT))) {
					//Look for param names (xml content)
					content = xmlNodeGetContent(subnode->xmlChildrenNode);
					if (content) {
						value1 = strdup(task_string);
						if (!value1) {
							xmlFree(content);
							xmlFreeDoc(document);
							return OPH_TP_TASK_PARSER_ERROR;
						}
						//Get and check value for parameter
						if (oph_tp_validate_task_string_param(task_string, subnode, (char *) content, value1)) {
							xmlFree(content);
							xmlFreeDoc(document);
							free(value1);
							return OPH_TP_TASK_PARSER_ERROR;
						}
						hashtbl_insert(*hashtbl, (char *) content, (void *) value1);
						free(value1);
						xmlFree(content);
					}
				}
				subnode = subnode->next;
			}
			hashtbl_insert(*hashtbl, OPH_IN_PARAM_OPERATOR_NAME, (void *) operator);
			break;
		}
		node = node->next;
	}
	// free up the parser context
	xmlFreeDoc(document);

	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_parse_multiple_value_param(char *values, char ***value_list, int *value_num)
{
	if (!values || !value_list || !value_num)
		return OPH_TP_TASK_PARSER_ERROR;

	*value_list = NULL;
	*value_num = 0;

	if (!strlen(values))
		return OPH_TP_TASK_PARSER_SUCCESS;

	int param_num = 1, i, j, msize = 0, csize = 0;

	//Check if string is correct
	for (i = 0; values[i]; i++) {
		if (values[i] == OPH_TP_PARAM_VALUE_SEPARATOR || values[i] == OPH_TP_PARAM_PARAM_SEPARATOR)
			return OPH_TP_TASK_PARSER_ERROR;
	}

	//Count number of parameters
	for (i = 0; values[i]; i++) {
		csize++;
		if (values[i] == OPH_TP_MULTI_VALUE_SEPARATOR) {
			param_num++;
			if (msize < csize)
				msize = csize;
			csize = 0;
		}
	}
	if (msize < csize)
		msize = csize;

	*value_list = (char **) malloc(param_num * sizeof(char *));
	if (!(*value_list))
		return OPH_TP_TASK_PARSER_ERROR;
	for (i = 0; i < param_num; i++)
		(*value_list)[i] = (char *) malloc((1 + msize) * sizeof(char));

	char *ptr_begin, *ptr_end;

	ptr_begin = values;
	ptr_end = strchr(values, OPH_TP_MULTI_VALUE_SEPARATOR);
	j = 0;
	while (ptr_begin) {
		if (ptr_end) {
			strncpy((*value_list)[j], ptr_begin, strlen(ptr_begin) - strlen(ptr_end));
			(*value_list)[j][strlen(ptr_begin) - strlen(ptr_end)] = 0;
			ptr_begin = ptr_end + 1;
			ptr_end = strchr(ptr_end + 1, OPH_TP_MULTI_VALUE_SEPARATOR);
		} else {
			strncpy((*value_list)[j], ptr_begin, strlen(ptr_begin));
			(*value_list)[j][strlen(ptr_begin)] = 0;
			ptr_begin = NULL;
		}
		j++;
	}

	*value_num = param_num;
	return OPH_TP_TASK_PARSER_SUCCESS;
}

int oph_tp_free_multiple_value_param_list(char **value_list, int value_num)
{
	int i;
	if (value_list) {
		for (i = 0; i < value_num; i++)
			if (value_list[i])
				free(value_list[i]);
		free(value_list);
	}
	return OPH_TP_TASK_PARSER_SUCCESS;
}
