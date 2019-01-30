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

#ifndef __OPH_JSON_COMMON_H__
#define __OPH_JSON_COMMON_H__

#include <stddef.h>

/***********OPH_JSON MACROS***********/

/* OPH_JSON OBJCLASSES */
#define OPH_JSON_TEXT 	   	"text"
#define OPH_JSON_GRID 	   	"grid"
#define OPH_JSON_MULTIGRID 	"multidimgrid"
#define OPH_JSON_TREE 	   	"tree"
#define OPH_JSON_DGRAPH    	"digraph"
#define OPH_JSON_GRAPH     	"graph"

/* OPH_JSON TYPES */
#define OPH_JSON_BYTE	"byte"
#define OPH_JSON_SHORT	"short"
#define OPH_JSON_INT	"int"
#define OPH_JSON_LONG	"long"
#define OPH_JSON_FLOAT 	"float"
#define OPH_JSON_DOUBLE	"double"
#define OPH_JSON_STRING	"string"
#define OPH_JSON_BLOB  	"blob"

/* OPH_JSON STATUS CODES */
#define OPH_JSON_SUCCESS		0
#define OPH_JSON_MEMORY_ERROR 		1
#define OPH_JSON_BAD_PARAM_ERROR	2
#define OPH_JSON_IO_ERROR 		3
#define OPH_JSON_GENERIC_ERROR 		4

/* OPH_JSON LOG MACROS */
#define OPH_JSON_LOG_MEMORY_ERROR	"Error allocating %s\n"
#define OPH_JSON_LOG_BAD_PARAM_ERROR	"Invalid parameter %s\n"
#define OPH_JSON_LOG_IO_ERROR		"Error in opening %s\n"

/***********OPH_JSON STRUCTURES***********/

/**
 * \brief Structure that defines an OPH_JSON response object
 * \param objclass Object class
 * \param objkey Object key
 * \param objcontent Object content
 * \param objcontent_num Number of content fragments
 */
typedef struct _oph_json_response {
	char *objclass;
	char *objkey;
	void *objcontent;
	unsigned int objcontent_num;
} oph_json_response;

/**
 * \brief Structure that defines an OPH_JSON data source
 * \param srckey Key identifying the source
 * \param srcname Name of the source
 * \param srcurl URL of the source
 * \param description Description of the source
 * \param producer User producing data
 * \param keys List of other source-specific parameters
 * \param keys_num Number of keys
 * \param values Values for all keys
 * \param values_num Number of values
 */
typedef struct _oph_json_source {
	char *srckey;
	char *srcname;
	char *srcurl;
	char *description;
	char *producer;
	char **keys;
	unsigned int keys_num;
	char **values;
	unsigned int values_num;
} oph_json_source;

/**
 * \brief Structure that defines a complete OPH_JSON object
 * \param source Data source
 * \param consumers Users interested in data
 * \param consumers_num Number of consumers
 * \param responseKeyset Array of objkey within response
 * \param responseKeyset_num Length of responseKeyset
 * \param response Response with JSON objects
 * \param response_num Number of response objects
 */
typedef struct _oph_json {
	oph_json_source *source;
	char **consumers;
	unsigned int consumers_num;
	char **responseKeyset;
	unsigned int responseKeyset_num;
	oph_json_response *response;
	unsigned int response_num;
} oph_json;

/**
 * \brief Structure that defines a link between nodes
 * \param node Index of the second node
 * \param description Link description
 */
typedef struct _oph_json_link {
	char *node;
	char *description;
} oph_json_link;

/**
 * \brief Structure that defines an array of links for a node
 * \param links Array of (out)links for one node
 * \param links_num Number of links
 */
typedef struct _oph_json_links {
	oph_json_link *links;
	unsigned int links_num;
} oph_json_links;

/***********OPH_JSON FUNCTIONS***********/

/**
 * \brief Function to allocate an OPH_JSON object
 * \param json Address of a pointer to an OPH_JSON object
 * \return 0 if successfull, N otherwise
 */
int oph_json_alloc(oph_json ** json);

/**
 * \brief Function to free an OPH_JSON object
 * \param json Pointer to an OPH_JSON object
 * \return 0 if successfull, N otherwise
 */
int oph_json_free(oph_json * json);

/**
 * \brief Function to add a consumer to an OPH_JSON object
 * \param json Pointer to an OPH_JSON object
 * \param consumer Name of the consumer user
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_consumer(oph_json * json, const char *consumer);

/**
 * \brief Function to create a JSON string from an OPH_JSON object.
 * \param json Pointer to an OPH_JSON object
 * \param jstring Output JSON string
 * \return 0 if successfull, N otherwise
 */
int oph_json_to_json_string(oph_json * json, char **jstring);

/**
 * \brief Function to write a JSON file from an OPH_JSON object.
 * \param json Pointer to an OPH_JSON object
 * \param filename Output absolute JSON file name
 * \return 0 if successfull, N otherwise
 */
int oph_json_to_json_file(oph_json * json, char *filename);

/**
 * \brief Function to write a JSON file from an OPH_JSON object.
 * \param json Pointer to an OPH_JSON object
 * \param filename Output absolute JSON file name
 * \param jstring Output JSON string
 * \return 0 if successfull, N otherwise
 */
int _oph_json_to_json_file(oph_json * json, char *filename, char **jstring);

/**
 * \brief Function to set the data source properties
 * \param json Pointer to an OPH_JSON object (required)
 * \param srckey Key identifying the source (required)
 * \param srcname Name of the source (required)
 * \param srcurl URL of the source or NULL
 * \param description Description of the source or NULL
 * \param producer Producer user or NULL
 * \return 0 if successfull, N otherwise
 */
int oph_json_set_source(oph_json * json, const char *srckey, const char *srcname, const char *srcurl, const char *description, const char *producer);

/**
 * \brief Function to add a property to the data source
 * \param json Pointer to an OPH_JSON object
 * \param key Key identifying the source property
 * \param value Value of the source property
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_source_detail(oph_json * json, const char *key, const char *value);

/**
 * \brief Function to check if objkey has to be written to file
 * \param objkeys List of admitted objkeys
 * \param objkeys_num Number of admitted objkeys
 * \param objkey Objkey to check for
 * \return 1 if printable, 0 otherwise
 */
int oph_json_is_objkey_printable(char **objkeys, int objkeys_num, const char *objkey);

/***********OPH_JSON INTERNAL FUNCTIONS***********/

// Check if measure type does exist
int oph_json_is_measuretype_correct(const char *measuretype);
// Check if type does exist
int oph_json_is_type_correct(const char *type);
// Add an objkey to the responseKeyset if new
int oph_json_add_responseKey(oph_json * json, const char *responseKey);
// Free consumers
int oph_json_free_consumers(oph_json * json);
// Free responseKeyset
int oph_json_free_responseKeyset(oph_json * json);
// Free source
int oph_json_free_source(oph_json * json);
// Free response
int oph_json_free_response(oph_json * json);

#endif
