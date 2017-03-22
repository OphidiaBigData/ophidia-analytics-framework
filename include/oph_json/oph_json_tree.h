/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#ifndef __OPH_JSON_TREE_H__
#define __OPH_JSON_TREE_H__

#include "oph_json_common.h"

/***********OPH_JSON_OBJ_TREE STRUCTURE***********/

/**
 * \brief Structure that defines the OBJCLASS tree
 * \param title Title
 * \param description Tree description
 * \param nodekeys Node properties
 * \param nodekeys_num Number of node properties
 * \param nodevalues Node properties values
 * \param nodevalues_num1 Number of nodes
 * \param nodevalues_num2 Number of properties
 * \param rootnode Index of root node
 * \param nodelinks Array of link lists between nodes. Array index is equal to the index of the first node in the links.
 * \param nodelinks_num Number of nodes
 */
typedef struct _oph_json_obj_tree {
	char *title;
	char *description;
	char **nodekeys;
	unsigned int nodekeys_num;
	char ***nodevalues;
	unsigned int nodevalues_num1;
	unsigned int nodevalues_num2;
	char *rootnode;
	oph_json_links *nodelinks;
	unsigned int nodelinks_num;
} oph_json_obj_tree;

/***********OPH_JSON_OBJ_TREE FUNCTIONS***********/

/**
 * \brief Function to add a tree object to OPH_JSON
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying objects (required)
 * \param title A title (required)
 * \param description A description string or NULL
 * \param nodekeys Node properties or NULL
 * \param nodekeys_num Number of node properties or 0
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_tree(oph_json * json, const char *objkey, const char *title, const char *description, char **nodekeys, int nodekeys_num);

/**
 * \brief Function to add a node to a tree object in OPH_JSON. Node indexes are autoincremental and starting from 0.
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying the object (required)
 * \param nodevalues The values of node properties or NULL
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_tree_node(oph_json * json, const char *objkey, char **nodevalues);

/**
 * \brief Function to set a node of a tree object in OPH_JSON as a root node
 * \param json Pointer to an OPH_JSON object
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying the object
 * \param rootnode The index of the root node
 * \return 0 if successfull, N otherwise
 */
int oph_json_set_tree_root(oph_json * json, const char *objkey, int rootnode);

/**
 * \brief Function to add a link between two nodes of a tree object in OPH_JSON
 * \param json Pointer to an OPH_JSON object (required)
 * \param objkey One of the pre-defined OPH_JSON objkeys identifying the object (required)
 * \param sourcenode The index of the parent node (required)
 * \param targetnode The index of the child node (required)
 * \param description A description for the link or NULL
 * \return 0 if successfull, N otherwise
 */
int oph_json_add_tree_link(oph_json * json, const char *objkey, int sourcenode, int targetnode, const char *description);

/***********OPH_JSON_OBJ_TREE INTERNAL FUNCTIONS***********/

// Free a tree object contents
int oph_json_free_tree(oph_json_obj_tree * obj);

#endif
