/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#ifndef __OPH_FRAMEWORK_PATHS_H
#define __OPH_FRAMEWORK_PATHS_H

#define OPH_VERSION						"Ophidia Analytics Framework, version %s\nCopyright (C) 2012-2021 CMCC Foundation - www.cmcc.it\n"
#define OPH_DISCLAIMER						"This program comes with ABSOLUTELY NO WARRANTY; for details type `oph_analytics_framework -x'.\nThis is free software, and you are welcome to redistribute it\nunder certain conditions; type `oph_analytics_framework -z' for details.\n"
#define OPH_WARRANTY						"\nTHERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES PROVIDE THE PROGRAM “AS IS” WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU. SHOULD THE PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION.\n"
#include "oph_license.h"

#define OPH_PREFIX_CLUSTER                                      OPH_WEB_SERVER_LOCATION"/share"
#define OPH_SUFFIX_DATA						"/%d/%d"

#define OPH_FRAMEWORK_CONF_PATH_SIZE             		1000

#define OPH_FRAMEWORK_OPERATOR_XML_FOLDER_PATH                  "%s/etc/operators_xml"
#define OPH_FRAMEWORK_PRIMITIVE_XML_FOLDER_PATH                 "%s/etc/primitives_xml"
#define OPH_FRAMEWORK_HIERARCHY_XML_FOLDER_PATH                 "%s/etc/hierarchies_xml"
#define OPH_FRAMEWORK_SCRIPT_FOLDER_PATH			"%s/etc/script"
#define OPH_FRAMEWORK_OPERATOR_XML_FILE_PATH_DESC               "%s/etc/operators_xml/%s"
#define OPH_FRAMEWORK_PRIMITIVE_XML_FILE_PATH_DESC              "%s/etc/primitives_xml/%s"
#define OPH_FRAMEWORK_HIERARCHY_XML_FILE_PATH_DESC              "%s/etc/hierarchies_xml/%s"
#define OPH_FRAMEWORK_OPERATOR_DTD_PATH     	                OPH_ANALYTICS_LOCATION"/etc/ophidia_dtd/ophidiaoperator.dtd"
#define OPH_FRAMEWORK_PRIMITIVE_DTD_PATH     	                OPH_ANALYTICS_LOCATION"/etc/ophidia_dtd/ophidiaprimitive.dtd"
#define OPH_FRAMEWORK_HIERARCHY_DTD_PATH     	                OPH_ANALYTICS_LOCATION"/etc/ophidia_dtd/ophidiahierarchy.dtd"

#define OPH_FRAMEWORK_FS_DEFAULT_PATH 				"-"

#define OPH_FRAMEWORK_DYNAMIC_LIBRARY_FILE_PATH         	"%s/etc/oph_analytics_driver"
#define OPH_FRAMEWORK_DYNAMIC_SERVER_FILE_PATH         		"%s/etc/oph_io_server"
#define OPH_FRAMEWORK_LOG_PATH                                  "log/container_%d.log"
#define OPH_FRAMEWORK_LOG_PATH_WITH_PREFIX                      "%s/container_%d.log"
#define OPH_FRAMEWORK_LOG_PATH_PREFIX                           "%s/log"
#define OPH_FRAMEWORK_OPHIDIADB_CONF_FILE_PATH          	"%s/etc/oph_configuration"
#define OPH_FRAMEWORK_DIMENSION_CONF_FILE_PATH          	"%s/etc/oph_configuration"
#define OPH_FRAMEWORK_SOAP_CONF_FILE_PATH			"%s/etc/oph_soap_configuration"
#define OPH_FRAMEWORK_SCRIPT_CONF_FILE_PATH			"%s/etc/oph_script_configuration"

#define OPH_FRAMEWORK_JSON_GENERIC_PATH				"%s/%s.json"
#define OPH_FRAMEWORK_JSON_PATH					"%s/sessions/%s/json/response/%s.json"
#define OPH_FRAMEWORK_MAP_FILES_PATH				"%s/sessions/%s/export/img"OPH_SUFFIX_DATA
#define OPH_FRAMEWORK_MISCELLANEA_FILES_PATH			"%s/sessions/%s/export/misc"
#define OPH_FRAMEWORK_HTML_FILES_PATH				"%s/sessions/%s/export/html"OPH_SUFFIX_DATA
#define OPH_FRAMEWORK_NCL_FILES_PATH				"%s/sessions/%s/export/ncl"OPH_SUFFIX_DATA
#define OPH_FRAMEWORK_NC_FILES_PATH				"%s/sessions/%s/export/nc"OPH_SUFFIX_DATA

#define OPH_FRAMEWORK_FIRST_ARROW_PATH_NO_PREFIX		OPH_WEB_SERVER"/img/first.png"
#define OPH_FRAMEWORK_PREV_ARROW_PATH_NO_PREFIX			OPH_WEB_SERVER"/img/prev.png"
#define OPH_FRAMEWORK_NEXT_ARROW_PATH_NO_PREFIX			OPH_WEB_SERVER"/img/next.png"
#define OPH_FRAMEWORK_LAST_ARROW_PATH_NO_PREFIX			OPH_WEB_SERVER"/img/last.png"
#define OPH_FRAMEWORK_MAP_FILES_PATH_NO_PREFIX      		OPH_WEB_SERVER"/img/%s"

#define OPH_FRAMEWORK_SERVER_LOG_PATH                           OPH_FRAMEWORK_LOG_PATH_PREFIX"/server.log"
#define OPH_FRAMEWORK_CONTAINER_LOG_PATH                        OPH_FRAMEWORK_LOG_PATH_PREFIX"/container_%d.log"
#define OPH_FRAMEWORK_IOSERVER_LOG_PATH2                        OPH_FRAMEWORK_LOG_PATH_PREFIX"/server_%s.log"

#define OPH_FRAMEWORK_IOSERVER_LOG_PATH				"log/server_%s.log"
#define OPH_FRAMEWORK_IOSERVER_LOG_PATH_WITH_PREFIX		"%s/server_%s.log"

#endif				//__OPH_FRAMEWORK_PATHS_H
