#
#    Ophidia Analytics Framework
#    Copyright (C) 2012-2021 CMCC Foundation
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

AUTOMAKE_OPTIONS = subdir-objects

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = . libltdl src 

if MYSQL_SUPPORT
sysconf_DATA = etc/oph_analytics_driver etc/oph_io_server etc/oph_configuration etc/ophidiadb.sql etc/oph_soap_configuration etc/oph_script_configuration
else
sysconf_DATA = etc/oph_analytics_driver etc/oph_io_server etc/oph_configuration etc/oph_soap_configuration etc/oph_script_configuration
endif
 
logdir = $(prefix)/log/
dist_log_DATA =

xmloperdir = $(prefix)/etc/operators_xml/
OPERATOR_XML=

OPERATOR_XML+=etc/operators_xml/OPH_CDO_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_FS_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_GENERIC_operator_1.0.xml

if MYSQL_SUPPORT
OPERATOR_XML+=etc/operators_xml/OPH_AGGREGATE2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_AGGREGATE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_APPLY_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_B2DROP_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CANCEL_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CLUSTER_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CONTAINERSCHEMA_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CREATECONTAINER_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CUBEELEMENTS_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CUBEIO_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CUBESCHEMA_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CUBESIZE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_DELETECONTAINER_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_DELETE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_DRILLDOWN_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_DUPLICATE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_ELSE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_ELSEIF_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_ENDFOR_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_ENDIF_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_EXPLORECUBE_operator_1.0.xml
if HAVE_NETCDF
OPERATOR_XML+=etc/operators_xml/OPH_EXPORTNC_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_IMPORTNC_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_IMPORTNC2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CONCATNC_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CONCATNC2_operator_1.0.xml
if GSL_SUPPORT
OPERATOR_XML+=etc/operators_xml/OPH_EXPLORENC_operator_1.0.xml
endif
if HAVE_PARALLEL_NETCDF
OPERATOR_XML+=etc/operators_xml/OPH_EXPORTNC2_operator_1.0.xml
endif
endif
if HAVE_ESDM
OPERATOR_XML+=etc/operators_xml/OPH_IMPORTESDM_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_IMPORTESDM2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_EXPORTESDM_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_CONCATESDM_operator_1.0.xml
endif
OPERATOR_XML+=etc/operators_xml/OPH_FOLDER_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_FOR_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_GET_CONFIG_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_HIERARCHY_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_IF_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_INPUT_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_INSTANCES_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_INTERCUBE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_LIST_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_LOGGINGBK_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_LOG_INFO_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MANAGE_SESSION_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MAN_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MASSIVE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MERGECUBES_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MERGECUBES2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MERGE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_METADATA_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_MOVECONTAINER_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_OPERATORS_LIST_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_PERMUTE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_PRIMITIVES_LIST_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_PUBLISH_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_RANDCUBE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_RANDCUBE2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_REDUCE2_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_REDUCE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_RESUME_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_ROLLUP_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SCRIPT_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SEARCH_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SERVICE_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SET_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SHOWGRID_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SPLIT_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_SUBSET_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_TASKS_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_UNPUBLISH_operator_1.0.xml
OPERATOR_XML+=etc/operators_xml/OPH_WAIT_operator_1.0.xml
endif

dist_xmloper_DATA = $(OPERATOR_XML)

if MYSQL_SUPPORT
xmlprimdir = $(prefix)/etc/primitives_xml/
dist_xmlprim_DATA = etc/primitives_xml/*.xml

xmlhierdir = $(prefix)/etc/hierarchies_xml/
dist_xmlhier_DATA = etc/hierarchies_xml/*.xml
endif

dtddir = $(prefix)/etc/ophidia_dtd/
if MYSQL_SUPPORT
dist_dtd_DATA = etc/ophidia_dtd/*.dtd
else
dist_dtd_DATA = etc/ophidia_dtd/ophidiaoperator.dtd
endif

imgdir = @OPH_WEB_SERVER_LOCATION@/img/
dist_img_DATA = etc/img/*.png

xmlwebdir = @OPH_WEB_SERVER_LOCATION@/operators_xml/
dist_xmlweb_DATA = $(OPERATOR_XML)

scriptdir = $(prefix)/etc/script/
dist_script_DATA =

