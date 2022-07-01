#
#    Ophidia Analytics Framework
#    Copyright (C) 2012-2022 CMCC Foundation
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

AM_CFLAGS = $(OPT) -I../../include -Wall -Wextra -Wformat

if DEBUG_1
driver_CFLAGS = -I../../include -I../../include/oph_ioserver $(OPT) -DOPH_DEBUG_LEVEL_1
else
if DEBUG_2
driver_CFLAGS = -I../../include -I../../include/oph_ioserver $(OPT) -DOPH_DEBUG_LEVEL_2 -DOPH_DEBUG_MYSQL
else
driver_CFLAGS = -I../../include -I../../include/oph_ioserver $(OPT) 
endif
endif

if HAVE_NETCDF
driver_CFLAGS += -DOPH_NETCDF
endif

if HAVE_ESDM
driver_CFLAGS += -DOPH_ESDM
if HAVE_ESDM_PAV_KERNELS
driver_CFLAGS += -DOPH_ESDM_PAV_KERNERS
endif
endif

#define set of libraries commonly called together
BASIC_LIB = -L.. -lophidiadb -loph_task_parser -loph_idstring -loph_json -ldebug -lhashtbl -loph_pid

driverdir=${DRIVER_PATH}
DRIVER=

DRIVER+=liboph_explorecubeoperator.la
DRIVER+=liboph_permuteoperator.la
DRIVER+=liboph_subsetoperator.la
DRIVER+=liboph_reduceoperator.la
DRIVER+=liboph_reduce2operator.la
DRIVER+=liboph_aggregateoperator.la
DRIVER+=liboph_aggregate2operator.la
DRIVER+=liboph_intercubeoperator.la
DRIVER+=liboph_intercube2operator.la
DRIVER+=liboph_drilldownoperator.la
DRIVER+=liboph_rollupoperator.la
DRIVER+=liboph_duplicateoperator.la
DRIVER+=liboph_applyoperator.la
DRIVER+=liboph_randcubeoperator.la
DRIVER+=liboph_randcube2operator.la
DRIVER+=liboph_deleteoperator.la
DRIVER+=liboph_listoperator.la
DRIVER+=liboph_unpublishoperator.la
DRIVER+=liboph_cubeelementsoperator.la
DRIVER+=liboph_cubesizeoperator.la
DRIVER+=liboph_deletecontaineroperator.la
DRIVER+=liboph_splitoperator.la
DRIVER+=liboph_mergeoperator.la
DRIVER+=liboph_mergecubesoperator.la
DRIVER+=liboph_mergecubes2operator.la
DRIVER+=liboph_cubeschemaoperator.la
DRIVER+=liboph_containerschemaoperator.la
DRIVER+=liboph_instancesoperator.la
DRIVER+=liboph_cubeiooperator.la
DRIVER+=liboph_tasksoperator.la
if HAVE_NETCDF
if GSL_SUPPORT
DRIVER+=liboph_explorencoperator.la
endif
DRIVER+=liboph_importncoperator.la
DRIVER+=liboph_importnc2operator.la
DRIVER+=liboph_importncsoperator.la
DRIVER+=liboph_exportncoperator.la
DRIVER+=liboph_concatncoperator.la
DRIVER+=liboph_concatnc2operator.la
if HAVE_PARALLEL_NETCDF
DRIVER+=liboph_exportnc2operator.la
endif
endif
DRIVER+=liboph_operators_listoperator.la
DRIVER+=liboph_primitives_listoperator.la
DRIVER+=liboph_log_infooperator.la
DRIVER+=liboph_publishoperator.la
DRIVER+=liboph_loggingbkoperator.la
DRIVER+=liboph_manoperator.la
DRIVER+=liboph_metadataoperator.la
DRIVER+=liboph_createcontaineroperator.la
DRIVER+=liboph_searchoperator.la
DRIVER+=liboph_folderoperator.la
DRIVER+=liboph_fsoperator.la
DRIVER+=liboph_hierarchyoperator.la
DRIVER+=liboph_movecontaineroperator.la
DRIVER+=liboph_showgridoperator.la
DRIVER+=liboph_scriptoperator.la
DRIVER+=liboph_cdooperator.la
DRIVER+=liboph_genericoperator.la
if HAVE_CURL
DRIVER+=liboph_b2dropoperator.la
endif
if HAVE_CFITSIO
DRIVER+=liboph_importfitsoperator.la
endif
if HAVE_ESDM
DRIVER+=liboph_importesdmoperator.la
DRIVER+=liboph_importesdm2operator.la
DRIVER+=liboph_exportesdmoperator.la
if HAVE_PARALLEL_ESDM
DRIVER+=liboph_exportesdm2operator.la
endif
DRIVER+=liboph_concatesdmoperator.la
DRIVER+=liboph_concatesdm2operator.la
if HAVE_ESDM_PAV_KERNELS
ESDM_PAV_LIBRARY= -L${prefix}/lib -lesdm_kernels
else
ESDM_PAV_LIBRARY=
endif
endif

driver_LTLIBRARIES = $(DRIVER)

liboph_aggregateoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_aggregateoperator_la_SOURCES = OPH_AGGREGATE_operator.c
liboph_aggregateoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_aggregateoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_aggregate2operator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_aggregate2operator_la_SOURCES = OPH_AGGREGATE2_operator.c
liboph_aggregate2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_aggregate2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_applyoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_applyoperator_la_SOURCES = OPH_APPLY_operator.c
liboph_applyoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_applyoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy $(LIBXML_LIB) -loph_driver_proc

if HAVE_CURL
liboph_b2dropoperator_la_CFLAGS =  ${MYSQL_CFLAGS} $(LIBCURL_INCLUDE) ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_b2dropoperator_la_SOURCES = OPH_B2DROP_operator.c
liboph_b2dropoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_b2dropoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(LIBCURL_LIB) $(BASIC_LIB) -loph_directory
endif

liboph_cdooperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" -I../oph_gsoap
liboph_cdooperator_la_SOURCES = OPH_CDO_operator.c
liboph_cdooperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_cdooperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_directory
if HAVE_ESDM
liboph_cdooperator_la_CFLAGS += $(ESDM_CFLAGS)
endif

liboph_genericoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" -I../oph_gsoap
liboph_genericoperator_la_SOURCES = OPH_GENERIC_operator.c
liboph_genericoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_genericoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_directory
if HAVE_ESDM
liboph_genericoperator_la_CFLAGS += $(ESDM_CFLAGS)
endif

liboph_containerschemaoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_containerschemaoperator_la_SOURCES = OPH_CONTAINERSCHEMA_operator.c
liboph_containerschemaoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_containerschemaoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_createcontaineroperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_createcontaineroperator_la_SOURCES = OPH_CREATECONTAINER_operator.c
liboph_createcontaineroperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_createcontaineroperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_dimension

liboph_cubeelementsoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_cubeelementsoperator_la_SOURCES = OPH_CUBEELEMENTS_operator.c
liboph_cubeelementsoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_cubeelementsoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver

liboph_cubeiooperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_cubeiooperator_la_SOURCES = OPH_CUBEIO_operator.c
liboph_cubeiooperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_cubeiooperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_cubeschemaoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_cubeschemaoperator_la_SOURCES = OPH_CUBESCHEMA_operator.c
liboph_cubeschemaoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_cubeschemaoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_dimension -loph_utility -loph_hierarchy

liboph_cubesizeoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_cubesizeoperator_la_SOURCES = OPH_CUBESIZE_operator.c
liboph_cubesizeoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_cubesizeoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_utility

liboph_deleteoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_deleteoperator_la_SOURCES = OPH_DELETE_operator.c
liboph_deleteoperator_la_LDFLAGS = -module -avoid-version -no-undefined 
liboph_deleteoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -loph_driver_proc $(BASIC_LIB) -loph_datacube -loph_ioserver 

liboph_deletecontaineroperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_deletecontaineroperator_la_SOURCES = OPH_DELETECONTAINER_operator.c
liboph_deletecontaineroperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_deletecontaineroperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_dimension -loph_driver_proc -loph_ioserver

liboph_drilldownoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_drilldownoperator_la_SOURCES = OPH_DRILLDOWN_operator.c
liboph_drilldownoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_drilldownoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_duplicateoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_duplicateoperator_la_SOURCES = OPH_DUPLICATE_operator.c
liboph_duplicateoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_duplicateoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_explorecubeoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} 
liboph_explorecubeoperator_la_SOURCES = OPH_EXPLORECUBE_operator.c
liboph_explorecubeoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_explorecubeoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_subset -loph_utility

liboph_folderoperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_folderoperator_la_SOURCES = OPH_FOLDER_operator.c
liboph_folderoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_folderoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_fsoperator_la_CFLAGS  =  ${MYSQL_CFLAGS} ${driver_CFLAGS}
liboph_fsoperator_la_SOURCES = OPH_FS_operator.c
liboph_fsoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_fsoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)
if HAVE_NETCDF
liboph_fsoperator_la_CFLAGS += $(NETCDF_CFLAGS)
liboph_fsoperator_la_LIBADD += $(NETCDF_LIBS) -L.. -loph_nc
endif
if HAVE_ESDM
liboph_fsoperator_la_CFLAGS += $(ESDM_CFLAGS)
liboph_fsoperator_la_LIBADD += $(ESDM_LIBS) -L.. -loph_esdm -loph_dimension
endif

liboph_hierarchyoperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_hierarchyoperator_la_SOURCES = OPH_HIERARCHY_operator.c
liboph_hierarchyoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_hierarchyoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_render_output

liboph_instancesoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_instancesoperator_la_SOURCES = OPH_INSTANCES_operator.c
liboph_instancesoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_instancesoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_intercubeoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_intercubeoperator_la_SOURCES = OPH_INTERCUBE_operator.c
liboph_intercubeoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_intercubeoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_intercube2operator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_intercube2operator_la_SOURCES = OPH_INTERCUBE2_operator.c
liboph_intercube2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_intercube2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

if HAVE_NETCDF
liboph_exportncoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_exportncoperator_la_SOURCES = OPH_EXPORTNC_operator.c
liboph_exportncoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_exportncoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(NETCDF_LIBS) -L.. -loph_nc $(BASIC_LIB) -loph_directory -loph_dimension -loph_datacube -loph_ioserver
if HAVE_PARALLEL_NETCDF
liboph_exportnc2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" 
liboph_exportnc2operator_la_SOURCES = OPH_EXPORTNC2_operator.c
liboph_exportnc2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_exportnc2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -loph_directory -loph_dimension -loph_datacube -loph_ioserver
endif
liboph_importncoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_importncoperator_la_SOURCES = OPH_IMPORTNC_operator.c
liboph_importncoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importncoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -loph_dimension -loph_hierarchy  -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc

liboph_importnc2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_importnc2operator_la_SOURCES = OPH_IMPORTNC2_operator.c
liboph_importnc2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importnc2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -lpthread -loph_dimension -loph_hierarchy  -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc

liboph_importncsoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_importncsoperator_la_SOURCES = OPH_IMPORTNCS_operator.c
liboph_importncsoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importncsoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -lpthread -loph_dimension -loph_hierarchy  -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc

liboph_concatncoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_concatncoperator_la_SOURCES = OPH_CONCATNC_operator.c
liboph_concatncoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_concatncoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc

liboph_concatnc2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(NETCDF_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_concatnc2operator_la_SOURCES = OPH_CONCATNC2_operator.c
liboph_concatnc2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_concatnc2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -lpthread -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc

if GSL_SUPPORT
liboph_explorencoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} $(NETCDF_CFLAGS) $(GSL_INCLUDE) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_explorencoperator_la_SOURCES = OPH_EXPLORENC_operator.c
liboph_explorencoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_explorencoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. $(NETCDF_LIBS) -loph_nc $(BASIC_LIB) -loph_dimension $(GSL_LIB)
endif
endif
liboph_listoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_listoperator_la_SOURCES = OPH_LIST_operator.c
liboph_listoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_listoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_utility

liboph_log_infooperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_log_infooperator_la_SOURCES = OPH_LOG_INFO_operator.c
liboph_log_infooperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_log_infooperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_loggingbkoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_loggingbkoperator_la_SOURCES = OPH_LOGGINGBK_operator.c
liboph_loggingbkoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_loggingbkoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_manoperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_manoperator_la_SOURCES = OPH_MAN_operator.c
liboph_manoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_manoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_render_output -loph_json

liboph_mergeoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_mergeoperator_la_SOURCES = OPH_MERGE_operator.c
liboph_mergeoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_mergeoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_mergecubesoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_mergecubesoperator_la_SOURCES = OPH_MERGECUBES_operator.c
liboph_mergecubesoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_mergecubesoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_mergecubes2operator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_mergecubes2operator_la_SOURCES = OPH_MERGECUBES2_operator.c
liboph_mergecubes2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_mergecubes2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_metadataoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_metadataoperator_la_SOURCES = OPH_METADATA_operator.c
liboph_metadataoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_metadataoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_movecontaineroperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_movecontaineroperator_la_SOURCES = OPH_MOVECONTAINER_operator.c
liboph_movecontaineroperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_movecontaineroperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_dimension

liboph_operators_listoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_operators_listoperator_la_SOURCES = OPH_OPERATORS_LIST_operator.c
liboph_operators_listoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_operators_listoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_permuteoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_permuteoperator_la_SOURCES = OPH_PERMUTE_operator.c
liboph_permuteoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_permuteoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_primitives_listoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_primitives_listoperator_la_SOURCES = OPH_PRIMITIVES_LIST_operator.c
liboph_primitives_listoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_primitives_listoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver

liboph_publishoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_publishoperator_la_SOURCES = OPH_PUBLISH_operator.c
liboph_publishoperator_la_LDFLAGS = -module -avoid-version -no-undefined 
liboph_publishoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_directory -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy

liboph_randcubeoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_randcubeoperator_la_SOURCES = OPH_RANDCUBE_operator.c
liboph_randcubeoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_randcubeoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_binary_io -loph_driver_proc

liboph_randcube2operator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_randcube2operator_la_SOURCES = OPH_RANDCUBE2_operator.c
liboph_randcube2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_randcube2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_binary_io -loph_driver_proc

liboph_reduceoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_reduceoperator_la_SOURCES = OPH_REDUCE_operator.c
liboph_reduceoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_reduceoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_reduce2operator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_reduce2operator_la_SOURCES = OPH_REDUCE2_operator.c
liboph_reduce2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_reduce2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_rollupoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_rollupoperator_la_SOURCES = OPH_ROLLUP_operator.c
liboph_rollupoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_rollupoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension -loph_hierarchy -loph_driver_proc

liboph_scriptoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" -I../oph_gsoap
liboph_scriptoperator_la_SOURCES = OPH_SCRIPT_operator.c
liboph_scriptoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_scriptoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_directory

liboph_searchoperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_searchoperator_la_SOURCES = OPH_SEARCH_operator.c
liboph_searchoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_searchoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) 

liboph_showgridoperator_la_CFLAGS  =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_showgridoperator_la_SOURCES = OPH_SHOWGRID_operator.c
liboph_showgridoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_showgridoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_dimension -loph_hierarchy

liboph_splitoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_splitoperator_la_SOURCES = OPH_SPLIT_operator.c
liboph_splitoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_splitoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_datacube -loph_ioserver -loph_dimension  -loph_hierarchy -loph_driver_proc

liboph_subsetoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_subsetoperator_la_SOURCES = OPH_SUBSET_operator.c
liboph_subsetoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_subsetoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -lpthread  -loph_subset -loph_datacube -loph_ioserver -loph_dimension  -loph_hierarchy -loph_driver_proc

liboph_tasksoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS}
liboph_tasksoperator_la_SOURCES = OPH_TASKS_operator.c
liboph_tasksoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_tasksoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB)

liboph_unpublishoperator_la_CFLAGS =  ${MYSQL_CFLAGS}  ${driver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" 
liboph_unpublishoperator_la_SOURCES = OPH_UNPUBLISH_operator.c
liboph_unpublishoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_unpublishoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(BASIC_LIB) -loph_directory

if HAVE_CFITSIO
liboph_importfitsoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} ${LIBCFITSIO_INCLUDE} -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_importfitsoperator_la_SOURCES = OPH_IMPORTFITS_operator.c
liboph_importfitsoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importfitsoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} ${LIBCFITSIO_LIB} -lpthread -loph_fits $(BASIC_LIB) -loph_dimension -loph_hierarchy  -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc
endif

if HAVE_ESDM
liboph_importesdmoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE) -I${prefix}/include
liboph_importesdmoperator_la_SOURCES = OPH_IMPORTESDM_operator.c
liboph_importesdmoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importesdmoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc -loph_esdm ${ESDM_PAV_LIBRARY}

liboph_importesdm2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE) -I${prefix}/include
liboph_importesdm2operator_la_SOURCES = OPH_IMPORTESDM2_operator.c
liboph_importesdm2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_importesdm2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -lpthread -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc -loph_esdm ${ESDM_PAV_LIBRARY}

liboph_exportesdmoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_exportesdmoperator_la_SOURCES = OPH_EXPORTESDM_operator.c
liboph_exportesdmoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_exportesdmoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -loph_directory -loph_dimension -loph_datacube -loph_ioserver -loph_esdm
if HAVE_PARALLEL_ESDM
liboph_exportesdm2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\"
liboph_exportesdm2operator_la_SOURCES = OPH_EXPORTESDM2_operator.c
liboph_exportesdm2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_exportesdm2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -loph_directory -loph_dimension -loph_datacube -loph_ioserver -loph_esdm
endif
liboph_concatesdmoperator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_concatesdmoperator_la_SOURCES = OPH_CONCATESDM_operator.c
liboph_concatesdmoperator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_concatesdmoperator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc -loph_esdm

liboph_concatesdm2operator_la_CFLAGS =  ${MYSQL_CFLAGS} ${driver_CFLAGS} $(ESDM_CFLAGS) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(LIBXML_INCLUDE)
liboph_concatesdm2operator_la_SOURCES = OPH_CONCATESDM2_operator.c
liboph_concatesdm2operator_la_LDFLAGS = -module -avoid-version -no-undefined
liboph_concatesdm2operator_la_LIBADD = -lz ${MYSQL_LDFLAGS} $(ESDM_LIBS) $(BASIC_LIB) -loph_dimension -loph_hierarchy -loph_datacube -loph_ioserver -loph_binary_io -loph_driver_proc -loph_esdm
endif

