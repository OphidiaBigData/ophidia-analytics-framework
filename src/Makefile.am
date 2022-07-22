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

SUBDIRS=

if STANDALONE_MODE
else
SUBDIRS+= oph_gsoap
endif

SUBDIRS+= . clients ioservers drivers

LIBRARY=

LIBRARY+= libdebug.la
LIBRARY+= liboph_json.la
LIBRARY+= libhashtbl.la
LIBRARY+= liboph_binary_io.la
LIBRARY+= liboph_idstring.la
LIBRARY+= liboph_pid.la
LIBRARY+= liboph_utility.la
LIBRARY+= liboph_directory.la
LIBRARY+= liboph_render_output.la
LIBRARY+= liboph_subset.la
LIBRARY+= liboph_task_parser.la
LIBRARY+= liboph_hierarchy.la
LIBRARY+= liboph_dimension.la
LIBRARY+= liboph_datacube.la
LIBRARY+= liboph_driver_proc.la
LIBRARY+= liboph_analytics_operator.la
LIBRARY+= liboph_ioserver_parser.la
LIBRARY+= liboph_ioserver.la
LIBRARY+= liboph_analytics_framework.la
if HAVE_NETCDF
LIBRARY+= liboph_nc.la
endif
if HAVE_CFITSIO
LIBRARY+= liboph_fits.la
endif
if HAVE_ESDM
LIBRARY+= liboph_esdm.la
if HAVE_ESDM_PAV_KERNELS
ESDM_PAV_INCLUDE= -DOPH_ESDM_PAV_KERNELS -I${prefix}/include
ESDM_PAV_LIBRARY= -L${prefix}/lib -lesdm_kernels
else
ESDM_PAV_INCLUDE=
ESDM_PAV_LIBRARY=
endif
endif

lib_LTLIBRARIES = $(LIBRARY)
if DEBUG_1
lib_CFLAGS =  -I. -I../include -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(OPT) -DOPH_WITH_VALGRIND -DOPH_TIME_DEBUG_1
else
if DEBUG_2
lib_CFLAGS =  -I. -I../include -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(OPT) -DOPH_WITH_VALGRIND -DOPH_TIME_DEBUG_2 -DOPH_DEBUG_MYSQL
else
lib_CFLAGS =  -I. -I../include -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DOPH_WEB_SERVER=\"@OPH_WEB_SERVER@\" -DOPH_WEB_SERVER_LOCATION=\"@OPH_WEB_SERVER_LOCATION@\" $(OPT)
endif
endif

if MYSQL_SUPPORT
LIBRARY+= libophidiadb.la
OPHIDIADB=-lophidiadb
lib_CFLAGS += -DOPH_MYSQL_SUPPORT
else
OPHIDIADB=-loph_pid
endif

if DB_SUPPORT
lib_CFLAGS += -DOPH_DB_SUPPORT
endif

libdebug_la_SOURCES = debug.c 
libdebug_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
libdebug_la_LDFLAGS = -static
libdebug_la_LIBADD = -lz -lm @LIBLTDL@

libhashtbl_la_SOURCES = hashtbl.c 
libhashtbl_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
libhashtbl_la_LDFLAGS = -static
libhashtbl_la_LIBADD = -lz -lm @LIBLTDL@

liboph_binary_io_la_SOURCES = oph-lib-binary-io.c
liboph_binary_io_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_binary_io_la_LDFLAGS = -static 
liboph_binary_io_la_LIBADD = -lz -lm @LIBLTDL@ -L. -ldebug

liboph_analytics_operator_la_SOURCES = oph_analytics_operator_library.c
liboph_analytics_operator_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_analytics_operator_la_LDFLAGS = -shared 
liboph_analytics_operator_la_LIBADD = @LIBLTDL@ -L. -ldebug -lhashtbl

liboph_ioserver_la_SOURCES = oph_ioserver/oph_ioserver_library.c
liboph_ioserver_la_CFLAGS= -prefer-pic -I../include/oph_ioserver -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_ioserver_la_LDFLAGS = -shared 
liboph_ioserver_la_LIBADD = @LIBLTDL@ -L. -ldebug -lpthread

liboph_ioserver_parser_la_SOURCES = oph_ioserver/oph_ioserver_parser_library.c
liboph_ioserver_parser_la_CFLAGS= -prefer-pic -I../include/oph_ioserver -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_ioserver_parser_la_LDFLAGS = -shared 
liboph_ioserver_parser_la_LIBADD = @LIBLTDL@ -L. -ldebug -lhashtbl

liboph_idstring_la_SOURCES = oph_idstring_library.c
liboph_idstring_la_CFLAGS= -prefer-pic -I../include @INCLTDL@
liboph_idstring_la_LDFLAGS = -static 
liboph_idstring_la_LIBADD = -lz -lm @LIBLTDL@ -L. -ldebug

liboph_analytics_framework_la_SOURCES = oph_analytics_framework.c
if STANDALONE_MODE
liboph_analytics_framework_la_CFLAGS= ${MYSQL_CFLAGS} -DOPH_STANDALONE_MODE -prefer-pic -I../include -Ioph_gsoap -Ioph_gsoap/$(INTERFACE_TYPE) @INCLTDL@ ${lib_CFLAGS}
liboph_analytics_framework_la_LIBADD = ${MYSQL_LDFLAGS} @LIBLTDL@ -L. ${OPHIDIADB} -loph_analytics_operator -loph_json -loph_task_parser
else
liboph_analytics_framework_la_CFLAGS= ${MYSQL_CFLAGS} -prefer-pic -I../include -Ioph_gsoap -Ioph_gsoap/$(INTERFACE_TYPE) @INCLTDL@ ${lib_CFLAGS}
liboph_analytics_framework_la_LIBADD = ${MYSQL_LDFLAGS} @LIBLTDL@ -Loph_gsoap -loph_soap -L. ${OPHIDIADB} -loph_analytics_operator -loph_json -loph_task_parser
endif
liboph_analytics_framework_la_LDFLAGS = -static $(LIB_OPERATOR)

liboph_json_la_SOURCES = oph_json/oph_json_common.c oph_json/oph_json_graph.c oph_json/oph_json_grid.c  oph_json/oph_json_multigrid.c oph_json/oph_json_text.c oph_json/oph_json_tree.c
liboph_json_la_CFLAGS= -prefer-pic -I../include -I../include/oph_json @INCLTDL@ $(JANSSON_INCLUDE) ${lib_CFLAGS}
liboph_json_la_LDFLAGS = -static -Wl,-rpath -Wl,$(JANSSON_LIBDIR)
liboph_json_la_LIBADD = -lz -lm @LIBLTDL@ $(JANSSON_LIB) -L. -ldebug

liboph_task_parser_la_SOURCES = oph_task_parser_library.c 
liboph_task_parser_la_CFLAGS= $(LIBXML_INCLUDE) -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_task_parser_la_LDFLAGS = -static  
liboph_task_parser_la_LIBADD = -lz -lm $(LIBXML_LIB) @LIBLTDL@ -L. -ldebug -lhashtbl

liboph_pid_la_SOURCES = oph_pid_library.c
if STANDALONE_MODE
liboph_pid_la_CFLAGS= -prefer-pic -DOPH_STANDALONE_MODE ${MYSQL_CFLAGS}  -I../include @INCLTDL@ ${lib_CFLAGS}
else
liboph_pid_la_CFLAGS= -prefer-pic ${MYSQL_CFLAGS}  -I../include @INCLTDL@ ${lib_CFLAGS}
endif
liboph_pid_la_LDFLAGS = -static
liboph_pid_la_LIBADD = -lz -lm @LIBLTDL@ -L. -ldebug

liboph_subset_la_SOURCES = oph_subset_library.c
liboph_subset_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_subset_la_LDFLAGS = -static
liboph_subset_la_LIBADD = -lz -lm @LIBLTDL@  -L. -ldebug

liboph_utility_la_SOURCES = oph_utility_library.c
liboph_utility_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_utility_la_LDFLAGS = -static
liboph_utility_la_LIBADD = -lz -lm @LIBLTDL@  -L. -ldebug

liboph_directory_la_SOURCES = oph_directory_library.c
liboph_directory_la_CFLAGS= -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_directory_la_LDFLAGS = -static  
liboph_directory_la_LIBADD = -lz -lm @LIBLTDL@ -L. -ldebug

liboph_render_output_la_SOURCES = oph_render_output_library.c 
liboph_render_output_la_CFLAGS= $(LIBXML_INCLUDE) -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_render_output_la_LDFLAGS = -static 
liboph_render_output_la_LIBADD = -lz -lm $(LIBXML_LIB) @LIBLTDL@ -L. -ldebug -loph_json

liboph_hierarchy_la_SOURCES = oph_hierarchy_library.c 
liboph_hierarchy_la_CFLAGS= $(LIBXML_INCLUDE) -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_hierarchy_la_LDFLAGS = -static 
liboph_hierarchy_la_LIBADD = -lz -lm $(LIBXML_LIB) @LIBLTDL@ -L. -ldebug

if MYSQL_SUPPORT
libophidiadb_la_SOURCES = ophidiadb/oph_ophidiadb_library.c ophidiadb/oph_odb_filesystem_library.c ophidiadb/oph_odb_job_library.c ophidiadb/oph_odb_metadata_library.c ophidiadb/oph_odb_storage_library.c ophidiadb/oph_odb_dimension_library.c ophidiadb/oph_odb_cube_library.c
libophidiadb_la_CFLAGS= ${MYSQL_CFLAGS} -prefer-pic -I../include -I../include/ophidiadb @INCLTDL@ ${lib_CFLAGS} $(LIBXML_INCLUDE)
libophidiadb_la_LDFLAGS = -static $(LIB_OPERATOR)
libophidiadb_la_LIBADD = -lz -lm ${MYSQL_LDFLAGS} @LIBLTDL@ -L. -ldebug -loph_pid -loph_hierarchy
endif

liboph_dimension_la_SOURCES = oph_dimension_library.c
liboph_dimension_la_CFLAGS= ${MYSQL_CFLAGS} -prefer-pic -I../include @INCLTDL@ ${lib_CFLAGS}
liboph_dimension_la_LDFLAGS = -static $(LIB_OPERATOR)
liboph_dimension_la_LIBADD = -lz -lm ${MYSQL_LDFLAGS} @LIBLTDL@ -L. -ldebug -loph_binary_io -loph_hierarchy 

liboph_datacube_la_SOURCES = oph_datacube_library.c
liboph_datacube_la_CFLAGS= ${MYSQL_CFLAGS} -prefer-pic -I../include -I../include/oph_ioserver @INCLTDL@ ${lib_CFLAGS}
liboph_datacube_la_LDFLAGS = -static
liboph_datacube_la_LIBADD = -lz -lm @LIBLTDL@ -L. -ldebug -loph_binary_io -loph_ioserver 

liboph_driver_proc_la_SOURCES = oph_driver_procedure_library.c
liboph_driver_proc_la_CFLAGS= ${MYSQL_CFLAGS} -prefer-pic -I../include -I../include/oph_ioserver @INCLTDL@ ${lib_CFLAGS}
liboph_driver_proc_la_LDFLAGS = -static
liboph_driver_proc_la_LIBADD = -lz -lm @LIBLTDL@ -L. -lpthread -ldebug ${OPHIDIADB} -loph_datacube  

if HAVE_NETCDF
liboph_nc_la_SOURCES = oph_nc_library.c
liboph_nc_la_CFLAGS= ${MYSQL_CFLAGS} $(NETCDF_CFLAGS) -prefer-pic -I../include -I../include/oph_ioserver @INCLTDL@ ${lib_CFLAGS}
liboph_nc_la_LDFLAGS = -static
liboph_nc_la_LIBADD = -lz -lm $(NETCDF_LIBS) @LIBLTDL@ -L. -ldebug -loph_binary_io -loph_ioserver -loph_datacube
endif

if HAVE_CFITSIO
liboph_fits_la_SOURCES = oph_fits_library.c
liboph_fits_la_CFLAGS= ${MYSQL_CFLAGS} ${LIBCFITSIO_INCLUDE} -prefer-pic -I../include -I../include/oph_ioserver @INCLTDL@ ${lib_CFLAGS}
liboph_fits_la_LDFLAGS = -static
liboph_fits_la_LIBADD = -lz -lm ${LIBCFITSIO_LIB} -lpthread @LIBLTDL@ -L. -ldebug -loph_binary_io -loph_ioserver -loph_datacube
endif

if HAVE_ESDM
liboph_esdm_la_SOURCES = oph_esdm_library.c
liboph_esdm_la_CFLAGS= ${MYSQL_CFLAGS} $(ESDM_CFLAGS) -prefer-pic -I../include -I../include/oph_ioserver @INCLTDL@ ${lib_CFLAGS} ${ESDM_PAV_INCLUDE}
liboph_esdm_la_LDFLAGS = -static
liboph_esdm_la_LIBADD = -lz -lm $(ESDM_LIBS) @LIBLTDL@ -L. -ldebug -loph_binary_io -loph_ioserver -loph_datacube ${ESDM_PAV_LIBRARY}
if MYSQL_SUPPORT
liboph_esdm_la_LIBADD += -lophidiadb
endif
endif

