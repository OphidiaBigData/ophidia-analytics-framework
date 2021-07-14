#
#    Ophidia Analytics Framework
#    Copyright (C) 2012-2020 CMCC Foundation
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

if !MPI_DISABLE

bin_PROGRAMS = esdm_pav_runtime_worker
else
if MYSQL_SUPPORT
if DEBUG_1
bin_PROGRAMS=oph_analytics_framework oph_ioserver_client
else
if DEBUG_2
bin_PROGRAMS=oph_analytics_framework oph_ioserver_client
else
bin_PROGRAMS=oph_analytics_framework
endif
endif
else
bin_PROGRAMS=oph_analytics_framework
endif

endif

bindir=${prefix}/bin

if MYSQL_SUPPORT
OPHIDIADB_CFLAGS=-DOPH_MYSQL_SUPPORT
OPHIDIADB_LIBS=-lophidiadb
else
OPHIDIADB_CFLAGS=
OPHIDIADB_LIBS=
endif

AM_CFLAGS = $(OPT)

oph_analytics_framework_SOURCES=oph_analytics_framework.c
oph_analytics_framework_CFLAGS= $(OPT) -I../../include @INCLTDL@ -DOPH_PARALLEL_LOCATION=\"${prefix}\" -DPACKAGE_VERSION=\"@PACKAGE_VERSION@\"
oph_analytics_framework_LDADD= -L.. -loph_analytics_framework @LIBLTDL@

oph_ioserver_client_SOURCES= oph_ioserver_client.c 
oph_ioserver_client_CFLAGS= $(OPT) -I../../include -I../../include/oph_ioserver @INCLTDL@ -DOPH_PARALLEL_LOCATION=\"${prefix}\" ${MYSQL_CFLAGS} -DPACKAGE_VERSION=\"@PACKAGE_VERSION@\" ${OPHIDIADB_CFLAGS}
oph_ioserver_client_LDADD= ${MYSQL_LDFLAGS} -L.. -loph_datacube ${OPHIDIADB_LIBS} @LIBLTDL@

if !MPI_DISABLE
esdm_pav_runtime_worker_SOURCES = esdm_pav_runtime_worker.c
esdm_pav_runtime_worker_CFLAGS = $(OPT) -I../../include -I../oph_gsoap @INCLTDL@ $(RABBITMQ_INCLUDE) -DOPH_ANALYTICS_LOCATION=\"${prefix}\" -DPACKAGE_VERSION=\"@PACKAGE_VERSION@\" -DMPI_DISABLE_SUPPORT
esdm_pav_runtime_worker_LDADD = -L.. -ldebug -loph_server_conf -lrabbitmq_utils -loph_analytics_framework $(RABBITMQ_LIB)
endif
