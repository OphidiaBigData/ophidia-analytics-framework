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

ioserver_CFLAGS = -I. -I../../include -I../../include/oph_ioserver $(OPT)

ioserverdir=${IOSERVER_PATH}
IOSERVER=

if MYSQL_SUPPORT
IOSERVER+=libmysql_ioserver.la
endif
if OPHIDIAIO_SERVER_SUPPORT
IOSERVER+=libophidia_ioserver.la
endif

ioserver_LTLIBRARIES = $(IOSERVER)

if DEBUG_1
libmysql_ioserver_la_CFLAGS =  ${MYSQL_CFLAGS}  ${ioserver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\"  $(OPT)
else
if DEBUG_2
libmysql_ioserver_la_CFLAGS =  ${MYSQL_CFLAGS}  ${ioserver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\"  $(OPT) -DOPH_DEBUG_MYSQL
else
libmysql_ioserver_la_CFLAGS =  ${MYSQL_CFLAGS}  ${ioserver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\"  $(OPT)
endif
endif

if MYSQL_SUPPORT
libmysql_ioserver_la_SOURCES = MYSQL_ioserver.c
libmysql_ioserver_la_LDFLAGS = -module -avoid-version -no-undefined 
libmysql_ioserver_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. -ldebug -lhashtbl -loph_ioserver_parser
endif

if OPHIDIAIO_SERVER_SUPPORT
libophidia_ioserver_la_SOURCES = OPHIDIA_ioserver.c
libophidia_ioserver_la_CFLAGS = ${ioserver_CFLAGS} -DOPH_ANALYTICS_LOCATION=\"${prefix}\"  $(OPT)  ${OPHIDIAIO_SERVER_CFLAGS}
libophidia_ioserver_la_LDFLAGS = -module -avoid-version -no-undefined
libophidia_ioserver_la_LIBADD = -lz ${MYSQL_LDFLAGS} -L.. -ldebug -lhashtbl ${OPHIDIAIO_SERVER_LIBS} -loph_io_client_interface -loph_ioserver_parser
endif

