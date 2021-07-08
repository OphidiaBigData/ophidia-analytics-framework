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

if HAVE_ESDM

KERNEL=

KERNEL+=liboph_esdm_kernels.la

lib_LTLIBRARIES = $(KERNEL)

liboph_esdm_kernels_la_CFLAGS = -prefer-pic -I../../include/esdm_kernels $(ESDM_CFLAGS)
liboph_esdm_kernels_la_SOURCES = oph_esdm_kernels.c
liboph_esdm_kernels_la_LDFLAGS = -static
liboph_esdm_kernels_la_LIBADD = $(ESDM_LIBS)

endif

