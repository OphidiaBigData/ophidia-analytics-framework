#
#    Ophidia Analytics Framework
#    Copyright (C) 2012-2023 CMCC Foundation
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

LIBRARY=liboph_soap.la
lib_LTLIBRARIES = $(LIBRARY)

if INTERFACE_TYPE_IS_SSL

liboph_soap_la_SOURCES = $(INTERFACE_TYPE)/oph_soap.c stdsoap2.c soapC.c soapClient.c
liboph_soap_la_CFLAGS= -prefer-pic -I../../include @INCLTDL@ -DOPH_ANALYTICS_LOCATION=\"${prefix}\" $(OPT) -I$(INTERFACE_TYPE) -DWITH_OPENSSL -DHAVE_OPENSSL_SSL_H $(LIBSSL_INCLUDE) -DINTERFACE_TYPE_IS_SSL
liboph_soap_la_LDFLAGS = -static
liboph_soap_la_LIBADD = -lm @LIBLTDL@ $(LIBSSL_LIB)

endif



if INTERFACE_TYPE_IS_GSI

DEFINE = -DPLUGIN_DEFAULT_PORT=$(PLUGIN_DEFAULT_PORT) \
	-DPLUGIN_DEFAULT_HOSTNAME=\"$(PLUGIN_DEFAULT_HOSTNAME)\" \
	-DAUTHORIZATION_FILE=\"$(AUTHORIZATION_FILE)\" \
	-DGSI_PLUGIN_DEBUG=1
if VOMS
GLOBUS_LIBS = -lglobus_gssapi_gsi -lglobus_gsi_proxy_core -lglobus_gsi_credential -lglobus_gsi_callback -lglobus_oldgaa -lglobus_gsi_sysconfig -lglobus_gsi_cert_utils -lglobus_openssl -lglobus_proxy_ssl -lglobus_openssl_error -lssl -lcrypto -lglobus_common
GLOBUS_GRAM_LIBS = -lglobus_gram_client -lglobus_gram_protocol -lglobus_io -lglobus_gss_assist -lglobus_gssapi_gsi -lglobus_gsi_proxy_core -lglobus_gsi_credential -lglobus_gsi_callback -lglobus_oldgaa -lglobus_gsi_sysconfig -lglobus_gsi_cert_utils -lglobus_openssl -lglobus_proxy_ssl -lglobus_openssl_error -lssl -lcrypto -lglobus_common
AM_CFLAGS = $(OPT) -D$(PLATFORM) -D$(VOMS_DEFINE) -I../include -I$(VOMS_INCLUDE) -Wall
MYCFLAGS = $(AM_CFLAGS) -I$(GLOBUS_INCLUDE) $(DEFINE)
VOMS_LINK = -lvomsapi
MYLDFLAGS  = -L$(VOMS_LIBS) $(VOMS_LINK) -L$(GLOBUS_LIB) $(GLOBUS_LIBS)
MYGRAMLDFLAGS  = -L$(VOMS_LIBS) $(VOMS_LINK) -L$(GLOBUS_LIB) $(GLOBUS_GRAM_LIBS)
else
GLOBUS_LIBS = -lglobus_gssapi_gsi -lglobus_gsi_proxy_core -lglobus_gsi_credential -lglobus_gsi_callback -lglobus_oldgaa -lglobus_gsi_sysconfig -lglobus_gsi_cert_utils -lglobus_openssl -lglobus_openssl_error -lglobus_proxy_ssl -lglobus_common -lssl -lcrypto -lltdl
GLOBUS_GRAM_LIBS = -lglobus_gram_client -lglobus_gram_protocol -lglobus_io -lglobus_xio -lgssapi_error -lglobus_gss_assist -lglobus_gssapi_gsi -lglobus_gsi_proxy_core -lglobus_gsi_credential -lglobus_gsi_callback -lglobus_oldgaa -lglobus_gsi_sysconfig -lglobus_gsi_cert_utils -lglobus_openssl -lglobus_openssl_error -lglobus_proxy_ssl -lglobus_common -lssl -lcrypto -lltdl -lm
AM_CFLAGS = $(OPT) -D$(PLATFORM) -I../include -Wall
MYCFLAGS = $(AM_CFLAGS) -I$(GLOBUS_INCLUDE) $(DEFINE)
MYLDFLAGS  = -L$(GLOBUS_LIB) $(GLOBUS_LIBS)
MYGRAMLDFLAGS  = -L$(GLOBUS_LIB) $(GLOBUS_GRAM_LIBS)
endif
liboph_soap_la_SOURCES = $(INTERFACE_TYPE)/oph_soap.c stdsoap2.c $(INTERFACE_TYPE)/gsi.c soapC.c soapClient.c
liboph_soap_la_CFLAGS= -prefer-pic -I../../include @INCLTDL@ -DOPH_ANALYTICS_LOCATION=\"${prefix}\" $(OPT) -I$(INTERFACE_TYPE) $(MYCFLAGS) -DINTERFACE_TYPE_IS_GSI
liboph_soap_la_LDFLAGS = -static $(MYLDFLAGS)
liboph_soap_la_LIBADD = -lm @LIBLTDL@

endif

distclean-local: 
	rm -f *~ #*.nsmap *.xsd *.wsdl *.xml soap*

