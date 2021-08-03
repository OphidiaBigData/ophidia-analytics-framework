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

#ifndef __OPH_SOAP_H__
#define __OPH_SOAP_H__

#include "soapH.h"

#include "oph_framework_paths.h"

#define OPH_SOAP_SERVER_CONFIGURATION OPH_FRAMEWORK_SOAP_CONF_FILE_PATH

typedef struct {
	char *server;
	char *username;
	char *password;
	char *host;
	char *port;
	int timeout;
	int recv_timeout;
} oph_soap_data;

int oph_notify(struct soap *soap, oph_soap_data * data, char *output_string, char *output_json, xsd__int * response);

int oph_soap_init(struct soap *soap, oph_soap_data * data);
int oph_soap_cleanup(struct soap *soap, oph_soap_data * data);

int oph_soap_free_data(oph_soap_data * data);

#endif
