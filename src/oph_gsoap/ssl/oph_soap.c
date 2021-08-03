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

#include "oph_soap.h"
#include "oph_common.h"
#include "oph.nsmap"

#include <signal.h>		/* defines SIGPIPE */

#define OPH_SOAP_URL_WITH_OPENSSL "https://%s:%s"
#define OPH_SOAP_URL_WITHOUT_OPENSSL "http://%s:%s"

void sigpipe_handle(int);

int oph_soap_read_config_file(oph_soap_data * data)
{
	if (!data)
		return -1;

	data->server = 0;
	data->username = 0;
	data->password = 0;
	data->host = 0;
	data->port = 0;

	char config[OPH_COMMON_BUFFER_LEN];
	snprintf(config, sizeof(config), OPH_SOAP_SERVER_CONFIGURATION, OPH_ANALYTICS_LOCATION);
	FILE *file = fopen(config, "r");
	if (!file)
		return -2;

	char buffer[OPH_COMMON_BUFFER_LEN];
	char *position;

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		position++;
		if (!(data->host = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
			fclose(file);
			return -4;
		}
		strncpy(data->host, position, strlen(position) + 1);
		data->host[strlen(position)] = '\0';
	}

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		if (data->host)
			free(data->host);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		position++;
		if (!(data->port = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
			fclose(file);
			if (data->host)
				free(data->host);
			return -4;
		}
		strncpy(data->port, position, strlen(position) + 1);
		data->port[strlen(position)] = '\0';
	}

	int n;
#ifdef WITH_OPENSSL
	n = 1 + snprintf(0, 0, OPH_SOAP_URL_WITH_OPENSSL, data->host, data->port);
#else
	n = 1 + snprintf(0, 0, OPH_SOAP_URL_WITHOUT_OPENSSL, data->host, data->port);
#endif
	if (!(data->server = (char *) malloc(n * sizeof(char)))) {
		fclose(file);
		if (data->host)
			free(data->host);
		if (data->port)
			free(data->port);
		return -4;
	}
#ifdef WITH_OPENSSL
	snprintf(data->server, n, OPH_SOAP_URL_WITH_OPENSSL, data->host, data->port);
#else
	snprintf(data->server, n, OPH_SOAP_URL_WITHOUT_OPENSSL, data->host, data->port);
#endif

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		oph_soap_free_data(data);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		position++;
		if (!(data->username = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
			fclose(file);
			oph_soap_free_data(data);
			return -4;
		}
		strncpy(data->username, position, strlen(position) + 1);
		data->username[strlen(position)] = '\0';
	}

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		oph_soap_free_data(data);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		position++;
		if (!(data->password = (char *) malloc((strlen(position) + 1) * sizeof(char)))) {
			fclose(file);
			oph_soap_free_data(data);
			return -4;
		}
		strncpy(data->password, position, strlen(position) + 1);
		data->password[strlen(position)] = '\0';
	}

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		oph_soap_free_data(data);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		data->timeout = (int) strtol(position + 1, NULL, 10);
	}

	if (!fgets(buffer, OPH_COMMON_BUFFER_LEN, file)) {
		fclose(file);
		oph_soap_free_data(data);
		return -3;
	}
	buffer[strlen(buffer) - 1] = '\0';
	position = strchr(buffer, '=');
	if (position != NULL) {
		data->recv_timeout = (int) strtol(position + 1, NULL, 10);
	}

	fclose(file);
	return 0;
}

int oph_soap_cleanup(struct soap *soap, oph_soap_data * data)
{
	if (soap) {
		soap_destroy(soap);
		soap_end(soap);
		soap_done(soap);	/* MUST call before CRYPTO_thread_cleanup */
	}
	oph_soap_free_data(data);
	return 0;
}

int oph_soap_free_data(oph_soap_data * data)
{
	if (data) {
		if (data->server) {
			free(data->server);
			data->server = 0;
		}
		if (data->username) {
			free(data->username);
			data->username = 0;
		}
		if (data->password) {
			free(data->password);
			data->password = 0;
		}
		if (data->host) {
			free(data->host);
			data->host = 0;
		}
		if (data->port) {
			free(data->port);
			data->port = 0;
		}
	}
	return 0;
}

int oph_soap_init(struct soap *soap, oph_soap_data * data)
{
	/* Need SIGPIPE handler on Unix/Linux systems to catch broken pipes: */
	signal(SIGPIPE, sigpipe_handle);

	if (!soap || !data)
		return -1;

	if (oph_soap_read_config_file(data))
		return -2;

#ifdef WITH_OPENSSL
	/* Init SSL */
	soap_ssl_init();
#endif

	soap_init(soap);
#ifdef WITH_OPENSSL
	/* Init gSOAP context */
	if (soap_ssl_client_context(soap, SOAP_TLSv1_2 | SOAP_SSL_SKIP_HOST_CHECK, NULL, NULL, NULL, NULL, NULL)) {
		soap_print_fault(soap, stderr);
		oph_soap_cleanup(soap, data);
		return -3;
	}
#endif

	return 0;
}

int oph_notify(struct soap *soap, oph_soap_data * data, char *output_string, char *output_json, xsd__int * response)
{
	soap->userid = data->username;	// Username has to set for each serve
	soap->passwd = data->password;	// Password has to set for each serve
	soap->connect_timeout = data->timeout;	/* try to connect for 1 minute */
	soap->send_timeout = soap->recv_timeout = data->recv_timeout;	/* if I/O stalls, then timeout after 3600 seconds */

	if (soap_call_oph__oph_notify(soap, data->server, "", output_string, output_json, response))
		return -1;

	return 0;
}

/******************************************************************************\
 *
 *	SIGPIPE
 *
\******************************************************************************/

void sigpipe_handle(int x)
{
	printf("Receive signal %d\n", x);
}
