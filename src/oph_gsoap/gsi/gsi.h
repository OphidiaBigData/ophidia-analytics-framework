/*
 * gsi.c version 3.3
 * 
 * Globus GSI support for gSOAP
 * 
 * Copyright (C) Massimo Cafaro, Daniele Lezzi University of Salento, Italy
 * and
 * Robert van Engelen, Florida State University, USA
 * 
 */


#ifndef __GSI_H
#define __GSI_H



#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <strings.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/select.h>

#ifdef GSI_PLUGIN_THREADS
#include <pthread.h>
#endif

#include "stdsoap2.h"
#include "gssapi.h"
#include "globus_common.h"
#include "globus_gsi_credential.h"
#include "globus_gsi_proxy.h"
#include "globus_openssl.h"
#include "openssl/x509.h"


#ifdef GLITE_VOMS
#include "voms/voms_apic.h"
#endif

#define GSI_PLUGIN_ID "GSI plugin for gSOAP v3.3"	/* GSI plugin identification */

#define GSI_PLUGIN_TOKEN_EOF 1
#define GSI_PLUGIN_TOKEN_ERR_MALLOC 2
#define GSI_PLUGIN_TOKEN_ERR_BAD_SIZE 3
#define GSI_PLUGIN_TOKEN_TIMEOUT 4

#ifdef GSI_PLUGIN_THREADS
/* mutexes to protect import/export of context and credentials
 * which does not appear to be thread-safe
 * used also to protect VOMS code
 */
static pthread_mutex_t gsi_plugin_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

/*
 * ! \struct gsi_plugin_data
 * 
 */

struct gsi_plugin_data {	/* plug-in's local data */

	/* this is needed to save a gSOAP callback, to be used in gsi_connect() */
    	int (*fopen)(struct soap *soap, const char *endpoint, const char *host, int port);

	/*
	 * ! \fn int (*gsi_authorization_callback)(struct soap *soap) 
	 * \brief  Authorization callback prototype 
         * \param struct soap *soap The current gSOAP runtime environment
	 * \param char *distinguished_name The remote peer distinguished name
	 * 
	 */

	int (*gsi_authorization_callback) (struct soap *soap, char *distinguished_name);

	/*
	 * ! \fn int  (*gsi_credential_renew_callback) (struct soap *soap, char *pwd);
	 * \brief  Credential renewal  callback prototype 
         * \param struct soap *soap The current gSOAP runtime environment
         * \param int lifetime The remaining lifetime (in seconds) of our credential;
         *                     if > 0 our credentila is valid, if  <= 0 our credential has expired
	 * 
	 */
	int (*gsi_credential_renew_callback) (struct soap *soap, int current_lifetime);


	 /*
         * ! \var gss_ctx_id_t  context GSI context
         */
        gss_ctx_id_t    context;

        /*
         * ! \var gss_cred_id_t credential GSI credential
         */
        gss_cred_id_t   credential;

        /*
         * ! \var gss_cred_id_t delegated_credential a delegated credential
         * that can be received from a client
         */
        gss_cred_id_t   delegated_credential;

	/*
	 * ! \var char *identity our distinguished name
	 *
	 */
	char           *identity;

	/*
	 * ! \var char *client_identity distinguished name of client
	 * connecting to server
	 *
	 */
	char           *client_identity;

	/*
	 * ! \var char *server_identity distinguished name of server we are
	 * connecting to
	 *
	 */
	char           *server_identity;

	/*
	 * ! \var char *proxy_filename filename of proxy credential
	 */
	char           *proxy_filename;


	/*
	 * ! \var OM_uint32 req_flags GSI connection request flags
	 */
	OM_uint32       req_flags;

	/*
	 * ! \var OM_uint32 ret_flags GSI connection returned  flags
	 */
	OM_uint32       ret_flags;

       /*
	* ! \var char *target_name DN of the remote web service
        * this is needed when requesting delegation of credentials
	*/
        char *target_name;	
    

     	/*
     	* \var int want_confidentiality Flag indicating whether or not 
	* the client or server wants encrypted communication  
     	*
     	*/
	int want_confidentiality;

    	/*
     	* \var short int recv_timeout timeout value (in seconds) for the receive timeout
     	*
     	*/
     	short int recv_timeout;
     
     	/*
     	* \var short int send_timeout timeout value (in seconds) for the send timeout
     	*
     	*/
     	short int send_timeout;



     	#ifdef GLITE_VOMS
     	/*
     	* \var char **fqans The VOMS attributes; this is a NULL terminated array
     	*
     	*/
     	char **fqans;
     	#endif



};


#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

int             globus_gsi(struct soap *soap, struct soap_plugin *p, void *arg);
int		gsi_acquire_credential(struct soap *soap);
int             gsi_acquire_delegated_credential(struct soap *current_soap, struct soap *new_soap);
int		gsi_listen(struct soap *soap, const char *host, int port, int backlog);
int		gsi_accept(struct soap *soap);
int             gsi_accept_security_context(struct soap *soap);
int             gsi_set_delegation(struct soap *soap, globus_bool_t delegation, char *target_name);
int             gsi_set_replay(struct soap *soap, globus_bool_t replay);
int             gsi_set_sequence(struct soap *soap, globus_bool_t sequence);
int             gsi_set_confidentiality(struct soap *soap, globus_bool_t confidentiality);
int             gsi_set_integrity(struct soap *soap, globus_bool_t integrity);
int             gsi_set_recv_timeout(struct soap *soap, short int recv_timeout);
int             gsi_set_send_timeout(struct soap *soap, short int send_timeout);
int             gsi_set_accept_timeout(struct soap *soap, short int accept_timeout);
int             gsi_set_connect_timeout(struct soap *soap, short int connect_timeout);
int 		gsi_authorization_callback_register(struct soap *soap, int (*gsi_authorization_callback) (struct soap *soap, char *distinguished_name));
int		gsi_credential_renew_callback_register(struct soap *soap, int (*credential_renew_callback) (struct soap *soap, int current_lifetime));

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif				/* __GSI_H */
