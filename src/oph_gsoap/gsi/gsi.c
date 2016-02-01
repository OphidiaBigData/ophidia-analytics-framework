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

#include "gsi.h"

#define min(a, b) ((a) < (b) ? (a) : (b))

#define gsi_plugin_fprintf(stream, fmt, ...) \
{ \
char buf[128] = {0}; \
struct tm tm; \
time_t now; \
char *timestamp; \
char dbg_fmt[] = "[%s] "; \
char *fmt_string; \
now = time(NULL); \
timestamp = asctime_r(gmtime_r(&now, &tm), buf); \
timestamp[strlen(timestamp)-1] = 0; \
fmt_string = (char *) malloc((strlen(dbg_fmt)+strlen(fmt)+1)); \
if(!fmt_string) \
        exit(EXIT_FAILURE); \
sprintf(fmt_string, "%s%s", dbg_fmt, fmt); \
fprintf(stream, fmt_string,  timestamp, __VA_ARGS__); \
free(fmt_string); \
}


#ifdef GSI_PLUGIN_DEBUG
#define DEBUG_STMT(level, fmt, ...) \
{ \
char buf[128] = {0}; \
struct tm tm; \
time_t now; \
char *timestamp; \
char dbg_fmt[] = "[%s][FILE %s LINE %d FUNCTION %s] "; \
char *fmt_string; \
now = time(NULL); \
timestamp = asctime_r(gmtime_r(&now, &tm), buf); \
timestamp[strlen(timestamp)-1] = 0; \
fmt_string = (char *) malloc((strlen(dbg_fmt)+strlen(fmt)+1)); \
if(!fmt_string) \
	exit(EXIT_FAILURE); \
sprintf(fmt_string, "%s%s", dbg_fmt, fmt); \
if(gsi_plugin_dbg_level >= level) \
        fprintf(stderr, fmt_string,  timestamp,  __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__); \
free(fmt_string); \
}
#else
#define DEBUG_STMT(level, fmt, ...)
#endif

/* debug framework
 * to activate define the symbol GSI_PLUGIN_DEBUG
 * and set the environmental variable GSI_PLUGIN_DEBUG_LEVEL
 *
 * GSI_PLUGIN_DEBUG_LEVEL = 0 -> debug disabled
 * GSI_PLUGIN_DEBUG_LEVEL = 1 -> entering / exiting a plugin function
 * GSI_PLUGIN_DEBUG_LEVEL = 2 -> execution summary
 * GSI_PLUGIN_DEBUG_LEVEL = 3 -> shows internal state
 *
 */


typedef struct gss_name_desc_struct {
    /* gss_buffer_desc  name_buffer ; */
    gss_OID                             name_oid;
    X509_NAME *                         x509n;
} gss_name_desc;

typedef struct globus_l_gsi_cred_handle_s
{
    /** The credential's signed certificate */
    X509 *                              cert;
    /** The private key of the credential */
    EVP_PKEY *                          key;
    /** The chain of signing certificates */
    STACK_OF(X509) *                    cert_chain;
    /** The immutable attributes of the credential handle */
    globus_gsi_cred_handle_attrs_t      attrs;
    /** The amout of time the credential is valid for */
    time_t                              goodtill;
} globus_i_gsi_cred_handle_t;


typedef struct gss_cred_id_desc_struct {
    globus_gsi_cred_handle_t            cred_handle;
    gss_name_desc *                     globusid;
    gss_cred_usage_t                    cred_usage;
    SSL_CTX *                           ssl_context;
} gss_cred_id_desc;

typedef enum {
    GSS_CON_ST_HANDSHAKE = 0,
    GSS_CON_ST_FLAGS,
    GSS_CON_ST_REQ,
    GSS_CON_ST_CERT,
    GSS_CON_ST_DONE          
} gss_con_st_t;

typedef enum
{
    GSS_DELEGATION_START,
    GSS_DELEGATION_DONE,
    GSS_DELEGATION_COMPLETE_CRED,
    GSS_DELEGATION_SIGN_CERT
} gss_delegation_state_t;


typedef struct gss_ctx_id_desc_struct{
    globus_mutex_t                      mutex;
    globus_gsi_callback_data_t          callback_data;
    gss_cred_id_desc *                  peer_cred_handle;
    gss_cred_id_desc *                  cred_handle;
    gss_cred_id_desc *                  deleg_cred_handle;
    globus_gsi_proxy_handle_t           proxy_handle;
    OM_uint32                           ret_flags;
    OM_uint32                           req_flags;
    OM_uint32                           ctx_flags;
    int                                 cred_obtained;
    SSL *                               gss_ssl;
    BIO *                               gss_rbio;
    BIO *                               gss_wbio;
    BIO *                               gss_sslbio;
    gss_con_st_t                        gss_state;
    int                                 locally_initiated;
    gss_delegation_state_t              delegation_state;
    gss_OID_set                         extension_oids;
} gss_ctx_id_desc;


static int gsi_plugin_dbg_level;

static int      gsi_copy(struct soap *soap, struct soap_plugin *q, struct soap_plugin *p);
static void     gsi_delete(struct soap *soap, struct soap_plugin *p);
static int      gsi_connect(struct soap *soap, const char *endpoint, const char *host, int port);
static int      gsi_init_security_context(struct soap *soap);
static int      gsi_send(struct soap *soap, const char *s, size_t n);
static size_t   gsi_recv(struct soap *soap, char *s, size_t n);
static void     gsi_display_status(char *msg, OM_uint32 maj_stat, OM_uint32 min_stat);
static void 	display_status(char *m, OM_uint32 code, int type);


static ssize_t readn(struct soap *soap, void *vptr, size_t n);
static ssize_t writen(struct soap *soap, const void *vptr, size_t n);

static int gsi_recv_token(struct soap *soap, void **bufp, size_t *sizep);
static int gsi_send_token(struct soap *soap, void *buf, size_t size);

 
#ifdef GLITE_VOMS
static int gsi_get_credential_data(gss_ctx_id_t gss_context, X509 **cert, STACK_OF(X509) **chain);
static int gsi_get_voms_data(struct gsi_plugin_data *data, X509 *cert, STACK_OF(X509) *chain);
#endif


/*
 * ! \fn int globus_gsi (struct soap *soap, struct soap_plugin *p, void *arg)
 * 
 * \brief This function sets plugin data
 * 
 * This function sets plugin data. it is one of the parameters of
 * soap_register_plugin()
 * 
 */

int
globus_gsi(struct soap *soap, struct soap_plugin *p, void *arg)
{
	if (!arg) DEBUG_STMT(1, "%s: param arg is null\n", GSI_PLUGIN_ID);

	char *dbg_level = NULL;

	DEBUG_STMT(1, "%s: L1 Entering globus_gsi()\n", GSI_PLUGIN_ID);

	if(!soap || !p){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting globus_gsi()\n", GSI_PLUGIN_ID);
		return SOAP_FAULT;
	}


	DEBUG_STMT(2, "%s: L2 Initializing the GSI plugin\n", GSI_PLUGIN_ID);

	dbg_level = getenv("GSI_PLUGIN_DEBUG_LEVEL");
	if(dbg_level)
		gsi_plugin_dbg_level = (int) strtol(dbg_level, (char **) NULL, 10);
	else
		gsi_plugin_dbg_level = 0;


	/* MUST be set to register */
	p->id = GSI_PLUGIN_ID;

	/* MUST be set to register */
	p->data = (void *) calloc(1, sizeof(struct gsi_plugin_data));
	if(!p->data){
		gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting globus_gsi()\n", GSI_PLUGIN_ID);
		return SOAP_EOM;
	}


	p->fcopy = gsi_copy;
	p->fdelete = gsi_delete;


	/* setup plugin data */

	/* save gSOAP callbacks */
	((struct gsi_plugin_data *) p->data)->fopen = soap->fopen;

	((struct gsi_plugin_data *) p->data)->gsi_authorization_callback = NULL;

	((struct gsi_plugin_data *) p->data)->gsi_credential_renew_callback = NULL;

	((struct gsi_plugin_data *) p->data)->context = GSS_C_NO_CONTEXT;

	((struct gsi_plugin_data *) p->data)->credential = GSS_C_NO_CREDENTIAL;

	((struct gsi_plugin_data *) p->data)->delegated_credential = GSS_C_NO_CREDENTIAL;

	((struct gsi_plugin_data *) p->data)->identity = NULL;

	((struct gsi_plugin_data *) p->data)->client_identity = NULL;

	((struct gsi_plugin_data *) p->data)->server_identity = NULL;

	((struct gsi_plugin_data *) p->data)->proxy_filename = NULL;

	((struct gsi_plugin_data *) p->data)->req_flags = GSS_C_MUTUAL_FLAG;

	((struct gsi_plugin_data *) p->data)->ret_flags = 0;

	((struct gsi_plugin_data *) p->data)->target_name = NULL;

    	((struct gsi_plugin_data *) p->data)->want_confidentiality = 0;

    	((struct gsi_plugin_data *) p->data)->recv_timeout = 120;
    
    	((struct gsi_plugin_data *) p->data)->send_timeout = 120;

	#ifdef GLITE_VOMS
        ((struct gsi_plugin_data *) p->data)->fqans = NULL;
        #endif

	/* setup plugin callbacks */
	soap->fopen = gsi_connect;
	soap->fsend = gsi_send;
	soap->frecv = gsi_recv;

	
	DEBUG_STMT(1, "%s: L1 Exiting globus_gsi()\n", GSI_PLUGIN_ID);
	return SOAP_OK;
}


/*
 * ! \fn static int gsi_copy (struct soap *soap, struct soap_plugin *q,
 * struct soap_plugin *p)
 * 
 * \brief This function copies plugin data; it is used internally.
 * 
 * This function copies plugin data.
 * 
 */

static int
gsi_copy(struct soap *soap, struct soap_plugin *q, struct soap_plugin *p)
{


    OM_uint32                   major_status;
    OM_uint32                   minor_status;
    gss_buffer_desc		token;
    gss_buffer_desc		cred_buffer;
    int 			return_value = SOAP_EOM; /* assume failure due to end of memory ;-) */

    struct gsi_plugin_data *gsi_data = NULL;
    struct gsi_plugin_data *gsi_src_data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_copy()\n", GSI_PLUGIN_ID);

	if(!soap || !q || !p){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_copy()\n", GSI_PLUGIN_ID);
		return SOAP_FAULT;
	}

	DEBUG_STMT(2, "%s: L2 Copying plugin data\n", GSI_PLUGIN_ID);

    	gsi_src_data = (struct gsi_plugin_data *) p->data;
	gsi_data = (struct gsi_plugin_data *) calloc(1, sizeof(struct gsi_plugin_data));
	if (!gsi_data){
		gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_copy()\n", GSI_PLUGIN_ID);
		return return_value;
	}


	/* now copy plugin data */

	gsi_data->fopen = gsi_src_data->fopen;

	gsi_data->gsi_authorization_callback = gsi_src_data->gsi_authorization_callback;
	gsi_data->gsi_credential_renew_callback = gsi_src_data->gsi_credential_renew_callback;


	#ifdef GSI_PLUGIN_THREADS
	pthread_mutex_lock(&gsi_plugin_lock);
	#endif
	if(gsi_src_data->context != GSS_C_NO_CONTEXT){

		major_status = gss_export_sec_context(&minor_status, &gsi_src_data->context, &token);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_export_sec_context() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_export_sec_context()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_import_sec_context(&minor_status, &token, &gsi_data->context);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_import_sec_context() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_import_sec_context()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_release_buffer(&minor_status, &token);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_buffer()", major_status, minor_status);
			goto exit;
		}
	}
	else
		gsi_data->context = GSS_C_NO_CONTEXT;

	if(gsi_src_data->credential != GSS_C_NO_CREDENTIAL){

		major_status = gss_export_cred(&minor_status, gsi_src_data->credential, NULL, 0, &cred_buffer);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_export_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_export_cred()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_import_cred(&minor_status, &gsi_data->credential, NULL, 0, &cred_buffer, 0, 0);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_import_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_import_cred()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_release_buffer(&minor_status, &cred_buffer);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_buffer()", major_status, minor_status);
			goto exit;
		}
	}
	else
		gsi_data->credential = GSS_C_NO_CREDENTIAL;

	if(gsi_src_data->delegated_credential != GSS_C_NO_CREDENTIAL){

		major_status = gss_export_cred(&minor_status, gsi_src_data->delegated_credential, NULL, 0, &cred_buffer);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_export_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_export_cred()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_import_cred(&minor_status, &gsi_data->delegated_credential, NULL, 0, &cred_buffer, 0, 0);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_import_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_import_cred()", major_status, minor_status);
			goto exit;
		}

		major_status = gss_release_buffer(&minor_status, &cred_buffer);
		if(major_status != GSS_S_COMPLETE){
			#ifdef GSI_PLUGIN_THREADS
			pthread_mutex_unlock(&gsi_plugin_lock);
			#endif
			gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_buffer()", major_status, minor_status);
			goto exit;
		}

	}
	else
		gsi_data->delegated_credential = GSS_C_NO_CREDENTIAL;

	#ifdef GSI_PLUGIN_THREADS
	pthread_mutex_unlock(&gsi_plugin_lock);
	#endif
	
	if (gsi_src_data->identity){
		gsi_data->identity = strdup(gsi_src_data->identity);
		if(!gsi_data->identity){
			gsi_plugin_fprintf(stderr, "%s: strdup() failed\n", GSI_PLUGIN_ID);
			goto exit;
		}
	}
	else
		gsi_data->identity = NULL;
	
	if (gsi_src_data->client_identity){
		gsi_data->client_identity = strdup(gsi_src_data->client_identity);
		if(!gsi_data->client_identity){
			gsi_plugin_fprintf(stderr, "%s: strdup() failed\n", GSI_PLUGIN_ID);
			goto exit;
		}
	}
	else
		gsi_data->client_identity = NULL;

	if (gsi_src_data->server_identity){
		gsi_data->server_identity = strdup(gsi_src_data->server_identity);
		if(!gsi_data->server_identity){
			gsi_plugin_fprintf(stderr, "%s: strdup() failed\n", GSI_PLUGIN_ID);
			goto exit;
		}
	}
	else
		gsi_data->server_identity = NULL;

	if (gsi_src_data->proxy_filename){
		gsi_data->proxy_filename = strdup(gsi_src_data->proxy_filename);
		if(!gsi_data->proxy_filename){
			gsi_plugin_fprintf(stderr, "%s: strdup() failed\n", GSI_PLUGIN_ID);
			goto exit;
		}
	}
	else
		gsi_data->proxy_filename = NULL;

	gsi_data->req_flags = gsi_src_data->req_flags;
	gsi_data->ret_flags = gsi_src_data->ret_flags;

    	if(gsi_src_data->target_name){
       		gsi_data->target_name = strdup(gsi_src_data->target_name);
		if(!gsi_data->target_name){
			gsi_plugin_fprintf(stderr, "%s: strdup() failed\n", GSI_PLUGIN_ID);
			goto exit;
		}
	}
    	else
        	gsi_data->target_name = NULL;

	gsi_data->want_confidentiality = gsi_src_data->want_confidentiality;
    	gsi_data->recv_timeout = gsi_src_data->recv_timeout;
	gsi_data->send_timeout = gsi_src_data->send_timeout;
	

#ifdef GLITE_VOMS

        if(gsi_src_data->fqans){

                int count = 0;
                int i;

                while(gsi_src_data->fqans[count])
                        count++;

                gsi_data->fqans = (char **) calloc(count + 1, sizeof(char *));
                if(!gsi_data->fqans)
                        goto exit;

                for(i = 0; i < count; i++){
                        if(gsi_src_data->fqans[i]){
                                gsi_data->fqans[i] = strdup(gsi_src_data->fqans[i]);
                                if(!gsi_data->fqans[i])
                                        goto exit;
                        }
                }


        }
        else
                gsi_data->fqans = NULL;

#endif


	q->data = gsi_data;
	return_value = SOAP_OK;
	DEBUG_STMT(1, "%s: L1 Exiting gsi_copy()\n", GSI_PLUGIN_ID);


exit:
	if(return_value == SOAP_EOM){

		if(gsi_data->context != GSS_C_NO_CONTEXT){
    			major_status = gss_delete_sec_context(&minor_status, &gsi_data->context, GSS_C_NO_BUFFER);
			if(major_status != GSS_S_COMPLETE){
				gsi_plugin_fprintf(stderr, "%s: gss_delete_sec_context() failed\n", GSI_PLUGIN_ID);
				gsi_display_status("gss_delete_sec_context()", major_status, minor_status);
			}
			gsi_data->context = GSS_C_NO_CONTEXT;
		}

    		if(gsi_data->credential != GSS_C_NO_CREDENTIAL){
    			major_status = gss_release_cred(&minor_status, &gsi_data->credential);
			if(major_status != GSS_S_COMPLETE){
				gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
				gsi_display_status("gss_release_cred()", major_status, minor_status);
			}
			gsi_data->credential = GSS_C_NO_CREDENTIAL;
		}

    		if(gsi_data->delegated_credential != GSS_C_NO_CREDENTIAL){
    			major_status = gss_release_cred(&minor_status, &gsi_data->delegated_credential);
			if(major_status != GSS_S_COMPLETE){
				gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
				gsi_display_status("gss_release_cred()", major_status, minor_status);
			}
			gsi_data->delegated_credential = GSS_C_NO_CREDENTIAL;
		}

		if(gsi_data->identity){
			free(gsi_data->identity);
			gsi_data->identity = NULL;
		}

		if(gsi_data->client_identity){
			free(gsi_data->client_identity);
			gsi_data->client_identity = NULL;
		}

		if(gsi_data->server_identity){
			free(gsi_data->server_identity);
			gsi_data->server_identity = NULL;
		}

		if(gsi_data->proxy_filename){
			free(gsi_data->proxy_filename);
			gsi_data->proxy_filename = NULL;
		}

		if(gsi_data->target_name){
			free(gsi_data->target_name);
			gsi_data->target_name = NULL;
		}

		#ifdef GLITE_VOMS
                if(gsi_data->fqans){
                        int i;
                        for(i = 0; gsi_data->fqans[i]; i++)
                                if(gsi_data->fqans[i])
                                        free(gsi_data->fqans[i]);

			free(gsi_data->fqans);
                        gsi_data->fqans = NULL;
                }
                #endif


		if(gsi_data){
			free(gsi_data);
			gsi_data = NULL;
		}

		DEBUG_STMT(1, "%s: L1 Exiting gsi_copy()\n", GSI_PLUGIN_ID);
		return return_value;
	}


	return return_value;



}


/*
 * ! \fn static void gsi_delete (struct soap *soap, struct soap_plugin *p)
 * 
 * \brief This function deletes plugin data; it is used internally.
 * 
 * This function deletes plugin data.
 * 
 */

static void
gsi_delete(struct soap *soap, struct soap_plugin *p)
{
	
    	OM_uint32                   major_status;
    	OM_uint32                   minor_status;
    
	struct gsi_plugin_data *data = (struct gsi_plugin_data *) p->data;



	DEBUG_STMT(1, "%s: L1 Entering gsi_delete()\n", GSI_PLUGIN_ID);

	if(!soap || !p){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_delete()\n", GSI_PLUGIN_ID);
		return;
	}


	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_delete()\n", GSI_PLUGIN_ID);
		return;
	}

	/* delete plugin data */
	DEBUG_STMT(2, "%s: L2 Deleting plugin data\n", GSI_PLUGIN_ID);

	if (data->identity){
		free(data->identity);
		data->identity = NULL;
	}

	if (data->client_identity){
		free(data->client_identity);
		data->client_identity = NULL;
	}

	if (data->server_identity){
		free(data->server_identity);
		data->server_identity = NULL;
	}

	if (data->proxy_filename){
		free(data->proxy_filename);
		data->proxy_filename = NULL;
	}

	if(data->context != GSS_C_NO_CONTEXT){
    		major_status = gss_delete_sec_context(&minor_status, &data->context, GSS_C_NO_BUFFER);
		if(major_status != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_delete_sec_context() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_delete_sec_context()", major_status, minor_status);
		}
		data->context = GSS_C_NO_CONTEXT;
	}

    	if(data->credential != GSS_C_NO_CREDENTIAL){
    		major_status = gss_release_cred(&minor_status, &data->credential);
		if(major_status != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_cred()", major_status, minor_status);
		}
		data->credential = GSS_C_NO_CREDENTIAL;
	}

    	if(data->delegated_credential != GSS_C_NO_CREDENTIAL){
    		major_status = gss_release_cred(&minor_status, &data->delegated_credential);
		if(major_status != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_cred()", major_status, minor_status);
		}
		data->delegated_credential = GSS_C_NO_CREDENTIAL;
	}

	if(data->target_name){
		free(data->target_name);
		data->target_name = NULL;
	}

	#ifdef GLITE_VOMS
        if(data->fqans){
        	int i;

                for(i = 0; data->fqans[i]; i++)
                	if(data->fqans[i])
                        	free(data->fqans[i]);

			free(data->fqans);
                        data->fqans = NULL;
        }
        #endif


	/* do not destroy the mutexes! */

	DEBUG_STMT(1, "%s: L1 Exiting gsi_delete()\n", GSI_PLUGIN_ID);

	free(data);
	
}

/* 
 * ! \fn int gsi_listen(struct soap *soap, char *host, int port, int backlog)
 * 
 * \brief This function listens for incoming connections
 * 
 * This function listens for incoming connections
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param char *host The hostname: you can safely pass NULL here
 * \param int port The port where we are listening on
 * \param int backlog The number of backlogged connections
 * 
 * \return the listening file descriptor on success, -1 on error
 * 
 */


int gsi_listen(struct soap *soap, const char *host, int port, int backlog)
{

	if(!soap){ /* host can safely be NULL */
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_listen()\n", GSI_PLUGIN_ID);
		return -1;
	}

	soap->bind_flags = SO_REUSEADDR;
	return soap_bind(soap, host, port, backlog);

}

/*
 * ! \fn int gsi_accept (struct soap *soap)
 * 
 * \brief This function accepts a new connection
 * 
 * This function accepts a new connection and retrieves the peer's IP address
 * 
 * \param struct soap *soap The current gSOAP runtime environment  
 *
 * \return the connected file descriptor on success, -1 on error
 * 
 */

int gsi_accept(struct soap *soap)
{

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_accept()\n", GSI_PLUGIN_ID);
		return -1;
	}

	return soap_accept(soap);

}


/*
 * ! \fn static int gsi_connect (struct soap *soap, const char *endpoint,
 * const char *host, unsigned short int port)
 * 
 * \brief Client side, set up the connection to the remote host; it is used
 * internally.
 * 
 * This function connects to a remote GSI enabled web service
 *
 * \return the connected file descriptor on success, -1 on error
 * 
 */

static int
gsi_connect(struct soap *soap, const char *endpoint, const char *host, int port)
{

	int             rc;
	int s;
	OM_uint32	major_status, minor_status, lifetime;
    
	struct gsi_plugin_data *data =  NULL;

 	DEBUG_STMT(1, "%s: L1 Entering gsi_connect()\n", GSI_PLUGIN_ID);

	if(!soap || !endpoint || !host){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
		return -1;
	}



	DEBUG_STMT(2, "%s: L2 Connecting to remote GSI enabled web service\n", GSI_PLUGIN_ID);

	if(data->gsi_credential_renew_callback){
        	major_status = gss_inquire_cred(& minor_status, data->credential, NULL, &lifetime, NULL, NULL);
		if(major_status == GSS_S_COMPLETE){
			rc = data->gsi_credential_renew_callback(soap, lifetime); /* try to renew the credential */
			if(rc == -1){
				return SOAP_INVALID_SOCKET;
			}
		}
	}


	data->context = GSS_C_NO_CONTEXT;

	/*
	 * connect to remote GSI enabled web service
	 */
	 
	s = data->fopen(soap, endpoint, host, port); 

	if (s != SOAP_INVALID_SOCKET) { /* we have a valid connected socket */


		DEBUG_STMT(2, "%s: L2 Establishing security context with remote GSI enabled web service\n", GSI_PLUGIN_ID);

		rc = gsi_init_security_context(soap);
		if (rc == 0) {

			DEBUG_STMT(2, "%s: L2 Established security context with: %s\n", GSI_PLUGIN_ID, data->server_identity);

			if(data->gsi_authorization_callback){
			
				DEBUG_STMT(2, "%s: L2 Calling authorization callback\n", GSI_PLUGIN_ID);
				
				rc = data->gsi_authorization_callback(soap, data->server_identity);
				
				if (rc != 0) { /* authorization failed */
					gsi_plugin_fprintf(stderr, "%s: Service %s is not authorized\n", GSI_PLUGIN_ID, data->server_identity);
					soap->error = SOAP_TCP_ERROR;
					soap_receiver_fault(soap, "Authorization error\n", NULL);
					soap_send_fault(soap);
 					DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
					return SOAP_INVALID_SOCKET;
				}
				else{ /* authorization was succesful */
				
					DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
					return s;
				}
				
			}
			else{ /* no authorization callback has been setup and called */
			
				DEBUG_STMT(2, "%s: WARNING: Remote GSI enabled Web Service has been authenticated but not authorized, since an authorization callback has not been provided\n", GSI_PLUGIN_ID);	
				DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
				return s;
			}

		} else { /* gsi_init_security_context() failed, there was an error establishing a security context */

			gsi_plugin_fprintf(stderr, "%s: There was an error while trying to establish a security context with the remote GSI enabled web service\n", GSI_PLUGIN_ID);
			soap->error = SOAP_TCP_ERROR;
 			DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
			return SOAP_INVALID_SOCKET;
		}
	}
	else { 		/* data->connected_fd == SOAP_INVALID_SOCKET */
			gsi_plugin_fprintf(stderr, "%s: Failed to connect to remote GSI enabled web service\n", GSI_PLUGIN_ID);
			soap->error = SOAP_TCP_ERROR;
 			DEBUG_STMT(1, "%s: L1 Exiting gsi_connect()\n", GSI_PLUGIN_ID);
			return SOAP_INVALID_SOCKET;
	}
	
		
}


/*! \fn static int gsi_send (struct soap *soap, const char *s, size_t n)
 *
 * \brief This function sends data using GSI; it is used internally.
 
 * This function sends data using GSI.
 *
 * \return SOAP_OK on success, SOAP_EOF on error
 */

static int
gsi_send (struct soap *soap, const char *s, size_t n)
{
    gss_buffer_desc             input_buffer;
    gss_buffer_desc             wrapped_buffer;
    OM_uint32                   major_status;
    OM_uint32                   minor_status;
    int				state;
          
  struct gsi_plugin_data *data = NULL;
  
  DEBUG_STMT(1, "%s: L1 Entering gsi_send()\n", GSI_PLUGIN_ID);

	if(!soap || !s){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
		return SOAP_EOF;
	}

  	data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
		return SOAP_EOF;
	}

  DEBUG_STMT(2, "%s: L2 Sending data\n", GSI_PLUGIN_ID);
  DEBUG_STMT(3, "%s: L3 Data to be sent: %s\n", GSI_PLUGIN_ID, s);

  input_buffer.value = (char *) s;
  input_buffer.length = n;
  
 
  /* Note that there is a maximum buffer size that can be wrapped. You can
   * obtain this limit by calling gss_wrap_size_limit.
   */
   
    major_status = gss_wrap(&minor_status,    /* (out) minor_status */
                            data->context,	      /* (in) context */
                            data->want_confidentiality,		      /* (in) confidentiality
					              request flag:
						          0  = integrity only,
						         !0 = conf + integrity */
                            GSS_C_QOP_DEFAULT,	     /* (in) quality of protection
					              mechanism specific */
                            &input_buffer,    /* (in) message */
                            &state,	      /* (out) confidentiality used */
                            &wrapped_buffer); /* (out) wrapped message */

    if(major_status != GSS_S_COMPLETE)
    {
  	gsi_plugin_fprintf(stderr, "%s:  An error occurred wrapping data\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_wrap()", major_status, minor_status);

  	DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
	return SOAP_EOF;

    }
    
    if(state){	
    	DEBUG_STMT(2, "%s: L2 confidentiality and integrity are in effect\n", GSI_PLUGIN_ID);
    }
    else{
    	DEBUG_STMT(2, "%s: L2 integrity is in effect\n", GSI_PLUGIN_ID);
    }
   
    if(gsi_send_token(soap, wrapped_buffer.value, wrapped_buffer.length) == 0)
    {

        major_status = gss_release_buffer(&minor_status, &wrapped_buffer);
	if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status, minor_status);
  	     	DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
	     	return SOAP_EOF; 
	}

  	DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
    	return SOAP_OK;
    }
    else{
    	     gsi_plugin_fprintf(stderr, "%s: An error occurred sending a GSS token\n", GSI_PLUGIN_ID);

             major_status = gss_release_buffer(&minor_status, &wrapped_buffer);
	     if(major_status != GSS_S_COMPLETE){
	     	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status, minor_status);
  	     	DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
		return SOAP_EOF;
	     }

  	     DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
	     return SOAP_EOF; 
        }

  	DEBUG_STMT(2, "%s: L2 Data has been sent succesfully\n", GSI_PLUGIN_ID);
  	DEBUG_STMT(1, "%s: L1 Exiting gsi_send()\n", GSI_PLUGIN_ID);
        
     	return SOAP_OK;

}


/*! \fn static size_t gsi_recv (struct soap *soap, char *s, size_t n)
 *
 * \brief This function receives data using GSI; it is used internally.
 
 * This function receives data using GSI.
 *
 * \return the number of bytes received on success, 0 on error
 *
 */

static size_t
gsi_recv (struct soap *soap, char *s, size_t n)
{

    gss_buffer_desc        wrapped_buffer;
    gss_buffer_desc        output_buffer;
    OM_uint32              major_status;
    OM_uint32              minor_status;
    int rc, state;
    size_t size; /* return value, number of bytes received */

    static char* buffer = NULL;
    static size_t buffer_size = 0;
    size_t  written = 0;
    
    struct gsi_plugin_data *data = NULL;

    DEBUG_STMT(1, "%s: L1 Entering gsi_recv()\n", GSI_PLUGIN_ID);

	if(!soap || !s){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
		return (size_t) 0;
	}

    	data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
		return (size_t) 0;
	}

    DEBUG_STMT(2, "%s: L2 Receiving Data\n", GSI_PLUGIN_ID);

    if(buffer){ /* we have buffered data */
	
		written = min(buffer_size, n); /* determine the correct amount of data */
		memcpy(s, buffer, written); /* and copy it into the caller's buffer */

		if(written == buffer_size){ /* we have consumed the whole buffer */
			free(buffer); /* so free it and reinitialize */
			buffer_size = 0;
			buffer = NULL;
        }
	else
		if(written < buffer_size){ /* the buffer has been consumed partially */
			size_t remaining_size;
			char* new_buffer;

			remaining_size = buffer_size - written; /* determine how much data is still available in the buffer */
			new_buffer = (char*) malloc(remaining_size); /* allocate a new temp buffer properly */
			if(!new_buffer){
    				gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
    				DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
				return (size_t) 0;
			}
			memcpy(new_buffer, buffer + written, remaining_size); /* and copy remaining data into it */
			free(buffer); /* now free our buffer, and reinitialize it using the new buffer */
			buffer_size = remaining_size;
			buffer = new_buffer;
	}

    }

    if(written == n){ /* the caller asked for n bytes, so we can now safely return */
    	DEBUG_STMT(2, "%s: L2 Received succesfully data\n", GSI_PLUGIN_ID);
    	DEBUG_STMT(3, "%s: L3 Data received: %s\n", GSI_PLUGIN_ID, s);
    	DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
	return written; 
    }

    n -= written; /* the caller needs this amount of data */
    size = written;

    rc = gsi_recv_token(soap, &wrapped_buffer.value, (size_t *) &wrapped_buffer.length);
    
    if(rc != 0){
    	gsi_plugin_fprintf(stderr, "%s: An error occurred receiving a GSS token\n", GSI_PLUGIN_ID);
    	DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
	return (size_t) 0;
    }

    major_status = gss_unwrap(&minor_status,	/* (out) minor status */
                              data->context,		/* (in) context */
                              &wrapped_buffer,	/* (in) wrapped buffer */
                              &output_buffer,	/* (out) unwrapped buffer */
                              &state,		/* (out) confidentiality
						         state
							 0  = integrity only
							 !0 = conf + int */
                              (gss_qop_t *)NULL);		/* (out) quality of
						         protection
							 mech specific */

    if(major_status != GSS_S_COMPLETE)
    {
    	gsi_plugin_fprintf(stderr, "%s: An error occurred unwrapping data\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_unwrap()", major_status, minor_status);

        major_status = gss_release_buffer(&minor_status, &wrapped_buffer);
	if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status, minor_status);
	}

    	DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
	return (size_t) 0;
    }
    
    if(state){
    	DEBUG_STMT(2, "%s: L2 confidentiality and integrity are in effect\n", GSI_PLUGIN_ID);
    }
    else{
    	DEBUG_STMT(2, "%s: L2 integrity is in effect\n", GSI_PLUGIN_ID);
    }	
    
    if(output_buffer.length <= n){ /* we received from the network no more than n bytes */
    	memcpy(s, output_buffer.value, output_buffer.length); /* copy them into the caller's buffer */
	size += output_buffer.length; /* and increment size as needed */
    }
    else
	if(output_buffer.length > n){ /* we received from the network more data than requested */
		size_t unconsumed_data;

		memcpy(s, output_buffer.value, n); /* copy the requested number of bytes into the caller's buffer */
		size += n; /* and increment size as needed  */
		unconsumed_data = output_buffer.length - n; /* now determine how many bytes are still available */
		buffer = (char*) malloc(unconsumed_data); /* and allocate our buffer properly */
		if(!buffer){
 			gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
    			DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);
			return (size_t) 0;
		}
		buffer_size = unconsumed_data; /* set the correct buffer_size */
		memcpy(buffer, (char*) output_buffer.value + n, unconsumed_data ); /* and copy remaining data into our buffer */
		
		/* next time, we fill the caller's buffer using the data we have just buffered */
		/* so that none of the bytes actually received will be discarded */
  
        }
	
   major_status = gss_release_buffer(&minor_status, &output_buffer);
   if(major_status != GSS_S_COMPLETE){
   	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_release_buffer()", major_status, minor_status);
    }

    major_status = gss_release_buffer(&minor_status, &wrapped_buffer);
    if(major_status != GSS_S_COMPLETE){
    	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_release_buffer()", major_status, minor_status);
    }

    DEBUG_STMT(2, "%s: L2 Received succesfully data\n", GSI_PLUGIN_ID);
    DEBUG_STMT(3, "%s: L3 Data received: %s\n", GSI_PLUGIN_ID, s);
    DEBUG_STMT(1, "%s: L1 Exiting gsi_recv()\n", GSI_PLUGIN_ID);

    return (size_t) size;

}



/*! \fn static int gsi_init_security_context(struct soap *soap)
 *
 * \brief This function establishes a GSI context client-side; it is used internally.
 * 
 * This function establishes a GSI context client-side.
 *
 * \return 0 on success, -1 on error
 *
 */

static int gsi_init_security_context(struct soap *soap)
{
    
    OM_uint32			major_status = 0;
    OM_uint32			major_status2 = 0;
    OM_uint32			major_status3 = 0;
    OM_uint32			major_status4 = 0;
    OM_uint32			minor_status = 0;
    OM_uint32			minor_status2 = 0;
    OM_uint32			minor_status3 = 0;
    OM_uint32			minor_status4 = 0;
    OM_uint32			time_ret;
    
    int	       		    	token_status = 0;
    gss_name_t			target_name = GSS_C_NO_NAME;
    gss_buffer_desc		target_name_buffer_desc  = GSS_C_EMPTY_BUFFER;
    gss_buffer_t		target_name_buffer = &target_name_buffer_desc;
    globus_bool_t		context_established = GLOBUS_FALSE;
    gss_OID *			actual_mech_type = NULL;

    gss_buffer_desc		input_token_desc  = GSS_C_EMPTY_BUFFER;
    gss_buffer_t    		input_token       = &input_token_desc;
    gss_buffer_desc 		output_token_desc = GSS_C_EMPTY_BUFFER;
    gss_buffer_t    		output_token      = &output_token_desc;

    
    
    struct gsi_plugin_data *data = NULL;

    DEBUG_STMT(1, "%s: L1 Entering gsi_init_security_context()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_init_security_context()\n", GSI_PLUGIN_ID);
		return -1;
	}

        data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_init_security_context()\n", GSI_PLUGIN_ID);
		return -1;
	}

    /* import the target name of the remote web service we want to connect to 
     * this is mandatory using the latest GSI package when requesting 
     * delegation of credentials
     */

    if(data->target_name){
    	DEBUG_STMT(2, "%s: L2 Importing remote web service target name\n", GSI_PLUGIN_ID);
       	target_name_buffer->value = strdup(data->target_name);
       	target_name_buffer->length = strlen(data->target_name);

    	major_status = gss_import_name(&minor_status, target_name_buffer, GSS_C_NO_OID, &target_name);
	if(major_status != GSS_S_COMPLETE){
		gsi_display_status("gss_import_name()", major_status, minor_status);
	}

    	major_status = gss_release_buffer(&minor_status, target_name_buffer);
    	if(major_status != GSS_S_COMPLETE){
    		gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status, minor_status);
	}
    }
    
    /* establish the security context with the server */

    while(!context_established)
    {
  	major_status = gss_init_sec_context(&minor_status,   /* (out) minor status */
				 data->credential,      /* (in) cred handle */
				 &data->context,         /* (in) sec context */
				 target_name,     /* (in) name of target */
				 GSS_C_NO_OID,    /* (in) mech type */
				 data->req_flags,       /* (in) request flags
						          bit-mask of:
							  GSS_C_DELEG_FLAG
							  GSS_C_REPLAY_FLAG
							  GSS_C_SEQUENCE_FLAG
							  GSS_C_CONF_FLAG
							  GSS_C_INTEG_FLAG
							  GSS_C_ANON_FLAG */
				 0,               /* (in) time ctx is valid
						          0 = default time */
				 GSS_C_NO_CHANNEL_BINDINGS, /* chan binding */
				 input_token,     /* (in) input token */
				 actual_mech_type,/* (out) actual mech */
				 output_token,    /* (out) output token */
				 &data->ret_flags,      /* (out) return flags
						           bit-mask of:
							  GSS_C_DELEG_FLAG
							  GSS_C_MUTUAL_FLAG
							  GSS_C_REPLAY_FLAG
							  GSS_C_SEQUENCE_FLAG
							  GSS_C_CONF_FLAG
							  GSS_C_INTEG_FLAG
							  GSS_C_ANON_FLAG
							  GSS_C_PROT_READY_FLAG
							  GSS_C_TRANS_FLAG */
				 &time_ret);      /* (out) time ctx is valid */


	/* free any token we've just processed */
	if(input_token->length > 0)
	{
	    free(input_token->value);
	    input_token->length = 0;
	}
	/* and send any new token to the server */
	if(output_token->length != 0)
	{
	    DEBUG_STMT(2, "%s: L2 Sending a GSS token\n", GSI_PLUGIN_ID);
	    token_status = gsi_send_token(soap, output_token->value, output_token->length);

	    if(token_status != 0)
	    {
		major_status = GSS_S_DEFECTIVE_TOKEN|GSS_S_CALL_INACCESSIBLE_WRITE;
	    }

	    /* now release the buffer */
	    major_status2 = gss_release_buffer(&minor_status2, output_token);
    	    if(major_status2 != GSS_S_COMPLETE){
    	    	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status2, minor_status2);
	    }

	}

	if (GSS_ERROR(major_status))
	{
	    gsi_plugin_fprintf(stderr, "%s: gss_init_sec_context() failed\n", GSI_PLUGIN_ID);
	    if (data->context != GSS_C_NO_CONTEXT)
	    {
	    	DEBUG_STMT(2, "%s: L2 Deleting security context\n", GSI_PLUGIN_ID);
		major_status3 = gss_delete_sec_context(&minor_status3, &data->context, GSS_C_NO_BUFFER);
		if(major_status3 != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_delete_sec_context() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_delete_sec_context()", major_status3, minor_status3);
		}
		break;
	    }
	}

	if(major_status & GSS_S_CONTINUE_NEEDED)
	{
	    DEBUG_STMT(2, "%s: L2 Receiving a GSS token\n", GSI_PLUGIN_ID);
	    token_status = gsi_recv_token(soap, &input_token->value, &input_token->length);

	    if(token_status != 0)
	    {
		major_status = GSS_S_DEFECTIVE_TOKEN | GSS_S_CALL_INACCESSIBLE_READ;
		break;
	    }
	}
	else
	{
	    DEBUG_STMT(2, "%s: L2 Security context has been established (client-side)\n", GSI_PLUGIN_ID);
	    context_established = 1;
	}
   } /* end of while() loop */

    if(input_token->length > 0)
    {
	    free(input_token->value);
            input_token->value = NULL;
	    input_token->length = 0;
    }

    if(data->target_name)
    {
	/* release target_name */
	major_status4 = gss_release_name(&minor_status4, &target_name);
	if(major_status4 != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_name() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_name()", major_status4, minor_status4);
	}
    }

    if(major_status != GSS_S_COMPLETE)
    {
	gsi_plugin_fprintf(stderr, "%s: Failed to establish security context (client-side)\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_init_sec_context():", major_status, minor_status);
	DEBUG_STMT(1, "%s: L1 Exiting gsi_init_security_context()\n", GSI_PLUGIN_ID);
	return -1;
    }
    else
    {

      /* context establishment has been successful, now retrieve from context the actual target_name */ 
      major_status = gss_inquire_context(&minor_status, data->context, NULL, &target_name, 0, actual_mech_type, NULL, NULL, NULL);
      if(major_status != GSS_S_COMPLETE){
	gsi_plugin_fprintf(stderr, "%s: gss_inquire_context() failed\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_inquire_context():", major_status, minor_status);
      }
    
     if(target_name)
     {
	  major_status = gss_display_name(&minor_status, target_name, target_name_buffer, NULL);
          if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_display_name() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_display_name():", major_status, minor_status);
   	  }
 
	  data->server_identity =  (char *) malloc((target_name_buffer->length+1)*sizeof(char));
	  if(data->server_identity){
		memcpy(data->server_identity, target_name_buffer->value, target_name_buffer->length);
		data->server_identity[target_name_buffer->length] = '\0';
		DEBUG_STMT(2, "%s: L2 Established security context with: %s\n", GSI_PLUGIN_ID, data->server_identity);
		if(data->ret_flags & GSS_C_MUTUAL_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: MUTUAL AUTHENTICATION IS ALLOWED\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_DELEG_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: CREDENTIALS CAN BE DELEGATED\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_REPLAY_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: PROTECTION AGAINST REPLAY ATTACKS IS IN EFFECT\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_SEQUENCE_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: DETECTION OF OUT OF SEQUENCE PACKETS IS IN EFFECT\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_CONF_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: DATA CONFIDENTIALITY IS ALLOWED\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_INTEG_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: DATA INTEGRITY IS ALLOWED\n", GSI_PLUGIN_ID);
	  }

         major_status = gss_release_name(&minor_status,&target_name);
         if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_name() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_name():", major_status, minor_status);
	}

         major_status = gss_release_buffer(&minor_status, target_name_buffer);
    	 if(major_status != GSS_S_COMPLETE){
    	 	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer():", major_status, minor_status);
	 }

     }
    }
   
   	DEBUG_STMT(1, "%s: L1 Exiting gsi_init_security_context()\n", GSI_PLUGIN_ID);
	return 0;
  
}


/*! \fn int gsi_accept_security_context(struct soap* soap)
 *
 * \brief This function establishes a GSI context server-side;
 * 
 * This function establishes a GSI context server-side.
 *
 * \param struct soap *soap The current gSOAP runtime environment
 *
 * \return 0 on success, -1 on error
 *
 */

int gsi_accept_security_context(struct soap* soap)
{
    OM_uint32			major_status;
    OM_uint32			major_status2;
    OM_uint32			major_status3;
    OM_uint32			minor_status;
    OM_uint32			minor_status2;
    OM_uint32			minor_status3;
    int			        token_status = 0;
    gss_name_t			client_name = GSS_C_NO_NAME;

    gss_buffer_desc		input_token_desc  = GSS_C_EMPTY_BUFFER;
    gss_buffer_t        	input_token       = &input_token_desc;
    gss_buffer_desc 		output_token_desc = GSS_C_EMPTY_BUFFER;
    gss_buffer_t    		output_token      = &output_token_desc;
    gss_buffer_desc		name_buffer_desc  = GSS_C_EMPTY_BUFFER;
    gss_buffer_t		name_buffer 	  = &name_buffer_desc;
    gss_OID		    	mech_type = GSS_C_NO_OID;
    OM_uint32			time_ret;

    #ifdef GLITE_VOMS
    int rc;
    X509 *cert = NULL;
    STACK_OF(X509) *chain = NULL;
    #endif

    struct gsi_plugin_data *data = NULL;

    DEBUG_STMT(1, "%s: L1 Entering gsi_accept_security_context()\n", GSI_PLUGIN_ID);

    if(!soap){
	gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
	DEBUG_STMT(1, "%s: L1 Exiting gsi_accept_security_context()\n", GSI_PLUGIN_ID);
	return -1;
    }

    data = (struct gsi_plugin_data *) soap_lookup_plugin (soap, GSI_PLUGIN_ID);
    if(!data){
	gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
	DEBUG_STMT(1, "%s: L1 Exiting gsi_accept_security_context()\n", GSI_PLUGIN_ID);
	return -1;
    }

    do
    {
	DEBUG_STMT(2, "%s: L2 Receiving a GSS token\n", GSI_PLUGIN_ID);
	token_status = gsi_recv_token(soap, &input_token->value, &input_token->length);
	if(token_status != 0)
	{
	    major_status = GSS_S_DEFECTIVE_TOKEN|GSS_S_CALL_INACCESSIBLE_READ;
	    break;
	}

        major_status = gss_accept_sec_context(&minor_status, /* (out) minor status */
				   &data->context,       /* (in) security context */
				   data->credential,    /* (in) cred handle */
				   input_token,   /* (in) input token */
				   GSS_C_NO_CHANNEL_BINDINGS, /* (in) */
				   &client_name,  /* (out) name of initiator */
				   &mech_type,    /* (out) mechanisms */
				   output_token,  /* (out) output token */
				   &data->ret_flags,    /* (out) return flags
						           bit-mask of:
							  GSS_C_DELEG_FLAG
							  GSS_C_MUTUAL_FLAG
							  GSS_C_REPLAY_FLAG
							  GSS_C_SEQUENCE_FLAG
							  GSS_C_CONF_FLAG
							  GSS_C_INTEG_FLAG
							  GSS_C_ANON_FLAG
							  GSS_C_PROT_READY_FLAG
							  GSS_C_TRANS_FLAG */
				   &time_ret,     /* (out) time ctx is valid */
				   &data->delegated_credential);         /* (out) delegated cred */

	if(output_token->length != 0)
	{
	    DEBUG_STMT(2, "%s: L2 Sending  a GSS token\n", GSI_PLUGIN_ID);
	    token_status = gsi_send_token(soap, output_token->value, output_token->length);
	    if(token_status != 0)
	    {
		major_status = GSS_S_DEFECTIVE_TOKEN|GSS_S_CALL_INACCESSIBLE_WRITE;
	    }
	    major_status2 = gss_release_buffer(&minor_status2, output_token);
    	    if(major_status2 != GSS_S_COMPLETE){
    	    	gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status2, minor_status2);
	    }
	}

	if (GSS_ERROR(major_status))
	{
	    gsi_plugin_fprintf(stderr, "%s: gss_accept_sec_context() failed\n", GSI_PLUGIN_ID);
	    if (data->context != GSS_C_NO_CONTEXT)
	    {
	    	DEBUG_STMT(2, "%s: L2 Deleting security context\n", GSI_PLUGIN_ID);
		major_status3 = gss_delete_sec_context(&minor_status3, &data->context, GSS_C_NO_BUFFER);
		if(major_status3 != GSS_S_COMPLETE){
				gsi_plugin_fprintf(stderr, "%s:  gss_delete_sec_context() failed\n", GSI_PLUGIN_ID);
				gsi_display_status("gss_delete_sec_context()", major_status3, minor_status3);
		}
		break;
	    }
	}

	if (input_token->length >0)
	{
	    free(input_token->value);
	    input_token->length = 0;
	}

    } while(major_status & GSS_S_CONTINUE_NEEDED);

    if (input_token->length >0)
    {
	free(input_token->value);
	input_token->length = 0;
    }

    /* authentication failed */
    if(major_status != GSS_S_COMPLETE)
    {
	DEBUG_STMT(2, "%s: L2 Failed to establish security context (server-side)\n", GSI_PLUGIN_ID);
	gsi_display_status("gss_accept_sec_context()", major_status, minor_status);
   	DEBUG_STMT(1, "%s: L1 Exiting gsi_accept_security_context()\n", GSI_PLUGIN_ID);
	return -1;
    }

    /* authentication succeeded, figure out who it is */
    if (major_status == GSS_S_COMPLETE)
    {
	DEBUG_STMT(2, "%s: L2 Established security context (server-side)\n", GSI_PLUGIN_ID);
    	major_status = gss_display_name(&minor_status, client_name, name_buffer, NULL);
        if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_display_name() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_display_name()", major_status, minor_status);
	}

        data->client_identity =  (char *) malloc(name_buffer->length+1);
        if (data->client_identity)
	{
		memcpy(data->client_identity, name_buffer->value, name_buffer->length);
		data->client_identity[name_buffer->length] = '\0';
		DEBUG_STMT(2, "%s: L2 Established security context with: %s\n", GSI_PLUGIN_ID, data->client_identity);
		if(data->ret_flags & GSS_C_MUTUAL_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: MUTUAL AUTHENTICATION IS ALLOWED\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_DELEG_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: CREDENTIALS CAN BE DELEGATED\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_REPLAY_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: PROTECTION AGAINST REPLAY ATTACKS IS IN EFFECT\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_SEQUENCE_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: DETECTION OF OUT OF SEQUENCE PACKETS IS IN EFFECT\n", GSI_PLUGIN_ID);
		if(data->ret_flags & GSS_C_CONF_FLAG){
			DEBUG_STMT(2, "%s: L2 Security context property: DATA CONFIDENTIALITY IS ALLOWED\n", GSI_PLUGIN_ID);
			data->want_confidentiality = 1;
		}
		if(data->ret_flags & GSS_C_INTEG_FLAG)
			DEBUG_STMT(2, "%s: L2 Security context property: DATA INTEGRITY IS ALLOWED\n", GSI_PLUGIN_ID);

                #ifdef GLITE_VOMS
		rc = gsi_get_credential_data(data->context, &cert, &chain);
		if(!rc) { /* success */
                	DEBUG_STMT(2, "%s: L2 Trying to extract VOMS attributes if available\n", GSI_PLUGIN_ID);
                	rc = gsi_get_voms_data(data, cert, chain);
                	if(rc){ /* failure */
                		DEBUG_STMT(2, "%s: L2 Failed to extract VOMS attributes\n", GSI_PLUGIN_ID);
                	}
		}
                #endif
	}
	else
	{
		gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
        	major_status = gss_release_name(&minor_status, &client_name);
        	if(major_status != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_release_name() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_name()", major_status, minor_status);
		}
        	major_status = gss_release_buffer(&minor_status2, name_buffer);
		if(major_status != GSS_S_COMPLETE){
			gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
			gsi_display_status("gss_release_buffer()", major_status, minor_status);
		}
    		DEBUG_STMT(1, "%s: L1 Exiting gsi_accept_security_context()\n", GSI_PLUGIN_ID);
		return -1;
	}

        major_status = gss_release_name(&minor_status, &client_name);
        if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_name() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_name()", major_status, minor_status);
	}
        major_status = gss_release_buffer(&minor_status2, name_buffer);
	if(major_status != GSS_S_COMPLETE){
		gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_release_buffer()", major_status, minor_status);
	}
    }
    
    DEBUG_STMT(1, "%s: L1 Exiting gsi_accept_security_context()\n", GSI_PLUGIN_ID);
    return 0;
 
}


/*
 * ! \fn int gsi_acquire_credential(struct soap *soap)
 * 
 * \brief This function acquires the GSI credential;
 * 
 * This function acquires GSI credential;
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_acquire_credential(struct soap *soap)
{
	OM_uint32       major_status;
	OM_uint32       minor_status;
	gss_name_t 	cred_name;
  	gss_buffer_desc cred_name_buffer;

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1,"%s: L1 Entering gsi_acquire_credential()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	/* release the current GSI credential */
        if(data->credential != GSS_C_NO_CREDENTIAL){
		DEBUG_STMT(2,"%s: L2 releasing our GSI credential\n", GSI_PLUGIN_ID);
                major_status = gss_release_cred(&minor_status, &data->credential);
                if(major_status != GSS_S_COMPLETE){
                        gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
                        gsi_display_status("gss_release_cred()", major_status, minor_status);
                }
                data->credential = GSS_C_NO_CREDENTIAL;

		if(data->identity)
			free(data->identity);

		DEBUG_STMT(2,"%s: L2 GSI credential released\n", GSI_PLUGIN_ID);
        }

	DEBUG_STMT(2,"%s: L2 Acquiring credential\n", GSI_PLUGIN_ID);

	/* acquire our credential */
	major_status = gss_acquire_cred(&minor_status,	/* (out) minor status */
					GSS_C_NO_NAME,	/* (in) desired name */
					GSS_C_INDEFINITE,	/* (in) desired time
								 * valid */
					GSS_C_NO_OID_SET,	/* (in) desired mechs */
					GSS_C_BOTH,	/* (in) cred usage
							 * GSS_C_BOTH
							 * GSS_C_INITIATE
							 * GSS_C_ACCEPT */
					&data->credential,	/* (out) cred handle */
					NULL,	/* (out) actual mechs */
					NULL);	/* (out) actual time valid */

	if (major_status != GSS_S_COMPLETE) {
		DEBUG_STMT(3,"%s: L3 Failed to acquire credential\n", GSI_PLUGIN_ID);
		gsi_display_status("gss_acquire_cred():", major_status, minor_status);
		DEBUG_STMT(1,"%s: L1 Exiting gsi_acquire_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	DEBUG_STMT(2,"%s: L2 Credential acquired\n", GSI_PLUGIN_ID);
	DEBUG_STMT(2,"%s: L2 Getting credential distinguished name\n", GSI_PLUGIN_ID);

	/* extract the ditinguished name from the credential, and put it in data->identity */
  	major_status = gss_inquire_cred(&minor_status, data->credential, &cred_name, NULL, NULL, NULL);
  	major_status = gss_display_name(&minor_status, cred_name, &cred_name_buffer, NULL);

	data->identity = (char *) malloc((cred_name_buffer.length + 1) * sizeof (char));
  	if(data->identity)
    	{
      		memcpy(data->identity, cred_name_buffer.value, cred_name_buffer.length);
      		data->identity[cred_name_buffer.length] = '\0';
  		major_status = gss_release_name(&minor_status, &cred_name);
  		major_status = gss_release_buffer(&minor_status, &cred_name_buffer);
		DEBUG_STMT(2,"%s: L2 credential distinguished name retrieved\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1,"%s: L1 Exiting gsi_acquire_credential()\n", GSI_PLUGIN_ID);
		return 0;
    	}

	gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
  	major_status = gss_release_name(&minor_status, &cred_name);
  	major_status = gss_release_buffer(&minor_status, &cred_name_buffer);
	DEBUG_STMT(2,"%s: L2 failing to retrieve distinguished name\n", GSI_PLUGIN_ID);
	DEBUG_STMT(1,"%s: L1 Exiting gsi_acquire_credential()\n", GSI_PLUGIN_ID);
	return -1;

}




/*
 * ! \fn int gsi_acquire_delegated_credential(struct soap *current_soap, struct soap *new_soap)
 * 
 * \brief This function copies the delegated credential available in the server current_soap environment
 * to the new_soap environment as the server GSI credential;
 * the new_soap environment is meant to be used to connect to another web service to delegate the received delegated credential 
 * 
 * This function copies the delegated credential available in the server current_soap environment
 * to the new_soap environment as the server GSI credential;
 * the new_soap environment is meant to be used to connect to another web service to delegate the received delegated credential 
 * 
 * \param struct soap *current_soap The current gSOAP runtime environment
 * \param struct soap *new_soap The gSOAP runtime environment to be used to connect to another web service to delegate the received delegated credential
 * 
 * \return 0 on success, -1 on error
 * 
 */
int
gsi_acquire_delegated_credential(struct soap *current_soap, struct soap *new_soap)
{

	OM_uint32                   major_status;
    	OM_uint32                   minor_status;
    	gss_buffer_desc             cred_buffer;
	gss_name_t 		    cred_name;
  	gss_buffer_desc   	    cred_name_buffer;
	struct gsi_plugin_data *current_data = NULL;
	struct gsi_plugin_data *new_data = NULL;

        DEBUG_STMT(1, "%s: L1 Entering gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);

	if(!current_soap || !new_soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	current_data = (struct gsi_plugin_data *) soap_lookup_plugin(current_soap, GSI_PLUGIN_ID);
	if(!current_data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	new_data = (struct gsi_plugin_data *) soap_lookup_plugin(new_soap, GSI_PLUGIN_ID);
	if(!new_data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if(current_data->delegated_credential == GSS_C_NO_CREDENTIAL){
        	gsi_plugin_fprintf(stderr, "%s: There is no valid delegated credential\n", GSI_PLUGIN_ID);
                DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
                return -1;
	}

        DEBUG_STMT(2, "%s: L2 Copying the received delegated credential from current_soap to new_soap, as GSI credential\n", GSI_PLUGIN_ID);

	/* copying the received delegated credential from current_soap to new_soap, as GSI credential */
	if(current_data->delegated_credential != GSS_C_NO_CREDENTIAL){

		if(new_data->credential != GSS_C_NO_CREDENTIAL){
			DEBUG_STMT(2,"%s: L2 releasing our GSI credential\n", GSI_PLUGIN_ID);
                	major_status = gss_release_cred(&minor_status, &new_data->credential);
                	if(major_status != GSS_S_COMPLETE){
                        	gsi_plugin_fprintf(stderr, "%s: gss_release_cred() failed\n", GSI_PLUGIN_ID);
                        	gsi_display_status("gss_release_cred()", major_status, minor_status);
                	}

                	new_data->credential = GSS_C_NO_CREDENTIAL;

			DEBUG_STMT(2,"%s: L2 GSI credential released\n", GSI_PLUGIN_ID);
        	}


                major_status = gss_export_cred(&minor_status, current_data->delegated_credential, NULL, 0, &cred_buffer);
                if(major_status != GSS_S_COMPLETE){
                        gsi_plugin_fprintf(stderr, "%s: gss_export_cred() failed\n", GSI_PLUGIN_ID);
                        gsi_display_status("gss_export_cred()", major_status, minor_status);
                        DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
                        return -1;
                }

                major_status = gss_import_cred(&minor_status, &new_data->credential, NULL, 0, &cred_buffer, 0, 0);
                if(major_status != GSS_S_COMPLETE){
                        gsi_plugin_fprintf(stderr, "%s: gss_import_cred() failed\n", GSI_PLUGIN_ID);
                        gsi_display_status("gss_import_cred()", major_status, minor_status);
                        DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
                        return -1;
                }

                major_status = gss_release_buffer(&minor_status, &cred_buffer);
                if(major_status != GSS_S_COMPLETE){
                        gsi_plugin_fprintf(stderr, "%s: gss_release_buffer() failed\n", GSI_PLUGIN_ID);
                        gsi_display_status("gss_release_buffer()", major_status, minor_status);
                        DEBUG_STMT(1, "%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
                        return -1;
                }
        }

	DEBUG_STMT(2,"%s: L2 GSI delegated credential acquired\n", GSI_PLUGIN_ID);

	
	/* extract the distinguished name from the credential, and put it in new_data->identity */
  	major_status = gss_inquire_cred(&minor_status, new_data->credential, &cred_name, NULL, NULL, NULL);
  	major_status = gss_display_name(&minor_status, cred_name, &cred_name_buffer, NULL);


	if(new_data->identity)
		free(new_data->identity);

	new_data->identity = (char *) malloc((cred_name_buffer.length + 1) * sizeof (char));
  	if(new_data->identity)
    	{
      		memcpy(new_data->identity, cred_name_buffer.value, cred_name_buffer.length);
      		new_data->identity[cred_name_buffer.length] = '\0';
  		major_status = gss_release_name(&minor_status, &cred_name);
  		major_status = gss_release_buffer(&minor_status, &cred_name_buffer);
		DEBUG_STMT(2,"%s: L2 credential distinguished name retrieved\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1,"%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
		return 0;
    	}

	gsi_plugin_fprintf(stderr, "%s: Not enough memory\n", GSI_PLUGIN_ID);
  	major_status = gss_release_name(&minor_status, &cred_name);
  	major_status = gss_release_buffer(&minor_status, &cred_name_buffer);
	DEBUG_STMT(2,"%s: L2 failing to retrieve distinguished name of credential\n", GSI_PLUGIN_ID);
	DEBUG_STMT(1,"%s: L1 Exiting gsi_acquire_delegated_credential()\n", GSI_PLUGIN_ID);
	return -1;


}





/*
 * ! \fn int gsi_set_delegation(struct soap *soap, globus_bool_t delegation, char *target_name)
 * 
 * \brief This function sets delegation; can be used client-side only.
 * 
 * This function sets delegation; can be used client-side only.
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * \param globus_bool_t delegation if GLOBUS_TRUE sets delegation
 * \param char *target_name The target name of the web service we want to connect to;
          note that this is mandatory when requesting delegation of credentials
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_set_delegation(struct soap *soap, globus_bool_t delegation, char *target_name)
{

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_delegation()\n", GSI_PLUGIN_ID);

	if(!soap){ /* target name is checked later */
		gsi_plugin_fprintf(stderr, "%s: NULL SOAP structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_delegation()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_delegation()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if (delegation == GLOBUS_FALSE){
		DEBUG_STMT(2, "%s: L2 Clearing delegation of credentials\n", GSI_PLUGIN_ID);

		if((data->req_flags & GSS_C_DELEG_FLAG) == GSS_C_DELEG_FLAG)
			data->req_flags = data->req_flags & (~GSS_C_DELEG_FLAG);
	
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_delegation()\n", GSI_PLUGIN_ID);
		return 0;
	}

	if (delegation == GLOBUS_TRUE && target_name)
	{
		DEBUG_STMT(2, "%s: L2 Requesting delegation of credentials\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_DELEG_FLAG) != GSS_C_DELEG_FLAG){
			data->req_flags = data->req_flags | GSS_C_DELEG_FLAG;
        		data->target_name = strdup(target_name);
		}
    	}
    	else{
		gsi_plugin_fprintf(stderr, "%s: When requesting delegation of credential, it is MANDATORY to supply the target name of the web service you want to connect to\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_delegation()\n", GSI_PLUGIN_ID);
     		return -1;
	}
     
	DEBUG_STMT(1, "%s: L1 Exiting gsi_set_delegation()\n", GSI_PLUGIN_ID);
	return 0;


}

/*
 * ! \fn int gsi_set_replay(struct soap *soap, globus_bool_t replay)
 * 
 * \brief This function sets replay attack detection; can be used client-side
 * only.
 * 
 * This function sets replay attack detection; can be used client-side only.
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * \param globus_bool_t replay if GLOBUS_TRUE sets replay attack detection
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_set_replay(struct soap *soap, globus_bool_t replay)
{

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_replay()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: NULL SOAP structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_replay()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_replay()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if (replay == GLOBUS_TRUE){
		DEBUG_STMT(2, "%s: L2 Requesting replay attack detection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_REPLAY_FLAG) != GSS_C_REPLAY_FLAG){
			data->req_flags = data->req_flags | GSS_C_REPLAY_FLAG;
			DEBUG_STMT(2, "%s: L2 Replay attack detection: request succesful\n", GSI_PLUGIN_ID);
		}
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_replay()\n", GSI_PLUGIN_ID);
		return 0;
	}

	if (replay == GLOBUS_FALSE){
		DEBUG_STMT(2, "%s: L2 Clearing replay attack detection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_REPLAY_FLAG) == GSS_C_REPLAY_FLAG)
			data->req_flags = data->req_flags & (~GSS_C_REPLAY_FLAG);
	}	

	DEBUG_STMT(1, "%s: L1 Exiting gsi_set_replay()\n", GSI_PLUGIN_ID);
	return 0;


}

/*
 * ! \fn int gsi_set_sequence(struct soap *soap, globus_bool_t sequence)
 * 
 * \brief This function sets out of sequence message detection; can be used
 * client-side only.
 * 
 * This function sets out of sequence message detection; can be used client-side
 * only.
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * \param globus_bool_t sequence if GLOBUS_TRUE sets out of sequence message
 * detection
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_set_sequence(struct soap *soap, globus_bool_t sequence)
{

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_sequence()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: NULL SOAP structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_sequence()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_sequence()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if (sequence == GLOBUS_TRUE){
		DEBUG_STMT(2, "%s: L2 Requesting out of sequence message detection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_SEQUENCE_FLAG) != GSS_C_SEQUENCE_FLAG){
			data->req_flags = data->req_flags | GSS_C_SEQUENCE_FLAG;
			DEBUG_STMT(2, "%s: L2 Out of sequence message detection: request succesful\n", GSI_PLUGIN_ID);
		}
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_sequence()\n", GSI_PLUGIN_ID);
		return 0;

	}

	if(sequence == GLOBUS_FALSE){
		DEBUG_STMT(2, "%s: L2 Clearing out of sequence message detection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_SEQUENCE_FLAG) == GSS_C_SEQUENCE_FLAG)
			data->req_flags = data->req_flags & (~GSS_C_SEQUENCE_FLAG);
	}

	DEBUG_STMT(1, "%s: L1 Exiting gsi_set_sequence()\n", GSI_PLUGIN_ID);
	return 0;	

}

/*
 * ! \fn int gsi_set_confidentiality(struct soap *soap, globus_bool_t confidentiality)
 * 
 * \brief This function sets message encryption; can be used client-side only.
 * 
 * This function sets message encryption; can be used client-side only.
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * \param globus_bool_t confidentiality if GLOBUS_TRUE sets message encryption
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_set_confidentiality(struct soap *soap, globus_bool_t confidentiality)
{

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_confidentiality()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: NULL SOAP structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_confidentiality()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_confidentiality()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if (confidentiality == GLOBUS_TRUE){
		DEBUG_STMT(2, "%s: L2 Requesting confidentiality (message encryption)\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_CONF_FLAG) != GSS_C_CONF_FLAG){
			data->req_flags = data->req_flags | GSS_C_CONF_FLAG;
			DEBUG_STMT(2, "%s: L2 Confidentiality (message encryption): request succesful\n", GSI_PLUGIN_ID);
		}

		data->want_confidentiality = 1;

		return 0;
	}

	if (confidentiality == GLOBUS_FALSE){
		DEBUG_STMT(2, "%s: L2 Clearing confidentiality (message encryption)\n", GSI_PLUGIN_ID);
		data->want_confidentiality = 0;
		if((data->req_flags & GSS_C_CONF_FLAG) == GSS_C_CONF_FLAG)
			data->req_flags = data->req_flags & (~GSS_C_CONF_FLAG);
	}

	DEBUG_STMT(1, "%s: L1 Exiting gsi_set_confidentiality()\n", GSI_PLUGIN_ID);
	return 0;


}

/*
 * ! \fn int gsi_set_integrity(struct soap *soap, globus_bool_t integrity)
 * 
 * \brief This function sets message anti-tampering protection; can be used
 * client-side only.
 * 
 * This function sets message anti-tampering protection; can be used client-side
 * only.
 * 
 * \param struct soap *soap The current gSOAP runtime environment
 * \param globus_bool_t integrity if GLOBUS_TRUE sets message anti-tampering
 * protection
 * 
 * \return 0 on success, -1 on error
 * 
 */

int 
gsi_set_integrity(struct soap *soap, globus_bool_t integrity)
{

	struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_integrity()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: NULL SOAP structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_integrity()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_integrity()\n", GSI_PLUGIN_ID);
		return -1;
	}

	if (integrity == GLOBUS_TRUE){
		DEBUG_STMT(2, "%s: L2 Requesting message anti-tampering protection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_INTEG_FLAG) != GSS_C_INTEG_FLAG){
			data->req_flags = data->req_flags | GSS_C_INTEG_FLAG;
			DEBUG_STMT(2, "%s: L2 Message anti-tampering protection: request succesful\n", GSI_PLUGIN_ID);
		}
		return 0;
	}

	if (integrity == GLOBUS_FALSE){
		DEBUG_STMT(2, "%s: L2 Clearing message anti-tampering protection\n", GSI_PLUGIN_ID);
		if((data->req_flags & GSS_C_INTEG_FLAG) == GSS_C_INTEG_FLAG)
			data->req_flags = data->req_flags & (~GSS_C_INTEG_FLAG);
	}

	DEBUG_STMT(1, "%s: L1 Exiting gsi_set_integrity()\n", GSI_PLUGIN_ID);
	return 0;


}


/*
 * ! \fn void gsi_display_status(char *msg, OM_uint32 maj_stat, OM_uint32 min_stat)
 * 
 * \brief This function displays GSS API error messages
 * 
 * This function display GSS API error messages
 * 
 * \param char *msg A message for the user related to the execution of the GSS API function that 
 * did not complete succesfully
 * \param OM_uint32 maj_stat The major status returned by a GSS API function 
 * \param OM_uint32 min_stat The minor status returned by a GSS API function 
 * 
 * \return void
 * 
 */

static void  gsi_display_status(char *msg, OM_uint32 maj_stat, OM_uint32 min_stat)
{ 
	display_status(msg, maj_stat, GSS_C_GSS_CODE);
	display_status(msg, min_stat, GSS_C_MECH_CODE);
}


/*
 * ! \fn display_status(char *m, OM_uint32 code, int type)
 * 
 * \brief This function displays GSS API error messages; it is used
 * internally by gsi_display_status()
 * 
 * This function display GSS API error messages; it is used
 * internally by gsi_display_status()
 * 
 * \param char *m A message for the user related to the execution of the GSS API function that 
 * did not complete succesfully
 * \param OM_uint32 code The major or minor status returned by a GSS API function 
 * \param int type The code to be used: can be either GSS_C_GSS_CODE or GSS_C_MECH_CODE
 * 
 * \return void
 * 
 */

static void display_status(char *m, OM_uint32 code, int type)
{

	OM_uint32 maj_stat, min_stat; 
	gss_buffer_desc msg = GSS_C_EMPTY_BUFFER;
	OM_uint32 msg_ctx;
	gss_OID g_mechOid = GSS_C_NULL_OID; 

	msg_ctx = 0;
	while (1) {
		maj_stat = gss_display_status(&min_stat, code, type, g_mechOid, &msg_ctx, &msg);

		if (maj_stat != GSS_S_COMPLETE) {
			gsi_plugin_fprintf(stderr, "%s: Error in gss_display_status()\n", GSI_PLUGIN_ID);
			break;
		}
		else
			gsi_plugin_fprintf(stderr, "%s: %s: %s\n", GSI_PLUGIN_ID, m, (char *)msg.value);

		if (msg.length != 0)
			(void) gss_release_buffer(&min_stat, &msg);

		if (!msg_ctx)
			break;
			
	}
}



/*
 * ! \fn readn(struct soap *soap, void *vptr, size_t n) 
 * 
 * \brief This function reads exactly n bytes from a file descriptor
 * 
 * This function reads exactly n bytes from a file descriptor 
 * 
 * \param struct soap *soap The gSOAP runtime environment
 * \param void *vptr The buffer to read into
 * \param size_t n The number of bytes that must be read
 * 
 * \return The number of bytes read on success; this should be n!
 *         on error: -1 if the socket is not valid,
 *                   -2 on select() timeout,
 *                   -3 on select() error; 
 *                   -4 on read() error; 
 *		     -5 on NULL parameter or plugin structure error
 * 
 */

static ssize_t readn(struct soap *soap, void *vptr, size_t n)
{
	size_t	nleft;
	ssize_t	nread;
	char	*ptr = NULL;
	int     rc;
	short int recv_timeout;
	int fd = -1;
    	struct gsi_plugin_data *data =  NULL;
	
   
	if(!soap || !vptr){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		return -5;
	}

    	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		return -5;
	}

	fd = soap->socket;

	ptr = (char *) vptr;
	nleft = n;

  	recv_timeout = data->recv_timeout;
	while (nleft > 0) {


		do {

                	fd_set          fdset;
                	struct timeval  tv;

                	/* reset errno value */
                	errno = 0;

                	FD_ZERO(&fdset);

                	/* check that fd is a valid file descriptor */
                	if(fd != -1)
                        	FD_SET(fd, &fdset);
                	else 
                        	return -1;

                	/* setup for timeout on select() */

                	tv.tv_sec = (int32_t) recv_timeout;
                	tv.tv_usec = 0;

                	rc = select((fd) + 1, &fdset, NULL, NULL, &tv);

        	} while(((rc == -1) && (errno == EINTR))); /* restart select() if it has been interrupted */

		if(rc == 0){ /* select() timeout */
			return -2;
		}
		
		if(rc == -1){ /* select() error */
			return -3;
		}

		if ( (nread = read(fd, ptr, nleft)) < 0) {
			if (errno == EINTR)
				nread = 0;		/* and call read() again */
			else
				return(-4);
		} else if (nread == 0)
			break;				/* EOF */

		nleft -= nread;
		ptr   += nread;
	}
	return(n - nleft);		/* return >= 0 */
}



/*
 * ! \fn writen(struct soap *soap, const void *vptr, size_t n) 
 * 
 * \brief This function writes exactly n bytes to a file descriptor
 * 
 * This function writes exactly n bytes to a file descriptor 
 * 
 * \param struct soap *soap The gSOAP runtime environment
 * \param const void *vptr The buffer to write from
 * \param size_t n The number of bytes that must be written
 * 
 * \return The number of bytes written;
 *         This is n on success
 *         on error: -1 if the socket is not valid,
 *                   -2 on select() timeout,
 *                   -3 on select() error; 
 *                   -4 on write() error; 
 *		     -5 on NULL parameter or plugin structure error
 * 
 */

static ssize_t writen(struct soap *soap, const void *vptr, size_t n)
{
	size_t		nleft;
	ssize_t		nwritten;
	const char	*ptr = NULL;
	int     	rc;
	short int send_timeout;
	int fd = -1;
    	struct gsi_plugin_data *data = NULL;

	if(!soap || !vptr){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		return -5;
	}

    	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		return -5;
	}

	fd = soap->socket;

	ptr = (char *) vptr;
	nleft = n;

	send_timeout = data->send_timeout;	

	while (nleft > 0) {

		do {

                	fd_set          fdset;
                	struct timeval  tv;

                	/* reset errno value */
                	errno = 0;

                	FD_ZERO(&fdset);

                	/* check that fd is a valid file descriptor */
                	if(fd != -1)
                        	FD_SET(fd, &fdset);
                	else 
                        	return -1;

                	/* setup for timeout on select() */

                	tv.tv_sec = (int32_t) send_timeout;
                	tv.tv_usec = 0;

                	rc = select((fd) + 1, NULL, &fdset, NULL, &tv);

        	} while(((rc == -1) && (errno == EINTR))); /* restart select() if it has been interrupted */

		if(rc == 0){ /* select() timeout */
			return -2;
		}
		
		if(rc == -1){ /* select() error */
			return -3;
		}

		if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
			if (nwritten < 0 && errno == EINTR)
				nwritten = 0;		/* and call write() again */
			else
				return(-4);			/* error */
		}

		nleft -= nwritten;
		ptr   += nwritten;
	}
	return(n);
}



/*
 * ! \fn gsi_recv_token(struct soap *soap, void **bufp, size_t *sizep) 
 * 
 * \brief This function receives a GSS token on the specified file descriptor 
 * 
 * This function receives a GSS token on the specified file descriptor 
 * The source code is strongly based on the corresponding function
 * available in the Globus Toolkit GSS ASSIST Library;
 * it has been modified to use a file descriptor
 * instead of a FILE * and to use a timeout  
 *
 * \param struct soap *soap The gSOAP runtime environment
 * \param void **bufp The buffer to read into
 * \param size_t *sizep The number of bytes that must be read
 * 
 * \return  0 on success, -1 on parameter error, an integer error code > 0 on internal error
 * 
 */

static int gsi_recv_token(
    struct soap	*			soap,
    void **                             bufp, 
    size_t *                            sizep)
{
    unsigned char                       int_buf[5];
    unsigned char *                     pp = NULL;
    unsigned char *                     bp = NULL;
    int                                 bsize = 0;
    int                                 dsize;
    int                                 size;
    void *                              cp;
    int                                 bytesread;
    int                                 return_value = 0;
    
	if(!soap || !bufp || !sizep){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		return -1;
	}

    bytesread = (int) readn(soap, int_buf, 4);

    if ((bytesread >= 0) && (bytesread != 4))
    {
        gsi_plugin_fprintf(stderr, "Failed reading token length: read %d bytes\n", bytesread);
        return_value = GSI_PLUGIN_TOKEN_EOF;
        goto exit;
    }

    if (bytesread == -2){
        return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
        goto exit;
    }
    
    /*
     * check if the length is missing, and we are receiving 
     * a SSL token directly. 
     * SSLv3 will start with a flag byte in the twenties
     * followed by major version 3 minor version 0  
     * Will also accept a SSLv2 hello 2 0 
     * or a TLS  3 1
     */
	 
    if (((int_buf[0]  >= 20) && (int_buf[0] <= 26) 
        && (((int_buf[1] == 3 && ((int_buf[2] == 0) || int_buf[2] == 1))
             || (int_buf[1] == 2 && int_buf[2] == 0))))
        || ((int_buf[0] & 0x80) && int_buf[2] == 1))
    {
        /* looks like a SSL token read rest of length */
    	bytesread = (int) readn(soap, &int_buf[4], 1); 
        if ((bytesread >=0) && (bytesread != 1))
        {
            gsi_plugin_fprintf(stderr, "%s FAILED READING EXTRA BYTE\n", GSI_PLUGIN_ID);
            return_value =  GSI_PLUGIN_TOKEN_EOF;
            goto exit;
        }
		
        if (bytesread == -2){
        	return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
        	goto exit;
    	}

        /*
	gsi_plugin_fprintf(stderr, "reading SSL token %.2x%.2x%.2x%.2x%.2x\n",
                int_buf[0], int_buf[1], int_buf[2], int_buf[3], int_buf[4]);
        
	*/

        if ((int_buf[0] & 0x80)) {
            /* looks like a sslv2 hello 
             * length is of following bytes in header. 
             * we read in 5, 2 length and 3 extra, 
             * so only need next dsize -3
             */
            dsize = ( ((unsigned int) int_buf[0] & 0x7f)<<8 
                      | (unsigned int) int_buf[1]) - 3;
        } else {
            dsize = (  ( ((unsigned int) int_buf[3]) << 8)
                       |   ((unsigned int) int_buf[4]) );
        }
        
        /* If we are using the globus_ssleay, with 
         * international version, we may be using the 
         * "26" type, where the length is really the hash 
         * length, and there is a hash, 8 byte seq andi
         * 4 byte data length following. We need to get
         * these as well. 
         */
        
        if (int_buf[0] == 26 ) 
        {
            bsize = dsize + 12;  /* MD, seq, data-length */
            bp = (unsigned char *)malloc(bsize);
            if (!bp)
            {
                return_value = GSI_PLUGIN_TOKEN_ERR_MALLOC;
                goto exit;
            }

  	    bytesread = (int) readn(soap, (void *) bp, bsize);
	    if ((bytesread >= 0) && (bytesread != bsize))
            {
            	return_value = GSI_PLUGIN_TOKEN_EOF;
               	goto exit;
            }


            if (bytesread == -2){
        	return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
        	goto exit;
    	    }


            dsize = (  ( ((unsigned int) bp[bsize-4]) << 24)
                       | ( ((unsigned int) bp[bsize-3]) << 16)
                       | ( ((unsigned int) bp[bsize-2]) << 8)
                       |   ((unsigned int) bp[bsize-1]) );
            
            size = bsize + dsize + 5;
        }
        else
        {
            size = dsize + 5; 
        }
        cp = (void *)malloc(size);				
        if (!cp)
        {
            return_value = GSI_PLUGIN_TOKEN_ERR_MALLOC;
            goto exit;
        }
        
        /* reassemble token header from in_buf and bp */

        pp = (unsigned char *) cp;
        memcpy(pp,int_buf,5);
        pp += 5;
        if (bp)
        {
            memcpy(pp,bp,bsize);
            pp += bsize;
            free(bp);
            bp = NULL;
        }
	
	bytesread = (int) readn(soap, pp, dsize);
	if ((bytesread >= 0) && (bytesread != dsize))
        {
            gsi_plugin_fprintf(stderr, "READ SHORT: %d, %d\n", dsize, bytesread);
            return_value = GSI_PLUGIN_TOKEN_EOF;
            goto exit;
        }

	
        if (bytesread == -2){
        	return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
        	goto exit;
    	}

    }
    else
    {
        size = (  ( ((unsigned int) int_buf[0]) << 24)
                  | ( ((unsigned int) int_buf[1]) << 16)
                  | ( ((unsigned int) int_buf[2]) << 8)
                  |   ((unsigned int) int_buf[3]) );
        
        if (size > 1<<24 || size < 0)  /* size may be garbage */
        {
            return_value = GSI_PLUGIN_TOKEN_ERR_BAD_SIZE; 
            goto exit;
        }
        
        cp = (void *)malloc(size);
        if (!cp)
        {
            return_value = GSI_PLUGIN_TOKEN_ERR_MALLOC;
        }

	bytesread = (int) readn(soap, cp, size);
	if ((bytesread >=0) && (bytesread != size))
        {
            gsi_plugin_fprintf(stderr, "read short: %d, %d\n", size, bytesread);
            return_value = GSI_PLUGIN_TOKEN_EOF;
            goto exit;
        }
	
	if (bytesread == -2){
                return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
                goto exit;
        }


    }
    
    *bufp = cp;
    *sizep = size;

 exit:
    return return_value;
}




/*
 * ! \fn gsi_send_token(struct soap *soap, void *buf, size_t size) 
 * 
 * \brief This function sends a GSS token on the specified file descriptor 
 * 
 * This function sends a GSS token on the specified file descriptor 
 * The source code is strongly based on the corresponding function
 * available in the Globus Toolkit GSS ASSIST Library;
 * it has been modified to use a file descriptor
 * instead of a FILE * and a timeout 
 * 
 * \param struct soap *soap The gSOAP runtime environment
 * \param void *buf The buffer to write from
 * \param size_t size The number of bytes that must be written
 * 
 * \return  0 on success, -1 on parameter error, an integer error code > 0 on internal error
 * 
 */

static int gsi_send_token(
    struct soap				*soap,
    void *                              buf, 
    size_t                              size)
{
    int                                 return_value = 0;
    unsigned char                       int_buf[4];
    char *                              header = (char *) buf;
    int					byteswritten;

	if(!soap || !buf){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		return -1;
	}

    if (!(size > 5 && header[0] <= 26 && header[0] >= 20
          && ((header[1] == 3 && (header[2] == 0 || header[2] == 1))
              || (header[1] == 2 && header[2] == 0))))
    {
        
            int_buf[0] =  size >> 24;
            int_buf[1] =  size >> 16;
            int_buf[2] =  size >>  8;
            int_buf[3] =  size;
            
            /* send length */
			            
	    byteswritten = (int) writen(soap, int_buf, 4);
	    if ((byteswritten >= 0) && (byteswritten != 4))
            {
                return_value = GSI_PLUGIN_TOKEN_EOF;
                goto exit;
            }

	    
	    if (byteswritten == -2){
                return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
                goto exit;
            }

    }
	
	/* send token */
    byteswritten = (int) writen(soap, buf, size);
    if ((byteswritten >= 0) && (byteswritten != (int)size))
    {
        return_value = GSI_PLUGIN_TOKEN_EOF;
        goto exit;
    }

    if (byteswritten == -2){
        return_value = GSI_PLUGIN_TOKEN_TIMEOUT;
        goto exit;
    }

 exit:	
    return return_value;
}



/*
 * ! \fn gsi_set_recv_timeout(struct soap *soap, short int recv_timeout)
 * 
 * \brief This function sets the receive timeout 
 * 
 * This function sets the receive timeout  
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param short int recv_timeout The value of the timeout (in seconds). 
 * It must be greater than 0
 * 
 * \return  0 on success, -1 on error
 * 
 */

int gsi_set_recv_timeout(struct soap *soap, short int recv_timeout)
{
    struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_recv_timeout()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_recv_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}


	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_recv_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}

	DEBUG_STMT(2, "%s: L2 Setting receive timeout value\n", GSI_PLUGIN_ID);

	if (recv_timeout > 0)
	{
		data->recv_timeout = recv_timeout;

	    DEBUG_STMT(1, "%s: L1 Exiting gsi_set_recv_timeout()\n", GSI_PLUGIN_ID);
	    return 0;
    }
    else
    {
 	  DEBUG_STMT(1, "%s: L1 Exiting gsi_set_recv_timeout()\n", GSI_PLUGIN_ID);
      return -1;
    }
}

/*
 * ! \fn gsi_set_send_timeout(struct soap *soap, short int send_timeout)
 * 
 * \brief This function sets the send timeout 
 * 
 * This function sets the send timeout  
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param short int send_timeout The value of the timeout (in seconds). 
 * It must be greater than 0
 * 
 * \return  0 on success, -1 on error
 * 
 */
 
int gsi_set_send_timeout(struct soap *soap, short int send_timeout)
{
    struct gsi_plugin_data *data = NULL;

	DEBUG_STMT(1, "%s: L1 Entering gsi_set_send_timeout()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_send_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_send_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}

	DEBUG_STMT(2, "%s: L2 Setting send timeout value\n", GSI_PLUGIN_ID);

	if (send_timeout > 0)
	{
		data->send_timeout = send_timeout;

	   DEBUG_STMT(1, "%s: L1 Exiting gsi_set_send_timeout()\n", GSI_PLUGIN_ID);
	   return 0;
    }
    else
    {
      DEBUG_STMT(1, "%s: L1 Exiting gsi_set_send_timeout()\n", GSI_PLUGIN_ID);
      return -1;
    }
}

/*
 * ! \fn gsi_set_accept_timeout(struct soap *soap, short int accept_timeout)
 * 
 * \brief This function sets the accept timeout 
 * 
 * This function sets the accept timeout  
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param short int accept_timeout The value of the timeout (in seconds). 
 * It must be greater than 0
 * 
 * \return  0 on success, -1 on error
 * 
 */
 
int gsi_set_accept_timeout(struct soap *soap, short int accept_timeout)
{

   DEBUG_STMT(1, "%s: L1 Entering gsi_set_accept_timeout()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_accept_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}


   DEBUG_STMT(2, "%s: L2 Setting accept timeout value\n", GSI_PLUGIN_ID);
   
   if(accept_timeout > 0)
   {
     soap->accept_timeout = (int) accept_timeout;
     DEBUG_STMT(1, "%s: L1 Exiting gsi_set_accept_timeout()\n", GSI_PLUGIN_ID);     
     return 0;
   }
   else
   {
     DEBUG_STMT(1, "%s: L1 Exiting gsi_set_accept_timeout()\n", GSI_PLUGIN_ID);     
     return -1;   
   }
   
}

/*
 * ! \fn gsi_set_connect_timeout(struct soap *soap, short int connect_timeout)
 * 
 * \brief This function sets the connect timeout 
 * 
 * This function sets the connect timeout  
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param short int connect_timeout The value of the timeout (in seconds). 
 * It must be greater than 0
 * 
 * \return  0 on success, -1 on error
 * 
 */
 
int gsi_set_connect_timeout(struct soap *soap, short int connect_timeout)
{

   DEBUG_STMT(1, "%s: L1 Entering gsi_set_connect_timeout()\n", GSI_PLUGIN_ID);

	if(!soap){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_set_connect_timeout()\n", GSI_PLUGIN_ID);
		return -1;
	}

   DEBUG_STMT(2, "%s: L2 Setting connect timeout value\n", GSI_PLUGIN_ID);
     
   if(connect_timeout > 0)
   {
     soap->connect_timeout = (int) connect_timeout;
     DEBUG_STMT(1, "%s: L1 Exiting gsi_set_connect_timeout()\n", GSI_PLUGIN_ID);
     return 0;
   }
   else
   {
     DEBUG_STMT(1, "%s: L1 Exiting gsi_set_connect_timeout()\n", GSI_PLUGIN_ID);
     return -1;
   }
  
}


/*
 * ! \fn gsi_authorization_callback_register(struct soap *soap, int (*gsi_authorization_callback) (struct soap *soap, char *distinguished_name)) 
 * 
 * \brief This function registers an authorization callback
 * 
 * This function registers  an authorization callback
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param int (*gsi_authorization_callback) (struct soap *soap, char *distinguished_name) This is the authorization callback prototype
 * 
 * \return  0 on success, -1 on error
 */
int gsi_authorization_callback_register(struct soap *soap, int (*gsi_authorization_callback) (struct soap *soap, char *distinguished_name))
{
	if(!soap || !gsi_authorization_callback){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_authorization_callback_register()\n", GSI_PLUGIN_ID);
		return -1;
	}

	struct gsi_plugin_data *data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_authorization_callback_register()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data->gsi_authorization_callback = gsi_authorization_callback;
	return 0;
}

/*
 * ! \fn gsi_credential_renew_callback_register(struct soap *soap, int (*credential_renew_callback) (struct soap *soap, int current_lifetime)) 
 * 
 * \brief This function registers a proxy credential renewal callback
 * 
 * This function registers a proxy credential renewal callback
 * 
 * \param struct soap *soap The current gSOAP runtime environment 
 * \param int (*credential_renew_callback) (struct soap *soap, int current_lifetime) This the credential callback prototype
 * 
 * \return  0 on success, -1 on error
 */
int gsi_credential_renew_callback_register(struct soap *soap, int (*credential_renew_callback) (struct soap *soap, int current_lifetime))
{
	if(!soap || !credential_renew_callback){
		gsi_plugin_fprintf(stderr, "%s: There are NULL parameters\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_credential_renew_callback_register()\n", GSI_PLUGIN_ID);
		return -1;
	}

	struct gsi_plugin_data *data = (struct gsi_plugin_data *) soap_lookup_plugin(soap, GSI_PLUGIN_ID);
	if(!data){
		gsi_plugin_fprintf(stderr, "%s: NULL plugin structure\n", GSI_PLUGIN_ID);
		DEBUG_STMT(1, "%s: L1 Exiting gsi_credential_renew_callback_register()\n", GSI_PLUGIN_ID);
		return -1;
	}

	data->gsi_credential_renew_callback = credential_renew_callback;
	return 0;
}




#ifdef GLITE_VOMS

/*! \fn int gsi_get_credential_data(gss_ctx_id_t gss_context, X509 **cert, STACK_OF(X509) **chain)
 *
 * \brief This function retrieves the peer certificate and certificate chain from the  GSI context;
 * 
 * This function retrieves the peer certificate and certificate chain from the  GSI context;
 *
 * \param gss_context  The GSS context
 * \param cert  Upon return this is the peer certificate
 * \param chain  Upon return this is the peer certificate chain
 *
 * \return 0 on success, -1 on error
 *
 */
static int gsi_get_credential_data(gss_ctx_id_t gss_context, X509 **cert, STACK_OF(X509) **chain)
{


    globus_gsi_cred_handle_t credential_handle;
    gss_cred_id_t credential;  
    gss_cred_id_desc *credential_description = NULL;
    gss_ctx_id_desc  *context = NULL;
    X509 *mycert = NULL;
    STACK_OF(X509) *mychain = NULL;
    int i;

   DEBUG_STMT(1, "%s: L1 Entering gsi_get_credential_data()\n", GSI_PLUGIN_ID);

	if(gss_context == GSS_C_NO_CONTEXT){
		gsi_plugin_fprintf(stderr, "%s: The GSI context is not valid\n", GSI_PLUGIN_ID);
   		DEBUG_STMT(1, "%s: L1 Exiting gsi_get_credential_data()\n", GSI_PLUGIN_ID);
		return -1;
	}

	
        context = (gss_ctx_id_desc *) gss_context;
	credential = (gss_cred_id_t) context->peer_cred_handle;
	if (credential == GSS_C_NO_CREDENTIAL) {
		gsi_plugin_fprintf(stderr, "%s: The peer credential is not valid\n", GSI_PLUGIN_ID);
   		DEBUG_STMT(1, "%s: L1 Exiting gsi_get_credential_data()\n", GSI_PLUGIN_ID);
		return -1;
	}

	credential_description = (gss_cred_id_desc *) credential;
	credential_handle = (globus_gsi_cred_handle_t) credential_description->cred_handle;
        mycert = X509_dup(credential_handle->cert);
	if(!cert){
		gsi_plugin_fprintf(stderr, "%s: No certificate has been found in the peer credential\n", GSI_PLUGIN_ID);
   		DEBUG_STMT(1, "%s: L1 Exiting gsi_get_credential_data()\n", GSI_PLUGIN_ID);
		return -1;
	}

	mychain = sk_X509_new_null();
        for(i = 0; i < sk_X509_num(credential_handle->cert_chain); ++i){

            X509 *tmp_cert;
            tmp_cert = X509_dup(sk_X509_value(credential_handle->cert_chain, i));
            if(tmp_cert)
                sk_X509_push(mychain, tmp_cert);
            else{
                if(mychain)
                        sk_X509_pop_free(mychain, X509_free);

                break;
            }

        }


	if(!mychain){
		gsi_plugin_fprintf(stderr, "%s: No certificate chain has been found in the peer credential\n", GSI_PLUGIN_ID);
		goto exit;
	}


	*cert = mycert;
	*chain = mychain;

	return 0;

exit:

	if(mycert)
		X509_free (mycert);

  	if(mychain)
		sk_X509_pop_free(mychain, X509_free);

   	DEBUG_STMT(1, "%s: L1 Exiting gsi_get_credential_data()\n", GSI_PLUGIN_ID);

	return -1;


}

/*! \fn int gsi_get_voms_data(struct gsi_plugin_data *data)
 *
 * \brief This function retrieves (if available) from the  GSI context VOMS attributes;
 * 
 * This function retrieves (if available) from the  GSI context VOMS attributes;
 *
 * \param data The gSOAP plugin structure
 * \param cert  the peer certificate
 * \param chain  the peer certificate chain
 *
 * \return 0 on success, -1 on error
 *
 */
static int gsi_get_voms_data(struct gsi_plugin_data *data, X509 *cert, STACK_OF(X509) *chain)
{

    struct voms **structvoms = NULL; 
    struct vomsdata *structvomsdata = NULL;
    char **fqan = NULL;
    int voms_error;
    int rc;
    char *voms_err_msg = NULL;
    int fqan_count = 0;
    int i = 0;
    int voms_return_value = -1; /* assume failure ;-) */


   DEBUG_STMT(1, "%s: L1 Entering gsi_get_voms_data()\n", GSI_PLUGIN_ID);

    	if(!data){
		gsi_plugin_fprintf(stderr, "%s: The plugin data structure is NULL\n", GSI_PLUGIN_ID);
		goto exit;		
	}

    	if(!cert){
		gsi_plugin_fprintf(stderr, "%s: The certificate is not valid\n", GSI_PLUGIN_ID);
		goto exit;		
	}

    	if(!chain){
		gsi_plugin_fprintf(stderr, "%s: The certificate chain is not valid\n", GSI_PLUGIN_ID);
		goto exit;		
	}


	#ifdef GSI_PLUGIN_THREADS
	pthread_mutex_lock(&gsi_plugin_lock);
	#endif
	structvomsdata = VOMS_Init(NULL, NULL); 
	if(!structvomsdata){
		#ifdef GSI_PLUGIN_THREADS
		pthread_mutex_unlock(&gsi_plugin_lock);
		#endif
		gsi_plugin_fprintf(stderr, "%s: VOMS_Init() failed\n", GSI_PLUGIN_ID);
		goto exit;		
	}

	rc = VOMS_Retrieve(cert, chain, RECURSE_CHAIN, structvomsdata, &voms_error);
	#ifdef GSI_PLUGIN_THREADS
	pthread_mutex_unlock(&gsi_plugin_lock);
	#endif
	if(!rc){ /* failure */

		if(rc == VERR_NOEXT){
		DEBUG_STMT(2, "%s: L2 There are NO VOMS extensions\n", GSI_PLUGIN_ID);
		}
		else{
			voms_err_msg = VOMS_ErrorMessage(structvomsdata, voms_error, NULL, 0);
			if(voms_err_msg){
				gsi_plugin_fprintf(stderr, "%s: The error is: %s\n", GSI_PLUGIN_ID, voms_err_msg);
				free(voms_err_msg);	
			}
		
		}

		goto exit;

	}

	/* count how many VOMS attributes are available */
	for (structvoms = structvomsdata->data; structvoms && *structvoms; structvoms++)
      		for (fqan = (*structvoms)->fqan; fqan && *fqan; fqan++)
			fqan_count++;

	/* allocate memory for the data->fqans field, taking into account that must be NULL terminated */
	if(fqan_count){
		data->fqans = calloc(fqan_count + 1, sizeof(*(data->fqans)));
		if(!data->fqans){
			gsi_plugin_fprintf(stderr, "%s: not enough memory\n", GSI_PLUGIN_ID);
			goto exit;		
		}


		/* insert VOMS attributes in data->fqans */
		for (structvoms = structvomsdata->data; structvoms && *structvoms; structvoms++){
      			for (fqan = (*structvoms)->fqan; fqan && *fqan; fqan++){
				data->fqans[i] = strdup(*fqan);
				if(!data->fqans[i]){
					gsi_plugin_fprintf(stderr, "%s: not enough memory\n", GSI_PLUGIN_ID);
					goto exit;		
				}

				i++;
			}
		}

	}


	voms_return_value = 0; /* success */


exit:

	if(structvomsdata)
		VOMS_Destroy(structvomsdata);

	if(cert)
		X509_free (cert);

  	if(chain)
		sk_X509_pop_free(chain, X509_free);


	if(voms_return_value == -1){
		if(data->fqans){
			int i;

			for(i = 0; data->fqans[i]; i++)
				if(data->fqans[i])
					free(data->fqans[i]);

			free(data->fqans);

		}

	}

   	DEBUG_STMT(1, "%s: L1 Exiting  gsi_get_voms_data()\n", GSI_PLUGIN_ID);

	return voms_return_value;

}
#endif


