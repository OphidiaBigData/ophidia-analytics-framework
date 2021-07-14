/*
    Esdm-pav-runtime
    Copyright (C) 2021 CMCC Foundation

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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <pthread.h>
#include <sys/wait.h>
#include <getopt.h>
#include <sys/types.h>

#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "assert.h"

#include "debug.h"
#include "oph_server_confs.h"
#include "oph_license.h"
#include "rabbitmq_utils.h"
#include "oph_gather.h"
#include "oph_analytics_framework.h"

#define OPH_SCHED_CONF_FILE OPH_WORKER_CONFIGURATION_FILE

int total_used_ncores = 0;
pthread_cond_t cond_var;
pthread_mutex_t ncores_mutex;

pthread_mutex_t framework_mutex;

int thread_number;

pthread_t *thread_cont_list = NULL;
int *pthread_create_arg = NULL;

int ptr_list_size = 13;
char *ptr_list[13];

amqp_connection_state_t *conn_thread_consume_list = NULL;
amqp_channel_t default_channel = 1;
amqp_basic_properties_t props;

sigset_t signal_set;

HASHTBL *hashtbl;

char *launcher = 0;
char *hostname = 0;
char *port = 0;
char *task_queue_name = 0;
char *username = 0;
char *password = 0;
char *framework_path = 0;
char *config_file = 0;
char *rabbitmq_direct_mode = "amq.direct";
char *max_ncores = 0;
char *global_ip_address = 0;
char *master_hostname = 0;
char *master_port = 0;
char *thread_number_str = 0;

pthread_mutex_t global_flag;

void kill_threads()
{
	int ii;
	for (ii = 0; ii < thread_number; ii++) {
		if (pthread_kill(thread_cont_list[ii], SIGUSR1) != 0)
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on pthread_kill - Thread: %d\n", ii);
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All worker threads have been closed\n");
}

void release_thread()
{
	pthread_exit(NULL);
}

void release_main()
{
	int ii;
	for (ii = 0; ii < thread_number; ii++) {
		if (pthread_join(thread_cont_list[ii], NULL) != 0)
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error join thread %d\n", ii);
	}

	for (ii = 0; ii < thread_number; ii++)
		close_rabbitmq_connection(conn_thread_consume_list[ii], (amqp_channel_t) (ii + 1));
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All RabbitMQ connections for consume system have been closed (%d connections)\n", ii);

	if (oph_server_conf_unload(&hashtbl) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on oph_server_conf_unload\n");

	free(pthread_create_arg);
	free(conn_thread_consume_list);
	free(thread_cont_list);

	for (ii = 0; ii < ptr_list_size; ii++) {
		if (ptr_list[ii])
			free(ptr_list[ii]);
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All memory allocations have been released\n");

	pthread_mutex_destroy(&global_flag);
	pthread_mutex_destroy(&ncores_mutex);
	pthread_mutex_destroy(&framework_mutex);
	pthread_cond_destroy(&cond_var);

	exit(0);
}

int process_message(amqp_envelope_t full_message)
{
	char *message = (char *) malloc(full_message.message.body.len + 1);
	strncpy(message, (char *) full_message.message.body.bytes, full_message.message.body.len);
	message[full_message.message.body.len] = 0;

	char *ptr = NULL;
	char *submission_string = strtok_r(message, "***", &ptr);
	if (!submission_string) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Fail to read submission_string parameter\n");
		return 0;
	}
	char *workflow_id = strtok_r(NULL, "***", &ptr);
	if (!workflow_id) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Fail to read workflow_id parameter\n");
		return 0;
	}
	char *job_id = strtok_r(NULL, "***", &ptr);
	if (!job_id) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Fail to read job_id parameter\n");
		return 0;
	}
	char *ncores = strtok_r(NULL, "***", &ptr);
	if (!ncores) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Fail to read ncores parameter\n");
		return 0;
	}
	char *log_string = strtok_r(NULL, "***", &ptr);
	if (!log_string) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Fail to read log_string parameter\n");
		return 0;
	}

	int process_ncores = atoi(ncores);
	if (process_ncores > atoi(max_ncores)) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "This consumer process does not support job processing that require more than %s cores\n", max_ncores);

		if (message)
			free(message);

		return 0;
	}

	submission_string++;
	submission_string[strlen(submission_string) - 1] = 0;

	pthread_mutex_lock(&ncores_mutex);
	while (process_ncores + total_used_ncores > atoi(max_ncores))
		pthread_cond_wait(&cond_var, &ncores_mutex);

	total_used_ncores += process_ncores;
	pthread_mutex_unlock(&ncores_mutex);

	pthread_mutex_lock(&framework_mutex);
	int res = oph_af_execute_framework(submission_string, 1, 0);
	pthread_mutex_unlock(&framework_mutex);
	if (res)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Framework execution failed! ERROR: %d\n", res);

	pthread_mutex_lock(&ncores_mutex);
	total_used_ncores -= process_ncores;
	pthread_cond_signal(&cond_var);
	pthread_mutex_unlock(&ncores_mutex);

	if (message)
		free(message);

	return 1;
}

void *worker_pthread_function(void *param)
{
	int thread_param = (int) *((int *) param);

	pthread_sigmask(SIG_UNBLOCK, &signal_set, NULL);

	struct sigaction thread_new_act, thread_old_act;

	thread_new_act.sa_handler = release_thread;
	sigemptyset(&thread_new_act.sa_mask);
	thread_new_act.sa_flags = 0;

	int res;
	if ((res = sigaction(SIGUSR1, &thread_new_act, &thread_old_act)) < 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to setup signal\n");
		exit(0);
	}

	int ack_res, nack_res;
	amqp_rpc_reply_t reply;
	amqp_envelope_t envelope;
	amqp_channel_t thread_channel = (amqp_channel_t) (thread_param + 1);

	while (conn_thread_consume_list[thread_param]) {
		// amqp_maybe_release_buffers_on_channel(conn_thread_consume_list[thread_param], thread_channel);
		amqp_maybe_release_buffers(conn_thread_consume_list[thread_param]);

		reply = amqp_consume_message(conn_thread_consume_list[thread_param], &envelope, NULL, 0);
		// CONN, POINTER TO MESSAGE, TIMEOUT = NULL = BLOCKING, UNUSED FLAG

		if (reply.reply_type == AMQP_RESPONSE_NORMAL)
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been consumed - Thread: %d\n", thread_param);
		else
			continue;

		if (process_message(envelope)) {
			// ACKNOWLEDGE MESSAGE
			ack_res = amqp_basic_ack(conn_thread_consume_list[thread_param], thread_channel, envelope.delivery_tag, 0);	// last param: if true, ack all messages up to this delivery tag, if false ack only this delivery tag
			if (ack_res != 0) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to ack message. Delivery tag: %d - Thread: %d\n", (int) envelope.delivery_tag, thread_param);

				nack_res = amqp_basic_reject(conn_thread_consume_list[thread_param], thread_channel, envelope.delivery_tag, (amqp_boolean_t) 1);	// 1 To put the message back in the queue
				if (nack_res != 0) {
					pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to Nack message. Delivery tag: %d - Thread: %d\n", (int) envelope.delivery_tag, thread_param);
					exit(0);
				} else
					pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been requeued - Thread: %d\n", thread_param);
			} else
				pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been acked - Thread: %d\n", thread_param);
		} else {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on message processing - Thread: %d\n", thread_param);

			nack_res = amqp_basic_reject(conn_thread_consume_list[thread_param], thread_channel, envelope.delivery_tag, (amqp_boolean_t) 1);	// 1 To put the message back in the queue
			if (nack_res != 0) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to Nack message. Delivery tag: %d - Thread: %d\n", (int) envelope.delivery_tag, thread_param);
				exit(0);
			} else
				pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been requeued - Thread: %d\n", thread_param);
		}

		amqp_destroy_envelope(&envelope);
	}

	return (NULL);
}

int main(int argc, char const *const *argv)
{
	int msglevel = LOG_INFO;
	set_debug_level(msglevel + 10);

	(void) argv;

	int ch, neededSize = 0;

	static char *USAGE = "\nUSAGE:\nesdm_pav_runtime_worker [-d] [-c <config_file>] [-L <launcher>] [-H <RabbitMQ hostname>] "
	    "[-P <RabbitMQ port>] [-Q <RabbitMQ task_queue>] [-a <master hostname>] [-b <master port>] [-u <RabbitMQ username>] "
	    "[-p <RabbitMQ password>] [-f <framework_path>] [-n <max_ncores>] [-t <thread_number>]\n";

	while ((ch = getopt(argc, (char *const *) argv, ":c:L:H:P:Q:a:b:u:p:f:n:t:dhxz")) != -1) {
		switch (ch) {
			case 'c':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				config_file = (char *) malloc(neededSize + 1);
				snprintf(config_file, neededSize + 1, "%s", optarg);
				ptr_list[0] = config_file;
				break;
			case 'L':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				launcher = (char *) malloc(neededSize + 1);
				snprintf(launcher, neededSize + 1, "%s", optarg);
				ptr_list[1] = launcher;
				break;
			case 'H':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				hostname = (char *) malloc(neededSize + 1);
				snprintf(hostname, neededSize + 1, "%s", optarg);
				ptr_list[2] = hostname;
				break;
			case 'P':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				port = (char *) malloc(neededSize + 1);
				snprintf(port, neededSize + 1, "%s", optarg);
				ptr_list[3] = port;
				break;
			case 'Q':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				task_queue_name = (char *) malloc(neededSize + 1);
				snprintf(task_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[4] = task_queue_name;
				break;
			case 'a':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				master_hostname = (char *) malloc(neededSize + 1);
				snprintf(master_hostname, neededSize + 1, "%s", optarg);
				ptr_list[5] = master_hostname;
				break;
			case 'b':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				master_port = (char *) malloc(neededSize + 1);
				snprintf(master_port, neededSize + 1, "%s", optarg);
				ptr_list[6] = master_port;
				break;
			case 'u':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				username = (char *) malloc(neededSize + 1);
				snprintf(username, neededSize + 1, "%s", optarg);
				ptr_list[7] = username;
				break;
			case 'p':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				password = (char *) malloc(neededSize + 1);
				snprintf(password, neededSize + 1, "%s", optarg);
				ptr_list[8] = password;
				break;
			case 'f':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				framework_path = (char *) malloc(neededSize + 1);
				snprintf(framework_path, neededSize + 1, "%s", optarg);
				ptr_list[9] = framework_path;
				break;
			case 'n':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				max_ncores = (char *) malloc(neededSize + 1);
				snprintf(max_ncores, neededSize + 1, "%s", optarg);
				ptr_list[10] = max_ncores;
				break;
			case 't':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				thread_number_str = (char *) malloc(neededSize + 1);
				snprintf(thread_number_str, neededSize + 1, "%s", optarg);
				ptr_list[11] = thread_number_str;
				break;
			case 'd':
				msglevel = LOG_DEBUG;
				break;
			case 'h':
				fprintf(stdout, "%s", USAGE);
				exit(0);
			case 'x':
				fprintf(stdout, "%s", OPH_WARRANTY);
				exit(0);
			case 'z':
				fprintf(stdout, "%s", OPH_CONDITIONS);
				exit(0);
			default:
				fprintf(stdout, "%s", USAGE);
				exit(0);
		}
	}

	set_debug_level(msglevel + 10);
	pmesg_safe(&global_flag, LOG_INFO, __FILE__, __LINE__, "Selected log level %d\n", msglevel);

	FILE *fp;
	fp = popen("hostname -I | awk '{print $1}'", "r");
	if (fp == NULL)
		exit(1);

	global_ip_address = (char *) malloc(16);
	if (fgets(global_ip_address, 16, fp) == NULL) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get ip address\n");
		release_main();

		exit(0);
	}
	pclose(fp);
	global_ip_address[strcspn(global_ip_address, "\n")] = 0;
	ptr_list[12] = global_ip_address;

	short unsigned int instance = 0;

	if (!config_file) {
		neededSize = snprintf(NULL, 0, "%s", OPH_SCHED_CONF_FILE);
		config_file = (char *) malloc(neededSize + 1);
		snprintf(config_file, neededSize + 1, "%s", OPH_SCHED_CONF_FILE);
		ptr_list[0] = config_file;
	}

	if (oph_server_conf_load(instance, &hashtbl, config_file)) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to open configuration file\n");
		exit(0);
	}

	if (!launcher) {
		if (oph_server_conf_get_param(hashtbl, "WORKER_LAUNCHER", &launcher)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get WORKER_LAUNCHER param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM WORKER_LAUNCHER: %s\n", launcher);

	if (!hostname) {
		if (oph_server_conf_get_param(hashtbl, "WORKER_HOSTNAME", &hostname)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get WORKER_HOSTNAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM WORKER_HOSTNAME: %s\n", hostname);

	if (!port) {
		if (oph_server_conf_get_param(hashtbl, "WORKER_PORT", &port)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get WORKER_PORT param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM WORKER_PORT: %s\n", port);

	if (!task_queue_name) {
		if (oph_server_conf_get_param(hashtbl, "TASK_QUEUE_NAME", &task_queue_name)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get TASK_QUEUE_NAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM TASK_QUEUE_NAME: %s\n", task_queue_name);

	if (!master_hostname) {
		if (oph_server_conf_get_param(hashtbl, "MASTER_HOSTNAME", &master_hostname)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get MASTER_HOSTNAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MASTER_HOSTNAME: %s\n", master_hostname);

	if (!master_port) {
		if (oph_server_conf_get_param(hashtbl, "MASTER_PORT", &master_port)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get MASTER_PORT param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MASTER_PORT: %s\n", master_port);

	if (!username) {
		if (oph_server_conf_get_param(hashtbl, "RABBITMQ_USERNAME", &username)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get RABBITMQ_USERNAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM RABBITMQ_USERNAME: %s\n", username);

	if (!password) {
		if (oph_server_conf_get_param(hashtbl, "RABBITMQ_PASSWORD", &password)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get RABBITMQ_PASSWORD param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM RABBITMQ_PASSWORD: %s\n", password);

	if (!framework_path) {
		if (oph_server_conf_get_param(hashtbl, "FRAMEWORK_PATH", &framework_path)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get FRAMEWORK_PATH param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM FRAMEWORK_PATH: %s\n", framework_path);

	if (!max_ncores) {
		if (oph_server_conf_get_param(hashtbl, "MAX_NCORES", &max_ncores)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get MAX_NCORES param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MAX_NCORES: %s\n", max_ncores);

	if (!thread_number_str) {
		if (oph_server_conf_get_param(hashtbl, "WORKER_THREAD_NUMBER", &thread_number_str)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get WORKER_THREAD_NUMBER param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM WORKER_THREAD_NUMBER: %s\n", thread_number_str);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = AMQP_DELIVERY_PERSISTENT;

	if (pthread_mutex_init(&ncores_mutex, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
	if (pthread_mutex_init(&framework_mutex, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
	if (pthread_cond_init(&cond_var, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on condition variable init\n");

	sigemptyset(&signal_set);
	sigaddset(&signal_set, SIGUSR1);

	thread_number = atoi(thread_number_str);
	thread_cont_list = (pthread_t *) malloc(sizeof(pthread_t) * thread_number);
	pthread_create_arg = (int *) malloc(sizeof(int) * thread_number);
	conn_thread_consume_list = (amqp_connection_state_t *) malloc(sizeof(amqp_connection_state_t) * thread_number);

	int ii;
	for (ii = 0; ii < thread_number; ii++) {
		rabbitmq_consume_connection(&conn_thread_consume_list[ii], (amqp_channel_t) (ii + 1), master_hostname, master_port, username, password, task_queue_name, rabbitmq_direct_mode, 1);

		*pthread_create_arg = ii;
		if (pthread_create(&(thread_cont_list[ii]), NULL, &worker_pthread_function, (void *) pthread_create_arg) != 0) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error creating thread %d\n", ii);
			exit(0);
		}
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "%s worker threads have been started\n", thread_number_str);

	struct sigaction new_act, old_act;

	new_act.sa_handler = kill_threads;
	sigemptyset(&new_act.sa_mask);
	new_act.sa_flags = 0;

	int res;
	if ((res = sigaction(SIGINT, &new_act, &old_act)) < 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to setup signal\n");
		exit(0);
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Esdm-pav-runtime-worker is ready to work\n");

	release_main();

	exit(0);
}
