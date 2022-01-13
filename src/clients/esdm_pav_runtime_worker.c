/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2020 CMCC Foundation

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

#include "assert.h"

#include "debug.h"
#include "oph_server_confs.h"
#include "oph_license.h"
#include "rabbitmq_utils.h"
#include "oph_analytics_framework.h"

#define OPH_SCHED_CONF_FILE OPH_WORKER_CONFIGURATION_FILE

int total_used_ncores = 0;
pthread_cond_t cond_var;
pthread_mutex_t ncores_mutex;

#ifdef UPDATE_CANCELLATION_SUPPORT
int *shared_ids_array = NULL;
pid_t *PID_array = NULL;

typedef struct {
	int id;
	int checks_todo;
	int consumed_messages;
} cancellation_struct;

int cancellation_struct_size = 0;
cancellation_struct *canc_struct_list = NULL;
int delete_routine_isup = 0;

pthread_rwlock_t *thread_lock_list = NULL;
pthread_rwlock_t struct_size_lock, canc_struct_lock;

pthread_t update_thread_tid, delete_thread_tid;

amqp_connection_state_t *conn_thread_publish_list = NULL;

amqp_connection_state_t consume_updater_conn, publish_db_conn, consume_delete_conn, publish_delete_conn, publish_delete_update_conn;
#endif

int thread_number;
int process_pid = -1;

pthread_t *thread_cont_list = NULL;
int *pthread_create_arg = NULL;

int ptr_list_size = 19;
char *ptr_list[19];

amqp_connection_state_t *conn_thread_consume_list = NULL;
amqp_channel_t default_channel = 1;
amqp_basic_properties_t props;

sigset_t signal_set;

HASHTBL *hashtbl;

char *hostname = 0;
char *port = 0;
char *task_queue_name = 0;
char *username = 0;
char *password = 0;
char *config_file = 0;
char *rabbitmq_direct_mode = "amq.direct";
char *max_ncores = 0;
char *master_hostname = 0;
char *master_port = 0;
char *thread_number_str = 0;
char *worker_launcher = 0;
char *framework_path = 0;

char nodename[1024];

pthread_mutex_t global_flag;

#ifdef UPDATE_CANCELLATION_SUPPORT
char *update_queue_name = 0;
char *db_manager_queue_name = 0;
char *delete_queue_name = 0;
char *cancellation_multiplication_factor = 0;
char *max_cancellation_struct_size_str = 0;
int max_cancellation_struct_size;
#endif

void kill_threads()
{
	int ii;
	for (ii = 0; ii < thread_number; ii++) {
		if (pthread_kill(thread_cont_list[ii], SIGUSR1) != 0)
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on pthread_kill - Thread: %d\n", ii);
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All worker threads have been closed\n");

#ifdef UPDATE_CANCELLATION_SUPPORT
	if (pthread_kill(update_thread_tid, SIGUSR1) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on pthread_kill - Update thread\n");
	else
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Updater thread has been closed\n");
	if (pthread_kill(delete_thread_tid, SIGUSR1) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on pthread_kill - Delete thread\n");
	else
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Deleter thread has been closed\n");
#endif
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

#ifdef UPDATE_CANCELLATION_SUPPORT
	if (pthread_join(update_thread_tid, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error join thread %d\n", ii);

	if (pthread_join(delete_thread_tid, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error join thread %d\n", ii);

	// REMOVE PROCESS ENTRIES FROM CENTRALIZED DB
	amqp_connection_state_t publish_remove_entries_conn;
	rabbitmq_publish_connection(&publish_remove_entries_conn, default_channel, master_hostname, master_port, username, password, db_manager_queue_name);

	char *delete_message = 0;
	create_update_message(nodename, port, "0", "0", delete_queue_name, process_pid, SHUTDOWN_MODE, &delete_message);

	// SEND MESSAGE TO DB MANAGER QUEUE
	int status = amqp_basic_publish(publish_remove_entries_conn,
					default_channel,
					amqp_cstring_bytes(""),
					amqp_cstring_bytes(db_manager_queue_name),
					0,	// mandatory (message must be routed to a queue)
					0,	// immediate (message must be delivered to a consumer immediately)
					&props,	// properties
					amqp_cstring_bytes(delete_message));

	if (status == AMQP_STATUS_OK) {
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", db_manager_queue_name, delete_message);
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Removed all process entries from centralized database\n");
	} else
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", db_manager_queue_name);

	if (delete_message)
		free(delete_message);

	char *delete_queue_from_rabbitmq = 0;

	int neededSize = snprintf(NULL, 0, "rabbitmqctl list_queues | awk '{ print $1 }' | grep \"%s_%s\" | " "xargs -L1 rabbitmqctl delete_queue > /dev/null", nodename, port);
	delete_queue_from_rabbitmq = (char *) malloc(neededSize + 1);
	snprintf(delete_queue_from_rabbitmq, neededSize + 1, "rabbitmqctl list_queues | awk '{ print $1 }' | grep \"%s_%s\" | " "xargs -L1 rabbitmqctl delete_queue > /dev/null", nodename, port);

	int systemRes = system(delete_queue_from_rabbitmq);
	if (systemRes != -1)
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All consumer queues have been deleted\n");

	if (delete_queue_from_rabbitmq)
		free(delete_queue_from_rabbitmq);

	pthread_rwlock_destroy(&struct_size_lock);
	pthread_rwlock_destroy(&canc_struct_lock);
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All locks have been destroyed\n");

	close_rabbitmq_connection(publish_db_conn, default_channel);
	close_rabbitmq_connection(publish_delete_conn, default_channel);
	close_rabbitmq_connection(publish_delete_update_conn, default_channel);
	close_rabbitmq_connection(publish_remove_entries_conn, default_channel);
	close_rabbitmq_connection(consume_updater_conn, default_channel);
	close_rabbitmq_connection(consume_delete_conn, default_channel);

	for (ii = 0; ii < thread_number; ii++) {
		pthread_rwlock_destroy(&thread_lock_list[ii]);
		close_rabbitmq_connection(conn_thread_publish_list[ii], (amqp_channel_t) (ii + 1));
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All RabbitMQ connections for cancellation system have been closed (%d connections)\n", ii + 6);
#endif

	for (ii = 0; ii < thread_number; ii++)
		close_rabbitmq_connection(conn_thread_consume_list[ii], (amqp_channel_t) (ii + 1));
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All RabbitMQ connections for consume system have been closed (%d connections)\n", ii);

	if (oph_server_conf_unload(&hashtbl) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on oph_server_conf_unload\n");

	free(pthread_create_arg);
	free(conn_thread_consume_list);
	free(thread_cont_list);

#ifdef UPDATE_CANCELLATION_SUPPORT
	free(shared_ids_array);
	free(PID_array);
	free(thread_lock_list);
	free(conn_thread_publish_list);
	free(canc_struct_list);
#endif

	for (ii = 0; ii < ptr_list_size; ii++) {
		if (ptr_list[ii])
			free(ptr_list[ii]);
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "All memory allocations have been released\n");

	if (pthread_mutex_destroy(&global_flag) != 0)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on mutex destroy\n");
	if (pthread_mutex_destroy(&ncores_mutex) != 0)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on mutex destroy\n");
	if (pthread_cond_destroy(&cond_var) != 0)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on condition variable destroy\n");

	exit(0);
}

#ifdef UPDATE_CANCELLATION_SUPPORT
int process_message(amqp_envelope_t full_message, int thread_param, amqp_channel_t channel)
{
#else
int process_message(amqp_envelope_t full_message)
{
#endif

	char *message = (char *) malloc(full_message.message.body.len + 1);
	strncpy(message, (char *) full_message.message.body.bytes, full_message.message.body.len);
	message[full_message.message.body.len] = 0;

	char *current = 0, *next = 0;

	if (split_by_delimiter(message, '*', 3, &current, &next) != 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to split by delimiter\n");
		return 1;
	}

	int neededSize = snprintf(NULL, 0, "%s", current);
	char *submission_string = (char *) malloc(neededSize + 1);
	snprintf(submission_string, neededSize + 1, "%s", current);

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "SUBMISSION_STRING: %s\n", submission_string);

	if (split_by_delimiter(next, '*', 3, &current, &next) != 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to split by delimiter\n");
		return 1;
	}

	neededSize = snprintf(NULL, 0, "%s", current);
	char *workflow_id = (char *) malloc(neededSize + 1);
	snprintf(workflow_id, neededSize + 1, "%s", current);

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "WORKFLOW_ID: %s\n", workflow_id);

	if (split_by_delimiter(next, '*', 3, &current, &next) != 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to split by delimiter\n");
		return 1;
	}

	neededSize = snprintf(NULL, 0, "%s", current);
	char *job_id = (char *) malloc(neededSize + 1);
	snprintf(job_id, neededSize + 1, "%s", current);

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "JOB_ID: %s\n", job_id);

	neededSize = snprintf(NULL, 0, "%s", next);
	char *ncores = (char *) malloc(neededSize + 1);
	snprintf(ncores, neededSize + 1, "%s", next);

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "NCORES: %s\n", ncores);

#ifdef UPDATE_CANCELLATION_SUPPORT
	pthread_rwlock_rdlock(&struct_size_lock);
	int list_size = cancellation_struct_size;
	pthread_rwlock_unlock(&struct_size_lock);

	int ii;
	for (ii = 0; ii < list_size; ii++) {
		pthread_rwlock_rdlock(&canc_struct_lock);
		int w_id = canc_struct_list[ii].id;
		pthread_rwlock_unlock(&canc_struct_lock);

		if (w_id != 0 && w_id == atoi(workflow_id)) {
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been discarded\n");

			if (message)
				free(message);
			if (submission_string)
				free(submission_string);
			if (workflow_id)
				free(workflow_id);
			if (job_id)
				free(job_id);
			if (ncores)
				free(ncores);

			return 1;
		}
	}

	// WRITE ON UPDATE_QUEUE AND SHARED ARRAY
	char *update_message = 0;
	create_update_message(nodename, port, workflow_id, job_id, delete_queue_name, process_pid, INSERT_JOB_MODE, &update_message);
	pthread_rwlock_rdlock(&thread_lock_list[thread_param]);
	shared_ids_array[thread_param] = atoi(workflow_id);
	pthread_rwlock_unlock(&thread_lock_list[thread_param]);

	int status = amqp_basic_publish(conn_thread_publish_list[thread_param],
					channel,
					amqp_cstring_bytes(""),
					amqp_cstring_bytes(update_queue_name),
					0,	// mandatory (message must be routed to a queue)
					0,	// immediate (message must be delivered to a consumer immediately)
					&props,	// properties
					amqp_cstring_bytes(update_message));

	if (status == AMQP_STATUS_OK)
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", update_queue_name, update_message);
	else
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", update_queue_name);

	if (update_message)
		free(update_message);
#endif

	int process_ncores = atoi(ncores);
	if (process_ncores > atoi(max_ncores)) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "This consumer process does not support job processing that require more than %s cores\n", max_ncores);

#ifdef UPDATE_CANCELLATION_SUPPORT

		char *update_message_2 = 0;
		create_update_message(nodename, port, workflow_id, job_id, delete_queue_name, process_pid, REMOVE_JOB_MODE, &update_message_2);
		pthread_rwlock_rdlock(&thread_lock_list[thread_param]);
		shared_ids_array[thread_param] = 0;
		pthread_rwlock_unlock(&thread_lock_list[thread_param]);

		status = amqp_basic_publish(conn_thread_publish_list[thread_param], channel, amqp_cstring_bytes(""), amqp_cstring_bytes(update_queue_name), 0,	// mandatory (message must be routed to a queue)
					    0,	// immediate (message must be delivered to a consumer immediately)
					    &props,	// properties
					    amqp_cstring_bytes(update_message_2));

		if (status == AMQP_STATUS_OK)
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", update_queue_name, update_message_2);
		else
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", update_queue_name);

		if (update_message_2)
			free(update_message_2);
#endif

		if (message)
			free(message);
		if (submission_string)
			free(submission_string);
		if (workflow_id)
			free(workflow_id);
		if (job_id)
			free(job_id);
		if (ncores)
			free(ncores);

		return 0;
	}

	char *exec_string = 0;

	if (strcmp(worker_launcher, "auto") != 0) {
		int neededSize = snprintf(NULL, 0, "%s -n %s %s %s", worker_launcher, ncores, framework_path, submission_string);
		exec_string = (char *) malloc(neededSize + 1);
		snprintf(exec_string, neededSize + 1, "%s -n %s %s %s", worker_launcher, ncores, framework_path, submission_string);
	} else {
		submission_string++;
		submission_string[strlen(submission_string) - 1] = 0;
	}

	pthread_mutex_lock(&ncores_mutex);
	while (process_ncores + total_used_ncores > atoi(max_ncores))
		pthread_cond_wait(&cond_var, &ncores_mutex);

	total_used_ncores += process_ncores;
	pthread_mutex_unlock(&ncores_mutex);

#ifdef UPDATE_CANCELLATION_SUPPORT
	pthread_rwlock_wrlock(&thread_lock_list[thread_param]);
	int exec_pid = fork();
	PID_array[thread_param] = exec_pid;
	pthread_rwlock_unlock(&thread_lock_list[thread_param]);
#else
	int exec_pid = fork();
#endif

	if (exec_pid < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on fork\n");
		exit(0);
	}
	if (exec_pid == 0) {
		if (strcmp(worker_launcher, "auto") != 0) {
			int systemRes = system(exec_string);
			if (systemRes == -1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on system: %s\n", exec_string);
				exit(0);
			} else
				exit(1);
		} else {
			if (oph_af_execute_framework(submission_string, 1, 0) != 0)
				exit(1);
			else
				exit(0);
		}
	}

	int pid_status;
	waitpid(exec_pid, &pid_status, 0);
	if (WIFEXITED(pid_status) != 0) {
		if (WEXITSTATUS(pid_status) == 0)
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Framework execution successful!\n");
		else
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Framework execution failed! Error on processing task\n");
	} else
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Framework execution failed! Child process crashed\n");

	pthread_mutex_lock(&ncores_mutex);
	total_used_ncores -= process_ncores;
	pthread_cond_signal(&cond_var);
	pthread_mutex_unlock(&ncores_mutex);

#ifdef UPDATE_CANCELLATION_SUPPORT
	// WRITE ON UPDATE_QUEUE AND SHARED ARRAY
	char *update_message_3 = 0;
	create_update_message(nodename, port, workflow_id, job_id, delete_queue_name, process_pid, REMOVE_JOB_MODE, &update_message_3);
	pthread_rwlock_rdlock(&thread_lock_list[thread_param]);
	shared_ids_array[thread_param] = 0;
	PID_array[thread_param] = 0;
	pthread_rwlock_unlock(&thread_lock_list[thread_param]);

	status = status = amqp_basic_publish(conn_thread_publish_list[thread_param], channel, amqp_cstring_bytes(""), amqp_cstring_bytes(update_queue_name), 0,	// mandatory (message must be routed to a queue)
					     0,	// immediate (message must be delivered to a consumer immediately)
					     &props,	// properties
					     amqp_cstring_bytes(update_message_3));

	if (status == AMQP_STATUS_OK)
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", update_queue_name, update_message_3);
	else
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", update_queue_name);

	if (update_message_3)
		free(update_message_3);
#endif

	if (message)
		free(message);
	if (workflow_id)
		free(workflow_id);
	if (job_id)
		free(job_id);
	if (ncores)
		free(ncores);

	if (strcmp(worker_launcher, "auto") != 0) {
		if (exec_string)
			free(exec_string);
		if (submission_string)
			free(submission_string);
	} else {
		if (submission_string--)
			free(submission_string--);
	}

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

	int ack_res, nack_res, process_res;
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

#ifdef UPDATE_CANCELLATION_SUPPORT
		process_res = process_message(envelope, thread_param, thread_channel);
#else
		process_res = process_message(envelope);
#endif

#ifdef UPDATE_CANCELLATION_SUPPORT
		pthread_rwlock_rdlock(&struct_size_lock);
		int list_size = cancellation_struct_size;
		pthread_rwlock_unlock(&struct_size_lock);

		int ii;
		for (ii = 0; ii < list_size; ii++) {
			pthread_rwlock_rdlock(&canc_struct_lock);
			canc_struct_list[ii].consumed_messages += 1;
			pthread_rwlock_unlock(&canc_struct_lock);
		}
#endif

		if (process_res == 1) {
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

#ifdef UPDATE_CANCELLATION_SUPPORT
void *update_pthread_function()
{
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

	amqp_rpc_reply_t reply;
	amqp_envelope_t envelope;

	int ack_res, nack_res, status;

	rabbitmq_consume_connection(&consume_updater_conn, default_channel, hostname, port, username, password, update_queue_name, rabbitmq_direct_mode, 1);
	rabbitmq_publish_connection(&publish_db_conn, default_channel, master_hostname, master_port, username, password, db_manager_queue_name);

	while (consume_updater_conn) {
		amqp_maybe_release_buffers_on_channel(consume_updater_conn, default_channel);

		reply = amqp_consume_message(consume_updater_conn, &envelope, NULL, 0);
		// CONN, POINTER TO MESSAGE, TIMEOUT = NULL = BLOCKING, UNUSED FLAG

		if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
			amqp_destroy_envelope(&envelope);
			continue;
		} else
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been consumed - Updater thread\n");

		char *message = (char *) malloc(envelope.message.body.len + 1);
		snprintf(message, envelope.message.body.len + 1, "%s", (char *) envelope.message.body.bytes);

		// SEND MESSAGE TO DB MANAGER QUEUE
		status = amqp_basic_publish(publish_db_conn, default_channel, amqp_cstring_bytes(""), amqp_cstring_bytes(db_manager_queue_name), 0,	// mandatory (message must be routed to a queue)
					    0,	// immediate (message must be delivered to a consumer immediately)
					    &props,	// properties
					    amqp_cstring_bytes(message));

		if (status == AMQP_STATUS_OK)
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", db_manager_queue_name, message);
		else
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", db_manager_queue_name);

		// ACKNOWLEDGE MESSAGE
		ack_res = amqp_basic_ack(consume_updater_conn, default_channel, envelope.delivery_tag, 0);	// last param: if true, ack all messages up to this delivery tag, if false ack only this delivery tag
		if (ack_res != 0) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to ack message. Delivery tag: %d - Updater thread: %d\n", (int) envelope.delivery_tag);

			nack_res = amqp_basic_reject(consume_updater_conn, default_channel, envelope.delivery_tag, (amqp_boolean_t) 1);	// 1 To put the message back in the queue
			if (nack_res != 0) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to Nack message. Delivery tag: %d - Updater thread: %d\n", (int) envelope.delivery_tag);
				exit(0);
			} else
				pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been requeued from updater_thread\n");
		} else
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been acked - Updater thread\n");

		if (message)
			free(message);

		amqp_destroy_envelope(&envelope);
	}

	return (NULL);
}
#endif

#ifdef UPDATE_CANCELLATION_SUPPORT
int stop_thread_function(int position)
{
	pthread_rwlock_wrlock(&thread_lock_list[position]);
	int pid = PID_array[position];
	pthread_rwlock_unlock(&thread_lock_list[position]);

	if (pid != 0) {
		int neededSize = snprintf(NULL, 0, "ps -ef | grep \"%d\" | awk 'NR==2 {print $2}'", pid);
		char *get_mpi_pid = (char *) malloc(neededSize + 1);
		snprintf(get_mpi_pid, neededSize + 1, "ps -ef | grep \"%d\" | awk 'NR==2 {print $2}'", pid);

		FILE *fp;
		fp = popen(get_mpi_pid, "r");
		if (fp == NULL)
			exit(1);

		char *mpi_pid = (char *) malloc(10);
		if (fgets(mpi_pid, 10, fp) == NULL) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get mpi pid\n");
			release_main();

			exit(0);
		}
		pclose(fp);
		mpi_pid[strcspn(mpi_pid, "\n")] = 0;

		neededSize = snprintf(NULL, 0, "kill -9 %d", pid);
		char *kill_parent = (char *) malloc(neededSize + 1);
		snprintf(kill_parent, neededSize + 1, "kill -9 %d", pid);

		neededSize = snprintf(NULL, 0, "kill -9 %s", mpi_pid);
		char *kill_command = (char *) malloc(neededSize + 1);
		snprintf(kill_command, neededSize + 1, "kill -9 %s", mpi_pid);

		int systemRes;

		pthread_rwlock_wrlock(&thread_lock_list[position]);
		pid = PID_array[position];
		pthread_rwlock_unlock(&thread_lock_list[position]);

		if (pid != 0) {
			systemRes = system(kill_parent);
			if (systemRes == -1) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to kill pid %d!\n", pid);

				return 0;
			}

			systemRes = system(kill_command);
			if (systemRes == -1) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to kill pid %d!\n", mpi_pid);

				return 0;
			}
		}

		if (get_mpi_pid)
			free(get_mpi_pid);
		if (mpi_pid)
			free(mpi_pid);
		if (kill_parent)
			free(kill_parent);
		if (kill_command)
			free(kill_command);

		pthread_rwlock_wrlock(&thread_lock_list[position]);
		shared_ids_array[position] = 0;
		pthread_rwlock_unlock(&thread_lock_list[position]);

		return 1;
	}

	return 0;
}
#endif

#ifdef UPDATE_CANCELLATION_SUPPORT
int delete_routine()
{
	delete_routine_isup = 1;
	int ii, total_killed = 0;

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Delete routine has been started\n");

	pthread_rwlock_wrlock(&struct_size_lock);
	int list_size = cancellation_struct_size;
	pthread_rwlock_unlock(&struct_size_lock);

	do {
		for (ii = 0; ii < list_size; ii++) {
			pthread_rwlock_wrlock(&canc_struct_lock);
			int wid_tocancel = canc_struct_list[ii].id;
			int checks_todo = canc_struct_list[ii].checks_todo;
			int consumed_messages = canc_struct_list[ii].consumed_messages;
			pthread_rwlock_unlock(&canc_struct_lock);

			if (wid_tocancel != 0) {
				if (consumed_messages > checks_todo) {
					pthread_rwlock_wrlock(&canc_struct_lock);
					canc_struct_list[ii].id = 0;
					canc_struct_list[ii].checks_todo = 0;
					canc_struct_list[ii].consumed_messages = 0;
					pthread_rwlock_unlock(&canc_struct_lock);

					pthread_rwlock_wrlock(&struct_size_lock);
					cancellation_struct_size -= 1;
					pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Removed wid %d from cancellation structure\n", wid_tocancel);
					pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Total workflows processed by delete routine: %d\n", cancellation_struct_size);
					pthread_rwlock_unlock(&struct_size_lock);
				} else {
					int jj;
					for (jj = 0; jj < thread_number; jj++) {
						pthread_rwlock_wrlock(&thread_lock_list[jj]);
						int shared_id = shared_ids_array[jj];
						pthread_rwlock_unlock(&thread_lock_list[jj]);

						if (shared_id == wid_tocancel) {
							pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Job processing (WORKFLOW_ID: %d) is going to be stopped\n", wid_tocancel);

							total_killed += stop_thread_function(jj);

							pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Job processing (WORKFLOW_ID: %d) has been stopped\n", wid_tocancel);
						}
					}
				}
			}
		}

		pthread_rwlock_wrlock(&struct_size_lock);
		list_size = cancellation_struct_size;
		pthread_rwlock_unlock(&struct_size_lock);
	} while (list_size > 0);

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Delete routine is ended. Total job killed: %d\n", total_killed);

	delete_routine_isup = 0;
	return 0;
}
#endif

#ifdef UPDATE_CANCELLATION_SUPPORT
void *delete_pthread_function()
{
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

	amqp_rpc_reply_t reply;
	amqp_envelope_t envelope;

	int ack_res, nack_res;

	rabbitmq_consume_connection(&consume_delete_conn, default_channel, hostname, port, username, password, delete_queue_name, rabbitmq_direct_mode, 1);
	rabbitmq_publish_connection(&publish_delete_update_conn, default_channel, hostname, port, username, password, update_queue_name);
	rabbitmq_publish_connection(&publish_delete_conn, default_channel, hostname, port, username, password, delete_queue_name);

	while (consume_delete_conn) {
		amqp_maybe_release_buffers_on_channel(consume_delete_conn, default_channel);

		reply = amqp_consume_message(consume_delete_conn, &envelope, NULL, 0);
		// CONN, POINTER TO MESSAGE, TIMEOUT = NULL = BLOCKING, UNUSED FLAG

		if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
			amqp_destroy_envelope(&envelope);
			continue;
		} else
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been consumed - Deleter thread\n");

		int neededSize = snprintf(NULL, 0, "%s", (char *) envelope.message.body.bytes);
		char *message = (char *) malloc(neededSize + 1);
		snprintf(message, neededSize + 1, "%s", (char *) envelope.message.body.bytes);

		char *current = 0, *next = 0;

		if (split_by_delimiter(message, '*', 3, &current, &next) != 0) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to split by delimiter\n");
			continue;
		}

		neededSize = snprintf(NULL, 0, "%s", current);
		char *w_id = (char *) malloc(neededSize + 1);
		snprintf(w_id, neededSize + 1, "%s", current);

		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "WORKFLOW_ID: %s\n", w_id);

		neededSize = snprintf(NULL, 0, "%s", next);
		char *checks_todo = (char *) malloc(neededSize + 1);
		snprintf(checks_todo, neededSize + 1, "%s", next);

		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "CHECKS_TODO: %s\n", checks_todo);

		int ii, id;
		for (ii = 0; ii < max_cancellation_struct_size; ii++) {
			pthread_rwlock_wrlock(&canc_struct_lock);
			id = canc_struct_list[ii].id;
			pthread_rwlock_unlock(&canc_struct_lock);

			if (id == 0) {
				pthread_rwlock_wrlock(&canc_struct_lock);
				canc_struct_list[ii].id = atoi(w_id);
				canc_struct_list[ii].checks_todo = atoi(checks_todo);
				if (canc_struct_list[ii].checks_todo == 0)
					canc_struct_list[ii].checks_todo = atoi(cancellation_multiplication_factor);
				else
					canc_struct_list[ii].checks_todo *= atoi(cancellation_multiplication_factor);
				canc_struct_list[ii].consumed_messages = 0;
				pthread_rwlock_unlock(&canc_struct_lock);

				pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Added %d checks to delete routine for workflow %s\n", canc_struct_list[ii].checks_todo, w_id);

				pthread_rwlock_wrlock(&struct_size_lock);
				cancellation_struct_size += 1;
				pthread_rwlock_unlock(&struct_size_lock);

				if (!delete_routine_isup)
					delete_routine();

				break;
			}
		}

		// ACKNOWLEDGE MESSAGE
		ack_res = amqp_basic_ack(consume_delete_conn, default_channel, envelope.delivery_tag, 0);	// last param: if true, ack all messages up to this delivery tag, if false ack only this delivery tag
		if (ack_res != 0) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to ack message. Delivery tag: %d - Deleter thread: %d\n", (int) envelope.delivery_tag);

			nack_res = amqp_basic_reject(consume_delete_conn, default_channel, envelope.delivery_tag, (amqp_boolean_t) 1);	// 1 To put the message back in the queue
			if (nack_res != 0) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Failed to Nack message. Delivery tag: %d - Deleter thread: %d\n", (int) envelope.delivery_tag);
				exit(0);
			} else
				pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been requeued from deleter_thread\n");
		} else
			pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "A message has been acked - Deleter thread\n");

		amqp_destroy_envelope(&envelope);

		if (message)
			free(message);
		if (w_id)
			free(w_id);
		if (checks_todo)
			free(checks_todo);
	}

	return (NULL);
}
#endif

int main(int argc, char const *const *argv)
{
	int msglevel = LOG_INFO;
	set_debug_level(msglevel);
	set_log_prefix(OPH_ANALYTICS_LOCATION);

	(void) argv;

	int ch, neededSize = 0;

#ifdef UPDATE_CANCELLATION_SUPPORT
	static char *USAGE = "\nUSAGE:\nesdm_pav_runtime_worker [-d] [-c <config_file>] [-H <RabbitMQ hostname>] "
	    "[-P <RabbitMQ port>] [-Q <RabbitMQ task_queue>] [-U <RabbitMQ update_queue>] [-a <master hostname>] [-b <master port>] "
	    "[-M <RabbitMQ db_manager_queue>] [-D <RabbitMQ delete_queue>] [-u <RabbitMQ username>] [-p <RabbitMQ password>] "
	    "[-n <max_ncores>] [-m <cancellation_multiplication_factor>] [-s <cancellation_struct_size>] "
	    "[-t <thread_number>] [-l <worker_launcher>] [-f <framework_path>] [-h <USAGE>]\n";

	while ((ch = getopt(argc, (char *const *) argv, ":c:H:P:Q:U:a:b:M:D:u:p:n:m:s:C:t:l:f:dhxz")) != -1) {
#else
	static char *USAGE = "\nUSAGE:\nesdm_pav_runtime_worker [-d] [-c <config_file>] [-H <RabbitMQ hostname>] "
	    "[-P <RabbitMQ port>] [-Q <RabbitMQ task_queue>] [-a <master hostname>] [-b <master port>] [-u <RabbitMQ username>] "
	    "[-p <RabbitMQ password>] [-n <max_ncores>] [-t <thread_number>] [-l <worker_launcher>] [-f <framework_path>] [-h <USAGE>]\n";

	while ((ch = getopt(argc, (char *const *) argv, ":c:H:P:Q:a:b:u:p:n:t:l:f:dhxz")) != -1) {
#endif

		switch (ch) {
			case 'c':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				config_file = (char *) malloc(neededSize + 1);
				snprintf(config_file, neededSize + 1, "%s", optarg);
				ptr_list[0] = config_file;
				break;
			case 'H':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				hostname = (char *) malloc(neededSize + 1);
				snprintf(hostname, neededSize + 1, "%s", optarg);
				ptr_list[1] = hostname;
				break;
			case 'P':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				port = (char *) malloc(neededSize + 1);
				snprintf(port, neededSize + 1, "%s", optarg);
				ptr_list[2] = port;
				break;
			case 'Q':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				task_queue_name = (char *) malloc(neededSize + 1);
				snprintf(task_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[3] = task_queue_name;
				break;
#ifdef UPDATE_CANCELLATION_SUPPORT
			case 'U':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				update_queue_name = (char *) malloc(neededSize + 1);
				snprintf(update_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[4] = update_queue_name;
				break;
#endif
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
#ifdef UPDATE_CANCELLATION_SUPPORT
			case 'M':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				db_manager_queue_name = (char *) malloc(neededSize + 1);
				snprintf(db_manager_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[7] = db_manager_queue_name;
				break;
			case 'D':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				delete_queue_name = (char *) malloc(neededSize + 1);
				snprintf(delete_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[8] = delete_queue_name;
				break;
#endif
			case 'u':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				username = (char *) malloc(neededSize + 1);
				snprintf(username, neededSize + 1, "%s", optarg);
				ptr_list[9] = username;
				break;
			case 'p':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				password = (char *) malloc(neededSize + 1);
				snprintf(password, neededSize + 1, "%s", optarg);
				ptr_list[10] = password;
				break;
			case 'n':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				max_ncores = (char *) malloc(neededSize + 1);
				snprintf(max_ncores, neededSize + 1, "%s", optarg);
				ptr_list[11] = max_ncores;
				break;
#ifdef UPDATE_CANCELLATION_SUPPORT
			case 'm':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				cancellation_multiplication_factor = (char *) malloc(neededSize + 1);
				snprintf(cancellation_multiplication_factor, neededSize + 1, "%s", optarg);
				ptr_list[12] = cancellation_multiplication_factor;
				break;
			case 's':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				max_cancellation_struct_size_str = (char *) malloc(neededSize + 1);
				snprintf(max_cancellation_struct_size_str, neededSize + 1, "%s", optarg);
				ptr_list[13] = max_cancellation_struct_size_str;
				break;
#endif
			case 't':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				thread_number_str = (char *) malloc(neededSize + 1);
				snprintf(thread_number_str, neededSize + 1, "%s", optarg);
				ptr_list[14] = thread_number_str;
				break;
			case 'l':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				worker_launcher = (char *) malloc(neededSize + 1);
				snprintf(worker_launcher, neededSize + 1, "%s", optarg);
				ptr_list[15] = worker_launcher;
				break;
			case 'f':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				framework_path = (char *) malloc(neededSize + 1);
				snprintf(framework_path, neededSize + 1, "%s", optarg);
				ptr_list[16] = framework_path;
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

	if (pthread_mutex_init(&global_flag, NULL) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
		exit(0);
	}

	set_debug_level(msglevel);
	pmesg_safe(&global_flag, LOG_INFO, __FILE__, __LINE__, "Selected log level %d\n", msglevel);

	gethostname(nodename, 1024);

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

#ifdef UPDATE_CANCELLATION_SUPPORT
	char *update_queue = 0;
	char *delete_queue = 0;

	if (!update_queue_name) {
		if (oph_server_conf_get_param(hashtbl, "UPDATE_QUEUE_NAME", &update_queue)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get UPDATE_QUEUE_NAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		} else {
			neededSize = snprintf(NULL, 0, "%s_%s_%s", update_queue, nodename, port);
			update_queue_name = (char *) malloc(neededSize + 1);
			snprintf(update_queue_name, neededSize + 1, "%s_%s_%s", update_queue, nodename, port);
		}
	} else {
		neededSize = snprintf(NULL, 0, "%s_%s_%s", update_queue_name, nodename, port);
		update_queue = (char *) malloc(neededSize + 1);
		snprintf(update_queue, neededSize + 1, "%s_%s_%s", update_queue_name, nodename, port);

		strcpy(update_queue_name, update_queue);
	}

	ptr_list[17] = update_queue_name;
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM UPDATE_QUEUE_NAME: %s\n", update_queue_name);

	if (!delete_queue_name) {
		if (oph_server_conf_get_param(hashtbl, "DELETE_QUEUE_NAME", &delete_queue)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get DELETE_QUEUE_NAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		} else {
			neededSize = snprintf(NULL, 0, "%s_%s_%s", delete_queue, nodename, port);
			delete_queue_name = (char *) malloc(neededSize + 1);
			snprintf(delete_queue_name, neededSize + 1, "%s_%s_%s", delete_queue, nodename, port);
		}
	} else {
		neededSize = snprintf(NULL, 0, "%s_%s_%s", delete_queue_name, nodename, port);
		delete_queue = (char *) malloc(neededSize + 1);
		snprintf(delete_queue, neededSize + 1, "%s_%s_%s", delete_queue_name, nodename, port);

		strcpy(delete_queue_name, delete_queue);
	}

	ptr_list[18] = delete_queue_name;
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM DELETE_QUEUE_NAME: %s\n", delete_queue_name);
#endif

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

#ifdef UPDATE_CANCELLATION_SUPPORT
	if (!db_manager_queue_name) {
		if (oph_server_conf_get_param(hashtbl, "DBMANAGER_QUEUE_NAME", &db_manager_queue_name)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get DBMANAGER_QUEUE_NAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM DBMANAGER_QUEUE_NAME: %s\n", db_manager_queue_name);
#endif

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

	if (!worker_launcher) {
		if (oph_server_conf_get_param(hashtbl, "LAUNCHER", &worker_launcher)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get WORKER_LAUNCHER param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM WORKER_LAUNCHER: %s\n", worker_launcher);

	if (strcmp(worker_launcher, "auto") != 0) {
		if (!framework_path) {
			if (oph_server_conf_get_param(hashtbl, "FRAMEWORK_PATH", &framework_path)) {
				pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get FRAMEWORK_PATH param\n");
				oph_server_conf_unload(&hashtbl);
				exit(0);
			}
		}
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM FRAMEWORK_PATH: %s\n", framework_path);
	}
#ifdef UPDATE_CANCELLATION_SUPPORT
	if (!cancellation_multiplication_factor) {
		if (oph_server_conf_get_param(hashtbl, "CANCELLATION_MULTIPLICATION_FACTOR", &cancellation_multiplication_factor)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get CANCELLATION_MULTIPLICATION_FACTOR param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM CANCELLATION_MULTIPLICATION_FACTOR: %s\n", cancellation_multiplication_factor);

	if (!max_cancellation_struct_size_str) {
		if (oph_server_conf_get_param(hashtbl, "MAX_CANCELLATION_STRUCT_SIZE", &max_cancellation_struct_size_str)) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Unable to get MAX_CANCELLATION_STRUCT_SIZE param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MAX_CANCELLATION_STRUCT_SIZE: %s\n", max_cancellation_struct_size_str);
#endif

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = AMQP_DELIVERY_PERSISTENT;

#ifdef UPDATE_CANCELLATION_SUPPORT
	if (pthread_rwlock_init(&struct_size_lock, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
	if (pthread_rwlock_init(&canc_struct_lock, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
#endif

	if (pthread_mutex_init(&ncores_mutex, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
	if (pthread_cond_init(&cond_var, NULL) != 0)
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on condition variable init\n");

	sigemptyset(&signal_set);
	sigaddset(&signal_set, SIGUSR1);

	thread_number = atoi(thread_number_str);
	thread_cont_list = (pthread_t *) malloc(sizeof(pthread_t) * thread_number);
	pthread_create_arg = (int *) malloc(sizeof(int) * thread_number);
	conn_thread_consume_list = (amqp_connection_state_t *) malloc(sizeof(amqp_connection_state_t) * thread_number);

#ifdef UPDATE_CANCELLATION_SUPPORT
	max_cancellation_struct_size = atoi(max_cancellation_struct_size_str);
	shared_ids_array = (int *) malloc(sizeof(int) * thread_number);
	PID_array = (pid_t *) malloc(sizeof(pid_t) * thread_number);
	thread_lock_list = (pthread_rwlock_t *) malloc(sizeof(pthread_rwlock_t) * thread_number);
	conn_thread_publish_list = (amqp_connection_state_t *) malloc(sizeof(amqp_connection_state_t) * (thread_number + 1));
	canc_struct_list = (cancellation_struct *) malloc(sizeof(cancellation_struct) * max_cancellation_struct_size);
#endif

	int ii;
#ifdef UPDATE_CANCELLATION_SUPPORT
	for (ii = 0; ii < thread_number; ii++) {
		canc_struct_list[ii].id = 0;
		canc_struct_list[ii].checks_todo = 0;
		canc_struct_list[ii].consumed_messages = 0;
	}

	// UPDATER THREAD
	if (pthread_create(&update_thread_tid, NULL, &update_pthread_function, NULL) != 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error creating update thread %d\n");
		exit(0);
	}
	// DELETER THREAD
	if (pthread_create(&delete_thread_tid, NULL, &delete_pthread_function, NULL) != 0) {
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error creating delete thread %d\n");
		exit(0);
	}
#endif

	for (ii = 0; ii < thread_number; ii++) {
#ifdef UPDATE_CANCELLATION_SUPPORT
		shared_ids_array[ii] = 0;

		rabbitmq_publish_connection(&conn_thread_publish_list[ii], (amqp_channel_t) (ii + 1), hostname, port, username, password, update_queue_name);

		if (pthread_rwlock_init(&thread_lock_list[ii], NULL) != 0)
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error on mutex init\n");
#endif

		rabbitmq_consume_connection(&conn_thread_consume_list[ii], (amqp_channel_t) (ii + 1), master_hostname, master_port, username, password, task_queue_name, rabbitmq_direct_mode, 1);

		*pthread_create_arg = ii;
		if (pthread_create(&(thread_cont_list[ii]), NULL, &worker_pthread_function, (void *) pthread_create_arg) != 0) {
			pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Error creating thread %d\n", ii);
			exit(0);
		}
	}

	pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "%s worker threads have been started\n", thread_number_str);

#ifdef UPDATE_CANCELLATION_SUPPORT
	process_pid = (int) getpid();

	// WRITE ON UPDATE_QUEUE AND SHARED ARRAY --> SET STATUS UP ON JOB DB
	rabbitmq_publish_connection(&conn_thread_publish_list[thread_number], (amqp_channel_t) (thread_number + 1), hostname, port, username, password, update_queue_name);

	char *update_message = 0;
	create_update_message(nodename, port, "0", "0", delete_queue_name, process_pid, START_MODE, &update_message);

	int status = amqp_basic_publish(conn_thread_publish_list[thread_number],
					(amqp_channel_t) thread_number + 1,
					amqp_cstring_bytes(""),
					amqp_cstring_bytes(update_queue_name),
					0,	// mandatory (message must be routed to a queue)
					0,	// immediate (message must be delivered to a consumer immediately)
					&props,	// properties
					amqp_cstring_bytes(update_message));

	if (status == AMQP_STATUS_OK)
		pmesg_safe(&global_flag, LOG_DEBUG, __FILE__, __LINE__, "Message has been sent on %s: %s\n", update_queue_name, update_message);
	else
		pmesg_safe(&global_flag, LOG_ERROR, __FILE__, __LINE__, "Cannot send message to %s\n", update_queue_name);

	if (update_message)
		free(update_message);

	close_rabbitmq_connection(conn_thread_publish_list[thread_number], (amqp_channel_t) (thread_number + 1));
#endif

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
