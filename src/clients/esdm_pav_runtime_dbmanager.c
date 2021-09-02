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
#include <getopt.h>
#include <ctype.h>
#include <sqlite3.h>

#include "assert.h"

#include "debug.h"
#include "oph_server_confs.h"
#include "rabbitmq_utils.h"

#define OPH_SCHED_CONF_FILE OPH_WORKER_CONFIGURATION_FILE

#define UNUSED(x) (void)(x)

sqlite3 *db = NULL;
char *err_msg = 0;

int ptr_list_size = 8;
char *ptr_list[8];

HASHTBL *hashtbl;

amqp_connection_state_t conn;
amqp_channel_t channel = 1;

char *hostname = 0;
char *port = 0;
char *db_manager_queue_name = 0;
char *username = 0;
char *password = 0;
char *config_file = 0;
char *rabbitmq_direct_mode = "amq.direct";
char *db_location = 0;
char *global_ip_address = 0;

int neededSize = 0;

int isPresent = 0;

void release_main()
{
	sqlite3_close(db);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sqlite connection has been closed\n");

	neededSize = snprintf(NULL, 0, "rm -rf %s", db_location);
	char *delete_database = (char *) malloc(neededSize + 1);
	snprintf(delete_database, neededSize + 1, "rm -rf %s", db_location);

	int systemRes = system(delete_database);
	if (systemRes != -1)
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sqlite database located in %s has been deleted\n", db_location);

	if (delete_database)
		free(delete_database);

	close_rabbitmq_connection(conn, channel);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "RabbitMQ connection for consume on %s has been closed\n", db_manager_queue_name);

	if (oph_server_conf_unload(&hashtbl) != 0)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on oph_server_conf_unload\n");

	int ii;
	for (ii = 0; ii < ptr_list_size; ii++) {
		if (ptr_list[ii])
			free(ptr_list[ii]);
	}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "All memory allocations have been released\n");

	exit(0);
}

int select_where_callback(void *NotUsed, int argc, char **argv, char **azColName)
{
	UNUSED(NotUsed);
	UNUSED(azColName);

	int ii = 0;
	for (ii = 0; ii < argc; ii++) {
		if (argv[ii]) {
			isPresent = 1;
			return 0;
		}
	}

	return 1;
}

int update_database(amqp_envelope_t full_message)
{
	char *select_sql = 0;
	char *select_insert_sql = 0;

	char *message = (char *) malloc(full_message.message.body.len + 1);
	strncpy(message, (char *) full_message.message.body.bytes, full_message.message.body.len);
	message[full_message.message.body.len] = 0;

	char *ptr = NULL;
	char *ip_address = strtok_r(message, "***", &ptr);
	if (!ip_address) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read ip_address parameter\n");
		return 0;
	}
	char *port = strtok_r(NULL, "***", &ptr);
	if (!port) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read port parameter\n");
		return 0;
	}
	char *thread_number = strtok_r(NULL, "***", &ptr);
	if (!thread_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read thread_number parameter\n");
		return 0;
	}
	char *workflow_id = strtok_r(NULL, "***", &ptr);
	if (!workflow_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read workflow_id parameter\n");
		return 0;
	}
	char *job_id = strtok_r(NULL, "***", &ptr);
	if (!job_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read job_id parameter\n");
		return 0;
	}
	char *delete_queue_name = strtok_r(NULL, "***", &ptr);
	if (!delete_queue_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read delete_queue_name parameter\n");
		return 0;
	}

	if (strcmp(workflow_id, "-1") == 0 && strcmp(job_id, "-1") == 0) {	// REMOVE PROCESS/THREADS ENTRIES FROM CENTRALIZED DB
		neededSize = snprintf(NULL, 0, "DELETE FROM job_table WHERE ip_address = \"%s\" and port = \"%s\" and " "delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);
		char *delete_sql = (char *) malloc(neededSize + 1);
		snprintf(delete_sql, neededSize + 1, "DELETE FROM job_table WHERE ip_address = \"%s\" and port = \"%s\" and " "delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);

		if (sqlite3_exec(db, delete_sql, 0, 0, &err_msg) != SQLITE_OK) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on delete query: %s\n", err_msg);

			sqlite3_free(err_msg);
			sqlite3_close(db);

			return 0;
		} else
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Entries have been deleted for IP_ADDRESS: %s - PORT: %s - DELETE_QUEUE_NAME: %s\n", ip_address, port, delete_queue_name);

		if (delete_sql)
			free(delete_sql);
		if (message)
			free(message);

		return 1;
	}

	neededSize = snprintf(NULL, 0, "SELECT * FROM job_table WHERE ip_address = \"%s\" and port = \"%s\" and thread_number = %s and "
			      "delete_queue_name =  \"%s\";", ip_address, port, thread_number, delete_queue_name);
	select_sql = (char *) malloc(neededSize + 1);
	snprintf(select_sql, neededSize + 1, "SELECT * FROM job_table WHERE ip_address = \"%s\" and port = \"%s\" and thread_number = %s and "
		 "delete_queue_name =  \"%s\";", ip_address, port, thread_number, delete_queue_name);

	if (sqlite3_exec(db, select_sql, select_where_callback, 0, &err_msg) != SQLITE_OK) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);

		sqlite3_free(err_msg);
		sqlite3_close(db);

		if (message)
			free(message);
		if (select_sql)
			free(select_sql);

		exit(0);
	}

	if (isPresent == 0) {
		neededSize = snprintf(NULL, 0, "INSERT INTO job_table (ip_address, port, thread_number, workflow_id, job_id, "
				      "delete_queue_name) VALUES(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");", ip_address, port, thread_number, workflow_id, job_id, delete_queue_name);
		select_insert_sql = (char *) malloc(neededSize + 1);
		snprintf(select_insert_sql, neededSize + 1, "INSERT INTO job_table (ip_address, port, thread_number, workflow_id, job_id, "
			 "delete_queue_name) VALUES(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");", ip_address, port, thread_number, workflow_id, job_id, delete_queue_name);
	} else {
		neededSize = snprintf(NULL, 0, "UPDATE job_table SET workflow_id = \"%s\", job_id = \"%s\" WHERE ip_address = \"%s\" and "
				      "port = \"%s\" and thread_number = \"%s\" and delete_queue_name = \"%s\";", workflow_id, job_id, ip_address, port, thread_number, delete_queue_name);
		select_insert_sql = (char *) malloc(neededSize + 1);
		snprintf(select_insert_sql, neededSize + 1, "UPDATE job_table SET workflow_id = \"%s\", job_id = \"%s\" WHERE ip_address = \"%s\" and "
			 "port = \"%s\" and thread_number = \"%s\" and delete_queue_name = \"%s\";", workflow_id, job_id, ip_address, port, thread_number, delete_queue_name);

		isPresent = 0;
	}

	while (sqlite3_exec(db, select_insert_sql, 0, 0, &err_msg) != SQLITE_OK) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "SQL error on select/insert query: %s\n", err_msg);

		/*sqlite3_free(err_msg);
		   sqlite3_close(db);

		   exit (0); */
	}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated. IP_ADDRESS: %s - PORT: %s - THREAD_NUMBER: %s - "
	      "WORKFLOW_ID: %s - JOB ID: %s - DELETE_QUEUE_NAME: %s\n", ip_address, port, thread_number, workflow_id, job_id, delete_queue_name);

	if (select_sql)
		free(select_sql);
	if (select_insert_sql)
		free(select_insert_sql);
	if (message)
		free(message);

	return 1;
}

int main(int argc, char const *const *argv)
{
	int msglevel = LOG_INFO;
	set_debug_level(msglevel + 10);

	(void) argv;

	int ch, neededSize = 0;

	static char *USAGE = "\nUSAGE:\noph_db_manager [-d] [-c <config_file>] [-H <RabbitMQ hostname>] [-P <RabbitMQ port>] "
	    "[M <RabbitMQ db_manager_queue>] [-u <RabbitMQ username>] [-p <RabbitMQ password>] [-s <sqlite_db_path>] [-h <USAGE>]\n";

	while ((ch = getopt(argc, (char *const *) argv, ":c:H:P:M:u:p:s:dhxzv")) != -1) {
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
			case 'M':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				db_manager_queue_name = (char *) malloc(neededSize + 1);
				snprintf(db_manager_queue_name, neededSize + 1, "%s", optarg);
				ptr_list[3] = db_manager_queue_name;
				break;
			case 'u':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				username = (char *) malloc(neededSize + 1);
				snprintf(username, neededSize + 1, "%s", optarg);
				ptr_list[4] = username;
				break;
			case 'p':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				password = (char *) malloc(neededSize + 1);
				snprintf(password, neededSize + 1, "%s", optarg);
				ptr_list[5] = password;
				break;
			case 'l':
				neededSize = snprintf(NULL, 0, "%s", optarg);
				db_location = (char *) malloc(neededSize + 1);
				snprintf(db_location, neededSize + 1, "%s", optarg);
				ptr_list[6] = db_location;
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
			case 'v':
				fprintf(stdout, "%s", OPH_VERSION);
				exit(0);
			default:
				fprintf(stdout, "%s", USAGE);
				exit(0);
		}
	}

	set_debug_level(msglevel + 10);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Selected log level %d\n", msglevel);

	FILE *fp;
	fp = popen("hostname -I | awk '{print $1}'", "r");
	if (fp == NULL)
		exit(1);

	global_ip_address = (char *) malloc(16);
	if (fgets(global_ip_address, 16, fp) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get ip address\n");
		release_main();

		exit(0);
	}
	pclose(fp);
	global_ip_address[strcspn(global_ip_address, "\n")] = 0;
	ptr_list[7] = global_ip_address;

	short unsigned int instance = 0;

	if (!config_file) {
		neededSize = snprintf(NULL, 0, "%s", OPH_SCHED_CONF_FILE);
		config_file = (char *) malloc(neededSize + 1);
		snprintf(config_file, neededSize + 1, "%s", OPH_SCHED_CONF_FILE);
		ptr_list[0] = config_file;
	}

	if (oph_server_conf_load(instance, &hashtbl, config_file)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open configuration file\n");
		exit(0);
	}

	if (!hostname) {
		if (oph_server_conf_get_param(hashtbl, "MASTER_HOSTNAME", &hostname)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get MASTER_HOSTNAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MASTER_HOSTNAME: %s\n", hostname);

	if (!port) {
		if (oph_server_conf_get_param(hashtbl, "MASTER_PORT", &port)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get MASTER_PORT param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM MASTER_PORT: %s\n", port);

	if (!db_manager_queue_name) {
		if (oph_server_conf_get_param(hashtbl, "DBMANAGER_QUEUE_NAME", &db_manager_queue_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get DBMANAGER_QUEUE_NAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM DBMANAGER_QUEUE_NAME: %s\n", db_manager_queue_name);

	if (!username) {
		if (oph_server_conf_get_param(hashtbl, "RABBITMQ_USERNAME", &username)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get RABBITMQ_USERNAME param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM RABBITMQ_USERNAME: %s\n", username);

	if (!password) {
		if (oph_server_conf_get_param(hashtbl, "RABBITMQ_PASSWORD", &password)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get RABBITMQ_PASSWORD param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM RABBITMQ_PASSWORD: %s\n", password);

	if (!db_location) {
		if (oph_server_conf_get_param(hashtbl, "DATABASE_PATH", &db_location)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get DATABASE_PATH param\n");
			oph_server_conf_unload(&hashtbl);
			exit(0);
		}
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LOADED PARAM DATABASE_PATH: %s\n", db_location);

	struct sigaction new_act, old_act;

	new_act.sa_handler = release_main;
	sigemptyset(&new_act.sa_mask);
	new_act.sa_flags = 0;

	int res;
	if ((res = sigaction(SIGINT, &new_act, &old_act)) < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup signal\n");
		exit(0);
	}

	amqp_envelope_t envelope;
	amqp_rpc_reply_t reply;
	int ack_res, nack_res;

	// PREPARE RABBITMQ CONSUME CONNECTION
	rabbitmq_consume_connection(&conn, channel, hostname, port, username, password, db_manager_queue_name, rabbitmq_direct_mode, 0);

	if (sqlite3_open(db_location, &db) != SQLITE_OK) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot open database: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);

		exit(0);
	}

	char *init_sql = "CREATE TABLE IF NOT EXISTS job_table (id_task INTEGER PRIMARY KEY AUTOINCREMENT, "
	    "ip_address VARCHAR(15) NOT NULL, port VARCHAR(4) NOT NULL, thread_number INTEGER NOT NULL, "
	    "workflow_id INTEGER NOT NULL, job_id INTEGER NOT NULL, delete_queue_name VARCHAR(40) NOT NULL)";

	if (sqlite3_exec(db, init_sql, 0, 0, &err_msg) != SQLITE_OK) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on init query: %s\n", sqlite3_errmsg(db));

		sqlite3_free(err_msg);
		sqlite3_close(db);

		exit(0);
	}

	for (;;) {
		amqp_maybe_release_buffers_on_channel(conn, channel);

		reply = amqp_consume_message(conn, &envelope, NULL, 0);
		// CONN, POINTER TO MESSAGE, TIMEOUT = NULL = BLOCKING, UNUSED FLAG

		if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on consume message\n");

			exit(0);
		} else
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "A message has been consumed\n");

		if (update_database(envelope) == 1) {
			// ACKNOWLEDGE MESSAGE
			ack_res = amqp_basic_ack(conn, channel, envelope.delivery_tag, 0);	// last param: if true, ack all messages up to this delivery tag, if false ack only this delivery tag
			if (ack_res != 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to ack message. Delivery tag: %d - Updater thread: %d\n", (int) envelope.delivery_tag);

				nack_res = amqp_basic_reject(conn, channel, envelope.delivery_tag, (amqp_boolean_t) 1);	// 1 To put the message back in the queue
				if (nack_res != 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Failed to Nack message. Delivery tag: %d - Updater thread: %d\n", (int) envelope.delivery_tag);
					exit(0);
				} else
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "A message has been requeued\n");
			} else
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "A message has been acked\n");
		}

		amqp_destroy_envelope(&envelope);
	}

	exit(0);
}
