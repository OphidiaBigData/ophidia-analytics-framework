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
int worker_id = -1;

int neededSize = 0;

void release_main()
{
	sqlite3_close(db);
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sqlite connection has been closed\n");

	if (close_rabbitmq_connection(conn, channel) != RABBITMQ_FAILURE)
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

	if(argv[0])
		worker_id = atoi(argv[0]);
	else
		worker_id = -1;

	return 0;
}

int check_ifpresent_callback(void *result, int argc, char **argv, char **azColName)
{
	UNUSED(azColName);

	int *res = (int *) result;

	if(argv[0])
		*res = atoi(argv[0]);

	return 0;
}

int update_database(amqp_envelope_t full_message)
{

	char *message = (char *) malloc(full_message.message.body.len + 1);
	strncpy(message, (char *) full_message.message.body.bytes, full_message.message.body.len);
	message[full_message.message.body.len] = 0;

	char *ptr = NULL;
	char *ip_address = strtok_r(message, "***", &ptr);
	if (!ip_address) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read ip_address parameter\n");
		return 1;
	}
	char *port = strtok_r(NULL, "***", &ptr);
	if (!port) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read port parameter\n");
		return 1;
	}
	char *workflow_id = strtok_r(NULL, "***", &ptr);
	if (!workflow_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read workflow_id parameter\n");
		return 1;
	}
	char *job_id = strtok_r(NULL, "***", &ptr);
	if (!job_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read job_id parameter\n");
		return 1;
	}
	char *delete_queue_name = strtok_r(NULL, "***", &ptr);
	if (!delete_queue_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read delete_queue_name parameter\n");
		return 1;
	}
	char *worker_pid = strtok_r(NULL, "***", &ptr);
	if (!worker_pid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read worker_pid parameter\n");
		return 1;
	}
	char *worker_count = strtok_r(NULL, "***", &ptr);
	if (!worker_count) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read worker_count parameter\n");
		return 1;
	}
	char *mode = strtok_r(NULL, "***", &ptr);
	if (!mode) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Fail to read mode parameter\n");
		return 1;
	}

	int mode_value = atoi(mode);

	switch (mode_value) {
		case INSERT_JOB_MODE: {	// INSERT NEW JOB ENTRY IN JOB TABLE
			neededSize = snprintf(NULL, 0, "SELECT id_worker FROM worker WHERE ip_address=\"%s\" and port=\"%s\" and "
				"delete_queue_name=\"%s\";", ip_address, port, delete_queue_name);
			char *get_id_worker_sql = (char *) malloc(neededSize + 1);
			snprintf(get_id_worker_sql, neededSize + 1, "SELECT id_worker FROM worker WHERE ip_address=\"%s\" and port=\"%s\" and "
				"delete_queue_name=\"%s\";", ip_address, port, delete_queue_name);

			while (sqlite3_exec(db, get_id_worker_sql, select_where_callback, 0, &err_msg) != SQLITE_OK)
				pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);
			free(get_id_worker_sql);

			if(worker_id != -1) {
				neededSize = snprintf(NULL, 0, "INSERT INTO job (workflow_id, job_id, id_worker) VALUES(\"%s\", \"%s\", \"%d\");", 
					workflow_id, job_id, worker_id);
				char *insert_into_sql = (char *) malloc(neededSize + 1);
				snprintf(insert_into_sql, neededSize + 1, "INSERT INTO job (workflow_id, job_id, id_worker) VALUES(\"%s\", \"%s\", \"%d\");", 
					workflow_id, job_id, worker_id);

				while (sqlite3_exec(db, insert_into_sql, 0, 0, &err_msg) != SQLITE_OK)
					pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);
				free(insert_into_sql);

				worker_id = -1;

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: inserted job in job table. WORKFLOW_ID: %s - JOB ID: %s\n", workflow_id, job_id);
			}

			break;
		}
		case REMOVE_JOB_MODE: {	// REMOVE JOB ENTRY FROM JOB TABLE
			neededSize = snprintf(NULL, 0, "DELETE FROM job WHERE workflow_id=\"%s\" and job_id=\"%s\";", workflow_id, job_id);
			char *delete_from_sql = (char *) malloc(neededSize + 1);
			snprintf(delete_from_sql, neededSize + 1, "DELETE FROM job WHERE workflow_id=\"%s\" and job_id=\"%s\";", workflow_id, job_id);

			while (sqlite3_exec(db, delete_from_sql, 0, 0, &err_msg) != SQLITE_OK)
				pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);
			free(delete_from_sql);

			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: removed job from job table. WORKFLOW_ID: %s - JOB ID: %s\n", workflow_id, job_id);

			break;
		}
		case START_MODE: { // SET WORKER STATUS TO UP OR ADD NEW WORKER IF NOT EXISTS
			int isPresent;

			neededSize = snprintf(NULL, 0, "SELECT COUNT(id_worker) FROM worker WHERE ip_address = \"%s\" and port = \"%s\" and "
				"delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);
			char *check_ifpresent_sql = (char *) malloc(neededSize + 1);
			snprintf(check_ifpresent_sql, neededSize + 1, "SELECT COUNT(id_worker) FROM worker WHERE ip_address = \"%s\" and port = "
				"\"%s\" and delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);

			while (sqlite3_exec(db, check_ifpresent_sql, check_ifpresent_callback, &isPresent, &err_msg) != SQLITE_OK)
				pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on delete query: %s\n", err_msg);
			free(check_ifpresent_sql);

			if(isPresent) {
				neededSize = snprintf(NULL, 0, "UPDATE worker SET status=\"up\", pid=%d, count=%d WHERE ip_address = \"%s\" and port = \"%s\" and "
					"delete_queue_name = \"%s\";", atoi(worker_pid), atoi(worker_count), ip_address, port, delete_queue_name);
				char *update_existing_worker_sql = (char *) malloc(neededSize + 1);
				snprintf(update_existing_worker_sql, neededSize + 1, "UPDATE worker SET status=\"up\", pid=%d, count=%d WHERE ip_address = \"%s\" and port = \"%s\" and "
					"delete_queue_name = \"%s\";", atoi(worker_pid), atoi(worker_count), ip_address, port, delete_queue_name);

				while (sqlite3_exec(db, update_existing_worker_sql, 0, 0, &err_msg) != SQLITE_OK)
					pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);
				free(update_existing_worker_sql);

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: set status \"up\" for worker. NODENAME: %s - PORT: %s "
					"- DELETE_QUEUE: %s\n", ip_address, port, delete_queue_name);
			} else {
				neededSize = snprintf(NULL, 0, "INSERT INTO worker (ip_address, port, delete_queue_name, status, pid, "
					"count) VALUES (\"%s\", \"%s\", \"%s\", \"up\", %d, %d);", ip_address, port, delete_queue_name, atoi(worker_pid), atoi(worker_count));
				char *insert_new_worker_sql = (char *) malloc(neededSize + 1);
				snprintf(insert_new_worker_sql, neededSize + 1, "INSERT INTO worker (ip_address, port, delete_queue_name, status, pid, "
					"count) VALUES (\"%s\", \"%s\", \"%s\", \"up\", %d, %d);", ip_address, port, delete_queue_name, atoi(worker_pid), atoi(worker_count));

				while (sqlite3_exec(db, insert_new_worker_sql, 0, 0, &err_msg) != SQLITE_OK)
					pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on select query: %s\n", err_msg);
				free(insert_new_worker_sql);

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: a new worker has been added. NODENAME: %s - PORT: %s "
					"- DELETE_QUEUE: %s\n", ip_address, port, delete_queue_name);
			}

			break;
		}
		case SHUTDOWN_MODE: { // REMOVE ALL WORKER JOB ENTRIES FROM JOB_TABLE AND SET WORKER STATUS=DOWN
			neededSize = snprintf(NULL, 0, "DELETE FROM job WHERE id_worker in (SELECT id_worker from worker WHERE "
				"ip_address = \"%s\" and port = \"%s\" and delete_queue_name = \"%s\");", ip_address, port, delete_queue_name);
			char *delete_sql = (char *) malloc(neededSize + 1);
			snprintf(delete_sql, neededSize + 1, "DELETE FROM job WHERE id_worker in (SELECT id_worker from worker WHERE "
				"ip_address = \"%s\" and port = \"%s\" and delete_queue_name = \"%s\");", ip_address, port, delete_queue_name);

			while (sqlite3_exec(db, delete_sql, 0, 0, &err_msg) != SQLITE_OK)
				pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on delete query: %s\n", err_msg);
			free(delete_sql);

			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: job entries has been removed for worker NODENAME: %s - PORT: %s "
				"- DELETE_QUEUE: %s\n", ip_address, port, delete_queue_name);

			neededSize = snprintf(NULL, 0, "UPDATE worker SET status=\"down\", pid=0, count=0 WHERE ip_address = \"%s\" and port = \"%s\" and "
				"delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);
			char *set_down_status_sql = (char *) malloc(neededSize + 1);
			snprintf(set_down_status_sql, neededSize + 1, "UPDATE worker SET status=\"down\", pid=0, count=0 WHERE ip_address = \"%s\" and port = \"%s\" and "
				"delete_queue_name = \"%s\";", ip_address, port, delete_queue_name);

			while (sqlite3_exec(db, set_down_status_sql, 0, 0, &err_msg) != SQLITE_OK)
				pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on delete query: %s\n", err_msg);
			free(set_down_status_sql);

			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database updated: set status \"down\" for worker NODENAME: %s - PORT: %s - "
				"DELETE_QUEUE: %s\n", ip_address, port, delete_queue_name);

			break;
		}
		default: {
			break;
		}
	}

	if (message)
		free(message);

	return 0;
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

	char *init_worker_sql = "CREATE TABLE IF NOT EXISTS worker (id_worker INTEGER PRIMARY KEY AUTOINCREMENT, "
	    "ip_address VARCHAR(15) NOT NULL, port VARCHAR(4) NOT NULL, delete_queue_name VARCHAR(40) NOT NULL, "
		"status VARCHAR(4) DEFAULT \"down\" NOT NULL, pid INTEGER DEFAULT 0, count INTEGER DEFAULT 0)";
	while (sqlite3_exec(db, init_worker_sql, 0, 0, &err_msg) != SQLITE_OK)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on init_worker_sql query: %s\n", sqlite3_errmsg(db));
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "The \"worker\" table has set correctly\n");

	char *init_job_sql = "CREATE TABLE IF NOT EXISTS job (id_task INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id INTEGER NOT NULL, "
		"job_id INTEGER NOT NULL, id_worker INTEGER NOT NULL, FOREIGN KEY (id_worker) REFERENCES worker (id_worker))";
	if (sqlite3_exec(db, init_job_sql, 0, 0, &err_msg) != SQLITE_OK)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on init_job_sql query: %s\n", sqlite3_errmsg(db));
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "The \"job\" table has set correctly\n");

	char *reset_worker_table_sql = "UPDATE worker SET pid=0, count=0, status=\"down\";";
	if (sqlite3_exec(db, reset_worker_table_sql, 0, 0, &err_msg) != SQLITE_OK)
		pmesg(LOG_ERROR, __FILE__, __LINE__, "SQL error on reset_worker_table_sql query: %s\n", sqlite3_errmsg(db));
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "The \"worker\" table has been reset correctly\n");

	for (;;) {
		amqp_maybe_release_buffers_on_channel(conn, channel);

		reply = amqp_consume_message(conn, &envelope, NULL, 0);
		// CONN, POINTER TO MESSAGE, TIMEOUT = NULL = BLOCKING, UNUSED FLAG

		if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on consume message\n");

			exit(0);
		} else
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "A message has been consumed\n");

		if (!update_database(envelope)) {
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
