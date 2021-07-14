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

#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#define RABBITMQ_FAILURE -1
#define RABBITMQ_SUCCESS 1


/**
 * \brief			            Function to prepare RabbitMQ connection to consume messages
 * \param conn                  Connection
 * \param channel               Channel
 * \param hostname              Hostname on which RabbitMQ is located
 * \param port                  Port on which RabbitMQ is located
 * \param username              RabbitMQ username
 * \param password              RabbitMQ password
 * \param queue_name            RabbitMQ queue name
 * \param exchange              RabbitMQ exchange
 * \param rabbitmq_deliveries   Number consumptions at a time
 * \return                      Return 1 for success, 0 otherwise
 */
int rabbitmq_consume_connection(amqp_connection_state_t * conn, amqp_channel_t channel, char *hostname, char *port,
				char *username, char *password, char *queue_name, char *exchange, int rabbitmq_deliveries);

/**
 * \brief			            Function to prepare RabbitMQ connection to publish messages
 * \param conn                  Connection
 * \param channel               Channel
 * \param hostname              Hostname on which RabbitMQ is located
 * \param port                  Port on which RabbitMQ is located
 * \param username              RabbitMQ username
 * \param password              RabbitMQ password
 * \param queue_name            RabbitMQ queue name
 * \return                      Return 1 for success, 0 otherwise
 */
int rabbitmq_publish_connection(amqp_connection_state_t * conn, amqp_channel_t channel, char *hostname, char *port, char *username, char *password, char *queue_name);

/**
 * \brief			            Function to close RabbitMQ connection
 * \param conn                  Connection
 * \param channel               Channel
 * \return                      Return 1 for success, 0 otherwise
 */
int close_rabbitmq_connection(amqp_connection_state_t conn, amqp_channel_t channel);


/**
 * \brief			            Function to get number of queueu messages in a RabbitMQ queue
 * \param hostname              Hostname on which RabbitMQ is located
 * \param port                  Port on which RabbitMQ is located
 * \param username              RabbitMQ username
 * \param password              RabbitMQ password
 * \param queue_name            RabbitMQ queue name
 * \return                      Number of message in queue_name
 */
int get_number_queued_messages(char *hostname, char *port, char *username, char *password, char *queue_name);
