#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <pthread.h>

const char *QUEUE_NAME = "hello_queue";

void *senderThread(void *conn) {
    amqp_connection_state_t connection = *((amqp_connection_state_t *)conn);

    amqp_channel_open(connection, 1);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(connection);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error opening channel in senderThread\n");
        pthread_exit(NULL);
    }

    amqp_queue_declare(connection, 1, amqp_cstring_bytes(QUEUE_NAME), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(connection);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error declaring queue in senderThread\n");
        pthread_exit(NULL);
    }

    int counter = 0;
    while (counter < 5) {
        char message[50];
        snprintf(message, sizeof(message), "Hello, RabbitMQ! - %d", counter);

        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2; // Persistent message

        amqp_basic_publish(connection, 1, amqp_empty_bytes, amqp_cstring_bytes(QUEUE_NAME), 0, 0, &props,
                           amqp_cstring_bytes(message));
        printf("Sent: %s\n", message);

        counter++;
        sleep(1);
    }

    pthread_exit(NULL);
}

void *receiverThread(void *conn) {
    amqp_connection_state_t connection = *((amqp_connection_state_t *)conn);

    amqp_channel_open(connection, 2);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(connection);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error opening channel in receiverThread\n");
        pthread_exit(NULL);
    }

    amqp_queue_declare(connection, 2, amqp_cstring_bytes(QUEUE_NAME), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(connection);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error declaring queue in receiverThread\n");
        pthread_exit(NULL);
    }

    amqp_basic_consume(connection, 2, amqp_cstring_bytes(QUEUE_NAME), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(connection);
    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error consuming message in receiverThread\n");
        pthread_exit(NULL);
    }
    while (1) {
	    amqp_envelope_t envelope;
	    amqp_maybe_release_buffers(connection);
	    amqp_rpc_reply_t consume_reply = amqp_consume_message(connection, &envelope, NULL, 0);

	    if (consume_reply.reply_type == AMQP_RESPONSE_NORMAL) {
		    char *message = (char *)malloc(envelope.message.body.len + 1);
		    memcpy(message, envelope.message.body.bytes, envelope.message.body.len);
		    message[envelope.message.body.len] = '\0';

		    printf("Received: %s\n", message);

		    free(message);
		    amqp_destroy_envelope(&envelope);
	    } else {
		    fprintf(stderr, "Error consuming message in receiverThread\n");
	    }
    }

    pthread_exit(NULL);
}

int main() {
    amqp_connection_state_t connection = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(connection);
    amqp_socket_open(socket, "localhost", 5672);

    if (!socket) {
        fprintf(stderr, "Could not create TCP socket\n");
        return 1;
    }

    amqp_rpc_reply_t login_reply = amqp_login(connection, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "Error logging in\n");
        return 1;
    }

    pthread_t senderThreadID, receiverThreadID;
    pthread_create(&senderThreadID, NULL, senderThread, (void *)&connection);
    pthread_create(&receiverThreadID, NULL, receiverThread, (void *)&connection);

    pthread_join(senderThreadID, NULL);
    pthread_join(receiverThreadID, NULL);

    amqp_channel_close(connection, 1, AMQP_REPLY_SUCCESS);
    amqp_channel_close(connection, 2, AMQP_REPLY_SUCCESS);
    amqp_connection_close(connection, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(connection);

    return 0;
}

