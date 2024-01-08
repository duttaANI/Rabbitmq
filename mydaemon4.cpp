#include <iostream>
#include <thread>
#include <amqp.h>
#include <amqp_tcp_socket.h>

const std::string QUEUE_NAME = "my_queue";

void producerThread(amqp_connection_state_t conn) {
    // Declare a queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t declare_reply = amqp_get_rpc_reply(conn);

    if (declare_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue" << std::endl;
        return;
    }

    // Produce messages
    for (int i = 0; i < 5; ++i) {
        std::string message = "Message " + std::to_string(i);
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;  // Persistent message

        amqp_basic_publish(conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, &props,
                           amqp_cstring_bytes(message.c_str()));
        std::cout << "Sent: " << message << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void consumerThread(amqp_connection_state_t conn) {
    // Declare the same queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t declare_reply = amqp_get_rpc_reply(conn);

    if (declare_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue" << std::endl;
        return;
    }

    // Consume messages
    amqp_basic_consume(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn);

    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error consuming queue" << std::endl;
        return;
    }

    while (true) {
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t ret = amqp_consume_message(conn, &envelope, nullptr, 0);

        if (ret.reply_type != AMQP_RESPONSE_NORMAL) {
            continue;
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        std::cout << "Received: " << message << std::endl;

        amqp_destroy_envelope(&envelope);
    }
}

int main() {
    // Connect to RabbitMQ server
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    // Create producer and consumer threads
    std::thread producer(producerThread, conn);
    std::thread consumer(consumerThread, conn);

    // Wait for threads to finish
    producer.join();
    consumer.join();

    // Close the connection
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}

