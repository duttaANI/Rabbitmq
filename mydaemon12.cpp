#include <iostream>
#include <thread>
#include <amqp.h>
#include <amqp_tcp_socket.h>

const std::string QUEUE_NAME = "hello_queue";

void senderThread(amqp_connection_state_t conn) {
    // Open a channel
    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error opening channel in senderThread" << std::endl;
        return;
    }

    // Declare a queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(conn);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue in senderThread" << std::endl;
        return;
    }

    int counter = 0;
    while (counter < 5) {
        std::string message = "Hello, RabbitMQ! - " + std::to_string(counter);
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;  // Persistent message

        amqp_basic_publish(conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, &props,
                           amqp_cstring_bytes(message.c_str()));
        std::cout << "Sent: " << message << std::endl;

        counter++;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void receiverThread(amqp_connection_state_t conn) {
    // Open a channel
    amqp_channel_open(conn, 2);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error opening channel in receiverThread" << std::endl;
        return;
    }

    // Declare the same queue
    amqp_queue_declare(conn, 2, amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(conn);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue in receiverThread" << std::endl;
        return;
    }

    // Bind the queue
    amqp_queue_bind(conn, 2, amqp_cstring_bytes(QUEUE_NAME.c_str()), amqp_cstring_bytes(""), amqp_empty_table);
    amqp_rpc_reply_t bind_reply = amqp_get_rpc_reply(conn);
    if (bind_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error binding queue in receiverThread" << std::endl;
        return;
    }

    while (true) {
        amqp_basic_properties_t props;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t consume_reply = amqp_consume_message(conn, &envelope, nullptr, 0);

        if (consume_reply.reply_type == AMQP_RESPONSE_NORMAL) {
            std::string message = std::string(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
            std::cout << "Received: " << message << std::endl;
            amqp_destroy_envelope(&envelope);
        } else {
            std::cerr << "Error consuming message in receiverThread" << std::endl;
        }
    }
}

int main() {
    // Connect to RabbitMQ server
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    if (!socket) {
        std::cerr << "Could not create TCP socket" << std::endl;
        return 1;
    }

    amqp_rpc_reply_t login_reply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error logging in" << std::endl;
        return 1;
    }

    std::thread sender([conn]() { senderThread(conn); });
    std::thread receiver([conn]() { receiverThread(conn); });

    sender.join();
    receiver.join();

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_channel_close(conn, 2, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}

