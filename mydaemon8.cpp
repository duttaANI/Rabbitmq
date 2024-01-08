#include <iostream>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <thread>
#include <chrono>

const std::string QUEUE_NAME = "my_queue";
const std::string EXCHANGE_NAME = "direct_exchange";
const std::string ROUTING_KEY = "my_routing_key";

void declareQueue(amqp_connection_state_t conn, const std::string& queueName) {
    // Create a channel
    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error opening channel" << std::endl;
        return;
    }

    // Declare queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queueName.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(conn);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue" << std::endl;
        return;
    }

    // Bind queue to exchange
    amqp_queue_bind(conn, 1, amqp_cstring_bytes(queueName.c_str()),
                    amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                    amqp_cstring_bytes(ROUTING_KEY.c_str()), amqp_empty_table);
    amqp_rpc_reply_t bind_reply = amqp_get_rpc_reply(conn);
    if (bind_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error binding queue to exchange" << std::endl;
        return;
    }
}

void senderThread() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    if (!socket) {
        std::cerr << "Could not create TCP socket for sender" << std::endl;
        return;
    }

    declareQueue(conn, QUEUE_NAME);

    // Declare exchange
    amqp_exchange_declare(conn, 1, amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                          amqp_cstring_bytes("direct"), 0, 0, 0, 0, amqp_empty_table);
    amqp_rpc_reply_t exchange_reply = amqp_get_rpc_reply(conn);
    if (exchange_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring exchange" << std::endl;
        return;
    }

    int counter = 1;
    while (counter <= 10) {
        // Send message
        std::string message = "Message " + std::to_string(counter);
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;  // Persistent message

        amqp_basic_publish(conn, 1, amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                           amqp_cstring_bytes(ROUTING_KEY.c_str()), 0, 0, &props,
                           amqp_cstring_bytes(message.c_str()));
        std::cout << "Sent: " << message << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(1));
        counter++;
    }

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
}

void receiverThread() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    if (!socket) {
        std::cerr << "Could not create TCP socket for receiver" << std::endl;
        return;
    }

    declareQueue(conn, QUEUE_NAME);

    int counter = 1;
    while (counter <= 10) {
        // Receive message
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t consume_reply = amqp_consume_message(conn, &envelope, nullptr, 0);
        if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
            std::cerr << "Error consuming message receiverThread: " << consume_reply.reply_type << std::endl;
            continue;
        }

        std::string message(
            static_cast<char*>(envelope.message.body.bytes), envelope.message.body.len);
        std::cout << "Received: " << message << std::endl;

        amqp_destroy_envelope(&envelope);
        counter++;
    }

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
}

int main() {
    amqp_connection_state_t mainConn = amqp_new_connection();
    amqp_socket_t* mainSocket = amqp_tcp_socket_new(mainConn);
    amqp_socket_open(mainSocket, "localhost", 5672);

    if (!mainSocket) {
        std::cerr << "Could not create TCP socket for main thread" << std::endl;
        return 1;
    }

    declareQueue(mainConn, QUEUE_NAME);

    std::thread sender(senderThread);
    std::thread receiver(receiverThread);

    sender.join();
    receiver.join();

    amqp_channel_close(mainConn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(mainConn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(mainConn);

    return 0;
}

