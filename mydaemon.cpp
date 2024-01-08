#include <iostream>
#include <thread>
#include <amqp.h>
#include <amqp_tcp_socket.h>

const std::string QUEUE_NAME = "hello_queue";
const std::string EXCHANGE_NAME = "hello_exchange";
const std::string ROUTING_KEY = "hello_routing_key";

std::string bytesToString(const amqp_bytes_t& bytes) {
    return std::string(static_cast<char*>(bytes.bytes), bytes.len);
}

std::string getQueueId(amqp_connection_state_t conn, const std::string& queueName) {
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queueName.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);

    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        amqp_queue_declare_ok_t* declare_ok = reinterpret_cast<amqp_queue_declare_ok_t*>(reply.reply.decoded);
        return bytesToString(declare_ok->queue);
    } else {
        std::cerr << "Error declaring queue" << std::endl;
        return "";
    }
}

int getMessageCount(amqp_connection_state_t conn, const std::string& queueName) {
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queueName.c_str()), 0, 0, 0, 1, amqp_empty_table);
    amqp_rpc_reply_t declare_reply = amqp_get_rpc_reply(conn);

    if (declare_reply.reply_type == AMQP_RESPONSE_NORMAL) {
        amqp_queue_declare_ok_t* declare_ok = reinterpret_cast<amqp_queue_declare_ok_t*>(declare_reply.reply.decoded);
        return static_cast<int>(declare_ok->message_count);
    } else {
        std::cerr << "Error declaring queue" << std::endl;
        return -1;
    }
}

void printQueueInfo(amqp_connection_state_t conn, const std::string& queueName) {
    std::string queueId = getQueueId(conn, queueName);
    int messageCount = getMessageCount(conn, queueName);

    if (!queueId.empty()) {
        std::cout << "Queue ID: " << queueId << std::endl;
        std::cout << "Number of messages in queue: " << messageCount << std::endl;
    }
}

void senderThread(amqp_connection_state_t conn) {
    // ... (unchanged)
}

void receiverThread(amqp_connection_state_t conn) {
    // ... (unchanged)
}

int main() {
    // Connect to RabbitMQ server
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    // ... (unchanged)

    // Print queue information
    printQueueInfo(conn, QUEUE_NAME);

    // Create sender and receiver threads
    std::thread sender(senderThread, conn);
    std::thread receiver(receiverThread, conn);

    // Wait for threads to finish
    sender.join();
    receiver.join();

    // Close the connection
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}

