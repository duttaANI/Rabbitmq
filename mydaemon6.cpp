#include <iostream>
#include <thread>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <functional> // Include this header for std::ref

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
    std::cout << "starting senderThread: " << std::endl;
    // Declare exchange
    amqp_exchange_declare(conn, 1, amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                          amqp_cstring_bytes("direct"), 0, 0, 0, 0, amqp_empty_table);
    amqp_rpc_reply_t exchange_reply = amqp_get_rpc_reply(conn);
    if (exchange_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring exchange senderThread" << std::endl;
        return;
    }

    int counter = 2;
    while (counter--) {
        // Send message
        std::string message = "Hello, RabbitMQ!";
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;  // Persistent message

        amqp_basic_publish(conn, 1, amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                           amqp_cstring_bytes(ROUTING_KEY.c_str()), 0, 0, &props,
                           amqp_cstring_bytes(message.c_str()));
        std::cout << "Sent: " << message << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "exiting senderThread: " << std::endl;
}
// Add a function to reconnect to RabbitMQ and reopen the channel
void reconnect(amqp_connection_state_t& conn) {
    // ... (reconnection logic)
    //amqp_connection_state_t conn = amqp_new_connection();
    std::cout << "starting reconnect: " << std::endl;
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);

    // Create channel
    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error opening channel receiverThread" << std::endl;
        return;
    }
}
void receiverThread(amqp_connection_state_t conn) {
    std::cout << "starting recieverThread: " << std::endl;

    // Declare queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()), 0, 0, 0, 1,
                       amqp_empty_table);
    amqp_rpc_reply_t queue_reply = amqp_get_rpc_reply(conn);
    if (queue_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error declaring queue receiverThread" << std::endl;
        return;
    }
    std::cout << "queue declared recieverThread: " << std::endl;

    // Bind queue to exchange
    amqp_queue_bind(conn, 1, amqp_cstring_bytes(QUEUE_NAME.c_str()),
                    amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
                    amqp_cstring_bytes(ROUTING_KEY.c_str()), amqp_empty_table);
    amqp_rpc_reply_t bind_reply = amqp_get_rpc_reply(conn);
    if (bind_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error binding queue to exchange receiverThread" << std::endl;
        return;
    }
    std::cout << "queue bound recieverThread: " << std::endl;


    int counter = 2;
    while (counter--) {
            std::cout << "counter recieverThread: " << counter << std::endl;
        // Receive message
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t consume_reply = amqp_consume_message(conn, &envelope, nullptr, 0);
        if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
            std::cout << "Error consuming message receiverThread: " << consume_reply.reply_type << std::endl;
            if (consume_reply.reply_type == AMQP_CHANNEL_CLOSE_METHOD) {
                    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
                    std::cout << "Channel closed by server. Attempting to reopen." << std::endl;
                    reconnect(conn);
                    continue;
            }
            continue;
        }

        //std::string message(
            //static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        std::string message(
                        static_cast<const char*>(envelope.message.body.bytes),
                        static_cast<const char*>(envelope.message.body.bytes) + envelope.message.body.len
                        );
        std::cout << "Received: " << message << std::endl;

        amqp_destroy_envelope(&envelope);
    }

    std::cout << "exiting recieverThread: " << std::endl;
}
int main() {

        // Connect to RabbitMQ server
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

        amqp_channel_open(conn, 1);
        amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn);
        if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
                std::cerr << "Error opening channel main" << std::endl;
                return 1;
        }

        // Print queue information
        printQueueInfo(conn, QUEUE_NAME);

        // Create sender and receiver threads using lambda functions
        std::thread sender([conn]() { senderThread(conn); });
        std::thread receiver([conn]() { receiverThread(conn); });

        // Wait for threads to finish
        sender.join();
        receiver.join();
                 // Close the connection
        amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);

        return 0;
}
