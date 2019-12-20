package com.example.demo.rabbit;

import com.example.demo.util.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Sender {


    private final static String QUEUE_NAME = "q_test_01";

    public static void main(String[] args) throws IOException, TimeoutException {

        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        for (int i = 0; i < 100; i++) {
            String message = "hello world"+i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        }
        channel.close();
        connection.close();

    }


}
