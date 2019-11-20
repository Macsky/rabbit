package com.example.demo.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConnectionUtil {


    public static Connection getConnection() throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("test");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        Connection connection = connectionFactory.newConnection();
        return connection;
    }


    public static QueueingConsumer getConsumer(String QueueName) throws IOException, TimeoutException {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QueueName,false,false,false,null);
        //同一时刻服务器只会发送一条消息给消费者
        //channel.basicQos(1);
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        return queueingConsumer;
    }



}
