package com.example.demo.rabbit;

import com.example.demo.util.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Receiver {


    private final static String QUEUE_NAME = "q_test_01";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //定义消费者
        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME,true,queueingConsumer);

        //获取消息
        while(true){
            Delivery delivery = queueingConsumer.nextDelivery();
            byte[] body = delivery.getBody();
            String message = new String(body);
            System.out.println("[X] ' received '"+message+"'");
        }



    }

}
