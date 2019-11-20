package com.example.demo;

import com.example.demo.util.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class OneSenderToManyConsumer {


    private final static String QUEUE_NAME = "test_queue_work";


    /**
     * 往RabbitMQ   中推送消息
     */
    @Test
    public void pushMessageToRabbitMQ() throws Exception {

        System.out.println("push to rabbit begining");
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = null;
        for (int i = 0; i < 100; i++) {
            message = "hello" + i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        }
        channel.close();
        connection.close();
        System.out.println("push to rabbit ending");

    }


    /**
     * 消费者一
     */
    @Test
    public void consumer() throws Exception {


        // CountDownLatch countDownLatch = new CountDownLatch(2);

        Runnable runnable = new Runnable() {
            final Connection connection2 = ConnectionUtil.getConnection();

            @Override
            public void run() {

                System.out.println(Thread.currentThread().getName() + "运行开始了---------------------------");

                Channel channel = null;
                try {
                    channel = connection2.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    //服务器一次发一条数据给消费者
                    channel.basicQos(1);
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(QUEUE_NAME, false, consumer);

                    while (true) {
                        Delivery delivery = consumer.nextDelivery();
                        byte[] body = delivery.getBody();
                        String message = new String(body);
                        String consumerName = Thread.currentThread().getName();
                        if ("consumerOne".equals(consumerName)) {
                            Thread.sleep(1000);
                        } else {
                            Thread.sleep(2000);
                        }
                        System.out.println(consumerName + "消费了消息：   " + message);
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);

                    }


                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (channel != null) {
                        try {
                            channel.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (connection2 != null) {
                        try {
                            connection2.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                }


            }
        };

        Thread consumerOne = new Thread(runnable);
        consumerOne.setName("consumerOne");
        Thread consumerTwo = new Thread(runnable);
        consumerTwo.setName("consumerTwo");
        consumerTwo.start();
        consumerOne.start();
        consumerOne.join();
        consumerTwo.join();
        //countDownLatch.await();
        System.out.println("主线程运行结束-------------");

    }


}
