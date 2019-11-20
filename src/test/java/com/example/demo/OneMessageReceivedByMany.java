package com.example.demo;


import com.example.demo.util.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 1、1个生产者，多个消费者
 * 2、每一个消费者都有自己的一个队列
 * 3、生产者没有将消息直接发送到队列，而是发送到了交换机
 * 4、每个队列都要绑定到交换机
 * 5、生产者发送的消息，经过交换机，到达队列，实现，一个消息被多个消费者获取的目的
 * 注意：一个消费者队列可以有多个消费者实例，只有其中一个消费者实例会消费
 */
public class OneMessageReceivedByMany {


    private final static String EXCHANGENAME = "test_exchange_fanout"; //exchange name
    private final static String QUEUE_NAME1 = "test_queue_work1"; //queue name2
    private final static String QUEUE_NAME2 = "test_queue_work2";//queue name2


    @Test
    public void createMessage() throws Exception {

        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明exchange
        channel.exchangeDeclare(EXCHANGENAME, "fanout");

        String message = null;
        for (int i = 0; i < 100; i++) {
            message = "hello world" + i;
            channel.basicPublish(EXCHANGENAME, "", null, message.getBytes());
            System.out.println("[x] sent '" + message + "'");
        }
        channel.close();
        connection.close();

    }

    @Test
    public void consumer() throws InterruptedException {

        Thread consumerTwo = new Thread(new ConsumerRunnable(QUEUE_NAME2, EXCHANGENAME, 1000L));
        consumerTwo.setName("consumerTwo");
        consumerTwo.start();

        Thread consumerOne = new Thread(new ConsumerRunnable(QUEUE_NAME1, EXCHANGENAME, 1000L));
        consumerOne.setName("consumerOne");
        consumerOne.start();
        consumerOne.join();
        consumerTwo.join();

        System.out.println("主线程运行结束----------------");

    }


    class ConsumerRunnable implements Runnable {

        private String queueName;
        private String exchangeName;
        private long sleepTime;

        public ConsumerRunnable(String queueName, String exchangeName, long sleepTime) {
            this.queueName = queueName;
            this.exchangeName = exchangeName;
            this.sleepTime = sleepTime;
        }

        @Override
        public void run() {

            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + "线程运行开始了");
            Connection connection = null;
            Channel channel = null;
            try {
                connection = ConnectionUtil.getConnection();
                channel = connection.createChannel();
                channel.queueDeclare(this.queueName, false, false, false, null);
                channel.queueBind(this.queueName, this.exchangeName, "");
                channel.basicQos(1);
                QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
                channel.basicConsume(this.queueName, false, queueingConsumer);
                while (true) {

                    Delivery delivery = queueingConsumer.nextDelivery();
                    byte[] body = delivery.getBody();
                    String message = new String(body);
                    System.out.println(threadName + "收到：" + message);
                    Thread.sleep(this.sleepTime);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
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
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }

            }


        }


    }


}
