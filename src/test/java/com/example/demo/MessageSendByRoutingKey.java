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
 * 在发布的时候给消息设置routing key,不同的队列绑定到不同的
 */
public class MessageSendByRoutingKey {


    private final static String EXCHANGE_NAME = "test_exchange_direct";//声明exchange

    private final static String QUEEU_NAME1 = "test_queue_direct_1";//声明队列

    private final static String QUEEU_NAME2 = "test_queue_direct_2";


    @Test
    public void createMessage() throws IOException, TimeoutException {

        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");


        for (int i = 0; i < 10; i++) {
            String deleteMessage = "删除操作"+i;
            channel.basicPublish(EXCHANGE_NAME, "delete", null, deleteMessage.getBytes());
            System.out.println("[X] sent '" + deleteMessage + "'");
        }

        for (int i = 0; i < 10; i++) {
            String updateMessage = "更新操作"+i;
            channel.basicPublish(EXCHANGE_NAME, "update", null, updateMessage.getBytes());
            System.out.println("[X] sent '" + updateMessage + "'");
        }
        for (int i = 0; i < 10; i++) {
            String insertMessage = "插入操作"+i;
            channel.basicPublish(EXCHANGE_NAME, "insert", null, insertMessage.getBytes());
            System.out.println("[X] sent '" + insertMessage + "'");
        }

        channel.close();
        connection.close();


    }


    @Test
    public void consumer() throws InterruptedException {

        RoutConsumerRunnable routConsumerRunnableOne = new RoutConsumerRunnable(EXCHANGE_NAME, QUEEU_NAME1, new String[]{"update","delete"}, 1000l);
        RoutConsumerRunnable routConsumerRunnableTwo = new RoutConsumerRunnable(EXCHANGE_NAME, QUEEU_NAME2, new String[]{"insert","update","delete"}, 1000l);
        Thread consumerThreadOne = new Thread(routConsumerRunnableOne);
        consumerThreadOne.setName("consumerThreadOne");
        consumerThreadOne.start();
        Thread consumerThreadTwo = new Thread(routConsumerRunnableTwo);
        consumerThreadTwo.setName("consumerThreadTwo");
        consumerThreadTwo.start();
        consumerThreadOne.join();
        consumerThreadTwo.join();

    }



    class RoutConsumerRunnable implements Runnable {


        private String exchangeName;
        private String queueName;
        private String[] routKeys;
        private long sleepTime;


        public RoutConsumerRunnable(String exchangeName, String queueName, String[] routKeys, long sleepTime) {
            this.exchangeName = exchangeName;
            this.queueName = queueName;
            this.routKeys = routKeys;
            this.sleepTime = sleepTime;
        }

        @Override
        public void run() {

            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + "运行开始-------------");
            Connection connection = null;
            Channel channel = null;
            try {
                connection = ConnectionUtil.getConnection();
                channel = connection.createChannel();
                channel.queueDeclare(queueName, false, false, false, null);
                if (routKeys != null && routKeys.length > 0) {
                    for (int i = 0; i < routKeys.length; i++) {
                        channel.queueBind(queueName, exchangeName, routKeys[i]);
                    }

                }
                //同一时刻服务器只会发一条消息给消费者
                channel.basicQos(1);
                QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
                channel.basicConsume(queueName, false, queueingConsumer);
                while (true) {

                    Delivery delivery = queueingConsumer.nextDelivery();
                    byte[] body = delivery.getBody();
                    String message = new String(body);
                    System.out.println(threadName + "消费了：" + message);
                    Thread.sleep(sleepTime);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                }


            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                if(channel!=null){
                    try {
                        channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                if(connection!=null){
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
