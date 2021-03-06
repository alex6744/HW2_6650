package test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class send {
  private final static String QUEUE_NAME = "hello";
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("54.218.125.222");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("12345");
    for (int i = 0; i < 62; i++) {
      try (Connection connection = factory.newConnection();
          Channel channel = connection.createChannel()) {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World1";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
      }
    }
  }
}
