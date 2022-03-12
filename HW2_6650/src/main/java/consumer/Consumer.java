package consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import servlet.skier.PostResponse;

public class Consumer {
  private static Map<Integer, List<String>> map;
  private static int threadNum;
  private static int basicQos;
  private static String queueName="post";
  public static void main(String[] args) throws IOException, TimeoutException {
    map=new ConcurrentHashMap<>();
    threadNum=Integer.parseInt(args[0]);
    basicQos=Integer.parseInt(args[1]);
    System.out.println("threadNum:"+threadNum);
    System.out.println("basic:"+basicQos);
    ConnectionFactory factory=new ConnectionFactory();

    factory.setHost("54.188.16.96");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("12345");

    Connection connection=factory.newConnection();
    for(int i=0;i<threadNum;i++){
        Runnable thread=()->{
          try {
            Channel channel=connection.createChannel();
            channel.basicQos(basicQos);

            channel.queueDeclare(queueName,false,false,false,null);

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
              String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
              String[] parts=message.split(",");
              int key=Integer.parseInt(parts[3]);
              if(map.containsKey(key)){
                List<String> m=map.get(key);
                m.add(message);
              }else{
                List<String> m=new ArrayList<>();
                m.add(message);
                map.put(key,m);
              }
              System.out.println(" [x] Received '" + message + "'");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
          } catch (IOException e) {
            e.printStackTrace();
          }


        };
        new Thread(thread).start();
    }

  }


}
