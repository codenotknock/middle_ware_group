package base;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.junit.Test;

import java.util.UUID;

/**
 * @author xiaofu
 * @date 2024/09/01
 * @program middle_ware_group
 * @description rocketmq-client-java 发送消息
 **/

public class MqProducerTest {

    @Test
    public void testNormal() throws Exception {
        //0.创建一个主题 主题类型必须 Normal  ./mqadmin updateTopic -n 10.15.0.9:9876 -t baseTopic -c DefaultCluster -a +message.type=NORMAL
        //1.创建 producer 对象
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoint = "10.15.0.9:8081";  //4.x版本: nameserv 地址 10.15.0.9:9876  5.x版本: proxy 组件地址 8081
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "baseTopic";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
            .setTopics(topic)
            .setClientConfiguration(configuration)
            .build();

        //2.创建一个 msg 对象
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        String key = UUID.randomUUID().toString();
        System.out.println("key: " + key);
        Message message = messageBuilder.setTopic(topic)
            //设置消息索引键，可根据关键字精确查找某条消息。
            .setKeys(key)
            //设置消息Tag，用于消费端根据指定Tag过滤消息。
            .setTag("logs")
            //消息体。
            .setBody("hello world".getBytes())
            .build();

        //3.通过 producer 发送 msg
        try {
            //发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.println(sendReceipt.getMessageId());
        } catch (ClientException e) {
            e.printStackTrace();
        }
        //4.关闭生产者
        producer.close();

    }
}
