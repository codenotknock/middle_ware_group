package delay;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.junit.Test;

import java.io.IOException;

/**
 * @author xiaofu
 * @date 2024/09/01
 * @program middle_ware_group
 * @description 延迟\定时消息的发送 delayTopic
 **/

public class MqDelayProducerTest {

    @Test
    public void test() throws ClientException, IOException {
        //1.连接到 rocketmq 服务器 proxy 组件
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("10.15.0.9:8081")
            .build();
        //定义主题
        String delayTopic = "DelayTopic";
        //2.创建 producer 生产者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Producer producer = provider.newProducerBuilder()
            .setTopics(delayTopic)
            .setClientConfiguration(clientConfiguration)
            .build();
        //3.构建消息并发送,发送消息
        //以下示例表示：延迟时间为10分钟之后的Unix时间戳。
        Long deliverTimeStamp = System.currentTimeMillis() + 1L * 60 * 1000;
        MessageBuilderImpl messageBuilder = new MessageBuilderImpl();
        Message message = messageBuilder.setTopic(delayTopic)
            .setKeys("MessageKey")
            .setTag("MessageTag")
            .setBody("Hello Delay Message".getBytes())
            .setDeliveryTimestamp(1716130110L * 1000)  //定时时间戳 或 延时时间戳
            .build();

        SendReceipt sendReceipt = producer.send(message);
        System.out.println("messageId: " + sendReceipt.getMessageId());
        producer.close();
    }

}
