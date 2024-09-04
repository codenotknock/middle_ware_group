package order;

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
 * @date 2024/09/02
 * @program middle_ware_group
 * @description 用来测试发送顺序的消息 - 生产者
 **/

public class TestOrderProducer {


    @Test
    public void test() throws IOException, ClientException {
        //1.连接到 rocketmq 服务器 proxy 组件
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints("10.15.0.9:8081").build();
        //定义主题
        String fifoTopic = "FIFOTopic";
        //2.创建 producer 生产者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Producer producer = provider.newProducerBuilder().setTopics(fifoTopic).setClientConfiguration(clientConfiguration).build();


        for (int i = 0; i < 10; i++) {
            String group = i % 2 == 0 ? "AA" : "BB";
            //3.构建消息并发送,发送消息
            MessageBuilderImpl messageBuilder = new MessageBuilderImpl();
            Message message = messageBuilder.setTopic(fifoTopic)
                .setKeys("MessageKey")
                .setTag("MessageTag")
                .setBody(("Hello Delay Message " + group + " " + i).getBytes())
                .setMessageGroup(group)
                .build();
            SendReceipt sendReceipt = producer.send(message);
            System.out.println("messageId: " + sendReceipt.getMessageId());
        }

        producer.close();

    }
}
