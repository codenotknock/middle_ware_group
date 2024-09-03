package order;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * @author xiaofu
 * @date 2024/09/02
 * @program middle_ware_group
 * @description 用来测试 Consumer 顺序消费
 **/

@Slf4j
public class TestOrderConsumer {

    /**
     * 测试 push Consumer 消费者
     * @throws ClientException
     * @throws InterruptedException
     */
    @Test
    public void testConsumer() throws ClientException, InterruptedException {
        //1.创建服务器配置对象   proxy 组件
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints("10.15.0.9:8081").build();
        //定义主题
        String fifoTopic = "FIFOTopic";
        //2.创建 push Consumer 对象
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup("FIFOGroup")
            .setSubscriptionExpressions(Collections.singletonMap(fifoTopic, filterExpression))
            .setMessageListener(messageView -> {
                // 处理消息并返回消费结果。
                String str = StandardCharsets.UTF_8.decode(messageView.getBody()).toString();

                log.info("Consume message successfully, messageId={},content={}", messageView.getMessageId(), str);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //根据消费结果返回状态。
                return ConsumeResult.SUCCESS;
                //消费消息
            }).build();

        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();
    }


    /**
     * 测试 Consumer 消费者  拉取顺序消费
     * @throws ClientException
     * @throws InterruptedException
     */
    @Test
    public void testSimpleConsumer() throws ClientException, InterruptedException {
        //创建配置对象
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("10.15.0.9:8081").build();

        //创建 simple consumer
        ClientServiceProvider clientServiceProvider = ClientServiceProvider.loadService();

        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        SimpleConsumer simpleConsumer = clientServiceProvider.newSimpleConsumerBuilder()
            .setConsumerGroup("FIFOGroup")
            .setClientConfiguration(clientConfiguration)
            .setSubscriptionExpressions(Collections.singletonMap("FIFOTopic", filterExpression))
            .setAwaitDuration(Duration.ofSeconds(10)) //间隔多久拉取一次
            .build();
        List<MessageView> messageViewList = null;
        do {
            try {
                messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));//第一个参数: 代表每次从服务器拉取 10 条消息  第二个参数: 拉取消息的超时时间
                messageViewList.forEach(messageView -> {
                    System.out.println(messageView);
                    //消费处理完成后，需要主动调用ACK提交消费结果。
                    try {
                        simpleConsumer.ack(messageView);
                    } catch (ClientException e) {
                        e.printStackTrace();
                    }
                });
            } catch (ClientException e) {
                //如果遇到系统流控等原因造成拉取失败，需要重新发起获取消息请求。
                e.printStackTrace();
            }
        } while (true);
    }


}
