package delay;

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

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * @author xiaofu
 * @date 2024/09/01
 * @program middle_ware_group
 * @description 对于延迟\定时消息的消费 pushConsumer、simpleConsumer
 **/

@Slf4j
public class MqDelayConsumerTest {

    /**
     * pushConsumer
     * @throws InterruptedException
     * @throws ClientException
     */
    @Test
    public void test() throws InterruptedException, ClientException {
        //1.创建服务器配置对象   proxy 组件
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints("10.15.0.9:8081").build();
        //定义主题
        String delayTopic = "DelayTopic";
        //2.创建 push Consumer 对象
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup("delayGroup")
            .setSubscriptionExpressions(Collections.singletonMap(delayTopic, filterExpression))
            .setMessageListener(messageView -> {
                // 处理消息并返回消费结果。
                log.info("Consume message successfully, messageId={},content={}", messageView.getMessageId(), messageView.getBody());
                System.out.println(messageView.getDeliveryTimestamp());
                //根据消费结果返回状态。
                return ConsumeResult.SUCCESS;
            }) //消费消息
            .build();

        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();


    }

    /**
     * simpleConsumer
     * @throws InterruptedException
     * @throws ClientException
     */
    @Test
    public void test2() throws ClientException {
        //创建配置对象
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints("10.15.0.9:8081").build();

        //创建 simple consumer
        ClientServiceProvider clientServiceProvider = ClientServiceProvider.loadService();
        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        SimpleConsumer simpleConsumer = clientServiceProvider.newSimpleConsumerBuilder()
            .setConsumerGroup("delayGroup")
            .setClientConfiguration(clientConfiguration)
            .setSubscriptionExpressions(Collections.singletonMap("DelayTopic", filterExpression))
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
