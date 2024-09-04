package transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.junit.Test;

import java.util.Collections;

/**
 * @author xiaofu
 * @date 2024/09/02
 * @program middle_ware_group
 * @description 事务消息消费
 **/

@Slf4j
public class TestTransactionConsumer {

    @Test
    public void test() throws ClientException, InterruptedException {

        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "TestTopic";

        //2.创建消费者 并指定订阅关系
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "10.15.0.9:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .build();
        //订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // 设置消费者分组。
            .setConsumerGroup("baseGroup")
            // 设置预绑定的订阅关系。
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // 设置消费监听器。
            .setMessageListener(messageView -> {
                // 处理消息并返回消费结果。
                log.info("Consume message successfully, messageId={},content={},message group={}", messageView.getMessageId(), messageView.getBody(), messageView.getMessageGroup());
                System.out.println(messageView.getDeliveryTimestamp());
                //根据消费结果返回状态。
                return ConsumeResult.SUCCESS;
            }).build();
        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();
    }

}
