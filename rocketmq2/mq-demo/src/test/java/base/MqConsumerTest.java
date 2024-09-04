package base;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author xiaofu
 * @date 2024/09/01
 * @program middle_ware_group
 * @description rocketmq-client-java 消费消息
 *
 * **PushConsumer：**
 * 优点：
 * - 实现简单：只需实现消息监听器即可，RocketMQ 会主动将消息推送给监听器。
 * - 及时性高：消息到达时会立即被推送给监听器，能够实现较高的实时性。
 * - 可扩展性好：可以很容易地添加新的消息监听器来处理不同类型的消息。
 * 缺点：
 * - 难以控制消息拉取速率：PushConsumer 的消费速率受消息生产速率和网络等因素影响，难以控制。
 * - 容易出现消息堆积：如果消费速率低于消息生产速率，可能导致消息堆积，影响系统性能。
 * 适用场景：
 * - 对消息实时性要求较高的场景，如实时日志处理、实时监控等。
 * - 需要简单快速地实现消息消费的场景。
 *
 * **SimpleConsumer：**
 * 优点：
 * - 控制消费速率：由消费者主动拉取消息，可以更好地控制消费速率，避免消息堆积。
 * - 可靠性高：消费者可以手动确认消息消费成功，确保消息不会丢失。
 * - 灵活性强：可以自由控制消息的拉取时间、批量处理等。
 * 缺点：
 * - 实现复杂：需要手动处理消息拉取和消费结果提交，相对复杂。
 * - 实时性较差：消息不是即时推送给消费者，而是消费者主动拉取，可能会有一定的延迟。
 * 适用场景：
 * - 对消息处理速率有严格要求的场景，可以通过控制拉取消息的频率来实现。
 * - 需要精细控制消息消费流程的场景，如消息去重、幂等性处理等。
 *
 * `综上所述，PushConsumer 适用于对消息实时性要求较高的场景，而 SimpleConsumer 则适用于对消息消费速率有严格要求或需要精细控制消费流程的场景`。
 **/

@Slf4j
public class MqConsumerTest {

    private ClientServiceProvider provider;

    private ClientConfiguration clientConfiguration;

    private FilterExpression filterExpression;


    @Before
    public void before() {
        provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "10.15.0.9:8081";
        clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
    }

    /**
     * 正常消息的消费
     *
     * @throws InterruptedException
     * @throws ClientException
     */
    @Test
    public void test() throws InterruptedException, ClientException {
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "testGroup";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "test";
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // 设置消费者分组。
            .setConsumerGroup(consumerGroup)
            // 设置预绑定的订阅关系。
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // 设置消费监听器。
            .setMessageListener(messageView -> {
                // 处理消息并返回消费结果。
                log.info("Consume message successfully, messageId={},content={}", messageView.getMessageId(), messageView.getBody());
                return ConsumeResult.SUCCESS;
            })
            .build();
        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();
    }

    /**
     * 使用SimpleConsumer消费普通消息，主动获取消息进行消费处理并提交消费结果
     * do while 循环，同步消费 拉取消息
     * @throws InterruptedException
     * @throws ClientException
     */
    @Test
    public void testSimpleConsumerSync() throws ClientException {
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "testGroup";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "test";
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // 设置消费者分组。
            .setConsumerGroup(consumerGroup)
            // 设置预绑定的订阅关系。
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .build();

        List<MessageView> messageViewList = null;
        do {
            try {
                messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));
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

    /**
     * 使用SimpleConsumer消费普通消息，主动获取消息进行消费处理并提交消费结果
     * do while 循环，异步消费 拉取消息
     * @throws InterruptedException
     * @throws ClientException
     */
    @Test
    public void testSimpleConsumerAsync() throws ClientException {
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "testGroup";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "test";
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // 设置消费者分组。
            .setConsumerGroup(consumerGroup)
            // 设置预绑定的订阅关系。
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // 间隔10s拉取一次
            .setAwaitDuration(Duration.ofSeconds(10))
            .build();

        ExecutorService receiveCallbackExecutor = Executors.newCachedThreadPool();
        // Set individual thread pool for ack callback.
        ExecutorService ackCallbackExecutor = Executors.newCachedThreadPool();
        do {
            final CompletableFuture<List<MessageView>> future0 = simpleConsumer.receiveAsync(10,
                Duration.ofSeconds(30));
            future0.whenCompleteAsync(((messages, throwable) -> {
                if (null != throwable) {
                    //log.error("Failed to receive message from remote", throwable);
                    // Return early.
                    return;
                }
                log.info("Received {} message(s)", messages.size());
                // Using messageView as key rather than message id because message id may be duplicated.
                final Map<MessageView, CompletableFuture<Void>> map =
                    messages.stream().collect(Collectors.toMap(message -> message, simpleConsumer::ackAsync));
                for (Map.Entry<MessageView, CompletableFuture<Void>> entry : map.entrySet()) {
                    final MessageId messageId = entry.getKey().getMessageId();
                    final CompletableFuture<Void> future = entry.getValue();
                    future.whenCompleteAsync((v, t) -> {
                        if (null != t) {
                            //log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                            // Return early.
                            return;
                        }
                        log.info("Message is acknowledged successfully, messageId={}", messageId);
                    }, ackCallbackExecutor);
                }

            }), receiveCallbackExecutor);
        } while (true);
    }
}
