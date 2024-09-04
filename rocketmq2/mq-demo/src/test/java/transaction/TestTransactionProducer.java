package transaction;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.assertj.core.util.Strings;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaofu
 * @date 2024/09/02
 * @program middle_ware_group
 * @description 事务消息
 **/

public class TestTransactionProducer {
    //演示demo，模拟订单表查询服务，用来确认订单事务是否提交成功。
    private static boolean checkOrderById(String orderId) {
        return true;
    }

    //演示demo，模拟本地事务的执行结果。
    private static boolean doLocalTransaction() {
        return true;
    }

    @Test
    public void test1() throws ClientException, IOException {
        //创建配置对象
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints("10.15.0.9:8081").build();
        //创建 producer
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        //构造事务生产者：事务消息需要生产者构建一个事务检查器，用于检查确认异常半事务的中间状态。
        Producer producer = provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setTransactionChecker(messageView -> {  //如果 rocketmq 接收到信息为 unknown 使用该方式自查
                /**
                 * 事务检查器一般是根据业务的ID去检查本地事务是否正确提交还是回滚，此处以订单ID属性为例。
                 * 在订单表找到了这个订单，说明本地事务插入订单的操作已经正确提交；如果订单表没有订单，说明本地事务已经回滚。
                 */
                final String orderId = messageView.getProperties().get("OrderId");
                if (Strings.isNullOrEmpty(orderId)) {
                    // 错误的消息，直接返回Rollback。
                    return TransactionResolution.ROLLBACK;
                }
                return checkOrderById(orderId) ? TransactionResolution.COMMIT : TransactionResolution.ROLLBACK;
            })
            .build();
        //开启事务分支
        final Transaction transaction;
        try {
            transaction = producer.beginTransaction();
        } catch (ClientException e) {
            e.printStackTrace();
            //事务分支开启失败，直接退出。
            return;
        }
        String orderId = UUID.randomUUID().toString();
        Message message = messageBuilder.setTopic("TestTopic")
            //设置消息索引键，可根据关键字精确查找某条消息。
            .setKeys("messageKey")
            //设置消息Tag，用于消费端根据指定Tag过滤消息。
            .setTag("messageTag")
            //一般事务消息都会设置一个本地事务关联的唯一ID，用来做本地事务回查的校验。
            .addProperty("OrderId", orderId)
            //消息体。
            .setBody("messageBody".getBytes())
            .build();
        //发送半事务消息
        final SendReceipt sendReceipt;
        try {
            sendReceipt = producer.send(message, transaction);
            System.out.println("messageId: " + sendReceipt.getMessageId());
        } catch (ClientException e) {
            //半事务消息发送失败，事务可以直接退出并回滚。
            return;
        }
        /**
         * 执行本地事务，并确定本地事务结果。
         * 1. 如果本地事务提交成功，则提交消息事务。
         * 2. 如果本地事务提交失败，则回滚消息事务。
         * 3. 如果本地事务未知异常，则不处理，等待事务消息回查。
         *
         */
        boolean localTransactionOk = doLocalTransaction();
        if (localTransactionOk) {
            try {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("成功提交事务,消息才会被置为可见性~~~~");
                transaction.commit();
            } catch (ClientException e) {
                // 业务可以自身对实时性的要求选择是否重试，如果放弃重试，可以依赖事务消息回查机制进行事务状态的提交。
                e.printStackTrace();
            }
        } else {
            try {
                System.out.println("回滚本地事务,消息不会被置为可见性~~~~");
                transaction.rollback();
            } catch (ClientException e) {
                // 建议记录异常信息，回滚异常时可以无需重试，依赖事务消息回查机制进行事务状态的提交。
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
