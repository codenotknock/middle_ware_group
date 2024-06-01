package com.xiaofu.es;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * @author fuzhouling
 * @date 2024/06/01
 * @program middle_ware_group
 * @description RestClient 初始化
 **/
public class EsServiceTest {
    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
//        RestHighLevelClient client = new RestHighLevelClient(RestClientBuilder(
//            HttpHost.create("http://192.168.123.456:9200")));
        client = mock(RestHighLevelClient.class);
    }

    @AfterEach
    void tearDown() throws IOException {
        // mock 模拟对象不需要关闭
//        client.close();
    }

    @Test
    void testInit() {
        // mock 模拟对象不需要验证
//        verify(client);
    }


}
