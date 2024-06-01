package com.xiaofu.es;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.xiaofu.es.constants.HotelConstants.MAPPING_TEMPLATE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fuzhouling
 * @date 2024/06/01
 * @program middle_ware_group
 * @description RestClient 操作索引库
 **/

public class EsHotelIndexTest {
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


    /**
     * 创建索引库
     *
     * @throws IOException
     */
    @Test
    void testCreateHotelIndex() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest("hotel");
        request.source(MAPPING_TEMPLATE, XContentType.JSON);

        // 模拟indices().create()方法的返回值
        when(client.indices().create(any(CreateIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(new CreateIndexResponse(true, true, "index created"));

        // 创建索引
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        // 验证indices().create()是否被调用
        verify(client).indices().create(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertTrue(response.isAcknowledged());

    }

    /**
     * 删除索引库
     *
     * @throws IOException
     */
    @Test
    void testDeleteHotelIndex() throws IOException {

        DeleteIndexRequest request = new DeleteIndexRequest("hotel");

        // 模拟indices().create()方法的返回值
        when(client.indices().delete(any(DeleteIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(new AcknowledgedResponse(any(), true));

        // 创建索引
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

        // 验证indices().create()是否被调用
        verify(client).indices().delete(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertTrue(response.isAcknowledged());

    }

    /**
     * 判断索引库是否存在
     *
     * @throws IOException
     */
    @Test
    void testExistsHotelIndex() throws IOException {

        GetIndexRequest request = new GetIndexRequest("hotel");

        // 模拟indices().create()方法的返回值
        when(client.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

        // 创建索引
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        // 验证indices().create()是否被调用
        verify(client).indices().exists(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(exists);

    }

}
