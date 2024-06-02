package com.xiaofu.es;

import com.alibaba.fastjson.JSON;
import com.xiaofu.es.entity.Hotel;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fuzhouling
 * @date 2024/06/01
 * @program middle_ware_group
 * @description RestClient 操作文档
 **/

public class EsHotelDocumentTest {
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
     * 新增文档
     *
     * @throws IOException
     */
    @Test
    void testAddHotelDoc() throws IOException {
        // 1. 创建 request 请求
        // 2. 准备json文档：从数据库查出对应的数据,再转换为文档类型（注意格式问题）
        // - id Long -> String
        // - 经纬度 location =  经度 + ”, “ + 维度 ;
        // 3.发送请求

        // 1. 创建 request 请求
        IndexRequest request = new IndexRequest("hotel").id("1");
        // 2. 准备json文档：从数据库查出对应的数据,再转换为文档类型（注意格式问题）
        request.source("json 数据", XContentType.JSON);

        when(client.index(any(IndexRequest.class), any(RequestOptions.class)))
            .thenReturn(any(IndexResponse.class));

        // 3.发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 验证 index()是否被调用
        verify(client).index(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);

    }

    /**
     * 查询文档
     *
     * @throws IOException
     */
    @Test
    void testGetHotelDoc() throws IOException {
        // 1. 创建 request 请求
        // 2. 发送请求
        // 3. 解析响应结果,json数据转换为对应的对象

        // 1. 创建 request 请求
        GetRequest request = new GetRequest("hotel", "1");

        when(client.get(any(GetRequest.class), any(RequestOptions.class))).thenReturn(any(GetResponse.class));

        // 2. 发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3. 解析响应结果，并把json 反序列化为对应的对象
        String sourceAsString = response.getSourceAsString();
        Hotel hotel = JSON.parseObject(sourceAsString, Hotel.class);
        System.out.println(hotel.toString());

        // 验证 get()是否被调用
        verify(client).get(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);

    }


    /**
     * 更新文档：全量更新、局部更新
     * 全量更新：删除id相同的旧文档、添加新文档（若id不存在，直接添加新文档）
     * 局部更新：只更新部分字段
     *
     * @throws IOException
     */
    @Test
    void testUpdateHotelDoc() throws IOException {
        // 局部更新演示
        // 1. 创建 request 请求
        // 2. 准备参数
        // 3. 发送请求

        // 1. 创建 request 请求
        UpdateRequest request = new UpdateRequest("hotel", "1");
        // 2. 准备参数
        request.doc(
            "stars", 5,
            "address", "上海市浦东新区"
        );

        when(client.update(any(UpdateRequest.class), any(RequestOptions.class))).thenReturn(any(UpdateResponse.class));
        // 3. 发送请求
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);


        // 验证 update()是否被调用
        verify(client).update(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);

    }

    /**
     * 删除文档
     *
     * @throws IOException
     */
    @Test
    void testDeleteHotelDoc() throws IOException {
        // 1. 创建 request 请求
        // 2. 发送请求

        // 1. 创建 request 请求
        DeleteRequest request = new DeleteRequest("hotel", "1");

        when(client.delete(any(DeleteRequest.class), any(RequestOptions.class))).thenReturn(any(DeleteResponse.class));
        // 2. 发送请求
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);


        // 验证 delete()是否被调用
        verify(client).delete(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);

    }

    /**
     * 批量导入文档：BulkRequest
     *
     * @throws IOException
     */
    @Test
    void testBatchPutHotelDoc() throws IOException {
        // 1. 创建 BulkRequest 请求
        // 2. 创建每个新增的request并添加到BulkRequest
        // - 可以从数据库中查询数据 for(data -> json)
        // 3. 发送请求

        // 1. 创建 BulkRequest 请求
        BulkRequest request = new BulkRequest();
        // 2.
        IndexRequest request1 = new IndexRequest("hotel").id("11").source("json1", XContentType.JSON);
        IndexRequest request2 = new IndexRequest("hotel").id("12").source("json2", XContentType.JSON);
        IndexRequest request3 = new IndexRequest("hotel").id("13").source("json3", XContentType.JSON);
        request.add(request1);
        request.add(request2);
        request.add(request3);

        when(client.bulk(any(BulkRequest.class), any(RequestOptions.class))).thenReturn(any(BulkResponse.class));
        // 3. 发送请求
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);


        // 验证 bulk()是否被调用
        verify(client).bulk(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);

    }

}
