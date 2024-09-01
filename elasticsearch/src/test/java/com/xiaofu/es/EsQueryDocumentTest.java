package com.xiaofu.es;

import com.alibaba.fastjson2.JSON;
import com.xiaofu.es.entity.Hotel;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fuzhouling
 * @date 2024/06/02
 * @program middle_ware_group
 * @description RestClient 查询文档
 **/
public class EsQueryDocumentTest {
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
     * 全文检索：multi_match、match_all、match 对应的API基本一致，只是查询条件不同
     *
     * @throws IOException
     */
    @Test
    void testMatchAllDoc() throws IOException {
        // 1. 创建 request 请求
        // 2. 准备DSL
        // 3. 发送请求
        // 4. 解析结果：总条数和结果数组(逐层解析)

        // 1. 创建 request 请求
        SearchRequest request = new SearchRequest("hotel");
        // 2. 准备DSL
        request.source().query(QueryBuilders.matchAllQuery());

        // 模拟 search 请求
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(any(SearchResponse.class));
        // 3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 解析结果
        handleResponse(response);

        // 验证 search()是否被调用
        verify(client).search(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);
    }

    /**
     * 全文检索：multi_match、match_all、match 对应的API基本一致，只是查询条件不同
     *
     * @throws IOException
     */
    @Test
    void testMatchDoc() throws IOException {
        // 1. 创建 request 请求
        // 2. 准备DSL
        // 3. 发送请求
        // 4. 解析结果：总条数和结果数组(逐层解析)

        // 1. 创建 request 请求
        SearchRequest request = new SearchRequest("hotel");
        // 2. 准备DSL
        // 单字段查询
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 多字段查询
//        request.source().query(QueryBuilders.multiMatchQuery("如家", "name", "business"));

        // 模拟 search 请求
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(any(SearchResponse.class));
        // 3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 解析结果
        handleResponse(response);

        // 验证 search()是否被调用
        verify(client).search(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);
    }

    /**
     * 精准查询 term、range ：keyWord、bool、
     *
     * @throws IOException
     */
    @Test
    public void testBoolDoc() throws IOException {
        // 1. 准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2. 准备DSL
        // 2.1 准备 BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.2 添加 term
        boolQuery.must(QueryBuilders.termQuery("city", "上海"));
        // 2.3 添加 range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(500));

        // 模拟 search 请求
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(any(SearchResponse.class));
        // 3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 解析结果
        handleResponse(response);

        // 验证 search()是否被调用
        verify(client).search(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);
    }

    /**
     * 分页和排序：from和size、sort
     *
     * @throws IOException
     */
    @Test
    public void testPageAndSortDoc() throws IOException {
        int page = 1, size = 10;
        // 1. 准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2. 准备DSL
        // 2.1 query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2 排序 sort
        request.source().sort("price", SortOrder.ASC);
        // 2.3 分页 from、size
        request.source().from((page - 1) * size).size(size);

        // 模拟 search 请求
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(any(SearchResponse.class));
        // 3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 解析结果
        handleResponse(response);

        // 验证 search()是否被调用
        verify(client).search(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);
    }

    /**
     * 高亮显示：包括高亮构建和结果解析两部分
     *
     * @throws IOException
     */
    @Test
    public void testHighlightDoc() throws IOException {
        // 1. 准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2. 准备DSL
        // 2.1 query
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 2.2 高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        // 模拟 search 请求
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(any(SearchResponse.class));
        // 3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4. 解析结果——highlight
        TotalHits totalHits = response.getHits().getTotalHits();
        System.out.println("共搜索到" + totalHits + "条数据！");
        SearchHit[] searchHits = response.getHits().getHits();
        LinkedList<Hotel> hotels = new LinkedList<>();
        for(SearchHit hit: searchHits) {
            String json = hit.getSourceAsString();
            Hotel hotel = JSON.parseObject(json, Hotel.class);
            System.out.println(json);
            // 高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                HighlightField name = highlightFields.get("name");
                if (Objects.nonNull(name)) {
                    // 覆盖非高亮的结果
                    hotel.setName(name.getFragments()[0].string());
                }
            }
            hotels.add(hotel);
        }

        // 验证 search()是否被调用
        verify(client).search(request, RequestOptions.DEFAULT);
        // 检查返回值是否符合预期
        assertNotNull(response);
    }

    /**
     * 解析响应数据
     *
     * @param response
     */
    private static void handleResponse(SearchResponse response) {
        // 4. 解析结果
        TotalHits totalHits = response.getHits().getTotalHits();
        System.out.println("共搜索到" + totalHits + "条数据！");
        SearchHit[] searchHits = response.getHits().getHits();
        LinkedList<Hotel> hotels = new LinkedList<>();
        for(SearchHit hit: searchHits) {
            String json = hit.getSourceAsString();
            Hotel hotel = JSON.parseObject(json, Hotel.class);
            System.out.println(json);
            hotels.add(hotel);
        }
    }

}
