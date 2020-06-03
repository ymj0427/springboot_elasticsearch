package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexWriterTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //创建索引库
    @Test
    public void testCreateIndex() throws IOException {
        //创建"创建索引库对象"对象
        CreateIndexRequest request = new CreateIndexRequest("java1906");
        //设置索引参数
        CreateIndexRequest settings = request.settings(Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0));
        request.mapping("course","{\n" +
                "  \"properties\": {\n" +
                "    \"name\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"description\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"studymodel\":{\n" +
                "      \"type\": \"text\"\n" +
                "    },\n" +
                "    \"pic\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"index\": false\n" +
                "    },\n" +
                "    \"studydate\":{\n" +
                "      \"type\": \"date\",\n" +
                "      \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
                "    },\n" +
                "    \"price\":{\n" +
                "      \"type\": \"double\"\n" +
                "    }\n" +
                "  }  \n" +
                "}", XContentType.JSON);
        //创建索引客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        CreateIndexResponse response = indices.create(request);
        System.out.println(response.isAcknowledged());

    }

    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //创建删除索引请求对象
        DeleteIndexRequest java1906 = new DeleteIndexRequest("java1906");
        //创建索引客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse response = indices.delete(java1906, RequestOptions.DEFAULT);

        System.out.println(response.isAcknowledged());
    }

    //添加文档
    @Test
    public void testAddDoc() throws IOException {
        //创建索引请求对象：索引当动词
        IndexRequest indexRequest = new IndexRequest("java1906", "course", "1");
        indexRequest.source("{\n" +
                " \"name\":\"spring cloud实战\",\n" +
                " \"description\":\"本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。\",\n" +
                " \"studymodel\":\"201001\",\n" +
                " \"price\":5.6\n" +
                "}",XContentType.JSON);
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    //批量添加  bulk
    @Test
    public void testBulkAddDoc() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("java1906","course")
                .source("{\"name\":\"hh实战\",\"description\":\"hh谁都不服\",\"studymodel\":\"201002\",\"price\":\"5.6\"}",XContentType.JSON)
                );
        bulkRequest.add(new IndexRequest("java1906","course")
                .source("{\"name\":\"java实战\",\"description\":\"java谁都不服\",\"studymodel\":\"201003\",\"price\":\"5.7\"}",XContentType.JSON)
                );
        BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.hasFailures());
    }

    //更新文档
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java1906", "course", "1");
        updateRequest.doc("{\n" +
                "  \"price\":7.6\n" +
                "}", XContentType.JSON);
        UpdateResponse updateResponse =
                restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
        DocWriteResponse.Result result = updateResponse.getResult();
        System.out.println(result);
    }

    //根据id删除文档
    @Test
    public void testDelDocument() throws IOException {
        //删除索引请求对象
        DeleteRequest deleteRequest = new DeleteRequest("java1906","course","1");
        //响应对象
        DeleteResponse deleteResponse =
                restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }
}
