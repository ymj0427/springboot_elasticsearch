package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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

}
