package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexSearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;
    private SearchResponse searchResponse;

    @Before
    public void initSearchRequest() {
        // 搜索请求对象
        searchRequest = new SearchRequest("java1906");
        searchRequest.types("course");
    }

    //查询文档
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906","course","1");
        GetResponse getResponse = restHighLevelClient.get(getRequest);
        boolean exists = getResponse.isExists();
        System.out.println(exists);
        String source = getResponse.getSourceAsString();
        System.out.println(source);
    }

    //搜索type下的全部记录
    @Test
    public void testSearchAll() throws IOException {
        //搜索元构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //设置搜索元
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    @After
    public void displayDoc() throws ParseException {
        // 搜索匹配结果
        SearchHits hits = searchResponse.getHits();
        // 搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到" + totalHits + "条文档");
        // 匹配的文档
        SearchHit[] searchHits = hits.getHits();
        // 日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (SearchHit hit : searchHits) {
            // 文档id
            String id = hit.getId();
            System.out.println("id：" + id);
            // 源文档内容
            String source = hit.getSourceAsString();
            System.out.println(source);

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields != null){
                HighlightField highlightField = highlightFields.get("name");
                Text[] fragments = highlightField.getFragments();
                System.out.println("高亮效果："+fragments[0].toString());
            }
        }
    }

    //分页查询
    @Test
    public void testSearchPage() throws IOException {
        //搜索元构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置分页
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(3);
        searchSourceBuilder.sort("price", SortOrder.ASC);

        //设置搜索元
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //match查询
    @Test
    public void testMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));

        //搜索源
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
    }

    //multi_match查询
    @Test
    public void testMultiMatchQuery() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发","name","description"));

        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        // 执行搜索
        searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
    }

    //bool查询
    @Test
    public void testBooleanQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //must
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "开发"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("description","开发"));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
    }

    //filter查询
    @Test
    public void testFilterQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(10).lte(100));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
    }

    //highlight查询
    @Test
    public void testHighLightQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","开发"));
        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));

        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }
}
