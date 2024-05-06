package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class HotelSearchTest {

    private RestHighLevelClient client;

    @Test
    void testMatchAll()throws IOException {//match_all查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.matchAllQuery());

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testMatch()throws IOException {//match查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.matchQuery("name","如家外滩"));

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testTerm()throws IOException {//term查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.termQuery("city","上海"));

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testRange()throws IOException {//rangge查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.rangeQuery("price").gte(200).lte(300));

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testBoolean()throws IOException {//boolean查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("city","深圳")).filter(QueryBuilders.rangeQuery("price").gte(200).lte(300)));

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testSort()throws IOException {//排序查询

        int page=3;
        int size=5;

        SearchRequest request=new SearchRequest("hotel");

       request.source().from((page-1)*size).size(size).query(QueryBuilders.matchAllQuery()).sort("price", SortOrder.ASC);
        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }

    @Test
    void testHightLight()throws IOException {//高亮查询
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.matchQuery("all","如家"))
                .highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        final SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);

    }





    private void handleResponse(SearchResponse response) {
        final SearchHits searchHits = response.getHits();
        System.out.println(searchHits.getTotalHits().value);

        final SearchHit[] hits = searchHits.getHits();

        for (SearchHit hit : hits) {
            final String sourceAsString = hit.getSourceAsString();

            final Hotel hotel = JSON.parseObject(sourceAsString, Hotel.class);

            final Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            if (!CollectionUtils.isEmpty(highlightFields)){
                final HighlightField name = highlightFields.get("name");

                if (name!=null){
                    final String text = name.getFragments()[0].toString();
                    hotel.setName(text);
                }

            }


            System.out.println(hotel);
        }
    }




    @BeforeEach
    public void init(){
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("120.26.3.155:9200")
        ));
    }

    @AfterEach
    public void close() throws Exception{
        this.client.close();
    }
}
