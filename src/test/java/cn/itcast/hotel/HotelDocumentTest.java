package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {

    @Autowired
    private HotelService hotelService;

    private RestHighLevelClient client;

    @Test
    void createDocument() throws IOException {//新增文档

        final Hotel byId = hotelService.getById(56977L);
        final HotelDoc hotelDoc = new HotelDoc(byId);

        IndexRequest request=new IndexRequest("hotel").id("56977");
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);

    }

    @Test
    void getDocument()throws IOException{//查询文档
        //设置参数
        GetRequest request=new GetRequest("hotel","56977");
        //获取响应
        final GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //拿到响应的重要部分
        final String json = response.getSourceAsString();
        //转换为对象
        final HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }


    @Test
    void updateDocument()throws IOException{//局部更新 (全量更新用新增即可)
        UpdateRequest request=new UpdateRequest("hotel","56977");
        request.doc(
                "name","广东惠阳华美达小酒店",
                "address","广东省惠州市惠阳区淡水街道"
        );
        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteDocument()throws IOException{
        DeleteRequest request=new DeleteRequest("hotel","56977");
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkRequest()throws IOException{//批量新增文档
        final List<Hotel> list = hotelService.list();

        BulkRequest request=new BulkRequest();//批量处理请求

        for (Hotel hotel:list) {
            final HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }

        client.bulk(request, RequestOptions.DEFAULT);
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
//56977