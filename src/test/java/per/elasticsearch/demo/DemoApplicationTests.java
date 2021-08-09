package per.elasticsearch.demo;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import per.elasticsearch.demo.service.ElasticSearchService;

import java.util.*;

@SpringBootTest
class DemoApplicationTests {
    @Autowired
    private ElasticSearchService service;

    @Test
    void contextLoads() {
        //1.字段名field，2.类型type
        Map<String,String>fields=new HashMap<>();
        fields.put("name","keyword");
        fields.put("programName","text");
        System.out.println(service.createIndex("new_index_1",null));
    }


    @Test
    void testBulkOperation(){
        //添加document操作
        IndexRequest indexRequest=new IndexRequest("desktop").id("testId")
                .source(XContentType.JSON,"testField1","testField1");
        //修改document操作
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("programId","5");
        jsonObject.put("programName","网易云音乐-updated");
        UpdateRequest updateRequest=new UpdateRequest("desktop","1006")
                .doc(jsonObject.toJSONString(),XContentType.JSON);
        //操作集合
        List<DocWriteRequest<?>>requests=new ArrayList<>();
        requests.add(indexRequest);
        requests.add(updateRequest);
        System.out.println(service.bulkRequest("desktop",requests));
    }

    @Test
    void testSearch(){
        List<JSONObject> jsonObjects = service.searchByKeyword("腾讯", "desktop","customer","student");
//        jsonObjects.forEach(x->{
//            System.out.println(JSONTools.formatJSON(x));
//        });
        if(jsonObjects!=null&&jsonObjects.size()>0){
            jsonObjects.forEach(System.out::println);
        }
    }

    @Test
    void testSearchAllIndex(){
        List<String> list = service.searchAllId("student");
        if(list!=null&&list.size()>0){
            list.forEach(System.out::println);
        }
    }

}
