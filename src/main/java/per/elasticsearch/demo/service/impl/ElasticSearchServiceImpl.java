package per.elasticsearch.demo.service.impl;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import per.elasticsearch.demo.service.ElasticSearchService;
import per.elasticsearch.demo.utils.DefaultXContentBuilderTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.service.impl
 * @description
 * @date 2021/8/4 17:42
 */
@Service
public class ElasticSearchServiceImpl implements ElasticSearchService {
    @Autowired
    @Qualifier(value = "restHighLevelClient")
    private RestHighLevelClient client;

    @Override
    public boolean createIndex(String index, Map<String,String>fields) {
        //先判断该索引是否存在
        if(existIndex(index)){
           return false;
        }
        CreateIndexRequest request=new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = null;
        //setting
        XContentBuilder setting = DefaultXContentBuilderTool.getSetting();
        //设置settings
        request.settings(setting);
        //mapping
        if(fields!=null&&fields.size()>0){
            JSONObject mapping=new JSONObject();
            mapping.put("dynamic",true);
            //字段json
            JSONObject properties=new JSONObject();
            for(Map.Entry<String,String>tmp:fields.entrySet()){
                String type=tmp.getValue();
                JSONObject field=new JSONObject();
                field.put("type",type);
                if(type.equals("text")){
                    field.put("analyzer","text_analyzer");
                }
                properties.put(tmp.getKey(),field);
            }
            mapping.put("properties",properties);
            //设置mappings
            request.mapping(mapping.toJSONString(),XContentType.JSON);
        }
        try {
            //请求结果
            createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean existIndex(String index) {
        GetIndexRequest request=new GetIndexRequest(index);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean deleteIndex(String index) {
        //先判断是否存在
        if(!existIndex(index)){
            return false;
        }
        DeleteIndexRequest request=new DeleteIndexRequest(index);
        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean createDocument(String index, String id, String content) {
        //查看文档是否存在
        if(existDocument(index,id)){
            return false;
        }
        IndexRequest request=new IndexRequest(index).id(id);
        request.source(content, XContentType.JSON);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (response.status()==RestStatus.CREATED);
    }

    @Override
    public boolean existDocument(String index, String id){
        GetRequest request=new GetRequest(index,id);
        try {
            return client.exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String getDocument(String index, String id) {
        GetRequest request=new GetRequest(index,id);
        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response.getSourceAsString();
    }

    @Override
    public boolean updateDocument(String index, String id, String content) {
        //判断文档是否存在
        if(!existDocument(index,id)){
            return false;
        }
        UpdateRequest request=new UpdateRequest(index,id);
        request.doc(content,XContentType.JSON);
        UpdateResponse response = null;
        try {
            response = client.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (response.status()==RestStatus.OK);
    }

    @Override
    public boolean deleteDocument(String index, String id) {
        if(!existDocument(index,id)){
            return false;
        }
        DeleteRequest request=new DeleteRequest(index,id);
        DeleteResponse response = null;
        try {
            response = client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (response.status()==RestStatus.OK);
    }

    @Override
    public boolean bulkInsert(String index, List<String> document) {
        BulkRequest request=new BulkRequest();
        for(String tmp:document){
            JSONObject jsonObject=JSONObject.parseObject(tmp);
            request.add(new IndexRequest(index).source(jsonObject,XContentType.JSON));
        }
        BulkResponse response = null;
        try {
            response = client.bulk(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BulkItemResponse[] items = response.getItems();
        for(BulkItemResponse tmp:items){
            System.out.println("操作:"+tmp.getOpType().name()+"-主键id:"+tmp.getId()+"-是否成功:"+(!tmp.isFailed()));
        }
        return !response.hasFailures();
    }

    @Override
    public boolean bulkRequest(String index, Iterable<DocWriteRequest<?>> requests) {
        BulkRequest bulkRequest=new BulkRequest();
        requests.forEach(x->{
            bulkRequest.add(x);
        });
        BulkResponse responses = null;
        try {
            responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BulkItemResponse[] items = responses.getItems();
        for(BulkItemResponse tmp:items){
            System.out.println("操作:"+tmp.getOpType().name()+"-主键id:"+tmp.getId()+"-是否成功:"+(!tmp.isFailed()));
        }
        return !responses.hasFailures();
    }

    @Override
    public List<JSONObject> searchByKeyword(String keyword,String...indices) {
        List<JSONObject>results=null;
        SearchRequest request=new SearchRequest();
        request.indices(indices);
        SearchSourceBuilder builder=new SearchSourceBuilder();
        //设置查找字段
        builder.query(QueryBuilders.multiMatchQuery(keyword,"programName","tag","username","address","name")
                            .analyzer("ik_smart"));
        request.source(builder);
        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SearchHit[] hits = response.getHits().getHits();
        if(hits.length>0){
            results=new ArrayList<>();
            for(SearchHit hit:hits){
                JSONObject jsonObject=JSONObject.parseObject(hit.getSourceAsString());
                results.add(jsonObject);
            }
        }
        return results;
    }

    @Override
    public List<String> searchAllId(String index) {
        if(!existIndex(index)){
            return null;
        }
        SearchRequest request=new SearchRequest(index);
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        request.source(builder);
        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SearchHit[] hits = response.getHits().getHits();
        List<String>results=null;
        if(hits.length>0){
            results=new ArrayList<>();
            for(SearchHit hit:hits){
                results.add(hit.getId());
            }
        }
        return results;
    }
}
