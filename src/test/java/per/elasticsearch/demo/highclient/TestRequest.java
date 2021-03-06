package per.elasticsearch.demo.highclient;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import per.elasticsearch.demo.utils.HighLevelClientTool;
import per.elasticsearch.demo.utils.JSONTools;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.highclient
 * @description
 * @date 2021/8/4 13:50
 */
public class TestRequest {
    @Test
    public void sendRequestSync() throws IOException {
        RestHighLevelClient client= new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http"))
        );
        GetSourceRequest request=new GetSourceRequest("customer","0001");
        String []includes=new String[]{"address","username","uid"};
        String []excludes=Strings.EMPTY_ARRAY;
        //?????????????????????????????????
        request.fetchSourceContext(new FetchSourceContext(true,includes,excludes));
        //?????????????????????
        request.refresh(true);
        GetSourceResponse response = client.getSource(request, RequestOptions.DEFAULT);
        Map<String, Object> source = response.getSource();
        for(Map.Entry<String,Object>tmp:source.entrySet()){
            System.out.println(tmp.getKey()+":"+tmp.getValue());
        }
        client.close();
    }
    @Test
    public void sendRequestAsync() throws IOException {
        RestHighLevelClient client=new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http"))
        );
        GetSourceRequest request=new GetSourceRequest("customer","0001");
        String []includes=new String[]{"address","username","uid"};
        String []excludes=Strings.EMPTY_ARRAY;
        //?????????????????????????????????
        request.fetchSourceContext(new FetchSourceContext(true,includes,excludes));
        //?????????????????????
        request.refresh(true);
        client.getSourceAsync(request, RequestOptions.DEFAULT, new ActionListener<GetSourceResponse>() {
            @Override
            public void onResponse(GetSourceResponse getSourceResponse) {
                System.out.println(getSourceResponse.getSource().toString());
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

    @Test
    public void sendRequestTest1() throws IOException {
        RestHighLevelClient client=new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200))
        );
        GetRequest request=new GetRequest("customer","0001");
        //??????
        boolean exists = false;
        try {
            exists=client.exists(request, RequestOptions.DEFAULT);
        }catch (Exception e){
            System.out.println("??????????????????!");
        }finally {
            if(exists){
                System.out.println("???????????????!");
            }else {
                System.out.println("??????????????????!");
            }
            client.close();
        }
    }

    @Test
    public void sendDeleteRequest() throws IOException {
        RestHighLevelClient client = HighLevelClientTool.getInstance();
        DeleteRequest request=new DeleteRequest("student","5");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        if(response.status().getStatus()==200){
            System.out.println("????????????:"+response.getResult().toString());
        }else {
            System.out.println("????????????:"+response.getResult().toString());
        }
        client.close();
    }

    @Test
    public void sendUpdateRequest() throws IOException {
        RestHighLevelClient client = HighLevelClientTool.getInstance();
        UpdateRequest request=new UpdateRequest("student","5");
        Map<String,Object>requestBody=new HashMap<>();
        requestBody.put("name","stu5-update");
        requestBody.put("age",24);
        request.doc(requestBody);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.getResult().toString());
    }

    /**
     * ??????bulkrequest??????????????????
     */
    @Test
    public void sendBulkRequest() throws IOException {
        RestHighLevelClient client=HighLevelClientTool.getInstance();
        //??????bulkrequest
        BulkRequest bulkRequest=new BulkRequest();
        //???bulkrequest?????????????????????
        //???
        Map<String,Object>studentMap=new HashMap<>();
        studentMap.put("id",6);
        studentMap.put("name","stu6");
        studentMap.put("address","hangzhou");
        bulkRequest.add(new IndexRequest("student").id("6").source(studentMap));
        //???
        bulkRequest.add(new DeleteRequest("student","5"));
        //???
        bulkRequest.add(new UpdateRequest("student","4").doc(XContentType.JSON,"name","stu4-update"));
        //????????????
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
        BulkItemResponse[] items = response.getItems();
        for(BulkItemResponse item:items){
            System.out.println(item.getOpType().toString());
        }
        client.close();

    }

    /**
     * ??????searchRequest
     */
    @Test
    public void sendSearchRequest() throws IOException {
        RestHighLevelClient client = HighLevelClientTool.getInstance();
        //indices:index????????????????????????
        SearchRequest request=new SearchRequest();
        //????????????
        request.indices("customer","student","desktop");
        //??????????????????SearchSourceBuilder?????????
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //??????????????????
        builder.query(QueryBuilders.matchAllQuery());
//        builder.fetchSource("id","address");
//        builder.query(QueryBuilders.termQuery("username","user1"));
//        builder.query(QueryBuilders.fuzzyQuery("programName","??????"));
//        builder.query(QueryBuilders.matchQuery("tags","level-2"));
//        builder.query(QueryBuilders.matchQuery("username","user").fuzziness(Fuzziness.AUTO));
//        builder.sort(new FieldSortBuilder("uid").order(SortOrder.ASC));
        //????????????
//        HighlightBuilder highlightBuilder=new HighlightBuilder();
//        HighlightBuilder.Field highlightField=new HighlightBuilder.Field("username");
//        highlightBuilder.field(highlightField);
//        builder.highlighter(highlightBuilder);

        builder.from(0);
        builder.size(100);
        //???SearchSourceBuilder?????????SearchRequest???
        request.source(builder);
        request.routing("routing");
        //????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //??????????????????
        SearchHits hits = response.getHits();
        SearchHit[] hitsArray = hits.getHits();
        for(SearchHit tmp:hitsArray){
            System.out.println(tmp.getSourceAsString());
        }

    }

    /**
     * ????????????
     */
    @Test
    public void sendAggregationSearch() throws IOException {
        RestHighLevelClient client=HighLevelClientTool.getInstance();
        SearchRequest request=new SearchRequest("desktop");
        SearchSourceBuilder builder=new SearchSourceBuilder();
        String group="group";
        //??????????????????????????????
        //????????????
        AvgAggregationBuilder agg1 = AggregationBuilders.avg(group).field("programId");
        //??????
        TermsAggregationBuilder agg2 = AggregationBuilders.terms(group).field("programId");
        //builder??????????????????
        builder.aggregation(agg1);
        request.source(builder);
        System.out.println("????????????:"+ JSONTools.formatJSON(JSONObject.parseObject(builder.toString())));
        SearchResponse response= client.search(request, RequestOptions.DEFAULT);
        System.out.println("?????????:"+response.status());
        if(RestStatus.OK==response.status()){
            Aggregations aggregations = response.getAggregations();
            for(Aggregation aggregation:aggregations){
                System.out.println("type:"+aggregation.getType());
            }
//            Terms terms=aggregations.get(group);
//            List<? extends Terms.Bucket> buckets = terms.getBuckets();
//            for(Terms.Bucket tmp:buckets){
//                JSONObject jsonObject=new JSONObject();
//                jsonObject.put("key",tmp.getKeyAsNumber());
//                jsonObject.put("doc_count",tmp.getDocCount());
//                System.out.println(JSONTools.formatJSON(jsonObject));
//            }

//            Avg tmpTerm = aggregations.get(group);
//            System.out.println(tmpTerm.getValue());
        }
    }



}
