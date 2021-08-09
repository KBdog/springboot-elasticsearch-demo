package per.elasticsearch.demo.highclient;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.highclient
 * @description
 * @date 2021/8/4 11:25
 */
public class TestSendRequestHigh {
    public static void main(String[] args) throws IOException {
        //使用低级客户端构建高级客户端
        RestHighLevelClient client=new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost",9200,"http"))
        );
        GetRequest request = getGetRequest();
        //发起同步请求
        try {
            GetResponse responseSyn = client.get(request, RequestOptions.DEFAULT);
            if(responseSyn.isExists()){
                System.out.println("同步请求成功!");
                System.out.println(responseSyn.getSourceAsString());
            }
        }catch (ElasticsearchException e){
            System.out.println("状态码:"+e.status());
        }

        //发起异步请求
        Cancellable cancellable = client.getAsync(request, RequestOptions.DEFAULT,
                new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse documentFields) {
                        if(documentFields.isExists()){
                            System.out.println("异步请求成功!");
                            System.out.println(documentFields.getSourceAsString());
                        }
                        try {
                            client.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        System.out.println("异步请求失败!");
                        try {
                            client.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                });
    }

    //index api
    public static IndexRequest getIndexRequest(){
        IndexRequest request=new IndexRequest("customer");
        request.id("0001");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        return request.source(jsonString, XContentType.JSON);
    }
    //get api
    public static GetRequest getGetRequest(){
        GetRequest request=new GetRequest("customer1","0001");
        //为特定字段设置源包含,源排除
        String []includes=new String[]{"username"};
        String []excludes=new String[]{"uid"};
        request.fetchSourceContext(new FetchSourceContext(true,includes,excludes));
        return request;
    }
    //get source api
    public static GetSourceRequest getSourceRequest(){
        GetSourceRequest request=new GetSourceRequest("customer1","0001");
        String []includes=new String[]{"username"};
        String []excludes=new String[]{"uid"};
        request.fetchSourceContext(new FetchSourceContext(true,includes,excludes));
        request.refresh(true);
        return request;
    }
}
