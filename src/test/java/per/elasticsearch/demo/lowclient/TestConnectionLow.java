package per.elasticsearch.demo.lowclient;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;

import java.io.IOException;

public class TestConnectionLow {
    public static void main(String[] args) throws IOException {
        //构造器
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );
        //设置每一次的请求头
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);
        //设置监听器
        builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(Node node) {
                System.out.println("出现错误的节点名:"+node.getName());
            }
        });
        //为客户端向子节点传递信息设置选择器
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        //设置请求配置的回调
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectionRequestTimeout(2000);
            }
        });
        //设置http客户端配置
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setProxy(new HttpHost("localhost",1080,"http"));
            }
        });
        //获取客户端client
        RestClient client = builder.build();
        System.out.println(client.isRunning());
        client.close();
    }
}
