package per.elasticsearch.demo.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.utils
 * @description
 * @date 2021/8/4 14:38
 */
public class HighLevelClientTool {
    private static RestHighLevelClient client=new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost",9200,"http"))
    );
    public static RestHighLevelClient getInstance(){
        return client;
    };
}
