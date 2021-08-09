package per.elasticsearch.demo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.config
 * @description
 * @date 2021/8/4 17:35
 */
@Configuration
public class ElasticSearchConfig {
    @Value("${elasticsearch.host}")
    private String host;
    @Value("${elasticsearch.port}")
    private Integer port;
    @Value("${elasticsearch.protocol}")
    private String protocol;

    @Bean(name = "restHighLevelClient")
    public RestHighLevelClient restHighLevelClient(){
        return new RestHighLevelClient(RestClient.builder(new HttpHost(host,port,protocol)));
    }
}
