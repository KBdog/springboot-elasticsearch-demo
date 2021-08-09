package per.elasticsearch.demo.lowclient;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import per.elasticsearch.demo.utils.JSONTools;
import java.io.IOException;


//发送同步请求
public class TestSendRequestSynLow {
    public static void main(String[] args) throws IOException {
        //创建构造器
        RestClientBuilder builder=RestClient.builder(new HttpHost("localhost",9200,"http"));
        //创建客户端
        RestClient restClient = builder.build();
        //创建请求
        Request request=new Request("GET","/desktop/_search");
        //设置请求体
        request.setJsonEntity("{\n" +
                "  \"aggs\": {\n" +
                "    \"programName_group\": {\n" +
                "      \"avg\": {\n" +
                "        \"field\": \"programId\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");
        //发送同步请求获得响应
        Response response = restClient.performRequest(request);
        HttpEntity entity = response.getEntity();
        String responseString = EntityUtils.toString(entity);
        //使用fastjson转换
        JSONObject jsonObject= JSONObject.parseObject(responseString);
        String formatJSON = JSONTools.formatJSON(jsonObject);
        System.out.println(formatJSON);
        //关闭客户端
        restClient.close();
    }
}
