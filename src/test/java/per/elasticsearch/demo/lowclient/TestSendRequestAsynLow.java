package per.elasticsearch.demo.lowclient;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import per.elasticsearch.demo.utils.JSONTools;
import java.io.IOException;


//发送异步请求
public class TestSendRequestAsynLow {
    public static void main(String[] args) {
        //创建构造器
        RestClientBuilder builder=RestClient.builder(new HttpHost("localhost",9200,"http"));
        //创建客户端
        RestClient restClient = builder.build();
        //创建请求
        Request request=new Request("GET","/desktop/_search");
        //发送异步请求获取响应
        restClient.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    System.out.println("异步请求成功!");
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    //使用fastjson转换
                    JSONObject jsonObject= JSONObject.parseObject(responseString);
                    String formatJSON = JSONTools.formatJSON(jsonObject);
                    System.out.println(formatJSON);
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    try {
                        restClient.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            @Override
            public void onFailure(Exception e) {
                System.out.println("异步请求失败!"+e.getMessage());
                try {
                    restClient.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }
}
