package per.elasticsearch.demo.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import per.elasticsearch.demo.model.HttpEnum;
import per.elasticsearch.demo.model.ResponseResult;
import per.elasticsearch.demo.service.ElasticSearchService;

import javax.servlet.http.HttpServletResponse;
import javax.xml.ws.Response;
import java.util.List;
import java.util.Map;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.controller
 * @description
 * @date 2021/8/6 16:31
 */
@RequestMapping("/api")
@RestController
@Slf4j
public class ElasticSearchController {
    @Autowired
    private ElasticSearchService service;

    @GetMapping("/q/{keyword}")
    public ResponseResult searchByKeyword(@PathVariable("keyword")String keyword, HttpServletResponse response){
        log.info("keyword:"+keyword);
        List<JSONObject> results = service.searchByKeyword(keyword, "customer", "desktop", "student", "cms");
        if(results!=null&&results.size()>0){
            return ResponseResult.ok(results);
        }else {
            response.setStatus(HttpEnum.NOTFOUND.code());
            return ResponseResult.notFound();
        }
    }

    @PutMapping("/createIndex/{index}")
    public ResponseResult createdIndex(@PathVariable("index") String index, @RequestBody(required = false) Map<String,String>fields){
        boolean flag = service.createIndex(index, fields);
        if(flag){
            return ResponseResult.created();
        }else {
            return ResponseResult.invalid_request();
        }
    }

    @DeleteMapping("/deleteIndex")
    public ResponseResult deleteIndex(@RequestBody String index){
        boolean flag = service.deleteIndex(index);
        if(flag){
            return ResponseResult.ok("删除成功！");
        }else {
            return ResponseResult.invalid_request();
        }
    }

    @PutMapping("/createDocument/{index}/{id}")
    public ResponseResult createdDocument(@PathVariable("index")String index,
                                          @PathVariable("id")String id,@RequestBody String jsonString,
                                            HttpServletResponse response){
        boolean flag = service.createDocument(index, id, jsonString);
        if(flag){
            return ResponseResult.created();
        }else {
            response.setStatus(HttpEnum.INVALID_REQUEST.code());
            return ResponseResult.invalid_request();
        }
    }

    @GetMapping("/document/{index}/{id}")
    public ResponseResult getDocument(@PathVariable("index") String index,@PathVariable("id")String id){
        String result = service.getDocument(index, id);
        JSONObject jsonObject=JSONObject.parseObject(result);
        return ResponseResult.ok(jsonObject);
    }

    @PostMapping("/updateDocument/{index}/{id}")
    public ResponseResult updateDocument(@PathVariable("index") String index,@PathVariable("id")String id,
                                         @RequestBody String jsonString){
        boolean flag = service.updateDocument(index, id, jsonString);
        if(flag){
            return ResponseResult.ok();
        }else {
            return ResponseResult.invalid_request();
        }
    }

    @DeleteMapping("/deleteDocument")
    public ResponseResult deleteDocument(@RequestBody Map<String,String>requestBody){
        boolean flag = service.deleteDocument(requestBody.get("index"), requestBody.get("id"));
        if(flag){
            return ResponseResult.ok();
        }else {
            return ResponseResult.invalid_request();
        }
    }

    @GetMapping("/searchAllId/{index}")
    public ResponseResult searchAllId(@PathVariable("index")String index){
        List<String> list = service.searchAllId(index);
        if(list!=null&&list.size()>0){
            return ResponseResult.ok(list);
        }else {
            return ResponseResult.notFound();
        }
    }

}
