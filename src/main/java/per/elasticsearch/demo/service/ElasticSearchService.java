package per.elasticsearch.demo.service;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.DocWriteRequest;

import java.util.List;
import java.util.Map;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.service
 * @description
 * @date 2021/8/4 17:41
 */
public interface ElasticSearchService {
    /**
     * 创建索引
     * @return
     */
    boolean createIndex(String index, Map<String,String>fields);

    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    boolean existIndex(String index);

    /**
     * 删除索引
     * @param index
     * @return
     */
    boolean deleteIndex(String index);

    /**
     * 新增文档
     * @param index
     * @param id
     * @param content
     * @return
     */
    boolean createDocument(String index,String id,String content);

    /**
     * 判断文档是否存在
     * @param index
     * @param id
     * @return
     */
    boolean existDocument(String index,String id);

    /**
     * 获取文档
     * @param index
     * @param id
     * @return
     */
    String getDocument(String index,String id);

    /**
     * 更新文档
     * @param index
     * @param id
     * @param content
     * @return
     */
    boolean updateDocument(String index,String id,String content);

    /**
     * 删除文档
     * @param index
     * @param id
     * @return
     */
    boolean deleteDocument(String index,String id);

    /**
     * 批量插入文档
     * @param index
     * @param document
     * @return
     */
    boolean bulkInsert(String index, List<String>document);


    /**
     * 批量请求:delete/index/update
     * @param index
     * @param requests
     * @return
     */
    boolean bulkRequest(String index,Iterable<DocWriteRequest<?>> requests);


    /**
     * 搜索请求
     * @param indices
     * @param keyword
     * @retur
     */
    List<JSONObject>searchByKeyword(String keyword,String...indices);

    /**
     * 查询某索引所有id
     * @param index
     * @return
     */
    List<String>searchAllId(String index);



}
