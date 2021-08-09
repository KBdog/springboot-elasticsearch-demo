package per.elasticsearch.demo.utils;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * @author lipeiyu
 * @package per.elasticsearch.demo.utils
 * @description
 * @date 2021/8/6 15:51
 */
public class DefaultXContentBuilderTool {
    public synchronized static XContentBuilder getSetting(){
        XContentBuilder setting= null;
        try {
            setting = XContentFactory.jsonBuilder();
            setting.startObject();{
                setting.startObject("index");{
                    setting.startObject("analysis");{
                        setting.startObject("filter");{
                            setting.startObject("edge_ngram_filter");{
                                setting.field("type","edge_ngram");
                                setting.field("min_gram","2");
                                setting.field("max_gram","10");
                            }setting.endObject();
                        }setting.endObject();
                        setting.startObject("analyzer");{
                            setting.startObject("text_analyzer");{
                                setting.field("tokenizer","ik_smart");
                                setting.startArray("filter");{
                                    setting.value("lowercase");
                                    setting.value("edge_ngram_filter");
                                }setting.endArray();
                            }setting.endObject();
                        }setting.endObject();
                    }setting.endObject();
                }setting.endObject();
            }setting.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return setting;
    }
}
