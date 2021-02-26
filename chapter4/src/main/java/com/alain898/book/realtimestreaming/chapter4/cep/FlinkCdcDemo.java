package com.alain898.book.realtimestreaming.chapter4.cep;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 有两种方式实现Flink CDC，一种是使用 Table/SQL API 方式，另外一种是使用 DataStream API 方式。
 *
 * 这里的 FlinkCdcDemo 类，是用 DataStream API 实现 Flink CDC 功能。
 */
public class FlinkCdcDemo {
    private static final Logger logger = LoggerFactory.getLogger(FlinkCdcDemo.class);

    public static void main(String[] args) throws Exception {

        // 源数据库
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("db001")
                .username("root")       // 测试用，生产不要用root账号！
                .password("123456")     // 测试用，生产不要用这种简单密码！
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        // 目标数据库
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    private IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        return Requests.indexRequest()
                                .index("table001")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // 实验时配置逐条插入，生产为了提升性能的话，可以改为批量插入
        esSinkBuilder.setBulkFlushMaxActions(1);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(sourceFunction)  // 设置源数据库
                .addSink(esSinkBuilder.build())  // 设置目标数据库
                .setParallelism(1); // 设置并行度为1，以保持消息的顺序

        env.execute("FlinkCdcDemo");
    }
}
