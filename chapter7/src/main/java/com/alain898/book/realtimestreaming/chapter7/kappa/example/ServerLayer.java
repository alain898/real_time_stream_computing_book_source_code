package com.alain898.book.realtimestreaming.chapter7.kappa.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class ServerLayer {

    private static final Logger logger = LoggerFactory.getLogger(ServerLayer.class);

    private Connection connection;
    private PreparedStatement preparedStatement;

    private String jdbcClass = "com.mysql.jdbc.Driver";
    private String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/kappa?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    private String mysqlUser = "root";
    private String mysqlPassword = "111111";
    private String mergeQueryTemplate =
            "SELECT product, sum(v_count) as s_count from\n" +
                    "(\n" +
                    "SELECT * FROM table_counts WHERE start=[v1] AND end=[v2] AND layer='batch'\n" +
                    "UNION\n" +
                    "SELECT * FROM table_counts WHERE start>=[v3] AND end<=[v4] AND layer='fast'\n" +
                    ") as union_table GROUP BY product;";
    private String mergeQuery = mergeQueryTemplate.replaceAll("\\[v\\d+\\]", "?");


    public void open() throws Exception {
        Class.forName(jdbcClass);
        connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
        System.out.println(mergeQuery);
        preparedStatement = connection.prepareStatement(mergeQuery);
    }

    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public List<Tuple2<String, Integer>> query(long timestamp) throws Exception {
        long batchLayerEnd = timestamp / BatchLayer.BATCH_LAYER_SLIDE.toMilliseconds();
        long batchLayerStart = batchLayerEnd - BatchLayer.BATCH_LAYER_SLIDES_IN_WINDOW;
        long fastLayerEnd = timestamp / FastLayer.FAST_LAYER_SLIDE.toMilliseconds();
        long fastLayerStart = batchLayerEnd * BatchLayer.BATCH_LAYER_SLIDE.toMilliseconds()
                / FastLayer.FAST_LAYER_SLIDE.toMilliseconds();

        String sql = mergeQueryTemplate
                .replace("[v1]", String.valueOf(batchLayerStart))
                .replace("[v2]", String.valueOf(batchLayerEnd))
                .replace("[v3]", String.valueOf(fastLayerStart))
                .replace("[v4]", String.valueOf(fastLayerEnd));
        System.out.println("timestamp: " + timestamp);
        System.out.println(sql);

        preparedStatement.setLong(1, batchLayerStart);
        preparedStatement.setLong(2, batchLayerEnd);
        preparedStatement.setLong(3, fastLayerStart);
        preparedStatement.setLong(4, fastLayerEnd);
        ResultSet rs = preparedStatement.executeQuery();
        List<Tuple2<String, Integer>> countGroupBy = new ArrayList<>();
        while (rs.next()) {
            String product = rs.getString("product");
            int s_count = rs.getInt("s_count");
            countGroupBy.add(new Tuple2<>(product, s_count));
        }
        return countGroupBy;
    }

    public static void main(String[] args) throws Exception {
        ServerLayer serverLayer = new ServerLayer();
        serverLayer.open();
        List<Tuple2<String, Integer>> result = serverLayer.query(System.currentTimeMillis());
        result.forEach(System.out::println);
        serverLayer.close();
    }
}
