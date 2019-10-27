package com.alain898.book.realtimestreaming.chapter7.kappa.example;


import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class JdbcWriter extends RichSinkFunction<CountedEvent> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcWriter.class);

    private Connection connection;
    private PreparedStatement preparedStatement;

    private String jdbcClass = "com.mysql.jdbc.Driver";
    private String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/kappa?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    private String mysqlUser = "root";
    private String mysqlPassword = "111111";
    private String inset_sql = "INSERT INTO table_counts(id,start,end,product,v_count,layer) VALUES(?,?,?,?,?,?) " +
            "ON DUPLICATE KEY UPDATE start=?,end=?,product=?,v_count=?,layer=?;";

    private long slideMS = 0;
    private long slideNumberInWindow = 0;
    private String layer = null;

    public JdbcWriter() {
    }

    public JdbcWriter(long slideMS, long slideNumberInWindow, String layer) {
        this.slideMS = slideMS;
        this.slideNumberInWindow = slideNumberInWindow;
        this.layer = layer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(jdbcClass);
        connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
        preparedStatement = connection.prepareStatement(inset_sql);
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(CountedEvent value, Context context) throws Exception {
        try {
// INSERT INTO table_counts(id,start,end,product,v_count) VALUES("c001",1566832263000,1566832263000,"prod_001", 10)
// ON DUPLICATE KEY UPDATE start="c001",start=1566832263000,end=1566832263000,product="prod_001",v_count=10;
            long start = value.minTimestamp / slideMS;
            long end = value.maxTimestamp / slideMS + slideNumberInWindow;
            String product = value.product;
            int v_count = value.count;
            String layer = this.layer;
            String id = DigestUtils.md5Hex(Joiner.on("&").join(Lists.newArrayList(start, end, product, layer)));

            preparedStatement.setString(1, id);
            preparedStatement.setLong(2, start);
            preparedStatement.setLong(3, end);
            preparedStatement.setString(4, product);
            preparedStatement.setInt(5, v_count);
            preparedStatement.setString(6, layer);
            preparedStatement.setLong(7, start);
            preparedStatement.setLong(8, end);
            preparedStatement.setString(9, product);
            preparedStatement.setInt(10, v_count);
            preparedStatement.setString(11, layer);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error(String.format("failed to insert value[%s]", JSONObject.toJSONString(value)), e);
        }
    }
}