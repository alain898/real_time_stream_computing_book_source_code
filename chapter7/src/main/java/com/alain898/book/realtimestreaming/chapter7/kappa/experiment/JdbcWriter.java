package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class JdbcWriter extends RichSinkFunction<Event> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    private String jdbcClass = "";
    private String mysqlUrl = "";
    private String mysqlUser = "";
    private String mysqlPassword = "";
    private String inset_sql = "";

    public JdbcWriter(String jdbcClass, String jdbcClass1, String jdbcClass2) {
        this.jdbcClass = jdbcClass;
        this.jdbcClass = jdbcClass1;
        this.jdbcClass = jdbcClass2;
    }

    public JdbcWriter() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(jdbcClass);
        // 获取数据库连接
        connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);//写入mysql数据库
        preparedStatement = connection.prepareStatement(inset_sql);//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        try {
            String name = String.valueOf(value);//获取JdbcReader发送过来的结果
            preparedStatement.setString(1, name);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}