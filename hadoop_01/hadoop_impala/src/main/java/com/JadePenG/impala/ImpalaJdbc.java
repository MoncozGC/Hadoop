package com.JadePenG.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Peng
 * @Description
 */
public class ImpalaJdbc {
    public static void main(String[] args) {
        //定义连接驱动类，以及连接url和执行的sql语句
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String driverUrl = "jdbc:hive2://node03:21050/myhive;auth=noSasl";
        String sql = "select * from user";

        //通过反射加载数据库连接驱动
        try {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(driverUrl);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            //通过查询，得到数据一共有多少列
            int count = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= count; i++) {
                    System.out.println(resultSet.getString(i) + "\t");
                }
                System.out.println("\t");
            }
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
