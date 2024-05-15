package utils;

import java.sql.*;
import java.util.ResourceBundle;

public class JdbcUtil {
    // 私有变量
    private static String url;
    private static String user;
    private static String password;

    // 静态块
    static {
        try {
            // 通过ResourceBundle类拿到数据库连接信息
            ResourceBundle resourceBundle = ResourceBundle.getBundle("db");
            String driver = resourceBundle.getString("driver");
            url = resourceBundle.getString("url");
            user = resourceBundle.getString("user");
            password = resourceBundle.getString("password");

            // 注册数据库的驱动
            Class.forName(driver);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    // 返回数据库连接
    public static Connection getConnection() {
        try {
            // 获取数据库连接（里面内容依次是：主机名和端口、用户名、密码）
            // 返回数据库连接
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // 关闭结果集
    public static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 关闭预处理对象
    public static void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 关闭数据库连接
    public static void closeConnection(Connection connection){
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 一次性关闭上面三个
    public static void closeResource(ResultSet resultSet,Statement statement,Connection connection) {
        closeResultSet(resultSet);
        closeStatement(statement);
        closeConnection(connection);
    }
}