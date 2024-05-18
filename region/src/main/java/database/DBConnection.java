package database;

import api.Hits;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import utils.JdbcUtil;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DBConnection {
    public static void update(String sql) throws SQLException {
        // 创建dbUtils里面的QueryRunner对象
        QueryRunner queryRunner = new QueryRunner();
        // 获取数据库连接
        Connection connection = JdbcUtil.getConnection();
        // 执行sql语句，并返回影响的行数
        int rows = queryRunner.update(connection, sql);
        // 关闭数据库连接
        JdbcUtil.closeConnection(connection);
    }

    public static Hits query(String sql) throws SQLException {
        // 创建dbUtils里面的QueryRunner对象
        QueryRunner queryRunner = new QueryRunner();
        // 获取数据库连接
        Connection connection = JdbcUtil.getConnection();
        // 执行查询，并以数组的形式返回查询结果（new ArrayListHandler()返回所有查询到的记录）
        List<Map<String, Object>> list = queryRunner.query(connection, sql, new MapListHandler());
        // 遍历集合
        StringBuilder schema = new StringBuilder();
        List<String> arr = new ArrayList<>();
        if (list.size() == 0) {
            return new Hits(null, null);
        }
        for (String key : list.get(0).keySet()) {
            schema.append(key).append(" ");
        }
        for (Map<String, Object> map : list) {
            StringBuilder record = new StringBuilder();
            for (String key : map.keySet()) {
                record.append(map.get(key)).append(" ");
            }
            arr.add(record.toString().trim());
        }
        // 关闭数据库连接
        JdbcUtil.closeConnection(connection);
        return new Hits(schema.toString().trim(), arr);
    }

    public static String showTables() {
        try {
            // 获取数据库连接
            Connection connection = JdbcUtil.getConnection();
            // 获取表名
            assert connection != null;
            Statement stmt = connection.createStatement();
            ResultSet tables = stmt.executeQuery("show tables");
            // 将表名拼接为字符串返回
            StringBuilder sb = new StringBuilder();
            while (tables.next()) {
                sb.append(tables.getString(1)).append(" ");
            }
            // 关闭数据库连接
            JdbcUtil.closeConnection(connection);
            JdbcUtil.closeStatement(stmt);
            JdbcUtil.closeResultSet(tables);

            return sb.toString().trim();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
