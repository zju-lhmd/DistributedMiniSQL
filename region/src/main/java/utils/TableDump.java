package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TableDump {

    private static final String DbName = "ddb";

    /**
     * 查询数据库中的表
     * @param username 账号
     * @param pwd 密码
     * @param url 地址
     * @return tableNames 表名
     */
    public static List showTables(String username, String pwd, String url) throws Exception {
        // mysql -hlocalhost -uroot -proot -e "use ddb; show tables;"
        StringBuilder sb = new StringBuilder();
        sb.append("mysql");
        sb.append(" -h").append(url);
        sb.append(" -u").append(username);
        sb.append(" -p").append(pwd);
        sb.append(" -e \"use ").append(DbName).append("; show tables;\"");
        System.out.println("cmd命令为："+sb.toString());

        Process process = getProcess(sb);
        System.out.println("开始查询表");

    }

    /**
     * 备份mysql数据库
     * @param username 账号
     * @param pwd 密码
     * @param url 地址
     * @param path 路径
     * @param tableName 表名
     */
    public static void dbBackUpMysql(String username, String pwd, String url, String path, String tableName) throws Exception {
        // mysqldump -h 127.0.0.1 -uroot -proot mysql user >D:/info/server/var/backupdata/backups.sql
        String dbName = DbName;
        dbName += " " + tableName;
        String pathSql = path + tableName + ".sql";
        File fileSql = new File(pathSql);
        File filePath = new File(path);
        //创建备份sql文件
        if (!filePath.exists()){
            filePath.mkdirs();
        }
        if (!fileSql.exists()){
            fileSql.createNewFile();
        }
        // mysqldump -hlocalhost -uroot -p123456 db > /home/back.sql
        StringBuilder sb = new StringBuilder();
        sb.append("mysqldump");
        sb.append(" -h").append(url);
        sb.append(" -u").append(username);
        sb.append(" -p").append(pwd);
        sb.append(" ").append(dbName).append(" >");
        sb.append(pathSql);
        System.out.println("cmd命令为：" + sb.toString());
        System.out.println("开始备份：" + dbName);

        Process process = getProcess(sb);
        //等待上述命令执行完毕后打印log
        process.waitFor();
        //输出返回的错误信息
        StringBuilder mes = new StringBuilder();
        String tmp = "";
        BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        while ((tmp = error.readLine()) != null) {
            mes.append(tmp).append("\n");
        }
        System.out.println("备份成功! ==> " + mes.toString());
        error.close();
    }

    /**
     * 数据库还原
     * @param username 账号
     * @param pwd 密码
     * @param url 地址
     * @param path 文件存放路径
     * @param tableName 表名
     */
    public static void dbRestoreMysql(String username, String pwd, String url, String path, String tableName) throws Exception{
        // mysql -hlocalhost -uroot -proot db < /home/back.sql
        StringBuilder sb = new StringBuilder();
        sb.append("mysql");
        sb.append(" -h").append(url);
        sb.append(" -u").append(username);
        sb.append(" -p").append(pwd);
        sb.append(" ").append(DbName).append(" <");
        sb.append(path).append(tableName).append(".sql");
        System.out.println("cmd命令为："+sb.toString());

        Process process = getProcess(sb);
        System.out.println("开始还原数据");
        InputStream is = process.getInputStream();
        BufferedReader bf = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bf.readLine())!=null) {
            System.out.println(line);
        }
        is.close();
        bf.close();
        System.out.println("还原成功！");
    }

    private static Process getProcess(StringBuilder sb) throws Exception {
        Process process;
        // 判断操作系统windows与linux使用的语句不一样
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            process = Runtime.getRuntime().exec("cmd /c"+sb.toString());
        } else if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            process = Runtime.getRuntime().exec("/bin/sh -c"+sb.toString());
        } else {
            throw new Exception("暂不支持该操作系统，进行数据库备份或还原！");
        }
        return process;
    }
}
