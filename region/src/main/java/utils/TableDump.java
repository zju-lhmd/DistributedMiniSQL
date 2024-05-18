package utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TableDump {

    private static final String DbName = "ddb";

    /**
     * 备份mysql数据库
     * @param username 账号
     * @param pwd 密码
     * @param url 地址
     * @param path 路径
     * @param tableName 表名
     */
    public static void dbBackUpMysql(String username, String pwd, String url, String path, String tableName) {
        // mysqldump -h 127.0.0.1 -uroot -proot mysql user >D:/info/server/var/backupdata/backups.sql
        try {
            String dbName = DbName;
            dbName += " " + tableName;
            String pathSql = path + tableName + ".sql";
            File fileSql = new File(pathSql);
            File filePath = new File(path);
            // 创建备份sql文件
            if (!filePath.exists()){
                filePath.mkdirs();
            }
            if (!fileSql.exists()){
                fileSql.createNewFile();
            }
            // mysqldump -hlocalhost -uroot -p123456 db --skip-comments > /home/back.sql
            StringBuilder sb = new StringBuilder();
            sb.append("mysqldump");
            sb.append(" -h" ).append("127.0.0.1"); // 默认本地数据库
            sb.append(" -u").append(username);
            sb.append(" -p").append(pwd);
            sb.append(" ").append(dbName);
            sb.append(" --skip-comments");
            sb.append(" >");
            sb.append(pathSql);
            System.out.println("cmd命令为：" + sb.toString());
            System.out.println("开始备份：" + dbName);

            Process process = getProcess(sb);
            // 等待上述命令执行完毕后打印log
            process.waitFor();
            // 输出返回的错误信息
            StringBuilder mes = new StringBuilder();
            String tmp = "";
            BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((tmp = error.readLine()) != null) {
                mes.append(tmp).append("\n");
            }
            System.out.println("备份成功! ==> " + mes.toString());
            error.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据库还原
     * @param username 账号
     * @param pwd 密码
     * @param url 地址
     * @param path 文件存放路径
     * @param tableName 表名
     */
    public static void dbRestoreMysql(String username, String pwd, String url, String path, String tableName) {
        // mysql -hlocalhost -uroot -proot db < /home/back.sql
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("mysql");
            sb.append(" -h").append("127.0.0.1");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] fileToBinary(String path) {
        byte[] buffer = null;
        try {
            File file = new File(path);
            FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
            byte[] b = new byte[1000];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer;
    }

    public static void binaryToFile(String path, byte[] fileData) {
        // 写到文件
        try {
            java.io.File file = new java.io.File(path);
            FileOutputStream fos = new FileOutputStream(file);
            FileChannel channel = fos.getChannel();
            channel.write(ByteBuffer.wrap(fileData));
            channel.close();
        } catch (Exception x) {
            x.printStackTrace();
        }
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
