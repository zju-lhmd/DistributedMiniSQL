import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import java.util.*;
import java.lang.*;
import java.nio.file.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ClientManager {
    private CacheManager cacheManager;
    private MasterConnector masterConnector;
    private String myHost;
    private final int myPort1 = 8081;
    private final int myPort2 = 8082;
    private final String masterHost = "10.192.192.131";
    private final int masterPort = 3932;
    public ClientManager() {
        try {
            myHost = InetAddress.getLocalHost().getHostAddress();
            System.out.println(myHost);
            masterConnector = new MasterConnector(masterHost, masterPort, myHost, myPort1);
        } catch (Exception e) {
            System.out.print("无法连接至网络");
            e.printStackTrace();
        }
        cacheManager = new CacheManager(1000);
    }

    public void execSingleSql(String sql) {
        Statement statement;
        int sql_type = -1;
        String tableName="";
        try {
            statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Insert) {
                sql_type = 0;
                Insert insertStatement = (Insert) statement;
                tableName = insertStatement.getTable().getName();
            } else if (statement instanceof Delete) {
                sql_type = 0;
                Delete deleteStatement = (Delete) statement;
                tableName = deleteStatement.getTable().getName();
            } else if (statement instanceof Update) {
                sql_type = 0;
                Update updateStatement = (Update) statement;
                tableName = updateStatement.getTable().getName();
            } else if (statement instanceof Select) {
                sql_type = 1;
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(statement);
                tableName = tableList.get(0);
                //System.out.println("Table: " + tableList.get(0)); // 获取列表中的第一个表名
            } else if (statement instanceof CreateTable) {
                sql_type = 2;
                CreateTable createStatement = (CreateTable) statement;
                tableName = createStatement.getTable().getName();
            } else if (statement instanceof Drop) {
                sql_type = 3;
                Drop dropStatement = (Drop) statement;
                tableName = dropStatement.getName().getName();
            } else {
                // 非法操作！
                sql_type = -1;
                throw new Exception();
            }
        } catch (JSQLParserException e) {
            // 语法错误！
            sql_type = -1;
            System.out.println("SQL语法错误: " + sql);
            //e.printStackTrace();
        } catch (Exception e) {
            System.out.println("不支持的指令: " + sql);
        }
        if(sql_type == -1) return;

        System.out.println(sql_type);
        System.out.println(tableName);

        boolean finish = true;
        List<String> tableLoc=null;
        if(sql_type==0||sql_type==1) {
            tableLoc = cacheManager.searchCache(tableName);
            if(tableLoc==null) {
                // 缓存未命中
                System.out.println("缓存未命中");
                ////// 查询Master
                /*here
                */
                tableLoc = query4loc(tableName);
                if(tableLoc==null) {
                    finish = true;
                    System.out.println("不存在的表: " + tableName);
                } else {
                    System.out.println(tableLoc);
                    cacheManager.addCache(tableName,tableLoc);
                }
                //tableLoc = new ArrayList<>();
                //cacheManager.addCache(tableName,tableLoc);
                //////
            }
        }
        finish = true;
        while(!finish) {
            // 检测命中 -> 查询Master -> 交由Master/交由Region -> 未结束 ->
            if((sql_type==0||sql_type==1) && tableLoc == null) {
                tableLoc = query4loc(tableName);
                if(tableLoc==null) {
                    finish = true;
                    System.out.println("不存在的表: " + tableName);
                    break;
                } else {
                    cacheManager.addCache(tableName,tableLoc);
                }
            }
            if(sql_type==0) {
                ////// 交由Region
                System.out.println("交由Region写");
                /*here
                 */
                System.out.println(tableLoc.get(0));
                String[] loc = tableLoc.get(0).split(":");
                String regionHost = loc[0];
                int regionPost = Integer.parseInt(loc[1]);
                int result = writeByRegion(regionHost, regionPost, sql);
                if(result == 0) {
                    System.out.println("执行成功: " + sql);
                    finish = true;
                } else {
                    System.out.println("执行失败: " + sql);
                    finish = false;
                    tableLoc = null;
                }
                //////
            } else if(sql_type==1) {
                ////// 交由Region
                System.out.println("交由Region读");
                /*here
                 */
                System.out.println(tableLoc.get(1));
                String[] loc = tableLoc.get(1).split(":");
                String regionHost = loc[0];
                int regionPost = Integer.parseInt(loc[1]);
                Hits result = readByRegion(regionHost, regionPost, sql);
                if(result != null) {
                    System.out.println("执行成功: " + sql);
                    List<String> columns = new ArrayList<>(Arrays.asList(result.schema.split("/")));
                    List<List<String>> rows = new ArrayList<>();
                    for(String record : result.records) {
                        List<String> row = new ArrayList<>(Arrays.asList(record.split("/")));
                        rows.add(row);
                    }
                    printTable(columns, rows);
                    finish = true;
                } else {
                    System.out.println("执行失败: " + sql);
                    finish = false;
                    tableLoc = null;
                }
                //////
            } else if(sql_type==2) {
                ////// 交由Master
                System.out.println("交由Master建");
                /*here
                 */
                List<String> result = createTable(tableName, sql);
                if(result != null) {
                    System.out.println("执行成功: " + sql);
                    cacheManager.addCache(tableName, result);
                } else {
                    System.out.println("执行失败: " + sql);
                }
                //////
                finish = true;
            } else {
                ////// 交由Master
                System.out.println("交由Master删");
                //////
                /*here
                 */
                int result = dropTable(tableName);
                if(result == 0) {
                    System.out.println("执行成功: " + sql);
                    cacheManager.removeCache(tableName);
                } else {
                    System.out.println("执行失败: " + sql);
                }
                finish = true;
            }
        }
    }
    public void execFile(String filePath) {
        Path path = Paths.get(filePath);
        try {
            List<String> lines = Files.readAllLines(path);
            for (String line : lines) {
                execSingleSql(line);
            }
        } catch (NoSuchFileException e) {
            System.out.println("文件不存在");
        } catch (IOException e) {
            System.out.println("未知的读取错误");
        }
    }
    // 绘制列表
    private static void printTable(List<String> columns, List<List<String>> rows)  {
        // 计算列宽
        int[] maxWidth = new int[columns.size()];
        for(int i = 0; i < columns.size(); i++) {
            maxWidth[i] = columns.get(i).length();
            for(List<String> row : rows) {
                if (row.get(i).length() > maxWidth[i]) {
                    maxWidth[i] = row.get(i).length();
                }
            }
        }

        StringBuilder formatBuilder = new StringBuilder();
        for (int width : maxWidth) {
            formatBuilder.append("| %-" + width + "s ");
        }
        formatBuilder.append("|%n");

        for(int width : maxWidth) {
            for(int i = 0; i < width+3; i++) {
                System.out.print("-");
            }
        }
        System.out.println("-");

        System.out.printf(formatBuilder.toString(), columns.toArray());

        for(int width : maxWidth) {
            for(int i = 0; i < width + 3; i++) {
                System.out.print("-");
            }
        }
        System.out.println("-");

        for(List<String> row : rows) {
            System.out.printf(formatBuilder.toString(), row.toArray());
        }

        for(int width : maxWidth) {
            for(int i = 0; i < width + 3; i++) {
                System.out.print("-");
            }
        }
        System.out.println("-");
    }
    private Hits readByRegion(String regionHost, int regionPost, String sql) {
        try {
            RegionConnector regionConnector = new RegionConnector(regionHost, regionPost, myHost, myPort2);
            CompletableFuture<Hits> future = regionConnector.read(myHost+":"+myPort2, sql);
            Hits result = future.get();
            regionConnector.closeConnection();
            return result;
        } catch (Exception e) {
            //连接失败，尝试重试
            System.out.println("连接错误...请等待后重试");
            return null;
        }
    }
    private int writeByRegion(String regionHost, int regionPost, String sql) {
        try {
            RegionConnector regionConnector = new RegionConnector(regionHost, regionPost, myHost, myPort2);
            CompletableFuture<Hits> future = regionConnector.write(myHost+":"+myPort2, sql);
            Hits result = future.get();
            regionConnector.closeConnection();
            if(result!=null)
                return 0;
            else
                return 1;
        } catch (Exception e) {
            //连接失败，尝试重试
            System.out.println("连接错误...请等待后重试");
            return 2;
        }
    }
    private List<String> query4loc(String tableName) {
        try {
            CompletableFuture<List<String>> future = masterConnector.query(myHost+":"+myPort1, tableName);
            List<String> result = future.get();
            return result;
        } catch (Exception e) {
            //发生错误
            System.out.println("连接错误...请等待后重试");
            return null;
        }
    }
    private List<String> createTable(String tableName, String sql) {
        try {
            CompletableFuture<List<String>> future = masterConnector.create(myHost+":"+myPort1, tableName, sql);
            List<String> result = future.get();
            //是否成功
            return result;
        } catch (Exception e) {
            //连接失败，尝试重试
            System.out.println("连接错误...请等待后重试");
            return null;
        }
    }
    private int dropTable(String tableName) {
        try {
            CompletableFuture<List<String>> future = masterConnector.drop(myHost+":"+myPort1, tableName);
            List<String> result = future.get();
            //是否成功
            if(result!=null)
                return 0;
            else
                return 1;
        } catch (Exception e) {
            //连接失败，尝试重试
            System.out.println("连接错误...请等待后重试");
            return 2;
        }
    }
    public void closeAll() {
        masterConnector.closeConnection();
    }
}
