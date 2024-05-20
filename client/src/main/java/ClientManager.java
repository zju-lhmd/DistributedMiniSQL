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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientManager {
    private CacheManager cacheManager;
    private MasterConnector masterConnector;
    private String myHost;
    private int myPort1 = 8081;
    private int myPort2 = 8082;
    private final String masterHost = "10.193.135.166";
    private final int masterPort = 3932;
    private static final Logger logger = LoggerFactory.getLogger(ClientManager.class);

    public ClientManager(int myPort1, int myPort2) {
        try {
            myHost = InetAddress.getLocalHost().getHostAddress();
            masterConnector = new MasterConnector(masterHost, masterPort, myHost, myPort1);
        } catch (Exception e) {
            System.out.print("无法连接至网络！");
        }
        cacheManager = new CacheManager(100);
        this.myPort1 = myPort1;
        this.myPort2 = myPort2;
    }

    public void execSingleSql(String sql) {
        if(sql.equalsIgnoreCase("print cache")) {
            System.out.println("当前缓存内容: ");
            System.out.println(cacheManager.cache);
            return;
        }
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
        } catch (Exception e) {
            System.out.println("不支持的指令: " + sql);
        }
        if(sql_type == -1) return;

        boolean finish = false;
        List<String> tableLoc=null;
        if(sql_type==0||sql_type==1) {
            tableLoc = cacheManager.searchCache(tableName);
            if(tableLoc==null) {
                //System.out.println("缓存未命中");
                tableLoc = query4loc(tableName);
                if(tableLoc==null) {
                    finish = true;
                    System.out.println("不存在的表: " + tableName);
                } else {
                    System.out.println(tableLoc);
                    cacheManager.addCache(tableName,tableLoc);
                }
            } else {
                logger.info("缓存命中: " + tableLoc);
            }
        }
        //finish = true;
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
                String[] loc = tableLoc.get(0).split(":");
                String regionHost = loc[0];
                int regionPost = Integer.parseInt(loc[1]);
                int result = writeByRegion(regionHost, regionPost, sql, tableName);
                if(result == 0) {
                    System.out.println("写执行完毕: " + sql);
                    finish = true;
                } else if (result == 3){
                    finish = true;
                    break;
                } else {
                    finish = false;
                    tableLoc = null;
                }
            } else if(sql_type==1) {
                ////// 交由Region
                String[] loc = tableLoc.get(1).split(":");
                String regionHost = loc[0];
                int regionPost = Integer.parseInt(loc[1]);
                Hits result = readByRegion(regionHost, regionPost, sql);
                if(result != null) {
                    if(result.schema == null) {
                        System.out.println("结果为空: " + sql);
                        break;
                    }else if(result.records==null) {
                        break;
                    }
                    System.out.println("读执行完毕: " + sql);
                    List<String> columns = new ArrayList<>(Arrays.asList(result.schema.split(" ")));
                    List<List<String>> rows = new ArrayList<>();
                    for(String record : result.records) {
                        List<String> row = new ArrayList<>(Arrays.asList(record.split(" ")));
                        rows.add(row);
                    }
                    printTable(columns, rows);
                    finish = true;
                } else {
                    finish = false;
                    tableLoc = null;
                }
            } else if(sql_type==2) {
                ////// 交由Master
                List<String> result = createTable(tableName, sql);
                if(result != null) {
                    System.out.println("建表执行成功: " + sql);
                    cacheManager.addCache(tableName, result);
                } else {
                    System.out.println("建表执行失败，表已存在: " + sql);
                }
                finish = true;
            } else {
                ////// 交由Master
                int result = dropTable(tableName);
                if(result == 0) {
                    System.out.println("删表执行成功: " + sql);
                    cacheManager.removeCache(tableName);
                } else {
                    System.out.println("删建表执行失败，表不存在: " + sql);
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
            System.out.println("文件不存在！");
        } catch (IOException e) {
            System.out.println("读取错误！");
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
            logger.info("尝试向[" + regionHost + ":" + regionPost + "]发送读请求...");
            RegionConnector regionConnector = new RegionConnector(regionHost, regionPost, myHost, myPort2);
            CompletableFuture<Hits> future = regionConnector.read(myHost+":"+myPort2, sql);
            Hits result = future.get();
            regionConnector.closeConnection();
            if(result.schema!=null && result.schema.equals("")) {
                logger.info("地址错误，正在重新查询地址...");
                return null;
            } else if(result.schema!=null && result.records==null) {
                System.out.println("SQL执行错误: "+result.schema);
                return result;
            }
            return result;
        } catch (Exception e) {
            logger.info("地址错误，正在重新查询地址...");
            return null;
        }
    }
    private int writeByRegion(String regionHost, int regionPost, String sql, String tableName) {
        try {
            logger.info("尝试向[" + regionHost + ":" + regionPost + "]发送读请求...");
            RegionConnector regionConnector = new RegionConnector(regionHost, regionPost, myHost, myPort2);
            CompletableFuture<Hits> future = regionConnector.write(myHost+":"+myPort2, sql, tableName);
            Hits result = future.get();
            regionConnector.closeConnection();
            if(result!=null) {
                if(result.schema.equals(""))
                    return 0;
                else {
                    System.out.println("SQL执行时发生错误: "+ result.schema);
                    return 3;
                }
            } else {
                logger.info("地址错误，正在重新查询地址...");
                return 2;
            }
        } catch (Exception e) {
            logger.info("地址错误，正在重新查询地址...");
            return 2;
        }
    }
    private List<String> query4loc(String tableName) {
        try {
            CompletableFuture<List<String>> future = masterConnector.query(myHost+":"+myPort1, tableName);
            List<String> result = future.get();
            if(result!=null) {
                logger.info("向Master查询 " + tableName + " 地址，结果为: " + result);
            }
            return result;
        } catch (Exception e) {
            //发生错误
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
            return 2;
        }
    }
    public void closeAll() {
        masterConnector.closeConnection();
    }
}
