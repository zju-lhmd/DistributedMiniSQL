import java.util.*;
public class Client {
    private ClientManager clientManager;
    public Client(int myPort1, int myPort2) {
        clientManager = new ClientManager(myPort1, myPort2);
    }
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("正在连接分布式数据库...请输入两个空闲端口以创立连接: ");
        System.out.print(">");
        int myPort1, myPort2;
        myPort1 = scanner.nextInt();
        myPort2 = scanner.nextInt();
        Client client = new Client(myPort1, myPort2);
        scanner.nextLine();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("退出系统...");
            client.clientManager.closeAll();
        }));

        System.out.print(">");
        String sql = scanner.nextLine();
        while(!sql.equalsIgnoreCase("quit")) {
            if (sql.toLowerCase().startsWith("execute file")) {
                System.out.println(sql.substring(12).trim());
                client.clientManager.execFile(sql.substring(12).trim());
            } else {
                client.clientManager.execSingleSql(sql);
            }
            System.out.print(">");
            sql = scanner.nextLine();
        }
    }
}