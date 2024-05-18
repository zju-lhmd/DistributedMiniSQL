import java.util.*;
public class Client {
    private ClientManager clientManager;
    public Client() {
        clientManager = new ClientManager();
    }
    public static void main(String[] args) {
        Client client = new Client();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("退出");
            //here
            client.clientManager.closeAll();
        }));

        Scanner scanner = new Scanner(System.in);
        String sql = scanner.nextLine();
        while(!sql.equalsIgnoreCase("quit")) {
            if (sql.toLowerCase().startsWith("execute file")) {
                System.out.println(sql.substring(12).trim());
                client.clientManager.execFile(sql.substring(12).trim());
            } else {
                client.clientManager.execSingleSql(sql);
            }
            sql = scanner.nextLine();
        }
    }
}