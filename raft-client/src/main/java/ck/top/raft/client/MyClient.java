package ck.top.raft.client;

import ck.top.raft.server.rpc.Request;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcServer;

import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class MyClient {
    public static final int PORT = 9988;

    public static void main(String[] args) throws Throwable {

        // 启动服务器
        RpcServer server = new RpcServer(PORT);

        RpcClient rpc = new RpcClient();
        String url = "localhost:9988";

        System.out.println("Enter command (insert <key> <value> / get <key> / delete <key> / display / exit): ");
        // 从键盘接收数据
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String input = sc.nextLine();
            String[] commandArgs = input.split(" ");

            if (commandArgs.length == 0) {
                System.out.println("Invalid command!");
                continue;
            }

            String command = commandArgs[0].toLowerCase();
            if (Objects.equals(command, "exit")) {
                break;
            }
            if (Objects.equals(command, "display")) {
                Request req = new Request(Request.DISPLAY, url);
                Object result = rpc.send(req);
                if (result instanceof List) {
                    displayLevels(result);
                } else {
                    System.out.println("Display error!");
                }
                continue;
            }

            switch (command) {
                case "insert":
                    if (commandArgs.length != 3) {
                        System.out.println("Invalid command. Usage: insert <key> <value>");
                    } else {
                        Request req = new Request(Request.INSERT, url, commandArgs[1], commandArgs[2]);
                        boolean inserted = rpc.send(req);
                        System.out.println("Insert " + (inserted ? "successful" : "failed"));
                    }
                    break;
                case "search":
                    if (commandArgs.length != 2) {
                        System.out.println("Invalid command. Usage: search <key>");
                    } else {
                        Request req = new Request(Request.SEARCH, url, commandArgs[1]);
                        boolean found = rpc.send(req);
                        System.out.println("Search " + (found ? "found" : "not found"));
                    }
                    break;
                case "get":
                    if (commandArgs.length != 2) {
                        System.out.println("Invalid command. Usage: delete <key>");
                    } else {
                        Request req = new Request(Request.GET, url, commandArgs[1]);
                        Object result = rpc.send(req);
                        if (result != null) {
                            System.out.println("Key: " + commandArgs[1] + " Value: " + result);
                        } else {
                            System.out.println("Key: " + commandArgs[1] + " not exist!");
                        }
                    }
                    break;
                case "delete":
                    if (commandArgs.length != 2) {
                        System.out.println("Invalid command. Usage: delete <key>");
                    } else {
                        Request req = new Request(Request.DELETE, url, commandArgs[1]);
                        boolean deleted = rpc.send(req);
                        System.out.println("Delete " + (deleted ? "successful" : "failed"));
                    }
                    break;
                default:
                    System.out.println("Invalid command!");
            }
        }
    }

    private static void displayLevels(List<SkipListLevel<String, String>> levels) {
        for (SkipListLevel<String, String> level : levels) {
            System.out.print("Level " + level.getLevel() + ": ");
            for (String node : level.getNodes()) {
                System.out.print(node + "; ");
            }
            System.out.println();
        }
    }

}
