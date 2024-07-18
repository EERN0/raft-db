package ck.top.raft.client;

import java.util.Objects;
import java.util.Scanner;

public class MyClient {
    public static final int PORT = 9988;

    public static void main(String[] args) throws Throwable {

        // 启动服务器
        //RpcServer server = new RpcServer(PORT);

        KVClient kvClient = new KVClient();

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

            switch (command) {
                case "insert":
                    if (commandArgs.length != 3) {
                        System.out.println("Invalid command. Usage: insert <key> <value>");
                    } else {
                        String inserted = kvClient.insert(commandArgs[1], commandArgs[2]);
                        System.out.println(inserted);
                    }
                    break;
                case "get":
                    if (commandArgs.length != 2) {
                        System.out.println("Invalid command. Usage: get <key>");
                    } else {
                        String value = kvClient.get(commandArgs[1]);
                        if (value != null) {
                            System.out.println("Key: " + commandArgs[1] + " Value: " + value);
                        } else {
                            System.out.println("Key: " + commandArgs[1] + " not exist!");
                        }
                    }
                    break;
                case "delete":
                    if (commandArgs.length != 2) {
                        System.out.println("Invalid command. Usage: delete <key>");
                    } else {
                        String deleted = kvClient.delete(commandArgs[1]);
                        System.out.println(deleted);
                    }
                    break;
                default:
                    System.out.println("Invalid command!");
            }
        }
    }
}
