package ck.top.raft.server.kv;

public class SkipListTest {
    public static void main(String[] args) {
        // 创建 SkipList 实例
        SkipList<String, String> skipList = SkipList.getInstance();

        // 测试插入
        System.out.println("插入键值对...");
        skipList.insert("key1", "value1");
        skipList.insert("key2", "value2");
        skipList.insert("key3", "value3");

        // 测试打印
        System.out.println("打印 SkipList:");
        skipList.print();

        // 测试查找
        System.out.println("查找 key2: " + skipList.get("key2"));  // 应返回 value2
        System.out.println("查找 key4: " + skipList.get("key4"));  // 应返回 null

        // 测试是否存在
        System.out.println("key1 是否存在? " + skipList.isExist("key1"));  // 应返回 true
        System.out.println("key4 是否存在? " + skipList.isExist("key4"));  // 应返回 false

        // 测试删除
        System.out.println("删除 key2...");
        skipList.remove("key2");

        System.out.println("删除后的 SkipList:");
        skipList.print();

        // 测试持久化
        System.out.println("将 SkipList 持久化到磁盘...");
        skipList.flush(skipList);

        // 创建新的 SkipList 实例并从磁盘加载
        System.out.println("从磁盘加载 SkipList...");
        SkipList<String, String> loadedSkipList = SkipList.getInstance();
        loadedSkipList.load(loadedSkipList);

        System.out.println("加载后的 SkipList:");
        loadedSkipList.print();
    }
}
