package ck.top.raft.server.kv;

import lombok.Data;
import lombok.Getter;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 基于跳表实现的kv存储引擎
 */
public class SkipList<K extends Comparable<K>, V> {

    /**
     * 跳表节点
     */
    @Data
    class Node<K extends Comparable<K>, V> {
        private K key;
        private V value;
        private int level;                      // 节点所在的层级
        private ArrayList<Node<K, V>> next;     // cur.next.get(i): 节点cur在i层的下一个节点的引用

        Node(K key, V value, int level) {
            this.key = key;
            this.value = value;
            this.level = level;
            this.next = new ArrayList<>(Collections.nCopies(level + 1, null));
        }
    }

    /**
     * 跳表每层包含的节点
     */
    class Level2Nodes<K, V> implements Serializable {
        private int level;
        private List<String> nodes;

        public Level2Nodes(int level) {
            this.level = level;
            this.nodes = new ArrayList<>();
        }

        public int getLevel() {
            return level;
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void addNode(K key, V value) {
            this.nodes.add(key + ":" + value);
        }
    }

    // 单例
    private volatile static SkipList<?, ?> instance;

    private static final int MAX_LEVEL = 32;    // 跳表的最大层数

    @Getter
    private final Node<K, V> header;      // 每一层都有头节点，不存实际数据

    private int nodeNum;            // 跳表中的节点数量

    @Getter
    private int skipListLevel;      // 跳表当前的层级

    private final String storeFile;

    private SkipList() {
        // 初始化跳表头节点，其层级等于跳表的最大层级
        this.header = new Node<>(null, null, MAX_LEVEL);
        // 设置跳表当前层级为 0，节点计数为 0
        this.nodeNum = 0;
        this.skipListLevel = 0;

        storeFile = "./skipList-raft" + System.getProperty("server.port") + "/store";
    }

    public static <K extends Comparable<K>, V> SkipList<K, V> getInstance() {
        if (instance == null) {
            synchronized (SkipList.class) {
                if (instance == null) {
                    instance = new SkipList<>();
                }
            }
        }
        @SuppressWarnings("unchecked")
        SkipList<K, V> castInstance = (SkipList<K, V>) instance;
        return castInstance;
    }

    /**
     * 创建新的Node节点
     *
     * @param key   键
     * @param value 值
     * @param level 节点所在层级
     * @return 返回创建后的节点
     */
    private Node<K, V> createNode(K key, V value, int level) {
        return new Node<>(key, value, level);
    }

    /**
     * 按一定概率随机生成节点的最高层级（最高层及以下都包含这个节点）：
     * 第1层(最底层) 100%，第2层 50%，第3层 25%
     *
     * @return 返回节点层级
     */
    private static int generateRandomLevel() {
        int level = 0;
        Random random = new Random();
        while (random.nextInt(2) == 1) {    // [0,1]随机选一个的概率为0.5
            level++;
        }
        return Math.min(level, MAX_LEVEL);
    }

    /**
     * @return 返回跳表中节点的数量
     */
    public int size() {
        return this.nodeNum;
    }

    /**
     * 向跳表中插入一个键值对，如果跳表中已经存在相同 key 的节点，则更新这个节点的 value
     *
     * @param key   插入的 Node 的键
     * @param value 插入的 Node 的值
     * @return 插入成功返回 true，插入失败返回 false
     */
    public synchronized boolean insert(K key, V value) {
        // 从跳表最高层的头节点开始搜索 key 待插入的位置
        Node<K, V> cur = this.header;
        // 更新各层链表指向的数组，存待插入key节点的前驱
        ArrayList<Node<K, V>> prevNodes = new ArrayList<>(Collections.nCopies(MAX_LEVEL + 1, null));

        // 从最高层向下搜索插入位置
        for (int i = this.skipListLevel; i >= 0; i--) {
            // 寻找i层小于且最接近key的节点
            while (cur.getNext().get(i) != null && cur.getNext().get(i).getKey().compareTo(key) < 0) {
                cur = cur.getNext().get(i);
            }
            // i层cur的下一个节点 >= key，cur作为待插入节点的前驱
            prevNodes.set(i, cur);
        }
        // 移动到最底层的下一个节点，准备插入
        cur = cur.getNext().get(0);
        // 检查待插入的节点的key是否等于cur
        if (cur != null && cur.getKey().compareTo(key) == 0) {
            // key已存在，更新节点的value
            cur.setValue(value);
            return true;
        }

        // 随机生成节点的层数
        int randomLevel = generateRandomLevel();
        // 跳表中不存在这个key
        if (cur == null || cur.getKey().compareTo(key) != 0) {
            // 生成节点的层数 大于 跳表当前的层数
            if (randomLevel > this.skipListLevel) {
                for (int i = this.skipListLevel + 1; i <= randomLevel; i++) {
                    // 新建一层，开始为头节点
                    prevNodes.set(i, header);
                }
                // 更新跳表的最高层数
                this.skipListLevel = randomLevel;
            }
            // 待插入的节点
            Node<K, V> node = createNode(key, value, randomLevel);

            // 修改每一层里面链表节点的指向
            for (int i = 0; i <= randomLevel; i++) {
                // i层 当前节点的next指向 前驱节点的next
                node.getNext().set(i, prevNodes.get(i).getNext().get(i));
                prevNodes.get(i).getNext().set(i, node);
            }
            nodeNum++;
            return true;
        }
        return false;
    }

    /**
     * 判断跳表中是否存在键为 key 的键值对
     *
     * @param key 键
     * @return 跳表中存在键为 key 的键值对返回 true，不存在返回 false
     */
    public boolean isExist(K key) {
        Node<K, V> cur = this.header;

        for (int i = this.skipListLevel; i >= 0; i--) {
            while (cur.getNext().get(i) != null && cur.getNext().get(i).getKey().compareTo(key) < 0) {
                cur = cur.getNext().get(i);
            }
            // cur小于key，cur下一个节点为null 或者  >= key，直接去下一层搜
        }
        // cur仍然小于key，拿到最底层的 >= key的节点
        cur = cur.getNext().get(0);
        return cur != null && cur.getKey().compareTo(key) == 0;
    }

    /**
     * 查找节点key对应的value
     *
     * @param key 键
     * @return 返回键为 key 的节点，如果不存在则返回 null
     */
    public V get(K key) {
        Node<K, V> cur = this.header;
        for (int i = this.skipListLevel; i >= 0; i--) {
            while (cur.getNext().get(i) != null && cur.getNext().get(i).getKey().compareTo(key) < 0) {
                cur = cur.getNext().get(i);
            }
        }
        cur = cur.getNext().get(0);
        if (cur != null && cur.getKey().compareTo(key) == 0) {
            return cur.getValue();
        }
        return null;
    }

    /**
     * 根据key移除节点
     *
     * @param key 键
     * @return 成功返回true；失败返回false
     */
    public synchronized boolean remove(K key) {
        Node<K, V> cur = this.header;
        ArrayList<Node<K, V>> prevNodes = new ArrayList<>(Collections.nCopies(MAX_LEVEL + 1, null));

        // 从最高层开始向下搜索待删除的节点
        for (int i = this.skipListLevel; i >= 0; i--) {
            while (cur.getNext().get(i) != null && cur.getNext().get(i).getKey().compareTo(key) < 0) {
                cur = cur.getNext().get(i);
            }
            // cur小于key，是待删除节点的前驱，记录每一层的前驱
            prevNodes.set(i, cur);
        }
        // 移动到最底层 第一个为null 或者 大于等于 key 的节点
        cur = cur.getNext().get(0);

        // cur值为key，是待删除的节点
        if (cur != null && cur.getKey().compareTo(key) == 0) {
            // 删掉每一层为key的节点
            for (int i = 0; i <= this.skipListLevel; i++) {
                // i层cur节点的前驱 prevNodes.get(i)
                if (prevNodes.get(i).getNext().get(i) != cur) break;

                prevNodes.get(i).getNext().set(i, cur.getNext().get(i));
            }
            // 更新跳表的层数，删去只剩头节点的层，保存 level 0
            while (this.skipListLevel > 0 && this.header.getNext() == null) {
                this.skipListLevel--;
            }

            this.nodeNum--;
            return true;
        }
        return false;
    }

    /**
     * 保存跳表结构
     *
     * @return 返回所创建的跳表
     */
    public List<Level2Nodes<K, V>> show() {
        List<Level2Nodes<K, V>> levels = new ArrayList<>();

        for (int i = this.skipListLevel; i >= 0; i--) {
            Node<K, V> cur = this.header.getNext().get(i);
            Level2Nodes<K, V> level = new Level2Nodes<>(i);
            while (cur != null) {
                level.addNode(cur.getKey(), cur.getValue());
                cur = cur.getNext().get(i);
            }
            levels.add(level);
        }
        return levels;
    }

    /**
     * 打印跳表的层次结构和key-value
     */
    public void print() {
        // 从最上层开始，一层层遍历
        for (int i = this.skipListLevel; i >= 0; i--) {
            Node<K, V> cur = this.header.getNext().get(i);
            System.out.print("Level " + i + ": ");
            while (cur != null) {
                System.out.print(cur.getKey() + ":" + cur.getValue() + "; ");
                // 移动到i层的下一个节点
                cur = cur.getNext().get(i);
            }
            System.out.println();
        }
    }

    /**
     * 持久化跳表数据至磁盘文件
     *
     * @param skipList 跳表实例
     */
    public void flush(SkipList<K, V> skipList) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(storeFile))) {
            // 只需要存最底层的全量数据
            Node<K, V> node = skipList.getHeader().getNext().get(0);
            while (node != null) {
                String data = node.getKey() + ":" + node.getValue() + ";";
                bufferedWriter.write(data);
                bufferedWriter.newLine();
                node = node.getNext().get(0);
            }
        } catch (IOException e) {
            throw new RuntimeException("写入文件失败", e);
        }
    }

    /**
     * 从磁盘文件读取数据加载到内存的跳表实例
     *
     * @return 加载后的跳表实例
     */
    public SkipList<K, V> load(SkipList<K, V> skipList) {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(storeFile))) {
            String data;
            // 读出每一行的key value
            while ((data = bufferedReader.readLine()) != null) {
                Node<K, V> node = parse(data);
                if (node != null) {
                    skipList.insert(node.getKey(), node.getValue());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return skipList;
    }

    /**
     * 解析字符串，获取 key value
     *
     * @param data 字符串 key:value;
     * @return 将 key 和 value 封装到 Node 对象中
     */
    private Node<K, V> parse(String data) {
        if (!isValid(data)) return null;
        // 截取key
        String k1 = data.substring(0, data.indexOf(":"));
        K key = (K) k1;
        // 截取value
        String v1 = data.substring(data.indexOf(":") + 1, data.length() - 1);
        V value = (V) v1;
        return new Node<K, V>(key, value, 0);
    }

    /**
     * 验证读取的字符串是否为 k1:v1; 形式
     *
     * @param data 字符串
     */
    private boolean isValid(String data) {
        if (data == null || data.isEmpty()) {
            return false;
        }
        // 检查是否包含分号
        return data.contains(":");
    }
}
