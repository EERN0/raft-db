package ck.top.raft.server.proto;

import java.io.*;

public class Persister {

    private String port;

    public Persister(String port) {
        this.port = port;
    }

    private String getFilePath() {
        return "state/" + port + "/raft_state.dat";
    }

    public static byte[] serialize(Object obj) throws IOException {
        // 字节输出流（内存缓冲区）
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // 对象输出流，将对象数据写入内存缓冲区baos
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        // 把字节数组转回对象
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        // 对象输入流，存储介质是内存缓冲区
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }

    public void save(byte[] raftStateBytes, byte[] snapshot) {
        String path = getFilePath();
        File file = new File(path);
        file.getParentFile().mkdirs();  // 创建父目录
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(raftStateBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readRaftState() {
        String path = getFilePath();
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[fis.available()];
            fis.read(data);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
