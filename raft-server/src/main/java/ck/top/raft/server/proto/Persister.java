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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ois.close();
        return obj;
    }

    public void save(byte[] raftState, byte[] snapshot) {
        String filePath = getFilePath();
        File file = new File(filePath);
        file.getParentFile().mkdirs();  // 创建父目录
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(raftState);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readRaftState() {
        String filePath = getFilePath();
        File file = new File(filePath);
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
