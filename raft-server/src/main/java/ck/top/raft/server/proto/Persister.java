package ck.top.raft.server.proto;

import java.io.*;

public class Persister {
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
        try (FileOutputStream fos = new FileOutputStream("raft_state.dat")) {
            fos.write(raftState);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readRaftState() {
        try (FileInputStream fis = new FileInputStream("raft_state.dat")) {
            byte[] data = new byte[fis.available()];
            fis.read(data);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
