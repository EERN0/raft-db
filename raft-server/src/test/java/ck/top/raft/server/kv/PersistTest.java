package ck.top.raft.server.kv;

import ck.top.raft.server.log.LogEntry;
import ck.top.raft.server.proto.Persister;
import ck.top.raft.server.proto.RaftState;
import lombok.Builder;
import lombok.Data;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

@SpringBootTest
public class PersistTest {

    private static final Logger log = LoggerFactory.getLogger(PersistTest.class);

    @Builder
    @Data
    static class Student implements Serializable {
        String name;
        int age;
    }

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        Persister persister = new Persister("1000");
        byte[] byteArray = Persister.serialize("aaa");

        Object obj = Persister.deserialize(byteArray);
        System.out.println(obj.toString());

        System.out.println("=================================");

        Student student = Student.builder().name("啊啊啊").age(10).build();
        byte[] bytes = Persister.serialize(student);
        String str = Persister.deserialize(bytes).toString();
        System.out.println(str);
    }

    //@Test
    //public void testRaftStateSerializeAndDeserialize() throws IOException, ClassNotFoundException {
    //    ArrayList<LogEntry> log = new ArrayList<>();
    //    LogEntry entry = LogEntry.builder().index(1).term(1).command(null).isValid(false).build();
    //    log.add(entry);
    //    RaftState raftState = RaftState.builder().votedFor("-1").logs(log).build();
    //    System.out.println(raftState.toString());
    //
    //    byte[] bytes = Persister.serialize(raftState);
    //    RaftState newRaft = new RaftState();
    //    newRaft = (RaftState) Persister.deserialize(bytes);
    //    System.out.println(newRaft.toString());
    //}
    //
    //@Test
    //public void testSaveAndReadRaftState() throws IOException, ClassNotFoundException {
    //    String basePath = System.getProperty("os.name").startsWith("Windows") ? "D:\\Java\\project\\raft-KV\\" : "/";
    //    String path = basePath + "state/9991/raft_state.dat";
    //    File file = new File(path);
    //    if (!file.exists()) {
    //        log.error("路径不存在，{}", path);
    //    }
    //
    //    ArrayList<LogEntry> log = new ArrayList<>();
    //    LogEntry entry = LogEntry.builder().index(1).term(1).command(null).isValid(false).build();
    //    log.add(entry);
    //    RaftState raftState = RaftState.builder().votedFor("-1").logs(log).build();
    //    byte[] bytes = Persister.serialize(raftState);
    //    System.out.println("bytes = " + Arrays.toString(bytes));
    //
    //    // raft节点状态（字节数组）写入文件
    //    FileOutputStream fos = new FileOutputStream(file);
    //    fos.write(bytes);
    //    // 读取文件的字节数组
    //    FileInputStream fis = new FileInputStream(file);
    //    byte[] data = new byte[fis.available()];
    //    fis.read(data);
    //    System.out.println("data = " + Arrays.toString(data));
    //    // 反序列化，字节数组还原为raft节点状态
    //    RaftState raft = (RaftState)Persister.deserialize(data);
    //    System.out.println(raft.toString());
    //}
}
