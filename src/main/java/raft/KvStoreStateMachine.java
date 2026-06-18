package raft;

import core.LSMStorageEngine;
import core.SnapshotManager;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Ratis 状态机实现，将 Raft 日志命令应用到 LSM 存储引擎。
 * <p>
 * 支持的操作：
 * <ul>
 *   <li>PUT - 写入键值对</li>
 *   <li>DELETE - 删除键</li>
 *   <li>FLUSH - 触发 MemTable 刷盘</li>
 * </ul>
 * 读请求通过 {@link #query(Message)} 直接查状态机。
 */
public class KvStoreStateMachine extends BaseStateMachine {

    private LSMStorageEngine engine;
    private String dataDir;
    private SimpleStateMachineStorage stateMachineStorage;

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage)
            throws IOException {
        super.initialize(server, groupId, storage);
        // Ratis 会把 raft 元数据存在 storage.getStorageDir() 下
        // KV-DB 真实数据放在 {storageDir}/kvdb
        this.dataDir = storage.getStorageDir().getCurrentDir().toPath()
                .resolve("kvdb").toString();
        this.engine = new LSMStorageEngine(dataDir);

        // 初始化状态机存储，用于快照
        this.stateMachineStorage = new SimpleStateMachineStorage();
        this.stateMachineStorage.init(storage);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        RaftProtos.LogEntryProto entry = trx.getLogEntry();
        TermIndex termIndex = TermIndex.valueOf(entry);

        try {
            // 解码命令
            Command cmd = CommandCodec.decode(
                    entry.getStateMachineLogEntry().getLogData().toByteArray());

            switch (cmd.type()) {
                case PUT -> engine.put(cmd.key(), cmd.value());
                case DELETE -> engine.delete(cmd.key());
                case FLUSH -> engine.flush();
            }
            updateLastAppliedTermIndex(termIndex);
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        } catch (Exception e) {
            LOG.error("applyTransaction failed for {}", termIndex, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        // 读请求直接查状态机
        try {
            byte[] key = CommandCodec.decodeGetRequest(request.getContent().toByteArray());
            Object value = engine.get(key);
            byte[] resp = CommandCodec.encodeGetResponse(value);
            return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(resp)));
        } catch (IOException e) {
            LOG.error("query failed", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public long takeSnapshot() throws IOException {
        TermIndex last = getLastAppliedTermIndex();
        if (last == null) {
            LOG.warn("takeSnapshot called but no log entry applied yet");
            return 0L;
        }

        // 1. 确保所有内存数据落盘为 SSTable
        engine.flushAndWait();

        // 2. 获取 Ratis 管理的快照目录
        File snapshotFile = stateMachineStorage.getSnapshotFile(last.getTerm(), last.getIndex());
        Path snapshotDir = snapshotFile.toPath().getParent()
                .resolve("kvdb-snapshot-" + last.getIndex());
        Files.createDirectories(snapshotDir);

        // 3. 使用 SnapshotManager 复制 SSTable + 元数据
        SnapshotManager manager = new SnapshotManager(engine.getDataDir(), engine.getVersionSet());
        SnapshotManager.SnapshotInfo info = manager.createSnapshotAt(snapshotDir);

        // 4. 写入 Ratis 快照标记文件，记录本次快照信息
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(snapshotFile.toPath()))) {
            pw.println("term=" + last.getTerm());
            pw.println("index=" + last.getIndex());
            pw.println("snapshotDir=" + snapshotDir);
            pw.println("totalFiles=" + info.totalFiles());
            pw.println("totalBytes=" + info.totalBytes());
        }

        LOG.info("Raft snapshot created at index={}, term={}, files={}, bytes={}",
                last.getIndex(), last.getTerm(), info.totalFiles(), info.totalBytes());
        return last.getIndex();
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
        super.close();
    }

    /**
     * 获取底层存储引擎（供测试使用）。
     */
    LSMStorageEngine getEngine() {
        return engine;
    }

    /**
     * 注入存储引擎（供测试使用，跳过 Ratis 初始化）。
     */
    void setEngineForTest(LSMStorageEngine engine) {
        this.engine = engine;
    }
}
