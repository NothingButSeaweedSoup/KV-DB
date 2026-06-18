package raft;

import core.LSMStorageEngine;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KvStoreStateMachineTest {

    @TempDir
    Path tempDir;

    private LSMStorageEngine engine;
    private KvStoreStateMachine stateMachine;

    @BeforeEach
    void setUp() throws IOException {
        engine = new LSMStorageEngine(tempDir.toString());
        stateMachine = new KvStoreStateMachine();
        stateMachine.setEngineForTest(engine);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }

    // ======================== 辅助方法 ========================

    /**
     * 构建一个 RaftProtos.LogEntryProto，用于模拟 Raft 日志条目。
     */
    private RaftProtos.LogEntryProto buildLogEntry(long term, long index, byte[] commandData) {
        RaftProtos.StateMachineLogEntryProto smLogEntry =
                RaftProtos.StateMachineLogEntryProto.newBuilder()
                        .setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(commandData))
                        .build();

        return RaftProtos.LogEntryProto.newBuilder()
                .setTerm(term)
                .setIndex(index)
                .setStateMachineLogEntry(smLogEntry)
                .build();
    }

    /**
     * 创建一个 mock 的 TransactionContext，返回指定的 LogEntryProto。
     */
    private TransactionContext mockTransactionContext(RaftProtos.LogEntryProto logEntry) {
        TransactionContext trx = mock(TransactionContext.class);
        when(trx.getLogEntry()).thenReturn(logEntry);
        return trx;
    }

    // ======================== PUT 测试 ========================

    @Test
    void applyTransaction_put_storesValue() throws Exception {
        byte[] key = "testKey".getBytes();
        String value = "testValue";
        Command cmd = new Command(Command.CommandType.PUT, key, value);
        byte[] cmdData = CommandCodec.encode(cmd);

        RaftProtos.LogEntryProto logEntry = buildLogEntry(1, 10, cmdData);
        TransactionContext trx = mockTransactionContext(logEntry);

        CompletableFuture<Message> future = stateMachine.applyTransaction(trx);
        Message result = future.get();

        assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");

        // 验证数据确实写入了引擎
        Object stored = engine.get(key);
        assertThat(stored).isEqualTo("testValue");
    }

    @Test
    void applyTransaction_put_multipleEntries() throws Exception {
        // 写入第一个条目
        byte[] key1 = "key1".getBytes();
        Command cmd1 = new Command(Command.CommandType.PUT, key1, "value1");
        RaftProtos.LogEntryProto entry1 = buildLogEntry(1, 1, CommandCodec.encode(cmd1));
        stateMachine.applyTransaction(mockTransactionContext(entry1)).get();

        // 写入第二个条目
        byte[] key2 = "key2".getBytes();
        Command cmd2 = new Command(Command.CommandType.PUT, key2, 42);
        RaftProtos.LogEntryProto entry2 = buildLogEntry(1, 2, CommandCodec.encode(cmd2));
        stateMachine.applyTransaction(mockTransactionContext(entry2)).get();

        // 验证两个值都能读到
        assertThat(engine.get(key1)).isEqualTo("value1");
        assertThat(engine.get(key2)).isEqualTo(42);
    }

    // ======================== DELETE 测试 ========================

    @Test
    void applyTransaction_delete_removesKey() throws Exception {
        // 先写入
        byte[] key = "toDelete".getBytes();
        Command putCmd = new Command(Command.CommandType.PUT, key, "value");
        stateMachine.applyTransaction(mockTransactionContext(buildLogEntry(1, 1, CommandCodec.encode(putCmd)))).get();

        assertThat(engine.get(key)).isEqualTo("value");

        // 再删除
        Command deleteCmd = new Command(Command.CommandType.DELETE, key, null);
        stateMachine.applyTransaction(mockTransactionContext(buildLogEntry(1, 2, CommandCodec.encode(deleteCmd)))).get();

        assertThat(engine.get(key)).isNull();
    }

    // ======================== FLUSH 测试 ========================

    @Test
    void applyTransaction_flush_doesNotThrow() throws Exception {
        Command flushCmd = new Command(Command.CommandType.FLUSH, null, null);
        RaftProtos.LogEntryProto logEntry = buildLogEntry(1, 1, CommandCodec.encode(flushCmd));

        CompletableFuture<Message> future = stateMachine.applyTransaction(mockTransactionContext(logEntry));
        Message result = future.get();

        assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");
    }

    // ======================== TermIndex 更新测试 ========================

    @Test
    void applyTransaction_updatesLastAppliedTermIndex() throws Exception {
        byte[] key = "key".getBytes();
        Command cmd = new Command(Command.CommandType.PUT, key, "val");

        // term=5, index=100
        RaftProtos.LogEntryProto logEntry = buildLogEntry(5, 100, CommandCodec.encode(cmd));
        stateMachine.applyTransaction(mockTransactionContext(logEntry)).get();

        TermIndex lastApplied = stateMachine.getLastAppliedTermIndex();
        assertThat(lastApplied).isNotNull();
        assertThat(lastApplied.getTerm()).isEqualTo(5);
        assertThat(lastApplied.getIndex()).isEqualTo(100);
    }

    // ======================== query 测试 ========================

    @Test
    void query_returnsValue_afterPut() throws Exception {
        byte[] key = "queryKey".getBytes();
        Command putCmd = new Command(Command.CommandType.PUT, key, "queryValue");
        stateMachine.applyTransaction(mockTransactionContext(buildLogEntry(1, 1, CommandCodec.encode(putCmd)))).get();

        // 构造 GET 请求
        byte[] getRequest = CommandCodec.encodeGetRequest(key);
        Message queryMsg = Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(getRequest));

        CompletableFuture<Message> result = stateMachine.query(queryMsg);
        Object value = CommandCodec.decodeGetResponse(result.get().getContent().toByteArray());

        assertThat(value).isEqualTo("queryValue");
    }

    @Test
    void query_returnsNull_forNonExistentKey() throws Exception {
        byte[] key = "nonExistent".getBytes();
        byte[] getRequest = CommandCodec.encodeGetRequest(key);
        Message queryMsg = Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(getRequest));

        CompletableFuture<Message> result = stateMachine.query(queryMsg);
        Object value = CommandCodec.decodeGetResponse(result.get().getContent().toByteArray());

        assertThat(value).isNull();
    }

    // ======================== 错误处理测试 ========================

    @Test
    void applyTransaction_propagatesError_forInvalidCommand() {
        // 构造一个无效的命令数据（单字节 0x7F 不是有效的 CommandType）
        byte[] invalidData = new byte[]{0x7F};
        RaftProtos.LogEntryProto logEntry = buildLogEntry(1, 1, invalidData);
        TransactionContext trx = mockTransactionContext(logEntry);

        CompletableFuture<Message> future = stateMachine.applyTransaction(trx);

        // 应该以异常完成
        assertThat(future).isCompletedExceptionally();
    }
}
