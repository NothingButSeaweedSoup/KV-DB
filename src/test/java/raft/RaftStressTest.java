package raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 三节点 Raft 集群压力测试。
 * <p>
 * 模拟生产场景：多线程并发写入、读取验证、Leader 故障切换后继续写入。
 * <p>
 * 生产环境建议运行 1 小时（调整 WRITE_COUNT 和 THREAD_COUNT）。
 * 测试中使用较短配置以控制测试时间。
 */
class RaftStressTest {

    private static final Logger log = LoggerFactory.getLogger(RaftStressTest.class);

    /** 每个线程写入的条数（测试用较短值，生产环境可设为 100000+） */
    private static final int WRITE_COUNT = 200;
    /** 并发写入线程数 */
    private static final int THREAD_COUNT = 4;
    /** 总写入条数 */
    private static final int TOTAL_WRITES = WRITE_COUNT * THREAD_COUNT;
    /** 最小可接受成功率 */
    private static final double MIN_SUCCESS_RATE = 0.90;

    @TempDir
    Path tempDir;

    private RaftClusterTestHelper cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new RaftClusterTestHelper(3, tempDir);
        cluster.startAll();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    // ======================== 多线程并发写入 ========================

    @Test
    void stressTest_concurrentWrites() throws Exception {
        RaftKVClient client = cluster.getClient();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
        ConcurrentHashMap<String, String> successfulWrites = new ConcurrentHashMap<>();

        log.info("开始压力测试: {} 线程 x {} 条 = {} 总写入", THREAD_COUNT, WRITE_COUNT, TOTAL_WRITES);

        long startTime = System.currentTimeMillis();

        // 并发写入
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                for (int i = 0; i < WRITE_COUNT; i++) {
                    try {
                        String key = "stress-" + threadId + "-" + i;
                        String value = "value-" + threadId + "-" + i;
                        client.put(key.getBytes(), value);
                        successfulWrites.put(key, value);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            }));
        }

        // 等待所有线程完成
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        long elapsed = System.currentTimeMillis() - startTime;

        log.info("写入完成: 成功={}, 失败={}, 耗时={}ms, 吞吐={} op/s",
                successCount.get(), errors.size(), elapsed,
                elapsed > 0 ? successCount.get() * 1000.0 / elapsed : 0);

        // 验证：大部分写入应成功
        assertThat(successCount.get())
                .as("成功写入数应 >= %.0f%%", MIN_SUCCESS_RATE * 100)
                .isGreaterThanOrEqualTo((int) (TOTAL_WRITES * MIN_SUCCESS_RATE));

        // 读取验证：只验证确认成功写入的 key
        int verifiedCount = 0;
        for (var entry : successfulWrites.entrySet()) {
            try {
                Object value = client.get(entry.getKey().getBytes());
                if (entry.getValue().equals(value)) {
                    verifiedCount++;
                }
            } catch (Exception e) {
                // 读取失败可接受
            }
        }

        log.info("读取验证: {}/{} 条成功写入的数据一致", verifiedCount, successCount.get());
        assertThat(verifiedCount)
                .as("成功写入的数据应大部分可读")
                .isGreaterThanOrEqualTo((int) (successCount.get() * 0.95));
    }

    // ======================== 故障切换 + 继续写入 ========================

    @Test
    void stressTest_writeWithFailover() throws Exception {
        RaftKVClient client = cluster.getClient();

        // 阶段 1：正常写入，记录成功写入的 key
        log.info("阶段 1: 正常写入 {} 条", WRITE_COUNT);
        List<String> phase1Keys = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < WRITE_COUNT; i++) {
            String key = "phase1-" + i;
            try {
                client.put(key.getBytes(), "v-" + i);
                phase1Keys.add(key);
            } catch (Exception e) {
                // 少量失败可接受
            }
        }
        log.info("阶段 1 成功写入: {}/{}", phase1Keys.size(), WRITE_COUNT);

        // 阶段 2：Leader 故障切换
        String leaderBefore = cluster.getLeaderId();
        log.info("阶段 2: 关闭 Leader {}", leaderBefore);
        cluster.killLeader();
        cluster.waitForNewLeader();

        String leaderAfter = cluster.getLeaderId();
        log.info("新 Leader: {}", leaderAfter);
        assertThat(leaderAfter).as("应选出新 Leader").isNotNull();

        // 阶段 3：故障切换后继续写入
        log.info("阶段 3: 故障切换后写入 {} 条", WRITE_COUNT);
        List<String> phase3Keys = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < WRITE_COUNT; i++) {
            String key = "phase3-" + i;
            try {
                client.put(key.getBytes(), "v-" + i);
                phase3Keys.add(key);
            } catch (Exception e) {
                // 少量失败可接受
            }
        }

        log.info("故障切换后写入成功: {}/{}", phase3Keys.size(), WRITE_COUNT);
        assertThat(phase3Keys.size())
                .as("故障切换后应能继续写入大部分数据")
                .isGreaterThanOrEqualTo(WRITE_COUNT / 2);

        // 阶段 4：验证所有阶段的数据
        log.info("阶段 4: 验证数据");
        int verifiedPhase1 = countVerified(client, phase1Keys, "v-");
        int verifiedPhase3 = countVerified(client, phase3Keys, "v-");

        log.info("验证结果: phase1={}/{}, phase3={}/{}",
                verifiedPhase1, phase1Keys.size(), verifiedPhase3, phase3Keys.size());

        assertThat(verifiedPhase1)
                .as("阶段 1 成功写入的数据在 Leader 切换后应大部分可读")
                .isGreaterThanOrEqualTo((int) (phase1Keys.size() * 0.95));
        assertThat(verifiedPhase3)
                .as("阶段 3 成功写入的数据应可读")
                .isEqualTo(phase3Keys.size());
    }

    // ======================== 持续写入稳定性 ========================

    @Test
    void stressTest_sustainedWrites() throws Exception {
        RaftKVClient client = cluster.getClient();
        int rounds = 5;
        ConcurrentHashMap<String, String> successfulWrites = new ConcurrentHashMap<>();

        log.info("持续写入稳定性测试: {} 轮，每轮 {} 条", rounds, WRITE_COUNT);

        for (int round = 0; round < rounds; round++) {
            int roundSuccess = 0;
            for (int i = 0; i < WRITE_COUNT; i++) {
                try {
                    String key = "sustained-" + round + "-" + i;
                    String value = "r" + round + "-v" + i;
                    client.put(key.getBytes(), value);
                    successfulWrites.put(key, value);
                    roundSuccess++;
                } catch (Exception e) {
                    // 记录但继续
                }
            }
            log.info("轮次 {}: 写入成功 {}/{}", round, roundSuccess, WRITE_COUNT);
        }

        log.info("总写入: {}", successfulWrites.size());

        assertThat(successfulWrites.size())
                .as("持续写入总成功数应 >= %.0f%%", MIN_SUCCESS_RATE * 100)
                .isGreaterThanOrEqualTo((int) (rounds * WRITE_COUNT * MIN_SUCCESS_RATE));

        // 随机抽样验证，只验证确认成功写入的 key
        List<String> keys = new ArrayList<>(successfulWrites.keySet());
        Collections.shuffle(keys);
        int sampleSize = Math.min(100, keys.size());
        int verified = 0;
        for (int i = 0; i < sampleSize; i++) {
            String key = keys.get(i);
            String expected = successfulWrites.get(key);
            try {
                Object value = client.get(key.getBytes());
                if (expected.equals(value)) {
                    verified++;
                }
            } catch (Exception e) {
                // 读取失败可接受
            }
        }

        log.info("抽样验证: {}/{} 条一致", verified, sampleSize);
        assertThat(verified)
                .as("抽样验证成功率应接近 100%")
                .isGreaterThanOrEqualTo((int) (sampleSize * 0.95));
    }

    private int countVerified(RaftKVClient client, List<String> keys, String valuePrefix) {
        int verified = 0;
        for (String key : keys) {
            try {
                int idx = Integer.parseInt(key.substring(key.lastIndexOf('-') + 1));
                Object value = client.get(key.getBytes());
                if ((valuePrefix + idx).equals(value)) {
                    verified++;
                }
            } catch (Exception e) {
                // 忽略
            }
        }
        return verified;
    }
}
