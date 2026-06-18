package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

/**
 * 数据完整性校验器。
 * <p>
 * 在启动时扫描 SSTable 文件，执行多级校验：
 * <ol>
 *   <li>索引可加载性：能正常打开文件并读取索引</li>
 *   <li>文件结构完整性：文件尾部索引偏移量合法、索引区域可读</li>
 *   <li>数据块可读性：抽样读取数据块验证偏移量与长度合法</li>
 * </ol>
 */
public class DataIntegrityChecker {

    private static final Logger log = LoggerFactory.getLogger(DataIntegrityChecker.class);

    private final String dbDir;
    private final int maxLevel;

    public DataIntegrityChecker(String dbDir, int maxLevel) {
        this.dbDir = dbDir;
        this.maxLevel = maxLevel;
    }

    /**
     * 校验所有 SSTable 文件的完整性。
     *
     * @return 校验结果
     */
    public CheckResult checkAll() {
        int totalFiles = 0;
        int corruptedFiles = 0;
        List<String> corruptedPaths = new ArrayList<>();

        for (int level = 0; level < maxLevel; level++) {
            Path levelPath = Path.of(dbDir, "level-" + level);
            if (!levelPath.toFile().exists()) {
                continue;
            }
            File[] files = levelPath.toFile().listFiles(
                    (dir, name) -> name.endsWith(Constants.File.SST_EXTENSION));
            if (files == null) {
                continue;
            }
            for (File file : files) {
                totalFiles++;
                if (!verifySSTable(file)) {
                    corruptedFiles++;
                    corruptedPaths.add(file.getAbsolutePath());
                }
            }
        }

        if (corruptedFiles > 0) {
            log.warn("数据完整性校验完成: {}/{} 个SSTable文件损坏", corruptedFiles, totalFiles);
            for (String path : corruptedPaths) {
                log.warn("  损坏文件: {}", path);
            }
        } else {
            log.info("数据完整性校验完成: {} 个SSTable文件全部正常", totalFiles);
        }

        return new CheckResult(totalFiles, corruptedFiles, corruptedPaths);
    }

    /**
     * 验证单个 SSTable 文件的完整性。
     * <ol>
     *   <li>尝试打开文件并加载索引</li>
     *   <li>验证文件尾部索引偏移量合法</li>
     *   <li>抽样读取数据块，验证偏移量和长度不越界</li>
     * </ol>
     */
    private boolean verifySSTable(File file) {
        // 第一级：索引可加载
        List<long[]> indexEntries; // 每项为 [keyLen, dataOffset]
        int entries;
        try (SSTable sstable = new SSTable(file.getAbsolutePath())) {
            entries = sstable.getEntryCount();
            if (entries < 0) {
                log.warn("SSTable条目数异常: {} (entries={})", file.getName(), entries);
                return false;
            }
            if (entries == 0) {
                return true; // 空文件视为正常
            }
            // 保存索引条目用于第三级抽样
            indexEntries = new ArrayList<>();
            for (var entry : ((java.util.concurrent.ConcurrentSkipListMap<byte[], Long>) getIndexField(sstable)).entrySet()) {
                indexEntries.add(new long[]{entry.getKey().length, entry.getValue()});
            }
        } catch (IOException e) {
            log.warn("SSTable索引加载失败: {} - {}", file.getName(), e.getMessage());
            return false;
        } catch (Exception e) {
            log.warn("SSTable校验异常: {} - {}", file.getName(), e.getMessage());
            return false;
        }

        // 第二级：文件结构完整性（直接读取文件尾部验证偏移量）
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < 8) {
                log.warn("SSTable文件过小: {} ({} bytes)", file.getName(), fileSize);
                return false;
            }

            // 读取尾部 8 字节（索引偏移量）
            ByteBuffer offsetBuf = ByteBuffer.allocate(8);
            channel.position(fileSize - 8);
            channel.read(offsetBuf);
            offsetBuf.flip();
            long indexOffset = offsetBuf.getLong();

            if (indexOffset < 0 || indexOffset >= fileSize - 8) {
                log.warn("SSTable索引偏移量非法: {} (offset={}, fileSize={})",
                        file.getName(), indexOffset, fileSize);
                return false;
            }

            // 验证索引区域可读：读取条目数
            channel.position(indexOffset);
            ByteBuffer countBuf = ByteBuffer.allocate(4);
            channel.read(countBuf);
            countBuf.flip();
            int indexSize = countBuf.getInt();

            if (indexSize < 0 || indexSize > 10_000_000) {
                log.warn("SSTable索引条目数异常: {} (indexSize={})", file.getName(), indexSize);
                return false;
            }

        } catch (IOException e) {
            log.warn("SSTable结构校验失败: {} - {}", file.getName(), e.getMessage());
            return false;
        }

        // 第三级：数据块抽样校验
        if (!verifyDataBlocks(file, indexEntries)) {
            return false;
        }

        return true;
    }

    /**
     * 通过反射获取 SSTable 的 index 字段（ConcurrentSkipListMap<byte[], Long>）。
     * 用于抽样校验，避免修改 SSTable 的公共 API。
     */
    private java.util.concurrent.ConcurrentSkipListMap<byte[], Long> getIndexField(SSTable sstable) {
        try {
            var field = SSTable.class.getDeclaredField("index");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            var index = (java.util.concurrent.ConcurrentSkipListMap<byte[], Long>) field.get(sstable);
            return index;
        } catch (Exception e) {
            throw new RuntimeException("无法访问 SSTable.index 字段", e);
        }
    }

    /**
     * 第三级校验：抽样读取数据块，验证偏移量和 key/value 长度合法。
     * <p>
     * 随机选取最多 5 个索引条目，读取对应偏移量处的数据块，
     * 校验 keyLen(4B) + key + valLen(4B) + value 不超出索引区域。
     */
    private boolean verifyDataBlocks(File file, List<long[]> indexEntries) {
        if (indexEntries.isEmpty()) {
            return true;
        }

        int sampleCount = Math.min(5, indexEntries.size());
        Random random = new Random(file.hashCode()); // 固定种子，保证可重复

        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long fileSize = channel.size();

            // 读取索引偏移量（数据区上界）
            ByteBuffer offsetBuf = ByteBuffer.allocate(8);
            channel.position(fileSize - 8);
            channel.read(offsetBuf);
            offsetBuf.flip();
            long indexOffset = offsetBuf.getLong();

            for (int i = 0; i < sampleCount; i++) {
                long[] entry = indexEntries.get(random.nextInt(indexEntries.size()));
                long dataOffset = entry[1];

                if (dataOffset < 0 || dataOffset >= indexOffset) {
                    log.warn("SSTable数据偏移量越界: {} (offset={}, indexOffset={})",
                            file.getName(), dataOffset, indexOffset);
                    return false;
                }

                // 读取 keyLen(4B)
                channel.position(dataOffset);
                ByteBuffer keyLenBuf = ByteBuffer.allocate(4);
                if (channel.read(keyLenBuf) != 4) {
                    log.warn("SSTable数据块读取失败: {} (offset={})", file.getName(), dataOffset);
                    return false;
                }
                keyLenBuf.flip();
                int keyLen = keyLenBuf.getInt();

                if (keyLen < 0 || keyLen > 10_000_000) {
                    log.warn("SSTable keyLen异常: {} (keyLen={})", file.getName(), keyLen);
                    return false;
                }

                // 跳过 key，读取 valLen(4B)
                long valLenOffset = dataOffset + 4 + keyLen;
                if (valLenOffset + 4 > indexOffset) {
                    log.warn("SSTable value区域越界: {} (valLenOffset={})", file.getName(), valLenOffset);
                    return false;
                }
                channel.position(valLenOffset);
                ByteBuffer valLenBuf = ByteBuffer.allocate(4);
                if (channel.read(valLenBuf) != 4) {
                    log.warn("SSTable valLen读取失败: {} (offset={})", file.getName(), valLenOffset);
                    return false;
                }
                valLenBuf.flip();
                int valLen = valLenBuf.getInt();

                if (valLen < 0 || valLen > 100_000_000) {
                    log.warn("SSTable valLen异常: {} (valLen={})", file.getName(), valLen);
                    return false;
                }

                // 验证 value 区域不超出索引区域
                long valueEnd = valLenOffset + 4 + valLen;
                if (valueEnd > indexOffset) {
                    log.warn("SSTable value数据越界: {} (valueEnd={}, indexOffset={})",
                            file.getName(), valueEnd, indexOffset);
                    return false;
                }
            }

        } catch (IOException e) {
            log.warn("SSTable数据块抽样校验失败: {} - {}", file.getName(), e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * 校验结果。
     *
     * @param totalFiles     文件总数
     * @param corruptedFiles 损坏文件数
     * @param corruptedPaths 损坏文件路径列表
     */
    public record CheckResult(int totalFiles, int corruptedFiles, List<String> corruptedPaths) {
        public boolean isAllHealthy() {
            return corruptedFiles == 0;
        }
    }
}
