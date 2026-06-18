package core;

import config.Config;
import util.BloomFilter;
import util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable implements AutoCloseable {
    private static final double FALSE_POSITIVE_RATE = 0.01;
    private final Config config;
    private final int level;
    private final ConcurrentSkipListMap<byte[], Long> index;
    private final String filePath;
    private final boolean useMmap;
    private long fileSize;
    private FileChannel channel;
    private MappedByteBuffer mmapBuffer;
    private FileChannel sstableChannel;
    private FileChannel indexChannel;
    private BloomFilter bloomFilter;

    public SSTable(Config config, int level) {
        this.config = config;
        this.level = level;
        this.filePath = getSSTablePath().toString();
        this.useMmap = false;
        this.channel = null;
        this.fileSize = 0;
        this.index = new ConcurrentSkipListMap<>(ByteUtil::compare);
        this.sstableChannel = null;
        this.indexChannel = null;
        this.bloomFilter = new BloomFilter(10000, FALSE_POSITIVE_RATE);
    }

    public SSTable(String filePath) throws IOException {
        this(filePath, false);
    }

    /**
     * 打开已有 SSTable 文件。
     *
     * @param filePath SSTable 文件路径
     * @param useMmap  是否使用 mmap 读取
     */
    public SSTable(String filePath, boolean useMmap) throws IOException {
        File file = new File(filePath);
        String parentPath = file.getParent() != null ? file.getParent() : ".";
        this.config = new Config.Builder()
                .setDataDir(parentPath)
                .build();
        this.level = 0;
        this.filePath = filePath;
        this.useMmap = useMmap;
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.fileSize = channel.size();
        if (useMmap) {
            this.mmapBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        }
        this.index = new ConcurrentSkipListMap<>(ByteUtil::compare);
        this.bloomFilter = new BloomFilter(10000, FALSE_POSITIVE_RATE);
        loadIndex();
    }

    public SSTable(String filePath, Map<byte[], byte[]> data) throws IOException {
        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        String parentPath = file.getParent() != null ? file.getParent() : ".";
        this.config = new Config.Builder()
                .setDataDir(parentPath)
                .build();
        this.level = 0;
        this.filePath = filePath;
        this.useMmap = false;
        this.channel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        this.index = new ConcurrentSkipListMap<>(ByteUtil::compare);
        this.bloomFilter = new BloomFilter(data.size(), FALSE_POSITIVE_RATE);
        writeData(data);
        this.fileSize = channel.size();
        channel.close();
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    private ByteBuffer serialize(byte[] key, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(key.length + value.length + 8);
        buffer.putInt(key.length);
        buffer.put(key);

        int valueLen = (value==null)? 0 : value.length;
        buffer.putInt(valueLen);
        if (value!=null){
            buffer.put(value);
        }
        buffer.flip();
        return buffer;
    }

    public void writeData(Map<byte[], byte[]> data) throws IOException {
        long offset = 0;// 文件的偏移量
        for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();

            index.put(key, offset);
            bloomFilter.add(key);

            ByteBuffer buffer = serialize(key, value);
            buffer.position(0);
            int bytesWritten = channel.write(buffer);
            offset += bytesWritten;
        }
        writeIndex();
    }

    public void writeIndex() throws IOException {
        long offset = channel.position();

        int totalSize = 4;
        for (Map.Entry<byte[], Long> entry : index.entrySet()) {
            totalSize += entry.getKey().length + Integer.SIZE + Long.SIZE;
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(index.size());
        for (Map.Entry<byte[], Long> entry : index.entrySet()) {
            byte[] key = entry.getKey();
            buffer.putInt(key.length);
            buffer.put(key);
            buffer.putLong(entry.getValue());
        }
        buffer.flip();
        channel.write(buffer);

        ByteBuffer offsetBuffer = ByteBuffer.allocate(8);
        offsetBuffer.putLong(offset);
        offsetBuffer.flip();
        channel.write(offsetBuffer);
    }

    public void loadIndex() throws IOException {
        ByteBuffer offsetBuffer = ByteBuffer.allocate(8);
        channel.position(channel.size() - 8);
        channel.read(offsetBuffer);
        offsetBuffer.flip();
        long indexOffset = offsetBuffer.getLong();

        channel.position(indexOffset);
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();
        int indexSize = buffer.getInt();
        for (int i = 0; i < indexSize; i++) {
            ByteBuffer keyLenBuffer = ByteBuffer.allocate(4);
            channel.read(keyLenBuffer);
            keyLenBuffer.flip();
            int keyLen = keyLenBuffer.getInt();
            ByteBuffer keyBuffer = ByteBuffer.allocate(keyLen);
            channel.read(keyBuffer);
            keyBuffer.flip();
            byte[] key = keyBuffer.array();
            ByteBuffer positionBuffer = ByteBuffer.allocate(8);
            channel.read(positionBuffer);
            positionBuffer.flip();
            long offset = positionBuffer.getLong();
            bloomFilter.add(key);
            index.put(key, offset);
        }
    }

    public byte[] get(byte[] key) throws IOException {
        if (!bloomFilter.mightMatch(key)) {
            return null;
        }
        if (key == null) {
            throw new IllegalArgumentException(Constants.Error.KEY_NULL);
        }
        Long offset = index.get(key);
        if (offset == null) {
            return null;
        }

        if (useMmap && mmapBuffer != null) {
            return getFromMmap(offset);
        }
        return getFromChannel(offset);
    }

    /**
     * 通过 FileChannel 读取 value。
     */
    private byte[] getFromChannel(long offset) throws IOException {
        ByteBuffer kyeLenBuffer = ByteBuffer.allocate(4);
        channel.position(offset);
        channel.read(kyeLenBuffer);
        kyeLenBuffer.flip();
        int keyLen = kyeLenBuffer.getInt();

        channel.position(keyLen + channel.position());

        ByteBuffer valueLenBuffer = ByteBuffer.allocate(4);
        channel.read(valueLenBuffer);
        valueLenBuffer.flip();
        int valueLen = valueLenBuffer.getInt();

        ByteBuffer valueBuffer = ByteBuffer.allocate(valueLen);
        channel.read(valueBuffer);
        valueBuffer.flip();
        byte[] value = new byte[valueLen];
        valueBuffer.get(value);
        return value;
    }

    /**
     * 通过 MmappedByteBuffer 读取 value，无需系统调用。
     */
    private byte[] getFromMmap(long offset) {
        int pos = (int) offset;
        int keyLen = mmapBuffer.getInt(pos);
        pos += 4 + keyLen; // 跳过 keyLen(4) + key
        int valueLen = mmapBuffer.getInt(pos);
        pos += 4; // 跳过 valueLen(4)
        byte[] value = new byte[valueLen];
        mmapBuffer.position(pos);
        mmapBuffer.get(value);
        return value;
    }

    public Map<byte[], byte[]> getAll() throws IOException {
        Map<byte[], byte[]> data = new ConcurrentSkipListMap<>(ByteUtil::compare);
        for (Map.Entry<byte[], Long> entry : index.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = get(key);
            if (value != null) {
                data.put(key, value);
            }
        }
        return data;
    }

    public void close() throws IOException {
        if (mmapBuffer != null) {
            // MappedByteBuffer 没有标准的 unmap API，但可以调用 cleaner
            // 在实际生产环境中通常由 GC 自动回收，这里显式置 null 帮助 GC
            mmapBuffer = null;
        }
        if (channel != null) {
            channel.close();
        }
    }

    public Path getSSTablePath() {
        return Paths.get(config.getDataDir(), Constants.File.SSTABLE_DIR,
                String.format("%d%s", level, Constants.File.SST_EXTENSION));
    }

    public String getFilePath() {
        return filePath;
    }

    /**
     * 获取此 SSTable 中最小的 key（按字典序）。
     *
     * @return 最小 key，若索引为空则返回 null
     */
    public byte[] getMinKey() {
        if (index.isEmpty()) {
            return null;
        }
        return index.firstKey();
    }

    /**
     * 获取此 SSTable 中最大的 key（按字典序）。
     *
     * @return 最大 key，若索引为空则返回 null
     */
    public byte[] getMaxKey() {
        if (index.isEmpty()) {
            return null;
        }
        return index.lastKey();
    }

    /**
     * 获取此 SSTable 中的条目数。
     */
    public int getEntryCount() {
        return index.size();
    }
}
