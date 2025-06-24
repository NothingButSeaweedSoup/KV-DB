package util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class DBConfigLoader {
    private long walSegmentSize;
    private long memTableThreshold;
    private static final String DB_CONFIG_FILE_PATH = "db-config.json";
    // 从配置文件中加载数据库配置
    public DBConfigLoader() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        // 判断配置文件是否存在
        if (new File(DB_CONFIG_FILE_PATH).exists()) {
            var jsonNode = objectMapper.readTree(new File(DB_CONFIG_FILE_PATH));
            this.walSegmentSize = jsonNode.get("walSegmentSize").asLong();
            this.memTableThreshold = jsonNode.get("memTableThreshold").asLong();
        }else{
            this.walSegmentSize = 1024 * 1024;
            this.memTableThreshold =4 * 1024 * 1024;
        }
    }

    public long getWalSegmentSize() {
        return walSegmentSize;
    }

    public long getMemTableThreshold() {
        return memTableThreshold;
    }
}
