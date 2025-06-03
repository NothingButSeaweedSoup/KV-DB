package core;

public final class Constants {

    private Constants() {
    }

    public static final class Tombstone {
        public static final byte[] TOMBSTONE = new byte[0];

        private Tombstone() {
        }
    }

    /**
     * 文件相关常量
     */
    public static final class File {
        /**
         * WAL文件扩展名
         */
        public static final String WAL_EXTENSION = ".wal";
        /**
         * SSTable文件扩展名
         */
        public static final String SST_EXTENSION = ".sst";
        /**
         * SSTable目录名
         */
        public static final String SSTABLE_DIR = "sstables";
        /**
         * 临时文件扩展名
         */
        public static final String TMP_EXTENSION = ".tmp";
        /**
         * 索引文件扩展名
         */
        public static final String IDX_EXTENSION = ".idx";

        private File() {
        }
    }

    /**
     * 操作类型常量
     */
    public static final class Operation {
        /**
         * 写入操作
         */
        public static final byte PUT = 0x01;
        /**
         * 删除操作
         */
        public static final byte DELETE = 0x02;

        private Operation() {
        }
    }

    /**
     * 错误消息常量
     */
    public static final class Error {
        /**
         * 键为null的错误消息
         */
        public static final String KEY_NULL = "Key cannot be null";
        /**
         * 值为null的错误消息
         */
        public static final String VALUE_NULL = "Value cannot be null";
        /**
         * 配置无效的错误消息
         */
        public static final String INVALID_CONFIG = "Invalid configuration";
        /**
         * 文件操作失败的错误消息
         */
        public static final String FILE_OPERATION_FAILED = "File operation failed";

        private Error() {
        }
    }

    /**
     * 系统常量
     */
    public static final class System {
        /**
         * 默认字符集
         */
        public static final String DEFAULT_CHARSET = "UTF-8";
        /**
         * 文件分隔符
         */
        public static final String FILE_SEPARATOR = java.io.File.separator;
        /**
         * 行分隔符
         */
        public static final String LINE_SEPARATOR = java.lang.System.lineSeparator();

        private System() {
        }
    }
}