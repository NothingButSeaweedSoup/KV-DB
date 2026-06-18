package raft;

/**
 * 状态机命令，封装 PUT / DELETE / FLUSH 操作。
 *
 * @param type  命令类型
 * @param key   键（不能为 null）
 * @param value 值（PUT 时非 null，DELETE/FLUSH 时可为 null）
 */
public record Command(CommandType type, byte[] key, Object value) {

    public enum CommandType {
        PUT((byte) 1),
        DELETE((byte) 2),
        FLUSH((byte) 3);

        private final byte code;

        CommandType(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static CommandType of(byte code) {
            for (CommandType t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown CommandType code: " + code);
        }
    }
}
