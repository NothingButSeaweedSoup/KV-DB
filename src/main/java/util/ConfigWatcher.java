package util;

import core.FsyncStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 配置文件热加载器。
 * <p>
 * 定期检查 db-config.json 的修改时间，若有变更则重新加载并通知所有监听器。
 */
public class ConfigWatcher {

    private static final Logger log = LoggerFactory.getLogger(ConfigWatcher.class);
    private static final long CHECK_INTERVAL_MS = 5000;

    private final String configPath;
    private final ScheduledExecutorService scheduler;
    private volatile long lastModified;
    private final AtomicReference<WatchedConfig> configRef;
    private final List<ConfigChangeListener> listeners = new CopyOnWriteArrayList<>();

    public ConfigWatcher(String configPath) {
        this.configPath = configPath;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "config-watcher");
            t.setDaemon(true);
            return t;
        });
        this.configRef = new AtomicReference<>(loadConfig());
    }

    /**
     * 注册配置变更监听器。
     */
    public void addListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除配置变更监听器。
     */
    public void removeListener(ConfigChangeListener listener) {
        listeners.remove(listener);
    }

    /**
     * 启动配置文件监控。
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAndReload,
                CHECK_INTERVAL_MS, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("配置热加载已启动，检查间隔: {}ms", CHECK_INTERVAL_MS);
    }

    /**
     * 停止配置文件监控。
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * 获取当前配置快照。
     */
    public WatchedConfig getConfig() {
        return configRef.get();
    }

    private void checkAndReload() {
        File file = new File(configPath);
        if (!file.exists()) {
            return;
        }
        long currentModified = file.lastModified();
        if (currentModified > lastModified) {
            lastModified = currentModified;
            try {
                WatchedConfig oldConfig = configRef.get();
                WatchedConfig newConfig = loadConfig();
                configRef.set(newConfig);
                log.info("配置文件已重新加载: fsyncStrategy={}", newConfig.fsyncStrategy());
                notifyListeners(oldConfig, newConfig);
            } catch (Exception e) {
                log.error("配置文件重新加载失败", e);
            }
        }
    }

    private void notifyListeners(WatchedConfig oldConfig, WatchedConfig newConfig) {
        for (ConfigChangeListener listener : listeners) {
            try {
                listener.onConfigChanged(oldConfig, newConfig);
            } catch (Exception e) {
                log.error("通知配置变更监听器失败", e);
            }
        }
    }

    private WatchedConfig loadConfig() {
        try {
            DBConfigLoader loader = new DBConfigLoader();
            File file = new File(configPath);
            if (file.exists()) {
                lastModified = file.lastModified();
            }
            return new WatchedConfig(
                    loader.getFsyncStrategy(),
                    loader.getWalSegmentSize(),
                    loader.getMemTableThreshold(),
                    loader.getSstTargetFileSize(),
                    loader.getLevel0FileNumCompactionTrigger()
            );
        } catch (IOException e) {
            log.warn("加载配置文件失败，使用默认配置", e);
            return new WatchedConfig(
                    FsyncStrategy.BATCH,
                    1024 * 1024,
                    4 * 1024 * 1024,
                    8 * 1024 * 1024,
                    4
            );
        }
    }

    /**
     * 配置变更监听器接口。
     */
    @FunctionalInterface
    public interface ConfigChangeListener {
        /**
         * 配置变更时回调。
         *
         * @param oldConfig 变更前的配置
         * @param newConfig 变更后的配置
         */
        void onConfigChanged(WatchedConfig oldConfig, WatchedConfig newConfig);
    }

    /**
     * 被监控的配置快照。
     */
    public record WatchedConfig(
            FsyncStrategy fsyncStrategy,
            long walSegmentSize,
            long memTableThreshold,
            long sstTargetFileSize,
            int level0FileNumCompactionTrigger
    ) {}
}
