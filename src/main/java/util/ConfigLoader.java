package util;

import cluster.ClusterConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class ConfigLoader {
    private static final String CONFIG_FILE_PATH = "cluster-config.json";

    public static ClusterConfig loadConfig() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(new File(CONFIG_FILE_PATH), ClusterConfig.class);
    }
}