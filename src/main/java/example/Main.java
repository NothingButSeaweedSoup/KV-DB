package example;

import core.LSMStorageEngine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String path = "data";
        LSMStorageEngine lsmStorageEngine = new LSMStorageEngine(path);
        for (int i = 0; i < 10000; i++){
            lsmStorageEngine.put(("key" + i).getBytes(StandardCharsets.UTF_8), ("value" + i));
            if(i == 10){
                lsmStorageEngine.put("key10".getBytes(StandardCharsets.UTF_8),  "v10");
            }
            if (i == 20){
                lsmStorageEngine.delete("key20".getBytes(StandardCharsets.UTF_8));
            }
        }
        lsmStorageEngine.close();
    }
}
