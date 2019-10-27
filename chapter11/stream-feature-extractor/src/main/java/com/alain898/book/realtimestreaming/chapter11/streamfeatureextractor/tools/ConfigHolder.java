package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.List;

public class ConfigHolder {

    private static volatile Config INSTANCE;

    private static final String CONFIG_FILE = "conf/application.conf";

    static {
        getInstance();
    }

    private static void loadConfig(String file) {
        synchronized (ConfigHolder.class) {
            INSTANCE = ConfigFactory.parseFile(new File(file)).resolve();
        }
    }

    public static Config getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (ConfigHolder.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            loadConfig(CONFIG_FILE);
            return INSTANCE;
        }
    }

    private ConfigHolder() {
    }

    public static int getInt(String path) {
        return INSTANCE.getInt(path);
    }

    public static long getLong(String path) {
        return INSTANCE.getLong(path);
    }

    public static String getString(String path) {
        return INSTANCE.getString(path);
    }

    public static Config getConfig(String path) {
        return INSTANCE.getConfig(path);
    }

    public static List<String> getStringList(String path) {
        return INSTANCE.getStringList(path);
    }

    public static boolean getBoolean(String path) {
        return INSTANCE.getBoolean(path);
    }
}
