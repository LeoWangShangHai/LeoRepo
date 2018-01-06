package com.e1ef.a2r.service.kinesis.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class ConfigurableConstants {
    private static Log loger = LogFactory.getLog(ConfigurableConstants.class);

    private static Properties config_pro = new Properties();

    private static String propertyFileName = "/src/main/resource/kinesis.properties";

    static {
        loger.debug(propertyFileName);
        InputStream in = null;
        try {
            in = ConfigurableConstants.class.getClassLoader()
                    .getResourceAsStream(propertyFileName);
            if (in != null) {
                config_pro.load(in);
            }
        } catch (IOException e) {
            loger.error(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    loger.error("close " + propertyFileName + " error!");
                }
            }
        }
    }

    public static String getProperty(String key, String defaultValue) {
        if (config_pro.getProperty(key)== null) {
            return defaultValue;
        }else{
            return config_pro.getProperty(key);
        }

    }

    public static String getProperty(String key) {
        return getProperty(key, null);
    }

    public static void main(String[] args) {
        loger.info(getProperty("database"));
    }
}