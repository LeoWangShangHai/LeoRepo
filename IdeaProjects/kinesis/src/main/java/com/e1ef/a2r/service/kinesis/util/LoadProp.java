package com.e1ef.a2r.service.kinesis.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class LoadProp {
    private static Log loger = LogFactory.getLog(LoadProp.class);
    private static final String KINESIS_CONFIG_FILE = "resource/kinesis.properties";
    private Properties prop;
    public String getProperty(String key){
        return prop.getProperty(key);
    }

    public LoadProp() throws IOException {

        try {
            prop = new Properties();
            FileInputStream fileInputStream = new FileInputStream(KINESIS_CONFIG_FILE);
            if(fileInputStream!=null){
                prop.load(fileInputStream);
            }


        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
    }
    public static void main(String[] args) {
        LoadProp loadProp = null;
        try {
            loadProp = new LoadProp();
            loger.info(loadProp.getProperty("inputfile"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
