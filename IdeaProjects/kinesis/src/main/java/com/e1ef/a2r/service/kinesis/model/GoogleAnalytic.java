package com.e1ef.a2r.service.kinesis.model;


import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GoogleAnalytic {
    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static GoogleAnalytic fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, GoogleAnalytic.class);
        } catch (IOException e) {
            return null;
        }
    }
    public static void main(String[] args) throws Exception {
        File file = new File("filePath");
        FileInputStream fis = new FileInputStream(file);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1);
        byte[] b = new byte[1024];
        int len = -1;
        while((len = fis.read(b)) != -1) {
            bos.write(b, 0, len);
        }
        byte[] fileByte = bos.toByteArray();
    }
}
