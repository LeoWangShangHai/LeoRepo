package com.e1ef.a2r.service.kinesis.util;

import java.io.*;

public class ReadFromFile {
    public static byte[] readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        byte[] byteArray = null;
        try {
            //System.out.println(System.currentTimeMillis());
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));
            String allLine = "";
            String singleLine = null;
            int line = 1;
            while ((singleLine = reader.readLine()) != null) {
                allLine = allLine + "line[" + line +"]"+singleLine + "\n";
                System.out.print(allLine);
                line++;

            }
            byteArray = allLine.getBytes("UTF-8");
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return byteArray;
    }

    public static void readFileByBytes(String fileName) {
        File file = new File(fileName);
        InputStream in = null;
        try {
            byte[] tempbytes = new byte[1024];
            int byteread = 0;
            in = new FileInputStream(fileName);
            ReadFromFile.showAvailableBytes(in);
            while ((byteread = in.read(tempbytes)) != -1) {
                System.out.println(byteread);
                System.out.write(tempbytes);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    private static void showAvailableBytes(InputStream in) {
        try {
            System.out.println("current bytes in Stream:" + in.available());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String fileName = "C:/Leo Wang/leads_score.csv";
        readFileByLines(fileName);

       /* byte[] byteArray = ReadFromFile.readFileByLines(fileName);
        String res = null;
        try {
            res = new String(byteArray,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.print(res);*/

    }
}
