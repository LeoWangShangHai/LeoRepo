import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 {
    private MessageDigest digest;
    private char hexDigits[] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    public MD5() {
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String md5(String str) {
        byte[] btInput = str.getBytes();
        digest.reset();
        digest.update(btInput);
        byte[] md = digest.digest();
        // 把密文转换成十六进制的字符串形式
        int j = md.length;
        char strChar[] = new char[j * 2];
        int k = 0;
        for (int i = 0; i < j; i++) {
            byte byte0 = md[i];
            strChar[k++] = hexDigits[byte0 >>> 4 & 0xf];
            strChar[k++] = hexDigits[byte0 & 0xf];
        }
        return new String(strChar);
    }
}
