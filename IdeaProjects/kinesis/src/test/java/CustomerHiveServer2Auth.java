//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//
//import javax.security.sasl.AuthenticationException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hive.service.auth.PasswdAuthenticationProvider;

/*


public class CustomHiveServer2Auth implements PasswdAuthenticationProvider{

    @Override
    public void Authenticate(String username, String password) throws AuthenticationException {
        // TODO Auto-generated method stub
        boolean ok = false;
        String passMd5 = new MD5().md5(password);
        HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        String filePath = conf.get("hive.server2.custom.authentication.file");
        System.out.println("hive.server2.custom.authentication.file [" + filePath + "] ..");
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                String[] datas = tempString.split(",", -1);
                if(datas.length != 2) continue;
                //ok
                if(datas[0].equals(username) && datas[1].equals(passMd5)) {
                    ok = true;
                    break;
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AuthenticationException("read auth config file error, [" + filePath + "] ..", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {}
            }
        }
        if(ok) {
            System.out.println("user [" + username + "] auth check ok .. ");
        } else {
            System.out.println("user [" + username + "] auth check fail .. ");
            throw new AuthenticationException("user [" + username + "] auth check fail .. ");
        }
    }
    public static void main(String[] args) {
        String passMd5 = new MD5().md5("1qazxsw2#Edc");
        System.out.println(passMd5);
    }

}
*/
