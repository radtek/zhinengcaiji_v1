import java.io.BufferedReader;

import org.apache.commons.net.ftp.FTPClient;

public class TestFtpClient
{
	public static void main(String[] args) throws Exception
	{
		MyClient c = new MyClient();
	}

}

class MyClient extends FTPClient
{
	public BufferedReader getBR()
	{
		return super._controlInput_;
	}
}
