import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class TestApacheTelnet
{
	private static final int IAC = 255;
	private static final int WILL = 251;
	private static final int WONT = 252;
	private static final int DO = 253;
	private static final int DONT = 254;

	public static int hand(InputStream is, OutputStream os) throws IOException
	{
		while (true)
		{
			int ch = is.read();
			if ( ch < 0 || ch != 255 )
				return ch;
			int cmd = is.read();
			int opt = is.read();
			switch (opt)
			{
				case 1: // echo协商选项,本程序未处理
					break;
				case 3: // supress go-ahead(抑制向前选项)
					break;
				case 24: // terminal type(终端类型选项)
					if ( cmd == 253 )
					{
						os.write(255);
						os.write(251);
						os.write(24);
						
						os.write(255);
						os.write(250);
						os.write(24);
						os.write(0);
						os.write(255);
						os.write(240);
					}
					else if ( cmd == 250 )
					{
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						int svr = is.read();
						while (svr != 240)
						{
							baos.write(svr);
							svr = is.read();
						}

					}
					break;
				default:
					if ( cmd == 253 )
					{
						os.write(255);
						os.write(252);
						os.write(opt);
					}
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		System.out.println(0xff);
		Socket s = new Socket("192.168.0.61", 23);
		InputStream is = s.getInputStream();
		OutputStream os = s.getOutputStream();
		hand(is, os);

		os.write("root\n".getBytes());
		os.flush();
		Thread.sleep(2000);
		os.write("uway\n".getBytes());
		os.flush();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		char[] buf = new char[1024];
		int read = br.read(buf);
		while (read > 0)
		{
			System.out.println(new String(buf, 0, read));
			read = br.read(buf);
		}
	}
}
