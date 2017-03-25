import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SendAAAAA
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		ServerSocket s = new ServerSocket(8899);
		Socket sock = s.accept();
		OutputStream out = sock.getOutputStream();
		for (;;)
		{
			out.write("asdlfkajsdlkfsajdflksajdflkasjdlkfajs在脸色dlfkjsadlkfjalksdflkdsajf\r\n".getBytes());
			out.flush();
			Thread.sleep(50);
		}
	}

}
