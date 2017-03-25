import java.io.OutputStream;
import java.net.Socket;

public class Send
{
	public static void main(String[] args) throws Exception
	{
		Socket s = new Socket("localhost", 8765);
		OutputStream out = s.getOutputStream();
		for (int i = 0; i < 10000; i++)
		{
			out.write("asdffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff+\n".getBytes());
		}
	}
}
