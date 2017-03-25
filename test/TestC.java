import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestC
{
	public static void main(String[] args)
	{
		FileOutputStream out = null;
		try
		{
			out = new FileOutputStream("f:\\a.txt");
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileChannel ch = out.getChannel();
		ByteBuffer buf = ByteBuffer.allocate(64);
		buf.put("hello".getBytes());
		try
		{
			ch.write(buf);
			out.flush();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
