import java.io.FileOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class TestBuffer implements Serializable
{

	static int j = 0;

	public static void main(String[] args) throws Exception
	{
		new Thread(new Runnable()
		{
			public void run()
			{
				j = j + 1;
			}
		}).start();
		new Thread(new Runnable()
		{
			public void run()
			{
				j = j - 1;
			}
		}).start();

		Charset decoder = Charset.forName("utf-8");
		FileOutputStream fis = new FileOutputStream("C:\\Users\\ChenSijiang\\Desktop\\A20120103.1000+0800-1100+0800_NodeB-DQ2_0457longfengdishuiW");
		FileChannel ch = fis.getChannel();
		ByteBuffer bb = ByteBuffer.allocate(64);
		bb.put("nihao".getBytes());
		bb.flip();

		// ch.write(bb);
		System.out.println("ha");
		ch.lock(1, 20, false);
		System.out.println();

	}
}
