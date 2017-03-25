import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

public class TestNIO
{
	public static void main(String[] args) throws Exception
	{
		ServerSocketChannel ssc = ServerSocketChannel.open();
		ssc.configureBlocking(false);
		ssc.socket().bind(new InetSocketAddress(8098));
		Selector sel = Selector.open();
		ssc.register(sel, SelectionKey.OP_ACCEPT);

		while (true)
		{
			int count = sel.select();
			if ( count < 1 )
				continue;
			System.out.println(count);
			Iterator<SelectionKey> it = sel.selectedKeys().iterator();
			while (it.hasNext())
			{
				SelectionKey sk = it.next();
				it.remove();
				if ( sk.isAcceptable() )
				{
					SocketChannel sc = ((ServerSocketChannel) sk.channel()).accept();
					sc.configureBlocking(false);
					sc.register(sel, SelectionKey.OP_READ);
				}
				else if ( sk.isReadable() )
				{
					ByteBuffer bb = ByteBuffer.allocate(1024);
					SocketChannel sc = (SocketChannel) sk.channel();
					sc.read(bb);
					bb.flip();
					Charset decoder = Charset.forName("utf-8");
					System.out.println(decoder.decode(bb).toString());
					bb.clear();
					bb.put("hello".getBytes());
					bb.flip();
					sc.write(bb);
					sc.close();
				}
			}
		}
	}
}
