import java.io.File;
import java.io.FileInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class TestExec
{
	public static void main(String[] args) throws Exception
	{
		File f = new File("F:\\资料\\原始数据\\pk_6\\亿阳\\UTRAN-SNAP20120103020002.xml");
		FileInputStream in = new FileInputStream(f);
		FileChannel fc = in.getChannel();
		MappedByteBuffer map = fc.map(MapMode.READ_ONLY, 0, f.length());
		String name = "<rncId>";
		byte[] buff = new byte[1024];
	}
}
