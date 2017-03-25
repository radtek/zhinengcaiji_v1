package parser.boco.am;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class TestSender {

	public static void main(String[] args) throws Exception {
		File file = new File("C:\\Users\\ChenSijiang\\Desktop\\江西亿阳告警.txt");
		Socket sock = new Socket();
		sock.connect(new InetSocketAddress(8899));
		OutputStream out = sock.getOutputStream();
		PrintWriter pw = new PrintWriter(out);
		LineIterator li = IOUtils.lineIterator(new FileInputStream(file), null);
		while (li.hasNext()) {
			pw.println(li.nextLine());
			pw.flush();
			Thread.sleep(100);
		}
	}
}
