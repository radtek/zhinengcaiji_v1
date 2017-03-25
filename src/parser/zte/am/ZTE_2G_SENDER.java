package parser.zte.am;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class ZTE_2G_SENDER {

	public static void main(String[] args) throws Exception {
		File rawFile = new File("E:\\new_svn_dir\\igp\\raw_datas\\GSM\\AM\\zte\\中兴2G.txt");
		FileInputStream in = new FileInputStream(rawFile);
		LineIterator lit = IOUtils.lineIterator(in, null);
		Socket socket = new Socket("127.0.0.1", 10010);
		OutputStream out = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(out);
		while (lit.hasNext()) {
			pw.println(lit.nextLine());
			pw.flush();
			Thread.sleep(800);
		}
	}
}
