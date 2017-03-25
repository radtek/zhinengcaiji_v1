import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FindCalTab
{
	public static void main(String[] args) throws Exception
	{
		FTPClient fc = new FTPClient();
		fc.connect("192.168.0.215",21);
		fc.login("root", "uwayroot");
	FTPFile[] fs=	fc.listFiles("/home/gpadmin/MeContext=*/*.txt");
		System.out.println(fs[0].getName());
		
//		File dir = new File("E:\\new_svn_dir\\cdlcollect\\bin\\czBSC5ExportData\\");
//		File[] fs = dir.listFiles();
//		for (File f : fs)
//		{
//			if ( f.getName().endsWith(".ctl") )
//			{
//				LineIterator it = IOUtils.lineIterator(new InputStreamReader(new FileInputStream(f)));
//				while (it.hasNext())
//				{
//					String line = it.nextLine().trim().toUpperCase();
//					if ( line.contains("APPEND INTO") )
//						System.out.println(line);
//				}
//				it.close();
//			}
//		}
	}
}
