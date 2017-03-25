import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class Yun
{
	public static void main(String[] args) throws Exception
	{
		List<String> list = IOUtils.readLines(new FileInputStream("C:\\Users\\ChenSijiang\\Desktop\\编辑1"));
		PrintWriter pw = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\aaaa.txt");
		for (int i = list.size() - 1; i >= 0; i--)
		{
			String line = list.get(i);
			boolean me = line.contains(",发送,");
			if ( me )
				pw.println(line.replace("13662579568,", "  我说的："));
			else
				pw.println(line.replace("13662579568,", "老婆说的："));
		}
		pw.close();
	}
}
