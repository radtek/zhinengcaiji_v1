package access;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class TestAPX
{
 

	public static void main(String[] args) throws Exception
	{
		String src = args[0];
		String dest = args[1];
		long per = Long.parseLong(args[2]);

		InputStream in = new FileInputStream(src);
		PrintWriter pw = new PrintWriter(dest);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		while ((line = br.readLine()) != null)
		{
			pw.println(line);
			pw.flush();
			Thread.sleep(per);
		}

		br.close();
		in.close();
		pw.close();
	}

}
