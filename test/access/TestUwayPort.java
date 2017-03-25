package access;

import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;

public class TestUwayPort
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{

		URLConnection con = new URL("http://114.255.89.4").openConnection();
		System.out.println(IOUtils.readLines(con.getInputStream()).get(0));
	}

}
