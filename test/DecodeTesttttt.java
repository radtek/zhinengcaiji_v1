import java.util.Arrays;

public class DecodeTesttttt
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		String str = "你好";
		byte[] b1 = str.getBytes("utf-8");// [-28, -67, -96, -27, -91, -67]
		byte[] b2 = str.getBytes("gb2312"); // [-60, -29, -70, -61]
		String[] s1 = { "asdf", "xxx" };
		System.out.println(Arrays.asList(b1));
		System.out.println(Arrays.asList(b2));
		System.out.println(Arrays.asList(s1));
	}
}
