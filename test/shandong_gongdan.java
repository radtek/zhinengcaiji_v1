public class shandong_gongdan
{

	static int test()
	{
		int i = 1;
		try
		{
			return i++;
		}
		finally
		{
			return i++;
		}
	}

	public static void main(String[] args) throws Exception
	{
		System.out.println(test());
		//
		// File dir = new
		// File("C:\\Users\\ChenSijiang\\Desktop\\这里FTP根目录\\20120416\\");
		// File[] fs = dir.listFiles();
		// for (File f : fs)
		// {
		// if ( f.getName().contains(".青岛.") )
		// {
		// FileUtils.copyFile(f, new File(f.getAbsolutePath().replace(".青岛.",
		// ".东营.")));
		// }
		// }
	}

}
