public class GPFProcess
{
	public static void main(String[] args) throws Exception
	{
		String gpfdistPath = "D:\\Program Files\\Greenplum\\greenplum-loaders-4.1.0.0-build-5\\bin\\gpfdist.exe -d d:\\ -l d:\\gp.log -p 8081";
		System.out.println(gpfdistPath);
		Process pro = Runtime.getRuntime().exec(gpfdistPath);
		
	}
}
