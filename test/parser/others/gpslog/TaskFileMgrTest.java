package parser.others.gpslog;

public class TaskFileMgrTest
{
	public static void main(String[] args) throws Exception
	{
		TaskFilesRecorder taskFileMgr = new TaskFilesRecorder("d:/test", 100, 5);
		
		int max = 183;
		for(int i=0; i < max; i++)
		{
			taskFileMgr.loadCache(1);
			System.out.println(i + ":" + taskFileMgr.containFileName(1, Integer.toString(i)));
			taskFileMgr.addFileName(1, Integer.toString(i));
			System.out.println(i + ":" + taskFileMgr.containFileName(1, Integer.toString(i)));
		}
		
		for(int i=0; i < max; i++)
		{
			System.out.println(i + ":" + taskFileMgr.containFileName(1, Integer.toString(i)));
		}
		
	}

}
