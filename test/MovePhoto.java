import java.io.File;

import org.apache.commons.io.FileUtils;

public class MovePhoto
{
	public static void main(String[] args) throws Exception
	{
		File dir1 = new File("C:\\xx\\");
		File targetDir = new File("C:\\xx\\");
		File[] subDirs = dir1.listFiles();
		for (File subDir : subDirs)
		{
			if ( subDir.getName().equals("full") )
			{
				System.out.println("full");
				continue;
			}
			File[] fss = subDir.listFiles();
			for (File f : fss)
			{
				if ( f.isFile() )
				{
					FileUtils.moveFile(f, new File(targetDir, f.getName()));
				}
			}
		}
	}
}
