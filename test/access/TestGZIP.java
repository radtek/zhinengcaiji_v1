package access;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;

public class TestGZIP
{
	public static void main(String[] args)
	{
		System.out.println((char)41);
//		System.out.println(FilenameUtils.getFullPath("E:\\资料\\解析\\联通二期数据和资料\\8-6号提交的数据【亿阳】\\阿朗20100801\\性能\\20100801--DQRNC01\\NodeB-DQ1_xiqudianhuazhan_BB\\A20100801.0000+0800-0100+0800_NodeB-DQ1_xiqudianhuazhan_BB.gz")+FilenameUtils.getBaseName("E:\\资料\\解析\\联通二期数据和资料\\8-6号提交的数据【亿阳】\\阿朗20100801\\性能\\20100801--DQRNC01\\NodeB-DQ1_xiqudianhuazhan_BB\\A20100801.0000+0800-0100+0800_NodeB-DQ1_xiqudianhuazhan_BB.gz"));
//		try
//		{
//			String inFilename = "E:\\资料\\解析\\联通二期数据和资料\\8-6号提交的数据【亿阳】\\阿朗20100801\\性能\\20100801--DQRNC01\\NodeB-DQ1_xiqudianhuazhan_BB\\A20100801.0000+0800-0100+0800_NodeB-DQ1_xiqudianhuazhan_BB.gz";
//			GZIPInputStream in = new
//			GZIPInputStream(new FileInputStream
//			(inFilename));
//			String outFilename = "E:\\资料\\解析\\联通二期数据和资料\\8-6号提交的数据【亿阳】\\阿朗20100801\\性能\\20100801--DQRNC01\\NodeB-DQ1_xiqudianhuazhan_BB\\A20100801.0000+0800-0100+0800_NodeB-DQ1_xiqudianhuazhan_BB";
//			OutputStream out = new FileOutputStream
//			(outFilename);
//			byte[] buf = new byte[1024];
//			int len;
//			while ((len = in.read(buf)) > 0)
//			{
//				out.write(buf, 0, len);
//			}
//			in.close();
//			out.close();
//		}
//		catch (IOException e)
//		{
//
//		}
	}
}
