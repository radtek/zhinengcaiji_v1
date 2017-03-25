package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class TestEncode {
	
	public static void tellMyEncoding(){
		
	}

	/**
	 * 娴嬭瘯
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File file = new File("/home/yuy/workplace/igp1/igp_v1.0/test/util/Testt.java");
		File file1 = new File("/home/yuy/workplace/igp1/igp_v1.0/test/util/Testt1.java");
		new Testt().tellMyEncoding();
		FileInputStream in = new FileInputStream(file);
		FileOutputStream out = new FileOutputStream(file1, true);
		byte[] buffer = new byte[1024];
		int n = 0;
		while(( n = in.read(buffer)) > -1){
			String str = new String(buffer,0,n);
			str = new String(str.getBytes("utf-8"),"gbk");//鍏堢敤gbk瑙ｇ爜锛岀劧鍚庣敤utf-8缂栫爜锛岀敓鎴愯繖鏍蜂竴涓瓧绗︿覆
			out.write(str.getBytes());
		}
		out.flush();
		out.close();
		in.close();
		
//		Class<Testt1> Testt = (Class<util.Testt1>) Class.forName("util.Testt1");
//		Testt1 testt = Testt.newInstance();
//		testt.tellMyEncoding();
		
	}

}
