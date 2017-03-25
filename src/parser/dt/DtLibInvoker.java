package parser.dt;

import util.Util;

/**
 * 北京路测 actix 动态库调用
 * @author yuy
 * @ 2013-10-17
 */
public class DtLibInvoker {
	
	/**
	 * 加载地图信息
	 * @param path
	 * @param f
	 * @return
	 * @throws Exception
	 */
	public native long LoadMaper(String path, int f);
	
	/**
	 * 得出多边形id，名称等
	 * @param map
	 * @param lat
	 * @param ing
	 * @return
	 * @throws Exception
	 */
	public native long JudgePointInPoly(long mapDate, double longitude, double latitude);
	
	/**
	 * 释放内存
	 * @param map
	 * @throws Exception
	 */
	public native void ReleaseMaper(long mapDate);
	
	/**
	 * 构造方法
	 * @param name dll名称
	 */
	public DtLibInvoker(String name) {
        System.loadLibrary(name);
    }
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println( System.getProperty("java.library.path"));
		String dllName = null;
		if(Util.is32Digit()){
			dllName = "DtLibInvokerProxy_x86";
		}else{
			dllName = "DtLibInvokerProxy_x64";
		}
		int f = 2;
		double longitude = 115.988708333333;
		double latitude = 39.6974583333333;
		long peice_id = 0;
		String path = "D:\\聊天软件\\qq\\Users\\549070861\\FileRecv\\GMS_info_bj0816.csv";
		long mapDate = 0;
		DtLibInvoker invoker = new DtLibInvoker(dllName);
		mapDate = invoker.LoadMaper(path, f);
		peice_id = invoker.JudgePointInPoly(mapDate, longitude, latitude);
		System.out.println("peice_id:" + peice_id);
		invoker.ReleaseMaper(mapDate);
	}
}
