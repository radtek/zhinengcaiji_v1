package parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 抽象的流解析器 AbstractParser
 * 
 * @author liangww 2012-4-20
 * @version 1.0.0 1.0.1 liangww 2012-06-06 增加dispose方法<br>
 */
public abstract class AbstractStreamParser extends Parser {

	/**
	 * 数据分析接口，接收从采集模块传送过来的流进行解析
	 * 
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	public void parse(InputStream in, OutputStream out) throws Exception {

	}

	/**
	 * 销毁方法
	 */
	public void dispose() {

	}
}
