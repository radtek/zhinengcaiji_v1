package util;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * 行读取器。与一般的行读取器不同，它获取（getLine）时不会自动读取下一行<br>
 * 必须要调用(move)
 * 
 * @author liangww
 * @version 1.0
 * @create 2012-7-14 下午04:53:54
 */
public class LineReader {

	String line = null;			// 当前那一行

	int lineNum = 0;

	BufferedReader reader = null;

	public LineReader(BufferedReader reader) {
		this.reader = reader;
	}

	/**
	 * 是否有一行
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean hasLine() throws IOException {
		if (line != null) {
			return true;
		}

		line = reader.readLine();
		if (line != null) {
			lineNum++;
			return true;
		}

		return false;
	}

	/**
	 * 获取当前行
	 * 
	 * @return
	 */
	public String getLine() {
		return line;
	}

	/**
	 * 移动到下行
	 */
	public void move() {
		line = null;
	}

	/**
	 * 获取当前行数
	 * 
	 * @return
	 */
	public int getCurrentLineNum() {
		return this.lineNum;
	}

}
