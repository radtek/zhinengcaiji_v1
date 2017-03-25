package tools.templet;

import java.io.IOException;

/**
 * 模板生成 接口 TempletMaker
 * 
 * @author litp
 * @since 1.0
 */
public interface TempletMaker {

	/**
	 * 生成模板
	 * 
	 * @throws IOException
	 */
	void make(String refFileName, String oFileName) throws Exception;
}
