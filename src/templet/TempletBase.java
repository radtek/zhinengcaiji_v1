package templet;

/**
 * 模板基类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public interface TempletBase {

	// 创建模板函数
	public void buildTmp(int nTmpID);

	public void buildTmp(TempletRecord record);

	// 解析模板函数
	public void parseTemp(String tempContent) throws Exception;
}
