package sqlldr;

import java.util.List;
import java.util.Map;

public interface SqlldrInterface {

	// 加载数据结构
	public Map<String, List<String>> loadDataStructrue(List<String> tables);

	// set表结构进去
	public void setTableCols(Map<String, List<String>> tableCols);

	// sqlldr初始化
	public void initSqlldr();

	// 写入数据
	public void writeRows(List<String> row, String tableName);

	// 执行sqlldr入库
	public boolean runSqlldr();

}
