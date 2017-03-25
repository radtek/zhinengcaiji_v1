package parser;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;

import store.AbstractStore;
import store.StoreFactory;
import templet.DBAutoTempletP;
import templet.DBAutoTempletP.Field;
import templet.DBAutoTempletP.Templet;
import templet.GenericSectionHeadD;
import templet.Table;
import access.DBAutoAccessor;
import exception.StoreException;

/**
 * 数据库智能解析，只需要根据解析模板的列就可以将数据解析出
 * 
 * @author ltp Jul 1, 2010
 * @since 3.1
 * @see DBAutoAccessor
 * @see DBAutoTempletP
 */
public class DBAutoParser extends Parser {

	private AbstractStore sqlldrStore;

	@Override
	public boolean parseData() throws Exception {
		throw new UnsupportedOperationException();
	}

	public int parseData(ResultSet rs, Templet parseTemp) throws Exception {
		Table tableD = null;
		int recordCount = 0;
		int tempId = parseTemp.getId();
		GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD) collectObjInfo.getDistributeTemplet()).getTemplet(tempId);
		if (templetD == null)
			throw new Exception("在分发模板中找不到对应的编号(解析编号=" + tempId + ")");
		tableD = templetD.getTables().get(0);
		String splitSign = tableD.getSplitSign();
		ResultSetMetaData meta = rs.getMetaData();
		Map<Integer, Field> fields = parseTemp.getFields();
		parseMeta(meta, fields);
		while (rs.next()) {
			StringBuilder colVals = new StringBuilder();
			for (Field f : fields.values()) {
				int index = f.getIndexInHead();
				// 如果解析模板中的列在数据库中存在，就取出相应的列值
				if (index > -1) {
					String colVal = rs.getString(index);// 列值
					colVals.append(removeNoise(f.getColType(), colVal)).append(splitSign);
				} else
				// 如果不存在就置为空
				{
					colVals.append("").append(splitSign);
				}
			}
			++recordCount;
			distribute(colVals.toString(), tempId, tableD);
		}
		commit();
		return recordCount;
	}

	private void parseMeta(ResultSetMetaData meta, Map<Integer, Field> fields) throws Exception {
		// 获取列数
		int colCount = meta.getColumnCount();
		for (Field f : fields.values()) {
			for (int i = 1; i <= colCount; i++) {
				int colType = meta.getColumnType(i);// 列类型
				String colName = meta.getColumnName(i);// 列名称
				if (colName.equalsIgnoreCase(f.getName())) {
					f.setIndexInHead(i);// 字段在表头的索引,从1开始
					f.setColType(colType);
					break;
				}
			}

			// 判断required字段是否存在
			if (f.getOccur() != null && f.getOccur().equalsIgnoreCase("required") && f.getIndexInHead() == -1)
				throw new Exception("required字段(" + f.getName() + ")不存在,放弃解析");
		}
	}

	private void distribute(String lineData, int templetId, Table tableD) throws StoreException {
		// 处理sqlldrstore
		if (sqlldrStore == null) {
			sqlldrStore = StoreFactory.getStore(templetId, tableD, this.collectObjInfo,dataTime);
			// sqlldrStore = new SqlldrStore(new SqlldrStoreParam(templetId,
			// tableD));
			// sqlldrStore.setCollectInfo(collectObjInfo);
			// sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
			// sqlldrStore.setDataTime(this.collectObjInfo.getLastCollectTime());
			// sqlldrStore.setOmcID(this.collectObjInfo.getDevInfo().getOmcID());
			sqlldrStore.open();
		}
		sqlldrStore.write(lineData);
	}

	private void commit() {
		if (sqlldrStore != null) {
			try {
				sqlldrStore.flush();
				sqlldrStore.commit();
				sqlldrStore.close();
				sqlldrStore = null;
			} catch (StoreException e) {
			}
		}
	}

	private String removeNoise(int colType, String colVal) {
		if (colVal == null)
			return "";

		// 将日期类型截取yyyy-mm-dd hh:mi:ss
		if (colType == 91 || colType == 92 || colType == 93)
			return colVal.substring(0, 19);

		// 字段中不能出现以下字符
		colVal = colVal.trim().replaceAll(";", " ").replaceAll("\r\n", " ");
		colVal = colVal.replaceAll("\n", " ").replaceAll("\r", " ");

		return colVal;
	}

}
