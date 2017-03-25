package distributor;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import parser.lucent.SmartPm1x;
import store.AbstractStore;
import store.StoreFactory;
import task.CollectObjInfo;
import templet.GenericSectionHeadD;
import templet.Table;
import templet.Table.Column;
import templet.lucent.SmartTemplet;
import util.Util;
import exception.ParseException;
import exception.StoreException;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 带表头的按段解析-数据分发类
 * 
 * @author liangww
 * @since 1.0
 * @see SmartPm1x
 * @see SmartTemplet
 * @see GenericSectionHeadD
 */
public class SmartDistributor extends Distribute {

	protected Map<String, AbstractStore> sqlldrStoreMap = new HashMap<String, AbstractStore>();

	private List<File> clobFiles = new ArrayList<File>();

	private int clobIndex;

	public SmartDistributor() {
	}

	public SmartDistributor(CollectObjInfo TaskInfo) {
		super(TaskInfo);
	}

	public void init(CollectObjInfo TaskInfo) {
		super.init(TaskInfo);
	}

	protected void init() {
		// do nothing
	}

	public void distribute(Object wParam, Object lParam) throws Exception {
		SmartTemplet templetP = (SmartTemplet) wParam;

		int pTempletID = templetP.getId();

		GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD) disTmp).getTemplet(pTempletID);
		if (templetD == null)
			throw new ParseException("在分发模板中找不到对应的templet编号(id=" + pTempletID + ")");
		Table tableD = templetD.getTables().get(pTempletID);
		if (tableD == null)
			throw new ParseException("在分发模板中找不到对应的table(id=" + pTempletID + ")");


		AbstractStore<?> sqlldrStore = this.sqlldrStoreMap.get(tableD.getName());
		// 处理sqlldrstore
		if (sqlldrStore == null) {
			sqlldrStore = StoreFactory.getStore(pTempletID, tableD, collectInfo,collectInfo.getLastCollectTime());
			// sqlldrStore = new SqlldrStore(new SqlldrStoreParam(pTempletID,
			// tableD));
			// sqlldrStore.setCollectInfo(collectInfo);
			// sqlldrStore.setTaskID(this.collectInfo.getTaskID());
			// sqlldrStore.setDataTime(this.collectInfo.getLastCollectTime());
			// sqlldrStore.setOmcID(this.collectInfo.getDevInfo().getOmcID());
			// sqlldrStore.setFlag(Util.getDateString_yyyyMMddHHmmssSSS(new
			// Date())
			// + "_" + Math.abs(new Random().nextInt()));
			sqlldrStore.open();
			sqlldrStoreMap.put(tableD.getName(), sqlldrStore);
		}

		String splitSignD = tableD.getSplitSign();
		StringBuilder dataRow = new StringBuilder();

		int reocrdNum = templetP.getRecordNum();
		for (int recordIndex = 0; recordIndex < reocrdNum; recordIndex++) {
			Iterator<Column> itr = tableD.getColumns().values().iterator();
			while (itr.hasNext()) {
				Column column = itr.next();
				String value = templetP.getFieldValue(recordIndex, column.getIndex());

				// if ( value == null )
				// log.error(collectInfo.getTaskID()
				// + " - 分发模板可能配置有误，无法获取到列信息，是否缺少<column节点，或将其写成了<field");
				if (column.getType() == ConstDef.COLLECT_FIELD_DATATYPE_LOB) {
					File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + collectInfo.getTaskID() + "_"
							+ Util.getDateString_yyyyMMddHHmmssSSS(collectInfo.getLastCollectTime()) + "_" + (clobIndex++) + ".clob");
					PrintWriter pw = new PrintWriter(clob);
					pw.print(value == null ? "" : value);
					pw.flush();
					pw.close();
					dataRow.append(clob.getAbsolutePath()).append(splitSignD);
					clobFiles.add(clob);
				}
				// add 2011-10-17 对时间类型的处理方式1970-01-01 00:00:00.1,处理后为1970-01-01
				// 00:00:00
				else if (column.getType() == ConstDef.COLLECT_FIELD_DATATYPE_DATATIME && column.isIgnore()) {
					String v = nvl(value);
					String d = util.Util.isNull(v) ? v : v.substring(0, v.lastIndexOf("."));
					dataRow.append(d).append(splitSignD);
				}// end
				else {
					dataRow.append(nvl(value)).append(splitSignD);
				}
			}

			// 写到sqlldrStore
			sqlldrStore.write(dataRow.toString());
			dataRow.setLength(0);
		}

	}

	private String nvl(String value) {
		return value == null ? "" : value;
	}

	public void commit() {
		Iterator<AbstractStore> itr = sqlldrStoreMap.values().iterator();
		while (itr.hasNext()) {
			AbstractStore sqlldrStore = itr.next();
			try {
				sqlldrStore.flush();
				sqlldrStore.commit();
				sqlldrStore.close();
			} catch (StoreException e) {
				log.error("执行sqlldr失败", e);
			}
		}

		for (File f : clobFiles) {
			f.delete();
		}
		clobFiles.clear();
		this.sqlldrStoreMap.clear();

	}
}
