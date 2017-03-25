package distributor;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import parser.GenericSectionHeadParser;
import store.AbstractStore;
import store.StoreFactory;
import task.CollectObjInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import templet.GenericSectionHeadP.Field;
import templet.GenericSectionHeadP.Templet;
import templet.Table;
import templet.Table.Column;
import util.Util;
import exception.ParseException;
import exception.StoreException;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 带表头的按段解析-数据分发类
 * 
 * @author YangJian
 * @since 3.1
 * @see GenericSectionHeadParser
 * @see GenericSectionHeadP
 * @see GenericSectionHeadD
 */
public class GenericSectionHeadDistributor extends Distribute {

	protected AbstractStore<?> sqlldrStore;

	private List<File> clobFiles = new ArrayList<File>();

	private int clobIndex;

	public GenericSectionHeadDistributor() {
	}

	public GenericSectionHeadDistributor(CollectObjInfo TaskInfo) {
		super(TaskInfo);
	}

	public void init(CollectObjInfo TaskInfo) {
		super.init(TaskInfo);
	}

	protected void init() {
		// do nothing
	}

	public void distribute(Object wParam, Object lParam) throws Exception {
		GenericSectionHeadP.Templet templetP = (Templet) wParam;
		int dsID = (Integer) lParam;

		if (!(disTmp instanceof GenericSectionHeadD) || dsID < 0)
			return;

		int pTempletID = templetP.getId();
		GenericSectionHeadP.Public pubP = templetP.getPublicElement();
		GenericSectionHeadP.DS dsP = templetP.getDsMap().get(dsID);
		GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD) disTmp).getTemplet(pTempletID);
		Table tableD = templetD.getTables().get(dsID);
		if (tableD == null)
			throw new ParseException("在分发模板中找不到对应的数据区域编号(id=" + dsID + ")");

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
		}

		String splitSignD = tableD.getSplitSign();
		StringBuilder dataRow = new StringBuilder();

		// 加上公共数据区域字段
		if (pubP != null) {
			Collection<Field> pubFileds = pubP.getFields().getFields().values();
			for (Field f : pubFileds) {
				dataRow.append(nvl(f.getValue())).append(splitSignD);
			}
		}

		// 普通数据
		Collection<Field> dsFileds = dsP.getFields().getFields().values();
		for (Field f : dsFileds) {
			Column column = tableD.getColumns().get(f.getIndex());
			if (column == null)
				log.error(collectInfo.getTaskID() + " - 分发模板可能配置有误，无法获取到列信息，是否缺少<column节点，或将其写成了<field");
			if (column.getType() == ConstDef.COLLECT_FIELD_DATATYPE_LOB) {
				File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + collectInfo.getTaskID() + "_"
						+ Util.getDateString_yyyyMMddHHmmssSSS(collectInfo.getLastCollectTime()) + "_" + (clobIndex++) + ".clob");
				PrintWriter pw = new PrintWriter(clob);
				pw.print(f.getValue() == null ? "" : f.getValue());
				pw.flush();
				pw.close();
				dataRow.append(clob.getAbsolutePath()).append(splitSignD);
				clobFiles.add(clob);
			}
			// add 2011-10-17 对时间类型的处理方式1970-01-01 00:00:00.1,处理后为1970-01-01
			// 00:00:00
			else if (tableD.getColumns().get(f.getIndex()).getType() == ConstDef.COLLECT_FIELD_DATATYPE_DATATIME
					&& tableD.getColumns().get(f.getIndex()).isIgnore()) {
				String v = nvl(f.getValue());
				String d = util.Util.isNull(v) ? v : v.substring(0, v.lastIndexOf("."));
				dataRow.append(d).append(splitSignD);
			}// end
			// add 2013-10-18 by yuy 对英文时间类型的处理：18-APR-13 02.23.08.689000 AM
			else if (tableD.getColumns().get(f.getIndex()).getType() == ConstDef.COLLECT_FIELD_DATATYPE_DATATIME2) {
				String v = nvl(f.getValue());
				String d = util.Util.isNull(v) ? v : Util.parseEnglishTime(v);
				dataRow.append(d).append(splitSignD);
			}// end
			else {
				dataRow.append(nvl(f.getValue())).append(splitSignD);
			}
		}

		// log.debug(dataRow.toString());
		sqlldrStore.write(dataRow.toString());
	}

	private String nvl(String value) {
		return value == null ? "" : value;
	}

	public void commit() {
		if (sqlldrStore != null) {
			try {
				sqlldrStore.flush();
				sqlldrStore.commit();
				sqlldrStore.close();
				sqlldrStore = null;
				for (File f : clobFiles) {
					if (f.exists()) {
						if (!f.delete())
							log.debug("CLOB文件删除失败：" + f);
					}
				}
				clobFiles.clear();
			} catch (StoreException e) {
				log.error("执行sqlldr失败", e);
			}
		}
	}
}
