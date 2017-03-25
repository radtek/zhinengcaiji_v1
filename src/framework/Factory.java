package framework;

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import templet.TempletBase;
import templet.TempletRecord;
import util.LogMgr;
import access.AbstractAccessor;
import distributor.Distribute;

/**
 * 对象工厂
 * 
 * @author YangJian
 * @since 3.0
 */
public class Factory {

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 根据接入方式创建数据接入器方法
	 */
	public static AbstractAccessor createAccessor(CollectObjInfo obj) {
		if (obj == null)
			return null;

		if (obj.getCollectType() != 9) {

			AbstractAccessor accessor = PBeanMgr.getInstance().getAccessorBean(obj.getCollectType());
			if (accessor == null)
				return null;

			accessor.setTaskInfo(obj);

			Parser parser = createParser(obj);
			if (parser == null) {
				logger.error("未找到parserId为" + obj.getParserID() + "的解析器，请查看pbean.xml是否有此parser");
			}

			Distribute distributor = createDistributor(obj);
			parser.setDistribute(distributor);

			accessor.setParser(parser);
			accessor.setDistributor(distributor);

			return accessor;
		} else {
			AbstractAccessor accessor = PBeanMgr.getInstance().getAccessorBean(obj.getCollectType());
			if (accessor == null)
				return null;

			accessor.setTaskInfo(obj);
			return accessor;
		}
	}

	/**
	 * 根据指定解析类型创建解析器工厂方法
	 * 
	 * @return
	 */
	public static Parser createParser(CollectObjInfo obj) {
		if (obj == null)
			return null;

		int parserID = obj.getParserID();

		Parser p = PBeanMgr.getInstance().getParserBean(parserID);
		if (p == null)
			return null;

		p.init(obj);
		//p.setCollectObjInfo(obj);

		return p;
	}

	/**
	 * 根据指定分发类型创建分发器工厂方法
	 * 
	 * @return
	 */
	public static Distribute createDistributor(CollectObjInfo obj) {
		if (obj == null)
			return null;

		int distID = obj.getDistributorID();

		Distribute d = PBeanMgr.getInstance().getDistributorBean(distID);
		if (d == null)
			return null;
		d.init(obj);

		return d;
	}

	/**
	 * 根据传入的解析类型创建模板
	 * 
	 * @param tmpType
	 *            模板类型
	 * @param tmpID
	 *            模板编号
	 * @return
	 */
	public static TempletBase createTemplet(int tmpType, int tmpID) {
		TempletBase templet = PBeanMgr.getInstance().getTemplateBean(tmpType);
		if (templet == null)
			return null;

		// 解析 解析模板
		templet.buildTmp(tmpID);

		return templet;
	}

	/**
	 * 根据传入的模板数据库记录创建模板
	 * 
	 * @param record
	 * @return
	 */
	public static TempletBase createTemplet(TempletRecord record) {
		if (record == null)
			return null;

		TempletBase templet = PBeanMgr.getInstance().getTemplateBean(record.getType());
		if (templet == null)
			return null;

		// 解析 解析模板
		templet.buildTmp(record);

		return templet;
	}

}
