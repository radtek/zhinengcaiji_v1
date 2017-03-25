package templet.lucent;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.LineReader;
import util.LogMgr;
import distributor.Distribute;
import framework.SystemConfig;

/**
 * 性能 smart的解析模板， 它支持树结构的。其中它有两三种子结点。第一种是它本身，第二种是公用的Fields,<br>
 * 第三种私有的Fields。这个公用的Fields的field可以给它子结点的smartTemplet获取。<br>
 * 它是以开始标记（用正则表达式）作用查找开始的。而结束标记，用正则表达式，当num为n时必须要配置的<br>
 * 另外注意一点，它是必须按顺序解析的.如果num不为n则endRegex不会起到作用。切忌！！<br>
 * 
 * 具体见luc_pm_smt_parse.xml <templet>结点<br>
 * 
 * @author liangww
 * @version 1.0.0 1.0.1 liangww 2012-07-17 增加setLogKey方法<br>
 *          1.0.2 liangww 2012-08-06 去掉没用的测试打印代码<br>
 * 
 * @create 2012-7-14 下午05:04:46
 * @see FieldGroup
 * @see Node
 */
public class SmartTemplet implements Node {

	private final static Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	private SmartTemplet pTemplet = null; // 父结点

	private List<Node> nodeList = new ArrayList<Node>();

	private int id = 0;						// templet id

	private int num = Integer.MAX_VALUE; 	// 记录个数，当为n时为未知个，这时必须配置endRegex

	private String startRegex = null; 		// 开始标记,用正则表达式

	private String endRegex = null; 		// 结束标记，用正则表达式，当num为n时必须要配置的

	private boolean ignore = false; 		// 该模板是否可以忽略分发，可以不配置

	private String logKey = null;

	private boolean bFirst = true;

	public void setPTemlple(SmartTemplet templet) {
		this.pTemplet = templet;
	}

	@Override
	public boolean isPublic() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getFieldValue(int recordIndex, int fieldIndex) {
		int index = recordIndex;

		// 这儿可以看nodeList看成一个多维的数组
		// 因为templet的记录是通过其nodeList组装的，而每个node都可以0或多个记录
		for (int i = 0; i < nodeList.size(); i++) {
			int recordNum = nodeList.get(i).getRecordNum();
			if (recordNum == 0) {
				continue;
			}

			String vaule = nodeList.get(i).getFieldValue(index % recordNum, fieldIndex);
			if (vaule != null) {
				return vaule;
			}

			index = index / recordNum;
		}

		//
		return getPTempletPublicFieldValue(fieldIndex);
	}

	private String getPTempletPublicFieldValue(int index) {
		if (pTemplet != null) {
			// 不行就获取父类的
			for (int i = 0; i < pTemplet.nodeList.size(); i++) {
				if (pTemplet.nodeList.get(i).isPublic()) {
					String vaule = pTemplet.nodeList.get(i).getFieldValue(0, index);
					if (vaule != null) {
						return vaule;
					}
				}// if(nodeList.get(i).isPublic())
			}

			return pTemplet.getPTempletPublicFieldValue(index);
		}

		return null;
	}

	public String getPublicFieldValue(int id) {
		return null;
	}

	public SmartTemplet getTemplet(int id) {
		return null;
	}

	public void build(Element element) throws Exception {

		// id="1" startRegex="HOUR" num="n"

		this.id = Integer.valueOf(element.attributeValue("id"));
		this.startRegex = element.attributeValue("startRegex");
		this.endRegex = element.attributeValue("endRegex");
		String value = element.attributeValue("num");
		if (value != null && !value.equals("n")) {
			this.num = Integer.valueOf(value);
		}
		//
		if (this.num == Integer.MAX_VALUE && endRegex == null) {
			throw new Exception("当num为n时，endRegex必须要配置");
		}
		// 忽略
		if ("true".equalsIgnoreCase(element.attributeValue("ignore"))) {
			ignore = true;
		}

		Iterator<Element> itr = element.elementIterator();
		while (itr.hasNext()) {
			Element child = itr.next();
			if (child.getNodeType() != Element.ELEMENT_NODE) {
				continue;
			}

			//
			if ("templet".equals(child.getName())) {
				SmartTemplet templet = new SmartTemplet();
				templet.build(child);
				templet.setPTemlple(this);
				this.nodeList.add(templet);
			} else if ("publicFields".equals(child.getName())) {
				FieldGroup publicFields = new FieldGroup();
				publicFields.setBPublic(true);
				publicFields.build(child);
				this.nodeList.add(publicFields);
			} else if ("fields".equals(child.getName())) {
				FieldGroup fields = new FieldGroup();
				fields.build(child);
				this.nodeList.add(fields);
			}
		}
	}

	/**
	 * 根据模板文件名获取模板list
	 * 
	 * @param templetFileName
	 * @return
	 */
	public static List<SmartTemplet> parseTemplet(String templetFileName) {
		List<SmartTemplet> map = new ArrayList<SmartTemplet>();

		InputStream in = null;
		try {
			File tmpFile = new File(SystemConfig.getInstance().getTempletPath() + File.separator + templetFileName);
			in = new FileInputStream(tmpFile);
			Document doc = new SAXReader().read(in);
			List<Element> templetElts = doc.getRootElement().elements();
			for (Element templetElt : templetElts) {
				if (templetElt.getNodeType() == Element.ELEMENT_NODE && "templet".equals(templetElt.getName())) {
					SmartTemplet templet = new SmartTemplet();
					templet.build(templetElt);
					map.add(templet);
				}
			}
			return map;
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("加载模板失败：", e);
		} finally {
			IOUtils.closeQuietly(in);
		}
		return null;
	}

	@Override
	public boolean canParse(String line) {
		return Pattern.compile(startRegex).matcher(line).find();
	}

	@Override
	public void parse(LineReader lineReader, Distribute distribute) throws Exception {
		int time = 0;
		while (time < num && lineReader.hasLine()) {
			// 不能能解析
			if (!canParse(lineReader.getLine())) {
				// 是否要查找end
				if (num == Integer.MAX_VALUE && isEnd(lineReader.getLine())) {
					break;
				}

				lineReader.move();
				continue;
			}

			for (int i = 0; i < nodeList.size() && lineReader.hasLine(); i++) {
				Node node = nodeList.get(i);
				try {
					node.parse(lineReader, distribute);
				} catch (Exception e) {
					// TODO: handle exception
					LOGGER.warn(logKey + node + "  parse异常", e);
				}
			}

			// 不是忽略
			if (!ignore) {
				// 分发
				try {
					distribute.distribute(this, null);
				} catch (Exception e) {
					// TODO: handle exception
					LOGGER.warn(logKey + this + "  分发异常", e);
				}
			} else if (bFirst) {
				// 只是第一次才写log
				LOGGER.debug(logKey + this + " ignore");
				bFirst = false;
			}

			// 把记录清了
			clearRecords();
			time++;
		}

	}

	@Override
	public int getRecordNum() {
		int addValue = 0;
		int recordSize = 1;

		// 这儿可以看nodeList看成一个多维的数组
		// 因为templet的记录是通过其nodeList组装的，而每个node都可以0或多个记录
		// 当所有的node的记录数为0时，才认为没有记录
		for (int i = 0; i < nodeList.size(); i++) {
			int num = nodeList.get(i).getRecordNum();
			addValue += num;
			recordSize *= ((num == 0) ? (1) : (num));
		}

		return (addValue != 0) ? recordSize : 0;
	}

	/**
	 * 
	 * @param line
	 * @return
	 */
	public boolean isEnd(String line) {
		return Pattern.compile(endRegex).matcher(line).find();
	}

	public void clearRecords() {
		for (int i = 0; i < nodeList.size(); i++) {
			nodeList.get(i).clearRecords();
		}
	}

	public int getId() {
		return this.id;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "templetid:" + id + ", startRegex:" + startRegex + ", endRegex:" + endRegex;
	}

	@Override
	public void setLogKey(String logKey) {
		// TODO Auto-generated method stub
		this.logKey = logKey;
		for (int i = 0; i < this.nodeList.size(); i++) {
			nodeList.get(i).setLogKey(logKey);
		}
	}

}
