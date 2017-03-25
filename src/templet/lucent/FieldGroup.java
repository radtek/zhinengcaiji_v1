package templet.lucent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import util.LineReader;
import util.LogMgr;
import util.Util;
import distributor.Distribute;
import formator.Formator;
import formator.Formators;
import framework.PBeanMgr;

/**
 * 域组，是由多个field组成的。而一个templet可以由多个fieldGroup组成的。它的解析是思路主要是<br>
 * 通过spilt分字符串分解，其中""，" "都认为不是域。它会一直分解，直到分解个数到达fieldSize，才会<br>
 * 停止。而field.srcIndex对应的值就是field这一次解出来的值。fieldGroup 一次解析有可会解析成多个记录， 这主要看num(配置)。其中的startRegex, endRegex, num与{@link SmartTemplet}一样的作用。<br>
 * 
 * 具体见luc_pm_smt_parse.xml <fields>结点<br>
 * 
 * @author liangww
 * @version 1.0.0 1.0.1 liangww 2012-07-17 增加setLogKey方法<br>
 * @create 2012-7-14 下午04:06:24
 */
public class FieldGroup implements Node {

	private final static Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	public static final String DEFAULT_SPILT = " "; 		// 默认的分割符

	private List<Field> fieldList = new ArrayList<Field>();

	private SmartTemplet pTemplet = null;				// 父结点

	private int num = Integer.MAX_VALUE;	// 记录个数

	private int fieldSize = 0;					// 字段个数

	private String spilt = null;					// 分隔符，可以不配置

	private String startRegex = null;					// 开始标记,用正则表达式

	private String endRegex = null;					// 结束标记，用正则表达式，当num>为n时必须要配置的

	// 存放记录
	private List<Map<Integer, String>> records = new ArrayList<Map<Integer, String>>();	//

	private boolean bPublic = false;	// 是否公用，可以不配置

	private String logKey = null;		//

	@Override
	public String getFieldValue(int recordIndex, int fieldIndex) {
		// TODO Auto-generated method stub
		if(records.size() > 0){
			return records.get(recordIndex).get(fieldIndex);
		}else{
			return null;
		}
		
	}

	@Override
	public boolean isPublic() {
		// TODO Auto-generated method stub
		return bPublic;
	}

	@Override
	public void setPTemlple(SmartTemplet templet) {
		// TODO Auto-generated method stub
		this.pTemplet = templet;
	}

	public void build(Element element) throws Exception {
		// <fields startRegex="ECP LCNTS" fieldSize="1272" num="1">
		this.fieldSize = Integer.valueOf(element.attributeValue("fieldSize"));
		this.startRegex = element.attributeValue("startRegex");
		this.spilt = element.attributeValue("spilt");
		if (spilt == null) {
			// 默认值
			spilt = DEFAULT_SPILT;
		}

		String value = element.attributeValue("num");
		if (value != null && !value.equals("n")) {
			this.num = Integer.valueOf(value);
		}

		//
		this.endRegex = element.attributeValue("endRegex");
		if (this.num == Integer.MAX_VALUE && endRegex == null) {
			throw new Exception("当num为n时，endRegex必须要配置");
		}

		Iterator<Element> itr = element.elementIterator();
		while (itr.hasNext()) {
			Element child = itr.next();
			if (child.getNodeType() != Element.ELEMENT_NODE) {
				continue;
			}
			//
			if ("field".equals(child.getName())) {
				Field field = new Field();
				field.build(child);
				this.fieldList.add(field);
			}// if("field".equals(child.getNodeTypeName()))
		}// while(itr.hasNext())

	}

	@Override
	public void parse(LineReader lineReader, Distribute distribute) throws Exception {
		// TODO Auto-generated method stub
		List<String> srcFieldValues = new ArrayList<String>();

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

			// 清空了srcFieldValues
			srcFieldValues.clear();
			getSrcFieldValues(lineReader, srcFieldValues);
			fixRecord(srcFieldValues);
			time++;
		}
	}

	/**
	 * 组装
	 * 
	 * @param srcFieldValues
	 */
	void fixRecord(List<String> srcFieldValues) {
		// 组装记录
		if (srcFieldValues.size() >= fieldSize) {
			Map<Integer, String> reocrd = new HashMap<Integer, String>();
			for (int i = 0; i < fieldList.size(); i++) {
				Field field = fieldList.get(i);
				String value = srcFieldValues.get(field.srcIndex);

				Formator formator = PBeanMgr.getInstance().getParseFormatorBean(field.formatType);
				reocrd.put(field.index, formator.format(value));
			}

			if (reocrd.size() != fieldList.size()) {
				LOGGER.warn(logKey + this + " 记录个数不对");
			} else {
				this.records.add(reocrd);
			}
		}
	}

	/**
	 * 
	 * @param lineReader
	 * @param srcFieldValues
	 * @throws IOException
	 */
	public void getSrcFieldValues(LineReader lineReader, List<String> srcFieldValues) throws IOException {
		// 获取到fieldSize
		while (lineReader.hasLine() && srcFieldValues.size() < fieldSize) {
			String line = lineReader.getLine();
			lineReader.move();

			String[] arrays = line.split(spilt);
			for (int i = 0; i < arrays.length; i++) {
				if (Util.isNotNull(arrays[i])) {
					srcFieldValues.add(arrays[i]);
				}
			}// for(int i=0; i < arrays.length; i++)
		}// while(lineReader.hasLine() && srcFieldValues.size() < fieldSize)
	}

	/**
	 * 
	 * @param line
	 * @return
	 */
	public boolean canParse(String line) {
		return Pattern.compile(startRegex).matcher(line).find();
	}

	/**
	 * 
	 * @param line
	 * @return
	 */
	public boolean isEnd(String line) {
		return Pattern.compile(endRegex).matcher(line).find();
	}

	public int getRecordNum() {
		return records.size();
	}

	public void clearRecords() {
		this.records.clear();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "fields:" + "fieldSize:" + fieldSize + ",num" + this.num + ", startRegex:" + startRegex + ", endRegex:" + endRegex;
	}

	public void setBPublic(boolean bPublic) {
		this.bPublic = bPublic;
	}

	@Override
	public void setLogKey(String logKey) {
		// TODO Auto-generated method stub
		this.logKey = logKey;
	}
}
