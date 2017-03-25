package parser.xparser.tag;

import java.sql.Timestamp;

import util.Util;

import framework.ConstDef;

/**
 * 属主标记
 * 
 * @author ltp Jan 6, 2010
 * @since 1.0
 * @see RecordElement
 */
public class OwnerElement extends Tag {

	private RuleElement ruleRef;

	public OwnerElement() {
		super("owner");
	}

	public OwnerElement(RuleElement ruleRef) {
		this();
		this.ruleRef = ruleRef;
	}

	/*
	 * 引用方法: <owner rule-ref="规则ID"> ... </owner>
	 */

	public RuleElement getRuleRef() {
		return ruleRef;
	}

	public void setRuleRef(RuleElement ruleRef) {
		this.ruleRef = ruleRef;
	}

	public RecordElement getRecordByName(String name, String file, Timestamp dataTime) {
		if (hasChild()) {
			for (Tag tag : getChild()) {
				if (tag != null) {
					RecordElement re = (RecordElement) tag;
					if (re.getOwnerName().equals(name) && Util.isNotNull(re.getFile()) && ConstDef.ParseFilePath(re.getFile(), dataTime).equals(file)) {
						return re;
					} else if (Util.isNull(re.getFile()) && re.getOwnerName().equals(name)) {
						return re;
					}
				}
			}
		}
		return null;
	}

	/**
	 * @param 字符串
	 * @return 返回字符串，字符串含义为数据的宿主
	 */
	@Override
	public Object apply(Object params) {
		if (ruleRef != null) {
			return ruleRef.apply(params);
		}
		return null;
	}

}
