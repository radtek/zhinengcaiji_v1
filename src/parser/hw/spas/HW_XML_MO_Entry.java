package parser.hw.spas;

import java.util.Map;

class HW_XML_MO_Entry {

	String className;

	String fdn;// 仅配置数据中有。

	Map<String, String> attrs;

	public HW_XML_MO_Entry() {
		super();
	}

	public HW_XML_MO_Entry(String className, String fdn, Map<String, String> attrs) {
		super();
		this.className = className;
		this.fdn = fdn;
		this.attrs = attrs;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getFdn() {
		return fdn;
	}

	public void setFdn(String fdn) {
		this.fdn = fdn;
	}

	public Map<String, String> getAttrs() {
		return attrs;
	}

	public void setAttrs(Map<String, String> attrs) {
		this.attrs = attrs;
	}

	@Override
	public String toString() {
		return "HW_XML_MO_Entry [className=" + className + ", fdn=" + fdn + ", attrs=" + attrs + "]";
	}

}
