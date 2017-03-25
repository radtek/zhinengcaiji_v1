package templet.hw.cdma.pm;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import templet.AbstractTempletBase;
import util.LogMgr;
import framework.SystemConfig;

public class CDMA_GD_HW_P extends AbstractTempletBase {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private SortedMap<Integer, CDMA_GD_HW_P_Templet> templets;

	@Override
	public void parseTemp(String tempContent) throws Exception {
		File file = new File(SystemConfig.getInstance().getTempletPath() + File.separator + tempContent);
		if (!file.exists() || !file.isFile())
			throw new Exception("模板文件（" + file + "）不存在。");

		SAXReader sx = new SAXReader();
		Document doc = sx.read(file);
		List<Element> templetEls = doc.getRootElement().elements();
		this.templets = new TreeMap<Integer, CDMA_GD_HW_P.CDMA_GD_HW_P_Templet>();
		for (int i = 0; i < templetEls.size(); i++) {
			Element empletEl = templetEls.get(i);
			int id = Integer.parseInt(empletEl.attributeValue("id"));
			String elementType = empletEl.attributeValue("elementType");
			SortedMap<Integer, CDMA_GD_HW_P_Field> fields = new TreeMap<Integer, CDMA_GD_HW_P.CDMA_GD_HW_P_Field>();
			List<Element> fieldEls = empletEl.elements("field");
			for (int j = 0; j < fieldEls.size(); j++) {
				String fname = fieldEls.get(j).attributeValue("name");
				int index = Integer.parseInt(fieldEls.get(j).attributeValue("index"));
				CDMA_GD_HW_P_Field f = new CDMA_GD_HW_P_Field(index, fname);
				fields.put(index, f);
			}
			this.templets.put(id, new CDMA_GD_HW_P_Templet(id, elementType, fields));
		}
	}

	public CDMA_GD_HW_P_Templet findTempletByElementType(String elementType) {
		if (elementType == null)
			return null;

		if (this.templets == null || this.templets.isEmpty())
			return null;
		for (int i = 0; i < this.templets.size(); i++) {
			if (this.templets.get(i) != null && this.templets.get(i).getElementType().equals(elementType))
				return this.templets.get(i);
		}
		return null;
	}

	public class CDMA_GD_HW_P_Templet {

		int id;

		String elementType;

		SortedMap<Integer, CDMA_GD_HW_P_Field> fields;

		public CDMA_GD_HW_P_Templet(int id, String elementType, SortedMap<Integer, CDMA_GD_HW_P_Field> fields) {
			super();
			this.id = id;
			this.elementType = elementType;
			this.fields = fields;
		}

		public boolean containsField(String field) {
			if (this.fields == null || this.fields.isEmpty())
				return false;
			Collection<CDMA_GD_HW_P_Field> co = this.fields.values();
			for (CDMA_GD_HW_P_Field f : co) {
				if (f != null && f.getName() != null && f.getName().equalsIgnoreCase(field))
					return true;
			}
			return false;
		}

		public CDMA_GD_HW_P_Field getField(String field) {
			if (this.fields == null || this.fields.isEmpty())
				return null;
			Collection<CDMA_GD_HW_P_Field> co = this.fields.values();
			for (CDMA_GD_HW_P_Field f : co) {
				if (f != null && f.getName() != null && f.getName().equalsIgnoreCase(field))
					return f;
			}
			return null;
		}

		public void resetRealIndexAndValues() {
			if (this.fields == null || this.fields.isEmpty())
				return;
			for (CDMA_GD_HW_P_Field f : this.fields.values()) {
				if (f != null) {
					f.setRealIndex(-1);
					f.setValue(null);
				}
			}
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getElementType() {
			return elementType;
		}

		public void setElementType(String elementType) {
			this.elementType = elementType;
		}

		public SortedMap<Integer, CDMA_GD_HW_P_Field> getFields() {
			return fields;
		}

		public void setFields(SortedMap<Integer, CDMA_GD_HW_P_Field> fields) {
			this.fields = fields;
		}

		@Override
		public String toString() {
			return "CDMA_GD_HW_P_Templet [id=" + id + ", elementType=" + elementType + ", fields=" + fields + "]";
		}

	}

	public class CDMA_GD_HW_P_Field {

		int index;

		int realIndex; // 在原始文件中的字段位置。

		String value; // 数据值。

		String name;

		public CDMA_GD_HW_P_Field(int index, String name) {
			super();
			this.index = index;
			this.name = name;
			this.realIndex = -1;
			this.value = null;
		}

		public int getRealIndex() {
			return realIndex;
		}

		public void setRealIndex(int realIndex) {
			this.realIndex = realIndex;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "CDMA_GD_HW_P_Field [index=" + index + ", name=" + name + ", realIndex=" + realIndex + ", value=" + value + "]";
		}

	}

}
