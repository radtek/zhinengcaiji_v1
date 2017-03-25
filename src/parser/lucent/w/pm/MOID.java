package parser.lucent.w.pm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MOID {

	private String src;// 原始内容

	private Map<String, String> contents = new HashMap<String, String>();// 保存每个属性与值

	private String last;// moid中的最后一个属性的名字

	public MOID(String src) {
		this.src = src;
		parse();
	}

	public MOID(String last, Object unused) {
		super();
		this.last = last;
	}

	public String getSrc() {
		return src;
	}

	public String getValueByName(String name) {
		return contents.get(name);
	}

	public String getLastName() {
		return last;
	}

	public void setLastName(String last) {
		this.last = last;
	}

	public String getLastValue() {
		return contents.get(getLastName());
	}

	public int getPropertyCount() {
		return contents.size();
	}

	public String[] listNames() {
		Iterator<String> it = contents.keySet().iterator();
		List<String> list = new ArrayList<String>();
		while (it.hasNext()) {
			list.add(it.next());
		}
		return list.toArray(new String[0]);
	}

	/**
	 * 业务上相等，即moid中，属性个数相同，并且最后一个属性的名字相同，便认为是同一个moid
	 * 
	 * @param moid
	 * @return
	 */
	public boolean equals2(MOID moid) {
		return this.getPropertyCount() == moid.getPropertyCount() && this.getLastName().equals(moid.getLastName());
	}

	@Override
	public boolean equals(Object obj) {
		MOID moid = (MOID) obj;
		return equals2(moid);
	}

	@Override
	public int hashCode() {
		int hash = 0;
		Iterator<String> it = contents.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			hash += key.hashCode();
		}
		return hash;
	}

	@Override
	public String toString() {
		return contents.toString();
	}

	private void parse() {
		String[] items = src.split(",");
		for (int i = 0; i < items.length; i++) {
			String[] nameValue = items[i].split("=");
			String name = nameValue[0];
			String value = nameValue[1];
			contents.put(name, value);
			if (i == items.length - 1) {
				last = name;
			}
		}
	}
}
