package access.special.luc;

import java.util.Map;
import java.util.SortedMap;

public class LucSubTemplet {

	String id;

	String table;

	String sct;

	String log;

	String separator;

	SortedMap<String/* src */, String/* dest */> fileds;

	public LucSubTemplet(String id, String table, String sct, String log, String separator, SortedMap<String, String> fileds) {
		super();
		this.id = id;
		this.table = table;
		this.sct = sct;
		this.log = log;
		this.separator = separator;
		this.fileds = fileds;
	}

	public String getId() {
		return id;
	}

	public String getTable() {
		return table;
	}

	public String getSct() {
		return sct;
	}

	public String getLog() {
		return log;
	}

	public String getSeparator() {
		return separator;
	}

	public Map<String, String> getFileds() {
		return fileds;
	}

	@Override
	public String toString() {
		return "LucSubTemplet [fileds=" + fileds + ", id=" + id + ", log=" + log + ", sct=" + sct + ", separator=" + separator + ", table=" + table
				+ "]";
	}

}
