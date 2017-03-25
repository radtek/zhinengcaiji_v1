package parser.boco.am;

import util.Util;

public class TField {

	public int index;

	public String name;

	public String dateFormat;

	public TField() {
		super();
	}

	public TField(int index, String name, String dateFormat) {
		super();
		this.index = index;
		this.name = name;
		this.dateFormat = dateFormat;
	}

	public boolean isDate() {
		return Util.isNotNull(this.dateFormat);
	}

	@Override
	public String toString() {
		return "TField [index=" + index + ", name=" + name + ", dateFormat=" + dateFormat + "]";
	}

}
