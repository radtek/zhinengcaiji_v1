package db.pojo;

import framework.ConstDef;

public class CollectType {

	private int value;

	private String name;

	public CollectType() {
		super();
	}

	public CollectType(int value, String name) {
		super();
		this.value = value;
		this.name = name;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static CollectType create(int value) {
		switch (value) {
			case ConstDef.COLLECT_TYPE_TELNET :
				return new CollectType(ConstDef.COLLECT_TYPE_TELNET, "telnet");
			case ConstDef.COLLECT_TYPE_TCP :
				return new CollectType(ConstDef.COLLECT_TYPE_TCP, "tcp");
			case ConstDef.COLLECT_TYPE_FTP :
				return new CollectType(ConstDef.COLLECT_TYPE_FTP, "ftp");
			case ConstDef.COLLECT_TYPE_FILE :
				return new CollectType(ConstDef.COLLECT_TYPE_FILE, "本地文件");
			case ConstDef.COLLECT_TYPE_DataBase :
				return new CollectType(ConstDef.COLLECT_TYPE_DataBase, "数据库");
			case 60 :
				return new CollectType(ConstDef.COLLECT_TYPE_DataBase, "数据库(表对表)");
		}
		return new CollectType(-1, "未知");
	}
}
