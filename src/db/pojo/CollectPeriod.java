package db.pojo;

import framework.ConstDef;

public class CollectPeriod {

	private int value;

	private String name;

	public CollectPeriod() {
		super();
	}

	public CollectPeriod(int value, String name) {
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

	public static CollectPeriod create(int value) {
		switch (value) {
			case ConstDef.COLLECT_PERIOD_FOREVER :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_FOREVER, "一直");
			case ConstDef.COLLECT_PERIOD_DAY :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_DAY, "天");
			case ConstDef.COLLECT_PERIOD_HOUR :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_HOUR, "小时");
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR, "半小时");
			case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_MINUTE_QUARTER, "15分钟");
			case ConstDef.COLLECT_PERIOD_4HOUR :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_4HOUR, "4小时");
			case ConstDef.COLLECT_PERIOD_5MINUTE :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_5MINUTE, "5分钟");
			case ConstDef.COLLECT_PERIOD_HALFDAY :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_HALFDAY, "12小时");
			case ConstDef.COLLECT_PERIOD_WEEK :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_WEEK, "一周");
			case ConstDef.COLLECT_PERIOD_MONTH :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_WEEK, "一月");
			case ConstDef.COLLECT_PERIOD_ONE_MINUTE :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_WEEK, "一分钟");
			case ConstDef.COLLECT_PERIOD_10MINUTE :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_10MINUTE, "10分钟");
			case ConstDef.COLLECT_PERIOD_2MINUTE :
				return new CollectPeriod(ConstDef.COLLECT_PERIOD_2MINUTE, "2分钟");
		}
		return new CollectPeriod(-1, "未知");
	}
}
