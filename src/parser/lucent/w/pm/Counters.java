package parser.lucent.w.pm;

import java.util.List;

/**
 * 一行记录，仅有值，没有字段名
 * 
 * @author ChenSijiang
 */
public class Counters {

	private List<String> values;

	public Counters(List<String> values) {
		this.values = values;
	}

	public List<String> getValues() {
		return values;
	}

	@Override
	public String toString() {
		return values.toString();
	}
}
