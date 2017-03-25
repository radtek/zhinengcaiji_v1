package templet;

import java.util.Comparator;

/**
 * id比较器
 * 
 * @author YangJian
 * @since 3.1
 */
public class IDComparator implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		return ((Integer) (o1.hashCode())).compareTo((Integer) (o2.hashCode()));
	}
}
