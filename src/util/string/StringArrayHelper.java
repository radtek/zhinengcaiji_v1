package util.string;

import java.util.*;

@SuppressWarnings("unchecked")
public final class StringArrayHelper {

	public static final int ASCENDING = 1;

	public static final int DESCENDING = -1;

	public static String[] addStringArrays(String[] dataA, String[] dataB) {

		String[] data = new String[dataA.length + dataB.length];

		System.arraycopy(dataA, 0, data, 0, dataA.length);
		System.arraycopy(dataB, 0, data, dataA.length, dataB.length);

		return data;
	}

	public static String[] addStringToArray(String dataA, String[] dataB) {

		String[] data = new String[dataB.length + 1];

		System.arraycopy(dataB, 0, data, 0, dataB.length);
		data[data.length - 1] = dataA;

		return data;
	}

	public static String arrayToString(String[] data, String delim) {
		return arrayToString(data, delim, 0, data.length);
	}

	public static String arrayToString(String[] data, String delim, int start, int end) {

		StringBuffer st = new StringBuffer();

		for (int loop = start; loop < end; loop++) {
			if (loop != start)
				st.append(delim);
			st.append(data[loop]);
		}

		return st.toString();
	}

	public static String[] parseFields(String source, char delimeter) {
		return parseFields(source, delimeter, '\"', '\"');
	}

	@SuppressWarnings("rawtypes")
	public static String[] parseFields(String source, char delimeter, char startIgnor, char endIgnor) {
		if (source == null)
			return new String[0];

		StringBuffer token = new StringBuffer();
		Vector tokens = new Vector();
		char c;

		int insideLevel = 0;
		for (int loop = 0; loop < source.length(); loop++) {
			c = source.charAt(loop);

			if (c == startIgnor && insideLevel == 0) {
				insideLevel++;
			} else if (c == endIgnor) {
				insideLevel--;

				if (startIgnor == endIgnor) {
					insideLevel--;
				}

				if (insideLevel < 0) {
					insideLevel = 0;
				}
			}

			if (insideLevel == 0 && c == delimeter) {
				tokens.addElement(token);
				token = new StringBuffer();
			} else {
				token.append(source.charAt(loop));
			}
		}

		if (token.length() > 0) {
			tokens.addElement(token);
		}

		return vectorToStringArray(tokens);
	}

	public static String[] parseFields(String source, String delimiters) {

		String[] data;

		StringTokenizer tokenizer = new StringTokenizer(source, delimiters);

		data = new String[tokenizer.countTokens()];
		int loop = 0;
		while (tokenizer.hasMoreTokens()) {
			data[loop] = tokenizer.nextToken();
			loop++;
		}

		return data;
	}

	@SuppressWarnings("rawtypes")
	public static String[] parseSpacedFields(String source) {

		if (source == null)
			return new String[0];

		StringBuffer token = new StringBuffer();
		Vector tokens = new Vector();
		char c;

		boolean insideQuotes = false;
		for (int loop = 0; loop < source.length(); loop++) {
			c = source.charAt(loop);

			if (c == '"' || c == '\'') {
				if (insideQuotes == true)
					insideQuotes = false;
				else
					insideQuotes = true;
			}

			if (!insideQuotes && c == ' ') {
				tokens.addElement(token);
				token = new StringBuffer();
			} else {
				token.append(source.charAt(loop));
			}
		}

		if (token.length() > 0)
			tokens.addElement(token);

		return vectorToStringArray(tokens);
	}

	@SuppressWarnings("rawtypes")
	public static String[] stringToArray(String data) {

		Vector arrayV = new Vector();

		StringBuffer buffer = new StringBuffer();

		for (int loop = 0; loop < data.length(); loop++) {
			switch (data.charAt(loop)) {
				case 0x0A :
					arrayV.addElement(buffer.toString());
					buffer = new StringBuffer();
					break;
				case 0x0D :
					break;
				case '\t' :
					buffer.append("    ");
					break;
				default :
					buffer.append(data.charAt(loop));
			}
		}

		if (buffer.length() > 0)
			arrayV.addElement(buffer.toString());

		String[] array = new String[arrayV.size()];
		for (int loop = 0; loop < arrayV.size(); loop++) {
			array[loop] = (String) arrayV.elementAt(loop);
		}

		return array;
	}

	@SuppressWarnings("rawtypes")
	public static String[] vectorToStringArray(Vector data) {

		String[] array = new String[data.size()];

		for (int loop = 0; loop < data.size(); loop++) {
			array[loop] = data.elementAt(loop).toString();
		}

		return array;
	}

	public static String[] stripDoubleQuotes(String[] src) {
		for (int loop = 0; loop < src.length; loop++) {
			if (src[loop].length() > 2 && src[loop].charAt(0) == '\"' && src[loop].charAt(src[loop].length() - 1) == '\"') {
				src[loop] = src[loop].substring(1, src[loop].length() - 1);
			}
		}

		return src;
	}

	public static String[] removeEmptyStrings(String[] src) {

		int emptyCount = 0;
		for (int loop = 0; loop < src.length; loop++) {
			if (src[loop].length() == 0) {
				emptyCount++;
			}
		}

		if (emptyCount == 0) {
			return src;
		}

		String[] target = new String[src.length - emptyCount];
		int entry = 0;

		for (int loop = 0; loop < src.length; loop++) {
			if (src[loop].length() > 0) {
				target[entry] = src[loop];
				entry++;
			}
		}

		return target;
	}

	public static final String[] sort(String[] table, int direction) {
		return applyIndex(table, createIndex(table, direction));
	}

	public static final String[] applyIndex(String[] table, int[] index) {

		// System.out.println("applyIndex");

		int size = table.length;
		if (index.length != size)
			throw (new ArrayIndexOutOfBoundsException());

		String[] tempTable = new String[table.length];
		int loop;
		for (loop = 0; loop < size; loop++)
			tempTable[loop] = table[index[loop]];

		return tempTable;

		// System.out.println("applyIndex done");
	}

	public final static int[] createIndex(String[] table, int direction) {

		// System.out.println("createIndex");

		int size = table.length;

		if (size == 0)
			return new int[0];

		if (direction != ASCENDING && direction != DESCENDING)
			return new int[table.length];

		int[] index = new int[size];
		boolean[] indexed = new boolean[size];

		int loop;
		for (loop = 0; loop < size; loop++)
			indexed[loop] = false;

		int loop2;
		int foundIndex;
		String foundObj;
		for (loop = 0; loop < size; loop++) {
			// System.out.println("createIndex:"+loop);

			foundIndex = 0;
			foundObj = null;

			for (loop2 = 0; loop2 < size; loop2++) {
				// System.out.println("createIndex2:"+loop);

				if (indexed[loop2] == true)
					continue;

				if (foundObj == null) {
					foundObj = table[loop2];
					foundIndex = loop2;
					continue;
				}

				if (direction == ASCENDING) {
					if (foundObj.compareTo(table[loop2]) > 0) {
						foundObj = table[loop2];
						foundIndex = loop2;
					}
				} else {
					if (foundObj.compareTo(table[loop2]) < 0) {
						foundObj = table[loop2];
						foundIndex = loop2;
					}
				}
			}
			// System.out.println("Indexed "+loop+" -> "+foundIndex+" =
			// "+(String)data.elementAt(foundIndex));
			indexed[foundIndex] = true;
			index[loop] = foundIndex;
		}
		// System.out.println("Index created");

		// System.out.println("createIndex done");

		return index;
	}

	@SuppressWarnings("rawtypes")
	public static Vector stringArrayToVector(String[] data) {
		Vector vData = new Vector();
		for (int loop = 0; loop < data.length; loop++) {
			if (data[loop].length() > 0 && data[loop].charAt(data[loop].length() - 1) == '\r') {
				data[loop] = data[loop].substring(0, data[loop].length() - 1);
			}
			vData.addElement(data[loop]);
		}

		return vData;
	}
}
