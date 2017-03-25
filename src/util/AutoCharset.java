/*
 * Copyright 2007 BeiJing ZCTT Co. Ltd.
 * All right reserved. 
 * File Name: AutoChartset.java
 * Create Date: Oct 25, 2007
 */
package util;

import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

public class AutoCharset {

	public final static String ORG_STRING = "is中文=1";

	private static Hashtable<String, CharsetMap> charsetHashtable = new Hashtable<String, CharsetMap>();

	static {
		// addCharsetHashtable(charsetHashtable, "US-ASCII", "ISO-8859-1");
		// addCharsetHashtable(charsetHashtable, "US-ASCII", "UTF-8");
		// addCharsetHashtable(charsetHashtable, "US-ASCII", "UTF-16");
		// addCharsetHashtable(charsetHashtable, "US-ASCII", "GBK");
		// addCharsetHashtable(charsetHashtable, "US-ASCII", "GB2312");

		// addCharsetHashtable(charsetHashtable, "ISO-8859-1", "US-ASCII");
		addCharsetHashtable(charsetHashtable, "ISO-8859-1", "UTF-8");
		addCharsetHashtable(charsetHashtable, "ISO-8859-1", "UTF-16");
		addCharsetHashtable(charsetHashtable, "ISO-8859-1", "GBK");
		// addCharsetHashtable(charsetHashtable, "ISO-8859-1", "GB2312");

		// addCharsetHashtable(charsetHashtable, "UTF-8", "US-ASCII");
		addCharsetHashtable(charsetHashtable, "UTF-8", "ISO-8859-1");
		addCharsetHashtable(charsetHashtable, "UTF-8", "UTF-16");
		addCharsetHashtable(charsetHashtable, "UTF-8", "GBK");
		// addCharsetHashtable(charsetHashtable, "UTF-8", "GB2312");

		// addCharsetHashtable(charsetHashtable, "UTF-16", "US-ASCII");
		addCharsetHashtable(charsetHashtable, "UTF-16", "ISO-8859-1");
		addCharsetHashtable(charsetHashtable, "UTF-16", "UTF-8");
		addCharsetHashtable(charsetHashtable, "UTF-16", "GBK");
		// addCharsetHashtable(charsetHashtable, "UTF-16", "GB2312");

		// addCharsetHashtable(charsetHashtable, "GBK", "US-ASCII");
		addCharsetHashtable(charsetHashtable, "GBK", "ISO-8859-1");
		addCharsetHashtable(charsetHashtable, "GBK", "UTF-8");
		addCharsetHashtable(charsetHashtable, "GBK", "UTF-16");
		// addCharsetHashtable(charsetHashtable, "GBK", "GB2312");

		// addCharsetHashtable(charsetHashtable, "GB2312", "US-ASCII");
		// addCharsetHashtable(charsetHashtable, "GB2312", "ISO-8859-1");
		// addCharsetHashtable(charsetHashtable, "GB2312", "UTF-8");
		// addCharsetHashtable(charsetHashtable, "GB2312", "UTF-16");
		// addCharsetHashtable(charsetHashtable, "GB2312", "GBK");
	}

	private static void addCharsetHashtable(Hashtable<String, CharsetMap> hashtable, String orgCharset, String newCharset) {
		hashtable.put(ORG_STRING + "$" + orgCharset + "$" + newCharset, new CharsetMapImpl(orgCharset, newCharset));
	}

	public static String getCorrectString(String orgString, String charsetCorrectString) {
		String realString = null;

		if (!ORG_STRING.equals(charsetCorrectString)) {
			String[] keys = charsetHashtable.keySet().toArray(new String[0]);
			for (int i = 0; i < keys.length; i++) {
				CharsetMap charsetMap = charsetHashtable.get(keys[i]);
				if (charsetMap != null) {
					if (!ORG_STRING.equals(charsetMap.map(charsetCorrectString)))
						continue;

					realString = charsetMap.map(orgString);
					break;
				}
			}

			if (realString == null) {
				System.out.println("not found charset");
				realString = orgString;
			}
		} else {
			realString = orgString;
		}

		return realString;
	}

	interface CharsetMap {

		public String map(String orgString);
	}

	static class CharsetMapImpl implements CharsetMap {

		private String orgCharset;

		private String newCharset;

		CharsetMapImpl(String orgCharset, String newCharset) {
			this.orgCharset = orgCharset;
			this.newCharset = newCharset;
		}

		public String map(String orgString) {
			try {
				return new String(orgString.getBytes(orgCharset), newCharset);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return orgString;
			}
		}
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		System.out.println(getCorrectString(new String("韩1登山队思考就阿訇复苏来扩大".getBytes(), "ISO-8859-1"), new String(ORG_STRING.getBytes(), "ISO-8859-1")));
	}
}
