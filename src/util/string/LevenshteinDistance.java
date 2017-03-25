package util.string;

/**
 * 来文史特距离算法
 * 
 * @author YangJian
 * @since 1.0
 */
public class LevenshteinDistance {

	/**
	 * 获取三个数中最小的一个
	 * 
	 * @param a
	 * @param b
	 * @param c
	 * @return
	 */
	private static int min(int a, int b, int c) {
		int mi;

		mi = a;
		if (b < mi) {
			mi = b;
		}
		if (c < mi) {
			mi = c;
		}
		return mi;
	}

	/**
	 * 计算编辑距离
	 * 
	 * @param s
	 * @param t
	 * @return
	 */
	public static int ld(String s, String t) {
		int d[][]; // 二维矩阵
		int n; // s的长度
		int m; // t的长度
		int i; // s的位置指针
		int j; // t的位置指针
		char s_i; // s指针位置的字符
		char t_j; // t指针位置的字符
		int cost; // 消耗

		// Step 1

		n = s.length();
		m = t.length();
		if (n == 0) {
			return m;
		}
		if (m == 0) {
			return n;
		}
		d = new int[n + 1][m + 1];

		// Step 2

		for (i = 0; i <= n; i++) {
			d[i][0] = i;
		}

		for (j = 0; j <= m; j++) {
			d[0][j] = j;
		}

		// Step 3

		for (i = 1; i <= n; i++) {

			s_i = s.charAt(i - 1);

			// Step 4

			for (j = 1; j <= m; j++) {

				t_j = t.charAt(j - 1);

				// Step 5

				if (s_i == t_j) {
					cost = 0;
				} else {
					cost = 1;
				}

				// Step 6

				d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);

			}

		}

		// Step 7

		return d[n][m];

	}

	/**
	 * 计算2个字符串的相似度
	 * 
	 * @param s
	 * @param t
	 * @return
	 */
	public static float similarity(String s, String t) {
		double len = s.length() > t.length() ? s.length() : t.length();

		int d = ld(s, t);

		return (float) ((len - d) / len);
	}

	// 单元测试
	public static void main(String[] args) {
		String s = "yangjian";
		String t = "yanjano";

		int ld = LevenshteinDistance.ld(s, t);

		System.out.println(ld);

		float similarity = LevenshteinDistance.similarity(s, t);
		System.out.println(similarity);
	}

}
