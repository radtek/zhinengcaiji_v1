package util.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import util.string.StringArrayHelper;

/**
 * 文件操作辅助类
 * 
 * @author YangJian
 * @since 1.0
 */
public class FileHelper {

	public static boolean buildFile(String fileName, byte[] data) {

		FileOutputStream out = null;

		try {
			out = new FileOutputStream(fileName);
			out.write(data);
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static boolean appendFile(String fileName, byte[] data) {

		FileOutputStream out = null;

		try {
			out = new FileOutputStream(fileName, true);
			out.write(data);
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static boolean buildFile(String fileName, int[] data) {

		FileOutputStream out = null;

		try {
			out = new FileOutputStream(fileName);
			for (int loop = 0; loop < data.length; loop++) {
				out.write(data[loop]);
			}
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static boolean buildFile(String fileName, String data) {

		FileOutputStream out = null;

		try {
			File file = new File(new File(fileName).getAbsolutePath());
			file.getParentFile().mkdirs();
			out = new FileOutputStream(file.getAbsolutePath());
			out.write(data.getBytes());
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static Hashtable<String, String> buildHashtableFromFile(String fileName) throws IOException {
		String fileData = FileHelper.readFile(fileName);
		Hashtable<String, String> data = new Hashtable<String, String>();

		String[] row = StringArrayHelper.parseFields(fileData, '\n');
		for (int loop = 0; loop < row.length; loop++) {
			if (row[loop].charAt(row[loop].length() - 1) == '\r') {
				row[loop] = row[loop].substring(0, row[loop].length() - 1);
			}
			String[] field = StringArrayHelper.parseFields(row[loop], '=');
			data.put(field[0], field[1]);
		}

		return data;
	}

	public static Vector<String> buildVectorFromFile(String fileName) throws IOException {
		String fileData = FileHelper.readFile(fileName);
		String[] row = StringArrayHelper.parseFields(fileData, '\n');
		Vector<String> data = new Vector<String>();
		for (int loop = 0; loop < row.length; loop++) {
			if (row[loop].charAt(row[loop].length() - 1) == '\r') {
				row[loop] = row[loop].substring(0, row[loop].length() - 1);
			}
			data.addElement(row[loop]);
		}

		return data;
	}

	public static List<String> buildListFromFile(String fileName) throws IOException {
		String fileData = FileHelper.readFile(fileName);
		String[] row = StringArrayHelper.parseFields(fileData, '\n');
		List<String> data = new ArrayList<String>();
		for (int loop = 0; loop < row.length; loop++) {
			if (row[loop].charAt(row[loop].length() - 1) == '\r') {
				row[loop] = row[loop].substring(0, row[loop].length() - 1);
			}
			data.add(row[loop]);
		}

		return data;
	}

	public static long copy(File fileSrc, File fileDest) {
		return copy(fileSrc, fileDest, null);
	}

	public static long copy(File fileSrc, File fileDest, ProgressListener listener) {
		byte[] buffer = new byte[5000];
		long count = 0;
		int sizeRead;

		try {
			FileInputStream iStream = new FileInputStream(fileSrc);

			new File(getPath(fileDest.toString())).mkdirs();

			FileOutputStream oStream = new FileOutputStream(fileDest);

			sizeRead = iStream.read(buffer);
			while (sizeRead > 0) {
				oStream.write(buffer, 0, sizeRead);
				oStream.flush();
				count += sizeRead;
				if (listener != null) {
					listener.progress(count, -1);
				}

				sizeRead = iStream.read(buffer);
			}

			iStream.close();
			oStream.flush();
			oStream.close();

			if (listener != null) {
				listener.finished();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public static long copy(InputStream iStream, String fileOut) {
		return copy(iStream, fileOut, null);
	}

	public static long copy(InputStream iStream, String fileOut, ProgressListener listener) {
		return copy(iStream, fileOut, listener, -1);
	}

	public static long copy(String fileSrc, String fileDest) {
		return copy(new File(fileSrc), new File(fileDest), null);
	}

	public static boolean isSame(File fileA, File fileB) {
		try {
			if (!fileA.exists() || !fileB.exists()) {
				return false;
			}

			if (fileA.length() != fileB.length()) {
				return false;
			}

			FileInputStream iStreamA = new FileInputStream(fileA);
			FileInputStream iStreamB = new FileInputStream(fileB);

			int inA = iStreamA.read();
			int inB = iStreamB.read();
			boolean same = true;

			while (inA != -1 && inB != -1) {
				if (inA != inB) {
					same = false;
					break;
				}
				inA = iStreamA.read();
				inB = iStreamB.read();
			}

			iStreamA.close();
			iStreamB.close();
			return same;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String getPath(String fileName) {
		File file = new File(fileName);
		return fileName.substring(0, fileName.indexOf(file.getName()));
	}

	public static byte[] readFile(InputStream iStream) throws IOException {
		int bytePos = 0;
		int bIn = iStream.read();
		byte[] byteArray = new byte[1000];

		while (bIn != -1) {
			byteArray[bytePos] = (byte) bIn;
			bytePos++;

			if (bytePos % 1000 == 0 && bytePos != 0) {
				byte[] newByteArray = new byte[bytePos + 1000];
				System.arraycopy(byteArray, 0, newByteArray, 0, bytePos);
				byteArray = newByteArray;
			}

			bIn = iStream.read();
		}

		byte[] newByteArray = new byte[bytePos];
		System.arraycopy(byteArray, 0, newByteArray, 0, bytePos);
		byteArray = newByteArray;

		return byteArray;
	}

	public static String readFile(String fileName) throws IOException {

		return readFileToBuffer(fileName).toString();
	}

	/**
	 *
	 */
	public static byte[] readTail(String fileName, int maxData) {
		File file = new File(fileName);
		if (!file.exists())
			return null;

		long length = file.length();
		long skip = length - maxData;

		if (length < maxData) {
			maxData = (int) length;
		}

		byte[] data = new byte[maxData];

		try {
			FileInputStream fI = new FileInputStream(fileName);
			if (length > maxData) {
				fI.skip(skip);
			}
			fI.read(data, 0, maxData);
			fI.close();
		} catch (Exception e) {
			System.out.println("Failed to check File " + fileName);
			e.printStackTrace();
			return null;
		}

		return data;
	}

	public static boolean buildFile(OutputStream out, String data) {
		try {
			out.write(data.getBytes());
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static long copy(InputStream iStream, String fileOut, ProgressListener listener, long length) {
		byte[] buffer = new byte[5000];
		long count = 0;
		int sizeRead;

		if (length == -1) {
			length = Long.MAX_VALUE;
		}

		try {
			new File(getPath(fileOut)).mkdirs();

			FileOutputStream oStream = new FileOutputStream(fileOut);

			sizeRead = iStream.read(buffer);
			while (sizeRead > 0 && count < length) {
				oStream.write(buffer, 0, sizeRead);
				// oStream.flush();
				count += sizeRead;
				if (listener != null) {
					listener.progress(count, -1);
				}

				if (count >= length) {
					break;
				}

				sizeRead = iStream.read(buffer);
			}

			// iStream.close();
			oStream.flush();
			oStream.close();

			if (listener != null) {
				listener.finished();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public static long copy(InputStream iStream, OutputStream oStream, ProgressListener listener, long length) {
		byte[] buffer = new byte[5000];
		long count = 0;
		int sizeRead;

		if (length == -1) {
			length = Long.MAX_VALUE;
		}

		try {
			sizeRead = iStream.read(buffer);
			while (sizeRead > 0 && count < length) {
				oStream.write(buffer, 0, sizeRead);
				count += sizeRead;
				if (listener != null) {
					listener.progress(count, -1);
				}

				if (count >= length) {
					break;
				}

				sizeRead = iStream.read(buffer);
			}

			oStream.flush();

			if (listener != null) {
				listener.finished();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public static long copy(String fileIn, OutputStream oStream, ProgressListener listener) {
		byte[] buffer = new byte[10000];
		long count = 0;
		int sizeRead;
		long length;

		try {
			if ((oStream instanceof BufferedOutputStream) == false) {
				oStream = new BufferedOutputStream(oStream);
			}

			length = new File(fileIn).length();
			InputStream iStream = new FileInputStream(fileIn);
			sizeRead = iStream.read(buffer);

			while (sizeRead > 0) {
				oStream.write(buffer, 0, sizeRead);
				count += sizeRead;
				oStream.flush();

				if (listener != null) {
					listener.progress(count, length);
				}

				sizeRead = iStream.read(buffer);
			}

			oStream.flush();
			iStream.close();
			// oStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public static StringBuffer readFileToBuffer(String fileName) throws IOException {

		return new StringBuffer(new String(readFile(new FileInputStream(new File(fileName).getAbsolutePath()))));
	}

	public static String getFileExtension(String fileName) {

		if (fileName.indexOf(".") < 0)
			return "";

		String ext = fileName.substring(fileName.lastIndexOf(".") + 1);
		return ext;
	}

	public static boolean deleteFile(String file) {
		return delete(new File(file).getAbsoluteFile());
	}

	public static boolean delete(File file) {
		if (file.isDirectory()) {
			String[] children = file.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = delete(new File(file, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return file.delete();

		// reference: http://javaalmanac.com/egs/java.io/DeleteDir.html
	}

	public static String createMD5(String fileName) throws IOException, NoSuchAlgorithmException {
		MessageDigest md5Algorithm = MessageDigest.getInstance("MD5");

		byte[] byteArray = new byte[1000];
		FileInputStream iStream = new FileInputStream(new File(fileName).getAbsolutePath());
		int size = iStream.read(byteArray);

		while (size != -1) {
			md5Algorithm.update(byteArray, 0, size);
			size = iStream.read(byteArray);
		}

		byte[] digest = md5Algorithm.digest();
		StringBuffer hexString = new StringBuffer();

		String hexDigit = null;
		for (int i = 0; i < digest.length; i++) {
			hexDigit = Integer.toHexString(0xFF & digest[i]);

			if (hexDigit.length() < 2) {
				hexDigit = "0" + hexDigit;
			}

			hexString.append(hexDigit);
		}

		return hexString.toString();
	}

}
