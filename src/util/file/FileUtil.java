package util.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

public class FileUtil {

	/**
	 * 将文件移动到另一个目录下. 只适合在本地硬盘下进行移动文件.不支持网络上的移动
	 * 
	 * @param sourfile
	 * @param newPath
	 * @return
	 */
	public static boolean filerename(String sourfile, String newPath) {
		try {
			// 文件原地址
			File oldFile = new File(sourfile);
			// new一个新文件夹
			File fnewpath = new File(newPath);
			// 判断文件夹是否存在
			if (!fnewpath.exists())
				fnewpath.mkdirs();
			// 将文件移到新文件里
			File fnew = new File(newPath + File.separator + oldFile.getName());
			oldFile.renameTo(fnew);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 复制目录下的文件（不包括该目录）到指定目录，会连同子目录一起复制过去。
	 * 
	 * @param targetFile
	 * @param path
	 */
	public static void copyFileFromDir(String targetDir, String path) {
		File file = new File(path);
		createFile(targetDir, false);
		if (file.isDirectory()) {
			copyFileToDir(targetDir, listFile(file));
		}
	}

	/**
	 * 复制目录下的文件（不包含该目录和子目录，只复制目录下的文件）到指定目录。
	 * 
	 * @param targetDir
	 * @param path
	 */
	public static void copyFileOnly(String targetDir, String path) {
		File file = new File(path);
		File targetFile = new File(targetDir);
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File subFile : files) {
				if (subFile.isFile()) {
					copyFile(targetFile, subFile);
				}
			}
		}
	}

	/**
	 * 复制目录到指定目录。targetDir是目标目录，path是源目录。 该方法会将path以及path下的文件和子目录全部复制到目标目录
	 * 
	 * @param targetDir
	 * @param path
	 */
	public static void copyDir(String targetDir, String path) {
		File targetFile = new File(targetDir);
		createFile(targetFile, false);
		File file = new File(path);
		if (targetFile.isDirectory() && file.isDirectory()) {
			copyFileToDir(targetFile.getAbsolutePath() + "/" + file.getName(), listFile(file));
		}
	}

	/**
	 * 复制一组文件到指定目录。targetDir是目标目录，filePath是需要复制的文件路径
	 * 
	 * @param targetDir
	 * @param filePath
	 */
	public static void copyFileToDir(String targetDir, String... filePath) {
		if (targetDir == null || "".equals(targetDir)) {
			// System.out.println("参数错误，目标路径不能为空");
			return;
		}
		File targetFile = new File(targetDir);
		if (!targetFile.exists()) {
			targetFile.mkdir();
		} else {
			if (!targetFile.isDirectory()) {
				// System.out.println("参数错误，目标路径指向的不是一个目录！");
				return;
			}
		}
		for (String path : filePath) {
			File file = new File(path);
			if (file.isDirectory()) {
				copyFileToDir(targetDir + "/" + file.getName(), listFile(file));
			} else {
				copyFileToDir(targetDir, file, "");
			}
		}
	}

	/**
	 * 复制文件到指定目录。targetDir是目标目录，file是源文件名，newName是重命名的名字。
	 * 
	 * @param targetFile
	 * @param file
	 * @param newName
	 */
	public static void copyFileToDir(String targetDir, File file, String newName) {
		String newFile = "";
		if (newName != null && !"".equals(newName)) {
			newFile = targetDir + "/" + newName;
		} else {
			newFile = targetDir + "/" + file.getName();
		}
		File tFile = new File(newFile);
		copyFile(tFile, file);
	}

	/**
	 * 复制文件。targetFile为目标文件，file为源文件
	 * 
	 * @param targetFile
	 * @param file
	 */
	public static void copyFile(File targetFile, File file) {
		if (targetFile.exists()) {
			// System.out.println("文件" + targetFile.getAbsolutePath()
			// + "已经存在，跳过该文件！");
			return;
		} else {
			createFile(targetFile, true);
		}
		// System.out.println("复制文件" + file.getAbsolutePath() + "到"
		// + targetFile.getAbsolutePath());
		try {
			InputStream is = new FileInputStream(file);
			FileOutputStream fos = new FileOutputStream(targetFile);
			byte[] buffer = new byte[1024];
			while (is.read(buffer) != -1) {
				fos.write(buffer);
			}
			is.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String[] listFile(File dir) {
		String absolutPath = dir.getAbsolutePath();
		String[] paths = dir.list();
		String[] files = new String[paths.length];
		for (int i = 0; i < paths.length; i++) {
			files[i] = absolutPath + "/" + paths[i];
		}
		return files;
	}

	public static void createFile(String path, boolean isFile) {
		createFile(new File(path), isFile);
	}

	public static void createFile(File file, boolean isFile) {
		if (!file.exists()) {
			if (!file.getParentFile().exists()) {
				createFile(file.getParentFile(), false);
			} else {
				if (isFile) {
					try {
						file.createNewFile();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					file.mkdir();
				}
			}
		}
	}
	
	/**
	 * 扫描本地文件<br>
	 * 
	 * @param path
	 *            指定的目录
	 * @param mappings
	 *            文件名需要匹配的正则表达式
	 * @return
	 */
	private List<String> listFiles(String dir, List<String> mappings) {
		if (mappings == null || mappings.isEmpty())
			return getFileNames(dir);
		List<String> fileList = new LinkedList<String>();
		for (int i = 0; i < mappings.size(); i++) {
			List<String> files = getFileNames(dir, mappings.get(i));
			if (files != null && !files.isEmpty())
				fileList.addAll(files);
		}
		return fileList;
	}
	
	/**
	 * 获取指定文件夹下的所有文件列表，包含子文件夹
	 * 
	 * @param path
	 *            文件夹路径
	 * @return 文件夹下的所有文件组成的列表
	 */
	public static List<String> getFileNames(String path, final String filter) {
		if (path == null || path.trim().isEmpty())
			return new ArrayList<String>();
		if (isEmpty(filter))
			return getFileNames(path);
		List<String> lst = new ArrayList<String>();
		if (isFile(path))
			lst.add(path);
		else {
			File pathFile = new File(path);
			File[] fileLst = pathFile.listFiles();
			if(fileLst == null)
				return lst;
			for (File f : fileLst) {
				if (f.isDirectory()) {
					lst.addAll(getFileNames(f.getPath(), filter));
					continue;
				}
				if (FilenameUtils.wildcardMatch(f.getName(), filter))
					lst.add(f.getAbsolutePath());
			}
		}
		return lst;
	}
	
	/**
	 * 获取指定文件夹下的所有文件列表，包含子文件夹
	 * 
	 * @param path
	 *            文件夹路径
	 * @return 文件夹下的所有文件组成的列表
	 */
	public static List<String> getFileNames(String path) {
		if (path == null || path.trim().isEmpty())
			return new ArrayList<String>();

		List<String> lst = new ArrayList<String>();
		if (isFile(path))
			lst.add(path);
		else {
			File pathFile = new File(path);
			File[] fileLst = pathFile.listFiles();
			for (File f : fileLst) {
				if (f.isFile())
					lst.add(f.getAbsolutePath());
				else
					lst.addAll(getFileNames(f.getPath())); // 递归调用
			}
		}

		return lst;
	}
	
	/**
	 * 判定指定的文件路径是否为文件还是文件夹
	 * 
	 * @param filePath
	 *            文件路径
	 * @return true表示为文件，false为文件夹
	 */
	public static boolean isFile(String filePath) {
		if (filePath == null || filePath.trim().isEmpty())
			return false;

		File file = new File(filePath);
		if (file.isFile())
			return true;
		else if (file.isDirectory())
			return false;
		else
			return false;
	}
	
	public static boolean isEmpty(String string) {
		return string == null || string.trim().length() == 0;
	}

}
