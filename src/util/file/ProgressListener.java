package util.file;

/**
 * The Progress Listener interface is implemented by a class that needs to be informed of the progress of a thread. The class could be a visual
 * component that informs a user of a tasks progress.
 */

public interface ProgressListener {

	void finished();

	void message(String message);

	/**
	 * @param poisition
	 *            long
	 * @param end
	 *            long
	 */
	void progress(long poisition, long end);
}
