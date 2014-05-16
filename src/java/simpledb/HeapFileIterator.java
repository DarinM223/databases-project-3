package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator class to iterate through all tuples in a DbFile. For description of
 * methods, see DbFileIterator
 * 
 * @author RadhikaMalik
 * 
 */
public class HeapFileIterator implements DbFileIterator {

	TransactionId tid;
	DbFile file;
	int currentPageNumber;
	HeapPage currentPage;
	int numPages;
	BufferPool pool;
	Iterator<Tuple> pageIterator;
	boolean open;

	/**
	 * Create a new HeapFileIterator for a given DbFile and TransactionId
	 * 
	 * @param tid
	 * @param file
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public HeapFileIterator(TransactionId tid, DbFile file) {

		this.tid = tid;
		this.file = file;
		currentPageNumber = 0;
		pool = Database.getBufferPool();
		numPages = ((HeapFile) file).numPages();

	}

	// See DbFileIterator.java
	@Override
	public void close() {
		open = false;
		currentPage = null;
		pageIterator = null;

	}

	// See DbFileIterator.java
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {

		if (open == false)
			return false;

		// if current page has tuples left, return true
		try {

			if (pageIterator.hasNext())
				return true;

			// otherwise increment currentPage
			currentPageNumber++;

			// check if pages have finished
			if (currentPageNumber >= numPages) {
				return false;
			}
			currentPage = (HeapPage) pool.getPage(this.tid, new HeapPageId(file
					.getId(), currentPageNumber), Permissions.READ_ONLY);
			pageIterator = currentPage.iterator();
			return pageIterator.hasNext();

		} catch (NullPointerException e) {
			System.out.println("NullPointerException");
			return false;
		}
	}

	// See DbFileIterator.java

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		// fail if iterator is closed
		if (open == false)
			throw new NoSuchElementException();

		//if tuples remain
		if (this.hasNext())
			return pageIterator.next();
		else
			throw new NoSuchElementException();

	}

	// See DbFileIterator.java
	@Override
	public void open() throws DbException, TransactionAbortedException {

		// already open so do nothing
		if (open == true)
			return;

		// was closed earlier so re-read current page
		currentPage = (HeapPage) pool.getPage(this.tid, new HeapPageId(file
				.getId(), currentPageNumber), Permissions.READ_ONLY);
		pageIterator = currentPage.iterator();
		open = true;

	}

	/**
	 * Restarts the iterator Throws DbException when you try to rewind closed
	 * iterator
	 */
	@Override
	public void rewind() throws DbException, TransactionAbortedException {

		// trying to rewind a closed iterator
		if (open == false)
			throw new RuntimeException("rewinding a closed iterator");

		// iterator is open so rewind to page 0!
		currentPageNumber = 0;
		currentPage = (HeapPage) pool.getPage(this.tid, new HeapPageId(file
				.getId(), currentPageNumber), Permissions.READ_ONLY);
		pageIterator = currentPage.iterator();

	}

}
