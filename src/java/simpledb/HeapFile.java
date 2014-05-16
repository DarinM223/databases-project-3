package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
        private File f;
        private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int off = pid.pageNumber() * BufferPool.PAGE_SIZE;
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        try {
                RandomAccessFile raf = new RandomAccessFile(this.f, "r");
                raf.seek(off);

                for (int i = 0; i < BufferPool.PAGE_SIZE; i++) {
                        data[i] = raf.readByte();
                }

                raf.close();
                return new HeapPage((HeapPageId)pid, data);
        } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
        } catch (IOException e) {
        	    throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile file = null;
        byte[] bytes = page.getPageData();
        try {
                file = new RandomAccessFile(getFile(), "rw");
                file.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
                file.write(bytes);
                file.close();
        } catch (FileNotFoundException e) {
                e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        BufferPool pool = Database.getBufferPool();
        if (t == null) {
                throw new DbException("Tuple is null");
        }

        ArrayList<Page> pages = new ArrayList<Page>();

        //build list of pages
        for (int i = 0; i < this.numPages(); i++) {
                PageId pid = new HeapPageId(getId(), i);
                HeapPage page = (HeapPage) pool.getPage(tid, pid, Permissions.READ_WRITE);
                if (page.getNumEmptySlots() != 0) {
                        page.insertTuple(t);
                        pages.add(page);
                        break;
                }
        }
        //if there are no pages, create page data
        if (pages.isEmpty()) {
                PageId pid = new HeapPageId(getId(), numPages());
                try {
                        byte[] b = HeapPage.createEmptyPageData();
                        RandomAccessFile file = new RandomAccessFile(getFile(), "rw");
                        file.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
                        file.write(b);
                        file.close();
                } catch (FileNotFoundException e) {
                        e.printStackTrace();
                }
                HeapPage page = (HeapPage) pool.getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                pages.add(page);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        
        BufferPool pool = Database.getBufferPool();
        PageId pid = t.getRecordId().getPageId();

        HeapPage page = (HeapPage) pool.getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> retList = new ArrayList<Page>();
        //add deleted page to return list
        retList.add(page);
        return retList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

