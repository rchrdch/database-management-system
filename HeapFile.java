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

    // additional fields
    private File file;
    private TupleDesc tupleD;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {

        int bpsize = BufferPool.getPageSize();
        RandomAccessFile rfile = null;

        try {
            rfile = new RandomAccessFile(file, "r");
            byte[] bytes = new byte[bpsize];
            rfile.seek((long) pid.pageNumber()* bpsize);
            rfile.read(bytes, 0, bpsize);
            rfile.close();

            return new HeapPage((HeapPageId) pid, bytes);
        } catch (IOException e) {
            // consulted: https://www.educative.io/edpresso/what-is-the-printstacktrace-method-in-java
            e.printStackTrace();
        }throw new IllegalArgumentException();
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        HeapPageId hid = (HeapPageId) pid;

        RandomAccessFile randFile = new RandomAccessFile(file, "rw");
        int pageSize = BufferPool.DEFAULT_PAGES;
        int pageNum = pid.pageNumber();
        int padding = pageSize * pageNum;
        byte[] byteArray = new byte[pageSize];

        byteArray = page.getPageData();
        randFile.seek(padding);
        randFile.write(byteArray, 0, pageSize);
        randFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int size = BufferPool.getPageSize();
        int pages= (int) Math.ceil(file.length()/size);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // initialize hPage
        HeapPage hPage = null;
        int intNum = this.numPages();

        for (int i = 0; i < this.numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (hpage.getNumEmptySlots() <= 0) {
                hpage = null;
            }
            else {
                hPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            }
        }

        if (hPage != null) {
            hPage.insertTuple(t);
            return new ArrayList<Page> (Arrays.asList(hPage));
        }

        HeapPageId hpid = new HeapPageId(this.getId(), intNum);
        HeapPage hPage2 = new HeapPage(hpid, HeapPage.createEmptyPageData());
        hPage2.insertTuple(t);

        RandomAccessFile randFile = new RandomAccessFile(this.file, "rw");
        int padding = BufferPool.PAGE_SIZE * intNum;
        randFile.seek(padding);
        byte[] newHeapPageData = hPage2.getPageData();
        randFile.write(newHeapPageData, 0, BufferPool.PAGE_SIZE);
        randFile.close();

        return new ArrayList<Page> (Arrays.asList(hPage2));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != this.getId()){
            throw new DbException("Tuple doesn't exist!");
        }
        if (pid.pageNumber() >= this.numPages()){
            throw new DbException("Tuple doesn't exist!");
        }
        HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hPage.deleteTuple(t);
        return new ArrayList<>(Collections.singleton(hPage));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // iterator class implemented below...
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator{
        // method descriptions available in B+DbFileIterator.java interface file

        // current heap page we are in
        private int pos;
        // iterator for tuples of current heap page
        private TransactionId tid;
        private Iterator<Tuple> tIter;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        // iterator to move through the tuples...
        private Iterator<Tuple> tupleIterator(HeapPageId pid) throws TransactionAbortedException, DbException{
            HeapPage pg = null;
            pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return pg.iterator();
        }

        public void open() throws DbException, TransactionAbortedException{
            this.pos = 0;
            // generate an id for heap page at current position
            HeapPageId hpid = new HeapPageId(getId(), pos);
            // initialize a tuple iterator for current heap page (current pos)
            tIter = tupleIterator(hpid);
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            // ArrayIndexOutOfBounds error
            // if a next page doesn't exist -- don't check the next page
            // reorder logic

            if(tIter.hasNext()){
                return true;
            }
            //System.out.println("position is "+ pos + "out of " + numPages());
            if(pos +1 >= numPages()){ // current page is the last one
                return false;
            } else{
                // increment once
                pos++;
                HeapPageId hpid = new HeapPageId(getId(), pos);
                tIter = tupleIterator(hpid);
                if(tIter == null){
                    return false;
                }
            }

            // are we at the end of all the pages?
            // if tIter does not have next, go to the next page (don't check if its null)
            /*
            while(pos + 1 < numPages() && !tIter.hasNext()){
                pos++;
                //System.out.println("position = " + pos);
                //System.out.println("numPages = " + numPages());
                HeapPageId hpid = new HeapPageId(getId(), pos);
                //System.out.println("passed hpid declaration");
                tIter = tupleIterator(hpid);
                //System.out.println("end of while loop");
            } */
            return tIter.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            // error checking...
            if(!hasNext()){
                throw new NoSuchElementException("There are no more tuples");
            }
            // return the next tuple in the current Heap Page
            return tIter.next();
        }

        public void rewind() throws DbException, TransactionAbortedException{
            close();
            open();
        }

        public void close(){
            this.tIter = null;
            this.pos = 0;
        }

    }
}

