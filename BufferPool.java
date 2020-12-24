package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // additional fields...
    private int numPages;
    private HashMap<PageId, Page> bMap;
    private HashMap<PageId, Page> pidToPg;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.bMap = new HashMap<PageId, Page>();
        this.pidToPg = new HashMap<PageId, Page>(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        // Check if buffer pool already contains the page
        if (bMap.containsKey(pid)){
            return bMap.get(pid);
        }

        // Check size of buffer pool
        if (bMap.size() >= numPages){
            evictPage();
        }

        // If buffer pool doesn't contain the page, add it
        int tableId = pid.getTableId();
        Page pg = Database.getCatalog().getDatabaseFile(tableId).readPage(pid);
        bMap.put(pid, pg);
        return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        // get the DbFile of the specified table
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // returns the pages that were modified
        ArrayList<Page> modPages = file.insertTuple(tid, t);

        // second part -- "marks any pages that were dirtied... as dirty..."
        for (Page i : modPages){
            // mark each page dirty
            i.markDirty(true, tid);
            // "Will acquire a write lock on the page the tuple is added to and
            // any other pages that are updated..."
            if (!this.pidToPg.containsKey(i.getId())){
                this.getPage(tid, i.getId(), Permissions.READ_WRITE);
            }
            // "...and adds versions of any pages that have been dirtied to the cache."
            this.pidToPg.put(i.getId(), i);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // similar to insertTuple

        // get ID for table tuple is in
        int ID = t.getRecordId().getPageId().getTableId();
        // get table tuple is in (based on ID above)
        DbFile table = Database.getCatalog().getDatabaseFile(ID);
        // returns the pages that were modified
        ArrayList<Page> modPages = table.deleteTuple(tid, t);

        // mark each modified page as dirty
        for (Page i : modPages){
            i.markDirty(true, tid);
            PageId pgID = i.getId();
            // "Will acquire a write lock on the page the tuple is removed from and
            //  any other pages that are updated..."
            if (!this.pidToPg.containsKey(pgID)){
                this.getPage(tid, pgID, Permissions.READ_WRITE);
            }
            // "...and adds versions of any pages that have been dirtied to the cache."
            this.pidToPg.put(pgID, i);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pg : bMap.keySet()){
            flushPage(pg);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page pg = bMap.get(pid);
        // ERROR CHECK -- if the page is not in the buffer pool OR it
        // is not dirty then it should not be flushed!
        if(pg == null || pg.isDirty() == null){
            return;
        }
        int tableID = pid.getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableID);
        file.writePage(pg);
        pg.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // eviction policy --> evict the first non-dirty page
        int counter = 0;
        Object pIDArray[] = bMap.keySet().toArray();
        PageId pID = (PageId) pIDArray[counter];

        try{
            Page pg = bMap.get(pID);
            // check to see if first page in the array is dirty
            // -- if not, evict
            if(pg.isDirty() == null){
                flushPage(pID);
            }
            // check to see if following pages are dirty
            // -- once you find a non-dirty page, evict that one
            if(pg.isDirty() != null) {
                for (int i = counter + 1; i < pIDArray.length; i++) {
                    PageId pID2 = (PageId) pIDArray[i];
                    Page pg2 = bMap.get(pID2);
                    if(pg2.isDirty() == null){
                        flushPage(pID2);
                        pID = pID2;
                        break;
                    }
                }
            }

        } catch (IOException e) {
            throw new DbException("Unable to evict a page!");
        }
        // remove the appropriate page
        bMap.remove(pID);
    }

}
