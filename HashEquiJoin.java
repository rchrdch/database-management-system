package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    // initializing fields
    private JoinPredicate pred;
    private DbIterator ch1;
    private DbIterator ch2;
    private Tuple tup1;
    private Tuple tup2;
    private TupleDesc joinedTup;
    HashMap<Object, ArrayList<Tuple>> hMap;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.ch1 = child1;
        this.ch2 = child2;
        TupleDesc tup1 = ch1.getTupleDesc();
        TupleDesc tup2 = ch2.getTupleDesc();
        this.joinedTup = TupleDesc.merge(tup1, tup2);
        hMap = new HashMap<Object, ArrayList<Tuple>>();
    }

    public JoinPredicate getJoinPredicate() {
        return this.pred;
    }

    public TupleDesc getTupleDesc() {
        return this.joinedTup;
    }

    public String getJoinField1Name() {
        TupleDesc tup1 = this.ch1.getTupleDesc();
        String name1 = tup1.getFieldName(this.pred.getField1());
        return name1;
    }

    public String getJoinField2Name() {
        TupleDesc tup2 = this.ch2.getTupleDesc();
        String name2 = tup2.getFieldName(this.pred.getField2());
        return name2;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.ch1.open();
        this.ch2.open();
        super.open();
        loadMap();
    }

    public void close() {
        super.close();
        this.ch1.close();
        this.ch2.close();
        this.listIt = null;
        this.tup1 = null;
        this.tup2 = null;
        this.hMap.clear();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.ch1.rewind();
        this.ch2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    private boolean loadMap() throws DbException, TransactionAbortedException{
        int num = 0;
        hMap.clear();
        while (ch1.hasNext() == true){
            tup1 = ch1.next();
            ArrayList<Tuple> lst = hMap.get(tup1.getField(pred.getField1()));
            if (lst == null){
                lst = new ArrayList<Tuple>();
                hMap.put(tup1.getField(pred.getField1()), lst);
            }
            lst.add(tup1);
            if (num++ == 20000){
                return true;
            }
        }
        boolean res = num > 0;
        return res;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    transient Iterator<Tuple> iter = null;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // option 1
        if(iter!= null && iter.hasNext()){
            int fields1, fields2;
            this.tup1 = iter.next();
            fields1 = tup1.getTupleDesc().numFields();
            fields2 = tup2.getTupleDesc().numFields();

            // combine the tuples
            Tuple newTup = new Tuple(joinedTup);
            // insert tup1 fields
            for(int i = 0; i<fields1; i++){
                newTup.setField(i, tup1.getField(i));
            }
            // insert tup2 fields -- include the offset for newTup
            int offset = fields1;
            for(int j = 0; j<fields2; j++){
                newTup.setField(offset+j, tup2.getField(j));
            }
            return newTup;
        }

        // option 2 -- loop ch2
        while(ch2.hasNext()){
            tup2 = ch2.next();
            // create newTup
            ArrayList<Tuple> twoTup = hMap.get(tup2.getField(pred.getField2()));
            // fill twoTup with the values from both tups
            if(twoTup == null){
            continue;
            }

            iter = twoTup.iterator();

            int fields1, fields2;
            this.tup1 = iter.next();
            fields1 = tup1.getTupleDesc().numFields();
            fields2 = tup2.getTupleDesc().numFields();

            // combine the tuples
            Tuple newTup = new Tuple(joinedTup);
            // insert tup1 fields
            for(int i = 0; i<fields1; i++){
                newTup.setField(i, tup1.getField(i));
            }
            // insert tup2 fields -- include the offset for newTup
            int offset = fields1;
            for(int j = 0; j<fields2; j++){
                newTup.setField(offset+j, tup2.getField(j));
            }
            return newTup;
        }

        // done with ch2
        ch2.rewind();
        if(loadMap()){
            return fetchNext();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.ch1, this.ch2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.ch1 = children[0];
        this.ch2 = children[1];
    }

}
