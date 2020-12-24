package simpledb;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // initialize fields
    private int gbField;
    private Type gbFieldType;
    private int aggrField;
    private Op aggrOp;
    private HashMap<Field, Integer> aggregate;
    private HashMap<Field, Integer> count;
    private boolean noGroupBool;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (this.gbField == Aggregator.NO_GROUPING){
            this.noGroupBool = true;
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggrField = afield;
        this.aggrOp = what;
        this.aggregate = new HashMap<Field, Integer>();
        this.count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key;
        int val, currAggVal, currCount;
        String fName = tup.getTupleDesc().getFieldName(aggrField);

        // grouping vs no group
        if(noGroupBool){ // not grouping
            //System.out.println("gbField is " + gbField);
            key = new IntField(Aggregator.NO_GROUPING);
        } else{ // grouping
            key = tup.getField(gbField);
            String gfName = tup.getTupleDesc().getFieldName(gbField);
        }

        val = ((IntField) tup.getField(aggrField)).getValue();

        // finds the current currAggVal
        if(count.containsKey(key) == false){
            if (aggrOp == Op.MAX) {
                aggregate.put(key, -99999);
                count.put(key, 0);
            }
            if(aggrOp == Op.MIN){
                aggregate.put(key, 99999);
                count.put(key, 0);
            }
            if(aggrOp == Op.COUNT || aggrOp == Op.AVG || aggrOp == Op.SUM){
                aggregate.put(key, 0);
                count.put(key, 0);
            }
        }

        currAggVal = aggregate.get(key);
        currCount = count.get(key);

        // different operator options...
        if(aggrOp == Op.MAX && val > currAggVal){
            currAggVal = val;
            aggregate.put(key, currAggVal);
        }
        if(aggrOp == Op.MIN && val < currAggVal){
            currAggVal = val;
            aggregate.put(key, currAggVal);
        }
        if(aggrOp == Op.COUNT){
            currAggVal++;
            aggregate.put(key, currAggVal);
        }
        if(aggrOp == Op.AVG){
            currCount++;
            count.put(key, currCount);
            currAggVal+=val;
            aggregate.put(key, currAggVal);
        }
        if(aggrOp == Op.SUM){
            currAggVal+= val;
            aggregate.put(key, currAggVal);
        }
    }

    // helper method for iterator()
    public TupleDesc getTupleDesc(){
        Type[] typeArray;
        String[] stringArray;
        TupleDesc tupD;

        if (noGroupBool){
            typeArray = new Type[1];
            stringArray = new String[1];
            typeArray[0] = Type.INT_TYPE;
            stringArray[0] = "";
        }
        else{
            typeArray = new Type[2];
            stringArray = new String[2];
            typeArray[0] = gbFieldType;
            typeArray[1] = Type.INT_TYPE;
            stringArray[0] = "";
            stringArray[1] = "";
        }
        return tupD = new TupleDesc(typeArray, stringArray);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator(){
        ArrayList<Tuple> tups = new ArrayList<Tuple>();
        TupleDesc tupD = this.getTupleDesc();

        for (Field key : aggregate.keySet()){
            int val2 = aggregate.get(key);
            if (noGroupBool == true){
                if (aggrOp == Op.AVG){
                    val2 = (val2/count.get(key));
                }
                Tuple tup = new Tuple(tupD);
                tup.setField(0, new IntField(val2));
                tups.add(tup);
            }
            else{
                if (aggrOp == Op.AVG){
                    val2 = (val2/count.get(key));
                }
                Tuple tup = new Tuple(tupD);
                tup.setField(0, key);
                tup.setField(1, new IntField(val2));
                tups.add(tup);
            }
        }

        return new TupleIterator(tupD, tups);
    }

}
