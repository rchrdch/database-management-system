package simpledb;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // initializing fields
    private int gbField;
    private Type gbFieldType;
    private int aggrField;
    private Op aggrOp;
    private ArrayList<Tuple> tups;
    private HashMap<Field, Integer> aggregate;
    private boolean noGroupBool;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (this.gbField == Aggregator.NO_GROUPING){
            this.noGroupBool = true;
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggrField = afield;
        this.aggrOp = what;
        this.aggregate = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key;
        int val, currAggVal, currCount;
        String fName = tup.getTupleDesc().getFieldName(aggrField);

        if (!noGroupBool){
            key = tup.getField(gbField);
            String gfName = tup.getTupleDesc().getFieldName(gbField);
        }
        else {
            key = new IntField(Aggregator.NO_GROUPING);
        }

        if (aggregate.containsKey(key) == false) {
            aggregate.put(key, 0);
        }

        currAggVal = aggregate.get(key);
        currAggVal++;
        aggregate.put(key, currAggVal);
    }

    // helper for iterator()
    public TupleDesc getTupleDesc(){
        Type[] typeArray;
        String[] stringArray;
        TupleDesc tupD;

        if (noGroupBool){
            typeArray = new Type[-1];
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tups = new ArrayList<Tuple>();
        TupleDesc tupD = this.getTupleDesc();

        for (Field key : aggregate.keySet()){
            int val = aggregate.get(key);

            if (noGroupBool == true){
                Tuple tup = new Tuple(tupD);
                tup.setField(0, new IntField(val));
                tups.add(tup);
            }
            else{
                Tuple tup = new Tuple(tupD);
                tup.setField(0, key);
                tup.setField(1, new IntField(val));
                tups.add(tup);
            }
        }
        return new TupleIterator(tupD, tups);
    }

}
