package simpledb;

import java.util.Map;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    //count of aggregate (for COUNT)
    private Map<Object, Integer> aggregatesCount;
    //check for first tuple to extract field name and type
    private boolean firstTuple = true;

    //stores the field result from a no grouping
    private IntField no_grouping;
    //stores the count from a no grouping
    private int count = 0;

    private TupleDesc tupleDesc = null;

    private String gbfield_name = null;
    private String afield_name = null;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.no_grouping = null;

        //count operation only supported
        if (!what.toString().equals("count")) {
                throw new IllegalArgumentException("Invalid Operator! Only count supported");
        }
        aggregatesCount = new HashMap<Object, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        
        Object groupByField = null;
        count++;

        //get field type and name for group by and field name for aggregate
        if (firstTuple) {
                if (gbfield != NO_GROUPING) {
                        gbfieldtype = tup.getField(gbfield).getType();
                        gbfield_name = tup.getTupleDesc().getFieldName(gbfield);
                }
                afield_name = tup.getTupleDesc().getFieldName(afield);
                firstTuple = false;
        }

        if (gbfield != NO_GROUPING) { 
                if (gbfieldtype == Type.INT_TYPE) {
                        groupByField = new Integer(((IntField)tup.getField(gbfield)).getValue());
                } else if (gbfieldtype == Type.STRING_TYPE) {
                        groupByField = ((StringField)tup.getField(gbfield)).getValue();
                }
                switch (what) {
                        case COUNT: {
                                //get current count and put count+1
                                Integer aggregate_count = aggregatesCount.get(groupByField);
                                aggregatesCount.put(groupByField, (aggregate_count != null ? aggregate_count + 1 : 1));
                                break;
                        }
                }
        } else { //no grouping
                no_grouping = new IntField(count); //create a new integer field with the count
                return;
        }
            
    }

   //creates list of tuples that store the results of the specified operator (COUNT in this case)
   private ArrayList<Tuple> createTupleList() {
           ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
           String aggregate_name = what + " (" + afield_name + ")";
           if (gbfield == Aggregator.NO_GROUPING) { //no grouping
                   //add the no_grouping tuple to the tupleList only
                   TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE }, new String[] { aggregate_name });
                   this.tupleDesc = td;
                   Tuple newtuple = new Tuple(td);
                   newtuple.setField(0, no_grouping);
                   tupleList.add(newtuple);
           } else {
                   TupleDesc td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, 
                                   new String[] {gbfield_name, aggregate_name});
                   this.tupleDesc = td;

                   //loop through the aggregates and add the result tuples into the tupleList
                   for (Map.Entry<Object, Integer> entry : aggregatesCount.entrySet()) {
                           Tuple tuple = new Tuple(tupleDesc);
                           int aggVal = 0;
                           switch (what) {
                                   case COUNT: {
                                           aggVal = entry.getValue();
                                           break;
                                   }
                           }
                           if (gbfieldtype == Type.INT_TYPE) {
                                   tuple.setField(0, new IntField((Integer) entry.getKey()));
                           } else if (gbfieldtype == Type.STRING_TYPE) {
                                   tuple.setField(0, new StringField((String) entry.getKey(), Type.STRING_TYPE.getLen()));
                           }
                           tuple.setField(1, new IntField(aggVal));
                           tupleList.add(tuple);
                   }
           }
           return tupleList;
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
        List<Tuple> l = createTupleList();
        return new TupleIterator(this.tupleDesc, l);
    }
}
