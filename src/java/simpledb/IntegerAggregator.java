package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    //operator to apply
    private Op what;

    private Map<Object, Integer> aggregates;
    //count of aggregate (for AVG and COUNT)
    private Map<Object, Integer> aggregatesCount;

    //Field for no grouping
    private IntField no_grouping;
    //count for no grouping
    private int count = 0;

    private TupleDesc tupleDesc = null;
    //if its the first tuple, get type and name from it
    private boolean firstTuple = true;

    private String gbfield_name = null;
    private String afield_name = null;

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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.no_grouping = null;

        aggregates = new HashMap<Object, Integer>();
        aggregatesCount = new HashMap<Object, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        count++;
        // some code goes here
        Object groupByField = null;

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
                        groupByField = ((StringField) tup.getField(gbfield)).getValue();
                }

                int val = ((IntField) tup.getField(afield)).getValue();
                Integer aggregate_value = aggregates.get(groupByField);
                switch (what) {
                        case MIN: {
                                          if ((aggregate_value != null && val < aggregate_value) || aggregate_value == null) {
                                                  aggregates.put(groupByField, val);
                                          }
                                          break;
                        }
                        case MAX: {
                                          if ((aggregate_value != null && val > aggregate_value) || aggregate_value == null) {
                                                  aggregates.put(groupByField, val);
                                          }
                                          break;
                        }
                        case SUM: {
                                          aggregates.put(groupByField, (aggregate_value == null ? val : (val + aggregate_value)));
                                          break;
                        }
                        case AVG: {
                                          Integer aggregate_count = aggregatesCount.get(groupByField);
                                          //if the aggregate value and count are not null, add it
                                          if (aggregate_value != null && aggregate_count != null) {
                                                  aggregates.put(groupByField, aggregate_value + val);
                                                  aggregatesCount.put(groupByField, aggregate_count + 1);
                                          } else if (aggregate_value == null && aggregate_count == null) { //if they are both null add default  
                                                  aggregates.put(groupByField, val);
                                                  aggregatesCount.put(groupByField, 1);
                                          } else { //otherwise they are out of sync
                                                  throw new RuntimeException("Aggregate value and count are out of sync");
                                          }
                                          break;
                        }
                        case COUNT: {
                                          Integer aggregate_count = aggregatesCount.get(groupByField);
                                          Integer cnt = (aggregate_count != null ? aggregate_count + 1 : 1);
                                          aggregates.put(groupByField, cnt);
                                          aggregatesCount.put(groupByField, cnt);
                                          break;
                        }
                                
                }

        } else { // if there is no grouping
                IntField newval = (IntField)tup.getField(afield);
                if (no_grouping == null) {
                        no_grouping = newval;
                        return;
                }
                //apply operator between the no_grouping Field and the Tuple's field
                switch (what) {
                        case MIN: {
                                          no_grouping = (no_grouping.getValue() > newval.getValue()) ? newval : no_grouping;
                                          break;
                        }
                        case MAX: {
                                          no_grouping = (no_grouping.getValue() < newval.getValue()) ? newval : no_grouping;
                                          break;
                        }
                        case SUM: 
                        case AVG: {
                                          no_grouping = new IntField(no_grouping.getValue() + newval.getValue());
                                          break;
                        }
                        case COUNT: {
                                          no_grouping = new IntField(count);
                                          break;
                        }
                }

        }


    }

    //creates list of tuples that store the results of the specified operator
    private ArrayList<Tuple> createTupleList() {
            ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
            String aggregate_name = what + " (" + afield_name + ")";
            if (gbfield == Aggregator.NO_GROUPING) { //for no grouping add one tuple using the no_grouping field and count
                    TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {aggregate_name});
                    this.tupleDesc = td;
                    Tuple newtuple = new Tuple(td);
                    if (what == Aggregator.Op.AVG) {
                         newtuple.setField(0, new IntField(no_grouping.getValue() / count));
                    } else {
                         newtuple.setField(0, no_grouping);
                    }
                    tupleList.add(newtuple);
            } else { //for a grouping, iterate through aggregate list and add Tuples with the results to the list
                    TupleDesc td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE},
                                    new String[] {gbfield_name, aggregate_name});
                    this.tupleDesc = td;
                    for (Map.Entry<Object, Integer> entry : aggregates.entrySet()) {
                            Tuple tuple = new Tuple(tupleDesc);
                            int aggVal = 0;
                            switch (what) {
                                    case AVG: {
                                                     Integer _count = aggregatesCount.get(entry.getKey());
                                                     if (_count == null) { //for some reason count is null
                                                             throw new RuntimeException("What the FUUU");
                                                     }
                                                     aggVal = entry.getValue() / _count; 
                                                     break;
                                    }
                                    case COUNT: {
                                                     aggVal = aggregatesCount.get(entry.getKey());
                                                     break;
                                    }
                                    default: {
                                                     aggVal = entry.getValue();
                                                     break;
                                    }
                            }
                            if (gbfieldtype == Type.INT_TYPE) {
                                    tuple.setField(0, new IntField((Integer) entry.getKey()));
                            } else if (gbfieldtype == Type.STRING_TYPE) {
                                    tuple.setField(0, new StringField((String)entry.getKey(), Type.STRING_TYPE.getLen()));
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new
        //UnsupportedOperationException("please implement me for lab2");
        List<Tuple> l = createTupleList();
        return new TupleIterator(this.tupleDesc, l);
    }
}
