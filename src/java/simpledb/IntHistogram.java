package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    //private int store[];
    private Map<Integer, Integer> store;
    private int buckets;
    private int min;
    private int max;
    private int width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        store = new HashMap<Integer, Integer>();
        for (int i = 0; i < buckets; i++) {
                store.put(i, 0);
        }
        this.width = (int) Math.ceil((double) (this.max - this.min + 1) / this.buckets);
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        this.ntups++;
        int index = (int)(Math.floor((int)Math.abs(v - this.min)/width));
        store.put(index, store.get(index)+1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        //return -1.0;
        double retVal = 0.0;
        String opString;
        switch (op) {
                case EQUALS:
                        retVal = eqSelectivity(v);
                        opString = "EQUALS TO";
                        break;
                case GREATER_THAN:
                        retVal = grSelectivity(v);
                        opString = "GREATER THAN";
                        break;
                case GREATER_THAN_OR_EQ:
                        retVal = grEqSelectivity(v);
                        opString = "GREATER THAN OR EQUAL TO";
                        break;
                case LESS_THAN:
                        retVal = leSelectivity(v);
                        opString = "LESS THAN";
                        break;
                case LESS_THAN_OR_EQ:
                        retVal = leEqSelectivity(v);
                        opString = "LESS THAN OR EQUAL TO";
                        break;
                case NOT_EQUALS:
                        retVal = 1 - eqSelectivity(v);
                        opString = "NOT EQUAL TO";
                        break;
                default:
                        opString = "";
                        break;
        }
        //for debugging purposes:
        //System.out.println("Selectivity of " + opString + " " + v + " is " + retVal);
        return retVal;
    }
    
    public double eqSelectivity(int v) {
            int index = (int)((v - this.min)/this.width);
            if (v > this.max || v < this.min) return 0.0;
            return ((double)store.get(index))/this.ntups;
    }
    public double leSelectivity(int v) {
            if (v < min) { return 0.0; }
            if (v > max) { return 1.0; }
            int index = (int)(((double)v - 1 - this.min)/this.width);
            double totalSelectivity = 0;
            for (int i = index; i >= 0; i--) {
                    totalSelectivity += store.get(i);
            }
            return totalSelectivity/this.ntups;
    }
    public double leEqSelectivity(int v) {
            if (v < min) { return 0.0; }
            if (v > max) { return 1.0; }
            int index = (int)(((double)v - this.min)/this.width);
            double totalSelectivity = 0;
            for (int i = index; i >= 0; i--) {
                    totalSelectivity += store.get(i);
            }
            return totalSelectivity/this.ntups;
    }
    public double grSelectivity(int v) {
            if (v < min) { return 1.0; }
            if (v > max) { return 0.0; }
            int index = (int)(((double)v + 1 - this.min) / this.width);
            double totalSelectivity = 0;
            for (int i = index; i < this.buckets; i++) {
                    totalSelectivity += store.get(i);
            }
            return totalSelectivity/this.ntups;
    }
    public double grEqSelectivity(int v) {
            if (v < min) { return 1.0; }
            if (v > max) { return 0.0; }
            int index = (int)(((double)v - this.min) / this.width);
            double totalSelectivity = 0;

            for (int i = index; i < this.buckets; i++) {
                    totalSelectivity += store.get(i);
            }
            return totalSelectivity/this.ntups;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
