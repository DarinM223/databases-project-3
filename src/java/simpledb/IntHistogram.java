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
        //fill the map with zeros initially
        for (int i = 1; i <= buckets; i++) {
                store.put(i, 0);
        }
        this.width = 1 + (int)(this.max - this.min) / this.buckets;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        this.ntups++;
        int index = 1 + (v - this.min)/width; 
        //increment the height of the bucket that hashes to the value
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
        switch (op) {
                case EQUALS:
                        retVal = eqSelectivity(v);
                        break;
                case GREATER_THAN:
                        retVal = grSelectivity(v);
                        break;
                case GREATER_THAN_OR_EQ:
                        // a >= b is equivalent to !(a < b)
                        retVal = 1 - leSelectivity(v);
                        break;
                case LESS_THAN:
                        retVal = leSelectivity(v);
                        break;
                case LESS_THAN_OR_EQ:
                        // a <= b is equivalent to !(a > b)
                        retVal = 1 - grSelectivity(v);
                        break;
                case NOT_EQUALS:
                        // a != b is equivalent to !(a == b)
                        retVal = 1 - eqSelectivity(v);
                        break;
                default:
                        break;
        }
        return retVal;
    }
    
    public double eqSelectivity(int v) {
            int index = (int)((v - this.min)/this.width) + 1;
            if (v > this.max || v < this.min) return 0.0;
            //return (h / w) / ntups
            return ((double)store.get(index))/ this.width / this.ntups;
    }

    public double leSelectivity(int v) {
            if (v < min) { return 0.0; }
            if (v > max) { return 1.0; }
            int index = (int)((v - this.min) / this.width) + 1;
            double totalSelectivity = 0;
            for (int i = 1; i < index; i++) {
                    totalSelectivity += store.get(i);
            }
            //the left of bucket b is the right of bucket (b - 1) plus one
            double b_left = ((index - 1) * this.width + this.min);
            // b_part is (const - b_left) / w_b
            double b_part = (v - b_left) / this.width;
            // b contributes b_f * b_part which is b_part * h_b / ntups
            totalSelectivity += b_part * store.get(index);
            //divide all of the added heights by ntups
            return totalSelectivity/this.ntups;
    }

    public double grSelectivity(int v) {
            if (v < min) { return 1.0; }
            if (v > max) { return 0.0; }
            int index = (int)((v - this.min) / this.width) + 1;
            double totalSelectivity = 0;
            for (int i = index+1; i <= this.buckets; i++) {
                    totalSelectivity += store.get(i);
            }
            double b_right = index * this.width + this.min - 1;
            // b_part is (b_right - const) / w_b
            double b_part = (double)(b_right - v) / this.width;
            // b contributes b_f * b_part which is b_part * h_b / ntups
            totalSelectivity += b_part * store.get(index);
            //divide all of the added heights by ntups
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
        //return 1.0;

        //Expected selectivity: (h_1 * h_1 + h_2 * h_2 + ... + h_n * h_n) / (ntups * ntups)
        
        double totalSelectivity = 0;
        // add heights of each bucket squared (h_1 * h_1 + h_2 * h_2 + ... + h_n * h_n)
        for (int i = 1; i <= buckets; i++) {
                totalSelectivity += (store.get(i) * store.get(i));
        }
        //divide the total by ntups squared (ntups * ntups)
        return totalSelectivity/this.ntups/this.ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
