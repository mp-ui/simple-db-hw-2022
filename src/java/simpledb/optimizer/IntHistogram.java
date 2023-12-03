package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int buckets;
    private final int min;
    private final int max;
    private final int[] values;
    private final int bucketSize;
    private final int bucketSizeRemain;

    private int numTuples;
    private int currentMin;
    private int currentMax;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        // 左右两边额外开两个区间，values[0]存放小于min的数据数量，values[buckets+1]保存大于max的数据数量
        this.values = new int[buckets + 2];
        // 平均每个桶的区间大小，若有剩余则放在最后一个区间，即values[bucket]
        this.bucketSize = (max - min + 1) / buckets;
        this.bucketSizeRemain = (max - min + 1) % buckets;
        this.numTuples = 0;
        this.currentMin = min;
        this.currentMax = max;
    }

    /**
     * 获取包含该数字的桶下标
     */
    private int getBucketIndex(int v) {
        if (v < min) {
            return 0;
        }
        if (v > max) {
            return buckets + 1;
        }
        // 每个区间大小为0的场景：max - min + 1 < buckets，这时所有数据都堆在最后一个区间values[bucket]
        if (bucketSize == 0) {
            return buckets;
        }
        // 计算bucketSize时可能会有余数，计算的结果可能会大于buckets
        // 当计算结果大于buckets时应该取buckets
        return Math.min(1 + (v - min) / bucketSize, buckets);
    }

    /**
     * 根据桶的下标计算区间范围，左闭右开
     */
    private int[] getBucketRange(int index) {
        if (index < 0 || index > buckets + 1) {
            throw new IndexOutOfBoundsException("bucket index " + index + " out of range [0, " + (buckets + 1) + "].");
        }
        if (index == 0) {
            return new int[]{currentMin, min};
        }
        if (index == buckets + 1) {
            return new int[]{max + 1, currentMax + 1};
        }
        int left = min + (index - 1) * bucketSize;
        int right = left + bucketSize;
        // 最后一个区间values[bucket]包括剩余的大小
        if (index == buckets) {
            right += bucketSizeRemain;
        }
        return new int[]{left, right};
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucketIndex = this.getBucketIndex(v);
        this.values[bucketIndex]++;
        this.numTuples++;
        // 更新最小值和最大值
        this.currentMin = Math.min(this.currentMin, v);
        this.currentMax = Math.max(this.currentMax, v);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (this.numTuples <= 0) {
            return 0;
        }
        int index = this.getBucketIndex(v);
        int[] range = this.getBucketRange(index);
        int width = range[1] - range[0];
        if (op == Predicate.Op.EQUALS) {
            if (width > 0) {
                return (this.values[index] * 1.0 / width) / this.numTuples;
            }
            return 0;
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            int width1 = range[1] - v;
            // 命中区间所贡献的选择性
            double estimateNumTuples = 0;
            if (width > 0) {
                estimateNumTuples = (this.values[index] * 1.0 / width) * width1;
            }
            // 该区间右边所有区间贡献的选择性
            for (int i = index + 1; i <= this.buckets + 1; ++i) {
                estimateNumTuples += this.values[i];
            }
            return estimateNumTuples / this.numTuples;
        } else if (op == Predicate.Op.GREATER_THAN) {
            return this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v + 1);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            // 区间是左闭右开，v和range[0]都包含在区间内，因此计算宽度应加1
            int width1 = v - range[0] + 1;
            // 命中区间所贡献的选择性
            double estimateNumTuples = 0;
            if (width > 0) {
                estimateNumTuples = this.values[index] * 1.0 / width * width1;
            }
            // 该区间左边所有区间贡献的选择性
            for (int i = index - 1; i >= 0; --i) {
                estimateNumTuples += this.values[i];
            }
            return estimateNumTuples / this.numTuples;
        } else if (op == Predicate.Op.LESS_THAN) {
            return this.estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v - 1);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        if (this.numTuples <= 0) {
            return 0;
        }
        double nums = 0;
        for (int i = 0; i <= this.buckets + 1; ++i) {
            nums += this.values[i];
        }
        return nums / this.numTuples;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", values=" + Arrays.toString(values) +
                ", bucketSize=" + bucketSize +
                ", bucketSizeRemain=" + bucketSizeRemain +
                ", numTuples=" + numTuples +
                ", currentMin=" + currentMin +
                ", currentMax=" + currentMax +
                '}';
    }
}
