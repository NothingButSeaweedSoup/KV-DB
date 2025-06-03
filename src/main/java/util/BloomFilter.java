package util;

import java.util.BitSet;

public class BloomFilter {
    private final int hashCount;
    private final BitSet bits;
    private final int size;

    public BloomFilter(int expectedElements, double falsePositiveRate) {
        this.size = calculateOptimalSize(expectedElements, falsePositiveRate);
        this.hashCount = calculateOptimalHashCount(expectedElements, size);
        this.bits = new BitSet(size);
    }

    private int calculateOptimalSize(int expectedElements, double falsePositiveRate) {
        return (int) (-expectedElements * Math.log(falsePositiveRate) / Math.log(2) / Math.log(2));
    }

    private int calculateOptimalHashCount(int expectedElements, double falsePositiveRate) {
        return (int) (Math.ceil((falsePositiveRate / (double) expectedElements) * Math.log(2)));
    }

    public boolean mightMatch(byte[] element){
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(element, i);
            if (!bits.get(Math.abs(hash % size))) {
                return false;
            }
        }
        return true;
    }

    public void add(byte[] element) {
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(element, i);
            bits.set(Math.abs(hash % size));
        }
    }

    private int hash(byte[] element, int seed) {
        int hash = seed;
        for (byte b : element) {
            hash = hash * 31 + (b & 0xff);
        }
        return hash;
    }

    public void clear() {
        bits.clear();
    }

    public int getSize() {
        return size;
    }

    public int getHashCount() {
        return hashCount;
    }
}
