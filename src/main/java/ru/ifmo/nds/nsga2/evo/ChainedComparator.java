package ru.ifmo.nds.nsga2.evo;

import ru.ifmo.nds.IIndividual;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ChainedComparator<T extends IIndividual> implements Comparator<T> {
    private final List<Comparator<T>> comparators;

    @SafeVarargs
    public ChainedComparator(Comparator<T>... comparators) {
        this.comparators = Arrays.asList(comparators);
    }

    @Override
    public int compare(T solution1, T solution2) {
        for (Comparator<T> comparator : comparators) {
            int flag = comparator.compare(solution1, solution2);
            if (flag != 0) {
                return flag;
            }
        }

        return 0;
    }
}