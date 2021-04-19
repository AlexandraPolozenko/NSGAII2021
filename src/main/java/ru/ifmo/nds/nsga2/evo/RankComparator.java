package ru.ifmo.nds.nsga2.evo;

import ru.ifmo.nds.impl.RankedIndividual;

import java.util.Comparator;

public class RankComparator implements Comparator<RankedIndividual> {
    @Override
    public int compare(RankedIndividual solution, RankedIndividual solution1) {
        return Integer.compare(solution.getRank(), solution1.getRank());
    }
}
