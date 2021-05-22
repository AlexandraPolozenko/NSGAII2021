package copypaste.sorter;

import ru.ifmo.nds.IIndividual;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ru.ifmo.nds.util.Utils.dominates;

/**
 * Can perform NDS only on a sorted population with one new point (or a contiguous set of points with the same rank)
 */
@ThreadSafe
public class IncrementalJFB extends JFB2014 {
    @Override
    public int[] performNds(IIndividual[] population) {
        throw new UnsupportedOperationException("Can't perform non-incremental sorting");
    }

    @Override
    protected void sweepA(IIndividual[] pop, int[] ranks, List<Integer> workingSet) {
        final Map<Integer, Integer> rightmostStairs = new HashMap<>(); //From rank to index

        for (int index : workingSet) {
            final double[] popIndex = pop[index].getObjectives();
            if (rightmostStairs.containsKey(ranks[index])) {
                final int t = rightmostStairs.get(ranks[index]);
                if (dominates(pop[t].getObjectives(), popIndex, pop[t].getObjectives().length) < 0) {
                    ++ranks[index];
                }
            }

            final Integer t = rightmostStairs.get(ranks[index]);
            if (t == null || pop[t].getObjectives()[1] > popIndex[1])
                rightmostStairs.put(ranks[index], index);
        }
    }

    @Override
    boolean sweepB(IIndividual[] pop, int[] ranks, List<Integer> lSet, List<Integer> hSet) {
        if (lSet == null || lSet.isEmpty() || hSet == null || hSet.isEmpty())
            return false;

        final Map<Integer, Integer> rightmostStairs = new HashMap<>(); //From rank to index

        int lIndex = 0;
        boolean rankChanged = false;
        for (int h : hSet) {
            while (lIndex < lSet.size() && lSet.get(lIndex) < h) {
                final int l = lSet.get(lIndex);
                final double[] popL = pop[l].getObjectives();
                if (!rightmostStairs.containsKey(ranks[l])) {
                    rightmostStairs.put(ranks[l], l);
                } else {
                    final int t = rightmostStairs.get(ranks[l]);
                    if (pop[t].getObjectives()[1] >= popL[1])
                        rightmostStairs.put(ranks[l], l);
                }

                lIndex++;
            }

            if (rightmostStairs.containsKey(ranks[h])) {
                final int t = rightmostStairs.get(ranks[h]);
                if (dominates(pop[t].getObjectives(), pop[h].getObjectives(), pop[t].getObjectives().length) < 0) {
                    ++ranks[h];
                    rankChanged = true;
                }
            }
        }
        return rankChanged;
    }
}
