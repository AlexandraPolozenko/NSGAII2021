package copypaste;

import javafx.util.Pair;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.INonDominationLevel;
import ru.ifmo.nds.PopulationSnapshot;
import ru.ifmo.nds.impl.RankedIndividual;
import ru.ifmo.nds.util.AscLexSortComparator;
import ru.ifmo.nds.util.RankedPopulation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("unused")
public interface IManagedPopulation<T> extends Cloneable {
    /**
     * @param individual Individual to add
     * @return Added individual's rank (starting from zero)
     */
    int addIndividual(@Nonnull IIndividual<T> individual);

    /**
     * @return Some valid state of non-domination levels. The returned value is guaranteed to persist, no other thread
     * should alter it. Index in list equals to layer's rank.
     */
    @Nonnull
    PopulationSnapshot<T> getSnapshot();

    /**
     * @return Working set of non-domination levels. The returned value cannot be altered directly, but may be
     * modified by some other thread at any time. Index in list equals to layer's rank.
     */
    @Nonnull
    default List<? extends INonDominationLevel<T>> getLevelsUnsafe() {
        return getSnapshot().getLevels();
    }

    int size();

    IManagedPopulation<T> clone();

    Map<Integer, Pair<Long, Integer>> getLevelsTs();

    /**
     * @param count max. number of solutions to return
     * @return list with min(population size, count) random solutions
     */
    @Nonnull
    default List<RankedIndividual<T>> getRandomSolutions(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("Negative number of random solutions requested");
        }

        final PopulationSnapshot<T> popSnap = getSnapshot();

        final int actualCount = Math.min(count, popSnap.getSize());
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final TreeSet<Integer> indices = new TreeSet<>();
        while (indices.size() < actualCount) {
            indices.add(random.nextInt(0, popSnap.getSize()));
        }

        final List<RankedIndividual<T>> res = new ArrayList<>();
        final Iterator<Integer> it = indices.iterator();
        Integer next = getNext(it);
        int prevLevelsSizeSum = 0;
        int rank = 0;
        for (INonDominationLevel<T> level : popSnap.getLevels()) {
            final int levelSize = level.getMembers().size();
            while (next != null && next - prevLevelsSizeSum < levelSize) {
                final IIndividual<T> ind = level.getMembers().get(next - prevLevelsSizeSum);
                res.add(new RankedIndividual<>(ind.getObjectives(), ind.getCrowdingDistance(), rank, ind.getPayload()));
                next = getNext(it);
            }
            prevLevelsSizeSum += levelSize;
            rank++;
        }

        return res;
    }

    default Integer getNext(Iterator<Integer> it) {
        return it.hasNext() ? it.next() : null;
    }

    default int determineRank(IIndividual<T> point) {
        final List<? extends INonDominationLevel<T>> ndLayers = getSnapshot().getLevels();

        int l = 0;
        int r = ndLayers.size() - 1;
        int lastNonDominating = r + 1;
        while (l <= r) { //двоичный поиск, куда воткнуть точку
            final int test = (l + r) / 2;
            if (!ndLayers.get(test).dominatedByAnyPointOfThisLayer(point)) {
                lastNonDominating = test;
                r = test - 1;
            } else {
                l = test + 1;
            }
        }

        return lastNonDominating;
    }

    default RankedPopulation<IIndividual<T>> toRankedPopulation() {
        final PopulationSnapshot<T> popSnap = getSnapshot();

        final IIndividual[] pop;
        final int[] sortedRanks;
        if (popSnap.getSize() > 0) {
            //noinspection unchecked
            pop = new IIndividual[popSnap.getSize()];
            final int[] ranks = new int[popSnap.getSize()];
            int j = 0;
            for (int i = 0; i < popSnap.getLevels().size(); ++i) {
                for (IIndividual<T> d : popSnap.getLevels().get(i).getMembers()) {
                    pop[j] = d;
                    ranks[j] = i;
                    j++;
                }
            }

            sortedRanks = ru.ifmo.nds.util.RankedIndividual.sortRanksForLexSortedPopulation(ranks, pop, IIndividual::getObjectives);

            Arrays.sort(pop, AscLexSortComparator.getInstance());
        } else {
            pop = null;
            sortedRanks = null;
        }
        //noinspection unchecked
        return new RankedPopulation<>(pop, sortedRanks);
    }
}
