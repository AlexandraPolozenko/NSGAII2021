package copypaste;

import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.util.ObjectiveComparator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;

import static ru.ifmo.nds.util.Utils.lexCompare;

public class SortedObjectives<T extends IIndividual<P>, P> {
    private final int dim;

    @Nonnull
    private final List<double[]> coordSorted;

    @Nonnull
    private final List<int[]> coordCorrespIndex;

    @Nonnull
    private final List<T> lexSortedPop;

    private SortedObjectives(final int dim,
                             @Nonnull final List<double[]> coordSorted,
                             @Nonnull final List<int[]> coordCorrespIndex,
                             @Nonnull final List<T> lexSortedPop) {
        this.dim = dim;
        this.coordSorted = coordSorted;
        this.coordCorrespIndex = coordCorrespIndex;
        this.lexSortedPop = lexSortedPop;
    }

    public static <T1 extends IIndividual<P1>, P1> SortedObjectives<T1, P1> empty(int dim) {
        return new SortedObjectives<>(dim, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public static <T1 extends IIndividual<P1>, P1> SortedObjectives<T1, P1> create(final int dim,
                                                                                   @Nonnull final List<T1> pop,
                                                                                   @Nonnull final BiFunction<T1, Double, T1> cdUpdater) {
        final SortedObjectives<T1, P1> empty = empty(dim);
        return empty.update(pop, Collections.emptyList(), cdUpdater);
    }

    private void merge(@Nonnull final List<T> pop,
                       @Nonnull final int[] ind,
                       @Nonnull final List<T> l,
                       @Nonnull final int[] il,
                       @Nonnull final List<T> r,
                       @Nonnull final int[] ir,
                       @Nonnull final Comparator<? super T> comparator) {

        int i = 0, j = 0, k = 0;
        while (i < l.size() && j < r.size()) {
            if (comparator.compare(l.get(i), r.get(j)) < 0) {
                pop.set(k, l.get(i));
                ind[k++] = il[i++];
            } else {
                pop.set(k, r.get(j));
                ind[k++] = ir[j++];
            }
        }
        while (i < l.size()) {
            pop.set(k, l.get(i));
            ind[k++] = il[i++];
        }
        while (j < r.size()) {
            pop.set(k, r.get(j));
            ind[k++] = ir[j++];
        }
    }

    private void syncMergeSort(@Nonnull final List<T> pop,
                               @Nonnull final int[] ind,
                               @Nonnull final Comparator<? super T> comparator) {
        final int n = pop.size();
        assert pop.size() == ind.length;
        if (n < 2) {
            return;
        }
        final int mid = n / 2;
        final List<T> l = new ArrayList<>(mid);
        final List<T> r = new ArrayList<>(n - mid);
        int[] li = new int[mid];
        int[] ri = new int[n - mid];

        for (int i = 0; i < mid; i++) {
            l.add(pop.get(i));
            li[i] = ind[i];
        }
        for (int i = mid; i < n; i++) {
            r.add(pop.get(i));
            ri[i - mid] = ind[i];
        }
        syncMergeSort(l, li, comparator);
        syncMergeSort(r, ri, comparator);

        merge(pop, ind, l, li, r, ri, comparator);
    }


    //ToAdd and ToRemove are lex. sorted
    public SortedObjectives<T, P> update(@Nonnull final List<T> toAdd,
                                         @Nonnull final List<T> toRemove,
                                         @Nonnull final BiFunction<T, Double, T> cdUpdater) {

        final int targetSize = lexSortedPop.size() + toAdd.size() - toRemove.size();
        final List<T> newLexSortedPop = new ArrayList<>(targetSize);
        int iPop = 0;
        int iAdd = 0;
        int iRem = 0;
        final int[] removedIndices = new int[toRemove.size()];
        final int[] addendIndices = new int[toAdd.size()];
        while (newLexSortedPop.size() < targetSize) {
            if (iRem < toRemove.size() && iPop < lexSortedPop.size() &&
                    lexSortedPop.get(iPop).equals(toRemove.get(iRem))) {
                removedIndices[iRem++] = iPop++;
            } else if (iPop >= lexSortedPop.size()) {
                newLexSortedPop.add(toAdd.get(iAdd));
                addendIndices[iAdd++] = newLexSortedPop.size() - 1;
            } else if (iAdd >= toAdd.size()) {
                newLexSortedPop.add(lexSortedPop.get(iPop++));
            } else {
                final T p = lexSortedPop.get(iPop);
                final T a = toAdd.get(iAdd);
                if (lexCompare(p.getObjectives(), a.getObjectives(), a.getObjectives().length) <= 0) {
                    newLexSortedPop.add(p);
                    iPop++;
                } else {
                    newLexSortedPop.add(a);
                    addendIndices[iAdd++] = newLexSortedPop.size() - 1;
                }
            }
        }

        while (iRem < toRemove.size() && iPop < lexSortedPop.size() &&
                lexSortedPop.get(iPop).equals(toRemove.get(iRem))) {
            removedIndices[iRem++] = iPop++;
        }

        final int[] indexCorrector = new int[lexSortedPop.size()];
        iPop = 0;
        iAdd = 0;
        iRem = 0;
        while (iPop < lexSortedPop.size()) {
            if (iRem < removedIndices.length && removedIndices[iRem] == iPop) {
                ++iRem;
                //++iPop;
                indexCorrector[iPop++] = Integer.MIN_VALUE; //FIXME: shitty magic
            } else if (iAdd < addendIndices.length && addendIndices[iAdd] <= iAdd + iPop - iRem) {
                ++iAdd;
            } else {
                indexCorrector[iPop++] = iAdd - iRem;
            }
        }

        final List<double[]> newCoordSorted = new ArrayList<>();
        final List<int[]> newCorrespIndex = new ArrayList<>();
        for (int obj = 0; obj < dim; ++obj) {
            final double[] oldCoord = coordSorted.isEmpty() ? new double[0] : coordSorted.get(obj);
            final int[] oldIndex = coordCorrespIndex.isEmpty() ? new int[0] : coordCorrespIndex.get(obj);

            final double[] newCoord = new double[targetSize];
            final int[] newIndex = new int[targetSize];

            newCoordSorted.add(newCoord);
            newCorrespIndex.add(newIndex);

            final ObjectiveComparator comparator = new ObjectiveComparator(obj);
            syncMergeSort(toAdd, addendIndices, comparator);

            int cAddends = 0;
            int cOldSorted = 0;
            int cNew = 0;
            while (cNew < targetSize) {
                if (cOldSorted >= oldCoord.length) {
                    newCoord[cNew] = toAdd.get(cAddends).getObjectives()[obj];
                    newIndex[cNew++] = addendIndices[cAddends++];
                } else if (cAddends >= toAdd.size()) {
                    if (indexCorrector[oldIndex[cOldSorted]] == Integer.MIN_VALUE) {
                        cOldSorted++;
                    } else {
                        newCoord[cNew] = oldCoord[cOldSorted];
                        newIndex[cNew++] = oldIndex[cOldSorted] + indexCorrector[oldIndex[cOldSorted++]];
                    }
                } else if (toAdd.get(cAddends).getObjectives()[obj] <= oldCoord[cOldSorted]) {
                    newCoord[cNew] = toAdd.get(cAddends).getObjectives()[obj];
                    newIndex[cNew++] = addendIndices[cAddends++];
                } else {
                    if (indexCorrector[oldIndex[cOldSorted]] == Integer.MIN_VALUE) {
                        cOldSorted++;
                    } else {
                        newCoord[cNew] = oldCoord[cOldSorted];
                        newIndex[cNew++] = oldIndex[cOldSorted] + indexCorrector[oldIndex[cOldSorted++]];
                    }
                }
            }
        }

        final List<T> rs = calculateCD(cdUpdater, targetSize, newLexSortedPop, newCoordSorted, newCorrespIndex);

        return new SortedObjectives<>(dim, newCoordSorted, newCorrespIndex, rs);
    }

    private List<T> calculateCD(@Nonnull final BiFunction<T, Double, T> cdUpdater,
                                final int targetSize,
                                @Nonnull final List<T> newLexSortedPop,
                                @Nonnull final List<double[]> newCoordSorted,
                                @Nonnull final List<int[]> newCorrespIndex) {
        final double[] cd = new double[targetSize];
        for (int obj = 0; obj < dim; ++obj) {
            final double[] coord = newCoordSorted.get(obj);
            final int[] index = newCorrespIndex.get(obj);

            cd[index[0]] = Double.POSITIVE_INFINITY;
            cd[index[index.length - 1]] = Double.POSITIVE_INFINITY;

            final double inverseDelta = 1 / (coord[coord.length - 1] - coord[0]);
            for (int j = 1; j < targetSize - 1; j++) {
                cd[index[j]] += (coord[j + 1] - coord[j - 1]) * inverseDelta;
            }
        }

        final List<T> rs = new ArrayList<>(targetSize);
        for (int i = 0; i < targetSize; ++i) {
            rs.add(cdUpdater.apply(newLexSortedPop.get(i), cd[i]));
        }
        return rs;
    }

    @Nonnull
    public List<T> getLexSortedPop() {
        return Collections.unmodifiableList(lexSortedPop);
    }
}
