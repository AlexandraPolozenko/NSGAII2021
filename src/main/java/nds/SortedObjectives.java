package nds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.util.ObjectiveComparator;
import ru.ifmo.nds.util.Utils;

public class SortedObjectives<T extends IIndividual<P>, P> {
    private final int dim;
    @Nonnull
    private final List<double[]> coordSorted;
    @Nonnull
    private final List<int[]> coordCorrespIndex;
    @Nonnull
    private final List<T> lexSortedPop;

    private SortedObjectives(int dim, @Nonnull List<double[]> coordSorted, @Nonnull List<int[]> coordCorrespIndex, @Nonnull List<T> lexSortedPop) {
        this.dim = dim;
        this.coordSorted = coordSorted;
        this.coordCorrespIndex = coordCorrespIndex;
        this.lexSortedPop = lexSortedPop;
    }

    public static <T1 extends IIndividual<P1>, P1> SortedObjectives<T1, P1> empty(int dim) {
        return new SortedObjectives(dim, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public static <T1 extends IIndividual<P1>, P1> SortedObjectives<T1, P1> create(int dim, @Nonnull List<T1> pop, @Nonnull BiFunction<T1, Double, T1> cdUpdater) {
        SortedObjectives<T1, P1> empty = empty(dim);
        return empty.update(pop, Collections.emptyList(), cdUpdater);
    }

    private void merge(@Nonnull List<T> pop, @Nonnull int[] ind, @Nonnull List<T> l, @Nonnull int[] il, @Nonnull List<T> r, @Nonnull int[] ir, @Nonnull Comparator<? super T> comparator) {
        int i = 0;
        int j = 0;
        int k = 0;

        while(i < l.size() && j < r.size()) {
            if (comparator.compare(l.get(i), r.get(j)) < 0) {
                pop.set(k, l.get(i));
                ind[k++] = il[i++];
            } else {
                pop.set(k, r.get(j));
                ind[k++] = ir[j++];
            }
        }

        while(i < l.size()) {
            pop.set(k, l.get(i));
            ind[k++] = il[i++];
        }

        while(j < r.size()) {
            pop.set(k, r.get(j));
            ind[k++] = ir[j++];
        }

    }

    private void syncMergeSort(@Nonnull List<T> pop, @Nonnull int[] ind, @Nonnull Comparator<? super T> comparator) {
        int n = pop.size();

        assert pop.size() == ind.length;

        if (n >= 2) {
            int mid = n / 2;
            List<T> l = new ArrayList(mid);
            List<T> r = new ArrayList(n - mid);
            int[] li = new int[mid];
            int[] ri = new int[n - mid];

            int i;
            for(i = 0; i < mid; ++i) {
                l.add(pop.get(i));
                li[i] = ind[i];
            }

            for(i = mid; i < n; ++i) {
                r.add(pop.get(i));
                ri[i - mid] = ind[i];
            }

            this.syncMergeSort(l, li, comparator);
            this.syncMergeSort(r, ri, comparator);
            this.merge(pop, ind, l, li, r, ri, comparator);
        }
    }

    public SortedObjectives<T, P> update(@Nonnull List<T> toAdd, @Nonnull List<T> toRemove, @Nonnull BiFunction<T, Double, T> cdUpdater) {
        int targetSize = this.lexSortedPop.size() + toAdd.size() - toRemove.size();
        List<T> newLexSortedPop = new ArrayList(targetSize);
        int iPop = 0;
        int iAdd = 0;
        int iRem = 0;
        int[] removedIndices = new int[toRemove.size()];
        int[] addendIndices = new int[toAdd.size()];

        while(true) {
            while(newLexSortedPop.size() < targetSize) {
                if (iRem < toRemove.size() && iPop < this.lexSortedPop.size() && ((IIndividual)this.lexSortedPop.get(iPop)).equals(toRemove.get(iRem))) {
                    removedIndices[iRem++] = iPop++;
                } else if (iPop >= this.lexSortedPop.size()) {
                    newLexSortedPop.add(toAdd.get(iAdd));
                    addendIndices[iAdd++] = newLexSortedPop.size() - 1;
                } else if (iAdd >= toAdd.size()) {
                    newLexSortedPop.add(this.lexSortedPop.get(iPop++));
                } else {
                    T p = this.lexSortedPop.get(iPop);
                    T a = toAdd.get(iAdd);
                    if (Utils.lexCompare(p.getObjectives(), a.getObjectives(), a.getObjectives().length) <= 0) {
                        newLexSortedPop.add(p);
                        ++iPop;
                    } else {
                        newLexSortedPop.add(a);
                        addendIndices[iAdd++] = newLexSortedPop.size() - 1;
                    }
                }
            }

            while(iRem < toRemove.size() && iPop < this.lexSortedPop.size() && ((IIndividual)this.lexSortedPop.get(iPop)).equals(toRemove.get(iRem))) {
                removedIndices[iRem++] = iPop++;
            }

            int[] indexCorrector = new int[this.lexSortedPop.size()];
            iPop = 0;
            iAdd = 0;
            iRem = 0;

            while(true) {
                while(iPop < this.lexSortedPop.size()) {
                    if (iRem < removedIndices.length && removedIndices[iRem] == iPop) {
                        ++iRem;
                        indexCorrector[iPop++] = -2147483648;
                    } else if (iAdd < addendIndices.length && addendIndices[iAdd] <= iAdd + iPop - iRem) {
                        ++iAdd;
                    } else {
                        indexCorrector[iPop++] = iAdd - iRem;
                    }
                }

                List<double[]> newCoordSorted = new ArrayList();
                List<int[]> newCorrespIndex = new ArrayList();

                for(int obj = 0; obj < this.dim; ++obj) {
                    double[] oldCoord = this.coordSorted.isEmpty() ? new double[0] : (double[])this.coordSorted.get(obj);
                    int[] oldIndex = this.coordCorrespIndex.isEmpty() ? new int[0] : (int[])this.coordCorrespIndex.get(obj);
                    double[] newCoord = new double[targetSize];
                    int[] newIndex = new int[targetSize];
                    newCoordSorted.add(newCoord);
                    newCorrespIndex.add(newIndex);
                    ObjectiveComparator comparator = new ObjectiveComparator(obj);
                    this.syncMergeSort(toAdd, addendIndices, comparator);
                    int cAddends = 0;
                    int cOldSorted = 0;
                    int cNew = 0;

                    while(cNew < targetSize) {
                        if (cOldSorted >= oldCoord.length) {
                            newCoord[cNew] = ((IIndividual)toAdd.get(cAddends)).getObjectives()[obj];
                            newIndex[cNew++] = addendIndices[cAddends++];
                        } else if (cAddends >= toAdd.size()) {
                            if (indexCorrector[oldIndex[cOldSorted]] == -2147483648) {
                                ++cOldSorted;
                            } else {
                                newCoord[cNew] = oldCoord[cOldSorted];
                                newIndex[cNew++] = oldIndex[cOldSorted] + indexCorrector[oldIndex[cOldSorted++]];
                            }
                        } else if (((IIndividual)toAdd.get(cAddends)).getObjectives()[obj] <= oldCoord[cOldSorted]) {
                            newCoord[cNew] = ((IIndividual)toAdd.get(cAddends)).getObjectives()[obj];
                            newIndex[cNew++] = addendIndices[cAddends++];
                        } else if (indexCorrector[oldIndex[cOldSorted]] == -2147483648) {
                            ++cOldSorted;
                        } else {
                            newCoord[cNew] = oldCoord[cOldSorted];
                            newIndex[cNew++] = oldIndex[cOldSorted] + indexCorrector[oldIndex[cOldSorted++]];
                        }
                    }
                }

                List<T> rs = this.calculateCD(cdUpdater, targetSize, newLexSortedPop, newCoordSorted, newCorrespIndex);
                return new SortedObjectives(this.dim, newCoordSorted, newCorrespIndex, rs);
            }
        }
    }

    private List<T> calculateCD(@Nonnull BiFunction<T, Double, T> cdUpdater, int targetSize, @Nonnull List<T> newLexSortedPop, @Nonnull List<double[]> newCoordSorted, @Nonnull List<int[]> newCorrespIndex) {
        double[] cd = new double[targetSize];

        for(int obj = 0; obj < this.dim; ++obj) {
            double[] coord = (double[])newCoordSorted.get(obj);
            int[] index = (int[])newCorrespIndex.get(obj);
            cd[index[0]] = 1.0D / 0.0;
            cd[index[index.length - 1]] = 1.0D / 0.0;
            double inverseDelta = 1.0D / (coord[coord.length - 1] - coord[0]);

            for(int j = 1; j < targetSize - 1; ++j) {
                cd[index[j]] += (coord[j + 1] - coord[j - 1]) * inverseDelta;
            }
        }

        List<T> rs = new ArrayList(targetSize);

        for(int i = 0; i < targetSize; ++i) {
            rs.add(cdUpdater.apply(newLexSortedPop.get(i), cd[i]));
        }

        return rs;
    }

    @Nonnull
    public List<T> getLexSortedPop() {
        return Collections.unmodifiableList(this.lexSortedPop);
    }
}

