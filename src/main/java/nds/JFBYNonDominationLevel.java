package nds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.dcns.sorter.JFB2014;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.util.RankedPopulation;
import ru.ifmo.nds.util.Utils;

@ThreadSafe
@Immutable
public class JFBYNonDominationLevel<T> implements INonDominationLevel<T> {
    @Nonnull
    private final JFB2014 sorter;
    @Nonnull
    private final List<IIndividual<T>> members;
    @Nonnull
    private final SortedObjectives<IIndividual<T>, T> sortedObjectives;

    public JFBYNonDominationLevel(@Nonnull JFB2014 sorter, @Nonnull List<IIndividual<T>> members) {
        this.sorter = sorter;
        if (!members.isEmpty()) {
            int dim = ((IIndividual)members.get(0)).getObjectives().length;
            this.sortedObjectives = SortedObjectives.create(dim, members, (i, d) -> {
                return new FitnessAndCdIndividual(i.getObjectives(), d, i.getPayload());
            });
            this.members = this.sortedObjectives.getLexSortedPop();
        } else {
            this.members = Collections.emptyList();
            this.sortedObjectives = SortedObjectives.empty(0);
        }

    }

    public JFBYNonDominationLevel(@Nonnull JFB2014 sorter, @Nonnull List<IIndividual<T>> members, @Nonnull SortedObjectives<IIndividual<T>, T> sortedObjectives) {
        this.sorter = sorter;
        this.members = Collections.unmodifiableList(members);
        this.sortedObjectives = sortedObjectives;
    }

    @Nonnull
    public List<IIndividual<T>> getMembers() {
        return this.members;
    }

    @Nonnull
    public SortedObjectives<IIndividual<T>, T> getSortedObjectives() {
        return this.sortedObjectives;
    }

    public MemberAdditionResult<T, JFBYNonDominationLevel<T>> addMembers(@Nonnull List<IIndividual<T>> addends) {
        int[] ranks = new int[this.members.size()];
        RankedPopulation<IIndividual<T>> rp = this.sorter.addRankedMembers(this.members, ranks, addends, 0);
        ArrayList<IIndividual<T>> nextLevel = new ArrayList(ranks.length);

        for(int i = 0; i < ((IIndividual[])rp.getPop()).length; ++i) {
            if (rp.getRanks()[i] != 0) {
                nextLevel.add(((IIndividual[])rp.getPop())[i]);
            }
        }

        SortedObjectives<IIndividual<T>, T> nso = this.sortedObjectives.update(addends, nextLevel, (ix, d) -> {
            return new FitnessAndCdIndividual(ix.getObjectives(), d, ix.getPayload());
        });
        return new MemberAdditionResult(nextLevel, new JFBYNonDominationLevel(this.sorter, nso.getLexSortedPop(), nso));
    }

    public boolean dominatedByAnyPointOfThisLayer(@Nonnull IIndividual point) {
        double[] pointObj = point.getObjectives();
        Iterator var3 = this.members.iterator();

        while(var3.hasNext()) {
            IIndividual member = (IIndividual)var3.next();
            double[] memberObj = member.getObjectives();
            if (memberObj[0] > pointObj[0]) {
                break;
            }

            if (Utils.dominates(memberObj, pointObj, pointObj.length) < 0) {
                return true;
            }
        }

        return false;
    }

    public JFBYNonDominationLevel<T> copy() {
        List<IIndividual<T>> newMembers = new ArrayList(this.members.size());
        newMembers.addAll(this.members);
        return new JFBYNonDominationLevel(this.sorter, newMembers, this.sortedObjectives);
    }

    public String toString() {
        return "members=" + this.members.stream().map(IIndividual::getObjectives).map(Arrays::toString).collect(Collectors.toList());
    }
}
