package copypaste.jfby;

import copypaste.INonDominationLevel;
import copypaste.sorter.JFB2014;
import org.moeaframework.problem.misc.Lis;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.util.RankedPopulation;
import ru.ifmo.nds.util.SortedObjectives;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static ru.ifmo.nds.util.Utils.dominates;

@ThreadSafe
@Immutable
public class JFBYNonDominationLevel<T> implements INonDominationLevel<T> {
    @Nonnull
    private final JFB2014 sorter;

    @Nonnull
    private final List<IIndividual<T>> membersLeft;
    @Nonnull
    private final List<IIndividual<T>> membersRight;

    @Nonnull
    private final SortedObjectives<IIndividual<T>, T> sortedObjectives;

    private final Lock leftMembersLock = new ReentrantLock();
    private final Lock rightMembersLock = new ReentrantLock();
    private final Lock levelLock = new ReentrantLock();


    /**
     * Inefficient (O(NlogN) CD recalc) new level construction
     * @param sorter Sorter impl
     * @param members Level members
     */
    public JFBYNonDominationLevel(@Nonnull JFB2014 sorter,
                                  @Nonnull List<IIndividual<T>> members) {
        this.sorter = sorter;
        if (!members.isEmpty()) {
            final int dim = members.get(0).getObjectives().length;
            this.sortedObjectives = SortedObjectives.create(dim, members, (i, d) -> new FitnessAndCdIndividual<>(i.getObjectives(), d, i.getPayload()));
            final List<IIndividual<T>> mems = sortedObjectives.getLexSortedPop();
            this.membersLeft = new ArrayList<>();
            this.membersRight = new ArrayList<>();
            for (int i = 0; i < mems.size() / 2; i++) {
                membersLeft.add(mems.get(i));
            }

            for (int i = mems.size() / 2; i < mems.size(); i++) {
                membersRight.add(mems.get(i));
            }
        } else {
            this.membersLeft = Collections.emptyList();
            this.membersRight = Collections.emptyList();
            this.sortedObjectives = SortedObjectives.empty(0);
        }
    }

    public JFBYNonDominationLevel(@Nonnull JFB2014 sorter,
                                  @Nonnull List<IIndividual<T>> members,
                                  @Nonnull SortedObjectives<IIndividual<T>, T> sortedObjectives) {
        this.sorter = sorter;
        final List<IIndividual<T>> mems = Collections.unmodifiableList(members);
        this.membersLeft = new ArrayList<>();
        this.membersRight = new ArrayList<>();
        for (int i = 0; i < mems.size() / 2; i++) {
            membersLeft.add(mems.get(i));
        }

        for (int i = mems.size() / 2; i < mems.size(); i++) {
            membersRight.add(mems.get(i));
        }
        this.sortedObjectives = sortedObjectives;
    }

    @Override
    @Nonnull
    public List<IIndividual<T>> getMembers() {
        final List<IIndividual<T>> members = new ArrayList<>(membersLeft);
        members.addAll(membersRight);

        return members;
    }

    @Nonnull
    public SortedObjectives<IIndividual<T>, T> getSortedObjectives() {
        return sortedObjectives;
    }

    @Override
    public MemberAdditionResult<T, JFBYNonDominationLevel<T>> addMembers(@Nonnull List<IIndividual<T>> addends) {
        final ArrayList<IIndividual<T>> nextLevel = new ArrayList<>();

//left
        try {
            leftMembersLock.lock();
            final int[] ranksLeft = new int[membersLeft.size()];
            final RankedPopulation<IIndividual<T>> rpLeft = sorter.addRankedMembers(membersLeft, ranksLeft, addends, 0);

            for (int i = 0; i < rpLeft.getPop().length; ++i) {
                if (rpLeft.getRanks()[i] != 0) {
                    nextLevel.add(rpLeft.getPop()[i]);
                }
            }
        } finally {
            leftMembersLock.unlock();
        }


//right
        try {
            rightMembersLock.lock();
            final int[] ranksRight = new int[membersRight.size()];
            final RankedPopulation<IIndividual<T>> rpRight = sorter.addRankedMembers(membersRight, ranksRight, addends, 0);

            for (int i = 0; i < rpRight.getPop().length; ++i) {
                if (rpRight.getRanks()[i] != 0) {
                    nextLevel.add(rpRight.getPop()[i]);
                }
            }
        } finally {
            rightMembersLock.unlock();
        }

        final SortedObjectives<IIndividual<T>, T> nso;
        try {
            levelLock.lock();
             nso = sortedObjectives.update(
                addends,
                nextLevel,
                (i, d) -> new FitnessAndCdIndividual<>(i.getObjectives(), d, i.getPayload())
            );
        } finally {
            levelLock.unlock();
        }

        return new MemberAdditionResult<>(
            nextLevel,
            new JFBYNonDominationLevel<>(sorter, nso.getLexSortedPop(), nso)
        );
    }

    @Override
    public boolean dominatedByAnyPointOfThisLayer(@Nonnull IIndividual point) {
        final double[] pointObj = point.getObjectives();
        for (IIndividual member : membersLeft) {
            final double[] memberObj = member.getObjectives();
            if (memberObj[0] > pointObj[0])
                break;
            if (dominates(memberObj, pointObj, pointObj.length) < 0)
                return true;
        }
        for (IIndividual member : membersRight) {
            final double[] memberObj = member.getObjectives();
            if (memberObj[0] > pointObj[0])
                break;
            if (dominates(memberObj, pointObj, pointObj.length) < 0)
                return true;
        }
        return false;
    }

    @Override
    public JFBYNonDominationLevel<T> copy() {
        final List<IIndividual<T>> newMembers = new ArrayList<>(membersLeft);
        newMembers.addAll(membersRight);
        return new JFBYNonDominationLevel<>(sorter, newMembers, sortedObjectives);
    }

    @Override
    public String toString() {
        return "membersLeft=" + membersLeft.stream()
                .map(IIndividual::getObjectives)
                .map(Arrays::toString)
                .collect(Collectors.toList()) +
                "membersRight=" + membersRight.stream()
                .map(IIndividual::getObjectives)
                .map(Arrays::toString)
                .collect(Collectors.toList());
    }
}
