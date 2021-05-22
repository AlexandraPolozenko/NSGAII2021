package copypaste.jfby;

import apple.laf.JRSUIUtils;
import copypaste.INonDominationLevel;
import copypaste.SortedObjectives;
import copypaste.sorter.JFB2014;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.util.RankedPopulation;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
    private volatile List<IIndividual<T>> membersLeft;
    @Nonnull
    private volatile List<IIndividual<T>> membersRight;
    @Nonnull
    private volatile List<IIndividual<T>> newMembers;
    @Nonnull
    private volatile List<IIndividual<T>> oldMembers;

    @Nonnull
    private volatile SortedObjectives<IIndividual<T>, T> sortedObjectives;

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
            this.newMembers = new ArrayList<>();
            this.oldMembers = new ArrayList<>();
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
        this.newMembers = new ArrayList<>();
        this.oldMembers = new ArrayList<>();
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
    public List<IIndividual<T>> addMembers(@Nonnull List<IIndividual<T>> addends) {
        final Set<IIndividual<T>> nextLevel;

//left
        try {
            leftMembersLock.lock();
            nextLevel = Collections.synchronizedSortedSet(new TreeSet<>(
                Comparator.comparingDouble(i -> i.getObjectives()[0])
            ));

            final RankedPopulation<IIndividual<T>> rpLeft = sorter.addRankedMembers(membersLeft, new int[membersLeft.size()], addends, 0);

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

            final RankedPopulation<IIndividual<T>> rpRight = sorter.addRankedMembers(membersRight, new int[membersRight.size()], addends, 0);

            for (int i = 0; i < rpRight.getPop().length; ++i) {
                if (rpRight.getRanks()[i] != 0) {
                    nextLevel.add(rpRight.getPop()[i]);
                }
            }

            final RankedPopulation<IIndividual<T>> rpNew = sorter.addRankedMembers(newMembers, new int[newMembers.size()], addends, 0);

            for (int i = 0; i < rpNew.getPop().length; ++i) {
                if (rpNew.getRanks()[i] != 0) {
                    nextLevel.add(rpNew.getPop()[i]);
                }
            }

            nextLevel.removeAll(oldMembers);
            oldMembers = new ArrayList<>(nextLevel);
            newMembers = new ArrayList<>(addends);
        } finally {
            rightMembersLock.unlock();
        }

        try {
            levelLock.lock();
            final SortedObjectives<IIndividual<T>, T> nso = sortedObjectives.update(
                addends,
                nextLevel.stream()
                    .filter(el -> sortedObjectives.getLexSortedPop().contains(el))
                    .collect(Collectors.toList()),
                (i, d) -> new FitnessAndCdIndividual<>(i.getObjectives(), d, i.getPayload())
            );

            final List<IIndividual<T>> mems = nso.getLexSortedPop();
            final List<IIndividual<T>> memsL = new ArrayList<>();
            final List<IIndividual<T>> memsR = new ArrayList<>();
            for (int i = 0; i < mems.size() / 2; i++) {
                memsL.add(mems.get(i));
            }

            for (int i = mems.size() / 2; i < mems.size(); i++) {
                memsR.add(mems.get(i));
            }

            this.sortedObjectives = nso;
            this.membersLeft = memsL;
            this.membersRight = memsR;

        } finally {
            levelLock.unlock();
        }

        return new ArrayList<>(nextLevel);
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
