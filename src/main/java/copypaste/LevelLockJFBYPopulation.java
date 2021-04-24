package copypaste;

import copypaste.jfby.JFBYNonDominationLevel;
import copypaste.sorter.IncrementalJFB;
import copypaste.sorter.JFB2014;
import javafx.util.Pair;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.util.median.QuickSelect;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class LevelLockJFBYPopulation<T> implements IManagedPopulation<T> {
    private static final double DEFAULT_DELETION_THRESHOLD = 1.2;

    private final List<Lock> levelLocks = new CopyOnWriteArrayList<>();
    private final Lock addLevelLock = new ReentrantLock();
    private final Lock removeLevelLock = new ReentrantLock();

    private final JFB2014 sorter;

    private final AtomicInteger size = new AtomicInteger(0);
    private final CopyOnWriteArrayList<JFBYNonDominationLevel<T>> nonDominationLevels;
    private final Map<IIndividual<T>, Boolean> presentIndividuals = new ConcurrentHashMap<>();

    private final long expectedPopSize;
    private final double deletionThreshold;

    private final Map<Integer, Pair<Long, Integer>> levelsTs;

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulation() {
        this(Long.MAX_VALUE);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulation(long expectedPopSize) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, DEFAULT_DELETION_THRESHOLD);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulation(final long expectedPopSize,
                                   final double deletionThreshold) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, deletionThreshold);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulation(@Nonnull final JFB2014 sorter,
                                   @Nonnull final CopyOnWriteArrayList<JFBYNonDominationLevel<T>> nonDominationLevels,
                                   final long expectedPopSize,
                                   final double deletionThreshold) {
        this.sorter = sorter;
        this.nonDominationLevels = nonDominationLevels;
        this.expectedPopSize = expectedPopSize;
        this.deletionThreshold = deletionThreshold;
        this.levelsTs = new ConcurrentHashMap<>();

        for (INonDominationLevel<T> level : nonDominationLevels) {
            levelLocks.add(new ReentrantLock());
            size.addAndGet(level.getMembers().size());
            for (IIndividual<T> individual : level.getMembers()) {
                presentIndividuals.put(individual, true);
            }
        }
    }

    @Override
    @Nonnull
    public PopulationSnapshot<T> getSnapshot() {
        final ArrayList<INonDominationLevel<T>> rs = new ArrayList<>(nonDominationLevels);
        int size = 0;
        for (INonDominationLevel<T> level : rs) {
            size += level.getMembers().size();
        }
        return new PopulationSnapshot<>(rs, size);
    }

    @Nonnull
    @Override
    public List<? extends INonDominationLevel<T>> getLevelsUnsafe() {
        return Collections.unmodifiableList(nonDominationLevels);
    }

    @SuppressWarnings("UnusedReturnValue")
    private int massRemoveWorst() {
        if (size.get() > expectedPopSize * deletionThreshold && removeLevelLock.tryLock()) {
            try {
                final int toDelete = (int) (size.get() - expectedPopSize);
                int remaining = toDelete;
                while (remaining > 0) {
                    final int lastLevelIndex = nonDominationLevels.size() - 1;
                    final Lock lastLevelLock;

                    lastLevelLock = acquireLock(lastLevelIndex);
                    addLevelLock.lock();
                    if (lastLevelLock != levelLocks.get(lastLevelIndex)
                        || lastLevelIndex != nonDominationLevels.size() - 1) {
                        lastLevelLock.unlock();
                        addLevelLock.unlock();
                        continue;
                    }

                    try {
                        final JFBYNonDominationLevel<T> lastLevel = nonDominationLevels.get(lastLevelIndex);
                        if (lastLevel.getMembers().size() <= remaining) {
                            levelLocks.remove(lastLevelIndex);
                            nonDominationLevels.remove(lastLevelIndex);
                            if (lastLevel.getMembers().isEmpty()) {
                                System.err.println("Empty last ND level! Levels = " + nonDominationLevels);
                            } else {
                                for (IIndividual individual : lastLevel.getMembers()) {
                                    presentIndividuals.remove(individual);
                                }
                            }
                            remaining -= lastLevel.getMembers().size();
                        } else {
                            final double[] cd = new double[lastLevel.getMembers().size()];
                            int i = 0;
                            for (IIndividual cdIndividual : lastLevel.getMembers()) {
                                cd[i++] = cdIndividual.getCrowdingDistance();
                            }
                            final double cdThreshold = new QuickSelect().getKthElement(cd, remaining);
                            final List<IIndividual<T>> newMembers = new ArrayList<>();
                            final List<IIndividual<T>> removals = new ArrayList<>();
                            for (IIndividual<T> individual : lastLevel.getMembers()) {
                                if (remaining > 0 && individual.getCrowdingDistance() <= cdThreshold) {
                                    presentIndividuals.remove(individual);
                                    removals.add(individual);
                                    --remaining;
                                } else {
                                    newMembers.add(individual);
                                }
                            }
                            if (newMembers.isEmpty()) {
                                System.err.println("Empty members generated");
                                System.out.println(cdThreshold);
                                System.out.println(Arrays.toString(cd));
                                throw new RuntimeException(remaining + "<" + lastLevel.getMembers().size());
                            }


                            final SortedObjectives<IIndividual<T>, T> nso = lastLevel.getSortedObjectives().update(
                                Collections.emptyList(),
                                removals,
                                (i1, d) -> new FitnessAndCdIndividual<>(i1.getObjectives(), d, i1.getPayload())
                            );

                            final JFBYNonDominationLevel<T> newLevel = new JFBYNonDominationLevel<>(sorter, nso.getLexSortedPop(), nso);
                            nonDominationLevels.set(lastLevelIndex, newLevel);
                        }

                    } finally {
                        lastLevelLock.unlock();
                        addLevelLock.unlock();
                    }
                }
                if (toDelete > 0) {
                    int tSize = size.get();
                    while (!size.compareAndSet(tSize, tSize - toDelete)) {
                        tSize = size.get();
                    }
                }
                return toDelete;
            } finally {
                removeLevelLock.unlock();
            }
        }
        return 0;
    }

    @Override
    public int size() {
        return size.get();
    }

    private int determineRank(IIndividual<T> point, int rankHint) {
        if (rankHint < 0) {
            final List<? extends INonDominationLevel<T>> ndLayers = getSnapshot().getLevels();

            int l = 0;
            int r = ndLayers.size() - 1;
            int lastNonDominating = r + 1;
            while (l <= r) {
                final int test = (l + r) / 2;
                if (!ndLayers.get(test).dominatedByAnyPointOfThisLayer(point)) {
                    lastNonDominating = test;
                    r = test - 1;
                } else {
                    l = test + 1;
                }
            }

            return lastNonDominating;
        } else {
            return rankHint + 1;
        }
    }

    @Override
    public int addIndividual(@Nonnull IIndividual<T> addend) {
        final long ts = System.nanoTime();

        if (presentIndividuals.putIfAbsent(addend, true) != null) {
            return determineRank(addend);
        }

        int rank = -1;
        while (true) {
            rank = determineRank(addend, rank);
            final Lock lock = acquireLock(rank);
            try {
                if (rank >= nonDominationLevels.size() && lock == addLevelLock) {
                    break;
                } else if (!nonDominationLevels.get(rank).dominatedByAnyPointOfThisLayer(addend)) {
                    lock.unlock();
                    break;
                } else {
                    lock.unlock();
                }
            } catch (ArrayIndexOutOfBoundsException ignored) {
                lock.unlock();
            }
        }

        if (rank >= nonDominationLevels.size()) {
            final List<IIndividual<T>> individuals = Collections.singletonList(addend);
            final JFBYNonDominationLevel<T> level = new JFBYNonDominationLevel<>(
                sorter,
                individuals
            );
            levelLocks.add(new ReentrantLock());
            nonDominationLevels.add(level);
            addLevelLock.unlock();
        } else {
            List<IIndividual<T>> addends = Collections.singletonList(addend);
            int i = rank;
            while (!addends.isEmpty() && i < nonDominationLevels.size()) {
                final INonDominationLevel.MemberAdditionResult<T, JFBYNonDominationLevel<T>> memberAdditionResult;
                addends = nonDominationLevels.get(i).addMembers(addends);

                i++;
            }
            if (!addends.isEmpty()) {
                try {
                    addLevelLock.lock();
                    levelLocks.add(new ReentrantLock());
                    final JFBYNonDominationLevel<T> level = new JFBYNonDominationLevel<>(sorter, addends);
                    nonDominationLevels.add(level);
                } finally {
                    addLevelLock.unlock();
                }
            }
        }

        final long spent = System.nanoTime() - ts;

        levelsTs.putIfAbsent(rank, new Pair(0l, 0));
        levelsTs.put(rank, new Pair(levelsTs.get(rank).getKey() + spent, levelsTs.get(rank).getValue() + 1));

        size.incrementAndGet();

        return rank;
    }

    private Lock acquireLock(int rank) {
        while (true) {
            Lock lock = null;
            try {
                if (rank < nonDominationLevels.size()) {
                    lock = levelLocks.get(rank);
                    lock.lock();
                    if (levelLocks.get(rank) == lock) {
                        return lock;
                    } else {
                        lock.unlock();
                    }
                } else {
                    addLevelLock.lock();
                    if (rank < nonDominationLevels.size()) {
                        addLevelLock.unlock();
                    } else {
                        return addLevelLock;
                    }
                }
            } catch (ArrayIndexOutOfBoundsException ignored) {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * @return A copy of this population. All layers are also copied.
     */
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public LevelLockJFBYPopulation<T> clone() {
        final LevelLockJFBYPopulation<T> copy = new LevelLockJFBYPopulation<>(sorter, nonDominationLevels, expectedPopSize, deletionThreshold);
        for (INonDominationLevel<T> level : nonDominationLevels) {
            copy.getSnapshot().getLevels().add(level.copy());
        }
        return copy;
    }

    @Override
    public Map<Integer, Pair<Long, Integer>> getLevelsTs() {
        return levelsTs;
    }
}
