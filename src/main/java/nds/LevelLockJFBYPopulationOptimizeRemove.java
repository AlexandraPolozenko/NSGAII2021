package nds;

import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.dcns.sorter.IncrementalJFB;
import ru.ifmo.nds.dcns.sorter.JFB2014;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class LevelLockJFBYPopulationOptimizeRemove<T> implements IManagedPopulation<T> {
    private static final double DEFAULT_DELETION_THRESHOLD = 1.2;

    private final List<Lock> levelLocks = new CopyOnWriteArrayList<>(); //thread-safe массив локов на уровень
    private final Lock addLevelLock = new ReentrantLock();
    private final Lock removeLevelLock = new ReentrantLock();

    private final JFB2014 sorter;

    private final AtomicInteger size = new AtomicInteger(0);
    private final CopyOnWriteArrayList<JFBYNonDominationLevel<T>> nonDominationLevels;
    private final Map<IIndividual<T>, Boolean> presentIndividuals = new ConcurrentHashMap<>();

    private final long expectedPopSize;
    private final double deletionThreshold;

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationOptimizeRemove() {
        this(Long.MAX_VALUE);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationOptimizeRemove(long expectedPopSize) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, DEFAULT_DELETION_THRESHOLD);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationOptimizeRemove(final long expectedPopSize,
                                           final double deletionThreshold) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, deletionThreshold);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationOptimizeRemove(@Nonnull final JFB2014 sorter,
                                           @Nonnull final CopyOnWriteArrayList<JFBYNonDominationLevel<T>> nonDominationLevels,
                                           final long expectedPopSize,
                                           final double deletionThreshold) {
        this.sorter = sorter;
        this.nonDominationLevels = nonDominationLevels;
        this.expectedPopSize = expectedPopSize;
        this.deletionThreshold = deletionThreshold;

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
                        }
                        break;
                    } finally {
                        lastLevelLock.unlock();
                        addLevelLock.unlock();
                    }
                }
                if (toDelete > 0) {
                    int tSize = size.get();
                    while (!size.compareAndSet(tSize, tSize - toDelete + remaining)) {
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
        if (presentIndividuals.putIfAbsent(addend, true) != null) {
            return determineRank(addend);
        }

        int rank = -1;
        while (true) { //TODO: under remove level lock? ReentrantReadWriteLock?
            rank = determineRank(addend, rank);
            final Lock lock = acquireLock(rank);
            try {
                if (rank >= nonDominationLevels.size() && lock == addLevelLock) {
                    break;
                } else if (!nonDominationLevels.get(rank).dominatedByAnyPointOfThisLayer(addend)) {
                    break;
                } else {
                    lock.unlock();
                }
            } catch (ArrayIndexOutOfBoundsException ignored) {
                lock.unlock();
            }
        }

        //Locked current level or all level addition

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
                //assertion: we have locked levelLocks.get(i)
                try {
                    final JFBYNonDominationLevel<T> level = nonDominationLevels.get(i);
                    memberAdditionResult = level.addMembers(addends);
                    nonDominationLevels.set(i, memberAdditionResult.getModifiedLevel());

                    if (!memberAdditionResult.getEvictedMembers().isEmpty()) {
                        acquireLock(i + 1);
                    }
                    addends = memberAdditionResult.getEvictedMembers();

                } finally {
                    levelLocks.get(i).unlock();
                }

                i++;
            }
            if (!addends.isEmpty()) {
                levelLocks.add(new ReentrantLock());
                final JFBYNonDominationLevel<T> level = new JFBYNonDominationLevel<>(sorter, addends); //New level - full CD calc
                nonDominationLevels.add(level);
                addLevelLock.unlock();
            }
        }

        size.incrementAndGet();
        massRemoveWorst();

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
    public LevelLockJFBYPopulationOptimizeRemove<T> clone() {
        final LevelLockJFBYPopulationOptimizeRemove<T> copy = new LevelLockJFBYPopulationOptimizeRemove<>(sorter, nonDominationLevels, expectedPopSize, deletionThreshold);
        for (INonDominationLevel<T> level : nonDominationLevels) {
            copy.getSnapshot().getLevels().add(level.copy());
        }
        return copy;
    }
}

