package nds.shard2;

import nds.IManagedPopulation;
import nds.INonDominationLevel;
import nds.PopulationSnapshot;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.dcns.sorter.IncrementalJFB;
import ru.ifmo.nds.dcns.sorter.JFB2014;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.util.median.QuickSelect;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static ru.ifmo.nds.util.Utils.lexCompare;

@ThreadSafe
public class LevelLockJFBYPopulationShardV2<T> implements IManagedPopulation<T> {
    private static final double DEFAULT_DELETION_THRESHOLD = 1.2;

    private final List<List<Lock>> levelLocks = new CopyOnWriteArrayList<>();
    private final Lock addLevelLock = new ReentrantLock();
    private final Lock removeLevelLock = new ReentrantLock();

    private final JFB2014 sorter;

    private final AtomicInteger size = new AtomicInteger(0);
    private final CopyOnWriteArrayList<List<JFBYNonDominationLevelShardV2<T>>> nonDominationLevels;
    private final Map<IIndividual<T>, Boolean> presentIndividuals = new ConcurrentHashMap<>();
    private final Random random = ThreadLocalRandom.current();

    private final long expectedPopSize;
    private final double deletionThreshold;

    private final AtomicLong atomicLong = new AtomicLong(0);

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationShardV2() {
        this(Long.MAX_VALUE);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationShardV2(long expectedPopSize) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, DEFAULT_DELETION_THRESHOLD);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationShardV2(final long expectedPopSize,
                                     final double deletionThreshold) {
        this(new IncrementalJFB(), new CopyOnWriteArrayList<>(), expectedPopSize, deletionThreshold);
    }

    @SuppressWarnings("WeakerAccess")
    public LevelLockJFBYPopulationShardV2(@Nonnull final JFB2014 sorter,
                                     @Nonnull final CopyOnWriteArrayList<List<JFBYNonDominationLevelShardV2<T>>> nonDominationLevels,
                                     final long expectedPopSize,
                                     final double deletionThreshold) {
        this.sorter = sorter;
        this.nonDominationLevels = nonDominationLevels;
        this.expectedPopSize = expectedPopSize;
        this.deletionThreshold = deletionThreshold;

        for (List<JFBYNonDominationLevelShardV2<T>> nonDominationLevelShards : nonDominationLevels) {
            final List<Lock> lockList = new ArrayList<>();
            levelLocks.add(lockList);
            for (INonDominationLevel<T> level : nonDominationLevelShards) {
                lockList.add(new ReentrantLock());
                size.addAndGet(level.getMembers().size());
                for (IIndividual<T> individual : level.getMembers()) {
                    presentIndividuals.put(individual, true);
                }
            }
        }

    }

    @Override
    @Nonnull
    public PopulationSnapshot<T> getSnapshot() {
        final ArrayList<INonDominationLevel<T>> rs = new ArrayList<>();
        for (List<JFBYNonDominationLevelShardV2<T>> shards : nonDominationLevels) {
            List<IIndividual<T>> levelMembers = new ArrayList<>();
            for (JFBYNonDominationLevelShardV2<T> shard : shards) {
                levelMembers = lexMerge(shard.getMembers(), levelMembers);
            }
            rs.add(new JFBYNonDominationLevelShardV2<>(sorter, levelMembers));
        }

        int size = 0;
        for (INonDominationLevel<T> level : rs) {
            size += level.getMembers().size();
        }
        return new PopulationSnapshot<T>(rs, size);
    }

    public List<List<JFBYNonDominationLevelShardV2<T>>> getSnapshotLevels() {
        final ArrayList<List<JFBYNonDominationLevelShardV2<T>>> rs = new ArrayList<>();
        for (List<JFBYNonDominationLevelShardV2<T>> levelShards : nonDominationLevels) {
            rs.add(new ArrayList<>(levelShards));
        }
        return rs;
    }

    @Nonnull
    @Override
    public List<? extends JFBYNonDominationLevelShardV2<T>> getLevelsUnsafe() {
        throw new IllegalStateException("Not supported");
    }

    @SuppressWarnings("UnusedReturnValue")
    private int massRemoveWorst() {
        if (size.get() > expectedPopSize * deletionThreshold && removeLevelLock.tryLock()) {
            try {
                int remaining = (int) (size.get() - expectedPopSize);

                final int lastLevelIndex = nonDominationLevels.size() - 1;
                if (lastLevelIndex == 0) {
                    final int shardId = random.nextInt(2);

                    final Lock lock = acquireLock(lastLevelIndex, shardId);
                    if (lock != null) {
                        final JFBYNonDominationLevelShardV2<T> lastLevel = nonDominationLevels.get(lastLevelIndex).get(shardId);
                        try {
                            if (lastLevel.getMembers().size() > 30) {

                                final double[] cd = new double[lastLevel.getMembers().size()];
                                int i = 0;
                                for (IIndividual cdIndividual : lastLevel.getMembers()) {
                                    cd[i++] = cdIndividual.getCrowdingDistance();
                                }
                                int kth = Math.min(cd.length / 2, remaining);
                                final double cdThreshold = new QuickSelect().getKthElement(cd, kth);

                                if (cdThreshold <= 0.001) {
                                    System.err.println(lastLevel.getMembers());
                                }

                                final List<IIndividual<T>> newMembers = new ArrayList<>();
                                final List<IIndividual<T>> removals = new ArrayList<>();
                                for (IIndividual<T> individual : lastLevel.getMembers()) {
                                    if (kth > 0 && individual.getCrowdingDistance() <= cdThreshold) {
                                        presentIndividuals.remove(individual);
                                        removals.add(individual);
                                        --kth;
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

                                final SortedObjectivesShardV2<IIndividual<T>, T> nso = lastLevel.getSortedObjectives().update(
                                    Collections.emptyList(),
                                    removals,
                                    (i1, d) -> new FitnessAndCdIndividual<>(i1.getObjectives(), d, i1.getPayload())
                                );

                                final JFBYNonDominationLevelShardV2<T> newLevel = new JFBYNonDominationLevelShardV2<>(sorter, nso.getLexSortedPop(), nso);
                                nonDominationLevels.get(lastLevelIndex).set(shardId, newLevel);
                            }
                        } finally {
                            lock.unlock();
                        }
                    }

                    return 0;
                }

                try {
                    final List<JFBYNonDominationLevelShardV2<T>> lastLevelShards = nonDominationLevels.get(lastLevelIndex);
                    final List<Lock> lastLevelShardLocks = levelLocks.get(lastLevelIndex);
                    for (int i = 0; i < lastLevelShards.size(); ++i) {
                        if (lastLevelShardLocks.get(i).tryLock()) {
                            try {
                                remaining -= lastLevelShards.get(i).getMembers().size();
                                for (IIndividual<T> tiIndividual : lastLevelShards.get(i).getMembers()) {
                                    presentIndividuals.remove(tiIndividual);
                                }
                                lastLevelShards.set(i, new JFBYNonDominationLevelShardV2<>(sorter, Collections.emptyList()));
                            } finally {
                                lastLevelShardLocks.get(i).unlock();
                            }
                        }
                        if (remaining <= 0)
                            break;
                    }
                    if (lastLevelShards.stream().mapToInt(level -> level.getMembers().size()).sum() == 0) {
                        nonDominationLevels.remove(lastLevelIndex);
                        levelLocks.remove(lastLevelIndex);
                    }
                } finally {
                    try {
                        //addLevelLock.unlock();
                    } catch (Exception ignored) {
                    }
                }
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
            final List<List<JFBYNonDominationLevelShardV2<T>>> ndLayers = getSnapshotLevels();

            int l = 0;
            int r = ndLayers.size() - 1;
            int lastNonDominating = r + 1;
            while (l <= r) {
                final int test = (l + r) / 2;
                if (ndLayers.get(test).stream().noneMatch(lev -> lev.dominatedByAnyPointOfThisLayer(point))) {
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
        while (true) {
            try {
                return doAddIndividual(addend);
            } catch (ArrayIndexOutOfBoundsException ignored) {
                ignored.printStackTrace();
            }
        }
    }

    public int doAddIndividual(@Nonnull IIndividual<T> addend) {
        if (presentIndividuals.putIfAbsent(addend, true) != null) {
            return determineRank(addend);
        }

        int rank = determineRank(addend, -1);

        //Locked current level or all level addition

        final long id = atomicLong.incrementAndGet();

        if (rank >= nonDominationLevels.size()) {
            addLevelLock.lock(); //fixme racy level add
            final List<IIndividual<T>> individuals = Collections.singletonList(addend);
            final JFBYNonDominationLevelShardV2<T> level = new JFBYNonDominationLevelShardV2<>(
                sorter,
                individuals
            );
            levelLocks.add(new CopyOnWriteArrayList<>(Arrays.asList(new ReentrantLock(), new ReentrantLock())));
            nonDominationLevels.add(new CopyOnWriteArrayList<>(Arrays.asList(level, new JFBYNonDominationLevelShardV2<>(sorter, Collections.emptyList()))));
            addLevelLock.unlock(); //created 2 shards
        } else {
            List<IIndividual<T>> addends = Collections.singletonList(addend);
            int i = rank;
            while (!addends.isEmpty() && i < nonDominationLevels.size()) {
                //assertion: we have locked levelLocks.get(i)
                final List<JFBYNonDominationLevelShardV2<T>> levelShards = nonDominationLevels.get(i);
                final int modifiedIndex = random.nextInt(levelShards.size());
                List<IIndividual<T>> nextAddends = new ArrayList<>();
                for (int j = 0; j < levelShards.size(); ++j) {
                    final JFBYNonDominationLevelShardV2<T> level = levelShards.get(j);
                    final INonDominationLevel.MemberAdditionResult<T, JFBYNonDominationLevelShardV2<T>> memberAdditionResult;
                    final Lock l = acquireLock(i, j);
                    try {
                        if (l == addLevelLock) {
                            break;
                        }
                        if (j == modifiedIndex) {
                            memberAdditionResult = level.addMembers(addends);
                        } else {
                            memberAdditionResult = level.evictDominatedMembers(addends);
                        }
                        levelShards.set(j, memberAdditionResult.getModifiedLevel());
                        nextAddends = lexMerge(nextAddends, memberAdditionResult.getEvictedMembers());
                    } finally {
                        l.unlock();
                    }
                }
                addends = nextAddends;
                i++;
            }
            if (!addends.isEmpty()) {
                addLevelLock.lock();

                final JFBYNonDominationLevelShardV2<T> level = new JFBYNonDominationLevelShardV2<>(
                    sorter,
                    addends
                );
                levelLocks.add(new CopyOnWriteArrayList<>(Arrays.asList(new ReentrantLock(), new ReentrantLock())));
                nonDominationLevels.add(new CopyOnWriteArrayList<>(Arrays.asList(level, new JFBYNonDominationLevelShardV2<>(sorter, Collections.emptyList()))));
                addLevelLock.unlock(); //created 2 shards
            }
        }


        size.incrementAndGet();
        massRemoveWorst();

        return rank;
    }

    private Lock acquireLock(int rank, int shard) {
        while (true) {
            Lock lock = null;
            try {
                if (rank < nonDominationLevels.size()) {
                    lock = levelLocks.get(rank).get(shard);
                    lock.lock();
                    if (levelLocks.get(rank).get(shard) == lock) {
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
    public LevelLockJFBYPopulationShardV2<T> clone() {
        throw new IllegalStateException("Ok maybe should implement it. Correctly!!");
    }


    private List<IIndividual<T>> lexMerge(@Nonnull final List<IIndividual<T>> aList,
                                          @Nonnull final List<IIndividual<T>> mList) {
        int ai = 0;
        int mi = 0;
        final List<IIndividual<T>> result = new ArrayList<>(aList.size() + mList.size());
        while (mi < mList.size() || ai < aList.size()) {
            if (mi >= mList.size()) {
                result.add(aList.get(ai));
                ++ai;
            } else if (ai >= aList.size()) {
                result.add(mList.get(mi));
                ++mi;
            } else {
                final IIndividual<T> m = mList.get(mi);
                final IIndividual<T> a = aList.get(ai);
                if (lexCompare(m.getObjectives(), a.getObjectives(), a.getObjectives().length) <= 0) {
                    result.add(m);
                    ++mi;
                } else {
                    result.add(a);
                    ++ai;
                }
            }
        }
        return result;
    }
}
