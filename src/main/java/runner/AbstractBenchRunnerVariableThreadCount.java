package runner;

import nds.IManagedPopulation;
import nds.LevelLockJFBYPopulationOptimizeRemove;
import nds.LevelLockJFBYPopulationOriginal;
import nds.LevelLockJFBYPopulationReleaseLockEarlier;
import nds.shard1.LevelLockJFBYPopulationShardV1;
import nds.shard2.LevelLockJFBYPopulationShardV2;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;
import org.moeaframework.core.indicator.Hypervolume;
import org.moeaframework.problem.DTLZ.DTLZ;
import ru.ifmo.nds.nsga2.NSGAIIMoeaRunner;
import ru.ifmo.nds.nsga2.SSNSGAII;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.moeaframework.core.Settings.KEY_FAST_NONDOMINATED_SORTING;

public abstract class AbstractBenchRunnerVariableThreadCount {
    abstract DTLZ getProblem();

    abstract int getDim();

    protected int getPopSize() {
        return 500;
    }

    protected int getTrueParetoFrontSize() {
        return 100;
    }

    protected int getRunCount() {
        return 3;
    }

    abstract int getNumberOfEvaluations();

    protected long getNumberOfIncrementalInsertions(final long nThreads) {
        long tmp = getNumberOfEvaluations();
        return tmp / nThreads;
    }

    private final Hypervolume hypervolume;

    public AbstractBenchRunnerVariableThreadCount() {
        final NondominatedPopulation trueParetoNP = new NondominatedPopulation();
        final DTLZ problem = getProblem();
        for (int i = 0; i < getTrueParetoFrontSize(); ++i) {
            final Solution solution = problem.generate();
            trueParetoNP.add(solution);
        }

        hypervolume = new Hypervolume(problem, trueParetoNP);

        System.setProperty(KEY_FAST_NONDOMINATED_SORTING, String.valueOf(true));
    }

    private void concurrentTestCommon(final int threadsCount,
                                      @Nonnull final Supplier<IManagedPopulation<Solution>> popSupplier) throws InterruptedException {

        final int popSize = getPopSize();
        final DTLZ problem = getProblem();

        final ExecutorService es = Executors.newFixedThreadPool(threadsCount);

        try {
            for (int i = 0; i < getRunCount(); ++i) {
                final IManagedPopulation<Solution> pop = popSupplier.get();
                final SSNSGAII nsga = NSGAIIMoeaRunner.newSSNSGAII(popSize, problem, pop);
                nsga.step();

                final CountDownLatch latch = new CountDownLatch(threadsCount);
                final long startTs = System.nanoTime();
                for (int t = 0; t < threadsCount; ++t) {
                    es.submit(() -> {
                        try {
                            for (long j = 0; j < getNumberOfIncrementalInsertions(threadsCount); ++j) {
                                nsga.step();
                            }
                        } catch (Throwable th) {
                            th.printStackTrace();
                            System.err.flush();
                            throw th;
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                latch.await();
            }
        } finally {
            es.shutdownNow();
        }
    }

    public void levelLockJfby(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulationOriginal<>(getPopSize()));
    }

    public void levelLockJfbyReleaseLocksEarlier(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulationReleaseLockEarlier<>(getPopSize()));
    }

    public void levelLockJfbyOptimizeRemove(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulationOptimizeRemove<>(getPopSize()));
    }

    public void levelLockJfbyShardV1(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulationShardV1<>(getPopSize()));
    }

    public void levelLockJfbyShardV2(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulationShardV2<>(getPopSize()));
    }
}
