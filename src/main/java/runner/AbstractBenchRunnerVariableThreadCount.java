package runner;

import copypaste.IManagedPopulation;
import copypaste.LevelLockJFBYPopulation;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;
import org.moeaframework.core.indicator.Hypervolume;
import org.moeaframework.problem.DTLZ.DTLZ;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.nsga2.NSGAIIMoeaRunner;
import ru.ifmo.nds.nsga2.SSNSGAII;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
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
        double stupidSum = 0;
        for (int i = 0; i < getTrueParetoFrontSize(); ++i) {
            final Solution solution = problem.generate();
            trueParetoNP.add(solution);
            stupidSum += Arrays.stream(solution.getObjectives()).sum();
        }

        hypervolume = new Hypervolume(problem, trueParetoNP);

        System.setProperty(KEY_FAST_NONDOMINATED_SORTING, String.valueOf(true));
    }

    private void printHV(IManagedPopulation<Solution> pop, int stackDepth, int runId, long runTime) {
       // System.err.println("Printing HV...");
        final NondominatedPopulation np = new NondominatedPopulation();
        for (IIndividual<Solution> iIndividual : pop.getLevelsUnsafe().get(0).getMembers()) {
            np.add(iIndividual.getPayload());
        }
        printHV(np, stackDepth + 1, runId, runTime);
    }

    private void printHV(final NondominatedPopulation np, int stackDepth, int runId, long runTime) {
        final DTLZ problem = getProblem();
        final String testMethod = getMethodName(stackDepth + 1);
        double stupidSum = 0;
        for (Solution solution : np) {
            stupidSum += Arrays.stream(solution.getObjectives()).sum();
        }
//        System.out.printf("%d\t%s\t%s\t%d\t%d\t%f\t%f\t%f\t%d\n", runId, testMethod, problem.getName(), getDim(),
//                getPopSize(), (double) runTime / 1e9, hypervolume.evaluate(np), stupidSum/np.size(), np.size()); //print milliseconds
//        System.out.flush();
    }

    private String getMethodName(int stackDepth) {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        return ste[stackDepth + 2].getMethodName();
    }

    private void concurrentTestCommon(final int threadsCount,
                                      @Nonnull final Supplier<IManagedPopulation<Solution>> popSupplier) throws InterruptedException {
        //System.out.println("Starting " + getMethodName(1));

        final int popSize = getPopSize();
        final DTLZ problem = getProblem();

        final ExecutorService es = Executors.newFixedThreadPool(threadsCount);
        final ArrayList<Long> average = new ArrayList<>();

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
                            //System.out.println(latch.toString());
                        }
                    });
                }

                latch.await();

                printHV(pop, 1, i, System.nanoTime() - startTs);

                average.add(pop.getLevelsTs().get(0).getKey() / pop.getLevelsTs().get(0).getValue());
            }
        } finally {
            es.shutdownNow();
        }

        System.out.println(average.stream().reduce(0l, (a, b) -> a += b) / average.size());
    }

    public void levelLockJfby(final int threadsCount) throws InterruptedException {
        concurrentTestCommon(threadsCount, () -> new LevelLockJFBYPopulation<>(getPopSize()));
    }
}
