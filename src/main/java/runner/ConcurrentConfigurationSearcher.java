package runner;

import org.moeaframework.problem.DTLZ.DTLZ;

import java.lang.reflect.Method;

public class ConcurrentConfigurationSearcher {
    public static void main(String[] args) throws Exception {
        System.out.println("original");
        runCheck(AbstractBenchRunnerVariableThreadCount.class.getMethod("levelLockJfby", int.class));

        System.out.println("release locks earlier");
        runCheck(AbstractBenchRunnerVariableThreadCount.class.getMethod("levelLockJfbyReleaseLocksEarlier", int.class));

        System.out.println("optimize mass remove");
        runCheck(AbstractBenchRunnerVariableThreadCount.class.getMethod("levelLockJfbyOptimizeRemove", int.class));

        System.out.println("level shard version 1");
        runCheck(AbstractBenchRunnerVariableThreadCount.class.getMethod("levelLockJfbyShardV1", int.class));

        System.out.println("level shard version 2");
        runCheck(AbstractBenchRunnerVariableThreadCount.class.getMethod("levelLockJfbyShardV2", int.class));
    }

    public static void runCheck(Method method) throws Exception {
        final AbstractBenchRunnerVariableThreadCount d3Runner = new AbstractBenchRunnerVariableThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1Plus1Ms(getDim());
            }

            @Override
            int getDim() {
                return 3;
            }

            @Override
            int getNumberOfEvaluations() {
                return 3000;
            }

            @Override
            protected int getPopSize() {
                return 250;
            }
        };

        long prevDuration = Integer.MAX_VALUE;
        for (int i = 1; i < 15; ++i) {
            System.out.println(i);
            final long start = System.currentTimeMillis();
            method.invoke(d3Runner, i);
            final long duration = System.currentTimeMillis() - start;
            System.out.println(duration);
            if (duration > prevDuration * 2)
                break;
            prevDuration = duration;
        }
    }
}
