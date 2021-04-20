package runner;

import org.moeaframework.problem.DTLZ.DTLZ;

public class ConcurrentConfigurationSearcher {
    public static void main(String[] args) throws Exception {
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

        System.out.println("dim3 ll");
        long prevDuration = Integer.MAX_VALUE;
        for (int i = 1; i < 301; ++i) {
            System.out.println(i);
            final long start = System.currentTimeMillis();
            d3Runner.levelLockJfby(i);
            final long duration = System.currentTimeMillis() - start;
//            System.out.println(duration);
            if (duration > prevDuration * 2)
                break;
            prevDuration = duration;
        }

        final AbstractBenchRunnerVariableThreadCount d3RunnerPop1000 = new AbstractBenchRunnerVariableThreadCount() {
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
                return 300000;
            }

            @Override
            protected int getPopSize() {
                return 1000;
            }
        };

        System.out.println("dim3 ll1000");
        prevDuration = Integer.MAX_VALUE;

        for (int i = 1; i < 301; ++i) {
            System.out.println(i);
            final long start = System.currentTimeMillis();
            d3RunnerPop1000.levelLockJfby(i);
            final long duration = System.currentTimeMillis() - start;
            if (duration > prevDuration * 2)
                break;
            prevDuration = duration;
        }

        final AbstractBenchRunnerVariableThreadCount d4Runner = new AbstractBenchRunnerVariableThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1Plus1Ms(getDim());
            }

            @Override
            int getDim() {
                return 4;
            }

            @Override
            int getNumberOfEvaluations() {
                return 200000;
            }
        };

        System.out.println("dim4 ll");
        prevDuration = Integer.MAX_VALUE;
        for (int i = 1; i < 301; ++i) {
            System.out.println(i);
            final long start = System.currentTimeMillis();
            d4Runner.levelLockJfby(i);
            final long duration = System.currentTimeMillis() - start;
            if (duration > prevDuration * 2)
                break;
            prevDuration = duration;
        }

        final AbstractBenchRunnerVariableThreadCount d5Runner = new AbstractBenchRunnerVariableThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1Plus1Ms(getDim());
            }

            @Override
            int getDim() {
                return 5;
            }

            @Override
            int getNumberOfEvaluations() {
                return 800000;
            }

            @Override
            protected int getRunCount() {
                return 3;
            }

            @Override
            protected int getPopSize() {
                return 800;
            }
        };

        System.out.println("dim5 ll");
        prevDuration = Integer.MAX_VALUE;
        for (int i = 1; i < 301; ++i) {
            System.out.println(i);
            final long start = System.currentTimeMillis();
            d5Runner.levelLockJfby(i);
            final long duration = System.currentTimeMillis() - start;
            if (duration > prevDuration * 2)
                break;
            prevDuration = duration;
        }
    }
}
