package runner;

import org.moeaframework.problem.DTLZ.DTLZ;
import org.moeaframework.problem.DTLZ.DTLZ1;

public class FixedThreadCountConfigurationsMain {
    public static void main(String[] args) throws Exception {
        System.out.println("dim3sleep0 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim3sleep0 = new AbstractBenchRunnerFixedThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1(getDim());
            }

            @Override
            int getDim() {
                return 3;
            }

            @Override
            int getNumberOfEvaluations() {
                return 300000;
            }
        };

        dim3sleep0.levelLockJfbyT3(); //TODO: call the methods for the configurations that you want to measure

        System.out.println("dim4sleep0 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim4sleep0 = new AbstractBenchRunnerFixedThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1(getDim());
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

//        dim4sleep0.cjfbyAltT3();
        //TODO: call some methods if you need to check 4d configurations

        System.out.println("dim5sleep0 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim5sleep0 = new AbstractBenchRunnerFixedThreadCount() {
            @Override
            DTLZ getProblem() {
                return new DTLZ1(getDim());
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

        dim5sleep0.testNSGAII();
//        dim5sleep0.tsT12();
        //TODO same here...

        System.out.println("dim3sleep1 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim3sleep1 = new AbstractBenchRunnerFixedThreadCount() {
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
        };

//        dim3sleep1.tsT12();
        //TODO ...and here, and further. These last tests are with 'IO-emulating' DTLZ1 problem

        System.out.println("dim4sleep1 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim4sleep1 = new AbstractBenchRunnerFixedThreadCount() {
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

//        dim4sleep1.tsT12();

        System.out.println("dim5sleep1 benchs...");
        final AbstractBenchRunnerFixedThreadCount dim5sleep1 = new AbstractBenchRunnerFixedThreadCount() {
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
        dim5sleep1.testNSGAII();
//        dim5sleep1.tsT12();
    }
}
