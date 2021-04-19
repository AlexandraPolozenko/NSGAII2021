package ru.ifmo.nds.nsga2.variation;

import org.moeaframework.core.Solution;
import org.moeaframework.core.Variable;
import org.moeaframework.core.Variation;
import org.moeaframework.core.variable.RealVariable;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SBX implements Variation {
    private final double probability;
    private final double distributionIndex;
    private final boolean swap;
    private final boolean symmetric;

    public SBX(double probability, double distributionIndex) {
        this(probability, distributionIndex, true, false);
    }

    public SBX(double probability, double distributionIndex, boolean swap, boolean symmetric) {
        this.probability = probability;
        this.distributionIndex = distributionIndex;
        this.swap = swap;
        this.symmetric = symmetric;
    }

    public double getProbability() {
        return this.probability;
    }

    public double getDistributionIndex() {
        return this.distributionIndex;
    }

    public boolean isSwap() {
        return this.swap;
    }

    public boolean isSymmetric() {
        return this.symmetric;
    }

    public int getArity() {
        return 2;
    }

    public Solution[] evolve(Solution[] parents) {
        Solution result1 = parents[0].copy();
        Solution result2 = parents[1].copy();
        final Random random = ThreadLocalRandom.current();
        if (random.nextDouble() <= this.probability) {
            for(int i = 0; i < result1.getNumberOfVariables(); ++i) {
                Variable variable1 = result1.getVariable(i);
                Variable variable2 = result2.getVariable(i);
                if (random.nextBoolean() && variable1 instanceof RealVariable && variable2 instanceof RealVariable) {
                    if (this.symmetric) {
                        evolve_symmetric((RealVariable)variable1, (RealVariable)variable2, this.distributionIndex, this.swap);
                    } else {
                        evolve_asymmetric((RealVariable)variable1, (RealVariable)variable2, this.distributionIndex, this.swap);
                    }
                }
            }
        }

        return new Solution[]{result1, result2};
    }

    public static void evolve(RealVariable v1, RealVariable v2, double distributionIndex) {
        evolve_asymmetric(v1, v2, distributionIndex, true);
    }

    public static void evolve_symmetric(RealVariable v1, RealVariable v2, double distributionIndex, boolean swap) {
        double x1 = v1.getValue();
        double x2 = v2.getValue();
        double lb = v1.getLowerBound();
        double ub = v1.getUpperBound();
        final Random random = ThreadLocalRandom.current();

        if (Math.abs(x1 - x2) > 1.0E-10D) {
            double y1;
            double y2;
            if (x2 > x1) {
                y2 = x2;
                y1 = x1;
            } else {
                y2 = x1;
                y1 = x2;
            }

            double beta;
            if (y1 - lb > ub - y2) {
                beta = 1.0D + 2.0D * (ub - y2) / (y2 - y1);
            } else {
                beta = 1.0D + 2.0D * (y1 - lb) / (y2 - y1);
            }

            beta = 1.0D / beta;
            double alpha = 2.0D - Math.pow(beta, distributionIndex + 1.0D);
            double rand = random.nextDouble();
            double betaq;
            if (rand <= 1.0D / alpha) {
                alpha *= rand;
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            } else {
                alpha *= rand;
                alpha = 1.0D / (2.0D - alpha);
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            }

            x1 = 0.5D * (y1 + y2 - betaq * (y2 - y1));
            x2 = 0.5D * (y1 + y2 + betaq * (y2 - y1));
            if (x1 < lb) {
                x1 = lb;
            } else if (x1 > ub) {
                x1 = ub;
            }

            if (x2 < lb) {
                x2 = lb;
            } else if (x2 > ub) {
                x2 = ub;
            }

            if (swap && random.nextBoolean()) {
                double temp = x1;
                x1 = x2;
                x2 = temp;
            }

            v1.setValue(x1);
            v2.setValue(x2);
        }

    }

    public static void evolve_asymmetric(RealVariable v1, RealVariable v2, double distributionIndex, boolean swap) {
        double x1 = v1.getValue();
        double x2 = v2.getValue();
        double lb = v1.getLowerBound();
        double ub = v1.getUpperBound();
        final Random random = ThreadLocalRandom.current();

        if (Math.abs(x1 - x2) > 1.0E-10D) {
            double y1;
            double y2;
            if (x2 > x1) {
                y2 = x2;
                y1 = x1;
            } else {
                y2 = x1;
                y1 = x2;
            }

            double beta = 1.0D / (1.0D + 2.0D * (y1 - lb) / (y2 - y1));
            double alpha = 2.0D - Math.pow(beta, distributionIndex + 1.0D);
            double rand = random.nextDouble();
            double betaq;
            if (rand <= 1.0D / alpha) {
                alpha *= rand;
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            } else {
                alpha *= rand;
                alpha = 1.0D / (2.0D - alpha);
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            }

            x1 = 0.5D * (y1 + y2 - betaq * (y2 - y1));
            beta = 1.0D / (1.0D + 2.0D * (ub - y2) / (y2 - y1));
            alpha = 2.0D - Math.pow(beta, distributionIndex + 1.0D);
            if (rand <= 1.0D / alpha) {
                alpha *= rand;
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            } else {
                alpha *= rand;
                alpha = 1.0D / (2.0D - alpha);
                betaq = Math.pow(alpha, 1.0D / (distributionIndex + 1.0D));
            }

            x2 = 0.5D * (y1 + y2 + betaq * (y2 - y1));
            if (x1 < lb) {
                x1 = lb;
            } else if (x1 > ub) {
                x1 = ub;
            }

            if (x2 < lb) {
                x2 = lb;
            } else if (x2 > ub) {
                x2 = ub;
            }

            if (swap && random.nextBoolean()) {
                double temp = x1;
                x1 = x2;
                x2 = temp;
            }

            v1.setValue(x1);
            v2.setValue(x2);
        }

    }
}
