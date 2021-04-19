package ru.ifmo.nds.nsga2.variation;


import org.moeaframework.core.Solution;
import org.moeaframework.core.Variable;
import org.moeaframework.core.Variation;
import org.moeaframework.core.variable.RealVariable;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class PM implements Variation {
    private final double probability;
    private final double distributionIndex;

    public PM(double probability, double distributionIndex) {
        this.probability = probability;
        this.distributionIndex = distributionIndex;
    }

    public double getProbability() {
        return this.probability;
    }

    public double getDistributionIndex() {
        return this.distributionIndex;
    }

    public Solution[] evolve(Solution[] parents) {
        Solution result = parents[0].copy();

        final Random random = ThreadLocalRandom.current();
        for(int i = 0; i < result.getNumberOfVariables(); ++i) {
            Variable variable = result.getVariable(i);
            if (random.nextDouble() <= this.probability && variable instanceof RealVariable) {
                evolve((RealVariable)variable, this.distributionIndex);
            }
        }

        return new Solution[]{result};
    }

    public static void evolve(RealVariable v, double distributionIndex) {
        final Random random = ThreadLocalRandom.current();
        double u = random.nextDouble();
        double x = v.getValue();
        double lb = v.getLowerBound();
        double ub = v.getUpperBound();
        double dx = ub - lb;
        double b;
        double delta;
        double bl;
        if (u < 0.5D) {
            bl = (x - lb) / dx;
            b = 2.0D * u + (1.0D - 2.0D * u) * Math.pow(1.0D - bl, distributionIndex + 1.0D);
            delta = Math.pow(b, 1.0D / (distributionIndex + 1.0D)) - 1.0D;
        } else {
            bl = (ub - x) / dx;
            b = 2.0D * (1.0D - u) + 2.0D * (u - 0.5D) * Math.pow(1.0D - bl, distributionIndex + 1.0D);
            delta = 1.0D - Math.pow(b, 1.0D / (distributionIndex + 1.0D));
        }

        x += delta * dx;
        if (x < lb) {
            x = lb;
        } else if (x > ub) {
            x = ub;
        }

        v.setValue(x);
    }

    public int getArity() {
        return 1;
    }
}
