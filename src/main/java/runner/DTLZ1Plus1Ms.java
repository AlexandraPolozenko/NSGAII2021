package runner;

import org.moeaframework.core.Solution;
import org.moeaframework.problem.DTLZ.DTLZ1;

public class DTLZ1Plus1Ms extends DTLZ1 {
    public DTLZ1Plus1Ms(int numberOfObjectives) {
        super(numberOfObjectives);
    }

    public DTLZ1Plus1Ms(int numberOfVariables, int numberOfObjectives) {
        super(numberOfVariables, numberOfObjectives);
    }

    @Override
    public void evaluate(Solution solution) {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        super.evaluate(solution);
    }
}
