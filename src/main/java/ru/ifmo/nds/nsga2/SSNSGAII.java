package ru.ifmo.nds.nsga2;

import copypaste.IManagedPopulation;
import org.apache.commons.math3.random.MersenneTwister;
import org.moeaframework.algorithm.AbstractAlgorithm;
import org.moeaframework.core.EpsilonBoxDominanceArchive;
import org.moeaframework.core.EpsilonBoxEvolutionaryAlgorithm;
import org.moeaframework.core.Initialization;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.NondominatedSortingPopulation;
import org.moeaframework.core.Problem;
import org.moeaframework.core.Solution;
import org.moeaframework.core.Variation;
import org.moeaframework.core.operator.CompoundVariation;
import ru.ifmo.nds.impl.FitnessAndCdIndividual;
import ru.ifmo.nds.impl.RankedIndividual;
import ru.ifmo.nds.nsga2.evo.ChainedComparator;
import ru.ifmo.nds.nsga2.evo.CrowdingComparator;
import ru.ifmo.nds.nsga2.evo.RankComparator;
import ru.ifmo.nds.nsga2.evo.Selection;
import ru.ifmo.nds.nsga2.variation.PM;
import ru.ifmo.nds.nsga2.variation.SBX;

import java.util.Comparator;
import java.util.List;

/**
 * Steady-state modification of NSGA2, implemented using incremental NDS.
 * <p>
 * References:
 * <ol>
 * <li>Deb, K. et al.  "A Fast Elitist Multi-Objective Genetic Algorithm:
 * NSGA-II."  IEEE Transactions on Evolutionary Computation, 6:182-197,
 * 2000.
 * <li>Kollat, J. B., and Reed, P. M.  "Comparison of Multi-Objective
 * Evolutionary Algorithms for Long-Term Monitoring Design."  Advances in
 * Water Resources, 29(6):792-807, 2006.
 * </ol>
 */
public class SSNSGAII extends AbstractAlgorithm implements EpsilonBoxEvolutionaryAlgorithm {
    private final Variation variation;
    private final Initialization initialization;
    private final IManagedPopulation<Solution> population;

    private final Comparator<RankedIndividual> comparator = new ChainedComparator<>(
            new RankComparator(),
            new CrowdingComparator<>()
    );

    public SSNSGAII(Problem problem, Variation variation, Initialization initialization, IManagedPopulation<Solution> population) {
        super(problem);
        //this.variation = variation;
        this.variation = new CompoundVariation(new SBX(1.0, 15.0),
                new PM(1.0 / problem.getNumberOfVariables(), 20.0));
        this.initialization = initialization;
        this.population = population;
    }

    @Override
    public void iterate() {
        try {
//            final long ts = System.currentTimeMillis();

            final Solution addend = generateOffspring();
            population.addIndividual(new FitnessAndCdIndividual<>(addend.getObjectives(), addend));

//            final long spent = System.currentTimeMillis() - ts;
//            if (spent > 4) {
//                System.out.println(spent);
//            }
        } catch (ArrayIndexOutOfBoundsException e) {
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                if (stackTraceElement.getClassName().contains(MersenneTwister.class.getSimpleName())) {
                    System.err.println("MersenneTwister has failed");
                    return;
                }
            }
            e.printStackTrace();
            throw e;
        }
    }

    private Solution generateOffspring() {
        try {
            final int mutationCandidatesCount = variation.getArity() * 2;
            final List<RankedIndividual<Solution>> mutationCandidates = population.getRandomSolutions(mutationCandidatesCount);
//        if (mutationCandidates.size() < mutationCandidatesCount) {
//            throw new RuntimeException("Failed to get enough mutation candidates: " + mutationCandidates);
//        }

            final RankedIndividual[] parents = new RankedIndividual[variation.getArity()];
            //System.err.println(mutationCandidates);
            for (int i = 0; i < mutationCandidates.size() - 1; i += 2) {
                final RankedIndividual<Solution> ind1 = mutationCandidates.get(i);
                final RankedIndividual<Solution> ind2 = mutationCandidates.get(i + 1);
                parents[i / 2] = Selection.binaryTournament(ind1, ind2, comparator);
            }

            final Solution[] parentSolutions = new Solution[variation.getArity()];
            for (int i = 0; i < variation.getArity(); ++i) {
                parentSolutions[i] = (Solution) parents[i].getPayload();
            }

            final Solution solution = variation.evolve(parentSolutions)[0];
            evaluate(solution);
            return solution;
        } catch (Exception e) {
            return generateOffspring();
        }
    }

    @Override
    protected void initialize() {
        super.initialize();

        final Solution[] initialSolutions = initialization.initialize();
        evaluateAll(initialSolutions);
        for (Solution s : initialSolutions) {
            population.addIndividual(new FitnessAndCdIndividual<>(s.getObjectives(), s));
            //System.out.println(Arrays.toString(s.getObjectives()));
        }
    }

    @Override
    public EpsilonBoxDominanceArchive getArchive() {
        throw new UnsupportedOperationException("Working with archive is not supported");
    }

    @Override
    public NondominatedSortingPopulation getPopulation() {
        throw new UnsupportedOperationException("Working with NondominatedPopulation is not supported");
    }

    @Override
    public NondominatedPopulation getResult() {
        throw new UnsupportedOperationException("Working with NondominatedPopulation is not supported");
    }
}
