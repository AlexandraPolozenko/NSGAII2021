package copypaste.jfby;

import copypaste.INonDominationLevel;
import copypaste.PopulationSnapshot;
import copypaste.sorter.JFB2014;
import ru.ifmo.nds.IIndividual;
import ru.ifmo.nds.util.RankedPopulation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class TotalSyncJFBYPopulation<T> extends JFBYPopulation<T> {
    public TotalSyncJFBYPopulation(long expectedPopSize) {
        super(expectedPopSize);
    }

    public TotalSyncJFBYPopulation(@Nonnull List<JFBYNonDominationLevel<T>> nonDominationLevels, long expectedPopSize) {
        super(nonDominationLevels, expectedPopSize);
    }

    public TotalSyncJFBYPopulation(@Nonnull JFB2014 sorter, long expectedPopSize) {
        super(sorter, expectedPopSize);
    }

    public TotalSyncJFBYPopulation(@Nonnull List<JFBYNonDominationLevel<T>> nonDominationLevels, @Nonnull JFB2014 sorter, long expectedPopSize) {
        super(nonDominationLevels, sorter, expectedPopSize);
    }

    @Nonnull
    @Override
    public synchronized PopulationSnapshot<T> getSnapshot() {
        final List<INonDominationLevel<T>> levels = new ArrayList<>();
        int size = 0;
        for (INonDominationLevel<T> level : super.getSnapshot().getLevels()) {
            levels.add(level.copy());
            size += level.getMembers().size();
        }
        return new PopulationSnapshot<>(levels, size);
    }

    @Nullable
    @Override
    synchronized IIndividual<T> intRemoveWorst() {
        return super.intRemoveWorst();
    }

    @Override
    public synchronized int size() {
        return super.size();
    }

    @Override
    public synchronized int addIndividual(@Nonnull IIndividual<T> addend) {
        return super.addIndividual(addend);
    }

    @Override
    public synchronized JFBYPopulation<T> clone() {
        return super.clone();
    }

    @Override
    public synchronized RankedPopulation<IIndividual<T>> toRankedPopulation() {
        return super.toRankedPopulation();
    }
}
