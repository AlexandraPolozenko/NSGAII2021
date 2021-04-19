package ru.ifmo.nds.nsga2.evo;

import ru.ifmo.nds.IIndividual;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;

public class CrowdingComparator<T extends IIndividual> implements Comparator<T>, Serializable {
    public int compare(@Nonnull final T solution1, @Nonnull final T solution2) {
        double crowding1 = solution1.getCrowdingDistance();
        double crowding2 = solution2.getCrowdingDistance();
        if (crowding1 > crowding2) {
            return -1;
        } else {
            return crowding1 < crowding2 ? 1 : 0;
        }
    }
}

