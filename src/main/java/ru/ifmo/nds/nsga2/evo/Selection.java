package ru.ifmo.nds.nsga2.evo;

import javax.annotation.Nonnull;
import java.util.Comparator;

public class Selection {
    public static <T> T binaryTournament(@Nonnull final T solution1,
                                         @Nonnull final T solution2,
                                         @Nonnull final Comparator<T> comparator) {
        int flag = comparator.compare(solution1, solution2);
        return flag > 0 ? solution2 : solution1;
    }
}
