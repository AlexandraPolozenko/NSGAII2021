package copypaste;

import ru.ifmo.nds.IIndividual;

import javax.annotation.Nonnull;
import java.util.List;

public interface INonDominationLevel<T> {
    class MemberAdditionResult<T1, L extends INonDominationLevel<T1>> {
        private final List<IIndividual<T1>> evicted;
        private final L modifiedLevel;

        public MemberAdditionResult(List<IIndividual<T1>> evicted, L modifiedLevel) {
            this.evicted = evicted;
            this.modifiedLevel = modifiedLevel;
        }

        public List<IIndividual<T1>> getEvictedMembers() {
            return evicted;
        }

        public L getModifiedLevel() {
            return modifiedLevel;
        }

    }

    /**
     * @return Lexicographically sorted members of this layer with respective CD
     */
    List<IIndividual<T>> getMembers();

    /**
     * Add new points (assuming that their ranks equal the rank of this level).
     *
     * @param addends New points
     * @return A set of evicted points that should be moved to the next level
     */
    INonDominationLevel.MemberAdditionResult addMembers(@Nonnull List<IIndividual<T>> addends);

    /**
     * @return true if {@code point} is dominated by any member of this layer
     */
    boolean dominatedByAnyPointOfThisLayer(@Nonnull IIndividual<T> point);

    /**
     * @return Shallow copy of this layer
     */
    INonDominationLevel<T> copy();
}

