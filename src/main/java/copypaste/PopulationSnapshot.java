package copypaste;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class PopulationSnapshot<T> {
    private final List<INonDominationLevel<T>> levels;
    private final int size;

    public PopulationSnapshot(List<INonDominationLevel<T>> levels, int size) {
        this.levels = levels;
        this.size = size;
    }

    public List<INonDominationLevel<T>> getLevels() {
        return levels;
    }

    public int getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PopulationSnapshot that = (PopulationSnapshot) o;
        return size == that.size &&
                Objects.equals(levels, that.levels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(levels, size);
    }

    @Override
    public String toString() {
        return "PopulationSnapshot{" + "levels=" + levels +
                ", size=" + size +
                '}';
    }
}
