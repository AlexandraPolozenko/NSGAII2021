package nds;

import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class PopulationSnapshot<T> {
    private final List<INonDominationLevel<T>> levels;
    private final int size;

    public PopulationSnapshot(List<INonDominationLevel<T>> levels, int size) {
        this.levels = levels;
        this.size = size;
    }

    public List<INonDominationLevel<T>> getLevels() {
        return this.levels;
    }

    public int getSize() {
        return this.size;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            PopulationSnapshot that = (PopulationSnapshot)o;
            return this.size == that.size && Objects.equals(this.levels, that.levels);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.levels, this.size});
    }

    public String toString() {
        return "PopulationSnapshot{levels=" + this.levels + ", size=" + this.size + '}';
    }
}

