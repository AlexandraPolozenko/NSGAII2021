package nds;

import ru.ifmo.nds.IIndividual;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public interface INonDominationLevel<T> {
    List<IIndividual<T>> getMembers();

    List addMembers(@Nonnull List<IIndividual<T>> var1);

    boolean dominatedByAnyPointOfThisLayer(@Nonnull IIndividual<T> var1);

    INonDominationLevel<T> copy();

    public static class MemberAdditionResult<T1, L extends INonDominationLevel<T1>> implements List {
        private final List<IIndividual<T1>> evicted;
        private final L modifiedLevel;

        public MemberAdditionResult(List<IIndividual<T1>> evicted, L modifiedLevel) {
            this.evicted = evicted;
            this.modifiedLevel = modifiedLevel;
        }

        public List<IIndividual<T1>> getEvictedMembers() {
            return this.evicted;
        }

        public L getModifiedLevel() {
            return this.modifiedLevel;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(final Object o) {
            return false;
        }

        @Override
        public Iterator iterator() {
            return null;
        }

        @Override
        public void forEach(final Consumer action) {

        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public boolean add(final Object o) {
            return false;
        }

        @Override
        public boolean remove(final Object o) {
            return false;
        }

        @Override
        public boolean addAll(final Collection c) {
            return false;
        }

        @Override
        public boolean removeIf(final Predicate filter) {
            return false;
        }

        @Override
        public boolean addAll(final int index, final Collection c) {
            return false;
        }

        @Override
        public void replaceAll(final UnaryOperator operator) {

        }

        @Override
        public void sort(final Comparator c) {

        }

        @Override
        public void clear() {

        }

        @Override
        public Object get(final int index) {
            return null;
        }

        @Override
        public Object set(final int index, final Object element) {
            return null;
        }

        @Override
        public void add(final int index, final Object element) {

        }

        @Override
        public Object remove(final int index) {
            return null;
        }

        @Override
        public int indexOf(final Object o) {
            return 0;
        }

        @Override
        public int lastIndexOf(final Object o) {
            return 0;
        }

        @Override
        public ListIterator listIterator() {
            return null;
        }

        @Override
        public ListIterator listIterator(final int index) {
            return null;
        }

        @Override
        public List subList(final int fromIndex, final int toIndex) {
            return null;
        }

        @Override
        public Spliterator spliterator() {
            return null;
        }

        @Override
        public Stream stream() {
            return null;
        }

        @Override
        public Stream parallelStream() {
            return null;
        }

        @Override
        public boolean retainAll(final Collection c) {
            return false;
        }

        @Override
        public boolean removeAll(final Collection c) {
            return false;
        }

        @Override
        public boolean containsAll(final Collection c) {
            return false;
        }

        @Override
        public Object[] toArray(final Object[] a) {
            return new Object[0];
        }
    }
}