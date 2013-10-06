package openworks.domaincheck;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Karthik Jayaraman
 * 
 */
public class BatchIterator<E> implements Iterable<Collection<E>>,
		Iterator<Collection<E>> {

	private Iterator<E> collectionIterator;
	private int batchSize;

	/**
	 * Returns a BatchIterator over the specified collection.
	 * 
	 * @param collection
	 *            the collection over which to iterate
	 * @param batchSize
	 *            the maximum size of each batch returned
	 */
	public BatchIterator(Collection<E> collection, int batchSize) {
		this.collectionIterator = collection.iterator();
		this.batchSize = batchSize;
	}

	/**
	 * Returns a BatchIterator over the specified collection. This is a
	 * convenience method to simplify the code need to loop over an existing
	 * collection.
	 * 
	 * @param collection
	 *            the collection over which to iterate
	 * @param batchSize
	 *            the maximum size of each batch returned
	 * @return a BatchIterator over the specified collection
	 */
	public static <E> BatchIterator<E> batches(Collection<E> collection,
			int batchSize) {
		return new BatchIterator<E>(collection, batchSize);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<Collection<E>> iterator() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		return collectionIterator.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */
	public Collection<E> next() {
		if (!collectionIterator.hasNext()) {
			throw new NoSuchElementException();
		}

		Collection<E> batch = new ArrayList<E>(batchSize);
		while ((batch.size() < batchSize) && collectionIterator.hasNext()) {
			batch.add(collectionIterator.next());
		}
		return batch;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#remove()
	 */
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
