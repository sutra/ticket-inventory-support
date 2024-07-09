package org.oxerr.ticket.inventory.support.cached;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.oxerr.ticket.inventory.support.Event;
import org.oxerr.ticket.inventory.support.Listing;

/**
 * Listing services.
 *
 * @param <P> the type of the event ID.
 * @param <I> the type of the listing ID.
 * @param <R> the type of create listing request.
 * @param <L> the type of {@link Listing}.
 * @param <E> the type of {@link Event}.
 */
public interface CachedListingService<
	P extends Serializable,
	I extends Serializable,
	R extends Serializable,
	L extends Listing<I, R>,
	E extends Event<P, I, R, L>
> {

	/**
	 * Updates the listings of the event.
	 *
	 * Deletes all listings that should be deleted, creates/updates all listings
	 * that should be created/updated.
	 *
	 * @param event the event.
	 * @return the result.
	 */
	CompletableFuture<Void> updateListings(E event);

	/**
	 * Get all cache names using Stream.
	 *
	 * @return the cache names.
	 */
	Stream<String> getCacheNamesStream();

	/**
	 * Gets all cache names using Stream.
	 *
	 * @param count keys loaded per request to cache system.
	 * @return the cache names.
	 */
	Stream<String> getCacheNamesStream(int count);

	/**
	 * Returns the size of the cache.
	 *
	 * @return the size of the cache.
	 */
	long getCacheSize();

	@Deprecated(since = "3.0.1", forRemoval = true)
	default long cacheSize() {
		return this.getCacheSize();
	}

	/**
	 * Returns the listing count which status is LISTED.
	 *
	 * @return the listing count which status is LISTED.
	 */
	long getListedCount();

	@Deprecated(since = "3.0.1", forRemoval = true)
	default long listedCount() {
		return this.getListedCount();
	}

}
