package org.oxerr.ticket.inventory.support.cached;

import java.io.Serializable;
import java.util.Optional;
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
	 * Returns {@code true} if, and only if, the listing is listed.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @return {@code true} if the listing is listed, otherwise {@code false}.
	 *
	 * @since 4.1.0
	 */
	boolean isListed(E event, L listing);

	/**
	 * Returns {@code true} if, and only if, the listing is listed.
	 *
	 * @param event the event.
	 * @param listingId the {@link Listing#getId()}.
	 * @return {@code true} if the listing is listed, otherwise {@code false}.
	 *
	 * @since 4.4.0
	 */
	boolean isListed(E event, I listingId);

	/**
	 * Returns the request of the listing.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 4.2.0
	 */
	@Deprecated(since = "5.2.0", forRemoval = true)
	Optional<R> getRequest(E event, L listing);

	/**
	 * Returns the request of the listing.
	 *
	 * @param eventId the event ID.
	 * @param listing the listing.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 5.2.0
	 */
	Optional<R> getRequest(P eventId, L listing);

	/**
	 * Returns the request of the listing.
	 *
	 * @param event the event.
	 * @param listingId the {@link Listing#getId()}.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 4.4.0
	 */
	@Deprecated(since = "5.2.0", forRemoval = true)
	Optional<R> getRequest(E event, I listingId);

	/**
	 * Returns the request of the listing.
	 *
	 * @param eventid the event ID.
	 * @param listingId the listing ID.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 5.2.0
	 */
	Optional<R> getRequest(P eventId, I listingId);

	/**
	 * Returns the request of the listing which is listed.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 4.3.0
	 */
	@Deprecated(since = "5.2.0", forRemoval = true)
	Optional<R> getListedRequest(E event, L listing);

	/**
	 * Returns the request of the listing which is listed.
	 *
	 * @param eventId the event ID.
	 * @param listing the listing.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 5.2.0
	 */
	Optional<R> getListedRequest(P eventId, L listing);

	/**
	 * Returns the request of the listing which is listed.
	 *
	 * @param event the event.
	 * @param listingId the {@link Listing#getId()}.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 4.4.0
	 */
	@Deprecated(since = "5.2.0", forRemoval = true)
	Optional<R> getListedRequest(E event, I listingId);

	/**
	 * Returns the request of the listing which is listed.
	 *
	 * @param eventId the event ID.
	 * @param listingId the listing ID.
	 * @return the request.
	 * @see Listing#getRequest()
	 *
	 * @since 5.2.0
	 */
	Optional<R> getListedRequest(P eventId, I listingId);

	/**
	 * Gets all cache names using Stream.
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

	/**
	 * Returns the listing count which status is listed.
	 *
	 * @return the listing count which status is listed.
	 */
	long getListedCount();

}
