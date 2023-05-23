package org.oxerr.ticket.inventory.support.cached;

import java.io.Serializable;

import org.oxerr.ticket.inventory.support.Event;
import org.oxerr.ticket.inventory.support.Listing;

public interface CachedListingService<
	R extends Serializable,
	L extends Listing<R>,
	E extends Event<R, L>
> {

	void updateEvent(E event);

	/**
	 * Returns the size of the cache.
	 *
	 * @return the size of the cache.
	 */
	long cacheSize();

	/**
	 * Returns the listing count which status is LISTED.
	 *
	 * @return the listing count which status is LISTED.
	 */
	long listedCount();

}
