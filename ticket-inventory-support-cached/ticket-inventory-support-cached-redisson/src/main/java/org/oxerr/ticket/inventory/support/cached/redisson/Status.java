package org.oxerr.ticket.inventory.support.cached.redisson;

/**
 * Status of the {@link CachedListing}.
 */
public enum Status {

	PENDING_CREATE,

	PENDING_UPDATE,

	PENDING_DELETE,

	LISTED;

}
