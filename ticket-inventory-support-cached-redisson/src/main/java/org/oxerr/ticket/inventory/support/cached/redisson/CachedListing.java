package org.oxerr.ticket.inventory.support.cached.redisson;

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.oxerr.ticket.inventory.support.Listing;

public class CachedListing<R extends Serializable> implements Serializable {

	private static final long serialVersionUID = 2023031801L;

	private Status status;

	private R request;

	public static <T extends Serializable> CachedListing<T> pending(Listing<T> listing) {
		return new CachedListing<>(Status.PENDING_LIST, listing.getRequest());
	}

	public static <T extends Serializable> CachedListing<T> listed(Listing<T> listing) {
		return new CachedListing<>(Status.LISTED, listing.getRequest());
	}

	public static <T extends Serializable> boolean shouldCreate(
		@Nonnull Listing<T> listing,
		@Nullable CachedListing<T> cachedListing) {
		return cachedListing == null
			|| cachedListing.getStatus() == Status.PENDING_LIST
			|| !cachedListing.getRequest().equals(listing.getRequest());
	}

	public CachedListing() {
	}

	public CachedListing(Status status, R request) {
		this.status = status;
		this.request = request;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public R getRequest() {
		return request;
	}

	public void setRequest(R request) {
		this.request = request;
	}

	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

}
