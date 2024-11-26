package org.oxerr.ticket.inventory.support.cached.redisson;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CachedListing<R extends Serializable> implements Serializable {

	private static final long serialVersionUID = 2023031801L;

	private R request;

	private Status status;

	public CachedListing() {
	}

	/**
	 * Constructor for backward compatibility.
	 *
	 * @param status status of the listing request.
	 * @param request request of the listing request.
	 * @deprecated Use {@link #CachedListing(R, Status)} instead.
	 */
	@Deprecated(since = "5.3.0", forRemoval = true)
	public CachedListing(Status status, R request) {
		this.status = status;
		this.request = request;
	}

	public CachedListing(R request, Status status) {
		this.request = request;
		this.status = status;
	}

	public R getRequest() {
		return request;
	}

	public void setRequest(R request) {
		this.request = request;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
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
