package org.oxerr.ticket.inventory.support;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Listing on the sales platform.
 *
 * @param <I> the type of the listing ID.
 * @param <R> the type of the creating listing request.
 */
public class Listing<I extends Serializable, R extends Serializable> implements Serializable {

	private static final long serialVersionUID = 2023052301L;

	private I id;

	private R request;

	public Listing() {
	}

	public Listing(I id, R request) {
		this.id = id;
		this.request = request;
	}

	public I getId() {
		return id;
	}

	public void setId(I id) {
		this.id = id;
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
