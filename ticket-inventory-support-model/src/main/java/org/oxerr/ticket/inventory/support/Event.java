package org.oxerr.ticket.inventory.support;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * The event.
 *
 * @param <P> the type of the event ID.
 * @param <I> the type of the listing ID.
 * @param <R> the type of the request to create listing.
 * @param <L> the type of the {@link Listing}.
 */
public class Event<P extends Serializable, I extends Serializable, R extends Serializable, L extends Listing<I, R>> implements Serializable {

	private static final long serialVersionUID = 2023052301L;

	/**
	 * The event identifier.
	 */
	private P id;

	/**
	 * The date when the event starts.
	 */
	private OffsetDateTime startDate;

	private List<L> listings;

	public Event() {
		this(null, null, Collections.emptyList());
	}

	public Event(P id, OffsetDateTime startDate) {
		this(id, startDate, Collections.emptyList());
	}

	public Event(P id, OffsetDateTime startDate, List<L> listings) {
		this.id = id;
		this.startDate = startDate;
		this.listings = listings;
	}

	public P getId() {
		return id;
	}

	public void setId(P id) {
		this.id = id;
	}

	public OffsetDateTime getStartDate() {
		return startDate;
	}

	public void setStartDate(OffsetDateTime startDate) {
		this.startDate = startDate;
	}

	public List<L> getListings() {
		return listings;
	}

	public void setListings(List<L> listings) {
		this.listings = listings;
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
