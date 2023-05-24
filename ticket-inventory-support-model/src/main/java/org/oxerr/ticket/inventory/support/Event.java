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
 * @param <R> the type of the request to create listing.
 * @param <L> the type of the {@link Listing}.
 */
public class Event<R extends Serializable, L extends Listing<R>> implements Serializable {

	private static final long serialVersionUID = 2023052301L;

	/**
	 * The event identifier.
	 */
	private String id;

	/**
	 * The date when the event starts.
	 */
	private OffsetDateTime startDate;

	private List<L> listings;

	public Event() {
		this(null, null, Collections.emptyList());
	}

	public Event(String id, OffsetDateTime startDate) {
		this(id, startDate, Collections.emptyList());
	}

	public Event(String id, OffsetDateTime startDate, List<L> listings) {
		this.id = id;
		this.startDate = startDate;
		this.listings = listings;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
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
