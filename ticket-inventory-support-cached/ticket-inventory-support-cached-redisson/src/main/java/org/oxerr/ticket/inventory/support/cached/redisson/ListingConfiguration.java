package org.oxerr.ticket.inventory.support.cached.redisson;

/**
 * Listing configuration.
 *
 * @since 5.0.0
 */
public class ListingConfiguration {

	private final boolean create;

	private final boolean update;

	private final boolean delete;

	public ListingConfiguration() {
		this(true, true, true);
	}

	public ListingConfiguration(boolean create, boolean update, boolean delete) {
		this.create = create;
		this.update = update;
		this.delete = delete;
	}

	public boolean isCreate() {
		return create;
	}

	public boolean isUpdate() {
		return update;
	}

	public boolean isDelete() {
		return delete;
	}

}
