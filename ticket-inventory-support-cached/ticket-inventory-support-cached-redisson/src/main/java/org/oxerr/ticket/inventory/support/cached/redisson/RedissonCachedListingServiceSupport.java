package org.oxerr.ticket.inventory.support.cached.redisson;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.oxerr.ticket.inventory.support.Event;
import org.oxerr.ticket.inventory.support.Listing;
import org.oxerr.ticket.inventory.support.cached.CachedListingService;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;

/**
 * The {@link CachedListingService} implementation using Redisson.
 *
 * @param <P> the type of the event ID.
 * @param <I> the type of the listing ID.
 * @param <R> the type of the create listing request.
 * @param <L> the type of the {@link Listing}.
 * @param <E> the type of the {@link Event}.
 * @param <C> the type of the {@link CachedListing}.
 */
public abstract
	class RedissonCachedListingServiceSupport<
	P extends Serializable,
	I extends Serializable,
	R extends Serializable,
	L extends Listing<I, R>,
	E extends Event<P, I, R, L>,
	C extends CachedListing<R>
>
	implements CachedListingService<P, I, R, L, E> {

	private final Logger log = LogManager.getLogger();

	private final ListingConfiguration configuration;

	protected final RedissonClient redisson;

	/**
	 * The key prefix for Redis entries.
	 */
	protected final String keyPrefix;

	protected final Executor executor;

	/**
	 * Constructs {@link RedissonCachedListingServiceSupport}.
	 *
	 * @param redisson the {@link RedissonClient}.
	 * @param keyPrefix the key prefix for Redis entries.
	 * @param create indicates if listings should be created.
	 *
	 * @deprecated Use {@link #RedissonCachedListingServiceSupport(RedissonClient, String, Executor, ListingConfiguration)} instead.
	 */
	@Deprecated(forRemoval = true, since = "5.0.0")
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redisson,
		final String keyPrefix,
		final boolean create
	) {
		this(redisson, keyPrefix, ForkJoinPool.commonPool(), create);
	}

	/**
	 * Constructs {@link RedissonCachedListingServiceSupport}.
	 *
	 * @param redisson the {@link RedissonClient}.
	 * @param keyPrefix the key prefix for Redis entries.
	 * @param executor the executor.
	 * @param create indicates if listings should be created.
	 *
	 * @deprecated Use {@link #RedissonCachedListingServiceSupport(RedissonClient, String, Executor, ListingConfiguration)} instead.
	 */
	@Deprecated(forRemoval = true, since = "5.0.0")
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redisson,
		final String keyPrefix,
		final Executor executor,
		final boolean create
	) {
		this(redisson, keyPrefix, executor, new ListingConfiguration(create, true, true));
	}

	/**
	 * Constructs {@link RedissonCachedListingServiceSupport}
	 * with {@link ForkJoinPool#commonPool()} as the executor.
	 *
	 * @param redisson the {@link RedissonClient}.
	 * @param keyPrefix the key prefix for Redis entries.
	 * @param configuration the configuration.
	 *
	 * @since 5.0.0
	 */
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redisson,
		final String keyPrefix,
		final ListingConfiguration configuration
	) {
		this(redisson, keyPrefix, ForkJoinPool.commonPool(), configuration);
	}

	/**
	 * Constructs {@link RedissonCachedListingServiceSupport}.
	 *
	 * @param redisson the {@link RedissonClient}.
	 * @param keyPrefix the key prefix for Redis entries.
	 * @param executor the executor.
	 * @param configuration the configuration.
	 *
	 * @since 5.0.0
	 */
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redisson,
		final String keyPrefix,
		final Executor executor,
		final ListingConfiguration configuration
	) {
		this.redisson = redisson;
		this.keyPrefix = keyPrefix;
		this.executor = executor;
		this.configuration = configuration;
	}

	/**
	 * Updates the listings of the events.
	 *
	 * Deletes all listings that should be deleted,
	 * and create the listings that should be created.
	 */
	@Override
	public CompletableFuture<Void> updateListings(final E event) {
		var cache = this.getEventCache(event.getId());
		return this.updateEvent(event, cache);
	}

	private CompletableFuture<Void> updateEvent(final E event, final RMap<I, C> cache) {
		List<CompletableFuture<Void>> cfs = new ArrayList<>(cache.size());

		// delete
		if (this.configuration.isDelete()) {
			cfs.addAll(this.delete(event, cache));
		}

		// update
		if (this.configuration.isUpdate()) {
			cfs.addAll(this.update(event, cache));
		}

		// create
		if (this.configuration.isCreate()) {
			cfs.addAll(this.create(event, cache));
		}

		return CompletableFuture.allOf(cfs.toArray(CompletableFuture[]::new));
	}

	private class ListingDetails {

		private final E event;

		private final L listing;

		private final C cachedListing;

		public ListingDetails(E event, L listing, C cachedListing) {
			this.event = event;
			this.listing = listing;
			this.cachedListing = cachedListing;
		}

		public E getEvent() {
			return event;
		}

		public L getListing() {
			return listing;
		}

		public C getCachedListing() {
			return cachedListing;
		}

	}

	private List<CompletableFuture<Void>> create(final E event, final RMap<I, C> cache) {
		final List<ListingDetails> pendingReplaces = event.getListings().stream()
			.map(listing -> new ListingDetails(event, listing, cache.get(listing.getId())))
			.filter(details -> this.shouldCreate(details.getEvent(), details.getListing(), details.getCachedListing()))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingReplaces.stream()
			.map(ListingDetails::getListing)
			.collect(Collectors.toMap(L::getId, v -> this.toCached(event, v, Status.PENDING_CREATE)));

		cache.putAll(pendings);

		// create
		return pendingReplaces.stream()
			.map(
				entity -> this.createListingAsync(entity.getEvent(), entity.getListing(), this.getPriority(entity.getEvent(), entity.getListing(), entity.getCachedListing()))
					.thenAccept((Boolean r) -> {
						if (r.booleanValue()) {
							var listing = entity.getListing();
							cache.put(listing.getId(), this.toCached(event, listing, Status.LISTED));
						}
					})
			).collect(Collectors.toList());
	}

	private List<CompletableFuture<Void>> update(final E event, final RMap<I, C> cache) {
		final List<ListingDetails> pendingReplaces = event.getListings().stream()
			.map(listing -> new ListingDetails(event, listing, cache.get(listing.getId())))
			.filter(details -> this.shouldUpdate(details.event, details.getListing(), details.getCachedListing()))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingReplaces.stream()
			.map(ListingDetails::getListing)
			.collect(Collectors.toMap(L::getId, v -> this.toCached(event, v, Status.PENDING_UPDATE)));

		cache.putAll(pendings);

		// update
		return pendingReplaces.stream()
			.map(
				entity -> this.updateListingAsync(entity.getEvent(), entity.getListing(), this.getPriority(entity.getEvent(), entity.getListing(), entity.getCachedListing()))
					.thenAccept((Boolean r) -> {
						if (r.booleanValue()) {
							var listing = entity.getListing();
							cache.put(listing.getId(), this.toCached(event, listing, Status.LISTED));
						}
					})
			).collect(Collectors.toList());
	}

	private List<CompletableFuture<Void>> delete(final E event, final RMap<I, C> cache) {
		final Set<I> inventoryListingIds = event.getListings().stream()
			.map(L::getId).collect(Collectors.toSet());

		final Map<I, C> pendings = cache.entrySet().stream()
			.filter(t -> this.shouldDelete(event, inventoryListingIds, t.getKey(), t.getValue()))
			.collect(Collectors.toMap(Map.Entry::getKey, e -> {
				var v = e.getValue();
				v.setStatus(Status.PENDING_DELETE);
				return v;
			}));

		cache.putAll(pendings);

		// delete
		return pendings.entrySet().stream()
			.map(
				entry -> this.deleteListingAsync(event, entry.getKey(), this.getPriority(event, null, entry.getValue()))
					.thenAccept((Boolean r) -> {
						if (r.booleanValue()) {
							cache.remove(entry.getKey());
						}
					})
			).collect(Collectors.toList());
	}

	protected boolean shouldCreate(
		@Nonnull final E event,
		@Nonnull final L listing,
		@Nullable final C cachedListing
	) {
		// Cached is null or pending create.

		var shouldCreate = cachedListing == null || cachedListing.getStatus() == Status.PENDING_CREATE;

		log.trace("shouldCreate: event={}, listing={}, cachedListing={}, shouldCreate={}",
			event::getId,
			listing::getId,
			() -> Optional.ofNullable(cachedListing).map(CachedListing::getStatus).orElse(null),
			() -> shouldCreate
		);

		return shouldCreate;
	}

	/**
	 * Returns if should update this listing.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @param cachedListing the cached listing.
	 * @return true if should update.
	 *
	 * @since 5.0.0
	 */
	protected boolean shouldUpdate(
		@Nonnull final E event,
		@Nonnull final L listing,
		@Nullable final C cachedListing
	) {
		// Cached is null or pending update.
		if (cachedListing == null) {
			return false;
		}

		if (cachedListing.getStatus() == Status.PENDING_UPDATE) {
			return true;
		}

		var shouldUpdate = cachedListing.getStatus() == Status.LISTED && !cachedListing.getRequest().equals(listing.getRequest());

		log.trace("shouldUpdate: event={}, listing={}, cachedListing={}, shouldUpdate={}",
			event::getId, listing::getId, cachedListing::getStatus, () -> shouldUpdate);

		return shouldUpdate;
	}

	/**
	 * Returns the priority.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @param cachedListing the cached listing.
	 * @return the priority
	 * @since 5.1.0
	 */
	protected int getPriority(
		@Nonnull final E event,
		@Nullable final L listing,
		@Nullable final C cachedListing
	) {
		return 0;
	}

	protected boolean shouldDelete(
		@Nonnull final E event,
		@Nonnull final Set<I> inventoryListingIds,
		@Nonnull final I listingId,
		@Nonnull final C cachedListing
	) {
		// The listing ID is not in the cache.

		var shouldDelete = !inventoryListingIds.contains(listingId);

		log.trace("shouldDelete: event={}, inventoryListingIds.size={}, listingId={}, cachedListing={}, shouldDelete={}",
			event::getId, inventoryListingIds::size, () -> listingId, cachedListing::getStatus, () -> shouldDelete);

		return shouldDelete;
	}

	protected abstract C toCached(E event, L listing, Status status);

	private CompletableFuture<Boolean> createListingAsync(E event, L listing, int priority ) {
		return this.callAsync(() -> {
			if (Optional.ofNullable(this.getEventCache(event.getId()).get(listing.getId())).map(C::getStatus).orElse(null) == Status.PENDING_CREATE) {
				// If it is still in PENDING_CREATE status, create the listing.
				this.createListing(event, listing, priority );
				return true;
			} else {
				return false;
			}
		});
	}

	private CompletableFuture<Boolean> updateListingAsync(E event, L listing, int priority) {
		return this.callAsync(() -> {
			if (Optional.ofNullable(this.getEventCache(event.getId()).get(listing.getId())).map(C::getStatus).orElse(null) == Status.PENDING_UPDATE) {
				// If it is still in PENDING_UPDATE status, update the listing.
				this.updateListing(event, listing, priority);
				return true;
			} else {
				return false;
			}
		});
	}

	private CompletableFuture<Boolean> deleteListingAsync(E event, I listingId, int priority ) {
		return this.callAsync(() -> {
			this.deleteListing(event, listingId, priority);
			return true;
		});
	}

	protected abstract void createListing(E event, L listing) throws IOException;

	protected void createListing(E event, L listing, int priority) throws IOException {
		this.createListing(event, listing);
	}

	/**
	 * Update the listing.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @throws IOException indicates update failed.
	 *
	 * @since 5.0.0
	 */
	protected void updateListing(E event, L listing) throws IOException {
		this.createListing(event, listing);
	}

	/**
	 * Update the listing.
	 *
	 * @param event the event.
	 * @param listing the listing.
	 * @param priority the priority.
	 * @throws IOException indicates update failed.
	 *
	 * @since 5.1.0
	 */
	protected void updateListing(E event, L listing, int priority) throws IOException {
		this.createListing(event, listing, priority);
	}

	protected abstract void deleteListing(E event, I listingId) throws IOException;

	protected void deleteListing(E event, I listingId, int priority) throws IOException {
		this.deleteListing(event, listingId);
	}

	@Override
	public boolean isListed(E event, L listing) {
		return isListed(event, listing.getId());
	}

	@Override
	public boolean isListed(E event, I listingId) {
		return getCachedListing(event.getId(), listingId).map(C::getStatus).orElse(null) == Status.LISTED;
	}

	@Override
	public Optional<R> getRequest(E event, L listing) {
		return getRequest(event.getId(), listing);
	}

	@Override
	public Optional<R> getRequest(P eventId, L listing) {
		return getRequest(eventId, listing.getId());
	}

	@Override
	public Optional<R> getRequest(E event, I listingId) {
		return getRequest(event.getId(), listingId);
	}

	@Override
	public Optional<R> getRequest(P eventId, I listingId) {
		return getCachedListing(eventId, listingId).map(C::getRequest);
	}

	@Override
	public Optional<R> getListedRequest(E event, L listing) {
		return getListedRequest(event.getId(), listing);
	}

	@Override
	public Optional<R> getListedRequest(P eventId, L listing) {
		return getListedRequest(eventId, listing.getId());
	}

	@Override
	public Optional<R> getListedRequest(E event, I listingId) {
		return getListedRequest(event.getId(), listingId);
	}

	@Override
	public Optional<R> getListedRequest(P eventId, I listingId) {
		return getCachedListing(eventId, listingId).filter(c -> c.getStatus() == Status.LISTED).map(C::getRequest);
	}

	private Optional<C> getCachedListing(P eventId, I listingId) {
		return Optional.ofNullable(this.getEventCache(eventId)).map(c -> c.get(listingId));
	}

	@Deprecated(since = "5.2.0", forRemoval = true)
	protected RMap<I, C> getCache(final E event) {
		return getEventCache(event.getId());
	}

	/**
	 * Returns the cache for the event.
	 *
	 * @param eventId the event ID.
	 * @return the cache for the event.
	 *
	 * @since 5.2.0
	 */
	protected RMap<I, C> getEventCache(P eventId) {
		var name = this.getCacheName(eventId);
		return this.getCache(name);
	}

	protected RMap<I, C> getCache(final String name) {
		return this.redisson.getMap(name);
	}

	public Stream<String> getCacheNamesStream() {
		return this.getCacheNamesStream(10);
	}

	public Stream<String> getCacheNamesStream(int chunkSize) {
		var keyPattern = this.getCacheNamePattern();
		var keysScanOptions = KeysScanOptions.defaults().pattern(keyPattern).chunkSize(chunkSize);
		return this.redisson.getKeys().getKeysStream(keysScanOptions);
	}

	@Deprecated(since = "5.2.0", forRemoval = true)
	protected String getCacheName(final E event) {
		return getCacheName(event.getId());
	}

	protected String getCacheName(P eventId) {
		return String.format("%s:listings:%s", keyPrefix, eventId);
	}

	protected String getCacheNamePattern() {
		return String.format("%s:listings:*", this.keyPrefix);
	}

	@Override
	public long getCacheSize() {
		return this.getCacheNamesStream().count();
	}

	@Override
	public long getListedCount() {
		return this.getCacheNamesStream().map(key -> {
			RMap<I, C> cache = this.redisson.getMap(key);
			return cache.values().stream().filter(l -> l.getStatus() == Status.LISTED).count();
		}).reduce(0L, Long::sum);
	}

	protected <T> CompletableFuture<T> callAsync(Callable<T> callable) {
		return callAsync(callable, this.executor);
	}

	// org.springframework.util.concurrent.FutureUtils
	protected static <T> CompletableFuture<T> callAsync(Callable<T> callable, Executor executor) {
		CompletableFuture<T> result = new CompletableFuture<>();
		return result.completeAsync(toSupplier(callable, result), executor);
	}

	// org.springframework.util.concurrent.FutureUtils
	protected static <T> Supplier<T> toSupplier(Callable<T> callable, CompletableFuture<T> result) {
		return () -> {
			try {
				return callable.call();
			} catch (Exception ex) {
				// wrap the exception just like CompletableFuture::supplyAsync does
				result.completeExceptionally((ex instanceof CompletionException) ? ex : new CompletionException(ex));
				return null;
			}
		};
	}

}
