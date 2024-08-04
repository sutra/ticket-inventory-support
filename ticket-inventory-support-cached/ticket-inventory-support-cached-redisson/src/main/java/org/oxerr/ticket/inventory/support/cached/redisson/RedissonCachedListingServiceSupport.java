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

import org.oxerr.ticket.inventory.support.Event;
import org.oxerr.ticket.inventory.support.Listing;
import org.oxerr.ticket.inventory.support.cached.CachedListingService;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

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
		var cache = this.getCache(event);
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

	private List<CompletableFuture<Void>> create(final E event, final RMap<I, C> cache) {
		final List<L> pendingReplaceListings = event.getListings().stream()
			.filter(listing -> this.shouldCreate(event, listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingReplaceListings.stream()
			.collect(Collectors.toMap(L::getId, v -> this.toCached(event, v, Status.PENDING_CREATE)));

		cache.putAll(pendings);

		// create
		return pendingReplaceListings.stream()
			.map(
				listing -> this.createListingAsync(event, listing)
					.thenAccept((Boolean r) -> {
						if (r.booleanValue()) {
							cache.put(listing.getId(), this.toCached(event, listing, Status.LISTED));
						}
					})
			).collect(Collectors.toList());
	}

	private List<CompletableFuture<Void>> update(final E event, final RMap<I, C> cache) {
		final List<L> pendingReplaceListings = event.getListings().stream()
			.filter(listing -> this.shouldUpdate(event, listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingReplaceListings.stream()
			.collect(Collectors.toMap(L::getId, v -> this.toCached(event, v, Status.PENDING_UPDATE)));

		cache.putAll(pendings);

		// update
		return pendingReplaceListings.stream()
			.map(
				listing -> this.updateListingAsync(event, listing)
					.thenAccept((Boolean r) -> {
						if (r.booleanValue()) {
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
		return pendings.entrySet().stream().map(Map.Entry::getKey).distinct()
			.map(listingId -> this.deleteListingAsync(event, listingId).thenAccept(r -> cache.remove(listingId)))
			.collect(Collectors.toList());
	}

	protected boolean shouldCreate(
		@Nonnull final E event,
		@Nonnull final L listing,
		@Nullable final C cachedListing
	) {
		// Cached is null or pending create.
		return cachedListing == null || cachedListing.getStatus() == Status.PENDING_CREATE;
	}

	/**
	 * Returns if should update this listing.
	 *
	 * @return true if should update.
	 *
	 * @since 5.0.0
	 */
	protected boolean shouldUpdate(
		@Nonnull final E event,
		@Nonnull final L listing,
		@Nullable final C cachedListing
	) {
		if (cachedListing == null) {
			return false;
		}

		if (cachedListing.getStatus() == Status.PENDING_UPDATE) {
			return true;
		}

		return cachedListing.getStatus() == Status.LISTED && !cachedListing.getRequest().equals(listing.getRequest());
	}

	protected boolean shouldDelete(
		@Nonnull final E event,
		@Nonnull final Set<I> inventoryListingIds,
		@Nonnull final I listingId,
		@Nonnull final C cachedListing
	) {
		// The listing ID is not in the cache.
		return !inventoryListingIds.contains(listingId);
	}

	protected abstract C toCached(E event, L listing, Status status);

	private CompletableFuture<Boolean> createListingAsync(E event, L listing) {
		return this.callAsync(() -> {
			if (Optional.ofNullable(this.getCache(event).get(listing.getId())).map(C::getStatus).orElse(null) == Status.PENDING_CREATE) {
				// If it is still in PENDING_CREATE status, create the listing.
				this.createListing(event, listing);
				return true;
			} else {
				return false;
			}
		});
	}

	private CompletableFuture<Boolean> updateListingAsync(E event, L listing) {
		return this.callAsync(() -> {
			if (Optional.ofNullable(this.getCache(event).get(listing.getId())).map(C::getStatus).orElse(null) == Status.PENDING_UPDATE) {
				// If it is still in PENDING_UPDATE status, update the listing.
				this.updateListing(event, listing);
				return true;
			} else {
				return false;
			}
		});
	}

	private CompletableFuture<Boolean> deleteListingAsync(E event, I listingId) {
		return this.callAsync(() -> {
			this.deleteListing(event, listingId);
			return true;
		});
	}

	protected abstract void createListing(E event, L listing) throws IOException;

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

	protected abstract void deleteListing(E event, I listingId) throws IOException;

	@Override
	public boolean isListed(E event, L listing) {
		return isListed(event, listing.getId());
	}

	@Override
	public boolean isListed(E event, I listingId) {
		return getCachedListing(event, listingId).map(C::getStatus).orElse(null) == Status.LISTED;
	}

	@Override
	public Optional<R> getRequest(E event, L listing) {
		return getRequest(event, listing.getId());
	}

	@Override
	public Optional<R> getRequest(E event, I listingId) {
		return getCachedListing(event, listingId).map(C::getRequest);
	}

	@Override
	public Optional<R> getListedRequest(E event, L listing) {
		return getListedRequest(event, listing.getId());
	}

	@Override
	public Optional<R> getListedRequest(E event, I listingId) {
		return getCachedListing(event, listingId).filter(c -> c.getStatus() == Status.LISTED).map(C::getRequest);
	}

	private Optional<C> getCachedListing(E event, I listingId) {
		return Optional.ofNullable(this.getCache(event)).map(c -> c.get(listingId));
	}

	protected RMap<I, C> getCache(final E event) {
		var name = this.getCacheName(event);
		return this.getCache(name);
	}

	protected RMap<I, C> getCache(final String name) {
		return this.redisson.getMap(name);
	}

	public Stream<String> getCacheNamesStream() {
		return this.getCacheNamesStream(10);
	}

	public Stream<String> getCacheNamesStream(int count) {
		var keyPattern = this.getCacheNamePattern();
		return this.redisson.getKeys().getKeysStreamByPattern(keyPattern, count);
	}

	protected String getCacheName(final E event) {
		return String.format("%s:listings:%s", keyPrefix, event.getId());
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
