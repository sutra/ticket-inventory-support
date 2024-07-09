package org.oxerr.ticket.inventory.support.cached.redisson;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

	private final boolean create;

	protected final RedissonClient redisson;

	/**
	 * The key prefix for Redis entries.
	 */
	protected final String keyPrefix;

	protected final Executor executor;

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
	 * @param executor the executor
	 * @param create indicates if listings should be created.
	 */
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redisson,
		final String keyPrefix,
		final Executor executor,
		final boolean create
	) {
		this.redisson = redisson;
		this.keyPrefix = keyPrefix;
		this.executor = executor;
		this.create = create;
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
		cfs.addAll(this.delete(event, cache));

		// create/update
		if (this.create) {
			cfs.addAll(this.create(event, cache));
		}

		return CompletableFuture.allOf(cfs.toArray(CompletableFuture[]::new));
	}

	private List<CompletableFuture<Void>> create(final E event, final RMap<I, C> cache) {
		final List<L> pendingCreateListings = event.getListings()
			.stream()
			.filter(listing -> this.shouldCreate(event, listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingCreateListings
			.stream()
			.collect(Collectors.toMap(L::getId, v -> this.toPending(event, v)));

		cache.putAll(pendings);

		// create
		return pendingCreateListings.parallelStream()
			.map(
				listing -> this.createListingAsync(event, listing)
					.thenAccept(r -> cache.put(listing.getId(), this.toListed(event, listing)))
			).collect(Collectors.toList());
	}

	private List<CompletableFuture<Void>> delete(final E event, final RMap<I, C> cache) {
		final Set<I> inventoryTicketIds = event
			.getListings()
			.stream()
			.map(L::getId)
			.collect(Collectors.toSet());

		return cache
			.entrySet()
			.stream()
			.filter(t -> this.shouldDelete(event, inventoryTicketIds, t.getKey(), t.getValue()))
			.map(Map.Entry::getKey)
			.distinct()
			.map(ticketId -> this.deleteListingAsync(event, ticketId).thenAccept(r -> cache.remove(ticketId)))
			.collect(Collectors.toList());
	}

	protected boolean shouldCreate(
		@Nonnull final E event,
		@Nonnull final L listing,
		@Nullable final C cachedListing
	) {
		return cachedListing == null
			|| cachedListing.getStatus() == Status.PENDING_LIST
			|| !cachedListing.getRequest().equals(listing.getRequest());
	}

	protected boolean shouldDelete(
		@Nonnull final E event,
		@Nonnull final Set<I> inventoryTicketIds,
		@Nonnull final I ticketId,
		@Nonnull final C cachedListing
	) {
		return !inventoryTicketIds.contains(ticketId);
	}

	private C toPending(final E event, final L listing) {
		return toCached(event, listing, Status.PENDING_LIST);
	}

	private C toListed(final E event, final L listing) {
		return toCached(event, listing, Status.LISTED);
	}

	protected abstract C toCached(E event, L listing, Status status);

	protected CompletableFuture<Void> createListingAsync(E event, L listing) {
		return this.callAsync(() -> {
			this.createListing(event, listing);
			return null;
		});
	}

	protected CompletableFuture<Void> deleteListingAsync(E event, I ticketId) {
		return this.callAsync(() -> {
			this.deleteListing(event, ticketId);
			return null;
		});
	}

	protected abstract void createListing(E event, L listing) throws IOException;

	protected abstract void deleteListing(E event, I ticketId) throws IOException;

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

	protected RMap<I, C> getCache(final E event) {
		var name = this.getCacheName(event);
		var cache = this.getCache(name);
		cache.expire(this.getCacheExpireDate(event));
		return cache;
	}

	protected RMap<I, C> getCache(final String name) {
		return this.redisson.getMap(name);
	}

	protected Instant getCacheExpireDate(final E event) {
		return event.getStartDate().toInstant();
	}

	@Deprecated(since = "3.0.1", forRemoval = true)
	protected Instant cacheEndDate(final E event) {
		return this.getCacheExpireDate(event);
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
