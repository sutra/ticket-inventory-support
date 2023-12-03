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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

	private final RedissonClient redisson;

	/**
	 * The key prefix for Redis entries.
	 */
	private final String keyPrefix;

	private final Executor executor;

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
		return this.doUpdateEvent(event);
	}

	private CompletableFuture<Void> doUpdateEvent(final E event) {
		final ConcurrentMap<I, C> eventCache = this.getEventCache(event);
		return this.updateEvent(event, eventCache);
	}

	private CompletableFuture<Void> updateEvent(final E event, final ConcurrentMap<I, C> eventCache) {
		List<CompletableFuture<Void>> cfs = new ArrayList<>(eventCache.size());

		// delete
		cfs.addAll(this.delete(event, eventCache));

		// create/update
		if (this.create) {
			cfs.addAll(this.create(event, eventCache));
		}

		return CompletableFuture.allOf(cfs.toArray(CompletableFuture[]::new));
	}

	private List<CompletableFuture<Void>> delete(final E event, final ConcurrentMap<I, C> eventCache) {
		final Set<I> inventoryTicketIds = event
			.getListings()
			.stream()
			.map(L::getId)
			.collect(Collectors.toSet());

		return eventCache
			.entrySet()
			.stream()
			.filter(t -> this.shouldDelete(event, inventoryTicketIds, t.getKey(), t.getValue()))
			.map(Map.Entry::getKey)
			.distinct()
			.map(ticketId -> this.deleteListingAsync(event, ticketId).thenAccept(r -> eventCache.remove(ticketId)))
			.collect(Collectors.toList());
	}

	protected boolean shouldDelete(
		@Nonnull final E event,
		@Nonnull final Set<I> inventoryTicketIds,
		@Nonnull final I ticketId,
		@Nonnull final C cachedListing
	) {
		return !inventoryTicketIds.contains(ticketId);
	}

	private List<CompletableFuture<Void>> create(final E event, final ConcurrentMap<I, C> eventCache) {
		final List<L> pendingCreateListings = event.getListings()
			.stream()
			.filter(listing -> this.shouldCreate(event, listing, eventCache.get(listing.getId())))
			.collect(Collectors.toList());

		final Map<I, C> pendings = pendingCreateListings
			.stream()
			.collect(Collectors.toMap(L::getId, v -> this.toPending(event, v)));

		eventCache.putAll(pendings);

		// create
		return pendingCreateListings.parallelStream()
			.map(
				listing -> this.createListingAsync(event, listing)
					.thenAccept(r -> eventCache.put(listing.getId(), this.toListed(event, listing)))
			).collect(Collectors.toList());
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

	private RMap<I, C> getEventCache(final E event) {
		var name = String.format("%s:listings:%s", keyPrefix, event.getId());
		RMap<I, C> eventCache = this.redisson.getMap(name);
		eventCache.expire(this.cacheEndDate(event));
		return eventCache;
	}

	protected Instant cacheEndDate(final E event) {
		return event.getStartDate().toInstant();
	}

	private C toPending(final E event, final L listing) {
		return toCached(event, listing, Status.PENDING_LIST);
	}

	private C toListed(final E event, final L listing) {
		return toCached(event, listing, Status.LISTED);
	}

	protected abstract void deleteListing(E event, I ticketId) throws IOException;

	protected abstract void createListing(E event, L listing) throws IOException;

	protected CompletableFuture<Void> deleteListingAsync(E event, I ticketId) {
		return callAsync(() -> {
			this.deleteListing(event, ticketId);
			return null;
		}, this.executor);
	}

	protected CompletableFuture<Void> createListingAsync(E event, L listing) {
		return callAsync(() -> {
			this.createListing(event, listing);
			return null;
		}, this.executor);
	}

	// org.springframework.util.concurrent.FutureUtils
	private static <T> CompletableFuture<T> callAsync(Callable<T> callable, Executor executor) {
		CompletableFuture<T> result = new CompletableFuture<>();
		return result.completeAsync(toSupplier(callable, result), executor);
	}

	// org.springframework.util.concurrent.FutureUtils
	private static <T> Supplier<T> toSupplier(Callable<T> callable, CompletableFuture<T> result) {
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

	protected abstract C toCached(E event, L listing, Status status);

	@Override
	public long cacheSize() {
		var keyPattern = this.getKeyPattern();
		return this.redisson.getKeys().getKeysStreamByPattern(keyPattern).count();
	}

	@Override
	public long listedCount() {
		var keyPattern = this.getKeyPattern();
		return this.redisson.getKeys().getKeysStreamByPattern(keyPattern).map(key -> {
			RMap<I, C> cache = this.redisson.getMap(key);
			return cache.values().stream().filter(l -> l.getStatus() == Status.LISTED).count();
		}).reduce(0L, Long::sum);
	}

	private String getKeyPattern() {
		return String.format("%s:listings:*", this.keyPrefix);
	}

}
