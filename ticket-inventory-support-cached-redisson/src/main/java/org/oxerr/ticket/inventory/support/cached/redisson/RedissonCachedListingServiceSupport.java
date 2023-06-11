package org.oxerr.ticket.inventory.support.cached.redisson;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.oxerr.ticket.inventory.support.Event;
import org.oxerr.ticket.inventory.support.Listing;
import org.oxerr.ticket.inventory.support.cached.CachedListingService;
import org.redisson.api.RMapCache;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;

/**
 * The {@link CachedListingService} implementation using Redisson.
 *
 * @param <R> the type of the create listing request.
 * @param <L> the type of the {@link Listing}.
 * @param <E> the type of the {@link Event}.
 * @param <C> the type of the {@link CachedListing}.
 */
public abstract
	class RedissonCachedListingServiceSupport<
	R extends Serializable,
	L extends Listing<R>,
	E extends Event<R, L>,
	C extends CachedListing<R>
>
	implements CachedListingService<R, L, E> {

	private final Logger log = LogManager.getLogger();

	private final boolean create;

	private final RedissonClient redissonClient;

	/**
	 * The key prefix for Redis entries(lock & cache).
	 */
	private final String keyPrefix;

	/**
	 * The listing cache.
	 *
	 * The key is event ID,
	 * the value is a map, with listing ID as key, and cached listing as value.
	 * Event ID -> <Listing ID, CachedListing>
	 */
	private final RMapCache<String, Map<String, C>> listingsCache;

	/**
	 * Constructs {@link RedissonCachedListingServiceSupport}.
	 *
	 * @param redissonClient the {@link RedissonClient}.
	 * @param keyPrefix the key prefix for Redis entries(lock & cache).
	 * @param create indicates if listings should be created.
	 */
	protected RedissonCachedListingServiceSupport(
		final RedissonClient redissonClient,
		final String keyPrefix,
		final boolean create
	) {
		this.redissonClient = redissonClient;
		this.keyPrefix = keyPrefix;
		this.create = create;

		final String cacheName = String.format("%s:listings", keyPrefix);
		this.listingsCache = redissonClient.getMapCache(cacheName);
	}

	/**
	 * Returns the listing cache.
	 *
	 * @return the listing cache.
	 */
	public RMapCache<String, Map<String, C>> getListingCache() {
		return listingsCache;
	}

	/**
	 * Updates the listings of the events.
	 * 
	 * Deletes all listings that should be deleted,
	 * and create the listings that should be created.
	 */
	@Override
	public void updateListings(final E event) {
		final String lockName = String.format("%s:event:lock:%s", keyPrefix, event.getId());
		final RReadWriteLock rwLock = redissonClient.getReadWriteLock(lockName);

		rwLock.writeLock().lock();

		try {
			this.doUpdateEvent(event);
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	private void doUpdateEvent(final E event) {
		final Map<String, C> cache = this.getOrCreateCache(event);

		try {
			this.updateEvent(event, cache);
		} finally {
			this.saveCache(event, cache);
		}
	}

	private void updateEvent(final E event, final Map<String, C> cache) {
		// delete
		this.delete(event, cache);

		// create/update
		if (this.create) {
			this.create(event, cache);
		}
	}

	private void delete(final E event, final Map<String, C> cache) {
		final Set<String> inventoryTicketIds = event
			.getListings()
			.stream()
			.map(L::getId)
			.collect(Collectors.toSet());

		final Set<String> pendingDeleteTicketIds = cache
			.entrySet()
			.stream()
			.filter(t -> this.shouldDelete(event, inventoryTicketIds, t.getKey(), t.getValue()))
			.map(Map.Entry::getKey)
			.collect(Collectors.toSet());

		for (final String ticketId : pendingDeleteTicketIds) {
			log.trace("Deleting {}", ticketId);

			try {
				this.doDelete(ticketId);
				cache.remove(ticketId);
			} catch (IOException e) {
				log.warn("Delete ticket {} failed.", ticketId, e);
			}
		}
	}

	protected boolean shouldDelete(
		@Nonnull final E event,
		@Nonnull final Set<String> inventoryTicketIds,
		@Nonnull final String ticketId,
		@Nonnull final C cachedListing
	) {
		return !inventoryTicketIds.contains(ticketId);
	}

	protected abstract void doDelete(String ticketId) throws IOException;

	private void create(final E event, final Map<String, C> cache) {
		final List<L> pendingCreateListings = event.getListings()
			.stream()
			.filter(listing -> this.shouldCreate(event, listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		final Map<String, C> pendings = pendingCreateListings
			.stream()
			.collect(Collectors.toMap(L::getId, v -> this.toPending(event, v)));

		cache.putAll(pendings);

		// Make sure the cache is saved even if program is killed
		this.saveCache(event, cache);

		// create
		for (L listing : pendingCreateListings) {
			log.trace("Creating {}", listing.getId());

			try {
				this.doCreate(event, listing);
				cache.put(listing.getId(), this.toListed(event, listing));
			} catch (IOException e) {
				log.warn("Create ticket {} failed.", listing.getId(), e);
			}
		}
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

	protected abstract void doCreate(E event, L listing) throws IOException;

	private Map<String, C> getOrCreateCache(final E event) {
		Map<String, C> cache = this.listingsCache.get(event.getId());
		return Optional.ofNullable(cache).orElseGet(HashMap::new);
	}

	private void saveCache(final E event, final Map<String, C> listings) {
		final long ttl = ttl(event);
		this.listingsCache.fastPut(event.getId(), listings, ttl, TimeUnit.MINUTES);
	}

	private long ttl(final E event) {
		return Math.max(1, Duration.between(Instant.now(), this.cacheEndDate(event)).toMinutes());
	}

	protected Temporal cacheEndDate(final E event) {
		return event.getStartDate();
	}

	private C toPending(final E event, final L listing) {
		return toCached(event, listing, Status.PENDING_LIST);
	}

	private C toListed(final E event, final L listing) {
		return toCached(event, listing, Status.LISTED);
	}

	protected abstract C toCached(E event, L listing, Status status);

	@Override
	public long cacheSize() {
		return this.getListingCache().size();
	}

	@Override
	public long listedCount() {
		return countListings(this.getListingCache());
	}

	private long countListings(final RMapCache<String, Map<String, C>> listingsCache) {
		return listingsCache.values()
			.stream()
			.map((Map<String, C> listings) -> listings.values().stream().filter(l -> l.getStatus() == Status.LISTED).count())
			.reduce(0L, Long::sum);
	}

}
