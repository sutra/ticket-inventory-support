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

public abstract
	class RedissonCachedListingServiceSupport<
	R extends Serializable,
	L extends Listing<R>,
	E extends Event<R, L>,
	C extends CachedListing<R>
>
	implements CachedListingService<R, L, E> {

	private final Logger log = LogManager.getLogger();

	private final RedissonClient redisson;

	/**
	 * The key prefix for Redis entries(lock & cache).
	 */
	private final String keyPrefix;

	// Event ID -> <Listing ID, CachedListing>
	private final RMapCache<String, Map<String, C>> listingsCache;

	protected RedissonCachedListingServiceSupport(RedissonClient redisson, String keyPrefix) {
		this.redisson = redisson;
		this.keyPrefix = keyPrefix;

		String cacheName = String.format("%s:listings", keyPrefix);
		this.listingsCache = redisson.getMapCache(cacheName);
	}

	public RMapCache<String, Map<String, C>> getListingCache() {
		return listingsCache;
	}

	public void updateListings(E event) {
		String lockName = String.format("%s:event:lock:%s", keyPrefix, event.getId());
		RReadWriteLock rwLock = redisson.getReadWriteLock(lockName);

		rwLock.writeLock().lock();

		try {
			this.doUpdateEvent(event);
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	private void doUpdateEvent(E event) {
		Map<String, C> cache = this.getOrCreateCache(event);

		try {
			this.updateEvent(event, cache);
		} finally {
			this.saveCache(event, cache);
		}
	}

	private void updateEvent(E event, Map<String, C> cache) {
		// delete
		this.delete(event, cache);

		// create/update
		this.create(event, cache);
	}

	private void delete(E event, Map<String, C> cache) {
		Set<String> inventoryTicketIds = event
			.getListings()
			.stream()
			.map(L::getId)
			.collect(Collectors.toSet());

		Set<String> pendingDeleteTicketIds = cache
			.entrySet()
			.stream()
			.filter(t -> this.shouldDelete(event, inventoryTicketIds, t.getKey(), t.getValue()))
			.map(Map.Entry::getKey)
			.collect(Collectors.toSet());

		for (String ticketId : pendingDeleteTicketIds) {
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
		@Nonnull E event,
		@Nonnull Set<String> inventoryTicketIds,
		@Nonnull String ticketId,
		@Nonnull C cachedListing
	) {
		return !inventoryTicketIds.contains(ticketId);
	}

	protected abstract void doDelete(String ticketId) throws IOException;

	private void create(E event, Map<String, C> cache) {
		List<L> pendingCreateListings = event.getListings()
			.stream()
			.filter(listing -> this.shouldCreate(event, listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		Map<String, C> pendings = pendingCreateListings
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

	protected boolean shouldCreate(@Nonnull E event, @Nonnull L listing, @Nullable C cachedListing) {
		return cachedListing == null
			|| cachedListing.getStatus() == Status.PENDING_LIST
			|| !cachedListing.getRequest().equals(listing.getRequest());
	}

	protected abstract void doCreate(E event, L listing) throws IOException;

	private Map<String, C> getOrCreateCache(E event) {
		Map<String, C> cache = this.listingsCache.get(event.getId());
		return Optional.ofNullable(cache).orElseGet(HashMap::new);
	}

	private void saveCache(E event, Map<String, C> listings) {
		long ttl = ttl(event);
		this.listingsCache.fastPut(event.getId(), listings, ttl, TimeUnit.MINUTES);
	}

	private long ttl(E event) {
		return Math.max(1, Duration.between(Instant.now(), this.cacheEndDate(event)).toMinutes());
	}

	protected Temporal cacheEndDate(E event) {
		return event.getStartDate();
	}

	private C toPending(E event, L listing) {
		return toCached(event, listing, Status.PENDING_LIST);
	}

	private C toListed(E event, L listing) {
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

	private long countListings(RMapCache<String, Map<String, C>> listingsCache) {
		return listingsCache.values()
			.stream()
			.map((Map<String, C> listings) -> listings.values().stream().filter(l -> l.getStatus() == Status.LISTED).count())
			.reduce(0L, Long::sum);
	}

}
