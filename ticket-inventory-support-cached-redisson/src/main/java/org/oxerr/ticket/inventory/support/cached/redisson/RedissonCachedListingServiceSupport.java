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
	E extends Event<R, L>
>
	implements CachedListingService<R, L, E> {

	private final Logger log = LogManager.getLogger();

	private final RedissonClient redisson;

	/**
	 * The key prefix for Redis entries(lock & cache).
	 */
	private final String keyPrefix;

	// Event ID -> <Listing ID, CachedListing>
	private final RMapCache<String, Map<String, CachedListing<R>>> listingsCache;

	protected RedissonCachedListingServiceSupport(RedissonClient redisson, String keyPrefix) {
		this.redisson = redisson;
		this.keyPrefix = keyPrefix;

		String cacheName = String.format("%s:listings", keyPrefix);
		this.listingsCache = redisson.getMapCache(cacheName);
	}

	public RMapCache<String, Map<String, CachedListing<R>>> getListingCache() {
		return listingsCache;
	}

	public void updateEvent(E event) {
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
		Map<String, CachedListing<R>> cache = this.getOrCreateCache(event);

		try {
			this.updateEvent(event, cache);
		} finally {
			this.saveCache(event, cache);
		}
	}

	private void updateEvent(E event, Map<String, CachedListing<R>> cache) {
		// delete
		this.delete(event, cache);

		// create/update
		this.create(event, cache);
	}

	private void delete(E event, Map<String, CachedListing<R>> cache) {
		Set<String> inventoryTicketIds = event.getListings()
			.stream()
			.map(L::getId)
			.collect(Collectors.toSet());

		Set<String> pendingDeleteTicketIds = cache.keySet()
			.stream()
			.filter(t -> !inventoryTicketIds.contains(t))
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

	protected abstract void doDelete(String ticketId) throws IOException;

	private void create(E event, Map<String, CachedListing<R>> cache) {
		List<L> pendingCreateListings = event.getListings()
			.stream()
			.filter(listing -> CachedListing.shouldCreate(listing, cache.get(listing.getId())))
			.collect(Collectors.toList());

		Map<String, CachedListing<R>> pendings = pendingCreateListings
			.stream()
			.collect(Collectors.toMap(L::getId, CachedListing::pending));

		cache.putAll(pendings);

		// Make sure the cache is saved even if program is killed
		this.saveCache(event, cache);

		// create
		for (L listing : pendingCreateListings) {
			log.trace("Creating {}", listing.getId());

			try {
				this.doCreate(listing);
				cache.put(listing.getId(), CachedListing.listed(listing));
			} catch (IOException e) {
				log.warn("Create ticket {} failed.", listing.getId(), e);
			}
		}
	}

	protected abstract void doCreate(L listing) throws IOException;

	private Map<String, CachedListing<R>> getOrCreateCache(E event) {
		Map<String, CachedListing<R>> cache = this.listingsCache.get(event.getId());
		return Optional.ofNullable(cache).orElseGet(HashMap::new);
	}

	private void saveCache(E event, Map<String, CachedListing<R>> listings) {
		long ttl = ttl(event);
		this.listingsCache.fastPut(event.getId(), listings, ttl, TimeUnit.MINUTES);
	}

	private long ttl(E event) {
		return Math.max(1, Duration.between(Instant.now(), this.cacheEnd(event)).toMinutes());
	}

	protected Temporal cacheEnd(E event) {
		return event.getStartDate();
	}

	@Override
	public long cacheSize() {
		return this.getListingCache().size();
	}

	@Override
	public long listedCount() {
		return countListings(this.getListingCache());
	}

	private long countListings(RMapCache<String, Map<String, CachedListing<R>>> listingsCache) {
		return listingsCache.values()
			.stream()
			.map((Map<String, CachedListing<R>> listings) -> listings.values().stream().filter(l -> l.getStatus() == Status.LISTED).count())
			.reduce(0L, Long::sum);
	}

}
