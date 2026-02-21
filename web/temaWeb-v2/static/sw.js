self.addEventListener("install", (event) => { self.skipWaiting(); });
self.addEventListener("activate", (event) => { event.waitUntil(self.clients.claim()); });
self.addEventListener("fetch", (event) => {
  // Network-only to prevent stale assets while iterating.
  event.respondWith(fetch(event.request));
});
