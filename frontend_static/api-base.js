(function () {
  var localApiBase = "/api";
  var legacyPrefixes = [
    "http://localhost:8000/api",
    "https://poc-lineage-backend.vercel.app/api",
    "https://poc-lineage-backend.vercel.app",
  ];

  if (typeof window === "undefined" || typeof window.fetch !== "function") {
    return;
  }

  var originalFetch = window.fetch.bind(window);

  function rewriteUrl(url) {
    if (typeof url !== "string") {
      return url;
    }

    for (var i = 0; i < legacyPrefixes.length; i += 1) {
      var prefix = legacyPrefixes[i];
      if (url.startsWith(prefix)) {
        return localApiBase + url.slice(prefix.length);
      }
    }

    return url;
  }

  window.fetch = function (input, init) {
    if (typeof input === "string") {
      return originalFetch(rewriteUrl(input), init);
    }

    if (typeof Request !== "undefined" && input instanceof Request) {
      var rewrittenUrl = rewriteUrl(input.url);
      if (rewrittenUrl !== input.url) {
        return originalFetch(new Request(rewrittenUrl, input), init);
      }
    }

    return originalFetch(input, init);
  };
})();
