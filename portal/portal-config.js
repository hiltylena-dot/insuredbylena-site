(() => {
  const hostname = window.location.hostname || "";
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost";
  const productionApi = "https://insuredbylena-portal-api-607620457436.us-central1.run.app";
  window.PORTAL_CONFIG = {
    ...(window.PORTAL_CONFIG || {}),
    ...(isLocalHost ? {} : { apiBase: productionApi }),
  };
})();
