import type { NextConfig } from "next";

// A static export: every page is HTML built once and served from cache by
// the edge. Pages read their IDs from the query string (/event?id=...) and
// fetch live state from the API on the same origin, so one cached copy of
// each page serves every event and every buyer.
const nextConfig: NextConfig = {
  output: "export",
  reactStrictMode: true,
  poweredByHeader: false,
};

export default nextConfig;
