# Changelog

## 4.5.0

- Add per-database write statistics and the optional `settings.captureWriteMetrics` callback.
- **Upgrade note:** NodeFire is now a peer dependency instead of a regular dependency. Applications
  must declare `nodefire: "^7.2.0"` (or a compatible 7.x version) in their own dependencies. If your
  application previously relied on Firelease to install NodeFire, add it explicitly before upgrading;
  package managers that do not install peers automatically will otherwise leave NodeFire missing,
  causing Firelease to fail at startup. This lets Firelease share the application's NodeFire instance
  and observe its writes.
