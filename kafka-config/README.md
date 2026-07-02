# Kafka Configuration

This folder documents the 3 Kafka topics used in this project — see `topics.yml` for the
names, partition counts, and which service reads/writes each one. (Same design explained
in [`docs/architecture.md`](../docs/architecture.md), just laid out here for quick
reference.)

The topics aren't created from this file — they're created by the `kafka-init` service in
[`docker-compose.yml`](../docker-compose.yml), which runs `kafka-topics --create` for each
one on startup. `topics.yml` just describes what that script does, in one readable place.
If you change one, update the other.

Broker settings like listener addresses and retention stay in `docker-compose.yml` as
environment variables, not here — those are about deploying the broker, not about what the
topics are for.
