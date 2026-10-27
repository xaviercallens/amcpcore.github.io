# AMCP Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `EventBroker.subscribeQueue(topicPattern, queueName, handler)`: competing-consumer subscriptions. Each event is
  delivered to one subscriber per queue name. The default implementation falls back to `subscribe`, so existing
  brokers keep working. `InMemoryEventBroker` implements it round-robin.
- `KafkaEventBroker` constructor `(bootstrap, groupId, instanceId, java.util.Properties extra)`. The extra properties
  apply to the producer and to every consumer. The 3-argument constructor is unchanged.
- `KafkaEventBroker` now uses the structured-mode CloudEvents JSON wire format, so topic, id, time, source and
  payload survive the round trip. A malformed record is logged and skipped.

### Fixed

- Kafka topic names are now the AMCP topic strings verbatim, for both publish and subscribe. Previously publish used
  `a.b.c` and subscribe used `a-b-c`, so the two never met.
- A `KafkaEventBroker` consumer now keeps all its subscriptions. A new subscribe no longer replaces earlier ones.
- Wildcard subscriptions use a Kafka `Pattern` (the union of all active patterns) instead of literal topic names.
- Each `KafkaConsumer` is created, polled and closed by one dedicated thread. Subscribe and unsubscribe enqueue
  changes and wake that thread. Handlers run on a separate virtual-thread executor. With no subscriptions the poll
  loop idles quietly, with no "Consumer is not subscribed" error spam.
- Received events keep their topic. Previously every record was rebuilt as `amcp.event`, so no subscription matched.
- `Event.matchesTopic("p.**")` matches `p` and `p.<anything>` only. It no longer matches `phive...`.
- `KafkaEventBroker.publish` completes only after Kafka acknowledges the record, and fails when the broker is not
  running.

- Review follow-ups on the Kafka transport (seven fixes):
  1. A `KafkaConsumer` that cannot be built (for example `max.poll.records=abc` through the extra properties) now makes
     `start()` return a failed future, or `subscribeQueue` throw `IllegalStateException` with the cause. Before, the
     poll thread died silently and `start()` reported success. A dead worker is never left in the worker map.
  2. A failed `consumer.subscribe/unsubscribe` no longer marks the pattern set as applied: the cause is logged once and
     the update is retried with bounded backoff.
  3. Delivery is now at-least-once: manual commit after the handlers of the polled batch finished (bounded wait of
     30 s), rewind and redelivery of failed records, at most 3 attempts per record. Auto-commit used to commit on poll,
     before the asynchronous handler ran, which was at-most-once. Handlers must be idempotent.
  4. Queue mode: a record on an owned partition with no matching local subscription is still committed for the group
     (a Kafka limitation), but a WARN is logged once per (topic, queue) pair, and the README requires every instance
     sharing a queue name to subscribe the same patterns.
  5. `Event.matchesTopic` and `TopicPatterns.toRegex` now agree on topics with empty segments: a wildcard never
     matches a trailing dot or doubled dot. A table-driven test runs the same cases through both.
  6. README: the topic-pickup time is stated in terms of `metadata.max.age.ms` (1 s here, plus a rebalance), and a
     subscription takes effect shortly after `subscribe()` returns.
  7. `InMemoryEventBroker.stop()` followed by `start()` no longer makes `publish` throw `RejectedExecutionException`.
  Not done: the queue-mode drop in item 4 is documented and logged, not prevented.

### Changed

- `KafkaEventBroker.subscribe` (fan-out) skips events older than the subscription's creation time minus 5 seconds,
  so a new broker instance does not replay history.
- `KafkaEventBroker.stop` wakes and joins every poll thread, closes consumers and producer, and shuts the executor
  down. Calling `start` or `stop` more than once is safe.

## [1.6.0] - 2024-11-10

### Added

#### 🚀 Strong Mobility Framework (NEW)
- **Automatic State Preservation**: Bytecode instrumentation for seamless agent migration
- **ATP Protocol**: Agent Transfer Protocol for standardized agent packaging
- **ExecutionStateCapture**: Stack frame serialization and restoration
- **ContinuationFramework**: Execution resumption after migration
- **StrongMobilityAgent Interface**: New interface for mobile agents
- **Security Model**: Code signing and sandboxing for safe migration
- **Use Cases**: Dynamic load balancing, follow-the-sun computing, edge-to-cloud workflows

#### 🔗 CloudEvents Integration (NEW)
- **CloudEvents v1.0 Compliance**: Full support for CloudEvents specification
- **Event Router**: Content-based and topic-based event routing
- **Event Tracing**: Distributed tracing with OpenTelemetry integration
- **Event Sourcing**: Event replay and audit trail capabilities
- **Event Types**: 8 new event types for agent and mesh operations
- **Event Filtering**: Advanced filtering expressions for event routing

#### 🛡️ Security Enhancements (NEW)
- **mTLS Support**: Mutual TLS for agent-to-agent communication
- **RBAC**: Role-based access control for agent operations
- **Audit Logging**: Comprehensive audit trails for compliance
- **Secret Management**: Integration with HashiCorp Vault
- **Certificate Management**: Automatic certificate rotation
- **Encryption**: End-to-end encryption for agent migration

#### ⚡ Enhanced LLM Orchestration v2 (EVOLVED)
- **Improved AsyncLLMConnector**: Better concurrency and timeout management
- **LLMFallbackSystem**: Intelligent pattern-based fallback with learning
- **Two-Tier Caching**: Memory + disk persistence with 24h TTL
- **Distributed Caching**: Redis support for distributed deployments
- **Model Prioritization**: Qwen2.5:0.5b, Gemma 2B, Qwen2 1.5B, Qwen2 7B
- **Adaptive Timeout Tuning**: Based on system resources
- **Performance Profiles**: Model-specific optimization profiles
- **Real-time Analytics**: Performance metrics and monitoring

#### 🔄 Advanced Agent Mesh (EVOLVED)
- **Dynamic Service Discovery**: Automatic agent registration and discovery
- **Load Balancing**: Round-robin, least connections, weighted distribution
- **Circuit Breaker Pattern**: Failure detection and automatic recovery
- **Health Checks**: Continuous health monitoring
- **Service Mesh Integration**: Istio, Linkerd, Consul support
- **Kubernetes Integration**: Native K8s service discovery
- **Observability**: Prometheus metrics, Grafana dashboards

#### 👨‍💻 Developer Experience (EVOLVED)
- **Enhanced CLI v2**: Interactive debugging and profiling commands
- **Visual Agent Designer**: Web-based agent composition tool
- **Performance Profiler**: CPU, memory, network, and latency analysis
- **Testing Framework**: Comprehensive testing utilities and mocks
- **Interactive Debugging**: Real-time agent inspection and debugging
- **Agent Composition**: Drag-and-drop agent builder
- **Mesh Visualization**: Visual representation of agent mesh

#### 📊 Observability & Monitoring (NEW)
- **Prometheus Integration**: Detailed metrics collection
- **Grafana Dashboards**: Pre-built dashboards for monitoring
- **Distributed Tracing**: OpenTelemetry integration
- **Performance Analytics**: Historical performance analysis
- **Alert System**: Configurable alerts for anomalies
- **Health Dashboard**: Real-time system health overview

### Changed

#### 🔄 Breaking Changes
- **Agent Interface**: Updated to support strong mobility
- **Event Model**: Now uses CloudEvents standard format
- **Configuration Format**: New schema for security and mesh settings
- **LLM Connector API**: New methods for enhanced orchestration
- **Mesh Configuration**: Updated for new service discovery features

#### 📈 Improvements
- **Performance**: 95% faster cached responses, 60% reduced resource usage
- **Scalability**: 10x concurrent request capacity
- **Reliability**: Enhanced error handling and recovery
- **Security**: Enterprise-grade security features
- **Documentation**: Comprehensive guides and examples

#### 🔧 Internal Changes
- Refactored agent lifecycle management
- Improved event bus implementation
- Enhanced caching layer
- Optimized network communication
- Better resource management

### Fixed

- Fixed agent state serialization issues
- Improved timeout handling in LLM connector
- Enhanced error messages for better debugging
- Fixed memory leaks in cache layer
- Improved network resilience

### Security

- Added mTLS support for all agent communications
- Implemented RBAC for fine-grained access control
- Added comprehensive audit logging
- Integrated HashiCorp Vault for secret management
- Added certificate management and rotation
- Implemented end-to-end encryption for migrations

### Deprecated

- Legacy `Agent` interface (use `StrongMobilityAgent` instead)
- Old event format (migrate to CloudEvents)
- Legacy configuration format (use new schema)

### Performance

- **Cache Performance**: 95% faster cached responses
- **Memory Usage**: 60% reduction in resource consumption
- **Concurrency**: 10x increase in concurrent request capacity
- **Migration Time**: 80-640ms end-to-end migration
- **Fallback Response**: <50ms pattern-based fallback

### Documentation

- Added comprehensive architecture documentation
- Created migration guide (v1.5 → v1.6)
- Added API documentation for new features
- Created performance benchmarks
- Added security guidelines
- Created deployment guides for various topologies

---

## [1.5.1] - 2024-10-10

### Added

- Qwen2.5:0.5b ultra-minimal LLM model integration
- LocalModelTester: Interactive terminal testing utility
- Performance optimization tools for Ollama
- Model-specific timeout configurations
- Enhanced GPU acceleration recommendations

### Changed

- Improved AsyncLLMConnector with performance-aware timeouts
- Enhanced model prioritization logic
- Optimized cache layer for better performance
- Improved error handling and recovery

### Fixed

- Fixed deprecated ProcessBuilder usage for Java 21
- Improved memory management in cache layer
- Enhanced timeout handling for slow models
- Fixed concurrent request handling

### Performance

- 50-80% faster model loading with keep-alive
- 30-50% faster responses with optimized timeouts
- Reduced memory pressure with desktop optimization
- Better resource utilization with priority tuning

---

## [1.5.0] - 2024-10-08

### Added

- LLMFallbackSystem: Intelligent fallback mechanism with pattern matching
- AsyncLLMConnector: Async LLM calls with extended timeouts
- LLMResponseCache: Two-tier caching (memory + disk)
- Enhanced GPU acceleration with smart recommendations
- Comprehensive testing framework (96 tests)
- Stock agent with real-time data support
- Travel planner agent
- Chat agent for conversational interactions

### Changed

- Improved agent mesh communication
- Enhanced CLI with multi-agent support
- Better error handling and recovery
- Optimized performance for local development

### Fixed

- Fixed agent registration issues
- Improved command handling in CLI
- Enhanced error messages
- Fixed memory leaks

### Performance

- 95% faster cached responses
- 60% reduced resource usage
- 10x concurrent request capacity
- Sub-50ms fallback response times

---

## [1.4.0] - 2024-09-15

### Added

- Core agent framework
- Event-driven communication
- Basic mesh support
- CLI interface
- Documentation

### Changed

- Improved agent lifecycle management
- Enhanced event bus

### Fixed

- Fixed compilation issues
- Improved error handling

---

## [1.0.0] - 2024-08-01

### Added

- Initial release
- Basic agent framework
- Event system
- Communication protocol

---

## Versioning

This project follows [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

## Release Schedule

- **v1.6.x**: Current (November 2024)
- **v1.7**: Q1 2025 - Advanced ML integration
- **v1.8**: Q2 2025 - Enterprise features
- **v2.0**: Q3 2025 - Major platform evolution

## Upgrade Guide

### From v1.5 to v1.6

See [MIGRATION_V1.5_TO_V1.6.md](docs/MIGRATION_V1.5_TO_V1.6.md) for detailed migration instructions.

### Key Changes

1. Update agent implementations to use `StrongMobilityAgent`
2. Migrate events to CloudEvents format
3. Update configuration files
4. Test thoroughly before deploying

## Support

For questions or issues:
- Check [GitHub Issues](https://github.com/xaviercallens/amcp-v1.5-opensource/issues)
- Review [Documentation](docs/)
- Contact maintainers

---

**Last Updated**: 2024-11-10  
**Maintained By**: AMCP Development Team
