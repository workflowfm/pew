# Changelog

A list of all tagged versions of [WorkflowFM-PEW](https://github.com/PetrosPapapa/WorkflowFM-PEW).

Includes feature updates, bug fixes, and open issues.

## Known Issues

* [KafkaExecutor, unhandled messages #13](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/13) - since v1.0
* [MultiStateExecutor race condition problems #9](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/9) - since v0.1
* [AkkaExecutor.Call may timeout #4](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/4) - since v0.1



## [v1.4.0](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.4.0) - 2019-07-01

### Features

* Improved `PiEventHandlers`. The `PromiseHandler` is now generalized to return a single object at the end of the workflow. The old `PromiseHandler` is an instance called `ResultHandler` (see also [#26](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/26)).
* Implemented `PiStream` using Akka's `BroadcastHub` to enable more flexible event handling (see also [#34](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/34)). Executors can now be mixed in with (at least) either of the two default observables, namely `SimplePiObservable` and `PiStream`.
* `SimMetricsActor` no longer keeps a reference to the `Coordinator`. This makes for a cleaner, more flexible implementation, allowing multiple simulations across multiple `Coordinator`s. The downside is that simulations can be run asynchronously, making it hard to disambiguate which results came from which `Coordinator`. We leave that problem to the user for now.


## [v1.3.0](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.3.0) - 2019-06-19

For some unknown reason, the version number was increased in `build.sbt` back in December without actually merging the intended changes or creating a new tag. In the meantime, [#45](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/45)) was merged with various bug fixes and minor changes, the Ski example was updated and some documentation was added. I decided to create the tag now and push the stream changes to 1.4.0.

### Features

* All `PiEvents` now carry an array of `PiMetadata`. The default value contains the system time of the event. Atomic processes can expose additional metadata for `PiEventReturn` (see also [#21](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/21)).
* Fixed some codec issues (see also [#31](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/31)).
* The simulator now measures the simulation's real (system) duration.
* Some `PiEvents` got rearranged or renamed (see also [#45](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/45)).
* Bugfixes and improvements for `KafkaExecutor` and `KafkaExecutorTests` (see also [#45](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/45)).
* Various improvements in Executor unit tests.


## [v1.2.2](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.2.2) - 2018-12-03

### Features

* Fixed `AkkaPiObservable` registering handlers globally (see also [#7](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/7)).
* Improved simulation `Coordinator`. Processes can now interact with the `Coordinator` and its clock cycle is a bit more robust (see also [#28](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/28)).
* Fixed/improved Codecs for custom data types, which includes `AnyCodec` and associated parts (see also [#29](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/29)).
* Fixed issues with the `ClassLoader` crashing in Kafka (see also [#30](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/30)).


## [v1.2.1](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.2.1) - 2018-11-21

### Features

* Fixed problems and improved the `Scheduler` ([issue 20](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/20)).
* Added unit tests for the `Scheduler` and `Task` priority.

## [v1.2.0](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.2.0) - 2018-11-14

### Features

* [Improved simulation analytics #18](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/18) (see also [#16](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/16)).
* That code is now disentangled from the individual parts of the simulation and concentrated on a `SimMetricsAggregator` within the `Coordinator`.
* The classes holding the metrics are cleaner and easier to expand or translate to outputs.
* This also solves the constraint of the `Coordinator` having to start from time `1L`. Instead you can now use a custom starting time (such as the current timestamp in milliseconds) with a default of `0L`.
* Key improvements with the timeline, including a bug with "relative time" mode.

## [v1.1.0](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.1.0) - 2018-11-07

### Features

* Implemented analytics and timeline visualization for any workflow execution using `PiEvent`s. [#15](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/15)


## [v1.0](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v1.0) - 2018-11-07

### Features

* Implementation of `KafkaExecutor` ([#10](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/10),[#11](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/11)).
* [Executors are now Observable #8](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/8) using `PiEventHandler`s and observing `PiEvent`s (see also [#5](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/5)).
* Also improved `ProcessExecutor` trait/API.
* Various improvements in simulation and the D3 timeline.


## [v0.1](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v0.1) - 2018-09-18

### Features

* Initial port from older SVN repository.
* Main PEW engine for execution of generated pi-calculus workflows.
* First versions of `AkkaExecutor` and `MongoExecutor`.
* Simulation capabilities with D3 timeline visualization.

