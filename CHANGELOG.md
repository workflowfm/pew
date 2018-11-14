# Changelog

A list of all tagged versions of [WorkflowFM-PEW](https://github.com/PetrosPapapa/WorkflowFM-PEW).
Includes feature updates, bug fixes, and open issues.

## Known Issues

* [KafkaExecutor, unhandled messages #13](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/13) - since v1.0
* [MultiStateExecutor race condition problems #9](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/9) - since v0.1
* [AkkaExecutor.Call may timeout #4](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/4) - since v0.1


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

* Implementation of `KafkaExecutor`.
* [Executors are now Observable #8](https://github.com/PetrosPapapa/WorkflowFM-PEW/pull/8) using `PiEventHandler`s and observing `PiEvent`s (see also [#8](https://github.com/PetrosPapapa/WorkflowFM-PEW/issues/5)).
* Also improved `ProcessExecutor` trait/API.
* Various improvements in simulation and the D3 timeline.


## [v0.1](https://github.com/PetrosPapapa/WorkflowFM-PEW/releases/tag/v0.1) - 2018-09-18

### Features

* Initial port from older SVN repository.
* Main PEW engine for execution of generated pi-calculus workflows.
* First versions of `AkkaExecutor` and `MongoExecutor`.
* Simulation capabilities with D3 timeline visualization.

