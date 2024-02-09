# LMR: Little MapReduce (or Local MapReduce)

lmr is a tool for executing MapReduce-shaped tasks on a workstation. It's
similar to tools like `xargs` or `parallel` but provides a bit more structure
around job execution, failure handling, caching, and output management.

LMR is currently experimental, and may undergo significant API changes before
a proper v1 release. Depend on it at your own risk.

## Reference

### Mapper Protocol

A mapper is provided as the first argument, and is required. A mapper can be
a binary on the PATH, any executable file, or a shell command. The mapper is
executed for each input chunk. The chunk is provided on stdin. By default
any mapper stdout is grouped under a "default" key. Additional outputs keys
can be created by writing to files in the "results" dir. This directory is
provided to the mapper script in the `LMR_RESULTS_DIR` environment variable.
For example, executing `echo ... > $LMR_RESULTS_DIR/foo` would produce an output
with the `foo` key.

## Roadmap

- Mapper
  - Better error messages on stage failure
  - Configurable parallelism
  - Optional Resubmission
  - Chunk output caching
    - Cache management commands
    - Configurable Cache Size
  - Progress Bar
  - Performance stats on map stages
- Reducer
  - Keyed output protocol. E.g. what happens when there are multiple keys?
  - Script reducer
  - Canned reducers
    - Concat + custom separator
    - Json Array
    - sum
- Project
  - Code Health
    - CI
    - Tests
    - Lints
    - Separate Modules
  - Example in docs
  - Binary builds
