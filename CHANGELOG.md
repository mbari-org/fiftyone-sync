# CHANGELOG

<!-- version list -->

## v0.12.3 (2026-08-20)

### Bug Fixes

- **app**: Encode Fast-VSS WebSocket project paths and reject empty embed POSTs (#37)
  ([#37](https://github.com/mbari-org/fiftyone-sync/pull/37),
  [`372ad98`](https://github.com/mbari-org/fiftyone-sync/commit/372ad9861c93e4776acd2b8b6a9a7d2a60f43c85))


## v0.12.2 (2026-08-04)

### Bug Fixes

- **app**: Request presigned media URLs when downloading from Tator (#34)
  ([#34](https://github.com/mbari-org/fiftyone-sync/pull/34),
  [`97172cb`](https://github.com/mbari-org/fiftyone-sync/commit/97172cb2fb91e752e59e48410eab98f2a3b77611))


## v0.12.1 (2026-08-01)

### Bug Fixes

- **infra**: Shorten README.md for Docker Hub push limit; serialize release workflow (#33)
  ([#33](https://github.com/mbari-org/fiftyone-sync/pull/33),
  [`1c692d4`](https://github.com/mbari-org/fiftyone-sync/commit/1c692d4436fd52761edc36b13b927324294f35bb))


## v0.12.0 (2026-08-01)

### Features

- **app**: Push verified_only filtering into Tator media/localization queries (#32)
  ([#32](https://github.com/mbari-org/fiftyone-sync/pull/32),
  [`c92b134`](https://github.com/mbari-org/fiftyone-sync/commit/c92b1341276d9a7fb4050ee096935f4ad8a45ded))

### Performance Improvements

- **app**: Parallelize image media download+crop in sequential sync path (#31)
  ([#31](https://github.com/mbari-org/fiftyone-sync/pull/31),
  [`b44e1fe`](https://github.com/mbari-org/fiftyone-sync/commit/b44e1fe43ca8b6feec7b9b28b631bfeab26ba3b6))


## v0.11.0 (2026-07-31)

### Features

- Filter verified samples in CSV export (#30)
  ([#30](https://github.com/mbari-org/fiftyone-sync/pull/30),
  [`157341c`](https://github.com/mbari-org/fiftyone-sync/commit/157341c42d1d8991c87704cabd5829a49d6740cc))


## v0.10.0 (2026-07-27)

### Features

- **app**: Add dataset rename support with flexible token auth (#28)
  ([#28](https://github.com/mbari-org/fiftyone-sync/pull/28),
  [`08ccb6a`](https://github.com/mbari-org/fiftyone-sync/commit/08ccb6abf121f7303d3e5882489702d0c26f973d))


## v0.9.2 (2026-07-17)

### Bug Fixes

- Repair orphaned embedding fields; incremental, concurrent embeddings with progress logging (#26)
  ([#26](https://github.com/mbari-org/fiftyone-sync/pull/26),
  [`fdcc343`](https://github.com/mbari-org/fiftyone-sync/commit/fdcc343741a4ee47a26bb555c32570f53fe75bcf))


## v0.9.1 (2026-07-16)

### Performance Improvements

- **app**: Streamline media download and crop caching (#25)
  ([#25](https://github.com/mbari-org/fiftyone-sync/pull/25),
  [`fc5e62d`](https://github.com/mbari-org/fiftyone-sync/commit/fc5e62da1a6ef4afa951f6909ff80ae347764411))


## v0.9.0 (2026-07-14)

### Features

- Add box type selector for dataset sync (#24)
  ([#24](https://github.com/mbari-org/fiftyone-sync/pull/24),
  [`e5f72a0`](https://github.com/mbari-org/fiftyone-sync/commit/e5f72a0c74e9e617f38633ef06e787ecdd9223f1))


## v0.8.0 (2026-07-08)

### Features

- **app**: Include section_id in Voxel51 dataset names (#22)
  ([#22](https://github.com/mbari-org/fiftyone-sync/pull/22),
  [`70bb9d9`](https://github.com/mbari-org/fiftyone-sync/commit/70bb9d99aaf75dc88eff5b52870331849d1283ef))


## v0.7.0 (2026-07-01)

### Features

- **sync**: Classification samples alongside localizations (#21)
  ([#21](https://github.com/mbari-org/fiftyone-sync/pull/21),
  [`a6462ab`](https://github.com/mbari-org/fiftyone-sync/commit/a6462aba83289f6e4b2fc9d674f25e59b2675ee7))


## v0.6.0 (2026-06-23)

### Features

- **sync**: Discover versioned prediction/score pairs as indexed labels (#20)
  ([#20](https://github.com/mbari-org/fiftyone-sync/pull/20),
  [`c7b7bf4`](https://github.com/mbari-org/fiftyone-sync/commit/c7b7bf43507bdef18658cdf6a46f9de325cd7c43))


## v0.5.0 (2026-06-14)

### Features

- **ui**: Add Voxel51 Management section to launcher template (#18)
  ([#18](https://github.com/mbari-org/fiftyone-sync/pull/18),
  [`f0e3de0`](https://github.com/mbari-org/fiftyone-sync/commit/f0e3de03ebd4d58718e7b6d8f5297de8e5f8166d))


## v0.4.0 (2026-06-13)

### Features

- **launcher**: Remove Recompute Dimreduce button (#17)
  ([#17](https://github.com/mbari-org/fiftyone-sync/pull/17),
  [`aef7984`](https://github.com/mbari-org/fiftyone-sync/commit/aef798484e86f5c9083221adcb21d8571d68211a))


## v0.3.0 (2026-05-30)

### Documentation

- More condensed README.md with absolute GitHub URLS
  ([`4cacdd4`](https://github.com/mbari-org/fiftyone-sync/commit/4cacdd43a98561f26e829647d9b661381b0cd879))

### Features

- **sync**: Add explicit dataset mapping for sync-to-tator (#15)
  ([#15](https://github.com/mbari-org/fiftyone-sync/pull/15),
  [`e52715f`](https://github.com/mbari-org/fiftyone-sync/commit/e52715ff118381ab34d9d04b5dd1a062a67bc52e))


## v0.2.0 (2026-05-19)

### Features

- Queue crop recompute and improve sync throughput (#13)
  ([#13](https://github.com/mbari-org/fiftyone-sync/pull/13),
  [`3a77370`](https://github.com/mbari-org/fiftyone-sync/commit/3a77370d6335b8ce067f22fea091bd481ea657ce))


## v0.1.2 (2026-05-12)

### Performance Improvements

- Run pushes via RQ with backpressure and locking (#11)
  ([#11](https://github.com/mbari-org/fiftyone-sync/pull/11),
  [`6124c12`](https://github.com/mbari-org/fiftyone-sync/commit/6124c126f373b0dc59954eb553083f1c4b94beb7))


## v0.1.1 (2026-05-12)

### Performance Improvements

- **sync**: Bulk Tator localization fetch and PATCH for sync-to-tator (#10)
  ([#10](https://github.com/mbari-org/fiftyone-sync/pull/10),
  [`7828f51`](https://github.com/mbari-org/fiftyone-sync/commit/7828f51b5ef1bc2f921c39f2d1acf3c2bd2aba1e))


## v0.1.0 (2026-05-09)

- Initial Release
