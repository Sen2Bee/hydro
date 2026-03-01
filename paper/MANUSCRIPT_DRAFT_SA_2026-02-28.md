# From Long-Term Erosion Risk to Event-Driven Parcel Monitoring in Saxony-Anhalt (Working Draft)

Stand: 2026-02-28  
Status: Arbeitsfassung (nicht eingereicht)

## Titel (Arbeitsversion)
From ABAG Baseline to Event-Driven Parcel Monitoring: A Reproducible Pipeline for Soil Erosion Risk in Saxony-Anhalt

## Abstract (Draft)
Soil erosion assessment in Germany is commonly based on long-term risk frameworks such as ABAG. While this supports policy and planning, it does not directly localize individual erosion-relevant events in space and time. We present a reproducible parcel-level pipeline for Saxony-Anhalt that combines (i) static ABAG-related susceptibility information, (ii) dynamic event windows derived from weather-driven detection, and (iii) event-oriented ML inference. The workflow is implemented as a resume-capable chunk runner, supports multi-window seasonal analysis (2023-2025 vegetation periods), and produces auditable artifacts for quality assurance and publication (run manifests, chunk state, QA reports, map-ready outputs). We describe the operational architecture, data flow, quality gates, and publication pathway. Current results are preliminary and focus on method robustness and reproducibility; final model performance metrics and comparative ablations are reported after completion of the full production run.

## 1. Introduction
Water erosion risk management requires both strategic and operational perspectives.  
1. Strategic perspective: long-term risk indication (ABAG-like factors and topographic susceptibility).  
2. Operational perspective: event localization (when/where severe rainfall conditions likely produce erosion-relevant responses).

The methodological gap is the transfer from long-term risk layers to event-scale parcel decisions. This study addresses that gap with a reproducible engineering pipeline designed for state-wide execution.

## 2. Study Area
Saxony-Anhalt (Germany), parcel-scale processing over a state-wide parcel inventory, evaluated in chunked batches for computational reliability and resume safety.

## 3. Data and Inputs (Current Implementation)
### 3.1 Parcel Geometry
State-wide parcel base loaded into local cache/SQLite and processed in fixed-size chunks.

### 3.2 Terrain and Hydrologic Derivatives
DEM-based flow routing and flow accumulation are computed per AOI request in the analysis backend and used in ABAG/event analysis paths.

### 3.3 Event Windows
Automatic event window detection is requested per parcel and time window via backend weather event endpoint, currently configured with `ICON2D` as operational source.
For production, this step is executed as a dedicated Stage A precompute (state-wide, chunked, resume-capable) before any ABAG/Event-ML inference.

### 3.4 ABAG + Event-ML Analyses
For each detected event and parcel, two analysis modes are executed:
1. `abag`
2. `erosion_events_ml`

This yields a unified parcel x event x analysis table.

## 4. Methods
### 4.1 Pipeline Design
The production workflow uses:
1. state-wide chunking (`1000` parcels per chunk),
2. checkpointed CSV export per chunk,
3. `.done` completion flags,
4. resumable state (`sa_chunk_run_state.json`),
5. centralized run manifests and logs.

Stage separation:
1. Stage A: weather/event precompute only (no erosion model inference),
2. Stage B: `abag` + `erosion_events_ml` from local cache (`cache-only`).

### 4.2 Spatial Sampling for Publication
For publication subsets, parcels are selected spatially stratified (grid-based deterministic sampling with seed) to avoid row-order bias and to ensure map-wide distribution.

### 4.3 Seasonal Windows
Configured windows:
1. 2023-04-01 to 2023-10-31
2. 2024-04-01 to 2024-10-31
3. 2025-04-01 to 2025-10-31

### 4.4 C-Factor Strategy (Current and Next)
Current baseline uses a configurable proxy method (`c_factor_method_v1`).  
Upgrade path (already implemented in code): crop-history-enhanced dynamic C via open-data ingestion and window-specific C generation.  
Sensitivity setup exists for low/base/high C configurations.

### 4.5 Quality Assurance
QA is enforced at three levels:
1. chunk-level run status and completion flags,
2. merged output validation and report generation,
3. publication artifact generation (top-10, map layers, histogram tables).

## 5. Reproducibility and Artifacts
Each run produces:
1. run logs with timestamp and command context,
2. chunk state with completed/failed indices,
3. chunk CSV outputs,
4. merged CSV + QA report + manifest,
5. quickcheck export (Top-10 + GeoJSON + report),
6. paper asset tables (counts/histograms in CSV/JSON).

This design supports interruption, resume, and independent audit.

## 6. Preliminary Operational Status (as of 2026-02-28)
The multi-window SA run is active and stable in chunked mode with continuous log updates and resume-enabled state tracking.  
At this stage, this manuscript intentionally does not claim final scientific performance metrics, because the full production run and final ablation comparison are still in progress.

Current production configuration:
1. Three parallel state-wide Stage A workers (2023, 2024, 2025 seasonal windows),
2. `2705` chunks per year-window (`1000` parcels/chunk),
3. central local cache under `data/events/sa_2km/icon2d_<window>/`,
4. simple human-readable overall progress log across all three workers.

## 7. Planned Evaluation (Final Paper Section)
### 7.1 Primary Metrics
1. Precision, Recall, F1, PR-AUC (event classification).
2. Calibration diagnostics.
3. Error segmentation by region/event intensity/crop group.

### 7.2 Ablation
1. Weather-only.
2. Weather + phenology proxy.
3. Weather + dynamic C proxy.
4. Weather + dynamic C + ABAG/topographic disposition.

### 7.3 Robustness
1. C-method sensitivity (low/base/high).
2. Temporal generalization across 2023/2024/2025 windows.
3. Spatial generalization under block-wise validation design.

## 8. Limitations (Current)
1. Final labeled event ground truth for causal erosion confirmation is still limited and under extension.
2. Current manuscript version emphasizes operational reproducibility; final inferential claims depend on completed evaluation.
3. Runtime heterogeneity across parcels (AOI complexity, event count) influences wall-clock throughput.
4. Stage A runtime is dominated by event extraction plus persistent cache I/O at parcel scale; this is expected for state-wide 3-window processing.

## 9. Practical Value
The proposed workflow is directly useful for:
1. state-level screening and prioritization,
2. parcel-level operational monitoring preparation,
3. transparent reporting for public-sector decision workflows.

## 10. Conclusion (Draft)
This work establishes a reproducible bridge from long-term erosion susceptibility mapping to event-driven parcel monitoring. The main contribution is a robust, resume-safe, auditable production architecture that enables scientifically credible evaluation at scale. Final performance and comparative evidence are provided after completion of the full multi-window run and ablation protocol.

---

## Appendix A: Current File Anchors (Project)
1. Protocol: `paper/PAPER_PROTOCOL_SA.md`
2. Chunk runbook: `paper/RUNBOOK_SA_AUTO_EVENTS_CHUNKS_2026-02-24.md`
3. Paper assets runbook: `paper/RUNBOOK_PAPER_ASSETS_2026-02-28.md`
4. C-method changelog: `paper/CHANGELOG_METHODIK_2026-02-28.md`
5. Next steps roadmap: `paper/ROADMAP_NEXT_STEPS_2026-02-28.md`

## Appendix B: Immediate To-Do for Submission-Ready Version
1. Complete all configured windows and merge outputs.
2. Run full ablation and sensitivity protocol.
3. Add final tables/figures and statistical uncertainty intervals.
4. Add related-work citations in journal style and finalize discussion.
