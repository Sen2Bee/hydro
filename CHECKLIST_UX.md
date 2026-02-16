# Local UX Checklist

Date:
Tester:
Branch/Commit:

## Core Flow
- [ ] App loads without console errors.
- [ ] Sidebar and map render correctly on first load.
- [ ] Threshold slider updates value and request payload.
- [ ] Upload mode accepts `.tif/.tiff` and starts analysis.
- [ ] Draw mode allows rectangle selection and analyze action.

## Progress and Status
- [ ] Progress indicator appears during run.
- [ ] Status transitions from running to success/failure.
- [ ] Error text is visible and understandable on failure.

## Map UX
- [ ] Result layer appears after successful run.
- [ ] Fit-to-result works.
- [ ] Basemap switch works.
- [ ] Layer toggle works (tiles + flow network).

## Data/Result Sanity
- [ ] `/v1/jobs/{id}` reaches `succeeded`.
- [ ] `/v1/jobs/{id}/results` returns at least one output.
- [ ] Output metadata includes `summary` and `geojson`.

## Responsive Check
- [ ] Desktop: sidebar usable, map controls not overlapping key UI.
- [ ] Mobile width (~390px): sidebar toggle works and map remains usable.

## Regression Snapshot
- [ ] No obvious visual regressions vs previous build.
- [ ] No broken German text or encoding artifacts in UI labels.

## Notes
- Issues found:
- Screenshots/recordings:
- Follow-up tasks:
