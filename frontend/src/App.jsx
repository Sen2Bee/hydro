import { useState, useCallback, useRef, useEffect, useMemo } from 'react'
import { MapContainer, TileLayer, GeoJSON, Rectangle, CircleMarker, Popup, Polyline, Polygon, Pane, useMap, useMapEvents } from 'react-leaflet'
import L from 'leaflet'
import './index.css'

const LEGACY_API_URL = import.meta.env.VITE_LEGACY_API_URL || 'http://127.0.0.1:8010'
const JOB_API_URL = import.meta.env.VITE_JOB_API_URL || 'http://127.0.0.1:8002'
const DEFAULT_API_MODE = (import.meta.env.VITE_API_MODE || 'legacy').toLowerCase() // legacy | jobs
const DEMO_PROJECT_ID = import.meta.env.VITE_DEMO_PROJECT_ID || '22222222-2222-2222-2222-222222222222'
const DEMO_MODEL_ID = import.meta.env.VITE_DEMO_MODEL_ID || '33333333-3333-3333-3333-333333333333'
const DEMO_TENANT_ID = import.meta.env.VITE_DEMO_TENANT_ID || '11111111-1111-1111-1111-111111111111'

const UI_STATE_KEY = 'hydrowatch:ui_state_v1'
// Default start view: Halle (Saale).
const DEFAULT_MAP_VIEW = { lat: 51.482, lon: 11.969, zoom: 12 }

function isDevUiEnabled() {
    // Hidden developer UI toggle:
    // - visit with ?dev=1 once (persists), or
    // - set localStorage hydrowatch:dev = "1"
    try {
        const qs = new URLSearchParams(window.location.search || '')
        if (qs.get('dev') === '1') {
            localStorage.setItem('hydrowatch:dev', '1')
            return true
        }
        return localStorage.getItem('hydrowatch:dev') === '1'
    } catch {
        return false
    }
}

function loadApiModeOverride() {
    const raw = (localStorage.getItem('hydrowatch:api_mode') || '').trim().toLowerCase()
    if (raw === 'jobs' || raw === 'legacy') return raw
    return DEFAULT_API_MODE === 'jobs' ? 'jobs' : 'legacy'
}

function saveApiModeOverride(mode) {
    try { localStorage.setItem('hydrowatch:api_mode', mode) } catch {}
}

function loadJobSelection() {
    try {
        const raw = localStorage.getItem('hydrowatch:job_selection')
        if (!raw) return { tenantId: DEMO_TENANT_ID, projectId: DEMO_PROJECT_ID, modelId: DEMO_MODEL_ID }
        const v = JSON.parse(raw)
        return {
            tenantId: v?.tenantId || DEMO_TENANT_ID,
            projectId: v?.projectId || DEMO_PROJECT_ID,
            modelId: v?.modelId || DEMO_MODEL_ID,
        }
    } catch {
        return { tenantId: DEMO_TENANT_ID, projectId: DEMO_PROJECT_ID, modelId: DEMO_MODEL_ID }
    }
}

function saveJobSelection(sel) {
    try { localStorage.setItem('hydrowatch:job_selection', JSON.stringify(sel)) } catch {}
}

function loadUiState() {
    try {
        const raw = localStorage.getItem(UI_STATE_KEY)
        if (!raw) return {}
        const v = JSON.parse(raw)
        return (v && typeof v === 'object') ? v : {}
    } catch {
        return {}
    }
}

function saveUiState(state) {
    try { localStorage.setItem(UI_STATE_KEY, JSON.stringify(state)) } catch {}
}

const OFFICIAL_WMS = {
    nrw: {
        label: 'NRW (Hochwasser-Gefahrenkarte)',
        baseUrl: 'https://www.wms.nrw.de/umwelt/HW_Gefahrenkarte?',
        scenarios: {
            hw: { label: 'Hohe Wahrscheinlichkeit (HW)' },
            mw: { label: 'Mittlere Wahrscheinlichkeit (MW)' },
            nw: { label: 'Niedrige Wahrscheinlichkeit (NW)' },
        },
        supports: { extent: true, depth: true },
        layerName: (type, scenarioKey) => {
            const suffix = scenarioKey || 'mw'
            if (type === 'extent') return `Grenze_der_ueberfluteten_Gebiete_${suffix}`
            if (type === 'depth') return `Tiefen_Ueberflutungsgebiet_${suffix}`
            return null
        },
    },
    'sachsen-anhalt': {
        label: 'Sachsen-Anhalt (LHW HWRM-RL)',
        baseUrl: 'https://www.geofachdatenserver.de/ws/wms/3e63df0e-407f-e46a/LHW-LSA_HWRMRL/ows.wms?',
        scenarios: {
            hw: { label: 'Haeufig (HQ10/20)' },
            mw: { label: 'Mittel (HQ100)' },
            nw: { label: 'Selten (HQ200)' },
        },
        supports: { extent: false, depth: true },
        layerName: (type, scenarioKey) => {
            if (type !== 'depth') return null
            if (scenarioKey === 'hw') return 'lhw_hwrmrl_wt_hq10_20'
            if (scenarioKey === 'mw') return 'lhw_hwrmrl_wt_hq100'
            if (scenarioKey === 'nw') return 'lhw_hwrmrl_wt_hq200'
            return 'lhw_hwrmrl_wt_hq100'
        },
    },
}

const BASEMAPS = {
    light: {
        label: 'Hell',
        // More contrast than CARTO Positron (light_all), still neutral.
        url: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        maxNativeZoom: 20,
    },
    satellite: {
        label: 'Satellit',
        url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attribution: '&copy; Esri',
        maxNativeZoom: 19,
    },
    topo: {
        label: 'Topo',
        url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
        attribution: '&copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
        maxNativeZoom: 17,
    },
    dark: {
        label: 'Dunkel',
        url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        maxNativeZoom: 20,
    },
}

const RISK_COLORS = {
    niedrig: '#2ecc71',
    mittel: '#f1c40f',
    hoch: '#e67e22',
    sehr_hoch: '#e74c3c',
}

const AOI_SOFT_LIMIT_KM2 = 12
const AOI_HIGH_LIMIT_KM2 = 35

// "Netzdichte" presets for the optional network overlay.
// "Einzugsgebiet" = contributing upstream area (proxy from flow accumulation).
// Important: absolute upstream-area thresholds don't work well across very small vs. large AOIs.
// We therefore use fractions of the maximum upstream area within the current AOI.
const CORRIDOR_DENSITY_PRESETS = [
    { key: 'coarse', label: 'Grob', min_frac_of_max: 0.15 },
    { key: 'medium', label: 'Mittel', min_frac_of_max: 0.05 },
    { key: 'fine', label: 'Fein', min_frac_of_max: 0.0 }, // no extra filtering
]

function km2ToHa(km2) {
    const v = Number(km2)
    if (!Number.isFinite(v)) return null
    return v * 100
}

function km2ToM2(km2) {
    const v = Number(km2)
    if (!Number.isFinite(v)) return null
    return v * 1_000_000
}

function upstreamKm2Of(feature) {
    const m2 = Number(feature?.properties?.upstream_area_m2)
    if (Number.isFinite(m2) && m2 > 0) return m2 / 1_000_000.0
    const km2 = Number(feature?.properties?.upstream_area_km2)
    if (Number.isFinite(km2) && km2 > 0) return km2
    return 0
}

function formatAreaCompact(m2, nf) {
    const v = Number(m2)
    if (!Number.isFinite(v) || v <= 0) return '0 m²'
    // Prefer ha for mid-range values (readable, avoids huge m2 numbers).
    if (v >= 1_000_000) {
        const km2 = v / 1_000_000
        return `${nf.format(km2)} km²`
    }
    if (v >= 10_000) {
        const ha = v / 10_000
        return `${nf.format(ha)} ha`
    }
    return `${nf.format(Math.round(v))} m²`
}

function officialLayerName(providerKey, type, scenarioKey) {
    const cfg = OFFICIAL_WMS[providerKey]
    if (!cfg) return null
    return cfg.layerName(type, scenarioKey)
}

function estimateBboxAreaKm2(bbox) {
    if (!bbox) return 0
    const dLat = Math.abs(bbox.north - bbox.south)
    const dLon = Math.abs(bbox.east - bbox.west)
    const latMid = ((bbox.north + bbox.south) / 2) * (Math.PI / 180)
    const kmPerDegLat = 111.32
    const kmPerDegLon = 111.32 * Math.cos(latMid)
    return dLat * kmPerDegLat * dLon * kmPerDegLon
}

function riskColorOf(feature) {
    const cls = feature?.properties?.risk_class
    return RISK_COLORS[cls] || '#00e5ff'
}

function corridorStyleOf(feature) {
    return corridorStyleOfWithMax(feature, null)
}

function corridorStyleOfWithMax(feature, maxKm2) {
    const km2 = upstreamKm2Of(feature)
    const mx = Number(maxKm2)
    const hasRel = Number.isFinite(km2) && Number.isFinite(mx) && mx > 0

    // 4 classes relative to AOI max. This makes styling stable across small/large AOIs.
    // Fractions roughly match our density presets (main/coarse/medium/fine).
    const frac = hasRel ? (km2 / mx) : null
    let color = 'rgba(0,229,255,0.45)'
    let weight = 2.0
    let opacity = 0.92

    if (hasRel) {
        if (frac >= 0.35) { color = 'rgba(0,145,234,0.90)'; weight = 3.6; opacity = 0.96 } // Hauptachsen
        else if (frac >= 0.15) { color = 'rgba(0,188,212,0.82)'; weight = 3.1; opacity = 0.95 } // Hauptbahnen
        else if (frac >= 0.05) { color = 'rgba(0,229,255,0.70)'; weight = 2.6; opacity = 0.93 } // Sammellinien
        else { color = 'rgba(0,229,255,0.55)'; weight = 2.1; opacity = 0.90 } // Nebenrinnen
    } else if (Number.isFinite(km2)) {
        // Fallback when maxKm2 isn't available.
        if (km2 >= 2.0) { color = 'rgba(0,145,234,0.85)'; weight = 3.4 }
        else if (km2 >= 0.5) { color = 'rgba(0,188,212,0.75)'; weight = 3.0 }
        else if (km2 >= 0.1) { color = 'rgba(0,229,255,0.65)'; weight = 2.6 }
    }

    return { color, weight, opacity }
}

function pickCriticalErosionSegments(geojson, maxN = 250) {
    const fc = geojson?.type === 'FeatureCollection' ? geojson : null
    const feats = Array.isArray(fc?.features) ? fc.features : []
    if (!feats.length) return geojson

    const scored = feats
        .map((f) => ({ f, s: Number(f?.properties?.risk_score) }))
        .filter((x) => Number.isFinite(x.s))
        .sort((a, b) => b.s - a.s)

    if (!scored.length) return geojson

    // Prefer meaningful classes; if that yields nothing, show the strongest lines anyway.
    const critical = scored.filter((x) => x.s >= 70).slice(0, maxN).map((x) => x.f)
    const fallback = scored.slice(0, Math.min(maxN, Math.max(60, Math.ceil(scored.length * 0.12)))).map((x) => x.f)

    return {
        type: 'FeatureCollection',
        features: (critical.length ? critical : fallback),
        analysis: fc.analysis,
    }
}

function bboxFromPoints(points) {
    if (!points?.length) return null
    let south = Infinity
    let west = Infinity
    let north = -Infinity
    let east = -Infinity
    for (const [lat, lng] of points) {
        if (lat < south) south = lat
        if (lat > north) north = lat
        if (lng < west) west = lng
        if (lng > east) east = lng
    }
    return { south, west, north, east }
}

function _signedRingAreaLonLat(ring) {
    // ring: [[lon,lat], ...]
    if (!Array.isArray(ring) || ring.length < 3) return 0
    let a = 0
    for (let i = 0; i < ring.length; i++) {
        const [x1, y1] = ring[i]
        const [x2, y2] = ring[(i + 1) % ring.length]
        a += (Number(x1) * Number(y2) - Number(x2) * Number(y1))
    }
    return a / 2
}

function polygonPointsFromGeoJson(geojson) {
    // Returns polygon points as [[lat,lng], ...] (no repeated last point).
    const g = geojson?.type === 'Feature' ? geojson.geometry : geojson
    const fc = geojson?.type === 'FeatureCollection' ? geojson : null

    const candidates = []
    const pushPoly = (polyCoords) => {
        // polyCoords: [ring1, ring2...], use exterior ring only.
        const ring = polyCoords?.[0]
        if (!Array.isArray(ring) || ring.length < 3) return
        candidates.push(ring)
    }

    if (fc) {
        for (const f of (fc.features || [])) {
            const gg = f?.geometry
            if (!gg) continue
            if (gg.type === 'Polygon') pushPoly(gg.coordinates)
            if (gg.type === 'MultiPolygon') {
                for (const poly of (gg.coordinates || [])) pushPoly(poly)
            }
        }
    } else if (g?.type === 'Polygon') {
        pushPoly(g.coordinates)
    } else if (g?.type === 'MultiPolygon') {
        for (const poly of (g.coordinates || [])) pushPoly(poly)
    }

    if (candidates.length === 0) return null

    // Pick the largest ring by absolute area.
    const ring = [...candidates].sort((a, b) => Math.abs(_signedRingAreaLonLat(b)) - Math.abs(_signedRingAreaLonLat(a)))[0]

    // GeoJSON uses [lon,lat].
    let points = ring.map((c) => [Number(c[1]), Number(c[0])]).filter((p) => p.every(Number.isFinite))
    if (points.length >= 2) {
        const first = points[0]
        const last = points[points.length - 1]
        if (Math.abs(first[0] - last[0]) < 1e-12 && Math.abs(first[1] - last[1]) < 1e-12) {
            points = points.slice(0, -1)
        }
    }
    return points.length >= 3 ? points : null
}

function pointSegNearest(px, py, ax, ay, bx, by) {
    const abx = bx - ax
    const aby = by - ay
    const apx = px - ax
    const apy = py - ay
    const ab2 = abx * abx + aby * aby
    if (ab2 === 0) {
        return { d2: apx * apx + apy * apy, qx: ax, qy: ay, t: 0 }
    }
    const t = Math.max(0, Math.min(1, (apx * abx + apy * aby) / ab2))
    const qx = ax + t * abx
    const qy = ay + t * aby
    const dx = px - qx
    const dy = py - qy
    return { d2: dx * dx + dy * dy, qx, qy, t }
}

function featureNearestToPoint(feature, lat, lon) {
    const g = feature?.geometry
    const coords = g?.coordinates
    if (!coords) return null
    const lines = g.type === 'MultiLineString' ? coords : [coords]
    let best = null
    for (const line of lines) {
        for (let i = 1; i < line.length; i++) {
            const a = line[i - 1]
            const b = line[i]
            const m = pointSegNearest(lon, lat, a[0], a[1], b[0], b[1])
            if (!Number.isFinite(m?.d2)) continue
            if (!best || m.d2 < best.d2) {
                best = { d2: m.d2, snapLat: Number(m.qy), snapLon: Number(m.qx) }
            }
        }
    }
    return best
}

function pointInPolygonLatLon(lat, lon, polyLatLon) {
    // Ray casting on lon/lat plane. Works well for small polygons (AOI/catchment scale).
    if (!Array.isArray(polyLatLon) || polyLatLon.length < 3) return false
    const x = Number(lon)
    const y = Number(lat)
    if (!Number.isFinite(x) || !Number.isFinite(y)) return false

    let inside = false
    for (let i = 0, j = polyLatLon.length - 1; i < polyLatLon.length; j = i++) {
        const yi = Number(polyLatLon[i][0])
        const xi = Number(polyLatLon[i][1])
        const yj = Number(polyLatLon[j][0])
        const xj = Number(polyLatLon[j][1])
        if (![xi, yi, xj, yj].every(Number.isFinite)) continue

        // Check if point is on a horizontal boundary intersection.
        const intersect = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / (yj - yi + 1e-15) + xi)
        if (intersect) inside = !inside
    }
    return inside
}

function featureDist2ToPoint(feature, lat, lon) {
    const n = featureNearestToPoint(feature, lat, lon)
    return Number.isFinite(n?.d2) ? n.d2 : Number.POSITIVE_INFINITY
}

function nearestFeatureToPoint(features, lat, lon) {
    if (!features?.length) return null
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null
    let best = null
    let bestD2 = Number.POSITIVE_INFINITY
    for (const f of features) {
        const d2 = featureDist2ToPoint(f, lat, lon)
        if (!Number.isFinite(d2)) continue
        if (d2 < bestD2) {
            bestD2 = d2
            best = f
        }
    }
    if (!best) return null
    return { feature: best, d2: bestD2 }
}

function approxMetersFromDegDist2(d2, atLatDeg) {
    if (!Number.isFinite(d2) || d2 < 0) return null
    const d = Math.sqrt(d2)
    const lat = Number(atLatDeg)
    const kmPerDegLat = 111.32
    const kmPerDegLon = 111.32 * Math.cos((lat * Math.PI) / 180)
    const kmPerDeg = (kmPerDegLat + kmPerDegLon) / 2
    return d * kmPerDeg * 1000
}

function featureScreenLengthPx(feature, map) {
    if (!feature || !map?.latLngToLayerPoint) return 0
    const g = feature?.geometry
    const coords = g?.coordinates
    if (!coords) return 0
    const lines = g.type === 'MultiLineString' ? coords : [coords]
    let total = 0
    for (const line of lines) {
        for (let i = 1; i < line.length; i++) {
            const a = line[i - 1]
            const b = line[i]
            const pa = map.latLngToLayerPoint([Number(a[1]), Number(a[0])])
            const pb = map.latLngToLayerPoint([Number(b[1]), Number(b[0])])
            if (!pa || !pb) continue
            const dx = Number(pb.x) - Number(pa.x)
            const dy = Number(pb.y) - Number(pa.y)
            const d = Math.hypot(dx, dy)
            if (Number.isFinite(d)) total += d
        }
    }
    return total
}

function snapParamsForZoom(zoom) {
    const z = Number(zoom)
    const zz = Number.isFinite(z) ? z : 14
    const snapMaxMeters = (zz <= 12) ? 650
        : (zz <= 13) ? 450
            : (zz <= 14) ? 280
                : (zz <= 15) ? 180
                    : 120
    const alphaUp = (zz <= 12) ? 2.5
        : (zz <= 13) ? 2.0
            : (zz <= 14) ? 1.0
                : (zz <= 15) ? 0.4
                    : 0.0
    return { snapMaxMeters, alphaUp }
}

function nearestFeatureIds(features, hotspot, count = 14) {
    if (!hotspot || !features?.length) return []
    const lat = Number(hotspot.lat)
    const lon = Number(hotspot.lon)
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return []
    return [...features]
        .map((f) => ({
            id: f?.properties?._fid,
            d2: featureDist2ToPoint(f, lat, lon),
        }))
        .filter((x) => Number.isFinite(x.d2) && x.id !== undefined)
        .sort((a, b) => a.d2 - b.d2)
        .slice(0, count)
        .map((x) => x.id)
}

function nearestHotspotToFeature(feature, hotspots) {
    if (!feature || !hotspots?.length) return null
    const geom = feature.geometry
    const coords = geom?.coordinates
    if (!coords) return null

    let latSum = 0
    let lonSum = 0
    let count = 0
    const lines = geom.type === 'MultiLineString' ? coords : [coords]
    for (const line of lines) {
        for (const [lon, lat] of line) {
            lonSum += Number(lon)
            latSum += Number(lat)
            count += 1
        }
    }
    if (!count) return null
    const cLat = latSum / count
    const cLon = lonSum / count

    let best = null
    let bestD2 = Number.POSITIVE_INFINITY
    for (const h of hotspots) {
        const hLat = Number(h.lat)
        const hLon = Number(h.lon)
        if (!Number.isFinite(hLat) || !Number.isFinite(hLon)) continue
        const dy = cLat - hLat
        const dx = cLon - hLon
        const d2 = dx * dx + dy * dy
        if (d2 < bestD2) {
            bestD2 = d2
            best = h
        }
    }
    return best
}

function PointCheckHandler({ geojson, enabled, onPick }) {
    const feats = geojson?.features || []
    useMapEvents({
        click: (evt) => {
            if (!enabled) return
            const lat = Number(evt?.latlng?.lat)
            const lon = Number(evt?.latlng?.lng)
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return

            const z = Number(evt?.target?.getZoom?.())
            const zoom = Number.isFinite(z) ? z : 14
            const { snapMaxMeters, alphaUp } = snapParamsForZoom(zoom)
            const map = evt?.target
            const minFeaturePx = (zoom >= 16) ? 8 : (zoom >= 14) ? 11 : 14

            let best = null
            let bestSnapLat = null
            let bestSnapLon = null
            let bestRank = Number.POSITIVE_INFINITY
            let bestD2 = Number.POSITIVE_INFINITY

            for (const f of feats) {
                if (map) {
                    const pxLen = featureScreenLengthPx(f, map)
                    if (Number.isFinite(pxLen) && pxLen > 0 && pxLen < minFeaturePx) continue
                }
                const nearest = featureNearestToPoint(f, lat, lon)
                const d2 = Number(nearest?.d2)
                if (!Number.isFinite(d2)) continue
                const meters = approxMetersFromDegDist2(d2, lat)
                if (!Number.isFinite(meters) || meters > snapMaxMeters) continue

                const upKm2 = upstreamKm2Of(f)
                const bonus = 1 + alphaUp * Math.log1p(upKm2 * 1000) // smooth preference for larger upstream area
                const rank = meters / bonus

                if (rank < bestRank) {
                    bestRank = rank
                    best = f
                    bestD2 = d2
                    bestSnapLat = Number(nearest?.snapLat)
                    bestSnapLon = Number(nearest?.snapLon)
                }
            }

            if (!best) {
                onPick?.({ lat, lon, found: false })
                return
            }

            const props = best?.properties || {}
            const meters = approxMetersFromDegDist2(bestD2, lat)
            onPick?.({
                lat: Number.isFinite(bestSnapLat) ? bestSnapLat : lat,
                lon: Number.isFinite(bestSnapLon) ? bestSnapLon : lon,
                found: true,
                distance_m: Number.isFinite(meters) ? Math.round(meters) : null,
                risk_score: props?.risk_score,
                risk_class: props?.risk_class,
                upstream_area_km2: props?.upstream_area_km2,
                slope_deg: props?.slope_deg,
                _fid: props?._fid,
            })
        },
    })
    return null
}

function SnapCursorHint({ geojson, enabled = true }) {
    const map = useMap()
    const feats = geojson?.features || []
    const rafRef = useRef(null)
    const lastRef = useRef({ lat: null, lon: null, z: null, state: null })

    const bboxIndex = useMemo(() => {
        // Precompute per-feature bbox for quick candidate filtering on mousemove.
        const out = []
        for (const f of feats) {
            const g = f?.geometry
            const coords = g?.coordinates
            if (!coords) continue
            const lines = g.type === 'MultiLineString' ? coords : [coords]
            let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity
            for (const line of lines) {
                for (const c of (line || [])) {
                    const lon = Number(c?.[0])
                    const lat = Number(c?.[1])
                    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue
                    if (lat < minLat) minLat = lat
                    if (lat > maxLat) maxLat = lat
                    if (lon < minLon) minLon = lon
                    if (lon > maxLon) maxLon = lon
                }
            }
            if (![minLat, maxLat, minLon, maxLon].every(Number.isFinite)) continue
            out.push({ f, minLat, maxLat, minLon, maxLon })
        }
        return out
    }, [feats])

    const setCursor = useCallback((state) => {
        const prev = lastRef.current.state
        if (prev === state) return
        lastRef.current.state = state
        try {
            const el = map?.getContainer?.()
            if (!el) return
            // Only override when we want to show the affordance.
            // Otherwise, let Leaflet manage the default (grab/grabbing etc.).
            el.style.cursor = (state === 'snap') ? 'pointer' : ''
        } catch {}
    }, [map])

    useMapEvents({
        mousemove: (evt) => {
            if (!enabled) return
            if (!map) return
            if (!bboxIndex.length) { setCursor(null); return }
            const lat = Number(evt?.latlng?.lat)
            const lon = Number(evt?.latlng?.lng)
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return
            const z = Number(map.getZoom?.())
            const zoom = Number.isFinite(z) ? z : 14

            const last = lastRef.current
            // Skip if essentially the same mouse position/zoom.
            if (last.lat !== null && Math.abs(lat - last.lat) < 1e-6 && Math.abs(lon - last.lon) < 1e-6 && last.z === zoom) return
            lastRef.current.lat = lat
            lastRef.current.lon = lon
            lastRef.current.z = zoom

            if (rafRef.current) cancelAnimationFrame(rafRef.current)
            rafRef.current = requestAnimationFrame(() => {
                const { snapMaxMeters, alphaUp } = snapParamsForZoom(zoom)
                const epsLat = snapMaxMeters / 111_320.0
                const cos = Math.cos((lat * Math.PI) / 180) || 1
                const epsLon = snapMaxMeters / (111_320.0 * Math.max(0.15, cos))

                let bestRank = Number.POSITIVE_INFINITY
                for (const it of bboxIndex) {
                    if (lat < it.minLat - epsLat || lat > it.maxLat + epsLat) continue
                    if (lon < it.minLon - epsLon || lon > it.maxLon + epsLon) continue
                    const d2 = featureDist2ToPoint(it.f, lat, lon)
                    if (!Number.isFinite(d2)) continue
                    const meters = approxMetersFromDegDist2(d2, lat)
                    if (!Number.isFinite(meters) || meters > snapMaxMeters) continue

                    const upKm2 = upstreamKm2Of(it.f)
                    const bonus = 1 + alphaUp * Math.log1p(upKm2 * 1000)
                    const rank = meters / bonus
                    if (rank < bestRank) bestRank = rank
                }

                setCursor(Number.isFinite(bestRank) ? 'snap' : null)
            })
        },
        mouseout: () => {
            setCursor(null)
        },
    })

    useEffect(() => () => {
        try { if (rafRef.current) cancelAnimationFrame(rafRef.current) } catch {}
        setCursor(null)
    }, [setCursor])

    return null
}

function PointCheckPopup({ pointCheck, onClose }) {
    const map = useMap()
    const fmtSig = useMemo(() => new Intl.NumberFormat('de-DE', { maximumSignificantDigits: 3 }), [])

    useEffect(() => {
        if (!map || !pointCheck) return
        const lat = Number(pointCheck.lat)
        const lon = Number(pointCheck.lon)
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return

        const score = pointCheck.found ? (pointCheck.risk_score ?? '-') : '-'
        const cls = pointCheck.found ? (pointCheck.risk_class ?? '-') : '-'
        const up = (pointCheck.found && Number.isFinite(Number(pointCheck.upstream_area_km2)))
            ? formatAreaCompact(km2ToM2(Number(pointCheck.upstream_area_km2)), fmtSig)
            : '-'
        const slope = (pointCheck.found && Number.isFinite(Number(pointCheck.slope_deg)))
            ? `${Number(pointCheck.slope_deg).toFixed(1)} deg`
            : '-'
        const dist = (pointCheck.found && Number.isFinite(Number(pointCheck.distance_m)))
            ? ` (~${Number(pointCheck.distance_m)} m)`
            : ''

        const html = pointCheck.found
            ? `<div class="pc">
                    <div class="pc-title">Objekt-Check</div>
                    <div class="pc-row"><span class="pc-k">Score</span><span class="pc-v">${score} <span class="pc-muted">(${cls})</span></span></div>
                    <div class="pc-row"><span class="pc-k">Einzugsgebiet</span><span class="pc-v">${up}</span></div>
                    <div class="pc-row"><span class="pc-k">Hang</span><span class="pc-v">${slope}${dist}</span></div>
                </div>`
            : `<div class="pc"><div class="pc-title">Objekt-Check</div><div class="pc-note">Kein Segment in der Naehe.</div></div>`

        const popup = L.popup({
            closeButton: true,
            autoClose: true,
            closeOnClick: false,
            className: 'pointcheck-popup',
            maxWidth: 260,
        })
            .setLatLng([lat, lon])
            .setContent(html)
            .openOn(map)

        const onPopupClose = () => onClose?.()
        map.on('popupclose', onPopupClose)

        return () => {
            try { map.off('popupclose', onPopupClose) } catch {}
            try { map.closePopup(popup) } catch {}
        }
    }, [map, pointCheck, onClose, fmtSig])

    return null
}

function FitBounds({ geojson, triggerKey, enabled = true, sidebarOpen = true }) {
    const map = useMap()
    const lastKeyRef = useRef(null)

    useEffect(() => {
        if (!enabled) return
        if (!map) return
        if (!geojson?.features?.length) return
        if (triggerKey !== undefined && triggerKey !== null && lastKeyRef.current === triggerKey) return

        const coords = []
        for (const f of geojson.features) {
            const g = f.geometry
            if (!g?.coordinates) continue
            if (g.type === 'LineString') {
                for (const c of g.coordinates) coords.push([c[1], c[0]])
            } else if (g.type === 'MultiLineString') {
                for (const line of g.coordinates) {
                    for (const c of line) coords.push([c[1], c[0]])
                }
            }
        }
        if (coords.length === 0) return

        lastKeyRef.current = triggerKey
        const leftPad = sidebarOpen ? 400 : 30
        map.fitBounds(coords, {
            paddingTopLeft: [leftPad, 30],
            paddingBottomRight: [30, 30],
        })
    }, [enabled, geojson, map, sidebarOpen, triggerKey])

    return null
}

function EnsureMapInteractivity({ enabled = true }) {
    const map = useMap()
    useEffect(() => {
        if (!enabled) return
        if (!map) return
        try {
            map.dragging?.enable?.()
            map.scrollWheelZoom?.enable?.()
            map.doubleClickZoom?.enable?.()
            map.touchZoom?.enable?.()
            map.boxZoom?.enable?.()
            map.keyboard?.enable?.()
            map.tap?.enable?.()
            try { map.getContainer().style.cursor = '' } catch {}
        } catch {}
    }, [enabled, map])
    return null
}

function DrawAreaHandler({ active, mode, onArea }) {
    const [rectStart, setRectStart] = useState(null)
    const [liveRect, setLiveRect] = useState(null)
    const [polyPoints, setPolyPoints] = useState([])
    const map = useMap()

    useEffect(() => {
        if (active) {
            map.getContainer().style.cursor = 'crosshair'
            map.dragging.disable()
            map.doubleClickZoom.disable()
        } else {
            map.getContainer().style.cursor = ''
            map.dragging.enable()
            map.doubleClickZoom.enable()
            setRectStart(null)
            setLiveRect(null)
            setPolyPoints([])
        }
        return () => {
            map.dragging.enable()
            map.doubleClickZoom.enable()
            map.getContainer().style.cursor = ''
        }
    }, [active, map])

    const finalizeRectangle = useCallback((a, b) => {
        const bounds = [[a.lat, a.lng], [b.lat, b.lng]]
        const south = Math.min(bounds[0][0], bounds[1][0])
        const north = Math.max(bounds[0][0], bounds[1][0])
        const west = Math.min(bounds[0][1], bounds[1][1])
        const east = Math.max(bounds[0][1], bounds[1][1])
        if (Math.abs(north - south) <= 0.0001 || Math.abs(east - west) <= 0.0001) return
        onArea({
            south, west, north, east,
            bounds,
            shapeType: 'rectangle',
            polygon: [
                [south, west], [south, east], [north, east], [north, west],
            ],
        })
    }, [onArea])

    const finalizePolygon = useCallback((points) => {
        if (!points || points.length < 3) return
        const bbox = bboxFromPoints(points)
        if (!bbox) return
        onArea({
            ...bbox,
            bounds: [[bbox.south, bbox.west], [bbox.north, bbox.east]],
            shapeType: 'polygon',
            polygon: points.map(([lat, lng]) => [lat, lng]),
        })
    }, [onArea])

    useEffect(() => {
        if (!active || mode !== 'polygon') return
        const onKeyDown = (e) => {
            const tag = (e.target && e.target.tagName) ? String(e.target.tagName).toLowerCase() : ''
            if (tag === 'input' || tag === 'textarea' || tag === 'select') return

            if (e.key === 'Escape' || e.key === 'Backspace' || e.key === 'Delete') {
                // Undo last vertex.
                e.preventDefault()
                setPolyPoints((prev) => prev.slice(0, Math.max(0, prev.length - 1)))
            }
            if ((e.ctrlKey || e.metaKey) && (e.key === 'z' || e.key === 'Z')) {
                e.preventDefault()
                setPolyPoints((prev) => prev.slice(0, Math.max(0, prev.length - 1)))
            }
        }
        window.addEventListener('keydown', onKeyDown)
        return () => window.removeEventListener('keydown', onKeyDown)
    }, [active, mode])

    useMapEvents({
        mousedown(e) {
            if (!active) return
            if (mode !== 'rectangle') return
            // Left mouse button only.
            if (e.originalEvent && e.originalEvent.button !== 0) return
            setRectStart(e.latlng)
            setLiveRect(null)
        },
        mousemove(e) {
            if (!active || !rectStart) return
            if (mode !== 'rectangle') return
            // Normalize bounds so Rectangle renders regardless of drag direction.
            const south = Math.min(rectStart.lat, e.latlng.lat)
            const north = Math.max(rectStart.lat, e.latlng.lat)
            const west = Math.min(rectStart.lng, e.latlng.lng)
            const east = Math.max(rectStart.lng, e.latlng.lng)
            setLiveRect([[south, west], [north, east]])
        },
        mouseup(e) {
            if (!active) return
            if (mode !== 'rectangle') return
            if (rectStart) {
                finalizeRectangle(rectStart, e.latlng)
                setRectStart(null)
                setLiveRect(null)
                setPolyPoints([])
            }
        },
        click(e) {
            if (!active || rectStart) return
            if (mode !== 'polygon') return
            setPolyPoints((prev) => {
                const nextPoint = [e.latlng.lat, e.latlng.lng]
                if (prev.length >= 3) {
                    const first = prev[0]
                    const dLat = first[0] - nextPoint[0]
                    const dLng = first[1] - nextPoint[1]
                    const closeToStart = (dLat * dLat + dLng * dLng) < (0.00012 * 0.00012)
                    if (closeToStart) {
                        finalizePolygon(prev)
                        return []
                    }
                }
                return [...prev, nextPoint]
            })
        },
        dblclick() {
            if (!active) return
            if (mode !== 'polygon') return
            setPolyPoints((prev) => {
                if (prev.length >= 3) finalizePolygon(prev)
                return []
            })
        },
        contextmenu() {
            if (!active) return
            setPolyPoints([])
            setRectStart(null)
            setLiveRect(null)
        },
    })

    return (
        <>
            {liveRect && (
                <Rectangle
                    bounds={liveRect}
                    pathOptions={{ color: '#00e5ff', weight: 2, dashArray: '6 4', fillColor: '#00e5ff', fillOpacity: 0.08 }}
                />
            )}
            {mode === 'polygon' && polyPoints.length >= 1 && polyPoints.map((p, idx) => (
                <CircleMarker
                    key={`vtx-${idx}`}
                    center={p}
                    radius={5}
                    pathOptions={{
                        color: '#ffffff',
                        weight: 2,
                        fillColor: '#00e5ff',
                        fillOpacity: 0.85,
                    }}
                />
            ))}
            {polyPoints.length >= 2 && (
                <Polyline positions={polyPoints} pathOptions={{ color: '#00e5ff', weight: 2, dashArray: '5 4' }} />
            )}
            {polyPoints.length >= 3 && (
                <Polygon positions={polyPoints} pathOptions={{ color: '#00e5ff', weight: 2, fillColor: '#00e5ff', fillOpacity: 0.08 }} />
            )}
        </>
    )
}

function DropZone({ onFile, disabled }) {
    const [dragOver, setDragOver] = useState(false)
    const inputRef = useRef()

    const handleDrag = useCallback((e) => {
        e.preventDefault()
        e.stopPropagation()
    }, [])

    const handleDrop = useCallback((e) => {
        handleDrag(e)
        setDragOver(false)
        if (disabled) return
        const file = e.dataTransfer?.files?.[0]
        if (file) onFile(file)
    }, [disabled, handleDrag, onFile])

    return (
        <div
            className={`dropzone${dragOver ? ' drag-over' : ''}`}
            onClick={() => inputRef.current?.click()}
            onDragEnter={(e) => { handleDrag(e); setDragOver(true) }}
            onDragOver={(e) => { handleDrag(e); setDragOver(true) }}
            onDragLeave={(e) => { handleDrag(e); setDragOver(false) }}
            onDrop={handleDrop}
        >
            <span className="dropzone-icon">DEM</span>
            <span className="dropzone-label">GeoTIFF hochladen</span>
            <span className="dropzone-hint">Datei ziehen oder klicken</span>
            <input
                ref={inputRef}
                type="file"
                accept=".tif,.tiff"
                onChange={(e) => {
                    const file = e.target.files?.[0]
                    if (file) onFile(file)
                }}
                disabled={disabled}
            />
        </div>
    )
}

function ProgressBar({ step, total, message }) {
    const pct = total > 0 ? Math.round((step / total) * 100) : 0
    return (
        <div className="progress-container">
            <div className="progress-label">
                <span className="spinner" />
                <span className="progress-step">{step}/{total}</span>
                {message}
            </div>
            <div className="progress-bar">
                <div className="progress-bar-fill" style={{ width: `${pct}%` }} />
            </div>
        </div>
    )
}

async function readNdjsonStream(response, onProgress, onResult, onError) {
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop()

        for (const line of lines) {
            if (!line.trim()) continue
            try {
                const evt = JSON.parse(line)
                if (evt.type === 'progress') onProgress(evt)
                else if (evt.type === 'result') onResult(evt.data)
                else if (evt.type === 'error') onError(evt.detail)
            } catch (err) {
                console.warn('NDJSON parse error', err)
            }
        }
    }
}

function ThresholdSlider({ value, onChange }) {
    return (
        <div className="section">
            <div className="section-title">Detailgrad (Threshold)</div>
            <div className="slider-row">
                <input
                    className="slider-input"
                    type="range"
                    min={50}
                    max={1000}
                    step={10}
                    value={value}
                    onChange={(e) => onChange(Number(e.target.value))}
                />
                <span className="slider-value">{value}</span>
            </div>
        </div>
    )
}

function StatsBox({ data }) {
    const [open, setOpen] = useState(false)
    if (!data) return null
    const nFeatures = data.features?.length ?? 0
    const metrics = data?.analysis?.metrics || {}
    const classes = data?.analysis?.class_distribution || {}
    const perf = data?.analysis?.performance || {}
    const assumptions = data?.analysis?.assumptions || {}

    return (
        <div className="section">
            <button className="accordion-header" onClick={() => setOpen((v) => !v)} type="button">
                Ergebnisse
                <span className={`accordion-chevron${open ? ' open' : ''}`}>v</span>
            </button>
            <div className={`accordion-body${open ? ' open' : ''}`}>
                <div className="stats-grid">
                    <div className="stat-card"><div className="stat-value">{nFeatures}</div><div className="stat-label">Fliesssegmente</div></div>
                    <div className="stat-card"><div className="stat-value">{metrics.risk_score_mean ?? '-'}</div><div className="stat-label">Risk avg</div></div>
                    <div className="stat-card"><div className="stat-value">{metrics.risk_score_max ?? '-'}</div><div className="stat-label">Risk max</div></div>
                    <div className="stat-card"><div className="stat-value">{metrics.aoi_area_km2 ?? '-'}</div><div className="stat-label">Flaeche km2</div></div>
                </div>
                <div className="stats-grid risk-stats-grid">
                    <div className="stat-card"><div className="stat-value">{classes.niedrig ?? 0}</div><div className="stat-label">Niedrig</div></div>
                    <div className="stat-card"><div className="stat-value">{classes.mittel ?? 0}</div><div className="stat-label">Mittel</div></div>
                    <div className="stat-card"><div className="stat-value">{classes.hoch ?? 0}</div><div className="stat-label">Hoch</div></div>
                    <div className="stat-card"><div className="stat-value">{classes.sehr_hoch ?? 0}</div><div className="stat-label">Sehr hoch</div></div>
                </div>
                {(perf.downsample_applied || perf.output_truncated) && (
                    <div className="perf-note">
                        {perf.downsample_applied && (
                            <div>
                                Large-AOI Modus: Raster reduziert auf {perf.work_width}x{perf.work_height} (Faktor ~{perf.scale_factor}).
                            </div>
                        )}
                        {perf.output_truncated && (
                            <div>
                                Ausgabe reduziert: {metrics.feature_count_output}/{metrics.feature_count} Segmente fuer stabile Darstellung.
                            </div>
                        )}
                    </div>
                )}
                <div className="source-note">
                    Quellen: Boden={assumptions.soil || 'proxy'}, Versiegelung={assumptions.impervious || 'proxy'}
                </div>
            </div>
        </div>
    )
}

function HotspotList({ data, selectedRank, onSelect }) {
    const [open, setOpen] = useState(false)
    const hotspots = data?.analysis?.hotspots || []
    if (!hotspots.length) return null

    return (
        <div className="section">
            <button className="accordion-header" onClick={() => setOpen((v) => !v)} type="button">
                Hotspots
                <span className={`accordion-chevron${open ? ' open' : ''}`}>v</span>
            </button>
            <div className={`accordion-body${open ? ' open' : ''}`}>
                <div className="hotspot-list">
                    {hotspots.map((h) => (
                        <button
                            type="button"
                            className={`hotspot-item hotspot-btn${selectedRank === h.rank ? ' selected' : ''}`}
                            key={`hot-${h.rank}`}
                            onClick={() => onSelect?.(h)}
                        >
                            <div className="hotspot-head">
                                <span className="hotspot-rank">#{h.rank}</span>
                                <span className={`risk-pill ${h.risk_class}`}>{h.risk_class}</span>
                                <span className="hotspot-score">{h.risk_score}</span>
                            </div>
                            {Number.isFinite(Number(h.upstream_area_km2)) && Number(h.upstream_area_km2) > 0 && (
                                <div className="hotspot-coord" style={{ opacity: 0.9 }}>
                                    Einzugsgebiet: {Number(h.upstream_area_km2).toFixed(2)} km2
                                </div>
                            )}
                            <div className="hotspot-coord">{Number(h.lat).toFixed(5)}, {Number(h.lon).toFixed(5)}</div>
                            <div className="hotspot-reason">{h.reason}</div>
                        </button>
                    ))}
                </div>
            </div>
        </div>
    )
}

function MeasuresPanel({ hotspot }) {
    const [open, setOpen] = useState(false)
    if (!hotspot) return null
    const measures = hotspot.measures || []
    if (!measures.length) return null

    return (
        <div className="section">
            <button className="accordion-header" onClick={() => setOpen((v) => !v)} type="button">
                Massnahmen (Hotspot #{hotspot.rank})
                <span className={`accordion-chevron${open ? ' open' : ''}`}>v</span>
            </button>
            <div className={`accordion-body${open ? ' open' : ''}`}>
                <div className="measures-list">
                    {measures.map((m) => (
                        <div className="measure-item" key={`ms-${m.id}`}>
                            <div className="measure-head">
                                <span className="measure-prio">P{m.priority}</span>
                                <span className="measure-title">{m.title}</span>
                            </div>
                            <div className="measure-meta">
                                Aufwand: {m.effort} | Dauer: {m.time}
                            </div>
                            <div className="measure-why"><strong>Warum:</strong> {m.why}</div>
                            <div className="measure-what"><strong>Was:</strong> {m.what}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    )
}

function ScenarioBox({ data }) {
    const scenarios = data?.analysis?.scenarios || []
    if (!scenarios.length) return null

    return (
        <div className="section">
            <div className="section-title">Szenarien (mm/1h)</div>
            <div className="scenario-list">
                {scenarios.map((s) => (
                    <div className="scenario-item" key={`sc-${s.rain_mm_per_h}`}>
                        <div className="scenario-rain">{s.rain_mm_per_h}</div>
                        <div className="scenario-metrics">
                            <div>Mean: {s.mean_score}</div>
                            <div>High: {s.high_share_percent}%</div>
                            <div>Very high: {s.very_high_share_percent}%</div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    )
}

function ExportBox({ data }) {
    if (!data) return null

    const onExportGeojson = () => {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/geo+json' })
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = `hydrowatch_result_${Date.now()}.geojson`
        a.click()
        URL.revokeObjectURL(a.href)
    }

    const onExportCsv = () => {
        const rows = [['rank', 'lat', 'lon', 'risk_score', 'risk_class', 'reason']]
        for (const h of data?.analysis?.hotspots || []) {
            rows.push([h.rank, h.lat, h.lon, h.risk_score, h.risk_class, (h.reason || '').replace(/,/g, ';')])
        }
        const text = rows.map((r) => r.join(',')).join('\n')
        const blob = new Blob([text], { type: 'text/csv;charset=utf-8;' })
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = `hydrowatch_hotspots_${Date.now()}.csv`
        a.click()
        URL.revokeObjectURL(a.href)
    }

    return (
        <div className="section">
            <div className="section-title">Export</div>
            <div className="export-actions">
                <button className="export-btn" onClick={onExportGeojson}>GeoJSON</button>
                <button className="export-btn" onClick={onExportCsv}>CSV (Hotspots)</button>
            </div>
        </div>
    )
}

function ActionSummary({ data }) {
    if (!data?.analysis) return null
    const metrics = data.analysis.metrics || {}
    const hotspots = data.analysis.hotspots || []
    const scenarios = data.analysis.scenarios || []
    const top = hotspots[0]
    const heavy = scenarios.find((s) => s.rain_mm_per_h === 100)

    return (
        <div className="section">
            <div className="section-title">Empfohlene naechste Schritte</div>
            <div className="action-summary">
                <div>
                    Risiko im Gebiet: <strong>{metrics.risk_score_mean ?? '-'} / 100</strong> (Max: {metrics.risk_score_max ?? '-'}).
                </div>
                {top && (
                    <div>
                        Prioritaet 1: Hotspot #{top.rank} ({top.risk_class}, Score {top.risk_score}) bei {Number(top.lat).toFixed(4)}, {Number(top.lon).toFixed(4)}.
                    </div>
                )}
                {heavy && (
                    <div>
                        Bei 100 mm/h liegen voraussichtlich <strong>{heavy.high_share_percent}%</strong> der Flaeche in hoher/ sehr hoher Klasse.
                    </div>
                )}
                <div>
                    Empfehlung: zuerst lokale Retention/Entsiegelung an Top-Hotspots planen und Wirkung mit 50/100 mm Szenario vergleichen.
                </div>
            </div>
        </div>
    )
}

function HelpPage() {
    return (
        <div className="help-page">
            <div className="help-page-inner">
                <div className="help-page-head">
                    <h1>Hydrowatch: Hilfe & Wissenschaftliche Methodik</h1>
                    <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                        <a className="help-back-btn" href="#/daten">Daten & Quellen</a>
                        <a className="help-back-btn" href="#/">Zurueck zur Karte</a>
                    </div>
                </div>

                <section className="help-page-section">
                    <h2>Ziel und Nutzen</h2>
                    <p>
                        Hydrowatch liefert eine indikative Risikoanalyse fuer Starkregenabfluss und Erosionsneigung.
                        Die Ergebnisse unterstuetzen Priorisierung von Flaechen, Hotspots und Massnahmen.
                    </p>
                </section>

                <section className="help-page-section">
                    <h2>Methodik in 6 Schritten</h2>
                    <ol>
                        <li>DEM einlesen und aufbereiten (Senken fuellen, Flats aufloesen).</li>
                        <li>D8-Fliessrichtung berechnen.</li>
                        <li>Fliessakkumulation berechnen.</li>
                        <li>Hangneigung aus DEM-Gradient ableiten.</li>
                        <li>Boden- und Versiegelungslayer auf AOI reprojizieren (windowed read).</li>
                        <li>Risiko-Score, Klassen, Hotspots und Szenarien berechnen.</li>
                    </ol>
                </section>

                <section className="help-page-section">
                    <h2>Score-Modell (v2)</h2>
                    <div className="help-page-formula">
                        Risk = 0.35 * Acc + 0.25 * Slope + 0.15 * Soil + 0.15 * Impervious + 0.10 * Rain
                    </div>
                    <p>
                        Alle Faktoren werden auf 0..1 normalisiert und anschliessend auf 0..100 skaliert.
                        Die Klassifizierung lautet: niedrig (&lt;45), mittel (45-69), hoch (70-84), sehr hoch (&gt;=85).
                    </p>
                </section>

                <section className="help-page-section">
                    <h2>Datenquellen und Fallback</h2>
                    <ul>
                        <li>Topographie (Sachsen-Anhalt): DGM1 (1 m) von LVermGeo Sachsen-Anhalt, lokal als COG-Katalog.</li>
                        <li>Boden (Sachsen-Anhalt): BGR BUEK250 als Raster (wenn konfiguriert).</li>
                        <li>Versiegelung (Sachsen-Anhalt): Copernicus HRL Imperviousness 10 m (wenn konfiguriert).</li>
                        <li>Wenn Layer fehlen: Proxy-Fallback, im Ergebnis transparent markiert (`external`/`proxy`).</li>
                    </ul>
                </section>

                <section className="help-page-section">
                    <h2>Interpretation</h2>
                    <ul>
                        <li>Hotspots markieren prioritaere Prufflaechen.</li>
                        <li>Szenarien 30/50/100 mm/h zeigen relative Belastungsanstiege.</li>
                        <li>Ergebnisse sind indikativ und nicht rechtsverbindlich.</li>
                    </ul>
                </section>

                <section className="help-page-section">
                    <h2>Wichtige Grenzen</h2>
                    <ul>
                        <li>Kein vollstaendiges 2D-Hydrodynamikmodell.</li>
                        <li>Qualitaet haengt von Layer-Aufloesung und Aktualitaet ab.</li>
                        <li>Bei grossen AOIs greift ein Performance-Modus (Downsampling/Output-Reduktion).</li>
                    </ul>
                </section>

                <section className="help-page-section">
                    <h2>Technische Referenzen im Projekt</h2>
                    <ul>
                        <li>Analysekern: <code>backend/processing.py</code></li>
                        <li>WCS/Provider: <code>backend/wcs_client.py</code></li>
                        <li>API-Streaming: <code>backend/main.py</code></li>
                        <li>UI-Interaktion: <code>frontend/src/App.jsx</code></li>
                        <li>Langfassung: <code>HILFE_WISSENSCHAFT.md</code></li>
                    </ul>
                </section>
            </div>
        </div>
    )
}

function HelpSection() {
    const [open, setOpen] = useState(false)
    return (
        <div className="section">
            <button className="accordion-header" onClick={() => setOpen((v) => !v)}>
                Hilfe & Methodik
                <span className={`accordion-chevron${open ? ' open' : ''}`}>v</span>
            </button>
            <div className={`accordion-body${open ? ' open' : ''}`}>
                <div className="help-grid">
                    <div className="help-block">
                        <div className="help-title">Bedienung</div>
                        <ul className="help-list">
                            <li>Gebiet: Zeichnen oder GeoJSON laden (im Gebiet-Block).</li>
                            <li>Polygon: Klicks setzen, auf Startpunkt klicken zum Abschliessen.</li>
                            <li>Polygon: letzten Punkt rueckgaengig mit Esc/Backspace/Delete/Ctrl+Z.</li>
                            <li>Rechteck: Shift gedrueckt halten und ziehen.</li>
                             <li>Lupe: Ortssuche.</li>
                             <li>Zielkreuz-Button: Zoom auf die gezeichnete AOI.</li>
                             <li>Layer-Button: Ebenen/Basiskarte umschalten.</li>
                              <li>Netz anzeigen: zeigt ein Abflussnetz; Netzdichte waehlen (Grob/Mittel/Fein) fuer die sichtbare Liniendichte.</li>
                              <li>Einzugsgebiet: beim Aktivieren wird kurz gerechnet (Busy-Cursor + "Berechne..." im Layer-Menue).</li>
                              <li>Klick in Karte: Objekt-Check (Risiko am Punkt).</li>
                             <li>Hotspot-Klick: Karte fokussiert + relevante Segmente hervorgehoben.</li>
                             <li>Daten & Quellen: i-Button oben rechts neben "RisikoKarte".</li>
                             <li>Merkt sich lokal: letzte Kartenposition und Einstellungen (z.B. Netzdichte, Basiskarte) im Browser-Speicher.</li>
                         </ul>
                     </div>
                    <div className="help-block">
                        <div className="help-title">Wissenschaftlicher Kern</div>
                        <ul className="help-list">
                            <li>Hydrologie: D8-Fliessrichtung und Fliessakkumulation (PySheds).</li>
                            <li>Terrain: Hangneigung aus DEM-Gradient.</li>
                            <li>Score v2: Kombination aus Akkumulation, Hang, Boden, Versiegelung, Regenhistorie.</li>
                            <li>Szenarien: relative Skalierung fuer 30/50/100 mm in 1h.</li>
                        </ul>
                    </div>
                    <div className="help-block">
                        <div className="help-title">Score-Formel (vereinfacht)</div>
                        <div className="help-formula">
                            Risk = 0.35 * Acc + 0.25 * Slope + 0.15 * Soil + 0.15 * Impervious + 0.10 * Rain
                        </div>
                        <div className="help-text">
                            Alle Terme werden auf 0..1 normiert, danach auf 0..100 skaliert.
                        </div>
                    </div>
                    <div className="help-block">
                         <div className="help-title">Interpretation</div>
                         <ul className="help-list">
                             <li>Risikoklassen sind indikativ, nicht rechtsverbindlich.</li>
                             <li>Hotspots priorisieren Begehung, Planung und Massnahmen.</li>
                             <li>Bei sehr grossen AOIs kann der Large-AOI-Modus aktiv sein.</li>
                         </ul>
                     </div>
                 </div>
            </div>
        </div>
    )
}

function Legend() {
    return (
        <div className="section">
            <div className="section-title">Legende</div>
            <div className="legend-row"><span className="legend-line" style={{ borderTop: `3px solid ${RISK_COLORS.sehr_hoch}` }} />Sehr hoch</div>
            <div className="legend-row"><span className="legend-line" style={{ borderTop: `3px solid ${RISK_COLORS.hoch}` }} />Hoch</div>
            <div className="legend-row"><span className="legend-line" style={{ borderTop: `3px solid ${RISK_COLORS.mittel}` }} />Mittel</div>
            <div className="legend-row"><span className="legend-line" style={{ borderTop: `3px solid ${RISK_COLORS.niedrig}` }} />Niedrig</div>
        </div>
    )
}

function ZoomControls({ geojson, area, sidebarOpen, onPlaceSelect }) {
    const map = useMap()
    const [searchOpen, setSearchOpen] = useState(false)

    const onZoomToAoi = useCallback(() => {
        const bounds = area?.bounds
        if (!bounds || bounds.length !== 2) return
        const leftPad = sidebarOpen ? 400 : 30
        map.fitBounds(bounds, {
            paddingTopLeft: [leftPad, 30],
            paddingBottomRight: [30, 30],
        })
    }, [area, map, sidebarOpen])

    return (
        <div className="map-controls-wrap">
            {searchOpen && (
                <div className="map-search-panel" role="dialog" aria-label="Ortssuche">
                    <div className="map-search-head">
                        <div className="map-search-title">Ort suchen</div>
                        <button
                            className="map-search-close"
                            onClick={() => setSearchOpen(false)}
                            aria-label="Ortssuche schliessen"
                            title="Schliessen"
                            type="button"
                        >
                            x
                        </button>
                    </div>
                    <PlaceSearchBox
                        variant="map"
                        autoFocus
                        // Important: Do not hard-bias the geocoder to the current AOI bbox.
                        // Users often search a place outside the current selection (e.g. start in Halle).
                        // AOI-bias can also lead to "no hits" for small AOIs depending on provider behavior.
                        bbox={null}
                        onSelect={(sel) => {
                            onPlaceSelect?.(sel)
                            setSearchOpen(false)
                        }}
                    />
                </div>
            )}

            <div className="map-controls">
                <button
                    className="map-ctrl-btn"
                    onClick={() => setSearchOpen((o) => !o)}
                    title="Ort suchen"
                    aria-label="Ort suchen"
                    type="button"
                >
                    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                        <path
                            d="M10.5 3a7.5 7.5 0 1 0 0 15a7.5 7.5 0 0 0 0-15zm0 2a5.5 5.5 0 1 1 0 11a5.5 5.5 0 0 1 0-11z"
                            fill="currentColor"
                        />
                        <path d="M16.6 16.6l4 4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                    </svg>
                </button>

                <button
                    className="map-ctrl-btn map-ctrl-btn-aoi"
                    onClick={onZoomToAoi}
                    title={area?.bounds ? 'Zoom auf Auswahl' : 'Kein Gebiet gewaehlt'}
                    disabled={!area?.bounds}
                    aria-label="Zoom auf Auswahl"
                    type="button"
                >
                    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                        <circle cx="12" cy="12" r="7.5" fill="none" stroke="currentColor" strokeWidth="2" />
                        <circle cx="12" cy="12" r="1.5" fill="currentColor" />
                        <path d="M12 2v4M12 18v4M2 12h4M18 12h4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                    </svg>
                </button>
                <button className="map-ctrl-btn" onClick={() => map.zoomIn()} title="Zoom in" type="button">+</button>
                <button className="map-ctrl-btn" onClick={() => map.zoomOut()} title="Zoom out" type="button">-</button>
            </div>
        </div>
    )
}

function PersistMapView({ onView }) {
    useMapEvents({
        moveend: (evt) => {
            const m = evt?.target
            if (!m) return
            const c = m.getCenter?.()
            const z = m.getZoom?.()
            if (!c || !Number.isFinite(c.lat) || !Number.isFinite(c.lng) || !Number.isFinite(z)) return
            onView?.({ lat: Number(c.lat), lon: Number(c.lng), zoom: Number(z) })
        },
        zoomend: (evt) => {
            const m = evt?.target
            if (!m) return
            const c = m.getCenter?.()
            const z = m.getZoom?.()
            if (!c || !Number.isFinite(c.lat) || !Number.isFinite(c.lng) || !Number.isFinite(z)) return
            onView?.({ lat: Number(c.lat), lon: Number(c.lng), zoom: Number(z) })
        },
    })
    return null
}

function zoomMinFracForNet(zoom) {
    const z = Number(zoom)
    if (!Number.isFinite(z)) return 0
    // Coarsen the network automatically when zoomed out.
    // This keeps large AOIs readable without extra UI toggles.
    if (z <= 12) return 0.15 // Grob
    if (z <= 14) return 0.05 // Mittel
    return 0.0 // Fein
}

function MapLayerPanel({ layers, onToggle, basemapKey, onBasemapChange, corridorDensity, onCorridorDensityChange, hasCorridors, minCorridorKm2, maxCorridorKm2, corridorTotalCount, corridorVisibleCount, hasPointCheck, catchmentLoading, catchmentMeta, catchmentError }) {
    const [open, setOpen] = useState(false)
    const panelRef = useRef(null)
    const dropdownRef = useRef(null)
    const density = CORRIDOR_DENSITY_PRESETS[Math.max(0, Math.min(CORRIDOR_DENSITY_PRESETS.length - 1, Number(corridorDensity) || 0))]
    const effMinKm2 = (Number.isFinite(Number(maxCorridorKm2)) && Number(maxCorridorKm2) > 0)
        ? Number(maxCorridorKm2) * Number(density?.min_frac_of_max || 0)
        : 0
    const effMinHa = km2ToHa(effMinKm2)
    const effMinHaLabel = Number.isFinite(Number(effMinHa))
        ? Number(effMinHa).toFixed(Number(effMinHa) >= 10 ? 0 : 1).replace('.', ',')
        : null
    const densityTitleBase = (Number(density?.min_frac_of_max || 0) > 0 && effMinHaLabel)
        ? `Netzdichte: ${density.label} (Einzugsgebiet ab ~${effMinHaLabel} ha)`
        : `Netzdichte: ${density.label}`
    const densityTitle = (Number.isFinite(Number(corridorTotalCount)) && Number.isFinite(Number(corridorVisibleCount)) && Number(corridorTotalCount) > 0)
        ? `${densityTitleBase} | Linien: ${Number(corridorVisibleCount)}/${Number(corridorTotalCount)}`
        : densityTitleBase

    const fmtSig = useMemo(() => new Intl.NumberFormat('de-DE', { maximumSignificantDigits: 3 }), [])
    const minM2 = km2ToM2(minCorridorKm2)
    const maxM2 = km2ToM2(maxCorridorKm2)
    const minLabel = formatAreaCompact(minM2, fmtSig)
    const maxLabel = formatAreaCompact(maxM2, fmtSig)

    const catchmentLabel = useMemo(() => {
        const km2 = Number(catchmentMeta?.area_km2)
        const ha = Number(catchmentMeta?.area_ha)
        const m2 = Number(catchmentMeta?.area_m2)
        if (Number.isFinite(km2) && km2 >= 1) return `${km2.toFixed(2).replace('.', ',')} km2`
        if (Number.isFinite(ha) && ha >= 1) return `${ha.toFixed(1).replace('.', ',')} ha`
        if (Number.isFinite(m2)) return `${Math.round(m2).toLocaleString('de-DE')} m2`
        return null
    }, [catchmentMeta])
    useEffect(() => {
        // Prevent map clicks ("Objekt-Check") when interacting with the layer menu.
        // Leaflet provides helpers that work more reliably than React stopPropagation alone.
        try {
            if (panelRef.current) {
                L.DomEvent.disableClickPropagation(panelRef.current)
                L.DomEvent.disableScrollPropagation(panelRef.current)
            }
            if (dropdownRef.current) {
                L.DomEvent.disableClickPropagation(dropdownRef.current)
                L.DomEvent.disableScrollPropagation(dropdownRef.current)
            }
        } catch {}
    }, [open])
    return (
        <div className="map-layer-panel" ref={panelRef}>
            <button
                className="map-ctrl-btn"
                onClick={(e) => { e.preventDefault(); e.stopPropagation(); setOpen((o) => !o) }}
                onMouseDown={(e) => e.stopPropagation()}
                onDoubleClick={(e) => e.stopPropagation()}
                title="Layers"
                type="button"
            >
                {open ? (
                    <span aria-hidden="true" style={{ fontSize: 20, lineHeight: 1 }}>x</span>
                ) : (
                    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                        <path
                            d="M12 3l9 5-9 5-9-5 9-5zm0 9l9-5v4l-9 5-9-5V7l9 5zm0 6l9-5v4l-9 5-9-5v-4l9 5z"
                            fill="currentColor"
                        />
                    </svg>
                )}
            </button>
            {open && (
                <div
                    className="map-layer-dropdown"
                    ref={dropdownRef}
                    onClick={(e) => e.stopPropagation()}
                    onMouseDown={(e) => e.stopPropagation()}
                    onDoubleClick={(e) => e.stopPropagation()}
                    onWheel={(e) => e.stopPropagation()}
                    onTouchStart={(e) => e.stopPropagation()}
                >
                    <div className="map-layer-section">Ebenen</div>
                    {layers.map((l) => {
                        if (l.id !== 'corridors') {
                            if (l.id === 'catchment') {
                                const disabled = !hasPointCheck
                                return (
                                    <label
                                        className={`map-layer-item${disabled ? ' is-disabled' : ''}`}
                                        key={l.id}
                                        title={disabled ? 'Klick auf ein Segment (Objekt-Check), dann kann das Einzugsgebiet berechnet werden.' : ''}
                                    >
                                        <input
                                            type="checkbox"
                                            checked={l.visible}
                                            disabled={disabled}
                                            onChange={() => { if (!disabled) onToggle(l.id) }}
                                        />
                                        <span className="layer-swatch" style={{ background: l.color }} />
                                        {l.name}
                                        {l.visible && !!catchmentLoading && (
                                            <span className="layer-inline-loading" title="Berechne...">
                                                <span className="tiny-spinner" aria-hidden="true" />
                                                <span className="layer-inline-loading-text">Berechne...</span>
                                            </span>
                                        )}
                                        {l.visible && !catchmentLoading && !!catchmentError && (
                                            <span className="layer-inline-loading layer-inline-error" title={String(catchmentError)}>
                                                Fehler
                                            </span>
                                        )}
                                        {l.visible && !catchmentLoading && !catchmentError && !!catchmentLabel && (
                                            <span className="layer-inline-loading layer-inline-ok" title="Flaeche Einzugsgebiet (indikativ)">
                                                {catchmentLabel}
                                            </span>
                                        )}
                                    </label>
                                )
                            }
                            return (
                                <label className="map-layer-item" key={l.id}>
                                    <input type="checkbox" checked={l.visible} onChange={() => onToggle(l.id)} />
                                    <span className="layer-swatch" style={{ background: l.color }} />
                                    {l.name}
                                </label>
                            )
                        }
                        if (!hasCorridors) return null
                        return (
                            <div className="map-layer-item-block" key={l.id}>
                                <label className="map-layer-item">
                                    <input type="checkbox" checked={l.visible} onChange={() => onToggle(l.id)} />
                                    <span className="layer-swatch" style={{ background: l.color }} />
                                    Netz anzeigen
                                </label>
                                {l.visible && (
                                    <div className="map-layer-subcontrol">
                                        <div className="map-layer-subrow">
                                            <span className="map-layer-subtitle">Netzdichte</span>
                                        </div>
                                        <div
                                            className="map-layer-density-segments"
                                            role="radiogroup"
                                            aria-label="Netzdichte"
                                            title={densityTitle}
                                        >
                                            {CORRIDOR_DENSITY_PRESETS.map((p, idx) => {
                                                const active = idx === (Number(corridorDensity) || 0)
                                                return (
                                                    <button
                                                        key={p.key}
                                                        type="button"
                                                        role="radio"
                                                        aria-checked={active}
                                                        className={`map-layer-density-seg${active ? ' active' : ''}`}
                                                        onClick={() => onCorridorDensityChange?.(idx)}
                                                        title={p.label}
                                                    >
                                                        {p.label}
                                                    </button>
                                                )
                                            })}
                                        </div>
                                        <div className="map-layer-net-legend-compact" title="Linienstaerke/Farbe ~ Einzugsgebiet (beitragende Flaeche), relativ in der aktuellen Auswahl">
                                          <span className="map-layer-net-ext map-layer-net-min">{minLabel}</span>
                                          <span className="map-layer-net-mini-wrap" aria-hidden="true">
                                            <span className="map-layer-net-mini net1" />
                                            <span className="map-layer-net-mini net2" />
                                            <span className="map-layer-net-mini net3" />
                                            <span className="map-layer-net-mini net4" />
                                          </span>
                                          <span className="map-layer-net-ext map-layer-net-max">{maxLabel}</span>
                                        </div>
                                    </div>
                                )}
                            </div>
                        )
                    })}
                    <div className="map-layer-section">Basiskarte</div>
                    {Object.entries(BASEMAPS).map(([key, bm]) => (
                        <label className="map-layer-item" key={key}>
                            <input type="radio" name="basemap" checked={basemapKey === key} onChange={() => onBasemapChange(key)} />
                            {bm.label}
                        </label>
                    ))}
                </div>
            )}
        </div>
    )
}

function HotspotNavigator({ hotspot }) {
    const map = useMap()

    useEffect(() => {
        if (!hotspot) return
        const lat = Number(hotspot.lat)
        const lon = Number(hotspot.lon)
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return
        const targetZoom = Math.max(map.getZoom(), 14)
        map.flyTo([lat, lon], targetZoom, { duration: 0.8 })
    }, [hotspot, map])

    return null
}

function CatchmentNavigator({ geojson, triggerKey, sidebarOpen = true, enabled = true }) {
    const map = useMap()
    const lastKeyRef = useRef(null)

    useEffect(() => {
        if (!enabled) return
        if (!geojson?.features?.length) return
        if (triggerKey !== undefined && triggerKey !== null && lastKeyRef.current === triggerKey) return

        const coords = []
        for (const f of geojson.features) {
            const g = f.geometry
            const c = g?.coordinates
            if (!c) continue
            // Support Polygon / MultiPolygon (GeoJSON lon/lat)
            if (g.type === 'Polygon') {
                for (const ring of c) for (const pt of ring) coords.push([pt[1], pt[0]])
            } else if (g.type === 'MultiPolygon') {
                for (const poly of c) for (const ring of poly) for (const pt of ring) coords.push([pt[1], pt[0]])
            }
        }
        if (!coords.length) return

        // Guard against bad CRS (e.g. meters interpreted as lon/lat) to avoid "zoom to Europe".
        const valid = []
        let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity
        for (const [lat, lon] of coords) {
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue
            valid.push([lat, lon])
            if (lat < minLat) minLat = lat
            if (lat > maxLat) maxLat = lat
            if (lon < minLon) minLon = lon
            if (lon > maxLon) maxLon = lon
        }
        if (valid.length < 3) return

        const spanLat = maxLat - minLat
        const spanLon = maxLon - minLon
        if (!Number.isFinite(spanLat) || !Number.isFinite(spanLon)) return
        if (spanLat > 30 || spanLon > 30) return

        lastKeyRef.current = triggerKey
        const leftPad = sidebarOpen ? 400 : 30
        map.fitBounds(valid, {
            paddingTopLeft: [leftPad, 30],
            paddingBottomRight: [30, 30],
            maxZoom: 15,
        })
    }, [enabled, geojson, map, sidebarOpen, triggerKey])

    return null
}

function FlyToHandler({ selection, sidebarOpen }) {
    const map = useMap()

    useEffect(() => {
        if (!selection || !map) return
        const lat = Number(selection.lat)
        const lon = Number(selection.lon)
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return

        const bbox = selection.bbox
        if (Array.isArray(bbox) && bbox.length === 4) {
            const south = Number(bbox[0])
            const north = Number(bbox[1])
            const west = Number(bbox[2])
            const east = Number(bbox[3])
            if ([south, north, west, east].every(Number.isFinite)) {
                const leftPad = sidebarOpen ? 400 : 30
                map.fitBounds([[south, west], [north, east]], {
                    paddingTopLeft: [leftPad, 30],
                    paddingBottomRight: [30, 30],
                })
                return
            }
        }

        const targetZoom = Math.max(map.getZoom(), 13)
        map.flyTo([lat, lon], targetZoom, { duration: 0.8 })
    }, [selection, map, sidebarOpen])

    return null
}

function PlaceSearchBox({ bbox, onSelect, autoFocus = false, variant = 'sidebar' }) {
    const [q, setQ] = useState('')
    const [open, setOpen] = useState(false)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState(null)
    const [results, setResults] = useState([])
    const inputRef = useRef(null)
    const cacheRef = useRef(new Map())
    const inflightRef = useRef(false)
    const pendingRef = useRef(null) // string | null
    const skipNextRef = useRef(null) // string | null
    const ignoreFocusUntilRef = useRef(0)

    useEffect(() => {
        if (!autoFocus) return
        // Defer focus to after mount/layout so the panel can position itself.
        const t = setTimeout(() => inputRef.current?.focus(), 0)
        return () => clearTimeout(t)
    }, [autoFocus])

    const bboxSouth = bbox?.south
    const bboxWest = bbox?.west
    const bboxNorth = bbox?.north
    const bboxEast = bbox?.east

    useEffect(() => {
        const runSearch = async (query, controller) => {
            const cacheKey = query.toLowerCase()
            const cached = cacheRef.current.get(cacheKey)
            if (Array.isArray(cached) && cached.length > 0) {
                setResults(cached)
                setOpen(true)
                setError(null)
                return
            }

            setLoading(true)
            setOpen(true) // show "Suche..." immediately (otherwise it feels broken on slow providers)
            setError(null)
            let timeout = null
            try {
                timeout = setTimeout(() => {
                    try { controller.abort() } catch {}
                }, 6500)
                const params = new URLSearchParams({ q: query, limit: '6' })
                if (bboxSouth !== undefined && bboxWest !== undefined && bboxNorth !== undefined && bboxEast !== undefined) {
                    params.set('south', String(bboxSouth))
                    params.set('west', String(bboxWest))
                    params.set('north', String(bboxNorth))
                    params.set('east', String(bboxEast))
                }
                const res = await fetch(`${LEGACY_API_URL}/geocode?${params.toString()}`, { signal: controller.signal })
                if (!res.ok) {
                    const txt = await res.text()
                    let detail = txt
                    try { detail = JSON.parse(txt).detail } catch {}
                    throw new Error(detail)
                }
                const json = await res.json()
                const next = json?.results || []
                cacheRef.current.set(cacheKey, next)
                setResults(next)
                setOpen(true)
            } catch (e) {
                if (e?.name === 'AbortError') return
                setError(e?.message || String(e))
                setResults([])
                setOpen(true)
            } finally {
                if (timeout) clearTimeout(timeout)
                setLoading(false)
            }
        }

        const query = q.trim()
        if (skipNextRef.current && query === skipNextRef.current) {
            skipNextRef.current = null
            return
        }
        if (query.length < 2) {
            setResults([])
            setError(null)
            return
        }
        const ac = new AbortController()
        const t = setTimeout(async () => {
            // Ensure only one network request at a time to avoid server-side rate-limit queues.
            if (inflightRef.current) {
                pendingRef.current = query
                return
            }

            inflightRef.current = true
            try {
                await runSearch(query, ac)
            } finally {
                inflightRef.current = false
                const pending = pendingRef.current
                pendingRef.current = null
                if (pending && pending !== query && pending.trim().length >= 2) {
                    // Fire the latest query after the current one finishes.
                    const ac2 = new AbortController()
                    await runSearch(pending, ac2)
                }
            }
        }, 260)
        return () => {
            ac.abort()
            clearTimeout(t)
        }
    }, [q, bboxSouth, bboxWest, bboxNorth, bboxEast])

    const pick = useCallback((r) => {
        if (!r) return
        // Prevent immediate re-query for the picked label, and prevent focus-reopen.
        skipNextRef.current = (r.display_name || '').trim()
        ignoreFocusUntilRef.current = Date.now() + 600
        setQ(r.display_name || '')
        setOpen(false)
        setResults([])
        setError(null)
        onSelect?.({
            label: r.display_name,
            lat: r.lat,
            lon: r.lon,
            bbox: r.boundingbox ? [r.boundingbox[0], r.boundingbox[1], r.boundingbox[2], r.boundingbox[3]] : null,
        })
    }, [onSelect])

    // bbox is reserved for future: bias results to AOI / state selection.
    void bbox
    const query = q.trim()

    return (
        <div className={`place-search${variant === 'map' ? ' place-search--map' : ''}`}>
            <div className="place-search-row">
                <input
                    ref={inputRef}
                    className="place-search-input"
                    placeholder="Ort suchen (z.B. Halle, Magdeburg, Koeln)"
                    value={q}
                    onChange={(e) => {
                        const next = e.target.value
                        setQ(next)
                        const t = next.trim()
                        if (t.length >= 2) setOpen(true)
                        if (t.length === 0) { setResults([]); setError(null); setOpen(false) }
                    }}
                    autoFocus={!!autoFocus}
                    onFocus={() => {
                        if (Date.now() < ignoreFocusUntilRef.current) return
                        // Only open if there is something to show.
                        if (loading || error || (results && results.length > 0) || query.length >= 2) setOpen(true)
                    }}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' && results?.[0]) pick(results[0])
                        if (e.key === 'Escape') setOpen(false)
                    }}
                />
                {q.trim().length > 0 && (
                    <button
                        className="place-search-btn"
                        onClick={() => { setQ(''); setResults([]); setError(null); setOpen(false) }}
                        title="Clear"
                        aria-label="Clear"
                        type="button"
                    >
                        x
                    </button>
                )}
            </div>
            {open && (loading || error || query.length >= 2) && (
                <div className="place-search-dropdown">
                    {loading && <div className="place-search-item muted">Suche...</div>}
                    {error && <div className="place-search-item error">Fehler: {error}</div>}
                    {!loading && !error && results?.map((r) => (
                        <button
                            key={`${r.lat},${r.lon},${r.display_name}`}
                            className="place-search-item"
                            onClick={() => pick(r)}
                            type="button"
                        >
                            {r.display_name}
                        </button>
                    ))}
                    {!loading && !error && (!results || results.length === 0) && (
                        <div className="place-search-item muted">Keine Treffer.</div>
                    )}
                </div>
            )}
        </div>
    )
}

function DataSourcesPage() {
    return (
        <div className="help-page">
            <div className="help-page-inner">
                <div className="help-page-head">
                    <h1>RisikoKarte: Daten & Quellen</h1>
                    <a className="help-back-btn" href="#/">Zurueck zur Karte</a>
                </div>

                <section className="help-page-section">
                    <h2>Kurz</h2>
                    <p>
                        DGM1 (1 m): LVermGeo Sachsen-Anhalt (Geodatenportal), Open Data. Verarbeitung: COG-Kacheln.
                    </p>
                    <p>
                        Ergebnisse sind eine Ersteinschaetzung (Screening) und ersetzen kein Gutachten.
                    </p>
                </section>

                <section className="help-page-section">
                    <h2>DGM1 Sachsen-Anhalt</h2>
                    <p>
                        Digitales Gelaendemodell DGM1 (1 m), Sachsen-Anhalt Ã¢â‚¬â€œ Quelle: Landesamt fuer Vermessung und Geoinformation Sachsen-Anhalt (LVermGeo),
                        bereitgestellt ueber das Geodatenportal Sachsen-Anhalt (Open Data). Nutzung in der App: lokaler COG-Katalog (`D:\data\st_dgm1_cog`).
                        Verarbeitung: Umwandlung in Cloud Optimized GeoTIFF (COG), Kachelung/Indexierung fuer performante Abfragen.
                    </p>
                </section>

                <section className="help-page-section">
                    <h2>Optionale Referenzlayer (falls eingeblendet)</h2>
                    <ul>
                        <li>Starkregen-Hinweiskarte: BKG (WMS), nur Referenzdarstellung.</li>
                        <li>Hochwasser/UEG: zustaendige Landesbehoerden (WMS), amtliche Referenz.</li>
                        <li>Bodendaten: BGR BUEK250 (Sachsen-Anhalt-Ausschnitt).</li>
                        <li>Landbedeckung: ESA WorldCover 10 m.</li>
                        <li>Versiegelung: Copernicus HRL Imperviousness 10 m.</li>
                    </ul>
                </section>

                <section className="help-page-section">
                    <h2>Disclaimer (Langfassung)</h2>
                    <p>
                        Die App liefert modellbasierte Ersteinschaetzungen auf Basis oeffentlich verfuegbarer Geodaten und vereinfachter Annahmen.
                        Lokale Gegebenheiten (Kanalnetz, Bauwerke, Bodenfeuchte, Bewirtschaftung, aktuelle Verstopfungen etc.) koennen die Realitaet stark beeinflussen.
                        Fuer Planungen und rechtssichere Bewertungen sind fachliche Detailuntersuchungen erforderlich.
                    </p>
                </section>
            </div>
        </div>
    )
}

function WmsOverlay({ visible, baseUrl, layerName, opacity = 0.6, zIndex = 350 }) {
    const map = useMap()

    useEffect(() => {
        if (!map || !visible || !baseUrl || !layerName) return
        const layer = L.tileLayer.wms(baseUrl, {
            layers: layerName,
            format: 'image/png',
            transparent: true,
            opacity,
            zIndex,
        })
        layer.addTo(map)
        return () => {
            try {
                map.removeLayer(layer)
            } catch {}
        }
    }, [map, visible, baseUrl, layerName, opacity, zIndex])

    return null
}

function WeatherPanel({ bbox, analysisType = 'starkregen', selectedEvent = null, onSelectEvent = null, disabled = false, onStateChange = null }) {
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState(null)
    const [data, setData] = useState(null)
    const [progress, setProgress] = useState({ step: 0, total: 0, message: '' })

    const defaultRange = useMemo(() => {
        const end = new Date()
        const start = new Date(end.getTime() - 90 * 24 * 3600 * 1000)
        const toIso = (d) => d.toISOString().slice(0, 10)
        return { start: toIso(start), end: toIso(end) }
    }, [])
    const [range, setRange] = useState(defaultRange)
    const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), [])

    const canRun = !!(bbox && Number.isFinite(bbox.south) && Number.isFinite(bbox.west) && Number.isFinite(bbox.north) && Number.isFinite(bbox.east))
    const bboxKey = useMemo(() => {
        if (!canRun) return 'none'
        return `${Number(bbox.south).toFixed(6)}:${Number(bbox.west).toFixed(6)}:${Number(bbox.north).toFixed(6)}:${Number(bbox.east).toFixed(6)}`
    }, [canRun, bbox])

    const samplePointsAuto = useCallback((b) => {
        const south = Number(b?.south)
        const west = Number(b?.west)
        const north = Number(b?.north)
        const east = Number(b?.east)
        if (![south, west, north, east].every(Number.isFinite)) return null
        const latC = (south + north) / 2
        const lonC = (west + east) / 2

        const dLat = Math.abs(north - south)
        const dLon = Math.abs(east - west)
        const latMidRad = ((south + north) / 2.0) * (Math.PI / 180.0)
        const kmPerDegLat = 111.32
        const kmPerDegLon = 111.32 * Math.max(0.01, Math.abs(Math.cos(latMidRad)))
        const areaKm2 = dLat * kmPerDegLat * dLon * kmPerDegLon
        if (areaKm2 <= 3.0) return `${latC.toFixed(5)},${lonC.toFixed(5)}`

        const latSpan = Math.max(0, north - south)
        const lonSpan = Math.max(0, east - west)
        const insetFrac = 0.10
        const latInset = Math.min(latSpan * insetFrac, latSpan * 0.45)
        const lonInset = Math.min(lonSpan * insetFrac, lonSpan * 0.45)
        const s = south + latInset
        const n = north - latInset
        const w = west + lonInset
        const e = east - lonInset
        if (areaKm2 <= 25.0) {
            const pts = [[latC, lonC], [s, w], [s, e], [n, w], [n, e]]
            return pts.map(([lat, lon]) => `${lat.toFixed(5)},${lon.toFixed(5)}`).join(';')
        }
        const latMid = (s + n) / 2
        const lonMid = (w + e) / 2
        const pts = [
            [s, w], [s, lonMid], [s, e],
            [latMid, w], [latMid, lonMid], [latMid, e],
            [n, w], [n, lonMid], [n, e],
        ]
        return pts.map(([lat, lon]) => `${lat.toFixed(5)},${lon.toFixed(5)}`).join(';')
    }, [])

    const summarizeStats = useCallback((resp) => {
        const per = resp?.stats?.perPoint || []
        if (!Array.isArray(per) || per.length === 0) return null
        const order = { trocken: 0, normal: 1, nass: 2 }
        const classes = per
            .map((p) => p?.antecedent_moisture?.class)
            .filter((c) => c && typeof c === 'string' && c in order)
        const counts = new Map()
        for (const c of classes) counts.set(c, (counts.get(c) || 0) + 1)
        let majority = null
        let best = -1
        for (const [k, v] of counts.entries()) {
            if (v > best) { best = v; majority = k }
        }
        const minClass = classes.length ? classes.reduce((a, b) => (order[a] <= order[b] ? a : b)) : null
        const maxClass = classes.length ? classes.reduce((a, b) => (order[a] >= order[b] ? a : b)) : null

        const getQ = (p, qStr) => {
            const qm = p?.precip_hourly?.quantiles_mm
            if (!qm) return null
            const v = qm[qStr] ?? null
            return typeof v === 'number' && Number.isFinite(v) ? v : null
        }
        const pickSeries = (qStr) => per.map((p) => getQ(p, qStr)).filter((v) => Number.isFinite(v))
        const median = (arr) => {
            if (!arr.length) return null
            const a = [...arr].sort((x, y) => x - y)
            const mid = Math.floor(a.length / 2)
            return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2
        }
        const mm = (arr) => arr.length ? { min: Math.min(...arr), max: Math.max(...arr), med: median(arr) } : null

        return {
            n: per.length,
            moisture: { majority, minClass, maxClass },
            q90: mm(pickSeries('0.9')),
            q95: mm(pickSeries('0.95')),
            q99: mm(pickSeries('0.99')),
        }
    }, [])

    const timelineEvents = useMemo(() => {
        const all = data?.events?.mergedTop || []
        const xs = [...(Array.isArray(all) ? all : [])]
        xs.sort((a, b) => String(a?.peak_ts || '').localeCompare(String(b?.peak_ts || '')))
        // Keep chart readable in narrow sidebar: show most recent events only.
        const MAX_BARS = 8
        return xs.length > MAX_BARS ? xs.slice(xs.length - MAX_BARS) : xs
    }, [data])

    const timelineMax = useMemo(() => {
        if (!timelineEvents.length) return 1
        let m = 1
        for (const ev of timelineEvents) {
            const v1 = Number(ev?.max_1h_mm) || 0
            const v6 = Number(ev?.max_6h_mm) || 0
            const score = Math.max(v1, v6 / 2)
            if (score > m) m = score
        }
        return m
    }, [timelineEvents])

    const pickNearestEventToNow = useCallback((events) => {
        if (!Array.isArray(events) || events.length === 0) return null
        const now = Date.now()
        let best = null
        let bestDt = Number.POSITIVE_INFINITY
        for (const ev of events) {
            const t = Date.parse(String(ev?.peak_ts || ''))
            if (!Number.isFinite(t)) continue
            const d = Math.abs(t - now)
            if (d < bestDt) {
                bestDt = d
                best = ev
            }
        }
        return best
    }, [])

    const run = useCallback(async () => {
        if (!canRun) return
        setLoading(true)
        setError(null)
        setData(null)
        setProgress({ step: 1, total: 6, message: 'AOI wird vorbereitet...' })
        try {
            const points = samplePointsAuto(bbox)
            if (!points) throw new Error('Ungueltige AOI')
            setProgress({ step: 2, total: 6, message: 'Wetterstatistik wird geladen...' })

            const fetchForRange = async (rr) => {
                const params = new URLSearchParams({
                    points,
                    quantiles: '0.9,0.95,0.99',
                    start: rr.start,
                    end: rr.end,
                })
                const statsRes = await fetch(`${LEGACY_API_URL}/abflussatlas/weather/stats?${params.toString()}`)
                if (!statsRes.ok) {
                    const txt = await statsRes.text()
                    let detail = txt
                    try { detail = JSON.parse(txt).detail } catch {}
                    throw new Error(detail)
                }
                setProgress((p) => ({ ...p, step: Math.max(3, Number(p.step) || 0), message: 'Ereignisse werden geladen...' }))
                const eventsRes = await fetch(`${LEGACY_API_URL}/abflussatlas/weather/events?${params.toString()}&source=hybrid_radar`)
                if (!eventsRes.ok) {
                    const txt = await eventsRes.text()
                    let detail = txt
                    try { detail = JSON.parse(txt).detail } catch {}
                    throw new Error(detail)
                }
                const statsJson = await statsRes.json()
                const eventsJson = await eventsRes.json()
                const summary = summarizeStats(statsJson)
                if (!summary) throw new Error('Keine Wetterdaten im Zeitfenster.')
                return {
                    summary,
                    stats: statsJson,
                    events: eventsJson?.events || { mergedTop: [], perPoint: [] },
                    endClamped: !!(statsJson?.meta?.endClampedToToday || eventsJson?.meta?.endClampedToToday),
                }
            }

            const daysInRange = (r) => {
                const s = new Date(`${r.start}T00:00:00Z`)
                const e = new Date(`${r.end}T00:00:00Z`)
                const d = Math.round((e.getTime() - s.getTime()) / (24 * 3600 * 1000)) + 1
                return Number.isFinite(d) ? Math.max(1, d) : 1
            }
            const buildRangeFromEnd = (endIso, days) => {
                const e = new Date(`${endIso}T00:00:00Z`)
                const s = new Date(e.getTime() - (Math.max(1, days) - 1) * 24 * 3600 * 1000)
                const toIso = (d) => d.toISOString().slice(0, 10)
                return { start: toIso(s), end: toIso(e) }
            }

            const initialRange = { start: range.start, end: range.end }
            setProgress({ step: 3, total: 6, message: 'Ursprungszeitraum wird ausgewertet...' })
            let result = await fetchForRange(initialRange)
            let usedRange = initialRange

            const initialCount = Number(result?.events?.mergedTop?.length || 0)
            if (initialCount === 0) {
                setProgress({ step: 4, total: 6, message: 'Keine Ereignisse gefunden, Zeitraum wird erweitert...' })
                const nowDays = daysInRange(initialRange)
                const expansionTargets = [365, 730]
                for (const spanDays of expansionTargets) {
                    if (nowDays >= spanDays) continue
                    const tryRange = buildRangeFromEnd(initialRange.end, spanDays)
                    const tryResult = await fetchForRange(tryRange)
                    if (Number(tryResult?.events?.mergedTop?.length || 0) > 0) {
                        result = tryResult
                        usedRange = tryRange
                        break
                    }
                }
            }

            setRange(usedRange)
            setData(result)
            const nearest = pickNearestEventToNow(result?.events?.mergedTop || [])
            onSelectEvent?.(nearest || null)
            setProgress({ step: 6, total: 6, message: 'Wetterdaten fertig geladen.' })
        } catch (e) {
            setError(e?.message || String(e))
            setProgress({ step: 0, total: 0, message: '' })
        } finally {
            setLoading(false)
        }
    }, [bbox, canRun, onSelectEvent, pickNearestEventToNow, range, samplePointsAuto, summarizeStats])

    const setStart = useCallback((v) => setRange((r) => ({ ...r, start: v })), [])
    const setEnd = useCallback((v) => {
        if (!v) return
        const end = v > todayIso ? todayIso : v
        setRange((r) => ({ ...r, end }))
    }, [todayIso])

    const toggleSelectEvent = useCallback((ev) => {
        const isSel = selectedEvent && selectedEvent.peak_ts === ev?.peak_ts && selectedEvent.point === ev?.point
        onSelectEvent?.(isSel ? null : ev)
    }, [onSelectEvent, selectedEvent])

    useEffect(() => {
        setData(null)
        setError(null)
        onSelectEvent?.(null)
    }, [bboxKey, onSelectEvent])

    useEffect(() => {
        onStateChange?.({
            ready: !!(canRun && data?.summary),
            loading: !!loading,
            error: error || null,
        })
    }, [onStateChange, canRun, data, loading, error])

    return (
        <div className={`section${disabled ? ' is-locked' : ''}`}>
            <div className="section-title">Regenereignisse Zeitraum:</div>
            <div className="bbox-info" style={{ marginTop: 8 }}>
                    <div className="bbox-actions weather-date-row" style={{ gap: 10, marginTop: 8, alignItems: 'end' }}>
                        <label className="weather-date-field" aria-label="Startdatum">
                            <input className="weather-date-input" type="date" value={range.start} max={todayIso} onChange={(e) => setStart(e.target.value)} disabled={disabled || loading} />
                        </label>
                        <label className="weather-date-field" aria-label="Endedatum">
                            <input className="weather-date-input" type="date" value={range.end} max={todayIso} onChange={(e) => setEnd(e.target.value)} disabled={disabled || loading} />
                        </label>
                        <button
                            className="bbox-analyze-btn"
                            type="button"
                            onClick={run}
                            disabled={disabled || !canRun || loading}
                            style={{ flex: '0 0 auto', width: 38, minWidth: 38, padding: '8px 0', marginBottom: 1, display: 'grid', placeItems: 'center' }}
                            title={loading ? 'Wetter wird geladen...' : 'Wetter/Ereignisse neu berechnen'}
                            aria-label={loading ? 'Wetter wird geladen' : 'Wetter/Ereignisse neu berechnen'}
                        >
                            {loading ? (
                                <span className="weather-run-spinner" aria-hidden="true" />
                            ) : (
                                <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
                                    <path d="M8 5l10 7l-10 7V5z" fill="currentColor" />
                                </svg>
                            )}
                        </button>
                    </div>
                    {loading && (
                        <div className="weather-progress" aria-live="polite">
                            <div className="weather-progress-row">
                                <span className="weather-progress-step">{progress.step || 0}/{progress.total || 0}</span>
                                <span className="weather-progress-msg">{progress.message || 'Wetter wird geladen...'}</span>
                            </div>
                            <div className="weather-progress-track" aria-hidden="true">
                                <span
                                    className="weather-progress-fill"
                                    style={{ width: `${(Number(progress.total) > 0) ? Math.max(0, Math.min(100, Math.round((Number(progress.step || 0) / Number(progress.total)) * 100))) : 0}%` }}
                                />
                            </div>
                        </div>
                    )}
                    {!canRun && <div className="aoi-warning" style={{ marginTop: 10 }}>Bitte zuerst ein Gebiet waehlen.</div>}
                    {disabled && <div className="aoi-warning" style={{ marginTop: 10 }}>Bitte zuerst Gebiet fertig festlegen.</div>}
                    {error && <p className="status-error">Fehler: {error}</p>}

                    {data?.summary && (
                        <div className="stats-box" style={{ marginTop: 10 }}>
                            {data?.endClamped && (
                                <div className="aoi-warning" style={{ marginBottom: 8 }}>
                                    Ende lag in der Zukunft und wurde auf heute begrenzt.
                                </div>
                            )}
                            <div className="weather-stats-compact" style={{ marginBottom: 8 }}>
                                <div className="weather-stats-line">
                                    <span className="k">Vorfeuchte:</span> <span className="v">{data.summary.moisture.majority || '-'}</span>
                                    <span className="sep">|</span>
                                    <span className="k" title="90%-Perzentil der stuendlichen Niederschlaege im gewaehlten Zeitraum (kein Live-Wert).">P90 Stunde:</span> <span className="v">{data.summary.q90 ? `${data.summary.q90.med.toFixed(1)} mm/h` : '-'}</span>
                                </div>
                                <div className="weather-stats-line">
                                    <span className="k" title="95%-Perzentil der stuendlichen Niederschlaege im gewaehlten Zeitraum (kein Live-Wert).">P95 Stunde:</span> <span className="v">{data.summary.q95 ? `${data.summary.q95.med.toFixed(1)} mm/h` : '-'}</span>
                                    <span className="sep">|</span>
                                    <span className="k" title="99%-Perzentil der stuendlichen Niederschlaege im gewaehlten Zeitraum (kein Live-Wert).">P99 Stunde:</span> <span className="v">{data.summary.q99 ? `${data.summary.q99.med.toFixed(1)} mm/h` : '-'}</span>
                                </div>
                            </div>

                            {timelineEvents.length > 0 && (
                                <div style={{ marginTop: 10 }}>
                                    {/*
                                      Adaptive label density:
                                      - few events: show timestamp inside bars
                                      - many events: keep bars clean, details stay below on selection
                                    */}
                                    {(() => {
                                        const showInlineBarText = timelineEvents.length <= 5
                                        return (
                                    <div
                                        style={{
                                            display: 'grid',
                                            gridTemplateColumns: `repeat(${Math.max(1, timelineEvents.length)}, minmax(30px, 1fr))`,
                                            gap: 6,
                                            alignItems: 'flex-end',
                                        }}
                                    >
                                        {timelineEvents.map((ev, idx) => {
                                            const v1 = Number(ev?.max_1h_mm) || 0
                                            const v6 = Number(ev?.max_6h_mm) || 0
                                            const score = Math.max(v1, v6 / 2)
                                            const h = Math.max(10, Math.round((score / Math.max(1, timelineMax)) * 44))
                                            const isSel = selectedEvent && selectedEvent.peak_ts === ev.peak_ts && selectedEvent.point === ev.point
                                            const cls = String(ev?.warnstufe || '').toLowerCase()
                                            const color = cls === 'extrem' ? '#ff4d4f' : (cls === 'unwetter' ? '#ff9f1a' : '#1ac8ff')
                                            const peakRaw = String(ev?.peak_ts || '')
                                            const d = peakRaw.slice(0, 10)
                                            const t = peakRaw.slice(11, 16)
                                            const dShort = d ? d.slice(2) : '--:--'
                                            return (
                                                <button
                                                    key={`tl-${ev.point}-${ev.peak_ts}-${idx}`}
                                                    type="button"
                                                    onClick={() => toggleSelectEvent(ev)}
                                                    disabled={analysisType !== 'starkregen'}
                                                    title={`${String(ev?.warnstufe || '-').toUpperCase()} | ${String(ev?.peak_ts || '').replace('T', ' ').slice(0, 16)} | Max1h ${v1} mm | Max6h ${v6} mm`}
                                                    style={{
                                                        minWidth: 30,
                                                        width: '100%',
                                                        height: 84,
                                                        borderRadius: 6,
                                                        border: isSel ? '1px solid #00e5ff' : '1px solid rgba(255,255,255,0.14)',
                                                        background: 'rgba(7,12,24,0.65)',
                                                        display: 'flex',
                                                        flexDirection: 'column',
                                                        justifyContent: 'flex-start',
                                                        alignItems: 'stretch',
                                                        padding: 4,
                                                        cursor: analysisType === 'starkregen' ? 'pointer' : 'not-allowed',
                                                    }}
                                                >
                                                    <span
                                                        className="weather-event-bar-fill"
                                                        style={{
                                                            display: 'block',
                                                            width: '100%',
                                                            height: `${h}px`,
                                                            borderRadius: 4,
                                                            background: color,
                                                            opacity: isSel ? 1 : 0.9,
                                                            marginTop: 'auto'
                                                        }}
                                                    >
                                                        {showInlineBarText && (
                                                            <span className="weather-event-bar-inline" aria-hidden="true">
                                                                <span>{dShort}</span>
                                                                <span>{t || '--:--'}</span>
                                                            </span>
                                                        )}
                                                    </span>
                                                </button>
                                            )
                                        })}
                                    </div>
                                        )
                                    })()}
                                    {selectedEvent && (
                                        <div className="weather-event-card">
                                            <div className="weather-event-card-head">
                                                <strong>{String(selectedEvent.warnstufe || '-').toUpperCase()}</strong>
                                                <span>{String(selectedEvent.peak_ts || '').replace('T', ' ').slice(0, 16)}</span>
                                            </div>
                                            <div className="weather-event-card-body">
                                                <span>Max 1h: {selectedEvent.max_1h_mm} mm</span>
                                                <span>Max 6h: {selectedEvent.max_6h_mm} mm</span>
                                            </div>
                                        </div>
                                    )}
                                </div>
                            )}

                            {timelineEvents.length === 0 && (
                                <div style={{ marginTop: 8, fontSize: 13, opacity: 0.8 }}>Keine Ereignisse im gewaehlten Zeitraum.</div>
                            )}
                        </div>
                    )}
                </div>
        </div>
    )
}

function App() {
    const initialUi = useMemo(() => loadUiState(), [])
    const uiStateRef = useRef(initialUi || {})
    const uiSaveTimerRef = useRef(null)
    const scheduleUiSave = useCallback((patch) => {
        try {
            uiStateRef.current = {
                ...(uiStateRef.current || {}),
                ...(patch || {}),
                _ts: Date.now(),
            }
            if (uiSaveTimerRef.current) clearTimeout(uiSaveTimerRef.current)
            uiSaveTimerRef.current = setTimeout(() => saveUiState(uiStateRef.current), 250)
        } catch {}
    }, [])

    const [hash, setHash] = useState(() => window.location.hash || '#/')
    const [geoJsonData, setGeoJsonData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [status, setStatus] = useState(null)
    const [error, setError] = useState(null)
    const [progressInfo, setProgressInfo] = useState({ step: 0, total: 0, message: '' })
    const [threshold, setThreshold] = useState(() => Number.isFinite(Number(initialUi?.threshold)) ? Number(initialUi.threshold) : 200)
    const [basemap, setBasemap] = useState(() => (initialUi?.basemap && BASEMAPS[initialUi.basemap]) ? initialUi.basemap : 'light')
    const [sidebarOpen, setSidebarOpen] = useState(() => (typeof initialUi?.sidebarOpen === 'boolean') ? initialUi.sidebarOpen : true)
    const [inputMode, setInputMode] = useState('draw')
    const devUi = useMemo(() => isDevUiEnabled(), [])
    const [apiMode, setApiMode] = useState('legacy') // auto-selected (jobs if reachable), unless dev override
    const [jobSel, setJobSel] = useState(() => loadJobSelection())
    const [jobMeta, setJobMeta] = useState({ tenants: [], projects: [], models: [], loading: false, error: null })
    const [drawActive, setDrawActive] = useState(false)
    const [drawMode, setDrawMode] = useState(() => (initialUi?.drawMode === 'rectangle' ? 'rectangle' : 'polygon')) // polygon | rectangle
    const [corridorDensity, setCorridorDensity] = useState(() => {
        const v = Number(initialUi?.corridorDensity)
        if (!Number.isFinite(v)) return 1 // default: Mittel
        return Math.max(0, Math.min(CORRIDOR_DENSITY_PRESETS.length - 1, Math.round(v)))
    })
    const [drawnArea, setDrawnArea] = useState(() => {
        const a = initialUi?.aoi
        if (!a || typeof a !== 'object') return null
        const south = Number(a.south)
        const west = Number(a.west)
        const north = Number(a.north)
        const east = Number(a.east)
        if (![south, west, north, east].every(Number.isFinite)) return null
        const shapeType = (a.shapeType === 'rectangle' || a.shapeType === 'polygon') ? a.shapeType : 'rectangle'
        const out = {
            south, west, north, east,
            bounds: [[south, west], [north, east]],
            shapeType,
        }
        if (shapeType === 'polygon' && Array.isArray(a.polygon) && a.polygon.length >= 3) {
            out.polygon = a.polygon
        }
        return out
    })
    const aoiFileRef = useRef(null)
    const [selectedHotspot, setSelectedHotspot] = useState(null)
    const [highlightFeatureIds, setHighlightFeatureIds] = useState([])
    const [aoiProvider, setAoiProvider] = useState('nrw')
    const [analysisType, setAnalysisType] = useState(() => (initialUi?.analysisType === 'erosion' ? 'erosion' : 'starkregen')) // starkregen | erosion
    const [demSource, setDemSource] = useState('wcs') // 'wcs' | 'cog' | 'public' (dev only)
    const [stPublicParts, setStPublicParts] = useState([1])
    const [stPublicConfirm, setStPublicConfirm] = useState(false)
    const [stPublicCacheDir, setStPublicCacheDir] = useState('')
    const [officialScenario, setOfficialScenario] = useState('mw')
    const [placeSelection, setPlaceSelection] = useState(null)
    const [pointCheck, setPointCheck] = useState(null)
    const [poi, setPoi] = useState(null) // last valid point-of-interest (for catchment), {lat, lon}
    const [catchmentGeojson, setCatchmentGeojson] = useState(null)
    const [catchmentMeta, setCatchmentMeta] = useState(null)
    const [catchmentLoading, setCatchmentLoading] = useState(false)
    const [catchmentError, setCatchmentError] = useState(null)
    const [catchmentFor, setCatchmentFor] = useState(null) // {lat,lon,aoiHash}
    const [selectedRainEvent, setSelectedRainEvent] = useState(null)
    const [weatherUiState, setWeatherUiState] = useState({ ready: false, loading: false, error: null })
    const [wcsHealth, setWcsHealth] = useState({ status: 'unknown', last: null, loading: false, error: null, show: false })
    const [fitKey, setFitKey] = useState(0)
    const [layers, setLayers] = useState(() => {
        const vis = (initialUi?.layerVis && typeof initialUi.layerVis === 'object') ? initialUi.layerVis : {}
        const v = (id, def) => (typeof vis?.[id] === 'boolean' ? !!vis[id] : def)
        return [
            { id: 'tiles', name: 'Basiskarte', visible: v('tiles', true), color: '#888' },
            { id: 'flow', name: 'Risikoklassen', visible: v('flow', true), color: '#00e5ff' },
            { id: 'corridors', name: 'Abflusskorridore', visible: v('corridors', false), color: '#00bcd4' },
            { id: 'catchment', name: 'Einzugsgebiet', visible: v('catchment', false), color: '#e5e7eb' },
            { id: 'official_extent', name: 'Amtlich: Ueberflutungsgrenze', visible: v('official_extent', false), color: '#76ff03' },
            { id: 'official_depth', name: 'Amtlich: Ueberflutungstiefe', visible: v('official_depth', false), color: '#00bcd4' },
        ]
    })
    const bboxAreaKm2 = useMemo(() => estimateBboxAreaKm2(drawnArea), [drawnArea])
    const bboxAreaLabel = useMemo(() => {
        const nf = new Intl.NumberFormat('de-DE', { maximumSignificantDigits: 3 })
        return formatAreaCompact(km2ToM2(bboxAreaKm2), nf)
    }, [bboxAreaKm2])
    const initialMapView = useMemo(() => {
        const mv = initialUi?.mapView
        const lat = Number(mv?.lat)
        const lon = Number(mv?.lon)
        const zoom = Number(mv?.zoom)
        if ([lat, lon, zoom].every(Number.isFinite)) return { lat, lon, zoom }
        return DEFAULT_MAP_VIEW
    }, [initialUi])
    const [mapViewLive, setMapViewLive] = useState(() => initialMapView)
    const analysisProgressPct = useMemo(() => {
        const total = Number(progressInfo?.total || 0)
        const step = Number(progressInfo?.step || 0)
        if (!loading || total <= 0) return 0
        return Math.max(0, Math.min(100, Math.round((step / total) * 100)))
    }, [loading, progressInfo])

    useEffect(() => () => {
        if (uiSaveTimerRef.current) clearTimeout(uiSaveTimerRef.current)
    }, [])

    useEffect(() => scheduleUiSave({ threshold }), [threshold, scheduleUiSave])
    useEffect(() => scheduleUiSave({ basemap }), [basemap, scheduleUiSave])
    useEffect(() => scheduleUiSave({ sidebarOpen }), [sidebarOpen, scheduleUiSave])
    useEffect(() => scheduleUiSave({ analysisType }), [analysisType, scheduleUiSave])
    useEffect(() => scheduleUiSave({ drawMode }), [drawMode, scheduleUiSave])
    useEffect(() => scheduleUiSave({ corridorDensity }), [corridorDensity, scheduleUiSave])
    useEffect(() => scheduleUiSave({ aoi: drawnArea }), [drawnArea, scheduleUiSave])
    useEffect(() => {
        const vis = {}
        for (const l of (layers || [])) vis[l.id] = !!l.visible
        scheduleUiSave({ layerVis: vis })
    }, [layers, scheduleUiSave])

    const beginRedraw = useCallback(() => {
        setInputMode('draw')
        if (drawnArea?.shapeType === 'rectangle') setDrawMode('rectangle')
        else setDrawMode('polygon')
        setDrawActive(true)
    }, [drawnArea])

    const importAoiFile = useCallback(async (file) => {
        if (!file) return
        try {
            const text = await file.text()
            const json = JSON.parse(text)
            const pts = polygonPointsFromGeoJson(json)
            if (!pts) throw new Error('Keine Polygon-Geometrie gefunden (erwartet: GeoJSON Polygon/MultiPolygon).')
            const bbox = bboxFromPoints(pts)
            if (!bbox) throw new Error('Ungueltige Geometrie.')
            setInputMode('draw')
            setDrawActive(false)
            setDrawMode('polygon')
            setDrawnArea({
                ...bbox,
                bounds: [[bbox.south, bbox.west], [bbox.north, bbox.east]],
                shapeType: 'polygon',
                polygon: pts,
            })
            setError(null)
            setStatus(null)
        } catch (e) {
            setError(`AOI-Import: ${e?.message || String(e)}`)
        } finally {
            if (aoiFileRef.current) aoiFileRef.current.value = ''
        }
    }, [])

    const toggleLayer = useCallback((id) => {
        setLayers((prev) => prev.map((l) => (l.id === id ? { ...l, visible: !l.visible } : l)))
    }, [])

    useEffect(() => {
        if (!devUi) return
        saveApiModeOverride(apiMode)
    }, [apiMode, devUi])

    useEffect(() => {
        // "Apple-art": Nutzer soll keine Datenquelle konfigurieren muessen.
        // Default:
        // - Sachsen-Anhalt: lokale COG-Kacheln (wenn vorhanden)
        // - sonst: WCS
        if (devUi) return
        if (aoiProvider === 'sachsen-anhalt') setDemSource('cog')
        else setDemSource('wcs')
        setStPublicConfirm(false)
    }, [aoiProvider, devUi])

    useEffect(() => {
        saveJobSelection(jobSel)
    }, [jobSel])

    useEffect(() => {
        // Auto-select best backend:
        // - Prefer Jobs (persistent runs) when reachable
        // - Fall back to Legacy (live streaming)
        // Dev override exists but is hidden by default.
        const ac = new AbortController()
        const t = setTimeout(() => ac.abort(), 800)

        ;(async () => {
            try {
                if (devUi) {
                    setApiMode(loadApiModeOverride())
                    return
                }
                const res = await fetch(`${JOB_API_URL}/health`, { signal: ac.signal })
                if (!res.ok) throw new Error('health not ok')
                const json = await res.json().catch(() => null)
                if (json?.status === 'ok') setApiMode('jobs')
                else setApiMode('legacy')
            } catch {
                setApiMode('legacy')
            } finally {
                clearTimeout(t)
            }
        })()

        return () => {
            clearTimeout(t)
            ac.abort()
        }
    }, [devUi])

    useEffect(() => {
        if (apiMode !== 'jobs') return
        const ac = new AbortController()
        setJobMeta((s) => ({ ...s, loading: true, error: null }))

        ;(async () => {
            try {
                // Load metadata.
                const tenantsRes = await fetch(`${JOB_API_URL}/v1/tenants`, { signal: ac.signal })
                if (!tenantsRes.ok) throw new Error('Job-API tenants konnten nicht geladen werden')
                const tenants = await tenantsRes.json()
                const tenantId = (tenants?.[0]?.id) || jobSel.tenantId || DEMO_TENANT_ID

                const [projectsRes, modelsRes] = await Promise.all([
                    fetch(`${JOB_API_URL}/v1/projects?tenant_id=${encodeURIComponent(tenantId)}`, { signal: ac.signal }),
                    fetch(`${JOB_API_URL}/v1/models`, { signal: ac.signal }),
                ])
                if (!projectsRes.ok || !modelsRes.ok) throw new Error('Job-API Metadaten konnten nicht geladen werden')
                let [projects, models] = await Promise.all([projectsRes.json(), modelsRes.json()])

                // Ensure default model exists.
                if (!Array.isArray(models) || models.length === 0) {
                    await fetch(`${JOB_API_URL}/v1/models`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ key: 'd8-fast', name: 'D8 Fast', category: 'hydrology' }),
                        signal: ac.signal,
                    }).catch(() => {})
                    const m2 = await fetch(`${JOB_API_URL}/v1/models`, { signal: ac.signal })
                    if (m2.ok) models = await m2.json()
                }

                // Ensure default project exists.
                if (!Array.isArray(projects) || projects.length === 0) {
                    await fetch(`${JOB_API_URL}/v1/projects`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ tenant_id: tenantId, name: 'Mein Projekt' }),
                        signal: ac.signal,
                    }).catch(() => {})
                    const p2 = await fetch(`${JOB_API_URL}/v1/projects?tenant_id=${encodeURIComponent(tenantId)}`, { signal: ac.signal })
                    if (p2.ok) projects = await p2.json()
                }

                const defaultModel = (models || []).find((m) => m?.key === 'd8-fast') || (models || [])[0]
                const defaultProject = (projects || [])[0]
                const modelId = defaultModel?.id || DEMO_MODEL_ID
                const projectId = defaultProject?.id || DEMO_PROJECT_ID

                setJobSel((s) => ({
                    tenantId,
                    projectId: s?.projectId || projectId,
                    modelId: s?.modelId || modelId,
                }))

                setJobMeta({
                    tenants: tenants || [],
                    projects: projects || [],
                    models: models || [],
                    loading: false,
                    error: null,
                })
            } catch (e) {
                if (String(e?.name || '') === 'AbortError') return
                setJobMeta((s) => ({ ...s, loading: false, error: e?.message || String(e) }))
                // If jobs are not usable, silently fall back to legacy for end users.
                if (!devUi) setApiMode('legacy')
            }
        })()

        return () => ac.abort()
    }, [apiMode, devUi])

    useEffect(() => {
        if (!drawnArea) return
        const ac = new AbortController()
        ;(async () => {
            try {
                const res = await fetch(`${LEGACY_API_URL}/detect-provider`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        south: drawnArea.south,
                        west: drawnArea.west,
                        north: drawnArea.north,
                        east: drawnArea.east,
                    }),
                    signal: ac.signal,
                })
                if (!res.ok) return
                const json = await res.json()
                const key = json?.provider?.key
                if (key) setAoiProvider(key)
            } catch {}
        })()
        return () => ac.abort()
    }, [drawnArea])

        useEffect(() => {
            const cfg = OFFICIAL_WMS[aoiProvider] || OFFICIAL_WMS.nrw
            setLayers((prev) => {
                const prevById = new Map(prev.map((l) => [l.id, l]))
                const next = [
                    { ...(prevById.get('tiles') || { id: 'tiles', name: 'Basiskarte', visible: true, color: '#888' }) },
                    { ...(prevById.get('flow') || { id: 'flow', name: 'Risikoklassen', visible: true, color: '#00e5ff' }) },
                    { ...(prevById.get('corridors') || { id: 'corridors', name: 'Abflusskorridore', visible: false, color: '#00bcd4' }) },
                    { ...(prevById.get('catchment') || { id: 'catchment', name: 'Einzugsgebiet', visible: false, color: '#e5e7eb' }) },
                ]
                if (cfg.supports.extent) {
                    const p = prevById.get('official_extent')
                    next.push({ id: 'official_extent', name: 'Amtlich: Ueberflutungsgrenze', visible: p?.visible ?? false, color: '#76ff03' })
                }
            if (cfg.supports.depth) {
                const p = prevById.get('official_depth')
                const name = aoiProvider === 'sachsen-anhalt' ? 'Amtlich: Wassertiefen' : 'Amtlich: Ueberflutungstiefe'
                next.push({ id: 'official_depth', name, visible: p?.visible ?? false, color: '#00bcd4' })
            }
            return next
        })
        setOfficialScenario((s) => (cfg.scenarios?.[s] ? s : 'mw'))
        if (aoiProvider !== 'sachsen-anhalt') {
            setDemSource('wcs')
            setStPublicConfirm(false)
        }
    }, [aoiProvider])

    useEffect(() => {
        // Keep layer labels aligned with the selected analysis mode.
        setLayers((prev) => prev.map((l) => {
            if (l.id !== 'flow') return l
            const name = analysisType === 'erosion' ? 'Erosionsklassen' : 'Risikoklassen'
            return { ...l, name }
        }))
        // Switching the analysis mode should not leave stale results on screen.
        setGeoJsonData(null)
        setSelectedHotspot(null)
        setHighlightFeatureIds([])
        setStatus(null)
        setError(null)
        if (analysisType !== 'starkregen') setSelectedRainEvent(null)
    }, [analysisType])

    const runStreamingRequest = useCallback(async (url, options = {}) => {
        // If drawing mode is active, panning is disabled. Running an analysis should always return to normal map interaction.
        setDrawActive(false)
        setLoading(true)
        setError(null)
        setGeoJsonData(null)
        setSelectedHotspot(null)
        setHighlightFeatureIds([])
        setProgressInfo({ step: 0, total: 0, message: 'Verbindung wird aufgebaut...' })

        try {
            const res = await fetch(url, options)
            if (!res.ok) {
                const text = await res.text()
                let detail = text
                try { detail = JSON.parse(text).detail } catch {}
                throw new Error(detail)
            }

            await readNdjsonStream(
                res,
                (evt) => setProgressInfo({ step: evt.step, total: evt.total, message: evt.message }),
                (data) => {
                    const enriched = {
                        ...data,
                        features: (data?.features || []).map((f, idx) => ({
                            ...f,
                            properties: { ...(f.properties || {}), _fid: idx },
                        })),
                    }
                    const n = enriched?.features?.length ?? 0
                    const mean = data?.analysis?.metrics?.risk_score_mean
                    const top = enriched?.analysis?.hotspots?.[0] ?? null
                    const wClass = String(data?.analysis?.assumptions?.weather_moisture_class || '').toLowerCase()
                    setGeoJsonData(enriched)
                    setSelectedHotspot(top)
                    setFitKey((k) => k + 1)
                    const kind = String(data?.analysis?.kind || '').toLowerCase()
                    const label = kind === 'erosion' ? 'mittlerer Erosionsindex' : 'mittleres Risiko'
                    const wx = (kind === 'starkregen' && wClass && wClass !== 'n/a') ? `, Vorfeuchte ${wClass}` : ''
                    setStatus(`Analyse fertig: ${n} Segmente, ${label} ${mean ?? '-'} / 100${wx}`)
                },
                (detail) => { throw new Error(detail) },
            )
        } catch (err) {
            console.error('Request error:', err)
            setError(`Fehler: ${err.message}`)
            setStatus(null)
        } finally {
            setLoading(false)
        }
    }, [setDrawActive])

    const handleFile = useCallback(async (file) => {
        if (!file) return
        const body = new FormData()
        body.append('file', file)
        await runStreamingRequest(`${LEGACY_API_URL}/analyze?threshold=${threshold}`, { method: 'POST', body })
    }, [threshold, runStreamingRequest, drawnArea, analysisType, aoiProvider, demSource, stPublicParts, stPublicConfirm, stPublicCacheDir, apiMode, jobSel, setDrawActive])

    const runLegacyBboxAnalyze = useCallback(async (at, isStPublic) => {
            const total = isStPublic ? 12 : 8
            setProgressInfo({ step: 0, total, message: 'DGM wird geladen...' })
            const parts = stPublicParts?.length ? stPublicParts.join(',') : '1'
            const confirm = isStPublic && stPublicConfirm ? '&public_confirm=true' : ''
            const cacheDir = isStPublic && stPublicCacheDir.trim()
                ? `&dem_cache_dir=${encodeURIComponent(stPublicCacheDir.trim())}`
                : ''
            const event1h = (at === 'starkregen' && Number.isFinite(Number(selectedRainEvent?.max_1h_mm)))
                ? `&weather_event_mm_h=${encodeURIComponent(String(Number(selectedRainEvent.max_1h_mm)))}`
                : ''
            await runStreamingRequest(
                `${LEGACY_API_URL}/analyze-bbox?threshold=${threshold}&provider=${encodeURIComponent(aoiProvider)}&dem_source=${encodeURIComponent(demSource)}&analysis_type=${encodeURIComponent(at)}${isStPublic ? `&st_parts=${encodeURIComponent(parts)}` : ''}${confirm}${cacheDir}${event1h}`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        south: drawnArea.south,
                        west: drawnArea.west,
                        north: drawnArea.north,
                        east: drawnArea.east,
                        polygon: (drawnArea.shapeType === 'polygon' && Array.isArray(drawnArea.polygon) && drawnArea.polygon.length >= 3)
                            ? drawnArea.polygon
                            : null,
                    }),
                },
            )
    }, [stPublicParts, stPublicConfirm, stPublicCacheDir, selectedRainEvent, runStreamingRequest, threshold, aoiProvider, demSource, drawnArea])

    const handleBboxAnalyze = useCallback(async () => {
        if (!drawnArea) return
        // Ensure normal map interaction during/after analysis (drawing disables dragging and can affect touch-zoom).
        setDrawActive(false)
        const at = (analysisType === 'erosion') ? 'erosion' : 'starkregen'
        const isStPublic = aoiProvider === 'sachsen-anhalt' && demSource === 'public'

        if (apiMode !== 'jobs') {
            await runLegacyBboxAnalyze(at, isStPublic)
            return
        }

        // Job mode: create job -> poll -> fetch result content.
        const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
        const parts = stPublicParts?.length ? stPublicParts : [1]

        setLoading(true)
        setError(null)
        setStatus(null)
        setGeoJsonData(null)
        setSelectedHotspot(null)
        setProgressInfo({ step: 0, total: 4, message: 'Job wird erstellt...' })

        try {
            const jobPayload = {
                project_id: jobSel.projectId || DEMO_PROJECT_ID,
                model_id: jobSel.modelId || DEMO_MODEL_ID,
                parameters: {
                    threshold,
                    provider: aoiProvider || 'auto',
                    dem_source: demSource || 'wcs',
                    analysis_type: at,
                    st_parts: isStPublic ? parts : undefined,
                    dem_cache_dir: isStPublic && stPublicCacheDir.trim() ? stPublicCacheDir.trim() : undefined,
                    bbox: {
                        south: drawnArea.south,
                        west: drawnArea.west,
                        north: drawnArea.north,
                        east: drawnArea.east,
                    },
                    polygon: (drawnArea.shapeType === 'polygon' && Array.isArray(drawnArea.polygon) && drawnArea.polygon.length >= 3)
                        ? drawnArea.polygon
                        : null,
                    // Apply explicitly selected rain event in job mode as well (parity with legacy mode).
                    weather_event_mm_h: (at === 'starkregen' && Number.isFinite(Number(selectedRainEvent?.max_1h_mm)))
                        ? Number(selectedRainEvent.max_1h_mm)
                        : undefined,
                    weather_event_peak_ts: (at === 'starkregen' && selectedRainEvent?.peak_ts)
                        ? String(selectedRainEvent.peak_ts)
                        : undefined,
                },
            }

            const res = await fetch(`${JOB_API_URL}/v1/jobs`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(jobPayload),
            })
            if (!res.ok) {
                const txt = await res.text()
                throw new Error(txt || `HTTP ${res.status}`)
            }
            const job = await res.json()
            const jobId = job?.id
            if (!jobId) throw new Error('Job-API: keine Job-ID erhalten')

            setProgressInfo({ step: 1, total: 4, message: `Job queued (${String(jobId).slice(0, 8)}...)` })

            const deadlineMs = Date.now() + 10 * 60 * 1000
            let status = null
            while (Date.now() < deadlineMs) {
                const jr = await fetch(`${JOB_API_URL}/v1/jobs/${jobId}`)
                if (!jr.ok) {
                    const txt = await jr.text()
                    throw new Error(txt || `Job status HTTP ${jr.status}`)
                }
                const j = await jr.json()
                status = j?.status
                if (status === 'succeeded') break
                if (status === 'failed') throw new Error('Job failed')
                setProgressInfo({ step: 2, total: 4, message: `Job ${status || '...'}...` })
                await sleep(1000)
            }
            if (status !== 'succeeded') throw new Error('Job timeout')

            setProgressInfo({ step: 3, total: 4, message: 'Ergebnis wird geladen...' })
            const out = await fetch(`${JOB_API_URL}/v1/jobs/${jobId}/outputs/latest/content?output_type=flow_network_geojson`)
            if (!out.ok) {
                const txt = await out.text()
                throw new Error(txt || `Output HTTP ${out.status}`)
            }
            const payload = await out.json()
            const result = payload?.geojson ? payload : (payload?.data || payload)
            const geojson = result?.geojson || result
            if (!geojson?.features) throw new Error('Ungueltiges Ergebnisformat (kein GeoJSON)')

            // Mirror legacy onResult behavior.
            setGeoJsonData(geojson)
            const hotspots = geojson?.analysis?.hotspots || []
            const top = hotspots?.[0]
            if (top) setSelectedHotspot(top)
            const n = geojson?.features?.length ?? 0
            const mean = geojson?.analysis?.metrics?.risk_score_mean
            const kind = String(geojson?.analysis?.kind || '').toLowerCase()
            const label = kind === 'erosion' ? 'mittlerer Erosionsindex' : 'mittleres Risiko'
            setStatus(`Analyse fertig: ${n} Segmente, ${label} ${mean ?? '-'} / 100`)
            setProgressInfo({ step: 4, total: 4, message: 'Fertig.' })
        } catch (err) {
            // If job mode fails (network/port/CORS), auto-fallback to legacy for robustness.
            if (!devUi) {
                try {
                    setApiMode('legacy')
                    setProgressInfo({ step: 0, total: 1, message: 'Job-API nicht erreichbar, Fallback auf Direktanalyse...' })
                    await runLegacyBboxAnalyze(at, isStPublic)
                    return
                } catch (legacyErr) {
                    setError(`Fehler: ${legacyErr?.message || String(legacyErr)}`)
                    setStatus(null)
                }
            } else {
                setError(`Fehler: ${err?.message || String(err)}`)
                setStatus(null)
            }
        } finally {
        setLoading(false)
        }
    }, [
        drawnArea,
        analysisType,
        selectedRainEvent,
        apiMode,
        jobSel,
        threshold,
        runStreamingRequest,
        aoiProvider,
        demSource,
        stPublicParts,
        stPublicConfirm,
        stPublicCacheDir,
        runLegacyBboxAnalyze,
        devUi,
    ])

    const runWcsSelftest = useCallback(async () => {
        if (!drawnArea) return
        setWcsHealth((s) => ({ ...s, loading: true, error: null, show: true }))
        try {
            const res = await fetch(`${LEGACY_API_URL}/wcs/selftest?provider=auto&south=${drawnArea.south}&west=${drawnArea.west}&north=${drawnArea.north}&east=${drawnArea.east}`)
            if (!res.ok) {
                const txt = await res.text()
                let detail = txt
                try { detail = JSON.parse(txt).detail } catch {}
                throw new Error(detail)
            }
            const json = await res.json()
            const steps = json?.steps || []
            const capOk = steps.find((s) => s.name === 'GetCapabilities')?.ok
            const descOk = steps.find((s) => s.name === 'DescribeCoverage')?.ok
            const covOk = steps.find((s) => String(s.name || '').startsWith('GetCoverage'))?.ok
            let status = 'red'
            if (capOk && descOk && covOk) status = 'green'
            else if (capOk && descOk) status = 'yellow'
            setWcsHealth({ status, last: json, loading: false, error: null, show: true })
        } catch (e) {
            setWcsHealth({ status: 'red', last: null, loading: false, error: e?.message || String(e), show: true })
        }
    }, [drawnArea])
    useEffect(() => {
        if (!geoJsonData?.features?.length || !selectedHotspot) {
            setHighlightFeatureIds([])
            return
        }
        setHighlightFeatureIds(nearestFeatureIds(geoJsonData.features, selectedHotspot, 14))
    }, [geoJsonData, selectedHotspot])

    const bm = BASEMAPS[basemap]
    const showTiles = layers.find((l) => l.id === 'tiles')?.visible ?? true
    const showFlow = layers.find((l) => l.id === 'flow')?.visible ?? true
    const showCorridors = layers.find((l) => l.id === 'corridors')?.visible ?? false
    const showCatchment = layers.find((l) => l.id === 'catchment')?.visible ?? false
    // Note: we intentionally do not gate clicks by catchment polygon.
    // Users must be able to set a new catchment point by clicking any segment.
    const showOfficialExtent = layers.find((l) => l.id === 'official_extent')?.visible ?? false
    const showOfficialDepth = layers.find((l) => l.id === 'official_depth')?.visible ?? false
    const helpRouteActive = hash === '#/hilfe'
    const sourcesRouteActive = hash === '#/daten'

    useEffect(() => {
        const onHash = () => setHash(window.location.hash || '#/')
        window.addEventListener('hashchange', onHash)
        return () => window.removeEventListener('hashchange', onHash)
    }, [])

    if (helpRouteActive) {
        return <HelpPage />
    }
    if (sourcesRouteActive) {
        return <DataSourcesPage />
    }
    const hotspots = geoJsonData?.analysis?.hotspots || []
    const hasCorridors = useMemo(() => {
        const feats = geoJsonData?.features || []
        if (!Array.isArray(feats) || feats.length === 0) return false
        // Only show the toggle if the backend actually provided contributing area info.
        return feats.some((f) => Number.isFinite(Number(f?.properties?.upstream_area_m2)) || Number.isFinite(Number(f?.properties?.upstream_area_km2)))
    }, [geoJsonData])
    const maxCorridorKm2 = useMemo(() => {
        const feats = geoJsonData?.features || []
        if (!Array.isArray(feats) || feats.length === 0) return 0
        let mx = 0
        for (const f of feats) {
            const v = upstreamKm2Of(f)
            if (Number.isFinite(v) && v > mx) mx = v
        }
        return mx
    }, [geoJsonData])
    const minCorridorKm2 = useMemo(() => {
        const feats = geoJsonData?.features || []
        if (!Array.isArray(feats) || feats.length === 0) return 0
        let mn = Number.POSITIVE_INFINITY
        for (const f of feats) {
            const v = upstreamKm2Of(f)
            if (Number.isFinite(v) && v >= 0 && v < mn) mn = v
        }
        return Number.isFinite(mn) ? mn : 0
    }, [geoJsonData])
    const displayCorridorsGeojson = useMemo(() => {
        if (!geoJsonData?.features?.length) return geoJsonData
        const preset = CORRIDOR_DENSITY_PRESETS[Math.max(0, Math.min(CORRIDOR_DENSITY_PRESETS.length - 1, Number(corridorDensity) || 0))]
        const sliderFrac = Number(preset?.min_frac_of_max || 0)
        // Starkregen: the slider is the primary control (users expect immediate effect).
        // Erosion: keep a small automatic coarsening when zoomed out to avoid visual overload.
        const zoomFrac = (analysisType === 'erosion') ? zoomMinFracForNet(mapViewLive?.zoom) : 0.0
        const frac = Math.max(Number.isFinite(sliderFrac) ? sliderFrac : 0, Number.isFinite(zoomFrac) ? zoomFrac : 0)
        if (!Number.isFinite(frac) || frac <= 0) return geoJsonData
        const mx = Number(maxCorridorKm2)
        if (!Number.isFinite(mx) || mx <= 0) return geoJsonData
        const minKm2 = mx * frac
        const feats = (geoJsonData.features || []).filter((f) => {
            const km2 = upstreamKm2Of(f)
            return Number.isFinite(km2) && km2 >= minKm2
        })
        return { ...geoJsonData, features: feats }
    }, [geoJsonData, corridorDensity, maxCorridorKm2, mapViewLive, analysisType])

    const corridorTotalCount = geoJsonData?.features?.length ?? 0
    const corridorVisibleCount = displayCorridorsGeojson?.features?.length ?? corridorTotalCount

    const displayFlowGeojson = useMemo(() => {
        if (!geoJsonData) return null
        if (analysisType === 'erosion') {
            // Erosion: avoid dense "zebra" networks; show only critical segments.
            return pickCriticalErosionSegments(geoJsonData, 250)
        }
        // Starkregen: users expect "Netzdichte" to reduce the visible (risk-colored) line density,
        // not only an optional overlay.
        if (analysisType === 'starkregen' && showCorridors) {
            return displayCorridorsGeojson || geoJsonData
        }
        return geoJsonData
    }, [geoJsonData, analysisType, showCorridors, displayCorridorsGeojson])

    const pointCheckGeojson = useMemo(() => {
        // Keep "Objekt-Check" aligned with what's visible.
        if (!geoJsonData) return null
        if (showFlow) return displayFlowGeojson || geoJsonData
        if (showCorridors) return displayCorridorsGeojson || geoJsonData
        return geoJsonData
    }, [geoJsonData, showFlow, showCorridors, displayFlowGeojson, displayCorridorsGeojson])

    useEffect(() => {
        setPointCheck(null)
    }, [analysisType, geoJsonData])

    useEffect(() => {
        // Avoid UI fighting while the user is drawing a new AOI.
        if (!drawActive) return
        setPointCheck(null)
        setPoi(null)
        setSelectedHotspot(null)
        setHighlightFeatureIds([])
    }, [drawActive])

    useEffect(() => {
        // Catchment is tied to the "current point". Clear when inputs change.
        setCatchmentGeojson(null)
        setCatchmentMeta(null)
        setCatchmentError(null)
        setCatchmentFor(null)
        setPoi(null)
        setSelectedRainEvent(null)
        setWeatherUiState({ ready: false, loading: false, error: null })
    }, [geoJsonData, drawnArea])

    useEffect(() => {
        if (!showCatchment) return
        if (!poi) return
        if (!drawnArea) return
        const lat = Number(poi.lat)
        const lon = Number(poi.lon)
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return

        const aoiHash = `${drawnArea.shapeType}:${drawnArea.south},${drawnArea.west},${drawnArea.north},${drawnArea.east}:${(drawnArea.polygon || []).length}`
        if (catchmentFor && catchmentFor.lat === lat && catchmentFor.lon === lon && catchmentFor.aoiHash === aoiHash && catchmentGeojson) return

        const ac = new AbortController()
        setCatchmentLoading(true)
        setCatchmentError(null)

        ;(async () => {
            try {
                const at = (analysisType === 'erosion') ? 'erosion' : 'starkregen'
                const isStPublic = aoiProvider === 'sachsen-anhalt' && demSource === 'public'
                const parts = stPublicParts?.length ? stPublicParts.join(',') : '1'
                const confirm = isStPublic && stPublicConfirm ? '&public_confirm=true' : ''
                const cacheDir = isStPublic && stPublicCacheDir.trim()
                    ? `&dem_cache_dir=${encodeURIComponent(stPublicCacheDir.trim())}`
                    : ''

                const url =
                    `${LEGACY_API_URL}/catchment-bbox?provider=${encodeURIComponent(aoiProvider)}&dem_source=${encodeURIComponent(demSource)}&analysis_type=${encodeURIComponent(at)}` +
                    `${isStPublic ? `&st_parts=${encodeURIComponent(parts)}` : ''}${confirm}${cacheDir}`

                const res = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    signal: ac.signal,
                    body: JSON.stringify({
                        south: drawnArea.south,
                        west: drawnArea.west,
                        north: drawnArea.north,
                        east: drawnArea.east,
                        polygon: (drawnArea.shapeType === 'polygon' && Array.isArray(drawnArea.polygon) && drawnArea.polygon.length >= 3)
                            ? drawnArea.polygon
                            : null,
                        point: { lat, lon },
                    }),
                })
                if (!res.ok) {
                    const txt = await res.text()
                    let detail = txt
                    try { detail = JSON.parse(txt).detail } catch {}
                    throw new Error(detail || `HTTP ${res.status}`)
                }
                const json = await res.json()
                setCatchmentGeojson(json?.geojson || null)
                setCatchmentMeta(json?.meta || null)
                setCatchmentFor({ lat, lon, aoiHash })
            } catch (e) {
                if (String(e?.name || '') === 'AbortError') return
                setCatchmentError(e?.message || String(e))
            } finally {
                setCatchmentLoading(false)
            }
        })()

        return () => ac.abort()
    }, [showCatchment, poi, drawnArea, aoiProvider, demSource, stPublicParts, stPublicConfirm, stPublicCacheDir, analysisType, catchmentFor, catchmentGeojson])

    const areaFixed = !!drawnArea && !drawActive
    const weatherStepEnabled = areaFixed
    const analysisStepEnabled = areaFixed && !!weatherUiState?.ready && !weatherUiState?.loading

    return (
        <div className="app-container">
            <button className="sidebar-toggle" onClick={() => setSidebarOpen((o) => !o)} aria-label="Toggle Sidebar">
                {sidebarOpen ? 'x' : '='}
            </button>

            <div className={`sidebar${sidebarOpen ? '' : ' collapsed'}`}>
                <div className="sidebar-header">
                    <div className="sidebar-head-row">
                        <h1>RisikoKarte</h1>
                        <button
                            className="icon-link-btn"
                            onClick={() => { window.location.hash = '#/hilfe' }}
                            title="Hilfe & Quellen"
                            aria-label="Hilfe & Quellen"
                            type="button"
                        >
                            i
                        </button>
                    </div>
                </div>

                <div className="section">
                    <div className="section-title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                        <span>Gebiet</span>
                        {drawnArea && !drawActive && (
                            <span style={{ fontSize: 12, opacity: 0.85, textTransform: 'none', letterSpacing: 'normal' }}>{bboxAreaLabel}</span>
                        )}
                    </div>
                    <div className="bbox-actions" style={{ gap: 10, marginTop: 8 }}>
                        <button
                            className={`draw-start-btn${drawnArea ? ' has-area' : ''}`}
                            onClick={() => {
                                setInputMode('draw')
                                if (drawnArea) beginRedraw()
                                else setDrawActive(true)
                            }}
                            disabled={loading}
                            type="button"
                            title={drawnArea ? 'Gebiet auf Karte neu zeichnen' : 'Gebiet auf Karte zeichnen'}
                        >
                            <span className="draw-start-icon" aria-hidden="true">
                                <svg viewBox="0 0 24 24" width="14" height="14" focusable="false">
                                    <path d="M4 17.5V20h2.5L17.8 8.7l-2.5-2.5L4 17.5z" fill="currentColor" />
                                    <path d="M14.6 5.4l2.5 2.5 1.1-1.1a1 1 0 0 0 0-1.4l-1.1-1.1a1 1 0 0 0-1.4 0l-1.1 1.1z" fill="currentColor" />
                                </svg>
                            </span>
                            <span>{drawnArea ? 'Zeichnen' : 'Gebiet waehlen'}</span>
                        </button>
                        <button
                            className="bbox-redraw-btn"
                            onClick={() => {
                                // Always available. If user is drawing, cancel drawing first to reduce confusion.
                                if (drawActive) setDrawActive(false)
                                setTimeout(() => aoiFileRef.current?.click(), 0)
                            }}
                            disabled={loading}
                            type="button"
                        >
                            Import
                        </button>
                        <input
                            ref={aoiFileRef}
                            type="file"
                            accept=".geojson,.json,application/geo+json,application/json"
                            style={{ display: 'none' }}
                            onChange={(e) => importAoiFile(e.target.files?.[0])}
                        />
                    </div>

                    {drawActive && (
                        <>
                            <div className="mode-toggle" style={{ marginTop: 10 }}>
                                <button
                                    className={`mode-btn${drawMode === 'polygon' ? ' active' : ''}`}
                                    onClick={() => setDrawMode('polygon')}
                                    disabled={loading}
                                    type="button"
                                >
                                    Polygon
                                </button>
                                <button
                                    className={`mode-btn${drawMode === 'rectangle' ? ' active' : ''}`}
                                    onClick={() => setDrawMode('rectangle')}
                                    disabled={loading}
                                    type="button"
                                >
                                    Rechteck
                                </button>
                                <button
                                    className="mode-btn"
                                    onClick={() => setDrawActive(false)}
                                    disabled={loading}
                                    type="button"
                                >
                                    Abbrechen
                                </button>
                            </div>
                            {drawMode === 'polygon' && (
                                <p className="draw-hint">
                                    Klicks setzen. Undo: Esc. Abschluss: Doppelklick.
                                </p>
                            )}
                            {drawMode === 'rectangle' && (
                                <p className="draw-hint">
                                    Ziehen zum Aufziehen des Rechtecks.
                                </p>
                            )}
                        </>
                    )}

                    {drawnArea && !drawActive && bboxAreaKm2 > AOI_SOFT_LIMIT_KM2 && (
                        <div className={`aoi-warning${bboxAreaKm2 > AOI_HIGH_LIMIT_KM2 ? ' high' : ''}`} style={{ marginTop: 12 }}>
                            {bboxAreaKm2 <= AOI_HIGH_LIMIT_KM2 && (
                                <span>Hinweis: kann laenger dauern, Large-AOI Modus kann aktiv werden.</span>
                            )}
                            {bboxAreaKm2 > AOI_HIGH_LIMIT_KM2 && (
                                <span>Grosse Auswahl: empfehlenswert ist Aufteilung in kleinere Teilgebiete.</span>
                            )}
                        </div>
                    )}
                </div>

                <WeatherPanel
                    bbox={drawnArea ? { south: drawnArea.south, west: drawnArea.west, north: drawnArea.north, east: drawnArea.east } : null}
                    analysisType={analysisType}
                    selectedEvent={selectedRainEvent}
                    onSelectEvent={setSelectedRainEvent}
                    disabled={!weatherStepEnabled}
                    onStateChange={setWeatherUiState}
                />

                <div className={`section${analysisStepEnabled ? '' : ' is-locked'}`}>
                    <div className="section-title">Analyse</div>
                    <div className="analysis-segments" role="radiogroup" aria-label="Analyse auswaehlen">
                        {[
                            { id: 'starkregen', label: 'Starkregen', icon: 'rain' },
                            { id: 'erosion', label: 'Erosion', icon: 'mountain' },
                            { id: 'sediment', label: 'Sediment', icon: 'sediment', disabled: true },
                        ].map((opt) => (
                            <button
                                key={opt.id}
                                type="button"
                                role="radio"
                                aria-checked={analysisType === opt.id}
                                className={`analysis-seg${analysisType === opt.id ? ' active' : ''}${opt.disabled ? ' is-disabled' : ''}`}
                                disabled={!!opt.disabled || !analysisStepEnabled}
                                onClick={() => { if (!opt.disabled) setAnalysisType(opt.id) }}
                            >
                                <span className="analysis-seg-icon" aria-hidden="true">
                                    {opt.icon === 'rain' && (
                                        <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                                            <path d="M7 18a4 4 0 0 1 0-8a5 5 0 0 1 9.6-1.4A3.5 3.5 0 1 1 17.5 18H7z" fill="currentColor" opacity="0.9" />
                                            <path d="M9 20l-1 2M13 20l-1 2M17 20l-1 2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                                        </svg>
                                    )}
                                    {opt.icon === 'mountain' && (
                                        <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                                            <path d="M3 19l7-12l4 7l2-3l5 8H3z" fill="currentColor" opacity="0.92" />
                                        </svg>
                                    )}
                                    {opt.icon === 'sediment' && (
                                        <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                                            <path d="M4 14c3 0 3-2 6-2s3 2 6 2s3-2 4-2v6H4v-2z" fill="currentColor" opacity="0.92" />
                                            <path d="M6 9h12" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" opacity="0.9" />
                                            <path d="M6 6h8" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" opacity="0.75" />
                                        </svg>
                                    )}
                                </span>
                                <span className="analysis-seg-label">{opt.label}</span>
                            </button>
                        ))}
                    </div>
                    <div className="analysis-meta">
                        <div className="analysis-meta-row">
                            <div className="analysis-desc" style={{ marginTop: 0 }}>
                                {analysisType === 'erosion'
                                    ? 'Erosions-Treiber, Hotspots'
                                    : analysisType === 'sediment'
                                        ? 'Sedimentpfade, Ablagerung (demnaechst)'
                                    : 'Abflusswege, Sammelpunkte, Hotspots'}
                            </div>
                        </div>
                    </div>
                    <div className="bbox-actions" style={{ marginTop: 12 }}>
                        <button
                            className={`bbox-analyze-btn${loading ? ' is-loading' : ''}`}
                            onClick={handleBboxAnalyze}
                            disabled={
                                loading ||
                                drawActive ||
                                !drawnArea ||
                                !analysisStepEnabled ||
                                analysisType === 'sediment' ||
                                (aoiProvider === 'sachsen-anhalt' &&
                                    demSource === 'public' &&
                                    (!stPublicConfirm || stPublicParts.length === 0))
                            }
                            type="button"
                        >
                            <span className="bbox-analyze-label">
                                {loading
                                    ? `${progressInfo?.step || 0}/${progressInfo?.total || 0} ${progressInfo?.message || 'Analyse laeuft...'}`
                                    : 'Analyse starten'}
                            </span>
                            {loading && (
                                <span className="bbox-analyze-progress" aria-hidden="true">
                                    <span className="bbox-analyze-progress-fill" style={{ width: `${analysisProgressPct}%` }} />
                                </span>
                            )}
                        </button>
                    </div>
                    {!analysisStepEnabled && (
                        <div className="aoi-warning" style={{ marginTop: 10 }}>
                            Bitte zuerst Regenereignisse laden.
                        </div>
                    )}
                </div>

                {devUi && (
                    <div className="section">
                        <div className="section-title">Dev</div>
                        <div className="bbox-info">
                            <div className="bbox-coords" style={{ fontSize: 12, opacity: 0.8 }}>
                                Backend auto: {apiMode} | Legacy: {LEGACY_API_URL} | Jobs: {JOB_API_URL}
                            </div>
                            <div className="bbox-actions" style={{ gap: 10, marginTop: 10 }}>
                                <select
                                    value={apiMode}
                                    onChange={(e) => setApiMode(String(e.target.value))}
                                    style={{ width: '100%' }}
                                    disabled={loading}
                                    aria-label="Dev backend mode"
                                >
                                    <option value="legacy">Force Legacy</option>
                                    <option value="jobs">Force Jobs</option>
                                </select>
                            </div>

                            {aoiProvider === 'sachsen-anhalt' && (
                                <details style={{ marginTop: 12 }}>
                                    <summary style={{ cursor: 'pointer', opacity: 0.9 }}>DEM Quelle (Dev)</summary>
                                    <div className="dem-source-box" style={{ marginTop: 10 }}>
                                        <label className="dem-source-opt">
                                            <input
                                                type="radio"
                                                name="dem-source"
                                                value="cog"
                                                checked={demSource === 'cog'}
                                                onChange={() => { setDemSource('cog'); setStPublicConfirm(false) }}
                                            />
                                            <span>COG (lokal, schnell)</span>
                                        </label>
                                        <label className="dem-source-opt">
                                            <input
                                                type="radio"
                                                name="dem-source"
                                                value="wcs"
                                                checked={demSource === 'wcs'}
                                                onChange={() => { setDemSource('wcs'); setStPublicConfirm(false) }}
                                            />
                                            <span>WCS (wenn verfuegbar)</span>
                                        </label>
                                        <label className="dem-source-opt">
                                            <input
                                                type="radio"
                                                name="dem-source"
                                                value="public"
                                                checked={demSource === 'public'}
                                                onChange={() => setDemSource('public')}
                                            />
                                            <span>Oeffentlicher Download (LVermGeo DGM1 ZIP, sehr gross)</span>
                                        </label>

                                        {demSource === 'public' && (
                                            <div className="dem-source-public">
                                                <div className="dem-source-hint">
                                                    Hinweis: Der Download kann mehrere GB umfassen. Daten werden lokal gecached (data/dem_cache).
                                                </div>
                                                <label style={{ display: 'flex', flexDirection: 'column', gap: 4, marginTop: 8 }}>
                                                    <span style={{ fontSize: 12, opacity: 0.8 }}>Download-Ordner (optional)</span>
                                                    <input
                                                        type="text"
                                                        placeholder="z.B. D:\\hydrowatch-cache oder .\\data\\dem_cache"
                                                        value={stPublicCacheDir}
                                                        onChange={(e) => setStPublicCacheDir(e.target.value)}
                                                    />
                                                </label>
                                                <div className="dem-source-parts">
                                                    {[1, 2, 3, 4].map((p) => (
                                                        <label key={p} className="dem-part">
                                                            <input
                                                                type="checkbox"
                                                                checked={stPublicParts.includes(p)}
                                                                onChange={(e) => {
                                                                    const checked = e.target.checked
                                                                    setStPublicParts((prev) => {
                                                                        const next = new Set(prev)
                                                                        if (checked) next.add(p)
                                                                        else next.delete(p)
                                                                        return Array.from(next).sort((a, b) => a - b)
                                                                    })
                                                                }}
                                                            />
                                                            <span>Part {p}</span>
                                                        </label>
                                                    ))}
                                                </div>
                                                <label className="dem-confirm">
                                                    <input
                                                        type="checkbox"
                                                        checked={stPublicConfirm}
                                                        onChange={(e) => setStPublicConfirm(e.target.checked)}
                                                    />
                                                    <span>Ich bestaetige Download/Cache lokaler DGM1-Daten.</span>
                                                </label>
                                            </div>
                                        )}
                                    </div>

                                    {demSource === 'wcs' && (
                                        <div className="bbox-actions" style={{ gap: 10, alignItems: 'center', marginTop: 10 }}>
                                            <button className="bbox-redraw-btn" onClick={runWcsSelftest} disabled={wcsHealth.loading} type="button">
                                                {wcsHealth.loading ? 'Teste...' : 'WCS Selftest'}
                                            </button>
                                            <span className={`wcs-pill ${wcsHealth.status}`} title="WCS Selftest (Capabilities/Describe/GetCoverage)">
                                                {wcsHealth.status === 'unknown' ? '?' : (wcsHealth.status === 'green' ? 'OK' : (wcsHealth.status === 'yellow' ? 'teilweise' : 'Fehler'))}
                                            </span>
                                        </div>
                                    )}
                                </details>
                            )}
                        </div>
                    </div>
                )}

                {/* AOI controls merged into the "Gebiet" section above. */}

                {(status || error) && (
                    <div className="section">
                        {status && <p className="status-success">{status}</p>}
                        {error && <p className="status-error">{error}</p>}
                    </div>
                )}

                <StatsBox data={geoJsonData} />
                <ScenarioBox data={geoJsonData} />
                <HotspotList
                    data={geoJsonData}
                    selectedRank={selectedHotspot?.rank ?? null}
                    onSelect={setSelectedHotspot}
                />
                <MeasuresPanel hotspot={selectedHotspot} />
                <ExportBox data={geoJsonData} />
                <ActionSummary data={geoJsonData} />

            </div>

            <MapContainer className={catchmentLoading ? 'map-busy' : ''} center={[initialMapView.lat, initialMapView.lon]} zoom={initialMapView.zoom} zoomControl={false} style={{ height: '100vh', width: '100%' }}>
                    <EnsureMapInteractivity enabled={!drawActive} />
                    <SnapCursorHint geojson={pointCheckGeojson || geoJsonData} enabled={!!geoJsonData && !drawActive && (showFlow || showCorridors)} />
                    <PersistMapView onView={(mv) => { scheduleUiSave({ mapView: mv }); setMapViewLive(mv) }} />
                    <ZoomControls geojson={geoJsonData} area={drawnArea} sidebarOpen={sidebarOpen} onPlaceSelect={setPlaceSelection} />
                    <HotspotNavigator hotspot={selectedHotspot} />
                    {/* No auto-zoom for catchment; users keep map context. */}
                    <CatchmentNavigator geojson={catchmentGeojson} triggerKey={`${catchmentFor?.lat ?? 'na'}:${catchmentFor?.lon ?? 'na'}:${catchmentFor?.aoiHash ?? 'na'}`} sidebarOpen={sidebarOpen} enabled={false} />
                    <FlyToHandler selection={placeSelection} sidebarOpen={sidebarOpen} />
                    <MapLayerPanel
                        layers={layers}
                        onToggle={toggleLayer}
                        basemapKey={basemap}
                        onBasemapChange={setBasemap}
                        corridorDensity={corridorDensity}
                        onCorridorDensityChange={setCorridorDensity}
                        hasCorridors={hasCorridors}
                        hasPointCheck={!!poi}
                        minCorridorKm2={minCorridorKm2}
                        maxCorridorKm2={maxCorridorKm2}
                        corridorTotalCount={corridorTotalCount}
                        corridorVisibleCount={corridorVisibleCount}
                        catchmentLoading={catchmentLoading}
                        catchmentMeta={catchmentMeta}
                        catchmentError={catchmentError}
                    />
                    {showTiles && (
                        <TileLayer
                            key={basemap}
                            attribution={bm.attribution}
                            url={bm.url}
                            // Allow "deep zoom" for precise POI placement even if the provider's tiles stop earlier.
                            maxNativeZoom={bm.maxNativeZoom || 19}
                            maxZoom={22}
                        />
                    )}

                <PointCheckHandler
                    geojson={pointCheckGeojson || geoJsonData}
                    // While drawing a new AOI we must not trigger "Objekt-Check" for the previous result layer.
                    enabled={!!geoJsonData && !drawActive && (showFlow || showCorridors)}
                    onPick={(info) => {
                        // If catchment is shown, only accept clicks that snap to a segment.
                        // This prevents accidental "point moves" in empty areas, while still allowing users
                        // to set a new catchment point anywhere by clicking a (major/minor) line.
                        if (showCatchment && catchmentGeojson?.features?.length && !info?.found) return

                        setPointCheck(info)
                        if (info?.found && Number.isFinite(Number(info.lat)) && Number.isFinite(Number(info.lon))) {
                            const lat = Number(info.lat)
                            const lon = Number(info.lon)
                            // If a catchment is currently shown, don't accidentally move the catchment point by clicking outside
                            // of the current catchment polygon. User can disable the layer to pick a new catchment location.
                            setPoi({ lat, lon })
                        }
                        if (info?.found && info?._fid !== undefined) setHighlightFeatureIds([info._fid])
                    }}
                />

                <WmsOverlay
                    visible={showOfficialExtent}
                    baseUrl={OFFICIAL_WMS[aoiProvider]?.baseUrl}
                    layerName={officialLayerName(aoiProvider, 'extent', officialScenario)}
                    opacity={0.75}
                    zIndex={420}
                />
                <WmsOverlay
                    visible={showOfficialDepth}
                    baseUrl={OFFICIAL_WMS[aoiProvider]?.baseUrl}
                    layerName={officialLayerName(aoiProvider, 'depth', officialScenario)}
                    opacity={0.55}
                    zIndex={410}
                />

                <DrawAreaHandler mode={drawMode} active={drawActive} onArea={(area) => { setDrawnArea(area); setDrawActive(false) }} />

                {drawnArea?.shapeType === 'rectangle' && drawnArea?.bounds && (
                    <Rectangle
                        bounds={drawnArea.bounds}
                        interactive={false}
                        pathOptions={{
                            color: drawActive ? '#9aa0a6' : '#00e5ff',
                            weight: 2,
                            dashArray: drawActive ? '4 4' : '6 4',
                            fillColor: drawActive ? '#9aa0a6' : '#00e5ff',
                            fillOpacity: drawActive ? 0.03 : 0.08,
                        }}
                    />
                )}
                {drawnArea?.shapeType === 'polygon' && drawnArea?.polygon?.length >= 3 && (
                    <Polygon
                        positions={drawnArea.polygon}
                        interactive={false}
                        pathOptions={{
                            color: drawActive ? '#9aa0a6' : '#00e5ff',
                            weight: 2,
                            dashArray: drawActive ? '4 4' : '6 4',
                            fillColor: drawActive ? '#9aa0a6' : '#00e5ff',
                            fillOpacity: drawActive ? 0.03 : 0.08,
                        }}
                    />
                )}

                {geoJsonData && (
                    <>
                        {showFlow && (
                            <Pane name="flowPane" style={{ zIndex: 520, pointerEvents: 'none' }}>
                                <GeoJSON
                                    key={`flow-${analysisType}-${showCorridors ? (Number(corridorDensity) || 0) : 'na'}-${(displayFlowGeojson || geoJsonData)?.features?.length ?? 0}`}
                                    data={displayFlowGeojson || geoJsonData}
                                    interactive={false}
                                    style={(feature) => ({
                                        color: riskColorOf(feature),
                                        weight: analysisType === 'erosion' ? 3.2 : 2.5,
                                        opacity: analysisType === 'erosion' ? 0.95 : 0.9,
                                    })}
                                />
                            </Pane>
                        )}

                        {showCorridors && (
                            <Pane name="corridorsPane" style={{ zIndex: 530, pointerEvents: 'none' }}>
                                <GeoJSON
                                    key={`corr-${corridorDensity}-${displayCorridorsGeojson?.features?.length ?? 0}`}
                                    data={displayCorridorsGeojson || geoJsonData}
                                    interactive={false}
                                    style={(feature) => corridorStyleOfWithMax(feature, maxCorridorKm2)}
                                />
                            </Pane>
                        )}
                        {showCatchment && catchmentGeojson?.features?.length > 0 && (
                            <Pane name="catchmentPane" style={{ zIndex: 610, pointerEvents: 'none' }}>
                                {/* Catchment: strong outline + subtle tint so it stays readable above dense networks. */}
                                <GeoJSON
                                    key={`cat-fill-${catchmentFor?.lat ?? 'na'}-${catchmentFor?.lon ?? 'na'}`}
                                    data={catchmentGeojson}
                                    interactive={false}
                                    style={() => ({
                                        color: 'rgba(255,255,255,0)',
                                        weight: 0,
                                        opacity: 0,
                                        fillColor: 'rgba(0,229,255,1.0)',
                                        fillOpacity: 0.06,
                                    })}
                                />
                                <GeoJSON
                                    key={`cat-halo-${catchmentFor?.lat ?? 'na'}-${catchmentFor?.lon ?? 'na'}`}
                                    data={catchmentGeojson}
                                    interactive={false}
                                    style={() => ({
                                        color: 'rgba(0,0,0,0.85)',
                                        weight: 10,
                                        opacity: 0.60,
                                        fillOpacity: 0,
                                        lineCap: 'round',
                                        lineJoin: 'round',
                                    })}
                                />
                                <GeoJSON
                                    key={`cat-${catchmentFor?.lat ?? 'na'}-${catchmentFor?.lon ?? 'na'}`}
                                    data={catchmentGeojson}
                                    interactive={false}
                                    style={() => ({
                                        color: 'rgba(255,255,255,0.98)',
                                        weight: 5,
                                        opacity: 1.0,
                                        fillOpacity: 0,
                                        lineCap: 'round',
                                        lineJoin: 'round',
                                    })}
                                />
                                <GeoJSON
                                    key={`cat-core-${catchmentFor?.lat ?? 'na'}-${catchmentFor?.lon ?? 'na'}`}
                                    data={catchmentGeojson}
                                    interactive={false}
                                    style={() => ({
                                        color: 'rgba(0,229,255,1.0)',
                                        weight: 2.5,
                                        opacity: 1.0,
                                        fillOpacity: 0,
                                        dashArray: '6 4',
                                        lineCap: 'round',
                                        lineJoin: 'round',
                                    })}
                                />
                            </Pane>
                        )}
                        {showCatchment && poi && Number.isFinite(Number(poi.lat)) && Number.isFinite(Number(poi.lon)) && (
                            <CircleMarker
                                center={[Number(poi.lat), Number(poi.lon)]}
                                radius={6}
                                interactive={false}
                                pathOptions={{ color: '#111827', weight: 2, fillColor: '#ffd54f', fillOpacity: 0.95 }}
                            />
                        )}

                        {showFlow && highlightFeatureIds.length > 0 && (
                            <Pane name="highlightPane" style={{ zIndex: 640, pointerEvents: 'none' }}>
                                <GeoJSON
                                    key={`hl-${selectedHotspot?.rank ?? 'none'}`}
                                    data={{
                                        type: 'FeatureCollection',
                                        features: (geoJsonData.features || []).filter((f) => highlightFeatureIds.includes(f?.properties?._fid)),
                                    }}
                                    interactive={false}
                                    style={() => ({
                                        color: '#ffd54f',
                                        weight: 4,
                                        opacity: 0.95,
                                    })}
                                />
                            </Pane>
                        )}
                        <FitBounds geojson={geoJsonData} triggerKey={fitKey} enabled={!drawActive} sidebarOpen={sidebarOpen} />
                        {selectedHotspot && (
                            <CircleMarker
                                center={[Number(selectedHotspot.lat), Number(selectedHotspot.lon)]}
                                radius={8}
                                pathOptions={{ color: '#ffffff', weight: 2, fillColor: riskColorOf({ properties: { risk_class: selectedHotspot.risk_class } }), fillOpacity: 0.9 }}
                            >
                                <Popup>
                                    <strong>Hotspot #{selectedHotspot.rank}</strong><br />
                                    Score: {selectedHotspot.risk_score} ({selectedHotspot.risk_class})<br />
                                    {selectedHotspot.reason}
                                </Popup>
                            </CircleMarker>
                        )}
                        {placeSelection && (
                            <CircleMarker
                                center={[Number(placeSelection.lat), Number(placeSelection.lon)]}
                                radius={7}
                                pathOptions={{ color: '#00e5ff', weight: 2, fillColor: '#001f2b', fillOpacity: 0.9 }}
                            >
                                <Popup>
                                    <strong>Ort</strong><br />
                                    {placeSelection.label}
                                </Popup>
                            </CircleMarker>
                        )}
                        {pointCheck && Number.isFinite(Number(pointCheck.lat)) && Number.isFinite(Number(pointCheck.lon)) && (
                            <CircleMarker
                                center={[Number(pointCheck.lat), Number(pointCheck.lon)]}
                                radius={6}
                                pathOptions={{ color: '#ffffff', weight: 2, fillColor: '#111827', fillOpacity: 0.9 }}
                            />
                        )}
                        <PointCheckPopup pointCheck={pointCheck} onClose={() => setPointCheck(null)} />
                    </>
                )}
            </MapContainer>
        </div>
    )
}

export default App



