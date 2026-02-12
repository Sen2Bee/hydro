import { useState, useCallback, useRef, useEffect } from 'react'
import { MapContainer, TileLayer, GeoJSON, Rectangle, useMap, useMapEvents } from 'react-leaflet'
import './index.css'

const API_URL = 'http://127.0.0.1:8001'

/* ========== Basemap definitions ====================================== */
const BASEMAPS = {
    standard: {
        label: 'Standard',
        url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
    },
    satellite: {
        label: 'Satellit',
        url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attribution: '&copy; Esri',
    },
    terrain: {
        label: 'Terrain',
        url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
        attribution: '&copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
    },
}

/* ========== FitBounds ================================================ */
function FitBounds({ geojson }) {
    const map = useMap()
    if (!geojson?.features?.length) return null

    const coords = []
    for (const f of geojson.features) {
        const g = f.geometry
        if (!g?.coordinates) continue
        if (g.type === 'LineString')
            for (const c of g.coordinates) coords.push([c[1], c[0]])
        else if (g.type === 'MultiLineString')
            for (const line of g.coordinates)
                for (const c of line) coords.push([c[1], c[0]])
    }
    if (coords.length > 0) map.fitBounds(coords, { padding: [30, 30] })
    return null
}

/* ========== DrawBboxHandler ========================================== */
function DrawBboxHandler({ active, onBbox }) {
    const [startPoint, setStartPoint] = useState(null)
    const [currentRect, setCurrentRect] = useState(null)
    const map = useMap()

    // Toggle drag behaviour when draw mode changes
    useEffect(() => {
        if (active) {
            map.dragging.disable()
            map.getContainer().style.cursor = 'crosshair'
        } else {
            map.dragging.enable()
            map.getContainer().style.cursor = ''
            setStartPoint(null)
            setCurrentRect(null)
        }
        return () => {
            map.dragging.enable()
            map.getContainer().style.cursor = ''
        }
    }, [active, map])

    useMapEvents({
        mousedown(e) {
            if (!active) return
            setStartPoint(e.latlng)
            setCurrentRect(null)
        },
        mousemove(e) {
            if (!active || !startPoint) return
            setCurrentRect([
                [startPoint.lat, startPoint.lng],
                [e.latlng.lat, e.latlng.lng],
            ])
        },
        mouseup(e) {
            if (!active || !startPoint) return
            const bounds = [
                [startPoint.lat, startPoint.lng],
                [e.latlng.lat, e.latlng.lng],
            ]
            setCurrentRect(bounds)
            setStartPoint(null)

            const south = Math.min(bounds[0][0], bounds[1][0])
            const north = Math.max(bounds[0][0], bounds[1][0])
            const west = Math.min(bounds[0][1], bounds[1][1])
            const east = Math.max(bounds[0][1], bounds[1][1])

            if (Math.abs(north - south) > 0.0001 && Math.abs(east - west) > 0.0001) {
                onBbox({ south, west, north, east, bounds })
            }
        },
    })

    if (!currentRect) return null

    return (
        <Rectangle
            bounds={currentRect}
            pathOptions={{
                color: '#00e5ff',
                weight: 2,
                dashArray: '6 4',
                fillColor: '#00e5ff',
                fillOpacity: 0.08,
            }}
        />
    )
}

/* ========== DropZone ================================================ */
function DropZone({ onFile, disabled }) {
    const [dragOver, setDragOver] = useState(false)
    const inputRef = useRef()

    const handleDrag = useCallback((e) => {
        e.preventDefault()
        e.stopPropagation()
    }, [])

    const handleDragIn = useCallback((e) => {
        handleDrag(e)
        setDragOver(true)
    }, [handleDrag])

    const handleDragOut = useCallback((e) => {
        handleDrag(e)
        setDragOver(false)
    }, [handleDrag])

    const handleDrop = useCallback(
        (e) => {
            handleDrag(e)
            setDragOver(false)
            if (disabled) return
            const file = e.dataTransfer?.files?.[0]
            if (file) onFile(file)
        },
        [handleDrag, disabled, onFile],
    )

    const handleClick = () => inputRef.current?.click()
    const handleChange = (e) => {
        const file = e.target.files?.[0]
        if (file) onFile(file)
    }

    return (
        <div
            className={`dropzone${dragOver ? ' drag-over' : ''}`}
            onClick={handleClick}
            onDragEnter={handleDragIn}
            onDragOver={handleDragIn}
            onDragLeave={handleDragOut}
            onDrop={handleDrop}
        >
            <span className="dropzone-icon">📂</span>
            <span className="dropzone-label">GeoTIFF hochladen</span>
            <span className="dropzone-hint">Datei hierhin ziehen oder klicken</span>
            <input
                ref={inputRef}
                type="file"
                accept=".tif,.tiff"
                onChange={handleChange}
                disabled={disabled}
            />
        </div>
    )
}

/* ========== ProgressBar ============================================= */
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

/* ========== NDJSON stream reader ==================================== */
async function readNdjsonStream(response, onProgress, onResult, onError) {
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const lines = buffer.split('\n')
        buffer = lines.pop() // keep incomplete line

        for (const line of lines) {
            if (!line.trim()) continue
            try {
                const evt = JSON.parse(line)
                if (evt.type === 'progress') onProgress(evt)
                else if (evt.type === 'result') onResult(evt.data)
                else if (evt.type === 'error') onError(evt.detail)
            } catch (e) {
                console.warn('Failed to parse NDJSON line:', line, e)
            }
        }
    }
}

/* ========== ThresholdSlider ========================================= */
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

/* ========== StatsBox ================================================ */
function StatsBox({ data }) {
    if (!data) return null

    const nFeatures = data.features?.length ?? 0

    let minLat = Infinity, maxLat = -Infinity, minLng = Infinity, maxLng = -Infinity
    for (const f of data.features || []) {
        const g = f.geometry
        if (!g?.coordinates) continue
        const lines = g.type === 'MultiLineString' ? g.coordinates : [g.coordinates]
        for (const line of lines) {
            for (const [lng, lat] of line) {
                if (lat < minLat) minLat = lat
                if (lat > maxLat) maxLat = lat
                if (lng < minLng) minLng = lng
                if (lng > maxLng) maxLng = lng
            }
        }
    }

    const latSpan = maxLat > -Infinity ? (maxLat - minLat).toFixed(3) : '–'
    const lngSpan = maxLng > -Infinity ? (maxLng - minLng).toFixed(3) : '–'

    return (
        <div className="section">
            <div className="section-title">Ergebnisse</div>
            <div className="stats-grid">
                <div className="stat-card">
                    <div className="stat-value">{nFeatures}</div>
                    <div className="stat-label">Fließsegmente</div>
                </div>
                <div className="stat-card">
                    <div className="stat-value">{latSpan}°</div>
                    <div className="stat-label">Lat-Spanne</div>
                </div>
                <div className="stat-card">
                    <div className="stat-value">{lngSpan}°</div>
                    <div className="stat-label">Lng-Spanne</div>
                </div>
                <div className="stat-card">
                    <div className="stat-value">EPSG</div>
                    <div className="stat-label">Koordinaten</div>
                </div>
            </div>
        </div>
    )
}

/* ========== Legend =================================================== */
function Legend() {
    return (
        <div className="section">
            <div className="section-title">Legende</div>
            <div className="legend-row">
                <span
                    className="legend-line"
                    style={{ borderTop: '3px solid #00e5ff' }}
                />
                Fließnetzwerk (Hauptkanäle)
            </div>
            <div className="legend-row">
                <span
                    className="legend-line"
                    style={{ borderTop: '2px solid #00e5ff', opacity: 0.5 }}
                />
                Nebenkanäle (geringer Abfluss)
            </div>
        </div>
    )
}

/* ========== LayerToggle ============================================= */
function LayerToggle({ layers, onToggle }) {
    return (
        <div className="section">
            <div className="section-title">Kartenebenen</div>
            <div className="layer-list">
                {layers.map((l) => (
                    <label className="layer-item" key={l.id}>
                        <input
                            type="checkbox"
                            checked={l.visible}
                            onChange={() => onToggle(l.id)}
                        />
                        <span
                            className="layer-swatch"
                            style={{ background: l.color }}
                        />
                        {l.name}
                    </label>
                ))}
            </div>
        </div>
    )
}

/* ========== BasemapSwitcher ========================================= */
function BasemapSwitcher({ active, onChange }) {
    return (
        <div className="section">
            <div className="section-title">Basiskarte</div>
            <div className="basemap-group">
                {Object.entries(BASEMAPS).map(([key, bm]) => (
                    <button
                        key={key}
                        className={`basemap-btn${active === key ? ' active' : ''}`}
                        onClick={() => onChange(key)}
                    >
                        {bm.label}
                    </button>
                ))}
            </div>
        </div>
    )
}

/* ========== AboutSection ============================================ */
function AboutSection() {
    const [open, setOpen] = useState(false)

    return (
        <div className="section">
            <button className="accordion-header" onClick={() => setOpen(!open)}>
                Über Hydrowatch
                <span className={`accordion-chevron${open ? ' open' : ''}`}>▼</span>
            </button>
            <div className={`accordion-body${open ? ' open' : ''}`}>
                <div className="about-text">
                    <p>
                        <strong>Hydrowatch Berlin</strong> analysiert digitale
                        Geländemodelle (GeoTIFF) und berechnet Fließnetzwerke
                        mittels D8-Algorithmus (PySheds).
                    </p>
                    <p>
                        <strong>Kartenauswahl:</strong> Zeichnen Sie ein Rechteck
                        auf der Karte, um automatisch ein DGM1 (1m) von
                        Geobasis NRW zu laden und zu analysieren.
                    </p>
                    <p>
                        <strong>Upload:</strong> Alternativ eigene GeoTIFF-Dateien
                        hochladen. Threshold anpassen (niedrig = mehr Detail).
                    </p>
                    <p>
                        Die Ergebnisse dienen der indikativen Analyse und
                        ersetzen keine professionelle hydrologische Bewertung.
                    </p>
                </div>
            </div>
        </div>
    )
}

/* ========== ZoomControls ============================================ */
function ZoomControls({ geojson }) {
    const map = useMap()
    return (
        <div className="map-controls">
            <button className="map-ctrl-btn" onClick={() => map.zoomIn()} title="Vergrößern">+</button>
            <button className="map-ctrl-btn" onClick={() => map.zoomOut()} title="Verkleinern">−</button>
            {geojson?.features?.length > 0 && (
                <button className="map-ctrl-btn" onClick={() => {
                    const coords = []
                    for (const f of geojson.features) {
                        const g = f.geometry
                        if (!g?.coordinates) continue
                        if (g.type === 'LineString')
                            for (const c of g.coordinates) coords.push([c[1], c[0]])
                        else if (g.type === 'MultiLineString')
                            for (const line of g.coordinates)
                                for (const c of line) coords.push([c[1], c[0]])
                    }
                    if (coords.length > 0) map.fitBounds(coords, { padding: [30, 30] })
                }} title="Auf Ergebnis zentrieren">⊕</button>
            )}
        </div>
    )
}

/* ========== MapLayerPanel =========================================== */
function MapLayerPanel({ layers, onToggle, basemapKey, onBasemapChange }) {
    const [open, setOpen] = useState(false)
    return (
        <div className="map-layer-panel">
            <button className="map-ctrl-btn" onClick={() => setOpen(o => !o)} title="Kartenebenen">
                {open ? '✕' : '◫'}
            </button>
            {open && (
                <div className="map-layer-dropdown">
                    <div className="map-layer-section">Ebenen</div>
                    {layers.map(l => (
                        <label className="map-layer-item" key={l.id}>
                            <input type="checkbox" checked={l.visible} onChange={() => onToggle(l.id)} />
                            <span className="layer-swatch" style={{ background: l.color }} />
                            {l.name}
                        </label>
                    ))}
                    <div className="map-layer-section">Basiskarte</div>
                    {Object.entries(BASEMAPS).map(([key, bm]) => (
                        <label className="map-layer-item" key={key}>
                            <input type="radio" name="basemap" checked={basemapKey === key}
                                onChange={() => onBasemapChange(key)} />
                            {bm.label}
                        </label>
                    ))}
                </div>
            )}
        </div>
    )
}

/* ========== Main App ================================================ */
function App() {
    const [geoJsonData, setGeoJsonData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [status, setStatus] = useState(null)
    const [error, setError] = useState(null)
    const [progressInfo, setProgressInfo] = useState({ step: 0, total: 0, message: '' })
    const [threshold, setThreshold] = useState(200)
    const [basemap, setBasemap] = useState('standard')
    const [sidebarOpen, setSidebarOpen] = useState(true)
    const [inputMode, setInputMode] = useState('upload') // 'upload' | 'draw'
    const [drawActive, setDrawActive] = useState(false)
    const [drawnBbox, setDrawnBbox] = useState(null)     // { south, west, north, east, bounds }
    const [layers, setLayers] = useState([
        { id: 'tiles', name: 'Basiskarte', visible: true, color: '#888' },
        { id: 'flow', name: 'Fließnetzwerk', visible: true, color: '#00e5ff' },
    ])

    const toggleLayer = useCallback((id) => {
        setLayers((prev) =>
            prev.map((l) => (l.id === id ? { ...l, visible: !l.visible } : l)),
        )
    }, [])

    /* ---- Streaming request helper ---- */
    const runStreamingRequest = useCallback(async (url, options = {}) => {
        setLoading(true)
        setError(null)
        setGeoJsonData(null)
        setProgressInfo({ step: 0, total: 0, message: 'Verbindung wird aufgebaut…' })

        try {
            const res = await fetch(url, options)

            if (!res.ok) {
                const text = await res.text()
                let detail = text
                try { detail = JSON.parse(text).detail } catch { }
                throw new Error(detail)
            }

            await readNdjsonStream(
                res,
                (evt) => setProgressInfo({ step: evt.step, total: evt.total, message: evt.message }),
                (data) => {
                    const n = data?.features?.length ?? 0
                    setGeoJsonData(data)
                    setStatus(`✓ Analyse abgeschlossen – ${n} Fließsegmente`)
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
    }, [])

    /* ---- File upload handler ---- */
    const handleFile = useCallback(
        async (file) => {
            if (!file) return
            const body = new FormData()
            body.append('file', file)
            await runStreamingRequest(
                `${API_URL}/analyze?threshold=${threshold}`,
                { method: 'POST', body },
            )
        },
        [threshold, runStreamingRequest],
    )

    /* ---- Bbox draw handler ---- */
    const handleBboxDrawn = useCallback((bbox) => {
        setDrawnBbox(bbox)
        setDrawActive(false)
    }, [])

    const handleBboxAnalyze = useCallback(async () => {
        if (!drawnBbox) return
        setProgressInfo({ step: 0, total: 8, message: 'DGM wird vom WCS geladen…' })
        await runStreamingRequest(
            `${API_URL}/analyze-bbox?threshold=${threshold}`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    south: drawnBbox.south,
                    west: drawnBbox.west,
                    north: drawnBbox.north,
                    east: drawnBbox.east,
                }),
            },
        )
    }, [drawnBbox, threshold, runStreamingRequest])

    const bm = BASEMAPS[basemap]
    const showTiles = layers.find((l) => l.id === 'tiles')?.visible ?? true
    const showFlow = layers.find((l) => l.id === 'flow')?.visible ?? true
    const defaultCenter = [50.94, 6.96]  // Köln, NRW

    return (
        <div className="app-container">
            {/* Hamburger toggle (mobile) */}
            <button
                className="sidebar-toggle"
                onClick={() => setSidebarOpen((o) => !o)}
                aria-label="Toggle Sidebar"
            >
                {sidebarOpen ? '✕' : '☰'}
            </button>

            {/* ---- Sidebar ---- */}
            <div className={`sidebar${sidebarOpen ? '' : ' collapsed'}`}>
                <div className="sidebar-header">
                    <h1>🌊 Hydrowatch</h1>
                    <p className="subtitle">Starkregen-Risikoanalyse</p>
                </div>

                {/* Mode toggle */}
                <div className="section">
                    <div className="section-title">Eingabemodus</div>
                    <div className="mode-toggle">
                        <button
                            className={`mode-btn${inputMode === 'upload' ? ' active' : ''}`}
                            onClick={() => {
                                setInputMode('upload')
                                setDrawActive(false)
                            }}
                        >
                            📂 Upload
                        </button>
                        <button
                            className={`mode-btn${inputMode === 'draw' ? ' active' : ''}`}
                            onClick={() => setInputMode('draw')}
                        >
                            ✏️ Kartenauswahl
                        </button>
                    </div>
                </div>

                {/* Upload mode */}
                {inputMode === 'upload' && (
                    <div className="section">
                        <div className="section-title">DEM Upload</div>
                        <DropZone onFile={handleFile} disabled={loading} />
                    </div>
                )}

                {/* Draw mode */}
                {inputMode === 'draw' && (
                    <div className="section">
                        <div className="section-title">Kartenauswahl (DGM1 – 1m)</div>
                        {!drawActive && !drawnBbox && (
                            <button
                                className="draw-start-btn"
                                onClick={() => {
                                    setDrawActive(true)
                                    setDrawnBbox(null)
                                }}
                                disabled={loading}
                            >
                                Rechteck zeichnen
                            </button>
                        )}
                        {drawActive && (
                            <p className="draw-hint">
                                Ziehen Sie ein Rechteck auf der Karte…
                            </p>
                        )}
                        {drawnBbox && !drawActive && (
                            <div className="bbox-info">
                                <div className="bbox-coords">
                                    {drawnBbox.south.toFixed(4)}°N – {drawnBbox.north.toFixed(4)}°N
                                    <br />
                                    {drawnBbox.west.toFixed(4)}°E – {drawnBbox.east.toFixed(4)}°E
                                </div>
                                <div className="bbox-actions">
                                    <button
                                        className="bbox-analyze-btn"
                                        onClick={handleBboxAnalyze}
                                        disabled={loading}
                                    >
                                        Analysieren
                                    </button>
                                    <button
                                        className="bbox-redraw-btn"
                                        onClick={() => {
                                            setDrawActive(true)
                                            setDrawnBbox(null)
                                        }}
                                        disabled={loading}
                                    >
                                        Neu zeichnen
                                    </button>
                                </div>
                            </div>
                        )}
                    </div>
                )}

                {/* Status (shared) */}
                {(loading || status || error) && (
                    <div className="section">
                        {loading && <ProgressBar step={progressInfo.step} total={progressInfo.total} message={progressInfo.message} />}
                        {status && !loading && (
                            <p className="status-success">{status}</p>
                        )}
                        {error && <p className="status-error">{error}</p>}
                    </div>
                )}

                {/* Threshold */}
                <ThresholdSlider value={threshold} onChange={setThreshold} />

                {/* Stats */}
                <StatsBox data={geoJsonData} />

                {/* Legend */}
                <Legend />



                {/* About */}
                <AboutSection />

                {/* Disclaimer */}
                <div className="disclaimer">
                    <strong>Haftungsausschluss:</strong> Indikative Analyse
                    basierend auf Topographiedaten. Keine rechtsverbindliche
                    Hochwasservorsorge.
                    <br />
                    DGM1-Daten: © Geobasis NRW, Datenlizenz Deutschland – Zero.
                </div>
            </div>

            {/* ---- Map ---- */}
            <MapContainer
                center={defaultCenter}
                zoom={11}
                zoomControl={false}
                style={{ height: '100vh', width: '100%' }}
            >
                <ZoomControls geojson={geoJsonData} />
                <MapLayerPanel layers={layers} onToggle={toggleLayer} basemapKey={basemap} onBasemapChange={setBasemap} />
                {showTiles && (
                    <TileLayer
                        key={basemap}
                        attribution={bm.attribution}
                        url={bm.url}
                    />
                )}

                {/* Draw handler */}
                <DrawBboxHandler
                    active={drawActive}
                    onBbox={handleBboxDrawn}
                />

                {/* Drawn bbox rectangle (persistent) */}
                {drawnBbox?.bounds && !drawActive && (
                    <Rectangle
                        bounds={drawnBbox.bounds}
                        pathOptions={{
                            color: '#00e5ff',
                            weight: 2,
                            dashArray: '6 4',
                            fillColor: '#00e5ff',
                            fillOpacity: 0.08,
                        }}
                    />
                )}

                {geoJsonData && showFlow && (
                    <>
                        <GeoJSON
                            key={JSON.stringify(geoJsonData).slice(0, 100)}
                            data={geoJsonData}
                            style={{
                                color: '#00e5ff',
                                weight: 2.5,
                                opacity: 0.85,
                            }}
                        />
                        <FitBounds geojson={geoJsonData} />
                    </>
                )}
            </MapContainer>
        </div>
    )
}

export default App
