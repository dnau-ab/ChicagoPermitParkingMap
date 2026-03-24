from __future__ import annotations

import argparse
import colorsys
import json
import os
from pathlib import Path
from typing import Any

import folium
import geopy.geocoders as geopy_geocoders
import pandas as pd
from branca.element import Element
from geopy.exc import GeopyError
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS, Nominatim, Photon


CHICAGO_CENTER = (41.8781, -87.6298)
DEFAULT_CSV = "Parking_Permit_Zones_20260313.csv"
DEFAULT_OUTPUT = "parking_permit_zones_map.html"
DEFAULT_CACHE = "geocode_cache_chicago.json"
DEFAULT_CACHE_SAVE_EVERY = 25
DEFAULT_GEOCODER = "nominatim"
DEFAULT_GEOCODE_TIMEOUT = 10
DEFAULT_GEOCODE_MIN_DELAY_SECONDS = 1.0
DEFAULT_GEOCODE_MAX_RETRIES = 2
DEFAULT_GEOCODE_ERROR_WAIT_SECONDS = 2.0
GEOCODER_GOOGLE_API_KEY_ENV = "GEOCODER_GOOGLE_API_KEY"
GEOCODER_HERE_API_KEY_ENV = "GEOCODER_HERE_API_KEY"
GEOCODER_MAPBOX_API_KEY_ENV = "GEOCODER_MAPBOX_API_KEY"


def zone_to_color(zone: str) -> str:
    """Create a deterministic, high-contrast color for each zone."""
    text = str(zone)
    h = (hash(text) % 360) / 360.0
    s = 0.65
    v = 0.90
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def build_address(row: pd.Series, number_col: str) -> str:
    number = str(row[number_col]).strip()
    direction = str(row["STREET DIRECTION"]).strip()
    street_name = str(row["STREET NAME"]).strip()
    street_type = str(row["STREET TYPE"]).strip()
    return f"{number} {direction} {street_name} {street_type}, Chicago, IL"


def normalize_ward_value(value: Any) -> str:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""

    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
    except ValueError:
        pass

    return text


def load_cache(cache_path: Path) -> dict[str, Any]:
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache_path: Path, cache: dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def geocode_address(
    address: str,
    geocode: RateLimiter,
    cache: dict[str, Any],
    cache_namespace: str,
) -> tuple[float, float] | None:
    cache_key = f"{cache_namespace}::{address}"
    cached = cache.get(cache_key)
    if cached is None:
        try:
            location = geocode(address)
        except (GeopyError, TimeoutError, OSError):
            cache[cache_key] = None
            return None

        if location is None:
            cache[cache_key] = None
            return None
        lat_lon = [location.latitude, location.longitude]
        cache[cache_key] = lat_lon
        return location.latitude, location.longitude

    if cached is None:
        return None

    return float(cached[0]), float(cached[1])


def create_geocoder(provider: str, timeout: int) -> Any:
    provider_key = provider.strip().lower()

    if provider_key == "nominatim":
        return Nominatim(user_agent="chicago-parking-permit-zones-map", timeout=timeout)

    if provider_key == "arcgis":
        return ArcGIS(timeout=timeout)

    if provider_key == "photon":
        return Photon(user_agent="chicago-parking-permit-zones-map", timeout=timeout)

    if provider_key == "google":
        google_api_key = get_required_env(GEOCODER_GOOGLE_API_KEY_ENV)
        google_cls = getattr(geopy_geocoders, "GoogleV3", None)
        if google_cls is None:
            raise RuntimeError("GoogleV3 geocoder class is unavailable in this geopy installation.")
        return google_cls(api_key=google_api_key, timeout=timeout)

    if provider_key == "here":
        here_api_key = get_required_env(GEOCODER_HERE_API_KEY_ENV)
        here_v7_cls = getattr(geopy_geocoders, "HereV7", None)
        if here_v7_cls is not None:
            return here_v7_cls(apikey=here_api_key, timeout=timeout)

        raise RuntimeError(
            "Here geocoder class is unavailable in this geopy installation. "
            "Upgrade geopy to a version that includes HereV7."
        )

    if provider_key == "mapbox":
        mapbox_api_key = get_required_env(GEOCODER_MAPBOX_API_KEY_ENV)
        mapbox_cls = getattr(geopy_geocoders, "MapBox", None)
        if mapbox_cls is None:
            raise RuntimeError("MapBox geocoder class is unavailable in this geopy installation.")
        return mapbox_cls(api_key=mapbox_api_key, timeout=timeout)

    supported = ["nominatim", "arcgis", "photon", "google", "here", "mapbox"]
    raise ValueError(f"Unsupported geocoder '{provider}'. Supported values: {', '.join(supported)}")


def get_required_env(variable_name: str) -> str:
    value = os.environ.get(variable_name, "").strip()
    if value:
        return value
    raise ValueError(
        f"Missing required environment variable '{variable_name}' for selected geocoder provider."
    )


def create_rate_limited_geocode(args: argparse.Namespace) -> RateLimiter:
    geolocator = create_geocoder(args.geocoder, args.geocode_timeout)
    return RateLimiter(
        geolocator.geocode,
        min_delay_seconds=args.geocode_min_delay_seconds,
        max_retries=args.geocode_max_retries,
        error_wait_seconds=args.geocode_error_wait_seconds,
        swallow_exceptions=True,
        return_value_on_exception=None,
    )


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an interactive OpenStreetMap visualization of Chicago parking permit zones "
            "with status/buffer filters and zone-based colors."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to the parking permit CSV. Defaults to data/Parking_Permit_Zones_20260313.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML map file. Defaults to src/parking_permit_zones_map.html",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="Path to geocode cache JSON. Defaults to data/geocode_cache_chicago.json",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional: process only first N rows (useful for quick testing).",
    )
    parser.add_argument(
        "--geocoder",
        type=str,
        default=DEFAULT_GEOCODER,
        choices=["nominatim", "arcgis", "photon", "google", "here", "mapbox"],
        help=(
            "Geocoding provider to use. 'nominatim' is the current default; "
            "API-key providers read credentials from environment variables: "
            "google->GEOCODER_GOOGLE_API_KEY, here->GEOCODER_HERE_API_KEY, "
            "mapbox->GEOCODER_MAPBOX_API_KEY."
        ),
    )
    parser.add_argument(
        "--geocode-timeout",
        type=int,
        default=DEFAULT_GEOCODE_TIMEOUT,
        help="Per-request geocoder timeout in seconds.",
    )
    parser.add_argument(
        "--geocode-min-delay-seconds",
        type=float,
        default=DEFAULT_GEOCODE_MIN_DELAY_SECONDS,
        help="Minimum delay between geocoding calls in seconds.",
    )
    parser.add_argument(
        "--geocode-max-retries",
        type=int,
        default=DEFAULT_GEOCODE_MAX_RETRIES,
        help="How many times to retry a failed geocode call.",
    )
    parser.add_argument(
        "--geocode-error-wait-seconds",
        type=float,
        default=DEFAULT_GEOCODE_ERROR_WAIT_SECONDS,
        help="Delay before retrying after geocode errors.",
    )
    return parser.parse_args()


def add_filter_controls(
    parking_map: folium.Map,
    geojson_var_name: str,
    statuses: list[str],
    buffers: list[str],
    wards: list[str],
    rendered_segments: int,
    skipped_rows: int,
) -> None:
    controls_html = """
    <style>
        #permit-panel-stack {
            position: fixed;
            top: clamp(8px, 2vh, 14px);
            left: clamp(8px, 2vw, 14px);
            bottom: clamp(8px, 2vh, 14px);
            width: clamp(240px, 30vw, 340px);
            max-width: calc(100vw - 16px);
            display: flex;
            flex-direction: column;
            gap: 8px;
            z-index: 9999;
            overflow-y: auto;
            padding-right: 2px;
            box-sizing: border-box;
            pointer-events: none;
        }

        .permit-panel {
            background: white;
            border: 1px solid #d0d7de;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.18);
            font-family: Arial, sans-serif;
            padding: 10px 12px;
            box-sizing: border-box;
            pointer-events: auto;
        }

        #permit-summary-panel {
            order: 1;
        }

        #permit-filter-panel {
            order: 2;
        }

        #permit-search-panel {
            order: 3;
        }

        .permit-panel-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            font-weight: bold;
            margin-bottom: 8px;
        }

        .permit-panel-toggle {
            border: 1px solid #cbd5e1;
            background: #f8fafc;
            border-radius: 6px;
            padding: 2px 8px;
            font-size: 12px;
            cursor: pointer;
        }

        .permit-panel-toggle:hover,
        .permit-search-action:hover {
            background: #eef2f7;
        }

        .permit-search-input {
            width: 100%;
            box-sizing: border-box;
            margin-bottom: 8px;
            border: 1px solid #cbd5e1;
            background: #f8fafc;
            border-radius: 6px;
            padding: 8px 10px;
            font-size: 13px;
            color: #111827;
            outline: none;
        }

        .permit-search-input:focus {
            border-color: #94a3b8;
            box-shadow: 0 0 0 2px rgba(148, 163, 184, 0.18);
            background: #ffffff;
        }

        .permit-search-actions {
            display: flex;
            gap: 8px;
            margin-bottom: 8px;
        }

        .permit-search-action {
            flex: 1;
            border: 1px solid #cbd5e1;
            background: #f8fafc;
            border-radius: 6px;
            padding: 6px 10px;
            font-size: 12px;
            cursor: pointer;
        }

        .permit-filter-select {
            width: 100%;
            box-sizing: border-box;
            border: 1px solid #cbd5e1;
            background: #f8fafc;
            border-radius: 6px;
            padding: 7px 10px;
            font-size: 13px;
            color: #111827;
            outline: none;
            appearance: none;
            -webkit-appearance: none;
            -moz-appearance: none;
            background-image: linear-gradient(45deg, transparent 50%, #64748b 50%), linear-gradient(135deg, #64748b 50%, transparent 50%);
            background-position: calc(100% - 16px) calc(50% - 2px), calc(100% - 11px) calc(50% - 2px);
            background-size: 5px 5px, 5px 5px;
            background-repeat: no-repeat;
            padding-right: 30px;
        }

        .permit-filter-select:focus {
            border-color: #94a3b8;
            box-shadow: 0 0 0 2px rgba(148, 163, 184, 0.18);
            background-color: #ffffff;
        }

        .permit-panel.is-collapsed .permit-panel-body {
            display: none;
        }

        .permit-panel.is-collapsed .permit-panel-header {
            margin-bottom: 0;
        }

        #permit-summary-panel .permit-panel-body {
            font-size: 13px;
        }

        #permit-filter-panel .permit-panel-body {
            max-height: min(28vh, 220px);
            overflow-y: auto;
            padding-right: 2px;
        }

        #permit-search-panel .permit-panel-body {
            max-height: min(40vh, 320px);
            overflow-y: auto;
            padding-right: 2px;
        }

        #addressSearchResults {
            max-height: min(24vh, 180px);
            overflow-y: auto;
        }

        @media (max-width: 900px) {
            #permit-panel-stack {
                top: 8px;
                left: 8px;
                right: 8px;
                bottom: 8px;
                width: auto;
                max-width: none;
            }

            .permit-panel {
                padding: 8px 10px;
            }

            #permit-filter-panel .permit-panel-body {
                max-height: min(28vh, 190px);
            }

            #permit-search-panel .permit-panel-body {
                max-height: min(36vh, 250px);
            }
        }

        @media (max-width: 560px) {
            #permit-panel-stack {
                gap: 6px;
            }

            .permit-panel {
                padding: 7px 9px;
            }

            #permit-summary-panel .permit-panel-body {
                font-size: 12px;
            }

            #permit-filter-panel .permit-panel-body {
                max-height: min(30vh, 190px);
            }

            #permit-search-panel .permit-panel-body {
                max-height: min(38vh, 220px);
            }

            #addressSearchResults {
                max-height: min(24vh, 130px);
            }
        }
    </style>

    <div id="permit-panel-stack">
        <div id="permit-summary-panel" class="permit-panel" data-default-mobile-collapsed="true">
            <div class="permit-panel-header">
                <span>Summary</span>
                <button id="permitSummaryToggle" class="permit-panel-toggle" type="button" aria-expanded="true">Collapse</button>
            </div>
            <div class="permit-panel-body">
                Chicago Parking Permit Zones<br>Rendered segments: __RENDERED__ | Skipped: __SKIPPED__
            </div>
        </div>

        <div id="permit-filter-panel" class="permit-panel" data-default-mobile-collapsed="true">
            <div class="permit-panel-header">
                <span>Filters</span>
                <button id="permitFilterToggle" class="permit-panel-toggle" type="button" aria-expanded="true">Collapse</button>
            </div>
            <div class="permit-panel-body">
                <label style="display:block; font-size:12px; margin-bottom:4px;">Status</label>
                <select id="statusFilter" class="permit-filter-select" style="margin-bottom:10px;"></select>

                <label style="display:block; font-size:12px; margin-bottom:4px;">Buffer</label>
                <select id="bufferFilter" class="permit-filter-select" style="margin-bottom:10px;"></select>

                <label style="display:block; font-size:12px; margin-bottom:4px;">Ward</label>
                <select id="wardFilter" class="permit-filter-select" style="margin-bottom:2px;"></select>
            </div>
        </div>

        <div id="permit-search-panel" class="permit-panel" data-default-mobile-collapsed="false">
            <div class="permit-panel-header">
                <span>Address Search</span>
                <button id="permitSearchToggle" class="permit-panel-toggle" type="button" aria-expanded="true">Collapse</button>
            </div>
            <div class="permit-panel-body">
                <input id="addressSearchInput" class="permit-search-input" type="text" placeholder="e.g. 6705 W Hurlbut St" />
                <div class="permit-search-actions">
                    <button id="addressSearchBtn" class="permit-search-action" type="button">Search</button>
                    <button id="addressClearBtn" class="permit-search-action" type="button">Clear</button>
                </div>
                <div id="addressSearchSummary" style="font-size:12px; margin-bottom:6px; color:#4b5563;">No search applied.</div>
                <div id="addressSearchResults" style="border-top:1px solid #e5e7eb; padding-top:6px; font-size:12px;"></div>
            </div>
        </div>

    </div>
    """.replace("__RENDERED__", str(rendered_segments)).replace("__SKIPPED__", str(skipped_rows))

    status_values = ["ALL", *statuses]
    buffer_values = ["ALL", *buffers]
    ward_values = ["ALL", *wards]

    controls_js = f"""
    <script>
        (function() {{
            var statusOptions = {json.dumps(status_values)};
            var bufferOptions = {json.dumps(buffer_values)};
            var wardOptions = {json.dumps(ward_values)};
            var searchActive = false;
            var matchedZones = new Set();
            var selectedRowId = null;

            function setupPanelToggle(panelId, buttonId) {{
                var panel = document.getElementById(panelId);
                var button = document.getElementById(buttonId);
                if (!panel || !button || button.dataset.bound === 'true') {{
                    return;
                }}

                function updateButtonState() {{
                    var collapsed = panel.classList.contains('is-collapsed');
                    button.textContent = collapsed ? 'Expand' : 'Collapse';
                    button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
                }}

                if (window.matchMedia && window.matchMedia('(max-width: 900px)').matches && panel.dataset.defaultMobileCollapsed === 'true') {{
                    panel.classList.add('is-collapsed');
                }}

                updateButtonState();
                button.addEventListener('click', function() {{
                    panel.classList.toggle('is-collapsed');
                    updateButtonState();
                }});
                button.dataset.bound = 'true';
            }}

            function addOptions(select, values) {{
                select.innerHTML = '';
                values.forEach(function(value) {{
                    var option = document.createElement('option');
                    option.value = value;
                    option.text = value;
                    select.appendChild(option);
                }});
                if (select.id === 'statusFilter' && values.indexOf('ACTIVE') >= 0) {{
                    select.value = 'ACTIVE';
                    return;
                }}

                select.value = 'ALL';
            }}

            function applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect) {{
                var selectedStatus = statusSelect.value;
                var selectedBuffer = bufferSelect.value;
                var selectedWard = wardSelect.value;

                geoJsonLayer.eachLayer(function(layer) {{
                    var props = layer.feature && layer.feature.properties ? layer.feature.properties : {{}};
                    var statusMatch = selectedStatus === 'ALL' || props.status === selectedStatus;
                    var bufferMatch = selectedBuffer === 'ALL' || props.buffer === selectedBuffer;
                    var wardLow = parseInt(props.ward_low, 10);
                    var wardHigh = parseInt(props.ward_high, 10);
                    var selectedWardNum = parseInt(selectedWard, 10);

                    var wardMatch = selectedWard === 'ALL' || (
                        !Number.isNaN(wardLow) &&
                        !Number.isNaN(wardHigh) &&
                        !Number.isNaN(selectedWardNum) &&
                        selectedWardNum >= wardLow &&
                        selectedWardNum <= wardHigh
                    );

                    var searchMatch = !searchActive || matchedZones.has(String(props.zone));
                    var visible = statusMatch && bufferMatch && wardMatch && searchMatch;
                    var isSelected = visible && selectedRowId !== null && String(props.row_id) === selectedRowId;

                    layer.setStyle({{
                        opacity: visible ? (isSelected ? 1.0 : 0.95) : 0.0,
                        weight: visible ? (isSelected ? 10 : 5) : 0
                    }});

                    if (isSelected && layer.bringToFront) {{
                        layer.bringToFront();
                    }}

                    if (!visible && layer.closeTooltip) {{
                        layer.closeTooltip();
                    }}
                }});
            }}

            function normalizeStreetText(text) {{
                return String(text || '')
                    .toUpperCase()
                    .replace(/,/g, ' ')
                    .replace(/\\s+/g, ' ')
                    .trim();
            }}

            function parseAddressQuery(query) {{
                var cleaned = normalizeStreetText(query)
                    .replace(/\bCHICAGO\b/g, ' ')
                    .replace(/\bIL\b/g, ' ')
                    .replace(/\\s+/g, ' ')
                    .trim();

                var match = cleaned.match(/^(\\d+)\\s+(.+)$/);
                if (!match) {{
                    return null;
                }}

                return {{
                    number: parseInt(match[1], 10),
                    street: match[2],
                }};
            }}

            function getLayerStreet(props) {{
                return normalizeStreetText(
                    (props.street_direction || '') + ' ' +
                    (props.street_name || '') + ' ' +
                    (props.street_type || '')
                );
            }}

            function formatResult(props) {{
                return 'Zone ' + props.zone + ' | ' + props.low + '-' + props.high + ' ' +
                    props.street_direction + ' ' + props.street_name + ' ' + props.street_type +
                    ' | Status: ' + props.status + ' | Buffer: ' + props.buffer;
            }}

            function zoomToResult(result, geoJsonLayer) {{
                if (!result || !result.bounds || !geoJsonLayer || !geoJsonLayer._map) {{
                    return;
                }}

                var sw = L.latLng(result.bounds[0][0], result.bounds[0][1]);
                var ne = L.latLng(result.bounds[1][0], result.bounds[1][1]);
                var bounds = L.latLngBounds(sw, ne);
                geoJsonLayer._map.fitBounds(bounds.pad(0.8));
            }}

            function renderSearchResults(results, geoJsonLayer) {{
                var summary = document.getElementById('addressSearchSummary');
                var resultsContainer = document.getElementById('addressSearchResults');
                resultsContainer.innerHTML = '';

                if (!searchActive) {{
                    summary.textContent = 'No search applied.';
                    return;
                }}

                summary.textContent = 'Matches: ' + results.length + ' row(s), ' + matchedZones.size + ' zone(s).';

                if (results.length === 0) {{
                    var emptyItem = document.createElement('div');
                    emptyItem.textContent = 'No matching address rows found.';
                    emptyItem.style.color = '#6b7280';
                    resultsContainer.appendChild(emptyItem);
                    return;
                }}

                results.forEach(function(result) {{
                    var item = document.createElement('div');
                    item.textContent = formatResult(result.props);
                    item.style.padding = '6px 4px';
                    item.style.borderBottom = '1px solid #f1f5f9';
                    item.style.cursor = 'pointer';
                    item.className = 'address-result-item';
                    item.dataset.rowId = String(result.props.row_id);
                    item.addEventListener('click', function() {{
                        selectedRowId = String(result.props.row_id);
                        var allItems = resultsContainer.querySelectorAll('.address-result-item');
                        allItems.forEach(function(node) {{
                            node.style.background = 'transparent';
                            node.style.fontWeight = 'normal';
                        }});
                        item.style.background = '#e0f2fe';
                        item.style.fontWeight = 'bold';
                        applyFilters(geoJsonLayer, document.getElementById('statusFilter'), document.getElementById('bufferFilter'), document.getElementById('wardFilter'));
                        zoomToResult(result, geoJsonLayer);
                    }});
                    resultsContainer.appendChild(item);
                }});
            }}

            function runAddressSearch(geoJsonLayer, statusSelect, bufferSelect, wardSelect) {{
                var input = document.getElementById('addressSearchInput');
                var parsed = parseAddressQuery(input.value || '');
                var results = [];
                matchedZones = new Set();

                if (!parsed || Number.isNaN(parsed.number)) {{
                    searchActive = false;
                    selectedRowId = null;
                    renderSearchResults([], geoJsonLayer);
                    applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    return;
                }}

                geoJsonLayer.eachLayer(function(layer) {{
                    var props = layer.feature && layer.feature.properties ? layer.feature.properties : {{}};
                    var low = parseInt(props.low, 10);
                    var high = parseInt(props.high, 10);
                    var layerStreet = getLayerStreet(props);
                    var queryStreet = normalizeStreetText(parsed.street);

                    var numberMatch = !Number.isNaN(low) && !Number.isNaN(high) && parsed.number >= low && parsed.number <= high;
                    var streetMatch = queryStreet.indexOf(layerStreet) >= 0 || layerStreet.indexOf(queryStreet) >= 0;

                    if (numberMatch && streetMatch) {{
                        matchedZones.add(String(props.zone));
                        var bounds = layer.getBounds ? layer.getBounds() : null;
                        results.push({{
                            props: props,
                            bounds: bounds ? [
                                [bounds.getSouthWest().lat, bounds.getSouthWest().lng],
                                [bounds.getNorthEast().lat, bounds.getNorthEast().lng]
                            ] : null,
                        }});
                    }}
                }});

                searchActive = true;
                selectedRowId = null;
                renderSearchResults(results, geoJsonLayer);
                applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
            }}

            function clearAddressSearch(geoJsonLayer, statusSelect, bufferSelect, wardSelect) {{
                var input = document.getElementById('addressSearchInput');
                input.value = '';
                searchActive = false;
                matchedZones = new Set();
                selectedRowId = null;
                renderSearchResults([], geoJsonLayer);
                applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
            }}

            function initFilters() {{
                var statusSelect = document.getElementById('statusFilter');
                var bufferSelect = document.getElementById('bufferFilter');
                var wardSelect = document.getElementById('wardFilter');
                var searchInput = document.getElementById('addressSearchInput');
                var searchBtn = document.getElementById('addressSearchBtn');
                var clearBtn = document.getElementById('addressClearBtn');
                var geoJsonLayer = window[{json.dumps(geojson_var_name)}];

                if (!statusSelect || !bufferSelect || !wardSelect || !searchInput || !searchBtn || !clearBtn || !geoJsonLayer || !geoJsonLayer.eachLayer) {{
                    return false;
                }}

                setupPanelToggle('permit-search-panel', 'permitSearchToggle');
                setupPanelToggle('permit-filter-panel', 'permitFilterToggle');
                setupPanelToggle('permit-summary-panel', 'permitSummaryToggle');

                if (statusSelect.dataset.initialized !== 'true') {{
                    addOptions(statusSelect, statusOptions);
                    statusSelect.dataset.initialized = 'true';
                }}

                if (bufferSelect.dataset.initialized !== 'true') {{
                    addOptions(bufferSelect, bufferOptions);
                    bufferSelect.dataset.initialized = 'true';
                }}

                if (wardSelect.dataset.initialized !== 'true') {{
                    addOptions(wardSelect, wardOptions);
                    wardSelect.dataset.initialized = 'true';
                }}

                if (statusSelect.dataset.bound !== 'true') {{
                    statusSelect.addEventListener('change', function() {{
                        applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    }});
                    statusSelect.dataset.bound = 'true';
                }}

                if (bufferSelect.dataset.bound !== 'true') {{
                    bufferSelect.addEventListener('change', function() {{
                        applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    }});
                    bufferSelect.dataset.bound = 'true';
                }}

                if (wardSelect.dataset.bound !== 'true') {{
                    wardSelect.addEventListener('change', function() {{
                        applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    }});
                    wardSelect.dataset.bound = 'true';
                }}

                if (searchBtn.dataset.bound !== 'true') {{
                    searchBtn.addEventListener('click', function() {{
                        runAddressSearch(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    }});
                    searchBtn.dataset.bound = 'true';
                }}

                if (clearBtn.dataset.bound !== 'true') {{
                    clearBtn.addEventListener('click', function() {{
                        clearAddressSearch(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                    }});
                    clearBtn.dataset.bound = 'true';
                }}

                if (searchInput.dataset.bound !== 'true') {{
                    searchInput.addEventListener('keydown', function(event) {{
                        if (event.key === 'Enter') {{
                            event.preventDefault();
                            runAddressSearch(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                        }}
                    }});
                    searchInput.dataset.bound = 'true';
                }}

                applyFilters(geoJsonLayer, statusSelect, bufferSelect, wardSelect);
                return true;
            }}

            if (!initFilters()) {{
                var attempts = 0;
                var timer = setInterval(function() {{
                    attempts += 1;
                    if (initFilters() || attempts > 40) {{
                        clearInterval(timer);
                    }}
                }}, 250);
            }}
        }})();
    </script>
    """

    parking_map.get_root().html.add_child(Element(controls_html))
    parking_map.get_root().html.add_child(Element(controls_js))


def main() -> None:
    args = get_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"
    output_dir = project_root / "output"

    csv_path = args.csv or (data_dir / DEFAULT_CSV)
    output_path = args.output or (output_dir / DEFAULT_OUTPUT)
    cache_path = args.cache or (data_dir / DEFAULT_CACHE)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if args.max_rows is not None and args.max_rows > 0:
        df = df.head(args.max_rows)

    for col in [
        "STATUS",
        "ZONE",
        "ADDRESS RANGE - LOW",
        "ADDRESS RANGE - HIGH",
        "STREET DIRECTION",
        "STREET NAME",
        "STREET TYPE",
        "BUFFER",
        "WARD - LOW",
        "WARD - HIGH",
    ]:
        df[col] = df[col].astype(str).str.strip()

    geocode = create_rate_limited_geocode(args)

    cache = load_cache(cache_path)

    features: list[dict[str, Any]] = []
    skipped_rows = 0
    total_rows = len(df)
    processed_rows = 0
    last_reported_pct = -1
    rows_since_cache_save = 0

    if total_rows == 0:
        print("Progress: 100% (0/0)")

    for _, row in df.iterrows():
        processed_rows += 1
        pct = int((processed_rows / total_rows) * 100) if total_rows > 0 else 100
        if pct != last_reported_pct:
            print(f"Progress: {pct}% ({processed_rows}/{total_rows})", end="\r", flush=True)
            last_reported_pct = pct

        start_address = build_address(row, "ADDRESS RANGE - LOW")
        end_address = build_address(row, "ADDRESS RANGE - HIGH")

        start_coords = geocode_address(start_address, geocode, cache, args.geocoder)
        end_coords = geocode_address(end_address, geocode, cache, args.geocoder)
        rows_since_cache_save += 1

        if rows_since_cache_save >= DEFAULT_CACHE_SAVE_EVERY:
            save_cache(cache_path, cache)
            rows_since_cache_save = 0

        if start_coords is None or end_coords is None:
            skipped_rows += 1
            continue

        zone = row["ZONE"]
        color = zone_to_color(zone)

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [start_coords[1], start_coords[0]],
                    [end_coords[1], end_coords[0]],
                ],
            },
            "properties": {
                "row_id": row["ROW ID"],
                "status": row["STATUS"],
                "zone": zone,
                "odd_even": row["ODD_EVEN"],
                "street_direction": row["STREET DIRECTION"],
                "street_name": row["STREET NAME"],
                "street_type": row["STREET TYPE"],
                "low": row["ADDRESS RANGE - LOW"],
                "high": row["ADDRESS RANGE - HIGH"],
                "buffer": row["BUFFER"],
                "ward_low": normalize_ward_value(row["WARD - LOW"]),
                "ward_high": normalize_ward_value(row["WARD - HIGH"]),
                "color": color,
            },
        }

        features.append(feature)

    if total_rows > 0:
        print(f"Progress: 100% ({total_rows}/{total_rows})")

    save_cache(cache_path, cache)

    parking_map = folium.Map(location=CHICAGO_CENTER, zoom_start=11, tiles="OpenStreetMap")

    geojson_data = {
        "type": "FeatureCollection",
        "features": features,
    }

    zone_lines = folium.GeoJson(
        geojson_data,
        name="Parking Permit Segments",
        style_function=lambda feat: {
            "color": feat["properties"]["color"],
            "weight": 5,
            "opacity": 0.95,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=[
                "row_id",
                "status",
                "zone",
                "odd_even",
                "low",
                "high",
                "street_direction",
                "street_name",
                "street_type",
                "buffer",
                "ward_low",
                "ward_high",
            ],
            aliases=[
                "Row ID",
                "Status",
                "Zone",
                "Odd/Even",
                "Address Low",
                "Address High",
                "Street Direction",
                "Street Name",
                "Street Type",
                "Buffer",
                "Ward Low",
                "Ward High",
            ],
            sticky=False,
        ),
    )

    zone_lines.add_to(parking_map)

    statuses = sorted(
        {
            value
            for value in df["STATUS"].astype(str).str.strip().tolist()
            if value and value.lower() != "nan"
        }
    )
    buffers = sorted(
        {
            value
            for value in df["BUFFER"].astype(str).str.strip().tolist()
            if value and value.lower() != "nan"
        }
    )
    wards = sorted(
        {
            value
            for value in (
                [normalize_ward_value(v) for v in df["WARD - LOW"].tolist()]
                + [normalize_ward_value(v) for v in df["WARD - HIGH"].tolist()]
            )
            if value and value.lower() != "nan"
        },
        key=lambda ward: (0, int(ward)) if ward.isdigit() else (1, ward),
    )

    add_filter_controls(
        parking_map,
        zone_lines.get_name(),
        statuses,
        buffers,
        wards,
        len(features),
        skipped_rows,
    )

    folium.LayerControl(collapsed=False).add_to(parking_map)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    parking_map.save(output_path)

    print(f"Saved map: {output_path}")
    print(f"Geocode cache: {cache_path}")
    print(f"Geocoder provider: {args.geocoder}")
    print(
        "Tip: first run may be slow due to geocoding; later runs are much faster because of cache reuse."
    )


if __name__ == "__main__":
    main()
