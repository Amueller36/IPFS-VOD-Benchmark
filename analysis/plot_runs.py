#!/usr/bin/env python3
import argparse
import io
import json
import pathlib
import warnings
import zipfile

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb
import numpy as np
import pandas as pd
import seaborn as sns





SCENARIO_PALETTE = {
    "GraphSync Adaptive Scheduler": "#1b9e77",
    "Boxo Bitswap Baseline": "#377eb8",
    "GraphSync (Prefetch)": "#7570b3",
}
SCENARIO_ORDER = ["GraphSync Adaptive Scheduler", "Boxo Bitswap Baseline"]
SCENARIO_DISPLAY = {
    "GraphSync Adaptive Scheduler": "GAS",
    "Boxo Bitswap Baseline": "BBB",
}

EXPERIMENT_ORDER = ["1_heterogen", "2_homogen", "3_bandbreitenstress", "4_packetloss"]
THESIS_OUT_EXPERIMENTS = {
    "experiment_1": "1_heterogen",
    "experiment_2": "2_homogen",
    "experiment_3": "3_bandbreitenstress",
    "experiment_4": "4_packetloss",
}

PEER_PALETTE = {
    "fast": "#005EA8",
    "slow": "#ff7f0e",
    "client": "#999999",
    "unknown": "#cccccc",
}
PEER_ORDER = ["fast", "slow", "client", "unknown"]



PEER_INDIVIDUAL_PALETTE = {
    "fast-1": "#2ca02c",
    "fast-2": "#98df8a",
    "slow-1": "#d62728",
    "slow-2": "#ff7f0e",
    "slow-3": "#ffbb78",
    "client-1": "#999999",
    "unknown-1": "#cccccc",
}

UI_PEER_PALETTE = {
    "fast": ["#005EA8", "#56B4E9", "#003F73", "#89CFF0"],
    "slow": ["#ff7f0e", "#ffa64d", "#ffbf80", "#cc5f00", "#ffd1a3"],
}
UI_PEER_FALLBACK = "#8c564b"

OUTCOME_PALETTE = {
    "success": "#2ca02c",
    "timeout_before_first_byte": "#ff7f0e",
    "zero_bytes": "#9467bd",
    "error": "#d62728",
    "cancelled": "#7f7f7f",
    "missing_summary": "#bcbd22",
    "unknown": "#cccccc",
}

def _scenario_colors(labels):
    fallback = list(sns.color_palette("Set2", 8))
    fb_idx = 0
    colors = []
    for label in labels:
        if label in SCENARIO_PALETTE:
            colors.append(SCENARIO_PALETTE[label])
        else:
            colors.append(fallback[fb_idx % len(fallback)])
            fb_idx += 1
    return colors


def scenario_display_label(label: str) -> str:
    return SCENARIO_DISPLAY.get(str(label), str(label))


def peer_display_label(peer_id, peer_label):
    label = str(peer_label or "unknown").strip().lower()
    peer = str(peer_id or "").strip()
    if not peer:
        return label
    suffix = peer[-6:] if len(peer) > 6 else peer
    return f"{label} {suffix}"


def peer_display_sort_key(name):
    label = str(name or "").split(" ", 1)[0]
    order_map = {"fast": 0, "slow": 1, "client": 2, "unknown": 3}
    return (order_map.get(label, 99), str(name))


def build_local_peer_color_map(peer_rows):
    seen = {}
    for peer_id, peer_label in peer_rows:
        display = peer_display_label(peer_id, peer_label)
        label = str(peer_label or "unknown").strip().lower()
        if display:
            seen[display] = label

    color_map = {}
    fallback_colors = list(sns.color_palette("tab10", 10))
    fallback_index = 0
    for label in ["fast", "slow", "client", "unknown"]:
        names = sorted([name for name, item_label in seen.items() if item_label == label], key=peer_display_sort_key)
        palette = UI_PEER_PALETTE.get(label)
        for idx, name in enumerate(names):
            if palette:
                color_map[name] = palette[idx % len(palette)]
            elif name in PEER_INDIVIDUAL_PALETTE:
                color_map[name] = PEER_INDIVIDUAL_PALETTE[name]
            else:
                color_map[name] = fallback_colors[fallback_index % len(fallback_colors)] if fallback_colors else UI_PEER_FALLBACK
                fallback_index += 1
    for name in sorted(set(seen) - set(color_map), key=peer_display_sort_key):
        color_map[name] = fallback_colors[fallback_index % len(fallback_colors)] if fallback_colors else UI_PEER_FALLBACK
        fallback_index += 1
    return color_map


def _peer_colors(labels):
    fallback = list(sns.color_palette("tab10", 10))
    fb_idx = 0
    colors = []
    for label in labels:
        key = str(label).lower()
        if key in PEER_PALETTE:
            colors.append(PEER_PALETTE[key])
        else:
            colors.append(fallback[fb_idx % len(fallback)])
            fb_idx += 1
    return colors


def _semantic_heatmap_colors(data: pd.DataFrame) -> np.ndarray:
    values = data.to_numpy(dtype=float)
    rgb = np.ones((values.shape[0], values.shape[1], 3), dtype=float)
    for col_idx, column in enumerate(data.columns):
        base = np.array(to_rgb(PEER_PALETTE.get(str(column).lower(), UI_PEER_FALLBACK)))
        for row_idx in range(values.shape[0]):
            intensity = max(0.0, min(1.0, values[row_idx, col_idx] / 100.0))
            rgb[row_idx, col_idx, :] = (1.0 - intensity) * np.array([1.0, 1.0, 1.0]) + intensity * base
    return rgb


def _ordered_scenarios(df: pd.DataFrame, col: str = "scenario_label") -> list:
    present = set(df[col].dropna().unique())
    ordered = [s for s in SCENARIO_ORDER if s in present]
    extras = sorted(present - set(ordered))
    return ordered + extras


def experiment_key_from_source(source: str) -> str:
    parts = pathlib.PurePosixPath(str(source).replace("\\", "/")).parts
    for part in parts:
        if part in EXPERIMENT_ORDER:
            return part
    return ""


def infer_experiment_from_out_dir(out_dir: pathlib.Path) -> str:
    for part in pathlib.PurePosixPath(str(out_dir).replace("\\", "/")).parts:
        key = THESIS_OUT_EXPERIMENTS.get(str(part))
        if key:
            return key
    return ""


def filter_dataframe_by_experiment(df: pd.DataFrame, experiment_key: str) -> pd.DataFrame:
    if df.empty or not experiment_key:
        return df
    if "experiment_key" not in df.columns:
        return df.iloc[0:0].copy()
    return df[df["experiment_key"] == experiment_key].copy()


def filter_loaded_data_by_experiment(
    run_df: pd.DataFrame,
    peer_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    scheduler_df: pd.DataFrame,
    bitswap_readiness_df: pd.DataFrame,
    experiment_key: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        filter_dataframe_by_experiment(run_df, experiment_key),
        filter_dataframe_by_experiment(peer_df, experiment_key),
        filter_dataframe_by_experiment(sample_df, experiment_key),
        filter_dataframe_by_experiment(prediction_df, experiment_key),
        filter_dataframe_by_experiment(scheduler_df, experiment_key),
        filter_dataframe_by_experiment(bitswap_readiness_df, experiment_key),
    )


def filter_dataframe_excluding_runs(df: pd.DataFrame, run_ids: set[str]) -> pd.DataFrame:
    if df.empty or not run_ids or "run_id" not in df.columns:
        return df
    return df[~df["run_id"].isin(run_ids)].copy()


def filter_loaded_data_excluding_runs(
    run_df: pd.DataFrame,
    peer_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    scheduler_df: pd.DataFrame,
    bitswap_readiness_df: pd.DataFrame,
    run_ids: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        filter_dataframe_excluding_runs(run_df, run_ids),
        filter_dataframe_excluding_runs(peer_df, run_ids),
        filter_dataframe_excluding_runs(sample_df, run_ids),
        filter_dataframe_excluding_runs(prediction_df, run_ids),
        filter_dataframe_excluding_runs(scheduler_df, run_ids),
        filter_dataframe_excluding_runs(bitswap_readiness_df, run_ids),
    )


def warn_peer_label_mismatches(run_df: pd.DataFrame, peer_df: pd.DataFrame) -> set[str]:
    if run_df.empty or peer_df.empty or "peer_network_json" not in run_df.columns:
        return set()
    required_peer_cols = {"run_id", "peer_label"}
    if not required_peer_cols.issubset(peer_df.columns):
        return set()

    labels_by_run = (
        peer_df.dropna(subset=["run_id", "peer_label"])
        .assign(peer_label=lambda df: df["peer_label"].astype(str).str.strip())
        .groupby("run_id")["peer_label"]
        .apply(lambda values: {value for value in values if value})
        .to_dict()
    )

    mismatched_run_ids = set()
    for _, run in run_df.iterrows():
        peer_network_blob = run.get("peer_network_json")
        if not peer_network_blob:
            continue
        try:
            peer_network = json.loads(peer_network_blob)
        except (TypeError, json.JSONDecodeError):
            continue
        configured_labels = {str(item.get("label") or "").strip() for item in peer_network if isinstance(item, dict)}
        configured_labels.discard("")
        if not configured_labels:
            continue

        observed_labels = labels_by_run.get(run.get("run_id"), set())
        unexpected_labels = sorted(observed_labels - configured_labels)
        if unexpected_labels:
            mismatched_run_ids.add(str(run.get("run_id")))
            source = run.get("source") or run.get("run_id")
            warnings.warn(
                "Run provider labels do not match params.peerNetwork in "
                f"{source}: unexpected labels {', '.join(unexpected_labels)}; "
                f"configured labels {', '.join(sorted(configured_labels))}; "
                "excluding this run from plot inputs",
                RuntimeWarning,
                stacklevel=2,
            )
    return mismatched_run_ids






THESIS_AXIS_LABEL_SIZE = 20
THESIS_TICK_LABEL_SIZE = 15
THESIS_LEGEND_SIZE = 16
THESIS_LEGEND_TITLE_SIZE = 17
THESIS_SUBPLOT_TITLE_SIZE = 20
THESIS_FIGURE_TITLE_SIZE = 24
THESIS_ANNOTATION_SIZE = 15
THESIS_PEER_TITLE_SIZE = 22
THESIS_PEER_PCT_SIZE = 16
THESIS_PEER_LEGEND_SIZE = 24
THESIS_PEER_LEGEND_TITLE_SIZE = 26
EMA_ACCURACY_AXIS_LIMIT_MBIT = 120
PEER_UTILIZATION_TITLE_SIZE = 30
PEER_UTILIZATION_SUBTITLE_SIZE = 28
PEER_UTILIZATION_PCT_SIZE = 22


def setup_style(context: str = "paper") -> None:
    sns.set_theme(style="whitegrid", context=context)
    try:
        import scienceplots

        plt.style.use(["science", "no-latex", "seaborn-v0_8-whitegrid"])
    except Exception:
        plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "axes.labelsize": THESIS_AXIS_LABEL_SIZE,
            "axes.titlesize": THESIS_SUBPLOT_TITLE_SIZE,
            "xtick.labelsize": THESIS_TICK_LABEL_SIZE,
            "ytick.labelsize": THESIS_TICK_LABEL_SIZE,
            "legend.fontsize": THESIS_LEGEND_SIZE,
            "legend.title_fontsize": THESIS_LEGEND_TITLE_SIZE,
            "figure.titlesize": THESIS_FIGURE_TITLE_SIZE,
        }
    )






def iter_json_payloads(input_path: pathlib.Path):
    if input_path.is_dir():
        for file_path in sorted(input_path.rglob("*.json")):
            yield file_path.as_posix(), file_path.read_text(encoding="utf-8")
        return
    if input_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(input_path, "r") as zf:
            for name in sorted(zf.namelist()):
                if not name.lower().endswith(".json"):
                    continue
                with zf.open(name, "r") as f:
                    yield name, io.TextIOWrapper(f, encoding="utf-8").read()
        return
    raise ValueError("--input must be a directory with JSON files or a .zip file")


def parse_json_event_lines(output_blob: str):
    events = []
    for raw_line in output_blob.splitlines():
        line = raw_line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        events.append(item)
    return events


def provider_label_from_host(host: str) -> str:
    value = (host or "").lower()
    if "fast" in value:
        return "fast"
    if "slow" in value:
        return "slow"
    if "client" in value:
        return "client"
    return "unknown"


def is_client_provider(host_or_peer: str, label: str = "", client_host: str = "", client_peer: str = "") -> bool:
    value = str(host_or_peer or "").lower()
    label_value = str(label or "").lower()
    return (
        label_value == "client"
        or "client" in value
        or (bool(client_host) and str(host_or_peer or "") == str(client_host))
        or (bool(client_peer) and str(host_or_peer or "") == str(client_peer))
    )


def scenario_label_from_preset(preset: str, mode: str) -> str:
    value = (preset or "").strip()
    mode_value = (mode or "").strip().lower()
    if value == "graphsync-adaptive":
        return "GraphSync Adaptive Scheduler"
    if value == "graphsync-prefetch":
        return "GraphSync (Prefetch)"
    if value == "boxo-bitswap-trace":
        return "Boxo Bitswap Baseline"
    if mode_value == "graphsync":
        return "GraphSync Adaptive Scheduler"
    if mode_value == "bitswap":
        return "Boxo Bitswap Baseline"
    if value and value != "custom":
        return value.replace("-", " ").title()
    return f"Custom ({mode or 'unknown'})"


def derive_outcome(state: str, summary: dict, output_blob: str) -> str:
    state_value = (state or "").strip().lower()
    output_value = (output_blob or "").lower()

    if state_value == "cancelled":
        return "cancelled"
    if "playback session elapsed before first byte" in output_value:
        return "timeout_before_first_byte"
    if state_value == "error":
        return "error"

    total_bytes = numeric_or_none((summary or {}).get("totalBytes"))
    if total_bytes is None:
        if state_value == "done":
            return "missing_summary"
        return state_value or "unknown"
    if total_bytes <= 0:
        if state_value == "done":
            return "zero_bytes"
        return state_value or "zero_bytes"
    if state_value in {"", "done"}:
        return "success"
    return state_value


def is_success_outcome(outcome: str) -> bool:
    return (outcome or "").strip().lower() == "success"


def numeric_or_none(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def nanos_to_seconds(value):
    num = numeric_or_none(value)
    if num is None:
        return None
    return num / 1e9


def bytes_to_mib(value):
    num = numeric_or_none(value)
    if num is None:
        return None
    return num / (1024 * 1024)


def rate_to_mib_per_s(value):
    num = numeric_or_none(value)
    if num is None:
        return None
    return num / (1024 * 1024)


def rate_to_mbit_per_s(value):
    num = numeric_or_none(value)
    if num is None:
        return None
    return num * 8 / 1_000_000


def parse_peer_network(params: dict) -> list:
    peer_network = params.get("peerNetwork")
    if not isinstance(peer_network, list):
        return []
    rows = []
    for item in peer_network:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        slot = str(item.get("slot") or "").strip()
        if not label or not slot:
            continue
        rows.append(
            {
                "slot": slot,
                "label": label,
                "rate_mbit": numeric_or_none(item.get("rateMbit")),
                "latency_ms": numeric_or_none(item.get("latencyMs")),
                "jitter_ms": numeric_or_none(item.get("jitterMs")),
                "loss_pct": numeric_or_none(item.get("lossPct")),
            }
        )
    return rows


def peer_network_json(peer_network: list) -> str:
    if not peer_network:
        return ""
    return json.dumps(peer_network, separators=(",", ":"))


def peer_network_by_unique_label(peer_network: list) -> dict:
    labels = {}
    duplicates = set()
    for item in peer_network:
        label = item.get("label")
        if not label:
            continue
        if label in labels:
            duplicates.add(label)
        labels[label] = item
    for label in duplicates:
        labels.pop(label, None)
    return labels


def configured_peer_fields(peer_label: str, peer_network_labels: dict) -> dict:
    item = peer_network_labels.get(str(peer_label or "").strip())
    if not item:
        return {
            "configured_slot": None,
            "configured_rate_mbit": None,
            "configured_latency_ms": None,
            "configured_jitter_ms": None,
            "configured_loss_pct": None,
        }
    return {
        "configured_slot": item.get("slot"),
        "configured_rate_mbit": item.get("rate_mbit"),
        "configured_latency_ms": item.get("latency_ms"),
        "configured_jitter_ms": item.get("jitter_ms"),
        "configured_loss_pct": item.get("loss_pct"),
    }


def seconds_list_to_csv_cell(value):
    if not isinstance(value, list):
        return ""
    numbers = [numeric_or_none(item) for item in value]
    return ";".join(f"{item:.6f}" for item in numbers if item is not None)


def load_runs(input_path: pathlib.Path):
    run_rows = []
    peer_rows = []
    sample_rows = []
    prediction_rows = []
    scheduler_rows = []
    bitswap_readiness_rows = []

    for source, payload in iter_json_payloads(input_path):
        obj = json.loads(payload)
        mode = obj.get("mode", "")
        run_id = obj.get("id") or pathlib.Path(source).stem
        experiment_key = experiment_key_from_source(source)
        summary = obj.get("summary")
        if not isinstance(summary, dict):
            summary = {}
        params = obj.get("params") or {}
        scenario_preset = str(params.get("scenarioPreset") or "custom")
        if scenario_preset == "kubo-ipfs-bitswap-baseline":
            continue
        scenario_label = scenario_label_from_preset(scenario_preset, mode)
        network_profile = str(params.get("networkProfile") or "default")
        peer_network = parse_peer_network(params)
        peer_network_labels = peer_network_by_unique_label(peer_network)
        peer_network_blob = peer_network_json(peer_network)
        state = str(obj.get("state") or "")
        error = str(obj.get("error") or "")
        output_blob = obj.get("output") or ""
        outcome = str(obj.get("outcome") or derive_outcome(state, summary, output_blob))
        outcome_reason = str(obj.get("outcomeReason") or "")
        events = parse_json_event_lines(output_blob)

        total_time_ns = summary.get("totalTime")
        ttfb_ns = summary.get("ttfb")
        playback_duration_s = nanos_to_seconds(summary.get("playbackDuration"))
        playback_completion_s = nanos_to_seconds(summary.get("playbackCompletion"))
        playback_extra_time_s = None
        if playback_duration_s is not None and playback_completion_s is not None:
            playback_extra_time_s = max(0.0, playback_completion_s - playback_duration_s)


        bitswap_client_data_recv = None
        bitswap_client_dup_data_recv = None
        bitswap_client_overhead_pct = None
        bitswap_client_measurement_scope = None
        for event in events:
            if event.get("type") == "bitswap_overhead_stats" and event.get("protocol") == "bitswap":
                bitswap_client_data_recv = numeric_or_none(event.get("dataReceivedBytes"))
                bitswap_client_dup_data_recv = numeric_or_none(event.get("duplicateDataReceivedBytes"))
                bitswap_client_overhead_pct = numeric_or_none(event.get("overheadPct"))
                bitswap_client_measurement_scope = event.get("measurementScope")
                break

        bitswap_raw = obj.get("bitswap") or {}
        bitswap_hosts = (bitswap_raw.get("delta") or {}).get("hosts") or {}
        if bitswap_client_data_recv is None or bitswap_client_dup_data_recv is None:
            for host_name, host_data in bitswap_hosts.items():
                if "client" in host_name.lower():
                    stat = host_data.get("stat") or {}
                    bitswap_client_data_recv = numeric_or_none(stat.get("dataRecv"))
                    bitswap_client_dup_data_recv = numeric_or_none(stat.get("dupDataRecv"))
                    break

        run_rows.append(
            {
                "run_id": run_id,
                "source": source,
                "experiment_key": experiment_key,
                "mode": mode,
                "scenario_preset": scenario_preset,
                "scenario_label": scenario_label,
                "network_profile": network_profile,
                "peer_network_json": peer_network_blob,
                "root": obj.get("root"),
                "started_at": obj.get("startedAt"),
                "ended_at": obj.get("endedAt"),
                "state": state,
                "error": error,
                "outcome": outcome,
                "outcome_reason": outcome_reason,
                "successful": 1.0 if is_success_outcome(outcome) else 0.0,
                "stall_count": numeric_or_none(summary.get("stallCount")),
                "cache_hit_rate": numeric_or_none(summary.get("cacheHitRate")),
                "avg_segment_fetch_s": nanos_to_seconds(summary.get("avgSegmentFetch")),
                "total_time_s": nanos_to_seconds(total_time_ns),
                "ttfb_s": nanos_to_seconds(ttfb_ns),
                "total_mib": bytes_to_mib(summary.get("totalBytes")),
                "throughput_mib_s": rate_to_mib_per_s(summary.get("throughputBytesPerSec")),
                "startup_delay_s": nanos_to_seconds(summary.get("startupDelay")),
                "stall_durations_s": seconds_list_to_csv_cell(summary.get("stallDurationsSec")),
                "stall_time_s": nanos_to_seconds(summary.get("totalStallTime")),
                "stall_ratio": numeric_or_none(summary.get("stallRatio")),
                "playback_overhead_ratio": numeric_or_none(summary.get("playbackOverheadRatio")),
                "deadline_miss_rate": numeric_or_none(summary.get("deadlineMissRate")),
                "ready_p50_s": nanos_to_seconds(summary.get("segmentReadyP50")),
                "ready_p95_s": nanos_to_seconds(summary.get("segmentReadyP95")),
                "segment_lateness_p50_s": nanos_to_seconds(summary.get("segmentLatenessP50")),
                "segment_lateness_p95_s": nanos_to_seconds(summary.get("segmentLatenessP95")),
                "playback_duration_s": playback_duration_s,
                "playback_completion_s": playback_completion_s,
                "playback_extra_time_s": playback_extra_time_s,
                "prefetch": params.get("prefetch"),
                "workers": params.get("workers"),
                "race_fanout": params.get("raceFanout"),
                "playback_ms": params.get("playbackMs"),
                "urgent_window": params.get("urgentWindow"),
                "ema_alpha": params.get("emaAlpha"),
                "use_dht": params.get("useDht"),
                "bitswap_data_recv": bitswap_client_data_recv,
                "bitswap_dup_data_recv": bitswap_client_dup_data_recv,
                "bitswap_overhead_pct": bitswap_client_overhead_pct,
                "bitswap_measurement_scope": bitswap_client_measurement_scope,
            }
        )

        for peer in obj.get("peers") or []:
            bytes_value = numeric_or_none(peer.get("bytes"))
            peer_label = peer.get("label") or "unknown"
            peer_rows.append(
                {
                    "run_id": run_id,
                    "experiment_key": experiment_key,
                    "mode": mode,
                    "scenario_preset": scenario_preset,
                    "scenario_label": scenario_label,
                    "peer_id": peer.get("peerId"),
                    "peer_label": peer_label,
                    "segments": numeric_or_none(peer.get("segments")),
                    "bytes": bytes_value,
                    "bytes_mib": bytes_to_mib(bytes_value),
                    "avg_net_ms": numeric_or_none(peer.get("avgNetMs")),
                    "ema": numeric_or_none(peer.get("ema")),
                    "failures": numeric_or_none(peer.get("failures")),
                    "source": "graphsync",
                    "provider_host": None,
                    "share_pct": None,
                    **configured_peer_fields(peer_label, peer_network_labels),
                }
            )

        bitswap = obj.get("bitswap") or {}
        bitswap_delta = bitswap.get("delta") or {}
        has_bitswap_ledger_provider_rows = False
        for provider in bitswap_delta.get("providerToClient") or []:
            bytes_sent = numeric_or_none(provider.get("bytesSentToClient"))
            provider_host = provider.get("host") or ""
            provider_label = provider.get("label") or provider_label_from_host(provider_host)
            provider_peer = provider.get("peerId") or provider_host
            if is_client_provider(provider_host or provider_peer, provider_label):
                continue
            has_bitswap_ledger_provider_rows = True
            peer_rows.append(
                {
                    "run_id": run_id,
                    "experiment_key": experiment_key,
                    "mode": mode,
                    "scenario_preset": scenario_preset,
                    "scenario_label": scenario_label,
                    "peer_id": provider_peer,
                    "peer_label": provider_label,
                    "segments": None,
                    "bytes": bytes_sent,
                    "bytes_mib": bytes_to_mib(bytes_sent),
                    "avg_net_ms": None,
                    "ema": None,
                    "failures": None,
                    "source": "bitswap-ledger",
                    "provider_host": provider_host,
                    "share_pct": numeric_or_none(provider.get("sharePct")),
                    **configured_peer_fields(provider_label, peer_network_labels),
                }
            )

        time_points = [numeric_or_none(event.get("timeNs")) for event in events]
        time_points = [time_ns for time_ns in time_points if time_ns is not None]
        min_ts = min(time_points) if time_points else None
        bitswap_trace_bytes = {}
        for event in events:
            event_type = str(event.get("type") or "")
            if not event_type:
                continue
            time_ns = numeric_or_none(event.get("timeNs"))
            rel_time_s = (time_ns - min_ts) / 1e9 if (time_ns is not None and min_ts is not None) else None

            if event_type == "throughput_sample":
                duration_ns = numeric_or_none(event.get("durationNs"))
                if duration_ns is None:
                    duration_ns = numeric_or_none(event.get("duration"))
                peer_label = event.get("peerLabel")
                sample_rows.append(
                    {
                        "run_id": run_id,
                        "experiment_key": experiment_key,
                        "mode": mode,
                        "scenario_preset": scenario_preset,
                        "scenario_label": scenario_label,
                        "peer": event.get("peer"),
                        "ema_mbit_s": rate_to_mbit_per_s(event.get("ema")),
                        "bytes": numeric_or_none(event.get("bytes")),
                        "duration_ms": duration_ns / 1e6 if duration_ns is not None else None,
                        "time_s": rel_time_s,
                        **configured_peer_fields(peer_label, peer_network_labels),
                    }
                )
                continue

            if event_type == "duration_prediction_sample":
                predicted_duration_ns = numeric_or_none(event.get("predictedDurationNs"))
                actual_duration_ns = numeric_or_none(event.get("actualDurationNs"))
                predicted_throughput = numeric_or_none(event.get("predictedThroughput"))
                actual_throughput = numeric_or_none(event.get("actualThroughput"))
                success_value = event.get("success")
                success_numeric = None
                if success_value is not None:
                    success_numeric = 1.0 if bool(success_value) else 0.0
                rel_error = None
                if predicted_duration_ns is not None and actual_duration_ns is not None and actual_duration_ns > 0:
                    rel_error = abs(predicted_duration_ns - actual_duration_ns) / actual_duration_ns
                inflight = numeric_or_none(event.get("inflightAtStart"))
                inflight_bucket = None
                if inflight is not None:
                    if inflight <= 0:
                        inflight_bucket = "0"
                    elif inflight == 1:
                        inflight_bucket = "1"
                    else:
                        inflight_bucket = "2+"
                prediction_rows.append(
                    {
                        "run_id": run_id,
                        "experiment_key": experiment_key,
                        "mode": mode,
                        "scenario_preset": scenario_preset,
                        "scenario_label": scenario_label,
                        "segment": numeric_or_none(event.get("segment")),
                        "playback_index": numeric_or_none(event.get("playbackIndex")),
                        "distance": numeric_or_none(event.get("distance")),
                        "urgent": 1.0 if bool(event.get("urgent")) else 0.0,
                        "method": event.get("method"),
                        "request_kind": event.get("requestKind"),
                        "peer": event.get("peer"),
                        "peer_label": event.get("peerLabel"),
                        "predicted_duration_ms": predicted_duration_ns / 1e6 if predicted_duration_ns is not None else None,
                        "actual_duration_ms": actual_duration_ns / 1e6 if actual_duration_ns is not None else None,
                        "relative_duration_error": rel_error,
                        "predicted_throughput_mbit_s": rate_to_mbit_per_s(predicted_throughput),
                        "actual_throughput_mbit_s": rate_to_mbit_per_s(actual_throughput),
                        "inflight_at_start": inflight,
                        "inflight_bucket": inflight_bucket,
                        "bytes": numeric_or_none(event.get("bytes")),
                        "success": success_numeric,
                        "outcome": event.get("outcome"),
                        "error": event.get("error"),
                        "time_s": rel_time_s,
                        **configured_peer_fields(event.get("peerLabel"), peer_network_labels),
                    }
                )
                continue

            if event_type == "bitswap_block_received" and event.get("protocol") == "bitswap":
                provider_host = event.get("providerHost") or ""
                provider_peer = event.get("peerId") or provider_host
                provider_label = event.get("providerLabel") or provider_label_from_host(provider_host or provider_peer)
                if not is_client_provider(provider_host or provider_peer, provider_label):
                    bytes_value = numeric_or_none(event.get("bytes")) or 0
                    current = bitswap_trace_bytes.get(provider_peer)
                    if current is None:
                        current = {
                            "peer_id": provider_peer,
                            "peer_label": provider_label,
                            "provider_host": provider_host,
                            "bytes": 0.0,
                        }
                    current["bytes"] += bytes_value
                    bitswap_trace_bytes[provider_peer] = current
                continue

            if event_type == "bitswap_virtual_segment_ready" and event.get("protocol") == "bitswap":
                provider_breakdown = event.get("providerBreakdown")
                provider_breakdown_json = ""
                if isinstance(provider_breakdown, dict):
                    provider_breakdown_json = json.dumps(provider_breakdown, separators=(",", ":"))
                bitswap_readiness_rows.append(
                    {
                        "run_id": run_id,
                        "experiment_key": experiment_key,
                        "mode": mode,
                        "scenario_preset": scenario_preset,
                        "scenario_label": scenario_label,
                        "event_type": event_type,
                        "segment": numeric_or_none(event.get("segment")),
                        "ready_time_s": nanos_to_seconds(event.get("readyTimeNs")),
                        "bytes_ready": numeric_or_none(event.get("bytesReady")),
                        "segment_bytes": numeric_or_none(event.get("segmentBytes")),
                        "provider_breakdown_json": provider_breakdown_json,
                        "time_s": rel_time_s,
                    }
                )
                continue

            if not event_type.startswith("scheduler_"):
                continue

            selected_peer = event.get("peer") or event.get("selectedPeer") or event.get("winnerPeer")
            selected_label = event.get("peerLabel") or event.get("selectedLabel") or event.get("winnerPeerLabel")
            estimate_ns = event.get("estimateNs")
            if estimate_ns is None:
                estimate_ns = event.get("selectedEstimateNs")
            duration_ns = numeric_or_none(event.get("durationNs"))
            success_value = event.get("success")
            success_numeric = None
            if success_value is not None:
                success_numeric = 1.0 if bool(success_value) else 0.0
            candidates = event.get("candidates")
            candidates_json = ""
            if isinstance(candidates, list):
                candidates_json = json.dumps(candidates, separators=(",", ":"))

            urgent_value = event.get("urgent")
            urgent_numeric = None
            if urgent_value is not None:
                urgent_numeric = 1.0 if bool(urgent_value) else 0.0

            scheduler_rows.append(
                {
                    "run_id": run_id,
                    "experiment_key": experiment_key,
                    "mode": mode,
                    "scenario_preset": scenario_preset,
                    "scenario_label": scenario_label,
                    "event_type": event_type,
                    "segment": numeric_or_none(event.get("segment")),
                    "playback_index": numeric_or_none(event.get("playbackIndex")),
                    "distance": numeric_or_none(event.get("distance")),
                    "urgent": urgent_numeric,
                    "method": event.get("method"),
                    "reason": event.get("reason"),
                    "peer": selected_peer,
                    "peer_label": selected_label,
                    "estimate_s": nanos_to_seconds(estimate_ns),
                    "deadline_s": nanos_to_seconds(event.get("deadlineNs")),
                    "duration_ms": duration_ns / 1e6 if duration_ns is not None else None,
                    "bytes": numeric_or_none(event.get("bytes")),
                    "success": success_numeric,
                    "error": event.get("error"),
                    "candidate_count": numeric_or_none(event.get("candidateCount")),
                    "race_fanout": numeric_or_none(event.get("raceFanout")),
                    "peer_count": numeric_or_none(event.get("peerCount")),
                    "urgent_window": numeric_or_none(event.get("urgentWindow")),
                    "playback_delay_s": nanos_to_seconds(event.get("playbackDelayNs")),
                    "run_urgent_window": numeric_or_none(params.get("urgentWindow")),
                    "race_useful_bytes": numeric_or_none(event.get("raceUsefulBytes")),
                    "race_wasted_bytes": numeric_or_none(event.get("raceWastedBytes")),
                    "race_total_bytes": numeric_or_none(event.get("raceTotalBytes")),
                    "race_started_candidates": numeric_or_none(event.get("raceStartedCandidates")),
                    "race_completed_candidates": numeric_or_none(event.get("raceCompletedCandidates")),
                    "race_cancelled_candidates": numeric_or_none(event.get("raceCancelledCandidates")),
                    "race_successful_candidates": numeric_or_none(event.get("raceSuccessfulCandidates")),
                    "race_metrics_complete": (
                        None
                        if event.get("raceMetricsComplete") is None
                        else (1.0 if bool(event.get("raceMetricsComplete")) else 0.0)
                    ),
                    "time_s": rel_time_s,
                    "candidates_json": candidates_json,
                    **configured_peer_fields(selected_label, peer_network_labels),
                }
            )

        if mode == "bitswap" and not has_bitswap_ledger_provider_rows:
            for provider in bitswap_trace_bytes.values():
                bytes_value = numeric_or_none(provider.get("bytes"))
                if bytes_value is None or bytes_value <= 0:
                    continue
                peer_rows.append(
                    {
                        "run_id": run_id,
                        "experiment_key": experiment_key,
                        "mode": mode,
                        "scenario_preset": scenario_preset,
                        "scenario_label": scenario_label,
                        "peer_id": provider.get("peer_id"),
                        "peer_label": provider.get("peer_label") or "unknown",
                        "segments": None,
                        "bytes": bytes_value,
                        "bytes_mib": bytes_to_mib(bytes_value),
                        "avg_net_ms": None,
                        "ema": None,
                        "failures": None,
                        "source": "bitswap-trace",
                        "provider_host": provider.get("provider_host"),
                        "share_pct": None,
                        **configured_peer_fields(provider.get("peer_label") or "unknown", peer_network_labels),
                    }
                )

    run_df = pd.DataFrame(run_rows)
    peer_df = pd.DataFrame(peer_rows)
    sample_df = pd.DataFrame(sample_rows)
    prediction_df = pd.DataFrame(prediction_rows)
    scheduler_df = pd.DataFrame(scheduler_rows)
    bitswap_readiness_df = pd.DataFrame(bitswap_readiness_rows)

    run_numeric_cols = [
        "stall_count",
        "cache_hit_rate",
        "avg_segment_fetch_s",
        "total_time_s",
        "ttfb_s",
        "total_mib",
        "throughput_mib_s",
        "startup_delay_s",
        "stall_time_s",
        "stall_ratio",
        "playback_overhead_ratio",
        "deadline_miss_rate",
        "ready_p50_s",
        "ready_p95_s",
        "segment_lateness_p50_s",
        "segment_lateness_p95_s",
        "playback_duration_s",
        "playback_completion_s",
        "playback_extra_time_s",
        "prefetch",
        "workers",
        "race_fanout",
        "playback_ms",
        "urgent_window",
        "ema_alpha",
        "successful",
        "bitswap_data_recv",
        "bitswap_dup_data_recv",
        "bitswap_overhead_pct",
    ]
    for col in run_numeric_cols:
        if col in run_df.columns:
            run_df[col] = pd.to_numeric(run_df[col], errors="coerce")

    configured_numeric_cols = ["configured_rate_mbit", "configured_latency_ms", "configured_jitter_ms", "configured_loss_pct"]
    peer_numeric_cols = ["segments", "bytes", "bytes_mib", "avg_net_ms", "ema", "failures", "share_pct", *configured_numeric_cols]
    for col in peer_numeric_cols:
        if col in peer_df.columns:
            peer_df[col] = pd.to_numeric(peer_df[col], errors="coerce")

    sample_numeric_cols = ["ema_mbit_s", "bytes", "duration_ms", "time_s", *configured_numeric_cols]
    for col in sample_numeric_cols:
        if col in sample_df.columns:
            sample_df[col] = pd.to_numeric(sample_df[col], errors="coerce")

    prediction_numeric_cols = [
        "segment",
        "playback_index",
        "distance",
        "urgent",
        "predicted_duration_ms",
        "actual_duration_ms",
        "relative_duration_error",
        "predicted_throughput_mbit_s",
        "actual_throughput_mbit_s",
        "inflight_at_start",
        "bytes",
        "success",
        "time_s",
        *configured_numeric_cols,
    ]
    for col in prediction_numeric_cols:
        if col in prediction_df.columns:
            prediction_df[col] = pd.to_numeric(prediction_df[col], errors="coerce")

    scheduler_numeric_cols = [
        "segment",
        "playback_index",
        "distance",
        "urgent",
        "estimate_s",
        "deadline_s",
        "duration_ms",
        "bytes",
        "success",
        "candidate_count",
        "race_fanout",
        "peer_count",
        "urgent_window",
        "playback_delay_s",
        "run_urgent_window",
        "race_useful_bytes",
        "race_wasted_bytes",
        "race_total_bytes",
        "race_started_candidates",
        "race_completed_candidates",
        "race_cancelled_candidates",
        "race_successful_candidates",
        "race_metrics_complete",
        "time_s",
        *configured_numeric_cols,
    ]
    for col in scheduler_numeric_cols:
        if col in scheduler_df.columns:
            scheduler_df[col] = pd.to_numeric(scheduler_df[col], errors="coerce")

    bitswap_readiness_numeric_cols = [
        "segment",
        "ready_time_s",
        "bytes_ready",
        "segment_bytes",
        "time_s",
    ]
    for col in bitswap_readiness_numeric_cols:
        if col in bitswap_readiness_df.columns:
            bitswap_readiness_df[col] = pd.to_numeric(bitswap_readiness_df[col], errors="coerce")

    return run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df






def save_svg(fig, path: pathlib.Path, *, tight_layout: bool = True):
    apply_thesis_typography(fig)
    if tight_layout:
        fig.tight_layout()
    fig.savefig(path, format="svg", bbox_inches="tight")
    plt.close(fig)


def apply_thesis_typography(fig) -> None:
    for ax in fig.axes:
        ax.title.set_fontsize(getattr(ax.title, "_thesis_fontsize_override", THESIS_SUBPLOT_TITLE_SIZE))
        ax.xaxis.label.set_fontsize(getattr(ax.xaxis.label, "_thesis_fontsize_override", THESIS_AXIS_LABEL_SIZE))
        ax.yaxis.label.set_fontsize(getattr(ax.yaxis.label, "_thesis_fontsize_override", THESIS_AXIS_LABEL_SIZE))
        tick_label_size = getattr(ax, "_thesis_tick_label_size_override", THESIS_TICK_LABEL_SIZE)
        ax.tick_params(axis="both", which="major", labelsize=tick_label_size)
        ax.tick_params(axis="both", which="minor", labelsize=tick_label_size)
        legend = ax.get_legend()
        if legend is not None:
            for text in legend.get_texts():
                text.set_fontsize(max(float(text.get_fontsize()), THESIS_LEGEND_SIZE))
            title = legend.get_title()
            if title is not None:
                title.set_fontsize(max(float(title.get_fontsize()), THESIS_LEGEND_TITLE_SIZE))
    for legend in fig.legends:
        for text in legend.get_texts():
            text.set_fontsize(max(float(text.get_fontsize()), THESIS_LEGEND_SIZE))
        title = legend.get_title()
        if title is not None:
            title.set_fontsize(max(float(title.get_fontsize()), THESIS_LEGEND_TITLE_SIZE))


def cleaned_xy(df: pd.DataFrame, x_col: str, y_col: str) -> pd.DataFrame:
    out = df.copy()
    out = out.dropna(subset=[x_col])
    out[x_col] = out[x_col].astype(str)
    out[y_col] = pd.to_numeric(out[y_col], errors="coerce")
    out = out[~out[y_col].isin([float("inf"), float("-inf")])]
    out = out.dropna(subset=[x_col, y_col])
    return out


def adaptive_distribution(
    ax,
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    y_label: str,
    x_label: str = "Abrufpfad",
    show_x_labels: bool = True,
    force_box: bool = False,
):
    cleaned = cleaned_xy(df, x_col, y_col)
    if cleaned.empty:
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.text(0.5, 0.5, "Keine Daten", transform=ax.transAxes, ha="center", va="center", fontsize=THESIS_ANNOTATION_SIZE)
        return

    order = [s for s in SCENARIO_ORDER if s in cleaned[x_col].unique()]
    extras = sorted(set(cleaned[x_col].unique()) - set(order))
    order = order + extras
    palette = {s: SCENARIO_PALETTE.get(s, "#888888") for s in order}

    group_sizes = cleaned.groupby(x_col)[y_col].count()
    use_bar = bool((group_sizes < 3).any()) and not force_box
    show_all_points = bool((group_sizes < 10).any())

    if use_bar:
        sns.barplot(
            data=cleaned, x=x_col, y=y_col, hue=x_col,
            order=order, palette=palette, ax=ax, errorbar=None, alpha=0.85,
            legend=False,
        )
        sns.stripplot(
            data=cleaned, x=x_col, y=y_col,
            order=order, ax=ax, color="black", alpha=0.6, size=5.5,
            legend=False,
        )
    else:
        sns.boxplot(
            data=cleaned, x=x_col, y=y_col, hue=x_col,
            order=order, palette=palette, ax=ax,
            fliersize=0 if show_all_points else 1,
            legend=False,
        )
        if show_all_points:
            sns.stripplot(
                data=cleaned, x=x_col, y=y_col,
                order=order, ax=ax, color="black", alpha=0.5, size=4.5,
                legend=False,
            )

    ax.set_title(title)
    ax.set_ylabel(y_label)
    if show_x_labels:
        ax.set_xlabel(x_label)
        xticks = ax.get_xticks()
        ax.set_xticks(xticks)
        ax.set_xticklabels(
            [scenario_display_label(label.get_text()) for label in ax.get_xticklabels()],
            rotation=0,
            ha="center",
        )
    else:
        ax.set_xlabel("")
        ax.set_xticklabels([])


def annotate_exclusions(ax, excluded_df: pd.DataFrame, group_col: str):
    if excluded_df.empty:
        return
    grouped = excluded_df.groupby(group_col)["run_id"].nunique().sort_values(ascending=False)
    if grouped.empty:
        return
    parts = [f"{scenario_display_label(name)}: {count}" for name, count in grouped.items()]
    label = "Ausgeschlossene Runs ohne Erfolg: " + ", ".join(parts[:4])
    if len(parts) > 4:
        label += ", ..."
    ax.text(0.01, 1.03, label, transform=ax.transAxes, ha="left", va="bottom", fontsize=THESIS_ANNOTATION_SIZE)


def _success_filter(run_df: pd.DataFrame):
    group_col = "scenario_label"
    base = run_df.dropna(subset=[group_col]).copy()
    if base.empty:
        return base, base
    include_mask = (pd.to_numeric(base.get("successful"), errors="coerce") > 0) & (
        pd.to_numeric(base.get("total_mib"), errors="coerce") > 0
    )
    return base[include_mask].copy(), base[~include_mask].copy()


def build_segment_lateness_df(
    run_df: pd.DataFrame,
    scheduler_df: pd.DataFrame,
    bitswap_readiness_df: pd.DataFrame,
) -> pd.DataFrame:
    success_runs, _ = _success_filter(run_df)
    required = {"run_id", "mode", "scenario_label", "startup_delay_s", "playback_ms"}
    if success_runs.empty or not required.issubset(success_runs.columns):
        return pd.DataFrame()

    run_info = success_runs[list(required)].copy()
    run_info["startup_delay_s"] = pd.to_numeric(run_info["startup_delay_s"], errors="coerce")
    run_info["playback_ms"] = pd.to_numeric(run_info["playback_ms"], errors="coerce")
    run_info = run_info.dropna(subset=["run_id", "startup_delay_s", "playback_ms"])
    run_info = run_info[run_info["playback_ms"] > 0]
    if run_info.empty:
        return pd.DataFrame()
    run_info["playback_interval_s"] = run_info["playback_ms"] / 1000.0

    frames = []

    if not scheduler_df.empty:
        gs = scheduler_df.copy()
        gs = gs[
            (gs.get("mode") == "graphsync")
            & (gs.get("event_type") == "scheduler_result")
            & (pd.to_numeric(gs.get("success"), errors="coerce") > 0)
        ]
        if not gs.empty:
            gs["segment"] = pd.to_numeric(gs["segment"], errors="coerce")
            gs["ready_time_s"] = pd.to_numeric(gs["time_s"], errors="coerce")
            gs = gs.dropna(subset=["run_id", "segment", "ready_time_s"])
            gs = (
                gs.sort_values(["run_id", "segment", "ready_time_s"])
                .groupby(["run_id", "segment"], as_index=False)
                .first()
            )
            frames.append(gs[["run_id", "segment", "ready_time_s"]])

    if not bitswap_readiness_df.empty:
        bs = bitswap_readiness_df.copy()
        bs = bs[bs.get("mode") == "bitswap"]
        if not bs.empty:
            bs["segment"] = pd.to_numeric(bs["segment"], errors="coerce")
            bs["ready_time_s"] = pd.to_numeric(bs["ready_time_s"], errors="coerce")
            bs = bs.dropna(subset=["run_id", "segment", "ready_time_s"])
            bs = (
                bs.sort_values(["run_id", "segment", "ready_time_s"])
                .groupby(["run_id", "segment"], as_index=False)
                .first()
            )
            frames.append(bs[["run_id", "segment", "ready_time_s"]])

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.merge(
        run_info[["run_id", "mode", "scenario_label", "startup_delay_s", "playback_interval_s"]],
        on="run_id",
        how="inner",
    )
    if out.empty:
        return pd.DataFrame()

    out["deadline_s"] = out["startup_delay_s"] + out["segment"] * out["playback_interval_s"]
    out["lateness_s"] = (out["ready_time_s"] - out["deadline_s"]).clip(lower=0.0)
    out["deadline_missed"] = out["ready_time_s"] > out["deadline_s"]
    return out[
        [
            "run_id",
            "mode",
            "scenario_label",
            "segment",
            "ready_time_s",
            "deadline_s",
            "lateness_s",
            "deadline_missed",
        ]
    ].copy()






def provider_share_df(peer_df: pd.DataFrame) -> pd.DataFrame:
    if peer_df.empty:
        return pd.DataFrame()
    df = peer_df.copy()
    df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce")
    df = df.dropna(subset=["run_id", "scenario_label", "peer_label", "bytes"])
    df = df[df["bytes"] > 0]
    if df.empty:
        return pd.DataFrame()
    df["peer_id"] = df["peer_id"].fillna(df["provider_host"]).fillna(df["peer_label"])
    grouped = df.groupby(["run_id", "scenario_label", "mode", "peer_id", "peer_label"], as_index=False)["bytes"].sum()
    totals = grouped.groupby(["run_id", "scenario_label", "mode"], as_index=False)["bytes"].sum().rename(columns={"bytes": "run_total"})
    merged = grouped.merge(totals, on=["run_id", "scenario_label", "mode"], how="left")
    merged["share_pct"] = 100.0 * merged["bytes"] / merged["run_total"].replace(0, pd.NA)
    merged = merged.dropna(subset=["share_pct"])
    return merged


def provider_concentration_df(peer_df: pd.DataFrame) -> pd.DataFrame:
    shares = provider_share_df(peer_df)
    if shares.empty:
        return pd.DataFrame()
    concentration = (
        shares.groupby(["run_id", "scenario_label"], as_index=False)
        .agg(
            top1_share=("share_pct", "max"),
            hhi=("share_pct", lambda x: ((x / 100.0) ** 2).sum()),
        )
        .copy()
    )
    concentration["effective_providers"] = 1.0 / concentration["hhi"].replace(0, pd.NA)
    return concentration


def provider_average_share_df(peer_df: pd.DataFrame) -> pd.DataFrame:
    if peer_df.empty:
        return pd.DataFrame()

    df = peer_df.copy()
    df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce")
    df = df.dropna(subset=["run_id", "scenario_label", "peer_label", "bytes"])
    df = df[df["bytes"] > 0]
    if df.empty:
        return pd.DataFrame()

    peer_agg = df.groupby(["run_id", "scenario_label", "peer_label", "peer_id"], as_index=False)["bytes"].sum()
    run_totals = peer_agg.groupby(["run_id", "scenario_label"], as_index=False)["bytes"].sum().rename(columns={"bytes": "run_total"})
    peer_agg = peer_agg.merge(run_totals, on=["run_id", "scenario_label"], how="left")
    peer_agg["share_pct"] = 100.0 * peer_agg["bytes"] / peer_agg["run_total"].replace(0, pd.NA)
    peer_agg = peer_agg.dropna(subset=["share_pct"])
    if peer_agg.empty:
        return pd.DataFrame()

    avg_shares = (
        peer_agg.groupby(["scenario_label", "peer_label", "peer_id"], as_index=False)["share_pct"]
        .mean()
    )
    avg_shares["peer_name"] = avg_shares.apply(
        lambda row: peer_display_label(row.get("peer_id"), row.get("peer_label")),
        axis=1,
    )
    return avg_shares






def graphsync_scheduler_results(scheduler_df: pd.DataFrame) -> pd.DataFrame:
    if scheduler_df.empty:
        return pd.DataFrame()
    df = scheduler_df.copy()
    df = df[df["mode"] == "graphsync"]
    df = df[df["event_type"] == "scheduler_result"]
    if df.empty:
        return pd.DataFrame()
    df = df[pd.to_numeric(df.get("success"), errors="coerce") > 0]
    df = df.dropna(subset=["run_id", "scenario_label", "segment", "distance", "peer", "peer_label"])
    if df.empty:
        return pd.DataFrame()
    df["peer_label"] = df["peer_label"].astype(str).str.lower()
    return df
def fig_qoe_summary(run_df: pd.DataFrame, out_dir: pathlib.Path):
    df, excluded = _success_filter(run_df)
    qoe_scenarios = {"GraphSync Adaptive Scheduler", "Boxo Bitswap Baseline"}
    df = df[df["scenario_label"].isin(qoe_scenarios)].copy()
    excluded = excluded[excluded["scenario_label"].isin(qoe_scenarios)].copy()
    if df.empty:
        return
    group_col = "scenario_label"

    metrics = [
        ("startup_delay_s", "(a) Startup delay", "Startup delay [s]"),
        ("stall_count", "(b) Playback Stall Count", "Stall-Ereignisse"),
        ("stall_time_s", "(c) Total Stall Time", "Stall-Zeit [s]"),
        ("playback_extra_time_s", "(d) Extra Playback Time", "Zusätzliche Wiedergabezeit [s]"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(11, 8.2))
    flat_axes = list(np.ravel(axes))

    for idx, (metric, title, ylabel) in enumerate(metrics):
        adaptive_distribution(
            flat_axes[idx],
            df,
            group_col,
            metric,
            title,
            ylabel,
            show_x_labels=True,
            force_box=True,
        )

    flat_axes[3].yaxis.label._thesis_fontsize_override = 12
    annotate_exclusions(flat_axes[0], excluded, group_col)

    fig.suptitle("QoE-Metriken nach Abrufpfad", fontsize=THESIS_FIGURE_TITLE_SIZE, y=1.01)
    save_svg(fig, out_dir / "qoe_summary.svg")






def fig_throughput_summary(run_df: pd.DataFrame, out_dir: pathlib.Path):
    df, excluded = _success_filter(run_df)
    scenarios = {"GraphSync Adaptive Scheduler", "Boxo Bitswap Baseline"}
    df = df[df["scenario_label"].isin(scenarios)].copy()
    excluded = excluded[excluded["scenario_label"].isin(scenarios)].copy()
    if df.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 4.0))
    adaptive_distribution(
        ax,
        df,
        "scenario_label",
        "throughput_mib_s",
        "Effektiver Durchsatz nach Abrufpfad",
        "Durchsatz [MiB/s]",
        show_x_labels=True,
        force_box=True,
    )
    annotate_exclusions(ax, excluded, "scenario_label")
    fig.subplots_adjust(left=0.12, right=0.985, top=0.87, bottom=0.27)
    save_svg(fig, out_dir / "throughput_summary.svg", tight_layout=False)






def draw_segment_lateness_distribution(ax, segment_lateness_df: pd.DataFrame):
    scenarios = set(SCENARIO_ORDER)
    df = segment_lateness_df[segment_lateness_df["scenario_label"].isin(scenarios)].copy()
    df = df.dropna(subset=["scenario_label", "lateness_s"])
    df["lateness_s"] = pd.to_numeric(df["lateness_s"], errors="coerce")
    df = df[df["lateness_s"] > 0]
    if df.empty:
        ax.set_title("(b) Tail-Bereich der Segmentverspätung")
        ax.set_xlabel("Segmentverspätung [s]")
        ax.text(0.5, 0.5, "Keine verspäteten Segmente", transform=ax.transAxes, ha="center", va="center", fontsize=THESIS_ANNOTATION_SIZE)
        return

    order = _ordered_scenarios(df)
    df = df.dropna(subset=["lateness_s"])
    if df.empty:
        ax.set_title("(b) Tail-Bereich der Segmentverspätung")
        ax.set_xlabel("Segmentverspätung [s]")
        ax.text(0.5, 0.5, "Keine verspäteten Segmente", transform=ax.transAxes, ha="center", va="center", fontsize=THESIS_ANNOTATION_SIZE)
        return

    x_max = max(float(df["lateness_s"].max()) * 1.08, 1.0)
    for y_pos, scenario in enumerate(order):
        values = df[df["scenario_label"] == scenario]["lateness_s"].dropna()
        if values.empty:
            continue
        p50 = float(values.quantile(0.50))
        p95 = float(values.quantile(0.95))
        if x_max > p95:
            ax.fill_betweenx(
                [y_pos - 0.34, y_pos + 0.34],
                p95,
                x_max,
                color="#d62728",
                alpha=0.05,
                linewidth=0,
                zorder=0,
            )

    palette = {scenario: SCENARIO_PALETTE.get(scenario, "#888888") for scenario in order}
    sns.stripplot(
        data=df,
        x="lateness_s",
        y="scenario_label",
        order=order,
        hue="scenario_label",
        palette=palette,
        ax=ax,
        jitter=0.23,
        size=1.8,
        alpha=0.18,
        legend=False,
        zorder=2,
    )

    for y_pos, scenario in enumerate(order):
        values = df[df["scenario_label"] == scenario]["lateness_s"].dropna()
        if values.empty:
            continue
        p50 = float(values.quantile(0.50))
        p95 = float(values.quantile(0.95))
        ax.vlines(p50, y_pos - 0.33, y_pos + 0.33, color="#008b8b", linestyle="--", linewidth=1.4, zorder=3)
        ax.vlines(p95, y_pos - 0.33, y_pos + 0.33, color="#b000b5", linestyle="--", linewidth=1.4, zorder=3)

    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    ax.legend(
        handles=[
            Line2D([0], [0], color="#008b8b", linestyle="--", linewidth=1.4, label="p50 verspätet"),
            Line2D([0], [0], color="#b000b5", linestyle="--", linewidth=1.4, label="p95 verspätet"),
            Patch(facecolor="#d62728", alpha=0.08, edgecolor="none", label="Tail ab p95"),
        ],
        fontsize=THESIS_LEGEND_SIZE,
        loc="upper right",
        framealpha=0.9,
    )
    ax.set_xlim(0, x_max)
    ax.set_title("(b) Tail-Bereich der Segmentverspätung")
    ax.set_xlabel("Segmentverspätung [s]")
    ax.set_ylabel("")
    yticks = ax.get_yticks()
    ax.set_yticks(yticks)
    ax.set_yticklabels([scenario_display_label(label.get_text()) for label in ax.get_yticklabels()])
    ax.grid(True, axis="x", alpha=0.25)


def fig_segment_timing_summary(run_df: pd.DataFrame, segment_lateness_df: pd.DataFrame, out_dir: pathlib.Path):
    df, excluded = _success_filter(run_df)
    scenarios = {"GraphSync Adaptive Scheduler", "Boxo Bitswap Baseline"}
    df = df[df["scenario_label"].isin(scenarios)].copy()
    excluded = excluded[excluded["scenario_label"].isin(scenarios)].copy()
    if df.empty:
        return
    df["deadline_miss_rate_pct"] = pd.to_numeric(df["deadline_miss_rate"], errors="coerce") * 100.0
    group_col = "scenario_label"

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.2), gridspec_kw={"width_ratios": [0.9, 1.6]})

    adaptive_distribution(
        axes[0],
        df,
        group_col,
        "deadline_miss_rate_pct",
        "(a) Deadline Miss Rate",
        "Verpasste Deadlines [%]",
        show_x_labels=True,
        force_box=True,
    )
    annotate_exclusions(axes[0], excluded, group_col)
    draw_segment_lateness_distribution(axes[1], segment_lateness_df)

    fig.suptitle("Segment-Timing relativ zur geplanten Deadline", fontsize=THESIS_FIGURE_TITLE_SIZE, y=1.01)
    save_svg(fig, out_dir / "segment_timing_summary.svg")




def fig_provider_load(peer_df: pd.DataFrame, out_dir: pathlib.Path):
    concentration = provider_concentration_df(peer_df)
    if concentration.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    adaptive_distribution(
        ax,
        concentration,
        "scenario_label",
        "effective_providers",
        "Effektive Provider-Anzahl nach Abrufpfad",
        "Effektive Provider (1 / HHI)",
    )
    max_y = max(5.2, pd.to_numeric(concentration["effective_providers"], errors="coerce").max() * 1.15)
    ax.set_ylim(0, max_y)
    ax.set_title("Effektive Provider-Anzahl aus Byte-Anteilen")

    save_svg(fig, out_dir / "provider_load.svg")


def draw_provider_share_pie(
    ax,
    legend_ax,
    avg_shares: pd.DataFrame,
    scenario: str,
    title: str,
    *,
    radius: float = 1.15,
    pctdistance: float = 0.72,
    pct_fontsize: int = PEER_UTILIZATION_PCT_SIZE,
    pct_fontweight: str = "bold",
    legend_fontsize: int = THESIS_PEER_LEGEND_SIZE,
    legend_title_fontsize: int = THESIS_PEER_LEGEND_TITLE_SIZE,
    title_pad: int = 7,
):
    part = avg_shares[avg_shares["scenario_label"] == scenario].copy()
    part = part[part["share_pct"] > 0]
    legend_ax.axis("off")

    if part.empty:
        ax.set_title(title, fontsize=PEER_UTILIZATION_SUBTITLE_SIZE)
        ax.text(0.5, 0.5, "Keine Daten", ha="center", va="center", transform=ax.transAxes)
        ax.axis("off")
        return

    local_names = sorted(part["peer_name"].tolist(), key=peer_display_sort_key)
    local_color_map = build_local_peer_color_map(
        part[["peer_id", "peer_label"]].drop_duplicates().itertuples(index=False, name=None)
    )

    part = part.set_index("peer_name").reindex(local_names).dropna(subset=["share_pct"])
    values = part["share_pct"].values
    labels_list = part.index.tolist()
    colors = [local_color_map.get(n, UI_PEER_FALLBACK) for n in labels_list]

    def autopct(value):
        return f"{value:.0f}%" if value >= 4 else ""

    wedges, _, _ = ax.pie(
        values, labels=None, colors=colors,
        startangle=90, counterclock=False,
        radius=radius,
        autopct=autopct,
        pctdistance=pctdistance,
        wedgeprops={"edgecolor": "white", "linewidth": 1.2},
        textprops={"fontsize": pct_fontsize, "fontweight": pct_fontweight},
    )
    ax.set_title(title, fontsize=PEER_UTILIZATION_SUBTITLE_SIZE, pad=title_pad)
    legend_labels = [f"{name} ({value:.0f}%)" for name, value in zip(labels_list, values)]
    legend_ax.legend(
        wedges,
        legend_labels,
        title="Provider",
        loc="center left",
        fontsize=legend_fontsize,
        title_fontsize=legend_title_fontsize,
        frameon=False,
        labelspacing=0.42,
        handlelength=1.1,
        borderaxespad=0.0,
    )


def fig_provider_usage(peer_df: pd.DataFrame, out_dir: pathlib.Path):
    concentration = provider_concentration_df(peer_df)
    avg_shares = provider_average_share_df(peer_df)
    scenarios = [scenario for scenario in SCENARIO_ORDER if scenario in set(avg_shares.get("scenario_label", []))]
    if concentration.empty or avg_shares.empty or not scenarios:
        return

    fig = plt.figure(figsize=(12.4, 5.45))
    grid = fig.add_gridspec(
        2,
        3,
        width_ratios=[1.25, 1.35, 0.78],
        height_ratios=[1, 1],
        wspace=0.08,
        hspace=0.42,
    )
    ax_load = fig.add_subplot(grid[:, 0])
    ax_pie_top = fig.add_subplot(grid[0, 1])
    ax_legend_top = fig.add_subplot(grid[0, 2])
    ax_pie_bottom = fig.add_subplot(grid[1, 1])
    ax_legend_bottom = fig.add_subplot(grid[1, 2])

    adaptive_distribution(
        ax_load,
        concentration,
        "scenario_label",
        "effective_providers",
        "(a) Effektive Providerzahl",
        "Effektive Provider (1 / HHI)",
    )
    ax_load.set_title("(a) Effektive Providerzahl", pad=10)
    max_y = max(5.2, pd.to_numeric(concentration["effective_providers"], errors="coerce").max() * 1.15)
    ax_load.set_ylim(0, max_y)

    pie_axes = [(ax_pie_top, ax_legend_top), (ax_pie_bottom, ax_legend_bottom)]
    for (ax, legend_ax), scenario in zip(pie_axes, scenarios):
        draw_provider_share_pie(
            ax,
            legend_ax,
            avg_shares,
            scenario,
            f"({chr(98 + scenarios.index(scenario))}) {scenario_display_label(scenario)}",
            radius=1.42,
            pctdistance=0.71,
            pct_fontsize=16,
            pct_fontweight="bold",
            legend_fontsize=THESIS_LEGEND_SIZE,
            legend_title_fontsize=THESIS_LEGEND_TITLE_SIZE,
            title_pad=10,
        )
    for ax, legend_ax in pie_axes[len(scenarios):]:
        ax.axis("off")
        legend_ax.axis("off")

    fig.suptitle("Provider-Nutzung nach Abrufpfad", fontsize=THESIS_FIGURE_TITLE_SIZE, y=0.985)
    fig.subplots_adjust(left=0.07, right=0.985, top=0.81, bottom=0.12)
    save_svg(fig, out_dir / "provider_usage.svg", tight_layout=False)









def fig_gs_scheduler_analysis(scheduler_df: pd.DataFrame, out_dir: pathlib.Path):
    df = graphsync_scheduler_results(scheduler_df)
    if df.empty:
        return


    work = df.copy()
    work["urgency_zone"] = (pd.to_numeric(work.get("urgent"), errors="coerce") > 0).map({True: "urgent", False: "future"})
    bytes_series = pd.to_numeric(work.get("bytes"), errors="coerce")
    use_bytes = bytes_series.notna().any() and bytes_series.fillna(0).sum() > 0
    work["weight"] = bytes_series.fillna(0) if use_bytes else 1.0

    grouped = work.groupby(["scenario_label", "urgency_zone", "peer_label"], as_index=False)["weight"].sum()
    totals = grouped.groupby(["scenario_label", "urgency_zone"], as_index=False)["weight"].sum().rename(columns={"weight": "zone_total"})
    merged = grouped.merge(totals, on=["scenario_label", "urgency_zone"], how="left")
    merged["share_pct"] = 100.0 * merged["weight"] / merged["zone_total"].replace(0, pd.NA)
    merged = merged.dropna(subset=["share_pct"])

    pivot_urgency = pd.DataFrame()
    if not merged.empty:
        pivot_urgency = merged.pivot(index=["scenario_label", "urgency_zone"], columns="peer_label", values="share_pct").fillna(0)


    dist_work = df.copy()
    distance_values = pd.to_numeric(dist_work.get("distance"), errors="coerce")
    bins = [-1e9, 0, 2, 5, 10, 20, 50, 1e9]
    bin_labels = ["<=0", "1-2", "3-5", "6-10", "11-20", "21-50", ">50"]
    dist_work["distance_bucket"] = pd.cut(distance_values, bins=bins, labels=bin_labels, include_lowest=True)
    dist_work = dist_work.dropna(subset=["distance_bucket", "peer_label", "scenario_label"])

    heatmap_data = pd.DataFrame()
    if not dist_work.empty:
        counts = (
            dist_work.groupby(["scenario_label", "distance_bucket", "peer_label"], as_index=False, observed=False)
            .size()
            .rename(columns={"size": "segments"})
        )
        if not counts.empty:
            pivot_dist = counts.pivot_table(
                index=["scenario_label", "distance_bucket"], columns="peer_label",
                values="segments", fill_value=0, observed=False,
            )
            heatmap_shares = 100.0 * pivot_dist.div(pivot_dist.sum(axis=1).replace(0, pd.NA), axis=0)
            heatmap_data = heatmap_shares.apply(pd.to_numeric, errors="coerce").fillna(0.0)
            heatmap_data.index = [f"{scenario_display_label(s)} | {b}" for s, b in heatmap_data.index]

    has_left = not pivot_urgency.empty
    has_right = not heatmap_data.empty
    if not has_left and not has_right:
        return

    if has_left and has_right:
        fig_height = max(5.5, 0.35 * len(heatmap_data) + 2)
        fig, axes = plt.subplots(1, 2, figsize=(15, fig_height),
                                 gridspec_kw={"width_ratios": [1, 1.3]})
        ax_left, ax_right = axes[0], axes[1]
    elif has_left:
        fig, ax_left = plt.subplots(figsize=(10, 5.5))
        ax_right = None
    else:
        fig_height = max(5.5, 0.35 * len(heatmap_data) + 2)
        fig, ax_right = plt.subplots(figsize=(10, fig_height))
        ax_left = None

    if has_left and ax_left is not None:

        col_order = [p for p in PEER_ORDER if p in pivot_urgency.columns]
        col_extras = sorted(set(pivot_urgency.columns) - set(col_order))
        pivot_urgency = pivot_urgency[col_order + col_extras]

        bar_labels = [f"{scenario_display_label(s)}\n{z}" for s, z in pivot_urgency.index]
        pivot_plot = pivot_urgency.copy()
        pivot_plot.index = bar_labels
        bar_colors = _peer_colors(pivot_plot.columns)

        pivot_plot.plot(kind="bar", stacked=True, ax=ax_left, width=0.78, color=bar_colors)
        ax_left.set_title("(a) Zuweisungsanteil nach Urgency Zone")
        ax_left.set_xlabel("Abrufpfad / Zone")
        ax_left.set_ylabel("Zugewiesener Anteil [%]")
        ax_left.tick_params(axis="x", rotation=0)
        for label in ax_left.get_xticklabels():
            label.set_ha("center")
        ax_left.legend(title="Provider-Typ", fontsize=THESIS_LEGEND_SIZE, title_fontsize=THESIS_LEGEND_TITLE_SIZE)

    if has_right and ax_right is not None:
        heatmap_colors = _semantic_heatmap_colors(heatmap_data)
        ax_right.imshow(heatmap_colors, aspect="auto")
        ax_right.set_xticks(np.arange(len(heatmap_data.columns)))
        ax_right.set_xticklabels(heatmap_data.columns)
        ax_right.set_yticks(np.arange(len(heatmap_data.index)))
        ax_right.set_yticklabels(heatmap_data.index)
        ax_right.set_xticks(np.arange(-0.5, len(heatmap_data.columns), 1), minor=True)
        ax_right.set_yticks(np.arange(-0.5, len(heatmap_data.index), 1), minor=True)
        ax_right.grid(which="minor", color="white", linestyle="-", linewidth=1.0)
        ax_right.tick_params(which="minor", bottom=False, left=False)
        for x_idx, column in enumerate(heatmap_data.columns):
            color = PEER_PALETTE.get(str(column).lower(), "#333333")
            ax_right.get_xticklabels()[x_idx].set_color(color)
            ax_right.get_xticklabels()[x_idx].set_fontweight("bold")
        for y_idx, (_, row_values) in enumerate(heatmap_data.iterrows()):
            for x_idx, value in enumerate(row_values):
                text_color = "white" if value >= 58 else "#222222"
                ax_right.text(x_idx, y_idx, f"{value:.0f}", ha="center", va="center", fontsize=THESIS_ANNOTATION_SIZE, color=text_color)
        ax_right.set_title("(b) Provider-Anteil nach Distanzklasse")
        ax_right.set_xlabel("Provider-Typ")
        ax_right.set_ylabel("Abrufpfad | Distanzklasse")

    save_svg(fig, out_dir / "gs_scheduler_analysis.svg")









def fig_peer_utilization(peer_df: pd.DataFrame, out_dir: pathlib.Path):
    avg_shares = provider_average_share_df(peer_df)
    if avg_shares.empty:
        return


    scenarios = _ordered_scenarios(avg_shares)
    if not scenarios:
        return


    n = len(scenarios)
    ncols = min(n, 3)
    nrows = (n + ncols - 1) // ncols

    fig = plt.figure(figsize=(10.8 * ncols, 6.7 * nrows))
    grid = fig.add_gridspec(
        nrows,
        ncols * 2,
        width_ratios=[1.0, 0.86] * ncols,
        wspace=0.16,
        hspace=0.36,
    )

    scenario_data = {}
    for scenario in scenarios:
        part = avg_shares[avg_shares["scenario_label"] == scenario].copy()
        part = part[part["share_pct"] > 0]
        scenario_data[scenario] = part

    for idx, scenario in enumerate(scenarios):
        row, col = divmod(idx, ncols)
        ax = fig.add_subplot(grid[row, col * 2])
        legend_ax = fig.add_subplot(grid[row, col * 2 + 1])
        legend_ax.axis("off")

        draw_provider_share_pie(ax, legend_ax, avg_shares, scenario, scenario_display_label(scenario))

    fig.suptitle("Aggregierte Provider-Anteile", fontsize=PEER_UTILIZATION_TITLE_SIZE, y=0.98)
    fig.subplots_adjust(left=0.035, right=0.98, top=0.82, bottom=0.08)
    save_svg(fig, out_dir / "peer_utilization.svg", tight_layout=False)









def _gs_race_overhead(run_df: pd.DataFrame, scheduler_df: pd.DataFrame) -> dict[str, float]:
    if scheduler_df.empty:
        return {}

    def overhead_from_results() -> dict[str, float]:
        required = {"mode", "event_type", "method", "success", "run_id", "race_wasted_bytes"}
        if not required.issubset(scheduler_df.columns):
            return {}
        race_results = scheduler_df[
            (scheduler_df["mode"] == "graphsync")
            & (scheduler_df["event_type"] == "scheduler_result")
            & (scheduler_df["method"] == "race")
            & (scheduler_df["success"] > 0)
        ].copy()
        if race_results.empty:
            return {}
        race_results["race_wasted_bytes"] = pd.to_numeric(race_results["race_wasted_bytes"], errors="coerce").fillna(0)
        wasted_per_run = race_results.groupby("run_id")["race_wasted_bytes"].sum().to_dict()

        overhead: dict[str, float] = {}
        for _, row in run_df.iterrows():
            rid = row["run_id"]
            if row.get("mode") != "graphsync":
                continue
            total = numeric_or_none(row.get("total_mib"))
            if total is None or total <= 0:
                continue
            total_bytes = total * 1024 * 1024
            wasted = wasted_per_run.get(rid, 0.0)
            overhead[rid] = 100.0 * wasted / total_bytes if total_bytes > 0 else 0.0
        return overhead

    exact = scheduler_df[
        (scheduler_df["mode"] == "graphsync")
        & (scheduler_df["event_type"] == "scheduler_race_overhead")
    ].copy()
    if exact.empty or "race_metrics_complete" not in exact.columns:
        return overhead_from_results()

    incomplete_run_ids = set(exact.loc[exact["race_metrics_complete"] <= 0, "run_id"].astype(str))
    expected_counts: dict[str, int] = {}
    if {"mode", "event_type", "method", "success", "run_id"}.issubset(scheduler_df.columns):
        successful_results = scheduler_df[
            (scheduler_df["mode"] == "graphsync")
            & (scheduler_df["event_type"] == "scheduler_result")
            & (scheduler_df["method"] == "race")
            & (scheduler_df["success"] > 0)
        ]
        if not successful_results.empty:
            expected_counts = successful_results.groupby("run_id").size().astype(int).to_dict()

    wasted_per_run: dict[str, float] = {}
    complete = exact[exact["race_metrics_complete"] > 0]
    complete_counts = complete.groupby("run_id").size().astype(int).to_dict()
    for _, r in complete.iterrows():
        run_id = r["run_id"]
        if str(run_id) in incomplete_run_ids:
            continue
        wasted = numeric_or_none(r.get("race_wasted_bytes"))
        if wasted is None:
            continue
        wasted_per_run[run_id] = wasted_per_run.get(run_id, 0.0) + wasted


    overhead: dict[str, float] = {}
    for _, row in run_df.iterrows():
        rid = row["run_id"]
        if row.get("mode") != "graphsync":
            continue
        if str(rid) in incomplete_run_ids:
            continue
        expected = expected_counts.get(rid)
        if expected is not None and complete_counts.get(rid, 0) < expected:
            continue
        total = numeric_or_none(row.get("total_mib"))
        if total is None or total <= 0:
            continue
        total_bytes = total * 1024 * 1024
        wasted = wasted_per_run.get(rid, 0.0)
        overhead[rid] = 100.0 * wasted / total_bytes if total_bytes > 0 else 0.0

    return overhead if overhead else overhead_from_results()


def overhead_dataframe(run_df: pd.DataFrame, scheduler_df: pd.DataFrame) -> pd.DataFrame:
    df, _ = _success_filter(run_df)
    if df.empty:
        return pd.DataFrame()

    gs_overhead = _gs_race_overhead(run_df, scheduler_df)

    rows = []
    missing_bitswap = []
    missing_graphsync = []
    for _, row in df.iterrows():
        run_id = row["run_id"]
        mode = row.get("mode", "")

        if mode == "graphsync":
            if run_id not in gs_overhead:
                missing_graphsync.append(run_id)
                continue
            overhead_pct = gs_overhead[run_id]
        else:
            data_recv = numeric_or_none(row.get("bitswap_data_recv"))
            dup_recv = numeric_or_none(row.get("bitswap_dup_data_recv"))
            if data_recv is not None and data_recv > 0 and dup_recv is not None:
                overhead_pct = 100.0 * dup_recv / data_recv
            else:
                missing_bitswap.append(run_id)
                continue

        rows.append({
            "scenario_label": row["scenario_label"],
            "run_id": run_id,
            "overhead_pct": overhead_pct,
        })
    if missing_graphsync:
        print(
            "warning: skipping GraphSync overhead for runs without complete race accounting: "
            + ", ".join(str(run_id) for run_id in missing_graphsync)
        )
    if missing_bitswap:
        print(
            "warning: skipping Bitswap overhead for runs without duplicate-byte stats: "
            + ", ".join(str(run_id) for run_id in missing_bitswap)
        )
    return pd.DataFrame(rows)


def fig_overhead(
    run_df: pd.DataFrame, scheduler_df: pd.DataFrame, out_dir: pathlib.Path,
):
    overhead_df = overhead_dataframe(run_df, scheduler_df)

    if overhead_df.empty:
        return


    avg = overhead_df.groupby("scenario_label", as_index=False)["overhead_pct"].mean()


    order = [s for s in SCENARIO_ORDER if s in avg["scenario_label"].values]
    extras = sorted(set(avg["scenario_label"].values) - set(order))
    order_list = order + extras
    avg["scenario_label"] = pd.Categorical(avg["scenario_label"], categories=order_list, ordered=True)
    avg = avg.sort_values("scenario_label")

    colors = _scenario_colors(avg["scenario_label"].tolist())

    fig, ax = plt.subplots(figsize=(10, 4.0))
    bars = ax.bar(
        range(len(avg)), avg["overhead_pct"].values,
        color=colors, width=0.72, edgecolor="white", linewidth=0.8,
    )
    ax.set_xticks(range(len(avg)))
    ax.set_xticklabels([scenario_display_label(label) for label in avg["scenario_label"].tolist()], rotation=0, ha="center")
    ax.set_ylabel("Zusätzlicher Datentransfer [%]")
    ax.set_title("Zusätzlicher Datentransfer nach Abrufpfad")
    ax.xaxis.label._thesis_fontsize_override = 15
    ax.yaxis.label._thesis_fontsize_override = 12
    ax.title._thesis_fontsize_override = 14
    ax._thesis_tick_label_size_override = 12


    for bar_obj, val in zip(bars, avg["overhead_pct"].values):
        x_pos = bar_obj.get_x() + bar_obj.get_width() / 2
        y_pos = bar_obj.get_height() + 0.3 if val > 0 else 0.3
        ax.text(
            x_pos,
            y_pos,
            f"{val:.2f}%",
            ha="center", va="bottom", fontsize=11, fontweight="bold",
        )

    ax.set_ylim(0, max(avg["overhead_pct"].max() * 1.18, 1))
    fig.subplots_adjust(left=0.14, right=0.985, top=0.84, bottom=0.22)
    save_svg(fig, out_dir / "overhead.svg", tight_layout=False)
def export_tables(
    run_df: pd.DataFrame,
    peer_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    scheduler_df: pd.DataFrame,
    bitswap_readiness_df: pd.DataFrame,
    segment_lateness_df: pd.DataFrame,
    out_dir: pathlib.Path,
):
    run_df.to_csv(out_dir / "runs_flat.csv", index=False)
    if not peer_df.empty:
        peer_df.to_csv(out_dir / "peers_flat.csv", index=False)
        shares = provider_share_df(peer_df)
        if not shares.empty:
            shares.to_csv(out_dir / "provider_shares.csv", index=False)
            concentration = (
                shares.groupby(["run_id", "scenario_label"], as_index=False)
                .agg(
                    top1_share=("share_pct", "max"),
                    hhi=("share_pct", lambda x: ((x / 100.0) ** 2).sum()),
                )
                .copy()
            )
            concentration["effective_providers"] = 1.0 / concentration["hhi"].replace(0, pd.NA)
            concentration.to_csv(out_dir / "provider_concentration.csv", index=False)
    if not sample_df.empty:
        sample_df.to_csv(out_dir / "samples_flat.csv", index=False)
    if not prediction_df.empty:
        prediction_df.to_csv(out_dir / "predictions_flat.csv", index=False)
    if not scheduler_df.empty:
        scheduler_df.to_csv(out_dir / "scheduler_events.csv", index=False)
    if not bitswap_readiness_df.empty:
        bitswap_readiness_df.to_csv(out_dir / "bitswap_segment_readiness.csv", index=False)
    if not segment_lateness_df.empty:
        segment_lateness_df.to_csv(out_dir / "segment_lateness.csv", index=False)





def fig_ema_accuracy(sample_df: pd.DataFrame, out_dir: pathlib.Path) -> None:
    if sample_df.empty or "mode" not in sample_df.columns:
        print("F10 skipped: no GraphSync throughput samples.")
        return
    gs_mask = sample_df["mode"] == "graphsync"
    df = sample_df.loc[gs_mask].copy()
    if df.empty:
        print("F10 skipped: no GraphSync throughput samples.")
        return


    valid = df["duration_ms"].notna() & (df["duration_ms"] > 0) & df["bytes"].notna()
    df = df.loc[valid].copy()
    if df.empty:
        print("F10 skipped: no valid throughput samples with duration > 0.")
        return
    df["actual_mbit_s"] = df["bytes"] / (df["duration_ms"] / 1000) * 8 / 1_000_000


    df["rel_error"] = (df["ema_mbit_s"] - df["actual_mbit_s"]).abs() / df["actual_mbit_s"].replace(0, float("nan"))

    scenarios = sorted(df["scenario_label"].unique(),
                       key=lambda s: SCENARIO_ORDER.index(s) if s in SCENARIO_ORDER else 999)


    fig, (ax_scatter, ax_error) = plt.subplots(1, 2, figsize=(13, 5))



    for scenario in scenarios:
        sdf = df[
            (df["scenario_label"] == scenario)
            & (df["actual_mbit_s"] <= EMA_ACCURACY_AXIS_LIMIT_MBIT)
            & (df["ema_mbit_s"] <= EMA_ACCURACY_AXIS_LIMIT_MBIT)
        ]
        color = SCENARIO_PALETTE.get(scenario, "#888888")
        ax_scatter.scatter(sdf["actual_mbit_s"], sdf["ema_mbit_s"],
                           s=8, alpha=0.3, color=color, label=scenario_display_label(scenario), edgecolors="none")


    lim = EMA_ACCURACY_AXIS_LIMIT_MBIT
    ax_scatter.plot([0, lim], [0, lim], "k--", alpha=0.5, linewidth=1, label="Ideale Vorhersage")
    ax_scatter.set_xlim(0, lim)
    ax_scatter.set_ylim(0, lim)
    ax_scatter.set_xlabel("Tatsächlicher Durchsatz (Mbit/s)")
    ax_scatter.set_ylabel("EMA-Schätzung (Mbit/s)")
    ax_scatter.set_title("(a) EMA und tatsächlicher Durchsatz")
    ax_scatter.legend(fontsize=THESIS_LEGEND_SIZE, loc="upper left", framealpha=0.8)


    window = 20
    for scenario in scenarios:
        sdf = df[df["scenario_label"] == scenario].sort_values("time_s")
        if len(sdf) < window:
            continue
        rolling_err = sdf["rel_error"].rolling(window=window, min_periods=5).mean()
        color = SCENARIO_PALETTE.get(scenario, "#888888")
        ax_error.plot(sdf["time_s"], rolling_err, color=color, alpha=0.8,
                      linewidth=1.2, label=scenario_display_label(scenario))

    ax_error.set_xlabel("Zeit [s]")
    ax_error.set_ylabel("Relativer Fehler (gleitender Mittelwert)")
    ax_error.yaxis.label._thesis_fontsize_override = 17
    ax_error.set_title(f"(b) EMA-Fehler nach Update (Fenster={window})")
    ax_error.legend(fontsize=THESIS_LEGEND_SIZE, loc="upper right", framealpha=0.8)
    ax_error.set_ylim(bottom=0)

    fig.suptitle("Genauigkeit der EMA-Durchsatzschätzung", fontsize=THESIS_FIGURE_TITLE_SIZE, y=1.01)
    path = out_dir / "ema_accuracy.svg"
    save_svg(fig, path)


def fig_duration_prediction_accuracy(prediction_df: pd.DataFrame, out_dir: pathlib.Path) -> None:
    if prediction_df.empty:
        print("duration_prediction_accuracy skipped: no prediction telemetry.")
        return
    if "mode" not in prediction_df.columns:
        print("duration_prediction_accuracy skipped: missing mode column.")
        return
    df = prediction_df[prediction_df["mode"] == "graphsync"].copy()
    if df.empty:
        print("duration_prediction_accuracy skipped: no GraphSync prediction telemetry.")
        return
    df = df.dropna(subset=["predicted_duration_ms", "actual_duration_ms"])
    df = df[(df["predicted_duration_ms"] > 0) & (df["actual_duration_ms"] > 0)]
    if df.empty:
        print("duration_prediction_accuracy skipped: no valid duration predictions.")
        return

    kind_palette = {
        "urgent": "#005EA8",
        "normal": "#009E73",
        "discovery": "#E69F00",
        "rescue": "#D55E00",
        "race": "#56B4E9",
    }

    local_axis_label_size = max(22, THESIS_AXIS_LABEL_SIZE)
    local_tick_label_size = max(16, THESIS_TICK_LABEL_SIZE)
    local_legend_size = max(17, THESIS_LEGEND_SIZE)
    local_subplot_title_size = max(20, THESIS_SUBPLOT_TITLE_SIZE - 2)
    local_figure_title_size = max(26, THESIS_FIGURE_TITLE_SIZE)

    df["predicted_duration_s"] = df["predicted_duration_ms"] / 1000.0
    df["actual_duration_s"] = df["actual_duration_ms"] / 1000.0

    fig = plt.figure(figsize=(11.8, 8.5))
    grid = fig.add_gridspec(
        2,
        2,
        width_ratios=[1.22, 1.12],
        height_ratios=[1.0, 1.0],
        wspace=0.46,
        hspace=0.58,
    )
    ax_scatter = fig.add_subplot(grid[:, 0])
    ax_error = fig.add_subplot(grid[0, 1])
    ax_inflight = fig.add_subplot(grid[1, 1])
    axes = [ax_scatter, ax_error, ax_inflight]

    max_val = max(df["predicted_duration_s"].max(), df["actual_duration_s"].max())
    lim = max_val * 1.05 if max_val > 0 else 1.0
    for kind, group in df.groupby("request_kind", dropna=False):
        label = str(kind or "unknown")
        ax_scatter.scatter(
            group["actual_duration_s"],
            group["predicted_duration_s"],
            s=14,
            alpha=0.45,
            color=kind_palette.get(label, "#888888"),
            label=label,
            edgecolors="none",
        )
    ax_scatter.plot([0, lim], [0, lim], "k--", alpha=0.5, linewidth=1, label="Ideale Vorhersage")
    ax_scatter.set_xlim(0, lim)
    ax_scatter.set_ylim(0, lim)
    ax_scatter.set_xlabel("Tatsächliche Dauer [s]")
    ax_scatter.set_ylabel("Geschätzte Dauer [s]")
    ax_scatter.set_title("(a) Geschätzt vs. gemessen")
    ax_scatter.legend(fontsize=local_legend_size, loc="upper left", framealpha=0.85)

    window = 20
    time_df = df.dropna(subset=["time_s", "relative_duration_error"]).sort_values("time_s")
    for kind, group in time_df.groupby("request_kind", dropna=False):
        if len(group) < 5:
            continue
        label = str(kind or "unknown")
        rolling = group["relative_duration_error"].rolling(window=window, min_periods=5).mean()
        ax_error.plot(
            group["time_s"],
            rolling,
            color=kind_palette.get(label, "#888888"),
            linewidth=1.5,
            alpha=0.85,
            label=label,
        )
    ax_error.set_xlabel("Zeit [s]")
    ax_error.set_ylabel("Relativer Dauerfehler")
    ax_error.set_title("(b) Fehlerverlauf")
    ax_error.set_ylim(bottom=0)
    handles, labels = ax_error.get_legend_handles_labels()
    if handles:
        ax_error.legend(handles, labels, fontsize=local_legend_size, loc="upper right", framealpha=0.85)

    inflight_df = df.dropna(subset=["inflight_bucket", "relative_duration_error"]).copy()
    if inflight_df.empty:
        ax_inflight.text(0.5, 0.5, "Keine Inflight-Daten", transform=ax_inflight.transAxes, ha="center", va="center")
    else:
        order = [bucket for bucket in ["0", "1", "2+"] if bucket in set(inflight_df["inflight_bucket"])]
        values = [inflight_df[inflight_df["inflight_bucket"] == bucket]["relative_duration_error"].values for bucket in order]
        ax_inflight.boxplot(values, tick_labels=order, showfliers=False)
        for pos, bucket in enumerate(order, start=1):
            points = inflight_df[inflight_df["inflight_bucket"] == bucket]["relative_duration_error"].values
            jitter = np.linspace(-0.08, 0.08, len(points)) if len(points) > 1 else np.array([0.0])
            ax_inflight.scatter(np.full(len(points), pos) + jitter, points, s=12, alpha=0.4, color="#444444")
    ax_inflight.set_xlabel("Laufende Provider-Abrufe\nbeim Start", labelpad=8)
    ax_inflight.set_ylabel("Relativer Dauerfehler")
    ax_inflight.set_title("(c) Fehler nach Provider-Last")
    ax_inflight.set_ylim(bottom=0)

    for ax in axes:
        ax.title._thesis_fontsize_override = local_subplot_title_size
        ax.xaxis.label._thesis_fontsize_override = local_axis_label_size
        ax.yaxis.label._thesis_fontsize_override = local_axis_label_size
        ax._thesis_tick_label_size_override = local_tick_label_size

    fig.suptitle("Genauigkeit der GraphSync-Dauerschätzung", fontsize=THESIS_FIGURE_TITLE_SIZE, y=1.02)
    path = out_dir / "duration_prediction_accuracy.svg"
    if fig._suptitle is not None:
        fig._suptitle.set_fontsize(local_figure_title_size)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.88, bottom=0.13, wspace=0.46, hspace=0.58)
    save_svg(fig, path, tight_layout=False)
    print(f"  F9 {path}")




def main():
    parser = argparse.ArgumentParser(description="Generate thesis-ready benchmark figures from exported run JSON files.")
    parser.add_argument("--input", required=True, help="Path to run JSON directory or ZIP downloaded from /runs/download")
    parser.add_argument("--out", default="analysis/figures", help="Directory for SVG outputs")
    parser.add_argument("--experiment", default="", help="Optional experiment folder key, for example 1_heterogen")
    parser.add_argument(
        "--context",
        default="paper",
        choices=("paper", "notebook", "talk", "poster"),
        help="Matplotlib/seaborn plotting context.",
    )
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    setup_style(args.context)
    run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df = load_runs(input_path)
    if run_df.empty:
        raise SystemExit("No run records found in input.")

    available_experiments = []
    if "experiment_key" in run_df.columns:
        available_experiments = sorted([key for key in run_df["experiment_key"].dropna().unique() if str(key)])

    experiment_key = str(args.experiment or "").strip()
    inferred_experiment = infer_experiment_from_out_dir(out_dir)
    if not experiment_key and inferred_experiment:
        experiment_key = inferred_experiment
        print(f"Selected experiment from output directory: {experiment_key}")
    elif experiment_key:
        print(f"Selected experiment: {experiment_key}")

    if experiment_key:
        if available_experiments and experiment_key not in available_experiments:
            raise SystemExit(
                f"Experiment {experiment_key!r} not found in input. "
                f"Available experiments: {', '.join(available_experiments)}"
            )
        run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df = filter_loaded_data_by_experiment(
            run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df, experiment_key
        )
        if run_df.empty:
            raise SystemExit(f"No run records left after filtering to experiment {experiment_key!r}.")
    elif len(available_experiments) > 1:
        warnings.warn(
            "Input contains multiple experiment folders but no --experiment was selected and no thesis "
            "output folder could be inferred; generating aggregate plots across "
            f"{', '.join(available_experiments)}.",
            RuntimeWarning,
            stacklevel=2,
        )

    mismatched_run_ids = warn_peer_label_mismatches(run_df, peer_df)
    if mismatched_run_ids:
        run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df = filter_loaded_data_excluding_runs(
            run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df, mismatched_run_ids
        )
        if run_df.empty:
            raise SystemExit("No run records left after excluding runs with mismatched provider labels.")

    segment_lateness_df = build_segment_lateness_df(run_df, scheduler_df, bitswap_readiness_df)


    export_tables(run_df, peer_df, sample_df, prediction_df, scheduler_df, bitswap_readiness_df, segment_lateness_df, out_dir)


    fig_qoe_summary(run_df, out_dir)
    fig_throughput_summary(run_df, out_dir)
    fig_segment_timing_summary(run_df, segment_lateness_df, out_dir)
    fig_provider_load(peer_df, out_dir)
    fig_peer_utilization(peer_df, out_dir)
    fig_provider_usage(peer_df, out_dir)
    fig_overhead(run_df, scheduler_df, out_dir)
    fig_gs_scheduler_analysis(scheduler_df, out_dir)
    fig_duration_prediction_accuracy(prediction_df, out_dir)
    fig_ema_accuracy(sample_df, out_dir)

    print(f"Loaded runs: {len(run_df)}")
    print(f"Loaded peer rows: {len(peer_df)}")
    print(f"Loaded throughput samples: {len(sample_df)}")
    print(f"Loaded duration prediction samples: {len(prediction_df)}")
    print(f"Loaded scheduler events: {len(scheduler_df)}")
    print(f"Loaded bitswap segment readiness events: {len(bitswap_readiness_df)}")
    print(f"Output directory: {out_dir}")


if __name__ == "__main__":
    main()
