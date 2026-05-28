#!/usr/bin/env python3
import io
import os
import pathlib
import subprocess
import tempfile
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


RUNS_DIR = os.environ.get("RUNS_DIR", "/data/runs")
PLOT_SCRIPT = os.environ.get("PLOT_SCRIPT", "/app/analysis/plot_runs.py")
ADDR = os.environ.get("PLOT_SERVER_ADDR", "0.0.0.0")
PORT = int(os.environ.get("PLOT_SERVER_PORT", "8090"))
PLOT_EXTENSIONS = {".svg", ".csv"}


def has_run_json(path: pathlib.Path) -> bool:
    return any(item.is_file() and item.suffix.lower() == ".json" for item in path.iterdir())


def plot_groups(runs_path: pathlib.Path) -> list[tuple[str, pathlib.Path]]:
    groups = []
    for child in sorted(runs_path.iterdir(), key=lambda item: item.name):
        if child.is_dir() and has_run_json(child):
            groups.append((child.name, child))
    if groups:
        return groups
    return [("custom", runs_path)]


def generated_plot_files(path: pathlib.Path) -> list[pathlib.Path]:
    return sorted(
        item
        for item in path.rglob("*")
        if item.is_file() and item.suffix.lower() in PLOT_EXTENSIONS
    )


def process_output(proc: subprocess.CompletedProcess) -> str:
    return (proc.stdout + "\n" + proc.stderr).strip()


def build_plots_zip(runs_path: pathlib.Path, plot_script: str, python_cmd: str = "python") -> bytes:
    with tempfile.TemporaryDirectory(prefix="bench-plots-") as tmp_dir:
        tmp_path = pathlib.Path(tmp_dir)
        outputs = []
        for group_name, group_input in plot_groups(runs_path):
            group_output = tmp_path / group_name
            group_output.mkdir(parents=True, exist_ok=True)
            cmd = [
                python_cmd,
                plot_script,
                "--input",
                str(group_input),
                "--out",
                str(group_output),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                details = process_output(proc)
                raise RuntimeError(f"plot generation failed for {group_name}\n{details}")
            outputs.append((group_name, group_output, proc))

        files = []
        for group_name, group_output, _ in outputs:
            for file_path in generated_plot_files(group_output):
                rel = file_path.relative_to(group_output).as_posix()
                files.append((file_path, f"{group_name}/{rel}"))
        if not files:
            details = "\n".join(
                f"[{group_name}]\n{process_output(proc)}"
                for group_name, _, proc in outputs
            ).strip()
            raise RuntimeError(f"no plot files generated\n{details}")

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path, arcname in sorted(files, key=lambda item: item[1]):
                zf.write(file_path, arcname=arcname)
        return buffer.getvalue()


class Handler(BaseHTTPRequestHandler):
    def _write_text(self, code: int, message: str):
        payload = (message + "\n").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/healthz":
            self._write_text(200, "ok")
            return
        if self.path != "/plots/download":
            self._write_text(404, "not found")
            return

        runs_path = pathlib.Path(RUNS_DIR)
        if not runs_path.exists():
            self._write_text(404, f"runs directory not found: {RUNS_DIR}")
            return

        try:
            payload = build_plots_zip(runs_path, PLOT_SCRIPT)
        except RuntimeError as err:
            self._write_text(500, str(err))
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", "attachment; filename=bench-plots.zip")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main():
    server = ThreadingHTTPServer((ADDR, PORT), Handler)
    print(f"plot-server listening on http://{ADDR}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
