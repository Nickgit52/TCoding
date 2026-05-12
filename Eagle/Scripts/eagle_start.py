#!/usr/bin/env python3
"""
eagle_start.py — Eagle startup script (Mac side).
Usage: python3 Scripts/eagle_start.py

Orchestrates the full pipeline:
  1. Check/start Parallels + Windows 11
  2. Wait for the Sierra Chart volume to mount
  3. Check Sierra Chart state (sync is owned by Pulse `sync` alias)
  4. Run the pipeline: build_history → build_candles → build_features
  5. Run analyses: market_profile, orderflow_regimes
  6. (optional) Launch the dashboard

Flags:
  --no-build     Skip the ticks/candles/features rebuild
  --no-analysis  Skip market_profile + orderflow
  --dashboard    Launch the dashboard after the pipeline
  --sync-only    Sync .scid only, do not run anything else
"""
import subprocess
import sys
import time
from pathlib import Path

EAGLE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = EAGLE_DIR / "Scripts"
SC_VOLUME = Path("/Volumes/[C] Windows 11")
SC_DATA = SC_VOLUME / "SierraChart" / "Data"
VM_NAME = "Windows 11"

# Terminal colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


def log(msg, color=RESET):
    print(f"  {color}{msg}{RESET}")


def log_step(step, msg):
    print(f"\n{CYAN}[{step}]{RESET} {BOLD}{msg}{RESET}")


def run_script(name, description):
    """Run a Python script and return True on success."""
    script_path = SCRIPTS_DIR / name
    if not script_path.exists():
        log(f"Script not found: {name}", RED)
        return False

    log(f"→ {description}...")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(EAGLE_DIR),
    )
    if result.returncode != 0:
        log(f"ERROR: {name} failed (code {result.returncode})", RED)
        return False
    return True


def check_parallels_running():
    """Check whether the Windows 11 VM is running in Parallels."""
    try:
        result = subprocess.run(
            ["prlctl", "list", "-a"],
            capture_output=True, text=True, timeout=10
        )
        if VM_NAME in result.stdout and "running" in result.stdout:
            return True
        if VM_NAME in result.stdout and "stopped" in result.stdout:
            return False
        # VM exists but state unknown
        return "suspended" not in result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def start_parallels():
    """Start the Windows 11 VM."""
    log_step("1", "Starting Parallels / Windows 11")

    if check_parallels_running():
        log("VM already running ✓", GREEN)
        return True

    log("Starting the VM...", YELLOW)
    try:
        result = subprocess.run(
            ["prlctl", "start", VM_NAME],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            log(f"prlctl error: {result.stderr.strip()}", RED)
            log("Start Parallels manually and rerun the script.", YELLOW)
            return False
        log("VM started ✓", GREEN)
        return True
    except FileNotFoundError:
        log("prlctl not found — is Parallels installed?", RED)
        return False
    except subprocess.TimeoutExpired:
        log("Startup timeout — check Parallels manually", RED)
        return False


def wait_for_volume(timeout=180):
    """Wait for the Sierra Chart volume to mount."""
    log_step("2", "Waiting for the Sierra Chart volume")

    # First check whether the base volume is mounted (even without SierraChart/Data)
    if SC_DATA.exists():
        n_scid = len(list(SC_DATA.glob("*.scid")))
        log(f"Volume mounted ✓ — {n_scid} .scid files found", GREEN)
        return True

    if SC_VOLUME.exists() and not SC_DATA.exists():
        log(f"Windows volume mounted but SierraChart/Data not found", YELLOW)
        log(f"Check the path: {SC_DATA}", YELLOW)
        return False

    log(f"Waiting for {SC_VOLUME}...", YELLOW)
    log(f"(Windows must be fully booted — desktop visible)", YELLOW)
    start = time.time()
    while time.time() - start < timeout:
        if SC_DATA.exists():
            time.sleep(2)  # Small delay so the FS settles
            n_scid = len(list(SC_DATA.glob("*.scid")))
            log(f"Volume mounted ✓ — {n_scid} .scid files ({time.time()-start:.0f}s)", GREEN)
            return True
        if SC_VOLUME.exists() and not SC_DATA.exists():
            log(f"Windows volume mounted but SierraChart/Data not found", YELLOW)
            return False
        time.sleep(5)
        elapsed = int(time.time() - start)
        if elapsed % 30 == 0:
            log(f"  ...{elapsed}s — still waiting for the Windows volume", YELLOW)

    log(f"Timeout after {timeout}s — the volume is not mounted", RED)
    log("Check that:", YELLOW)
    log("  1. Windows is fully booted (desktop visible)", YELLOW)
    log("  2. Parallels Tools is installed in Windows", YELLOW)
    log("  3. File sharing is enabled in Parallels", YELLOW)
    return False


def check_sierra_state():
    """Display the Sierra Chart .scid state (informational; sync is owned by Pulse)."""
    log_step("3", "Sierra Chart state")

    if not SC_DATA.exists():
        log("Sierra Chart volume not available — skip", YELLOW)
        return False

    # Count active .scid on the volume
    scid_files = list(SC_DATA.glob("*.scid"))
    total_size = sum(f.stat().st_size for f in scid_files) / 1e9
    log(f"Sierra Chart: {len(scid_files)} .scid files ({total_size:.1f} GB)")
    log("Sync owned by Pulse (`sync` alias) — Eagle reads through Pulse's working copy.", CYAN)
    return True


def run_pipeline(skip_build=False, skip_analysis=False):
    """Run the full data pipeline."""

    if not skip_build:
        log_step("4", "Data pipeline")

        if not run_script("build_history.py", "Ticks .scid → Parquet (sync + incremental)"):
            log("Pipeline stopped — build_history failed", RED)
            return False

        if not run_script("build_candles.py", "Ticks → Candles (irreversible roll)"):
            log("Pipeline stopped — build_candles failed", RED)
            return False

        if not run_script("build_features.py", "Candles → ML Features"):
            log("build_features failed — continuing anyway", YELLOW)

    if not skip_analysis:
        log_step("5", "Analyses")

        run_script("market_profile.py", "Market Profile + Naked POCs")
        run_script("orderflow_regimes.py", "Order Flow Regimes")

    return True


def launch_dashboard():
    """Launch the dashboard in the background."""
    log_step("6", "Dashboard")
    server = EAGLE_DIR / "eagle_server.py"
    if not server.exists():
        log("eagle_server.py not found", RED)
        return

    log("Launching dashboard on http://localhost:8888 ...", GREEN)
    subprocess.Popen(
        [sys.executable, str(server)],
        cwd=str(EAGLE_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log("Dashboard launched in the background ✓", GREEN)


def main():
    print()
    print(f"  {BOLD}{'=' * 55}{RESET}")
    print(f"  {BOLD}{CYAN}  EAGLE — Pipeline Startup{RESET}")
    print(f"  {BOLD}{'=' * 55}{RESET}")

    args = set(sys.argv[1:])
    sync_only = "--sync-only" in args
    skip_build = "--no-build" in args
    skip_analysis = "--no-analysis" in args
    with_dashboard = "--dashboard" in args

    # Step 1: Parallels
    if not start_parallels():
        log("\nYou can rerun the script when Parallels is ready.", YELLOW)
        log(f"  python3 Scripts/eagle_start.py --no-build", YELLOW)
        sys.exit(1)

    # Step 2: Volume
    if not wait_for_volume():
        log("\nThe pipeline can run without the volume (cached data).", YELLOW)
        if sync_only:
            sys.exit(1)
        log("Continuing with existing data...", YELLOW)

    # Step 3: Sierra state info
    check_sierra_state()

    if sync_only:
        log("\n  --sync-only: stopping after sync.", CYAN)
        sys.exit(0)

    # Step 4-5: Pipeline
    run_pipeline(skip_build=skip_build, skip_analysis=skip_analysis)

    # Step 6: Dashboard (optional)
    if with_dashboard:
        launch_dashboard()

    # Summary
    print()
    print(f"  {BOLD}{'=' * 55}{RESET}")
    print(f"  {GREEN}  Eagle ready ✓{RESET}")
    print(f"  {BOLD}{'=' * 55}{RESET}")
    print()


if __name__ == "__main__":
    main()
