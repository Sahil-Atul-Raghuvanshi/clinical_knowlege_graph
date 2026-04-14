"""
Full end-to-end pipeline orchestrator.

Execution order:
  1. Clinical Knowledge Graph creation  (1_create_clinical_knowledge_graph_pipeline.py)
  2. Embeddings generation               (2_create_embeddings_pipeline.py)

The embeddings step only runs if the KG step completes successfully.
"""
import importlib.util
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
script_dir = Path(__file__).parent          # Scripts/Run_Pipeline
scripts_root = script_dir.parent            # Scripts
project_root = scripts_root.parent          # project root

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logs_dir = project_root / "logs"
logs_dir.mkdir(parents=True, exist_ok=True)

log_filename = logs_dir / f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

file_handler = logging.FileHandler(log_filename, encoding="utf-8", mode="a")
stream_handler = logging.StreamHandler(sys.stdout)
if hasattr(stream_handler.stream, "reconfigure"):
    stream_handler.stream.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[file_handler, stream_handler],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def import_module_from_file(file_path: str, module_name: str):
    """Dynamically import a module from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def run_phase(label: str, phase_number: int, total_phases: int, fn) -> bool:
    """Run a single pipeline phase and return True on success."""
    logger.info("=" * 80)
    logger.info(f"PHASE {phase_number}/{total_phases}: {label}")
    logger.info("=" * 80)

    start = time.time()
    try:
        fn()
        elapsed = time.time() - start
        logger.info(
            f"✓ PHASE {phase_number}/{total_phases} COMPLETED: {label} "
            f"(took {elapsed:.2f}s / {elapsed / 60:.2f} min)"
        )
        logger.info("")
        return True
    except SystemExit as exc:
        # Scripts may call sys.exit(1) on failure — treat non-zero as failure
        elapsed = time.time() - start
        if exc.code not in (None, 0):
            logger.error(
                f"✗ PHASE {phase_number}/{total_phases} FAILED: {label} "
                f"(exited with code {exc.code} after {elapsed:.2f}s)"
            )
            return False
        # exit code 0 means success
        logger.info(
            f"✓ PHASE {phase_number}/{total_phases} COMPLETED: {label} "
            f"(took {elapsed:.2f}s / {elapsed / 60:.2f} min)"
        )
        return True
    except Exception as exc:
        elapsed = time.time() - start
        logger.error(
            f"✗ PHASE {phase_number}/{total_phases} FAILED: {label} "
            f"(after {elapsed:.2f}s)"
        )
        logger.error(f"Error: {exc}")
        logger.exception("Full traceback:")
        return False


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 80)
    logger.info("FULL END-TO-END PIPELINE STARTED")
    logger.info(f"  Phase 1 : Clinical Knowledge Graph creation")
    logger.info(f"  Phase 2 : Embeddings generation")
    logger.info(f"  Log file: {log_filename}")
    logger.info("=" * 80)
    logger.info("")

    overall_start = time.time()

    # ------------------------------------------------------------------
    # Phase 1 — Clinical Knowledge Graph
    # ------------------------------------------------------------------
    kg_script_path = str(script_dir / "1_create_clinical_knowledge_graph_pipeline.py")

    # Set the env var that script 14 (cleanup) relies on before importing
    os.environ["FULL_LOAD_LOG_FILE"] = str(log_filename)

    kg_module = import_module_from_file(kg_script_path, "create_clinical_knowledge_graph_pipeline")

    kg_success = run_phase(
        label="Clinical Knowledge Graph Creation",
        phase_number=1,
        total_phases=2,
        fn=kg_module.main,
    )

    if not kg_success:
        logger.error("")
        logger.error("=" * 80)
        logger.error("PIPELINE ABORTED — Phase 1 failed.")
        logger.error("Fix the errors above and re-run the pipeline.")
        logger.error("=" * 80)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 2 — Embeddings
    # ------------------------------------------------------------------
    emb_script_path = str(script_dir / "2_create_embeddings_pipeline.py")
    emb_module = import_module_from_file(emb_script_path, "create_embeddings_pipeline")

    emb_success = run_phase(
        label="Embeddings Generation",
        phase_number=2,
        total_phases=2,
        fn=emb_module.main,
    )

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    total_elapsed = time.time() - overall_start

    logger.info("")
    logger.info("=" * 80)
    logger.info("FULL PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"  Phase 1 — KG Creation : {'✓ SUCCESS' if kg_success  else '✗ FAILED'}")
    logger.info(f"  Phase 2 — Embeddings  : {'✓ SUCCESS' if emb_success else '✗ FAILED'}")
    logger.info(f"  Total time            : {total_elapsed:.2f}s ({total_elapsed / 60:.2f} min)")
    logger.info(f"  Log file              : {log_filename}")
    logger.info("=" * 80)

    if not emb_success:
        logger.error("One or more phases failed. Check the log for details.")
        sys.exit(1)

    logger.info("✓ ALL PHASES COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
