#!/usr/bin/env python3
"""
DAT535 Lab Pipeline Orchestrator
=================================

Run individual labs or all labs in sequence.

Usage:
    python run_pipeline.py lab2          # Run Lab 2 only
    python run_pipeline.py lab3          # Run Lab 3 only
    python run_pipeline.py all           # Run all labs (Lab 2 then Lab 3)
    python run_pipeline.py --help        # Show help
"""

import sys
import argparse
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_lab2():
    """Run Lab 2: Spark Fundamentals & the Medallion Architecture."""
    logger.info("=" * 60)
    logger.info("RUNNING LAB 2: SPARK FUNDAMENTALS & MEDALLION ARCHITECTURE")
    logger.info("=" * 60)

    from lab2_pipeline import Lab2Pipeline

    pipeline = Lab2Pipeline()
    return pipeline.run()


def run_lab3():
    """Run Lab 3: Advanced Spark & Production Patterns.

    Requires Lab 2 to have already run, since Lab 3 loads the shared Silver-layer
    dataset that Lab 2 produces.
    """
    logger.info("=" * 60)
    logger.info("RUNNING LAB 3: ADVANCED SPARK & PRODUCTION PATTERNS")
    logger.info("=" * 60)

    from lab3_pipeline import Lab3Pipeline

    pipeline = Lab3Pipeline()
    return pipeline.run()


def run_all():
    """Run all labs in sequence (Lab 2 must complete before Lab 3)."""
    logger.info("=" * 60)
    logger.info("RUNNING ALL LABS")
    logger.info("=" * 60)

    results = {}
    total_start = time.time()

    # Lab 2 (generates and saves the shared dataset used by Lab 3)
    try:
        results["lab2"] = run_lab2()
        logger.info("✓ Lab 2 completed successfully")
    except Exception as e:
        logger.error(f"✗ Lab 2 failed: {e}")
        results["lab2"] = {"status": "failed", "error": str(e)}

    # Lab 3 (only attempt if Lab 2 succeeded, since it depends on Lab 2's output)
    if results["lab2"].get("status") == "success":
        try:
            results["lab3"] = run_lab3()
            logger.info("✓ Lab 3 completed successfully")
        except Exception as e:
            logger.error(f"✗ Lab 3 failed: {e}")
            results["lab3"] = {"status": "failed", "error": str(e)}
    else:
        logger.warning("Skipping Lab 3 because Lab 2 did not complete successfully")
        results["lab3"] = {"status": "skipped"}

    total_time = time.time() - total_start

    # Summary
    logger.info("=" * 60)
    logger.info("ALL LABS SUMMARY")
    logger.info("=" * 60)

    for lab, result in results.items():
        status = result.get("status", "unknown")
        elapsed = result.get("elapsed_time", 0)
        logger.info(f"  {lab}: {status} ({elapsed:.2f}s)")

    logger.info(f"  Total time: {total_time:.2f}s")

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="DAT535 Lab Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_pipeline.py lab2          # Run Lab 2 only
    python run_pipeline.py lab3          # Run Lab 3 only
    python run_pipeline.py all           # Run all labs

Labs:
    lab2    Spark Fundamentals & Medallion Architecture - DataFrame/RDD/SQL
            conversions, MapReduce, I/O, Bronze/Silver/Gold layers
    lab3    Advanced Spark & Production Patterns - window functions, joins,
            partitioning, caching, UDFs, streaming, optimization
            (requires Lab 2's shared dataset to have been generated first)
        """,
    )

    parser.add_argument(
        "pipeline", choices=["lab2", "lab3", "all"], help="Pipeline to run"
    )

    args = parser.parse_args()

    # Dispatch to appropriate pipeline
    pipeline_map = {"lab2": run_lab2, "lab3": run_lab3, "all": run_all}

    try:
        result = pipeline_map[args.pipeline]()

        # Check status
        if args.pipeline == "all":
            all_success = all(r.get("status") == "success" for r in result.values())
            return 0 if all_success else 1
        else:
            return 0 if result.get("status") == "success" else 1

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
