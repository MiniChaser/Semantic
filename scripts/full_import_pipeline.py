#!/usr/bin/env python3
"""
Full Import Pipeline: Execute Stage 1 and Stage 2 sequentially

This script orchestrates a complete import pipeline from scratch:
1. Stage 1: Download and import ALL papers from S2 dataset to dataset_all_papers
2. Stage 2: Filter conference papers to dataset_papers table

Features:
- Automatic sequential execution of both stages
- Downloads fresh data from Semantic Scholar
- Clears existing data and starts from scratch
- Comprehensive error handling
- Total execution time tracking
- Configurable resource usage

Usage:
  # Basic usage (download to default directory)
  uv run python scripts/full_import_pipeline.py --data-dir downloads/

  # Conservative mode (minimal resources)
  uv run python scripts/full_import_pipeline.py --data-dir downloads/ --conservative

  # Custom worker configuration
  uv run python scripts/full_import_pipeline.py --data-dir downloads/ --max-workers 4

  # Skip stage 2 index rebuild (faster, but requires manual rebuild)
  uv run python scripts/full_import_pipeline.py --data-dir downloads/ --skip-stage2-rebuild
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


class FullImportPipeline:
    """
    Orchestrates the complete import pipeline
    """

    def __init__(self, args):
        self.args = args
        self.project_root = Path(__file__).parent.parent
        self.stage1_script = self.project_root / "scripts" / "import_papers_stage1_all.py"
        self.stage2_script = self.project_root / "scripts" / "import_papers_stage2_conferences.py"
        self.start_time = None
        self.stage1_time = None
        self.stage2_time = None

    def run_stage1(self) -> bool:
        """
        Execute Stage 1: Download and import all papers
        """
        print("\n" + "="*80)
        print("STAGE 1: Download and Import ALL Papers")
        print("="*80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Data directory: {self.args.data_dir}")
        print(f"Dataset: {self.args.dataset_name}")
        print()

        # Build command for stage 1
        cmd = [
            "uv", "run", "python",
            str(self.stage1_script),
            "--data-dir", self.args.data_dir,
            "--dataset-name", self.args.dataset_name,
        ]

        # Add resource control options
        if self.args.conservative:
            cmd.append("--conservative")
        elif self.args.max_workers:
            cmd.extend(["--max-workers", str(self.args.max_workers)])

        if self.args.nice_priority is not None:
            cmd.extend(["--nice-priority", str(self.args.nice_priority)])

        if self.args.chunk_size:
            cmd.extend(["--chunk-size", str(self.args.chunk_size)])

        if self.args.pipeline_depth:
            cmd.extend(["--pipeline-depth", str(self.args.pipeline_depth)])

        # Note: We do NOT add --skip-truncate or --process-only
        # This ensures fresh download and clean import

        print(f"Executing: {' '.join(cmd)}\n")

        stage1_start = datetime.now()

        try:
            result = subprocess.run(cmd, check=True)
            self.stage1_time = (datetime.now() - stage1_start).total_seconds()

            print("\n" + "="*80)
            print("✅ STAGE 1 COMPLETED SUCCESSFULLY")
            print("="*80)
            print(f"Stage 1 time: {self.stage1_time:.2f}s ({self.stage1_time/60:.2f} minutes, {self.stage1_time/3600:.2f} hours)")

            return True

        except subprocess.CalledProcessError as e:
            self.stage1_time = (datetime.now() - stage1_start).total_seconds()
            print("\n" + "="*80)
            print("❌ STAGE 1 FAILED")
            print("="*80)
            print(f"Error code: {e.returncode}")
            print(f"Stage 1 time: {self.stage1_time:.2f}s ({self.stage1_time/60:.2f} minutes)")
            return False

        except KeyboardInterrupt:
            self.stage1_time = (datetime.now() - stage1_start).total_seconds()
            print("\n\n" + "="*80)
            print("⚠️  STAGE 1 INTERRUPTED BY USER")
            print("="*80)
            raise

    def run_stage2(self) -> bool:
        """
        Execute Stage 2: Filter conference papers
        """
        print("\n" + "="*80)
        print("STAGE 2: Filter Conference Papers")
        print("="*80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Build command for stage 2
        cmd = [
            "uv", "run", "python",
            str(self.stage2_script),
        ]

        # Add stage 2 options
        if self.args.stage2_batch_size:
            cmd.extend(["--batch-size", str(self.args.stage2_batch_size)])

        if self.args.skip_stage2_rebuild:
            cmd.append("--skip-rebuild")

        if self.args.keep_stage2_indexes:
            cmd.append("--keep-indexes")

        print(f"Executing: {' '.join(cmd)}\n")

        stage2_start = datetime.now()

        try:
            result = subprocess.run(cmd, check=True)
            self.stage2_time = (datetime.now() - stage2_start).total_seconds()

            print("\n" + "="*80)
            print("✅ STAGE 2 COMPLETED SUCCESSFULLY")
            print("="*80)
            print(f"Stage 2 time: {self.stage2_time:.2f}s ({self.stage2_time/60:.2f} minutes, {self.stage2_time/3600:.2f} hours)")

            return True

        except subprocess.CalledProcessError as e:
            self.stage2_time = (datetime.now() - stage2_start).total_seconds()
            print("\n" + "="*80)
            print("❌ STAGE 2 FAILED")
            print("="*80)
            print(f"Error code: {e.returncode}")
            print(f"Stage 2 time: {self.stage2_time:.2f}s ({self.stage2_time/60:.2f} minutes)")
            return False

        except KeyboardInterrupt:
            self.stage2_time = (datetime.now() - stage2_start).total_seconds()
            print("\n\n" + "="*80)
            print("⚠️  STAGE 2 INTERRUPTED BY USER")
            print("="*80)
            raise

    def print_summary(self, success: bool):
        """
        Print final summary
        """
        total_time = (datetime.now() - self.start_time).total_seconds()

        print("\n" + "="*80)
        print("FULL IMPORT PIPELINE SUMMARY")
        print("="*80)

        if success:
            print("Status: ✅ SUCCESS")
        else:
            print("Status: ❌ FAILED")

        print()

        if self.stage1_time:
            print(f"Stage 1 time: {self.stage1_time:.2f}s ({self.stage1_time/60:.2f} min, {self.stage1_time/3600:.2f} hours)")
        else:
            print("Stage 1 time: Not completed")

        if self.stage2_time:
            print(f"Stage 2 time: {self.stage2_time:.2f}s ({self.stage2_time/60:.2f} min, {self.stage2_time/3600:.2f} hours)")
        else:
            print("Stage 2 time: Not completed")

        print(f"Total time:   {total_time:.2f}s ({total_time/60:.2f} min, {total_time/3600:.2f} hours)")

        print("="*80)

        if success:
            print("\n🎉 Full import pipeline completed successfully!")
            print("\nNext steps:")
            print("  - Verify data: SELECT COUNT(*) FROM dataset_papers;")
            print("  - Check conferences: SELECT conference, COUNT(*) FROM dataset_papers GROUP BY conference;")
        else:
            print("\n⚠️  Pipeline failed. Check error messages above for details.")

    def run(self) -> int:
        """
        Execute the full pipeline
        """
        self.start_time = datetime.now()

        print("="*80)
        print("FULL IMPORT PIPELINE")
        print("="*80)
        print(f"Started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        print("This pipeline will:")
        print("  1. Download fresh S2 dataset (if not using --process-only)")
        print("  2. Clear and reimport ALL papers to dataset_all_papers")
        print("  3. Filter and populate conference papers to dataset_papers")
        print()
        print("⚠️  WARNING: This will DELETE existing data in both tables!")
        print("="*80)

        try:
            # Execute Stage 1
            if not self.run_stage1():
                self.print_summary(success=False)
                return 1

            # Execute Stage 2
            if not self.run_stage2():
                self.print_summary(success=False)
                return 1

            # Success
            self.print_summary(success=True)
            return 0

        except KeyboardInterrupt:
            print("\n\n⚠️  Pipeline interrupted by user (Ctrl+C)")
            self.print_summary(success=False)
            return 130

        except Exception as e:
            print(f"\n\n❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            self.print_summary(success=False)
            return 1


def main():
    """
    Main entry point
    """
    parser = argparse.ArgumentParser(
        description='Full Import Pipeline: Execute Stage 1 and Stage 2 sequentially',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Full Import Pipeline: Complete Data Import from Scratch

This script orchestrates a complete import pipeline that:
1. Downloads fresh data from Semantic Scholar
2. Imports ALL papers (200M records) to dataset_all_papers
3. Filters conference papers to dataset_papers

⚠️  WARNING: This will DELETE all existing data in both tables!

Total Expected Time:
- Stage 1 (download + import all papers): 3-4 hours
- Stage 2 (filter conferences): 2-3 hours
- Total: ~5-7 hours

Examples:
  # Basic usage (recommended)
  %(prog)s --data-dir downloads/

  # Conservative mode (minimal resource usage, slower)
  %(prog)s --data-dir downloads/ --conservative

  # Custom worker count (2-4 recommended)
  %(prog)s --data-dir downloads/ --max-workers 4

  # Skip Stage 2 index rebuild (saves 30-70 minutes, but requires manual rebuild)
  %(prog)s --data-dir downloads/ --skip-stage2-rebuild

  # Custom dataset
  %(prog)s --data-dir /path/to/data --dataset-name abstracts
        """
    )

    # Required arguments
    parser.add_argument(
        '--data-dir',
        required=True,
        help='Data directory for downloaded files (e.g., downloads/)'
    )

    # Stage 1 options
    stage1_group = parser.add_argument_group('Stage 1 Options (All Papers Import)')

    stage1_group.add_argument(
        '--dataset-name',
        default='papers',
        help='S2 dataset name (default: papers, options: papers, abstracts, etc.)'
    )

    stage1_group.add_argument(
        '--conservative',
        action='store_true',
        help='Conservative mode: minimal resources (1 worker, lower priority). Prevents SSH issues.'
    )

    stage1_group.add_argument(
        '--max-workers',
        type=int,
        help='Maximum parallel workers (default: auto = 25%% of CPU cores, max 8)'
    )

    stage1_group.add_argument(
        '--nice-priority',
        type=int,
        help='Process nice priority (0-19, higher = lower priority, default: 10)'
    )

    stage1_group.add_argument(
        '--chunk-size',
        type=int,
        help='Papers per chunk (default: 200000)'
    )

    stage1_group.add_argument(
        '--pipeline-depth',
        type=int,
        help='Async pipeline queue depth (default: auto = workers * 2)'
    )

    # Stage 2 options
    stage2_group = parser.add_argument_group('Stage 2 Options (Conference Filtering)')

    stage2_group.add_argument(
        '--stage2-batch-size',
        type=int,
        help='Batch size for Stage 2 processing (default: 10000)'
    )

    stage2_group.add_argument(
        '--skip-stage2-rebuild',
        action='store_true',
        help='Skip index rebuild after Stage 2 (saves 30-70 min, but requires manual rebuild)'
    )

    stage2_group.add_argument(
        '--keep-stage2-indexes',
        action='store_true',
        help='Keep indexes during Stage 2 insert (slower, for small updates)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.conservative and args.max_workers:
        print("Error: Cannot specify both --conservative and --max-workers")
        return 1

    if args.skip_stage2_rebuild and args.keep_stage2_indexes:
        print("Error: Cannot specify both --skip-stage2-rebuild and --keep-stage2-indexes")
        return 1

    if args.max_workers and (args.max_workers < 1 or args.max_workers > 32):
        print("Error: --max-workers must be between 1 and 32")
        return 1

    if args.nice_priority is not None and (args.nice_priority < 0 or args.nice_priority > 19):
        print("Error: --nice-priority must be between 0 and 19")
        return 1

    # Create and run pipeline
    pipeline = FullImportPipeline(args)
    return pipeline.run()


if __name__ == '__main__':
    sys.exit(main())
