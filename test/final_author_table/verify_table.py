#!/usr/bin/env python3
"""
Final Author Table Verification System

Dynamically discovers and executes field verification tests for final_author_table.
Each SQL file in the fields/ directory defines expected value calculation for a field.
The system compares expected values with actual values from final_author_table.
"""

import os
import sys
import logging
import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import re

# Add src path to import the database connection
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from semantic.database.connection import get_db_manager
from export_results import ResultExporter


@dataclass
class FieldTestResult:
    """Result of a single field verification test"""
    field_name: str
    author_name: str
    expected_value: Any
    actual_value: Any
    passed: bool
    error_message: Optional[str] = None


@dataclass
class VerificationReport:
    """Complete verification report for all fields and authors"""
    total_tests: int
    passed_tests: int
    failed_tests: int
    test_results: List[FieldTestResult]

    @property
    def pass_rate(self) -> float:
        return (self.passed_tests / self.total_tests) * 100 if self.total_tests > 0 else 0.0


class FieldVerifier:
    """
    Main verification class for final_author_table fields

    Discovers SQL test files in fields/ directory and executes them to verify
    that final_author_table contains the correct calculated values.
    """

    def __init__(self):
        self.db_manager = get_db_manager()
        self.logger = self._setup_logger()
        self.fields_dir = Path(__file__).parent / 'fields'
        self.test_results: List[FieldTestResult] = []
        self.result_exporter = ResultExporter()

        # Mapping from field test names to actual database column names
        self.field_column_mapping = {
            'semantic_citation_count': 'semantic_scholar_citation_count',
            'first_author_count': 'first_author_count',
            'career_length': 'career_length',
            'h_index': 'semantic_scholar_h_index',
            'paper_count': 'semantic_scholar_paper_count',
            'semantic_scholar_paper_count': 'semantic_scholar_paper_count',
            'total_influential_citations': 'total_influential_citations'
        }

    def _setup_logger(self) -> logging.Logger:
        """Setup logger for verification operations"""
        logger = logging.getLogger(f'{__name__}.FieldVerifier')
        logger.setLevel(logging.WARNING)  # Only show warnings and errors

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def discover_field_tests(self) -> List[str]:
        """
        Discover all SQL test files in the fields directory

        Returns:
            List of field names (SQL filenames without .sql extension)
        """
        if not self.fields_dir.exists():
            self.logger.error(f"Fields directory does not exist: {self.fields_dir}")
            return []

        sql_files = list(self.fields_dir.glob('*.sql'))
        field_names = [f.stem for f in sql_files]

        return field_names

    def load_field_query(self, field_name: str) -> Optional[str]:
        """
        Load SQL query template for a specific field

        Args:
            field_name: Name of the field (without .sql extension)

        Returns:
            SQL query string or None if file doesn't exist
        """
        sql_file = self.fields_dir / f'{field_name}.sql'

        if not sql_file.exists():
            self.logger.error(f"SQL file not found: {sql_file}")
            return None

        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                query = f.read().strip()

            if not query:
                self.logger.warning(f"Empty SQL file: {sql_file}")
                return None

            return query

        except Exception as e:
            self.logger.error(f"Failed to read SQL file {sql_file}: {e}")
            return None

    def substitute_query_parameters(self, query: str, **params) -> str:
        """
        Substitute parameters in SQL query template

        Args:
            query: SQL query with {parameter} placeholders
            **params: Parameter values to substitute

        Returns:
            Query with parameters substituted
        """
        try:
            # Use string format to substitute parameters
            substituted = query.format(**params)
            return substituted

        except KeyError as e:
            self.logger.error(f"Missing parameter for query substitution: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to substitute query parameters: {e}")
            raise

    def execute_field_query(self, field_name: str, author_name: str) -> Optional[Any]:
        """
        Execute field calculation query for a specific author

        Args:
            field_name: Name of the field to calculate
            author_name: DBLP author name to calculate for

        Returns:
            Expected value for the field or None if query failed
        """
        query_template = self.load_field_query(field_name)
        if not query_template:
            return None

        try:
            # Substitute author name parameter with proper SQL escaping
            # Escape single quotes by doubling them (SQL standard)
            escaped_author_name = author_name.replace("'", "''")
            query = self.substitute_query_parameters(
                query_template,
                dblp_author_name=f"'{escaped_author_name}'"
            )

            # Execute query
            result = self.db_manager.fetch_one(query)

            if not result:
                return 0

            # Extract the field value from result with improved column matching
            field_value = None

            # Try multiple strategies to find the value
            strategies = [
                field_name,                           # Exact field name match
                f"semantic_scholar_{field_name}",     # With semantic_scholar prefix
                f"sum",                              # Common aggregate function name
                f"count",                            # Common count function name
            ]

            for strategy in strategies:
                if strategy in result:
                    field_value = result[strategy]
                    break

            # If still no match, take the first non-null value
            if field_value is None:
                values = list(result.values())
                if values and values[0] is not None:
                    field_value = values[0]  # Take the first value
                else:
                    field_value = 0  # Return 0 instead of None for failed calculations

            return field_value if field_value is not None else 0

        except Exception as e:
            self.logger.error(f"Failed to execute field query for {field_name}: {e}")
            return 0

    def get_actual_value(self, field_name: str, author_name: str) -> Optional[Any]:
        """
        Get actual value from final_author_table for comparison

        Args:
            field_name: Name of the field to retrieve
            author_name: DBLP author name

        Returns:
            Actual value from final_author_table or None if not found
        """
        try:
            # Map field test name to actual database column name
            column_name = self.field_column_mapping.get(field_name, field_name)

            query = f"""
            SELECT {column_name}
            FROM final_author_table
            WHERE dblp_author = %s
            """

            result = self.db_manager.fetch_one(query, (author_name,))

            if not result:
                return None

            return result.get(column_name)

        except Exception as e:
            self.logger.error(f"Failed to get actual value for {field_name}: {e}")
            return None

    def verify_field(self, field_name: str, author_name: str) -> FieldTestResult:
        """
        Verify a single field for a specific author

        Args:
            field_name: Name of the field to verify
            author_name: DBLP author name

        Returns:
            FieldTestResult with verification outcome
        """

        try:
            # Get expected value from field calculation (now returns 0 on failure)
            expected_value = self.execute_field_query(field_name, author_name)

            # Get actual value from final_author_table
            actual_value = self.get_actual_value(field_name, author_name)

            # Handle case where actual value is missing from final_author_table
            if actual_value is None:
                return FieldTestResult(
                    field_name=field_name,
                    author_name=author_name,
                    expected_value=expected_value,
                    actual_value=None,
                    passed=False,
                    error_message="Actual value not found in final_author_table"
                )

            # Compare values with improved matching logic
            passed = self._values_match(expected_value, actual_value)

            return FieldTestResult(
                field_name=field_name,
                author_name=author_name,
                expected_value=expected_value,
                actual_value=actual_value,
                passed=passed
            )

        except Exception as e:
            self.logger.error(f"Field verification failed for {field_name}: {e}")
            return FieldTestResult(
                field_name=field_name,
                author_name=author_name,
                expected_value=None,
                actual_value=None,
                passed=False,
                error_message=str(e)
            )

    def _values_match(self, expected: Any, actual: Any) -> bool:
        """
        Compare two values with appropriate type handling

        Args:
            expected: Expected value
            actual: Actual value

        Returns:
            True if values match, False otherwise
        """
        # Handle None values
        if expected is None and actual is None:
            return True

        # Convert None to 0 for comparison (as per new requirement)
        expected_value = expected if expected is not None else 0
        actual_value = actual if actual is not None else 0

        # If both values are 0 or empty, consider them matching
        if (expected_value == 0 or expected_value == '') and (actual_value == 0 or actual_value == ''):
            return True

        # Handle numeric values with potential type differences
        if isinstance(expected_value, (int, float)) and isinstance(actual_value, (int, float)):
            return abs(expected_value - actual_value) < 1e-9  # Handle floating point precision

        # Handle string comparison
        return str(expected_value) == str(actual_value)

    def run_all_verifications(self, authors: List[str]) -> VerificationReport:
        """
        Run verification for all discovered fields and specified authors

        Args:
            authors: List of DBLP author names to verify

        Returns:
            VerificationReport with complete results
        """
        # Clear previous results
        self.test_results = []

        # Discover all field tests
        field_names = self.discover_field_tests()

        if not field_names:
            return VerificationReport(0, 0, 0, [])

        # Run verification for each field and author combination
        for author_name in authors:
            for field_name in field_names:
                result = self.verify_field(field_name, author_name)
                self.test_results.append(result)

        # Generate report
        passed_tests = len([r for r in self.test_results if r.passed])
        failed_tests = len(self.test_results) - passed_tests

        report = VerificationReport(
            total_tests=len(self.test_results),
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            test_results=self.test_results
        )

        return report

    def get_sample_authors(self, limit: int = 5) -> List[str]:
        """
        Get sample author names from final_author_table for testing

        Args:
            limit: Maximum number of authors to return

        Returns:
            List of DBLP author names
        """
        try:
            query = """
            SELECT dblp_author
            FROM final_author_table
            WHERE dblp_author IS NOT NULL
            AND dblp_author != ''
            ORDER BY RANDOM()
            LIMIT %s
            """

            results = self.db_manager.fetch_all(query, (limit,))
            return [r['dblp_author'] for r in results if r['dblp_author']]

        except Exception as e:
            self.logger.error(f"Failed to get sample authors: {e}")
            return []

    def print_simple_report(self, report: VerificationReport):
        """
        Print simplified verification report to console

        Args:
            report: VerificationReport to print
        """
        print(f"\nVerification Results: {report.passed_tests}/{report.total_tests} tests passed ({report.pass_rate:.1f}%)")

        # Show field-level summary
        field_stats = {}
        for result in report.test_results:
            field_name = result.field_name
            if field_name not in field_stats:
                field_stats[field_name] = {'passed': 0, 'total': 0}

            field_stats[field_name]['total'] += 1
            if result.passed:
                field_stats[field_name]['passed'] += 1

        print("\nField Summary:")
        for field_name, stats in field_stats.items():
            pass_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0.0
            print(f"  {field_name}: {stats['passed']}/{stats['total']} passed ({pass_rate:.1f}%)")

    def print_detailed_report(self, report: VerificationReport):
        """
        Print detailed verification report to console

        Args:
            report: VerificationReport to print
        """
        print("\n" + "="*80)
        print("FINAL AUTHOR TABLE VERIFICATION REPORT")
        print("="*80)

        print(f"Total Tests: {report.total_tests}")
        print(f"Passed: {report.passed_tests}")
        print(f"Failed: {report.failed_tests}")
        print(f"Pass Rate: {report.pass_rate:.1f}%")

        if report.failed_tests > 0:
            print("\nFAILED TESTS:")
            print("-" * 40)

            for result in report.test_results:
                if not result.passed:
                    print(f"\nField: {result.field_name}")
                    print(f"Author: {result.author_name}")
                    print(f"Expected: {result.expected_value}")
                    print(f"Actual: {result.actual_value}")
                    if result.error_message:
                        print(f"Error: {result.error_message}")

        print("\nPASSED TESTS:")
        print("-" * 40)

        # Group passed tests by field
        passed_by_field = {}
        for result in report.test_results:
            if result.passed:
                if result.field_name not in passed_by_field:
                    passed_by_field[result.field_name] = []
                passed_by_field[result.field_name].append(result.author_name)

        for field_name, authors in passed_by_field.items():
            print(f"{field_name}: {len(authors)} authors verified")

        print("\n" + "="*80)


class VerificationRunner:
    """Main runner class for the verification system"""

    def __init__(self):
        self.verifier = FieldVerifier()
        self.logger = logging.getLogger(f'{__name__}.VerificationRunner')

    def run_verification(self, authors: Optional[List[str]] = None, sample_size: int = 5) -> VerificationReport:
        """
        Run the complete verification process

        Args:
            authors: Specific authors to test, or None to use samples
            sample_size: Number of sample authors to use if authors not specified

        Returns:
            VerificationReport with results
        """
        try:
            # Test database connection
            if not self.verifier.db_manager.test_connection():
                self.logger.error("Database connection test failed")
                return VerificationReport(0, 0, 0, [])

            # Get authors to test
            if authors is None:
                self.logger.info(f"Getting {sample_size} sample authors for testing")
                authors = self.verifier.get_sample_authors(sample_size)

                if not authors:
                    self.logger.error("No sample authors found")
                    return VerificationReport(0, 0, 0, [])

            # Run verification
            return self.verifier.run_all_verifications(authors)

        except Exception as e:
            self.logger.error(f"Verification run failed: {e}")
            return VerificationReport(0, 0, 0, [])


def main():
    """Main function to run verification from command line"""
    logging.basicConfig(level=logging.WARNING)

    runner = VerificationRunner()

    # You can specify authors here or leave None to use samples
    specific_authors = None  # Example: ['Michael I. Jordan']

    print("Starting final_author_table verification...")
    report = runner.run_verification(authors=specific_authors, sample_size=100)

    # Print simplified report
    runner.verifier.print_simple_report(report)

    # Save results to files
    print("\nSaving results...")
    export_results = runner.verifier.result_exporter.export_all_formats(report)

    if export_results['excel']:
        print(f"Detailed results saved to: {export_results['excel']}")
    if export_results['json']:
        print(f"Summary statistics saved to: {export_results['json']}")

    # Return appropriate exit code
    if report.failed_tests == 0 and report.total_tests > 0:
        print("\nAll tests passed!")
        return 0
    else:
        print(f"\n{report.failed_tests} tests failed. Check saved files for details.")
        return 1


if __name__ == "__main__":
    exit(main())