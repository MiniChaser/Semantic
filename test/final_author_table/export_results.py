#!/usr/bin/env python3
"""
Results Export Module

Handles exporting verification results to various formats (CSV, JSON, Excel).
Separated from the main verification logic for better modularity.
"""

import os
import csv
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Any, List
from datetime import datetime


class ResultExporter:
    """
    Handles exporting verification results to various formats (CSV, JSON, Excel)
    """

    def __init__(self):
        self.logger = logging.getLogger(f'{__name__}.ResultExporter')

    def _ensure_results_directory(self) -> Path:
        """
        Ensure results directory exists and return its path

        Returns:
            Path to results directory
        """
        results_dir = Path(__file__).parent / 'results'
        try:
            results_dir.mkdir(exist_ok=True)
            return results_dir
        except Exception as e:
            self.logger.error(f"Failed to create results directory: {e}")
            # Fallback to current directory
            return Path(__file__).parent

    def export_to_csv(self, report: 'VerificationReport') -> Optional[str]:
        """
        Export detailed test results to CSV file

        Args:
            report: VerificationReport with test results

        Returns:
            Path to saved CSV file or None if failed
        """
        try:
            results_dir = self._ensure_results_directory()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"verification_results_{timestamp}.csv"
            csv_path = results_dir / csv_filename

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['dblp_author_name', 'field_name', 'expected_value', 'actual_value', 'is_match', 'error_message']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for result in report.test_results:
                    writer.writerow({
                        'dblp_author_name': result.author_name,
                        'field_name': result.field_name,
                        'expected_value': result.expected_value,
                        'actual_value': result.actual_value,
                        'is_match': result.passed,
                        'error_message': result.error_message or ''
                    })

            return str(csv_path)

        except Exception as e:
            self.logger.error(f"Failed to export CSV results: {e}")
            return None

    def export_to_excel(self, report: 'VerificationReport') -> Optional[str]:
        """
        Export detailed test results to Excel file

        Args:
            report: VerificationReport with test results

        Returns:
            Path to saved Excel file or None if failed
        """
        try:
            # Try to import pandas for Excel export
            try:
                import pandas as pd
            except ImportError:
                # Fallback to CSV export
                return self.export_to_csv(report)

            results_dir = self._ensure_results_directory()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_filename = f"verification_results_{timestamp}.xlsx"
            excel_path = results_dir / excel_filename

            # Prepare data for pandas DataFrame
            data = []
            for result in report.test_results:
                data.append({
                    'dblp_author_name': result.author_name,
                    'field_name': result.field_name,
                    'expected_value': result.expected_value,
                    'actual_value': result.actual_value,
                    'is_match': result.passed,
                    'error_message': result.error_message or ''
                })

            # Create DataFrame and export to Excel
            df = pd.DataFrame(data)
            df.to_excel(excel_path, index=False, sheet_name='Verification Results')

            return str(excel_path)

        except Exception as e:
            self.logger.error(f"Failed to export Excel results: {e}")
            # Fallback to CSV if Excel export fails
            return self.export_to_csv(report)

    def export_to_json(self, report: 'VerificationReport') -> Optional[str]:
        """
        Export summary statistics to JSON file

        Args:
            report: VerificationReport with statistics

        Returns:
            Path to saved JSON file or None if failed
        """
        try:
            results_dir = self._ensure_results_directory()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"verification_summary_{timestamp}.json"
            json_path = results_dir / json_filename

            # Calculate field-level summary
            field_summary = {}
            for result in report.test_results:
                field_name = result.field_name
                if field_name not in field_summary:
                    field_summary[field_name] = {'total': 0, 'passed': 0, 'failed': 0}

                field_summary[field_name]['total'] += 1
                if result.passed:
                    field_summary[field_name]['passed'] += 1
                else:
                    field_summary[field_name]['failed'] += 1

            # Add pass rates to field summary
            for field_name, stats in field_summary.items():
                stats['pass_rate'] = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0.0

            summary_data = {
                'test_timestamp': datetime.now().isoformat(),
                'overall_summary': {
                    'total_tests': report.total_tests,
                    'passed_tests': report.passed_tests,
                    'failed_tests': report.failed_tests,
                    'pass_rate': report.pass_rate
                },
                'field_summary': field_summary,
                'test_configuration': {
                    'sample_size': report.total_tests,
                    'fields_tested': list(field_summary.keys())
                }
            }

            with open(json_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(summary_data, jsonfile, indent=2, ensure_ascii=False)

            return str(json_path)

        except Exception as e:
            self.logger.error(f"Failed to export JSON summary: {e}")
            return None

    def export_all_formats(self, report: 'VerificationReport') -> Dict[str, Optional[str]]:
        """
        Export results to all available formats

        Args:
            report: VerificationReport with results

        Returns:
            Dictionary with format names and file paths
        """
        return {
            'excel': self.export_to_excel(report),
            'json': self.export_to_json(report)
        }