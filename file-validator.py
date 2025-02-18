#!/usr/bin/env python3
"""
File Data Validator

This script validates the contents of a file against user-provided 
specifications.
It supports various data formats and validation rules.
"""

import argparse
import csv
from datetime import datetime
import json
import math
import re
import sys
from typing import Any, Callable, Dict, List, Union


class Validator:
    """Main validator class that handles file reading and validation."""

    def __init__(self, specs: Dict[str, Any]):
        """Initialize validator with provided specifications.
        
        Args:
            specs: Dictionary containing validation specifications
        """
        self.specs = specs
        self.format = specs.get('format', 'csv').lower()
        self.delimiter = specs.get('delimiter', ',')
        self.has_headers = specs.get('has_headers', True)
        self.columns = specs.get('columns', [])
        self.required_fields = [
            col['name'] for col in self.columns if col.get('required', False)
        ]
        self.validation_rules = self._compile_validation_rules()

    def _compile_validation_rules(self) -> Dict[str, List[Callable]]:
        """Compile validation rules for each column."""
        rules = {}
        for col in self.columns:
            col_rules = []
            
            # Type validation
            if 'type' in col:
                if col['type'] == 'int':
                    col_rules.append(lambda x, col=col: self._validate_int(x))
                elif col['type'] == 'float':
                    col_rules.append(lambda x, col=col: self._validate_float(x))
                elif col['type'] == 'bool':
                    col_rules.append(lambda x, col=col: self._validate_bool(x))
                elif col['type'] == 'date':
                    date_format = col.get('format', '%Y-%m-%d')
                    col_rules.append(
                        lambda x, fmt=date_format: self._validate_date(x, fmt)
                    )
                elif col['type'] == 'double':
                    col_rules.append(lambda x, col=col: self._validate_double(x))
                elif col['type'] == 'timestamp':
                    date_format = col.get('format', '%Y-%m-%d %H:%M:%S')
                    col_rules.append(
                        lambda x, fmt=date_format: self._validate_timestamp(x, fmt)
                    )
                    
            # Range validation
            if 'min' in col or 'max' in col:
                col_rules.append(lambda x, col=col: self._validate_range(x, col))
                
            # Regex pattern validation
            if 'pattern' in col:
                pattern = col['pattern']
                col_rules.append(lambda x, p=pattern: self._validate_pattern(x, p))
                
            # Allowed values validation
            if 'allowed_values' in col:
                allowed = col['allowed_values']
                col_rules.append(
                    lambda x, a=allowed: self._validate_allowed_values(x, a)
                )
                
            rules[col['name']] = col_rules
            
        return rules

    def validate_file(self, filepath: str) -> Dict[str, Any]:
        """Validate the given file according to specs.
        
        Args:
            filepath: Path to the file to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            if self.format == 'csv':
                return self._validate_csv(filepath)
            elif self.format == 'json':
                return self._validate_json(filepath)
            else:
                return {'valid': False, 'errors': [f"Unsupported format: {self.format}"]}
        except Exception as e:
            return {'valid': False, 'errors': [f"Validation error: {str(e)}"]}

    def _validate_csv(self, filepath: str) -> Dict[str, Any]:
        """Validate CSV file."""
        errors = []
        row_count = 0
        
        with open(filepath, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=self.delimiter)
            
            # Process headers if present
            if self.has_headers:
                headers = next(reader, None)
                if not headers:
                    return {'valid': False, 'errors': ["Empty file or couldn't read headers"]}
                    
                # Validate that all required columns are present
                for field in self.required_fields:
                    if field not in headers:
                        errors.append(f"Required column '{field}' is missing")
                
                # Create index mapping from column name to position
                col_indices = {col_name: idx for idx, col_name in enumerate(headers)}
            else:
                # If no headers, columns are identified by position
                col_indices = {col['name']: idx for idx, col in enumerate(self.columns)}
            
            # Process data rows
            for row_idx, row in enumerate(reader, start=1 if self.has_headers else 0):
                row_count += 1
                
                # Check for row length
                if len(row) < len(self.required_fields):
                    errors.append(
                        f"Row {row_idx}: Has fewer fields than required "
                        f"({len(row)} vs {len(self.required_fields)})"
                    )
                    continue
                    
                # Validate each column
                for col_name, rules in self.validation_rules.items():
                    if col_name in col_indices and col_indices[col_name] < len(row):
                        value = row[col_indices[col_name]]
                        col_errors = self._apply_rules(col_name, value, rules)
                        errors.extend(
                            [f"Row {row_idx}, Column '{col_name}': {err}" for err in col_errors]
                        )
                    elif col_name in self.required_fields:
                        errors.append(
                            f"Row {row_idx}: Required column '{col_name}' is missing or row is incomplete"
                        )
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'row_count': row_count
        }

    def _validate_json(self, filepath: str) -> Dict[str, Any]:
        """Validate JSON file."""
        errors = []
        
        with open(filepath, 'r') as jsonfile:
            try:
                data = json.load(jsonfile)
            except json.JSONDecodeError as e:
                return {'valid': False, 'errors': [f"Invalid JSON: {str(e)}"]}
        
        # Handle different JSON structures
        if isinstance(data, list):
            # List of objects
            for idx, item in enumerate(data):
                if not isinstance(item, dict):
                    errors.append(
                        f"Item {idx}: Expected an object, got {type(item).__name__}"
                    )
                    continue
                
                # Check required fields
                for field in self.required_fields:
                    if field not in item:
                        errors.append(f"Item {idx}: Required field '{field}' is missing")
                
                # Validate each field
                for field_name, rules in self.validation_rules.items():
                    if field_name in item:
                        value = item[field_name]
                        field_errors = self._apply_rules(field_name, value, rules)
                        errors.extend(
                            [f"Item {idx}, Field '{field_name}': {err}" for err in field_errors]
                        )
            
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'item_count': len(data)
            }
        
        elif isinstance(data, dict):
            # Single object
            for field in self.required_fields:
                if field not in data:
                    errors.append(f"Required field '{field}' is missing")
            
            # Validate each field
            for field_name, rules in self.validation_rules.items():
                if field_name in data:
                    value = data[field_name]
                    field_errors = self._apply_rules(field_name, value, rules)
                    errors.extend(
                        [f"Field '{field_name}': {err}" for err in field_errors]
                    )
            
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'item_count': 1
            }
        
        else:
            return {
                'valid': False,
                'errors': [f"Expected JSON array or object, got {type(data).__name__}"]
            }

    def _apply_rules(self, field_name: str, value: Any, rules: List[Callable]) -> List[str]:
        """Apply all validation rules for a field."""
        errors = []
        for rule in rules:
            result = rule(value)
            if not result['valid']:
                errors.append(result['error'])
        return errors

    # Validation helper methods
    def _validate_int(self, value: str) -> Dict[str, Any]:
        """Validate if the value is an integer."""
        try:
            int(value)
            return {'valid': True}
        except (ValueError, TypeError):
            return {'valid': False, 'error': f"'{value}' is not a valid integer"}

    def _validate_float(self, value: str) -> Dict[str, Any]:
        """Validate if the value is a float."""
        try:
            float(value)
            return {'valid': True}
        except (ValueError, TypeError):
            return {'valid': False, 'error': f"'{value}' is not a valid float"}

    def _validate_bool(self, value: str) -> Dict[str, Any]:
        """Validate if the value is a boolean."""
        lower_value = str(value).lower()
        if lower_value in ('true', 'false', '1', '0', 'yes', 'no', 'y', 'n'):
            return {'valid': True}
        return {'valid': False, 'error': f"'{value}' is not a valid boolean"}

    def _validate_date(self, value: str, format_str: str) -> Dict[str, Any]:
        """Validate if the value matches the date format."""
        from datetime import datetime
        try:
            datetime.strptime(value, format_str)
            return {'valid': True}
        except ValueError:
            return {
                'valid': False,
                'error': f"'{value}' does not match date format '{format_str}'"
            }

    def _validate_range(self, value: str, col_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if the value is within the specified range."""
        try:
            if col_spec.get('type') == 'int':
                num_value = int(value)
            else:
                num_value = float(value)
                
            min_val = col_spec.get('min')
            max_val = col_spec.get('max')
            
            if min_val is not None and num_value < min_val:
                return {
                    'valid': False,
                    'error': f"'{value}' is less than minimum value {min_val}"
                }
            if max_val is not None and num_value > max_val:
                return {
                    'valid': False,
                    'error': f"'{value}' is greater than maximum value {max_val}"
                }
                
            return {'valid': True}
        except (ValueError, TypeError):
            return {
                'valid': False,
                'error': f"'{value}' cannot be evaluated for range validation"
            }

    def _validate_pattern(self, value: str, pattern: str) -> Dict[str, Any]:
        """Validate if the value matches the regex pattern."""
        try:
            if re.match(pattern, str(value)):
                return {'valid': True}
            return {
                'valid': False,
                'error': f"'{value}' does not match pattern '{pattern}'"
            }
        except re.error:
            return {'valid': False, 'error': f"Invalid regex pattern: '{pattern}'"}

    def _validate_allowed_values(self, value: Any, allowed: List[Any]) -> Dict[str, Any]:
        """Validate if the value is in the list of allowed values."""
        if value in allowed:
            return {'valid': True}
        return {
            'valid': False,
            'error': f"'{value}' is not in allowed values: {allowed}"
        }

    def _validate_double(self, value: str) -> Dict[str, Any]:
        """Validate if the value is a double/float with proper decimal places."""
        try:
            float_val = float(value)
            # Check if it's not infinity or NaN
            if not math.isfinite(float_val):
                return {
                    'valid': False,
                    'error': f"'{value}' is not a valid finite number"
                }
            return {'valid': True}
        except (ValueError, TypeError):
            return {'valid': False, 'error': f"'{value}' is not a valid double"}

    def _validate_timestamp(self, value: str, format_str: str) -> Dict[str, Any]:
        """Validate if the value is a valid timestamp."""
        try:
            timestamp = datetime.strptime(value, format_str)
            return {'valid': True}
        except ValueError:
            return {
                'valid': False,
                'error': f"'{value}' does not match timestamp format '{format_str}'"
            }


def load_specs(specs_file: str) -> Dict:
    """Load validation specifications from a JSON file."""
    try:
        with open(specs_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error parsing specifications: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"Specifications file not found: {specs_file}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main function to parse arguments and validate the file."""
    parser = argparse.ArgumentParser(
        description='Validate file data against specifications'
    )
    parser.add_argument(
        'specs_file', help='Path to JSON file containing validation specifications'
    )
    parser.add_argument(
        'data_file', help='Path to the data file to validate'
    )
    parser.add_argument(
        '--output', '-o', help='Output validation results to file'
    )
    args = parser.parse_args()

    specs = load_specs(args.specs_file)
    validator = Validator(specs)
    results = validator.validate_file(args.data_file)
    
    # Output results
    output = {
        'valid': results['valid'],
        'data_file': args.data_file,
        'specs_file': args.specs_file,
        'errors': results['errors'] if not results['valid'] else [],
        'stats': {
            'total_rows' if 'row_count' in results else 'total_items': 
                results.get('row_count', results.get('item_count', 0)),
            'error_count': len(results.get('errors', []))
        }
    }
    
    # Print or save results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Validation results saved to {args.output}")
    else:
        print(json.dumps(output, indent=2))
    
    # Exit with appropriate status code
    sys.exit(0 if results['valid'] else 1)


if __name__ == "__main__":
    main()
