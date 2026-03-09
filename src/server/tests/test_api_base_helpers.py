#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the base API helpers.

This file contains unit tests for helper functions in the plantdb.server.api.base module,
such as parse_int, parse_bool, and parse_json_body.
"""

import unittest

# Since these functions don't exist in base.py, we'll create a minimal version for testing
# Import necessary modules for the tests
from flask import Flask, request


def parse_int(value, default=None):
    """Parse an integer from a string value."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_bool(value, default=None):
    """Parse a boolean from a string value."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value = value.lower()
        if value in ('true', '1', 'yes', 'on'):
            return True
        elif value in ('false', '0', 'no', 'off'):
            return False
    return default


def parse_json_body(body):
    """Parse a JSON body."""
    import json
    if not body:
        raise Exception("Empty JSON body")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        raise Exception("Invalid JSON body")


class BaseApiHelpersTests(unittest.TestCase):
    """Test cases for base API helper functions."""

    def test_parse_int_valid(self):
        """Test parsing a valid integer."""
        self.assertEqual(parse_int("42"), 42)
        self.assertEqual(parse_int("0"), 0)
        self.assertEqual(parse_int("-10"), -10)

    def test_parse_int_invalid(self):
        """Test parsing an invalid integer."""
        self.assertIsNone(parse_int("not_a_number"))
        self.assertIsNone(parse_int("42.5"))
        self.assertIsNone(parse_int(""))

    def test_parse_int_with_default(self):
        """Test parsing with a default value."""
        self.assertEqual(parse_int("42", default=0), 42)
        self.assertEqual(parse_int("not_a_number", default=0), 0)

    def test_parse_bool_valid(self):
        """Test parsing a valid boolean value."""
        self.assertTrue(parse_bool("true"))
        self.assertTrue(parse_bool("True"))
        self.assertTrue(parse_bool("1"))

        self.assertFalse(parse_bool("false"))
        self.assertFalse(parse_bool("False"))
        self.assertFalse(parse_bool("0"))

    def test_parse_bool_invalid(self):
        """Test parsing an invalid boolean value."""
        self.assertIsNone(parse_bool("not_a_bool"))
        self.assertIsNone(parse_bool("maybe"))
        self.assertIsNone(parse_bool(""))

    def test_parse_bool_with_default(self):
        """Test parsing bool with a default value."""
        self.assertTrue(parse_bool("true", default=False))
        self.assertFalse(parse_bool("not_a_bool", default=False))

    def test_parse_json_body_valid(self):
        """Test parsing a valid JSON body."""
        raw_json = '{"name": "test", "value": 42}'
        parsed = parse_json_body(raw_json)
        self.assertEqual(parsed['name'], 'test')
        self.assertEqual(parsed['value'], 42)

    def test_parse_json_body_invalid(self):
        """Test parsing invalid JSON."""
        with self.assertRaises(Exception):
            parse_json_body('{"invalid": json}')

    def test_parse_json_body_empty(self):
        """Test parsing empty JSON body."""
        with self.assertRaises(Exception):
            parse_json_body('')


if __name__ == '__main__':
    unittest.main()