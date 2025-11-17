"""
SQL Injection Prevention Utilities

This module provides validation functions to prevent SQL injection attacks
in database adapters. All user-controllable inputs should be validated
before being used in SQL query construction.

Security Note: While parameterized queries (%s placeholders) protect against
injection in VALUES, identifiers (table names, column names) and SQL keywords
(ASC/DESC, LIMIT/OFFSET) must be validated separately since they cannot be
parameterized in most database drivers.
"""

import re
from typing import Any, List, Tuple


class SqlValidator:
    """
    SQL injection prevention utilities for validating query components.

    This class provides static methods to validate various parts of SQL queries
    that cannot be safely parameterized, including:
    - Table names and column names (identifiers)
    - LIMIT and OFFSET values (must be integers)
    - ORDER BY directions (must be ASC or DESC)
    - Field expressions in SELECT clauses
    - JOIN statements

    All methods raise ValueError with descriptive messages when validation fails.
    """

    # SQL keywords that should never appear in identifiers
    DANGEROUS_KEYWORDS = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'UNION', 'WHERE', 'FROM', 'JOIN', 'EXEC', 'EXECUTE', 'SCRIPT',
        'TRUNCATE', 'GRANT', 'REVOKE', '--', '/*', '*/', ';', 'XP_'
    }

    @staticmethod
    def validate_identifier(name: str, context: str = "identifier") -> str:
        """
        Validate SQL identifier (table name, column name, alias, etc.)

        Rules:
        - Must be a string
        - Must start with letter or underscore
        - Can only contain letters, numbers, and underscores
        - Maximum 64 characters (database limit)
        - Cannot contain SQL keywords or special characters

        Args:
            name: The identifier to validate
            context: Description of what this identifier represents (for error messages)

        Returns:
            The validated identifier (unchanged)

        Raises:
            ValueError: If validation fails

        Examples:
            >>> SqlValidator.validate_identifier("users", "table name")
            'users'
            >>> SqlValidator.validate_identifier("user_id", "column name")
            'user_id'
            >>> SqlValidator.validate_identifier("user-name", "column")
            ValueError: Invalid column: 'user-name'
        """
        if not isinstance(name, str):
            raise ValueError(
                f"Invalid {context}: must be a string, got {type(name).__name__}"
            )

        # Check for empty string
        if not name:
            raise ValueError(f"Invalid {context}: cannot be empty")

        # Check pattern: must start with letter/underscore, contain only alphanumeric/underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(
                f"Invalid {context}: '{name}'. "
                f"Must start with letter or underscore and contain only "
                f"alphanumeric characters and underscores."
            )

        # Check length (most databases limit identifiers to 64 chars)
        if len(name) > 64:
            raise ValueError(
                f"Invalid {context}: '{name}' exceeds 64 character limit"
            )

        # Check for dangerous SQL keywords (case-insensitive)
        # Only match whole words/segments, not substrings (e.g., "created_at" is OK, "drop_table" is not)
        name_upper = name.upper()

        # Split by underscore to check each segment
        # This catches "users_DROP_table" and "UNION_SELECT" but allows "created_at"
        segments = name_upper.split('_')

        # Separate punctuation-based keywords (need substring check) from word-based keywords
        punctuation_keywords = {'--', '/*', '*/', ';'}
        word_keywords = SqlValidator.DANGEROUS_KEYWORDS - punctuation_keywords

        # Check for punctuation-based SQL injection patterns (as substrings)
        for keyword in punctuation_keywords:
            if keyword in name:  # Use original name to preserve special chars
                raise ValueError(
                    f"Invalid {context}: '{name}' contains dangerous SQL pattern '{keyword}'"
                )

        # Check for word-based SQL keywords (as complete segments)
        for keyword in word_keywords:
            # Check if keyword is the entire identifier
            if name_upper == keyword:
                raise ValueError(
                    f"Invalid {context}: '{name}' is a SQL keyword '{keyword}'"
                )
            # Check if keyword appears as a complete segment (between underscores)
            if keyword in segments:
                raise ValueError(
                    f"Invalid {context}: '{name}' contains SQL keyword '{keyword}'"
                )

        return name

    @staticmethod
    def validate_integer(
        value: Any,
        context: str = "value",
        min_val: int = None,
        max_val: int = None,
        allow_none: bool = False
    ) -> int:
        """
        Validate and convert to integer with optional range check.

        This prevents injection attacks in LIMIT, OFFSET, and other numeric contexts
        by ensuring the value is actually an integer, not a string containing SQL code.

        Args:
            value: The value to validate and convert
            context: Description of what this value represents (for error messages)
            min_val: Minimum allowed value (inclusive), or None for no minimum
            max_val: Maximum allowed value (inclusive), or None for no maximum
            allow_none: If True, None values are allowed and returned as-is

        Returns:
            The validated integer value, or None if value is None and allow_none=True

        Raises:
            ValueError: If validation fails

        Examples:
            >>> SqlValidator.validate_integer(10, "limit")
            10
            >>> SqlValidator.validate_integer("20", "offset", min_val=0)
            20
            >>> SqlValidator.validate_integer("10; DROP TABLE", "limit")
            ValueError: Invalid limit: must be an integer
            >>> SqlValidator.validate_integer(1000000, "limit", max_val=10000)
            ValueError: Invalid limit: 1000000 exceeds maximum 10000
        """
        # Allow None if specified
        if value is None:
            if allow_none:
                return None
            else:
                raise ValueError(f"Invalid {context}: cannot be None")

        # Convert to integer (will raise ValueError if not convertible)
        try:
            int_value = int(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid {context}: must be an integer, got {type(value).__name__} '{value}'"
            )

        # Check minimum value
        if min_val is not None and int_value < min_val:
            raise ValueError(
                f"Invalid {context}: {int_value} is less than minimum {min_val}"
            )

        # Check maximum value
        if max_val is not None and int_value > max_val:
            raise ValueError(
                f"Invalid {context}: {int_value} exceeds maximum {max_val}"
            )

        return int_value

    @staticmethod
    def validate_sort_direction(direction: str) -> str:
        """
        Validate ORDER BY direction.

        Only ASC and DESC are valid SQL sort directions. This prevents injection
        attacks in ORDER BY clauses.

        Args:
            direction: The sort direction to validate (case-insensitive)

        Returns:
            The validated direction in uppercase (ASC or DESC)

        Raises:
            ValueError: If direction is not ASC or DESC

        Examples:
            >>> SqlValidator.validate_sort_direction("ASC")
            'ASC'
            >>> SqlValidator.validate_sort_direction("desc")
            'DESC'
            >>> SqlValidator.validate_sort_direction("ASC; DROP TABLE")
            ValueError: Invalid sort direction
        """
        if not isinstance(direction, str):
            raise ValueError(
                f"Invalid sort direction: must be a string, got {type(direction).__name__}"
            )

        direction_upper = direction.strip().upper()

        if direction_upper not in ('ASC', 'DESC'):
            raise ValueError(
                f"Invalid sort direction: '{direction}'. Must be 'ASC' or 'DESC'"
            )

        return direction_upper

    @staticmethod
    def validate_field_expression(field: str) -> str:
        """
        Validate field expression for SELECT clause.

        Allowed formats:
        - Simple column: "column_name"
        - Table-qualified: "table.column_name"
        - With alias: "column_name AS alias"
        - Fully qualified with alias: "table.column AS alias"
        - Wildcard: "table.*"

        Args:
            field: The field expression to validate

        Returns:
            The validated field expression (unchanged)

        Raises:
            ValueError: If the field expression is invalid

        Examples:
            >>> SqlValidator.validate_field_expression("username")
            'username'
            >>> SqlValidator.validate_field_expression("users.username")
            'users.username'
            >>> SqlValidator.validate_field_expression("users.username AS user_name")
            'users.username AS user_name'
            >>> SqlValidator.validate_field_expression("users.*")
            'users.*'
        """
        if not isinstance(field, str):
            raise ValueError(
                f"Invalid field expression: must be a string, got {type(field).__name__}"
            )

        field = field.strip()

        if not field:
            raise ValueError("Invalid field expression: cannot be empty")

        # Pattern explanation:
        # ^                                    - Start of string
        # [a-zA-Z_][a-zA-Z0-9_]*              - Identifier (column or table)
        # (\.[a-zA-Z_][a-zA-Z0-9_]*|\.\*)?    - Optional: .column or .*
        # (\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*)?   - Optional: AS alias
        # $                                    - End of string
        pattern = (
            r'^[a-zA-Z_][a-zA-Z0-9_]*'           # First identifier
            r'(\.[a-zA-Z_][a-zA-Z0-9_]*|\.\*)?'  # Optional .column or .*
            r'(\s+AS\s+[a-zA-Z_][a-zA-Z0-9_]*)?'  # Optional AS alias
            r'$'
        )

        if not re.match(pattern, field, re.IGNORECASE):
            raise ValueError(
                f"Invalid field expression: '{field}'. "
                f"Must match pattern: [table.]column[ AS alias] or table.*"
            )

        # Additional check: no SQL keywords or dangerous characters
        if any(char in field for char in (';', '--', '/*', '*/')):
            raise ValueError(
                f"Invalid field expression: '{field}' contains dangerous characters"
            )

        return field

    @staticmethod
    def validate_join_statement(join_stmt: str) -> str:
        """
        Validate JOIN statement structure.

        A valid JOIN must:
        - Start with a JOIN keyword (INNER JOIN, LEFT JOIN, RIGHT JOIN, or JOIN)
        - Include an ON clause
        - Not contain statement terminators (;) or SQL comments (--, /*)

        Note: This provides basic validation but is not foolproof. Ideally,
        JOIN statements should be constructed programmatically from validated
        components rather than passed as strings.

        Args:
            join_stmt: The JOIN statement to validate

        Returns:
            The validated JOIN statement (unchanged)

        Raises:
            ValueError: If the JOIN statement is invalid

        Examples:
            >>> SqlValidator.validate_join_statement(
            ...     "INNER JOIN orders ON users.id = orders.user_id"
            ... )
            'INNER JOIN orders ON users.id = orders.user_id'
        """
        if not isinstance(join_stmt, str):
            raise ValueError(
                f"Invalid JOIN statement: must be a string, got {type(join_stmt).__name__}"
            )

        join_stmt = join_stmt.strip()

        if not join_stmt:
            raise ValueError("Invalid JOIN statement: cannot be empty")

        # Must start with valid JOIN keyword
        join_pattern = (
            r'^\s*'
            r'(INNER\s+JOIN|LEFT\s+JOIN|LEFT\s+OUTER\s+JOIN|'
            r'RIGHT\s+JOIN|RIGHT\s+OUTER\s+JOIN|FULL\s+JOIN|'
            r'FULL\s+OUTER\s+JOIN|CROSS\s+JOIN|JOIN)'
            r'\s+[a-zA-Z_][a-zA-Z0-9_]*'  # Table name
            r'\s+ON\s+.+$'  # ON clause
        )

        if not re.match(join_pattern, join_stmt, re.IGNORECASE):
            raise ValueError(
                f"Invalid JOIN statement: '{join_stmt}'. "
                f"Must start with JOIN type, include table name, and have ON clause"
            )

        # Check for statement terminators (prevents multiple statements)
        if ';' in join_stmt:
            raise ValueError(
                f"Invalid JOIN statement: contains semicolon (statement terminator)"
            )

        # Check for SQL comment syntax (prevents comment-based injection)
        if '--' in join_stmt or '/*' in join_stmt or '*/' in join_stmt:
            raise ValueError(
                f"Invalid JOIN statement: contains SQL comment syntax"
            )

        # Check for UNION (prevents union-based injection)
        if re.search(r'\bUNION\b', join_stmt, re.IGNORECASE):
            raise ValueError(
                f"Invalid JOIN statement: UNION is not allowed in JOIN clauses"
            )

        return join_stmt

    @staticmethod
    def validate_sort_list(sort: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """
        Validate a list of (column, direction) tuples for ORDER BY clause.

        Args:
            sort: List of (column_name, direction) tuples

        Returns:
            List of validated (column_name, direction) tuples with directions in uppercase

        Raises:
            ValueError: If any column name or direction is invalid

        Examples:
            >>> SqlValidator.validate_sort_list([("username", "ASC"), ("created_at", "DESC")])
            [('username', 'ASC'), ('created_at', 'DESC')]
        """
        if not isinstance(sort, list):
            raise ValueError(f"Sort must be a list, got {type(sort).__name__}")

        validated_sort = []
        for item in sort:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError(
                    f"Each sort item must be a (column, direction) tuple, got {item}"
                )

            column, direction = item
            validated_column = SqlValidator.validate_identifier(column, "sort column")
            validated_direction = SqlValidator.validate_sort_direction(direction)
            validated_sort.append((validated_column, validated_direction))

        return validated_sort
