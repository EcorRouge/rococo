"""
Comprehensive tests for SQL injection prevention utilities.

These tests verify that the SqlValidator class properly validates all types
of SQL inputs and blocks injection attempts while allowing legitimate queries.
"""

import pytest
from rococo.data.sql_validator import SqlValidator


class TestValidateIdentifier:
    """Tests for SQL identifier validation (table names, column names, etc.)"""

    def test_valid_simple_identifier(self):
        """Test valid simple identifiers"""
        assert SqlValidator.validate_identifier("users") == "users"
        assert SqlValidator.validate_identifier("user_table") == "user_table"
        assert SqlValidator.validate_identifier("_private") == "_private"
        assert SqlValidator.validate_identifier("Table123") == "Table123"

    def test_valid_identifier_with_numbers(self):
        """Test identifiers with numbers (not at start)"""
        assert SqlValidator.validate_identifier("users2") == "users2"
        assert SqlValidator.validate_identifier("table_v2_final") == "table_v2_final"

    def test_invalid_identifier_starts_with_number(self):
        """Test that identifiers cannot start with numbers"""
        with pytest.raises(ValueError, match="Invalid .* Must start with letter or underscore"):
            SqlValidator.validate_identifier("123users", "table")

    def test_invalid_identifier_with_special_chars(self):
        """Test that identifiers cannot contain special characters"""
        with pytest.raises(ValueError, match="Invalid"):
            SqlValidator.validate_identifier("user-table", "table")

        with pytest.raises(ValueError, match="Invalid"):
            SqlValidator.validate_identifier("user.table", "column")

        with pytest.raises(ValueError, match="Invalid"):
            SqlValidator.validate_identifier("user@table", "table")

        with pytest.raises(ValueError, match="Invalid"):
            SqlValidator.validate_identifier("user table", "table")

    def test_sql_injection_attempts_blocked(self):
        """Test that SQL injection attempts are blocked"""
        # SQL statement injection
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier("users; DROP TABLE users", "table")

        # SQL comment injection
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier("users--", "table")

        with pytest.raises(ValueError):
            SqlValidator.validate_identifier("users/*comment*/", "table")

        # UNION injection
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier("users_UNION_SELECT", "table")

        # OR injection
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier("username_OR_1=1", "column")

    def test_invalid_empty_identifier(self):
        """Test that empty identifiers are rejected"""
        with pytest.raises(ValueError, match="cannot be empty"):
            SqlValidator.validate_identifier("", "table")

    def test_invalid_non_string_identifier(self):
        """Test that non-string identifiers are rejected"""
        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_identifier(123, "table")

        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_identifier(None, "table")

        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_identifier(["users"], "table")

    def test_invalid_too_long_identifier(self):
        """Test that identifiers longer than 64 chars are rejected"""
        long_name = "a" * 65
        with pytest.raises(ValueError, match="exceeds 64 character limit"):
            SqlValidator.validate_identifier(long_name, "table")

    def test_valid_max_length_identifier(self):
        """Test that 64-character identifiers are accepted"""
        max_name = "a" * 64
        assert SqlValidator.validate_identifier(max_name) == max_name


class TestValidateInteger:
    """Tests for integer validation (LIMIT, OFFSET, etc.)"""

    def test_valid_integer(self):
        """Test valid integer inputs"""
        assert SqlValidator.validate_integer(10, "limit") == 10
        assert SqlValidator.validate_integer(0, "offset") == 0
        assert SqlValidator.validate_integer(1000, "limit") == 1000

    def test_valid_string_integer(self):
        """Test that string integers are converted"""
        assert SqlValidator.validate_integer("10", "limit") == 10
        assert SqlValidator.validate_integer("0", "offset") == 0

    def test_sql_injection_attempts_blocked(self):
        """Test that SQL injection attempts in integers are blocked"""
        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("10; DROP TABLE users", "limit")

        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("10 OR 1=1", "limit")

        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("10--", "limit")

        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("10 UNION SELECT", "limit")

    def test_invalid_non_numeric(self):
        """Test that non-numeric values are rejected"""
        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("abc", "limit")

        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer("10.5", "limit")

    def test_range_validation_min(self):
        """Test minimum value validation"""
        with pytest.raises(ValueError, match="less than minimum"):
            SqlValidator.validate_integer(-1, "offset", min_val=0)

        with pytest.raises(ValueError, match="less than minimum"):
            SqlValidator.validate_integer(5, "limit", min_val=10)

    def test_range_validation_max(self):
        """Test maximum value validation"""
        with pytest.raises(ValueError, match="exceeds maximum"):
            SqlValidator.validate_integer(100001, "limit", max_val=100000)

        with pytest.raises(ValueError, match="exceeds maximum"):
            SqlValidator.validate_integer(1000, "limit", max_val=500)

    def test_range_validation_both(self):
        """Test both min and max validation"""
        # Valid
        assert SqlValidator.validate_integer(50, "limit", min_val=0, max_val=100) == 50

        # Too low
        with pytest.raises(ValueError, match="less than minimum"):
            SqlValidator.validate_integer(-1, "limit", min_val=0, max_val=100)

        # Too high
        with pytest.raises(ValueError, match="exceeds maximum"):
            SqlValidator.validate_integer(101, "limit", min_val=0, max_val=100)

    def test_none_handling(self):
        """Test None value handling"""
        # Default: None not allowed
        with pytest.raises(ValueError, match="cannot be None"):
            SqlValidator.validate_integer(None, "limit")

        # When allow_none=True
        assert SqlValidator.validate_integer(None, "limit", allow_none=True) is None


class TestValidateSortDirection:
    """Tests for ORDER BY direction validation"""

    def test_valid_directions(self):
        """Test valid sort directions"""
        assert SqlValidator.validate_sort_direction("ASC") == "ASC"
        assert SqlValidator.validate_sort_direction("DESC") == "DESC"
        assert SqlValidator.validate_sort_direction("asc") == "ASC"
        assert SqlValidator.validate_sort_direction("desc") == "DESC"
        assert SqlValidator.validate_sort_direction("  ASC  ") == "ASC"

    def test_invalid_directions(self):
        """Test invalid sort directions"""
        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("ASCENDING")

        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("UP")

        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("")

    def test_sql_injection_attempts_blocked(self):
        """Test that SQL injection attempts are blocked"""
        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("ASC; DROP TABLE")

        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("ASC--")

        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_direction("DESC OR 1=1")

    def test_invalid_non_string(self):
        """Test that non-string directions are rejected"""
        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_sort_direction(123)

        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_sort_direction(None)


class TestValidateFieldExpression:
    """Tests for field expression validation in SELECT clause"""

    def test_valid_simple_field(self):
        """Test valid simple field names"""
        assert SqlValidator.validate_field_expression("username") == "username"
        assert SqlValidator.validate_field_expression("user_id") == "user_id"
        assert SqlValidator.validate_field_expression("_private") == "_private"

    def test_valid_qualified_field(self):
        """Test valid table.column expressions"""
        assert SqlValidator.validate_field_expression("users.username") == "users.username"
        assert SqlValidator.validate_field_expression("users.user_id") == "users.user_id"

    def test_valid_field_with_alias(self):
        """Test valid field expressions with aliases"""
        assert SqlValidator.validate_field_expression("username AS name") == "username AS name"
        assert SqlValidator.validate_field_expression("users.username AS user_name") == "users.username AS user_name"
        assert SqlValidator.validate_field_expression("count AS total") == "count AS total"

    def test_valid_wildcard(self):
        """Test valid wildcard expressions"""
        assert SqlValidator.validate_field_expression("users.*") == "users.*"
        assert SqlValidator.validate_field_expression("table_name.*") == "table_name.*"

    def test_invalid_field_expressions(self):
        """Test invalid field expressions"""
        # Multiple dots
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("db.users.username")

        # Invalid characters
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("user-name")

        # Starts with number
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("123column")

    def test_sql_injection_attempts_blocked(self):
        """Test that SQL injection attempts are blocked"""
        # Semicolon injection - caught by pattern validation
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("username; DROP TABLE")

        # Comment injection - caught by pattern validation (-- and /* don't match alphanumeric pattern)
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("username--")

        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("username/*")

        # UNION injection (won't match pattern)
        with pytest.raises(ValueError, match="Invalid field expression"):
            SqlValidator.validate_field_expression("* FROM admin_secrets WHERE 1=1")

    def test_invalid_empty_field(self):
        """Test that empty fields are rejected"""
        with pytest.raises(ValueError, match="cannot be empty"):
            SqlValidator.validate_field_expression("")

        with pytest.raises(ValueError, match="cannot be empty"):
            SqlValidator.validate_field_expression("   ")

    def test_invalid_non_string(self):
        """Test that non-string fields are rejected"""
        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_field_expression(123)

        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_field_expression(None)


class TestValidateJoinStatement:
    """Tests for JOIN statement validation"""

    def test_valid_inner_join(self):
        """Test valid INNER JOIN statements"""
        stmt = "INNER JOIN orders ON users.id = orders.user_id"
        assert SqlValidator.validate_join_statement(stmt) == stmt

    def test_valid_left_join(self):
        """Test valid LEFT JOIN statements"""
        stmt = "LEFT JOIN orders ON users.id = orders.user_id"
        assert SqlValidator.validate_join_statement(stmt) == stmt

        stmt2 = "LEFT OUTER JOIN orders ON users.id = orders.user_id"
        assert SqlValidator.validate_join_statement(stmt2) == stmt2

    def test_valid_right_join(self):
        """Test valid RIGHT JOIN statements"""
        stmt = "RIGHT JOIN orders ON users.id = orders.user_id"
        assert SqlValidator.validate_join_statement(stmt) == stmt

    def test_valid_simple_join(self):
        """Test valid simple JOIN statements"""
        stmt = "JOIN orders ON users.id = orders.user_id"
        assert SqlValidator.validate_join_statement(stmt) == stmt

    def test_valid_complex_on_clause(self):
        """Test JOIN with complex ON clauses"""
        stmt = "INNER JOIN orders ON users.id = orders.user_id AND orders.active = true"
        assert SqlValidator.validate_join_statement(stmt) == stmt

    def test_invalid_missing_on_clause(self):
        """Test that JOINs without ON clause are rejected"""
        with pytest.raises(ValueError, match="Must start with JOIN type"):
            SqlValidator.validate_join_statement("INNER JOIN orders")

    def test_invalid_missing_join_keyword(self):
        """Test that statements without JOIN keyword are rejected"""
        with pytest.raises(ValueError, match="Must start with JOIN type"):
            SqlValidator.validate_join_statement("orders ON users.id = orders.user_id")

    def test_sql_injection_semicolon_blocked(self):
        """Test that semicolon injection is blocked"""
        with pytest.raises(ValueError, match="contains semicolon"):
            SqlValidator.validate_join_statement(
                "INNER JOIN orders ON users.id = orders.user_id; DROP TABLE users"
            )

    def test_sql_injection_comment_blocked(self):
        """Test that comment injection is blocked"""
        with pytest.raises(ValueError, match="contains SQL comment syntax"):
            SqlValidator.validate_join_statement(
                "INNER JOIN orders ON users.id = orders.user_id--"
            )

        with pytest.raises(ValueError, match="contains SQL comment syntax"):
            SqlValidator.validate_join_statement(
                "INNER JOIN orders ON users.id = orders.user_id /* comment */"
            )

    def test_sql_injection_union_blocked(self):
        """Test that UNION injection is blocked"""
        with pytest.raises(ValueError, match="UNION is not allowed"):
            SqlValidator.validate_join_statement(
                "INNER JOIN orders ON 1=1 UNION SELECT password FROM admin"
            )

    def test_invalid_empty_statement(self):
        """Test that empty statements are rejected"""
        with pytest.raises(ValueError, match="cannot be empty"):
            SqlValidator.validate_join_statement("")

        with pytest.raises(ValueError, match="cannot be empty"):
            SqlValidator.validate_join_statement("   ")

    def test_invalid_non_string(self):
        """Test that non-string statements are rejected"""
        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_join_statement(123)

        with pytest.raises(ValueError, match="must be a string"):
            SqlValidator.validate_join_statement(None)


class TestValidateSortList:
    """Tests for sort list validation"""

    def test_valid_single_sort(self):
        """Test valid single sort"""
        result = SqlValidator.validate_sort_list([("username", "ASC")])
        assert result == [("username", "ASC")]

    def test_valid_multiple_sorts(self):
        """Test valid multiple sorts"""
        result = SqlValidator.validate_sort_list([
            ("username", "ASC"),
            ("created_at", "DESC")
        ])
        assert result == [("username", "ASC"), ("created_at", "DESC")]

    def test_normalizes_direction(self):
        """Test that directions are normalized to uppercase"""
        result = SqlValidator.validate_sort_list([
            ("username", "asc"),
            ("created_at", "desc")
        ])
        assert result == [("username", "ASC"), ("created_at", "DESC")]

    def test_invalid_not_list(self):
        """Test that non-list values are rejected"""
        with pytest.raises(ValueError, match="Sort must be a list"):
            SqlValidator.validate_sort_list("username ASC")

        with pytest.raises(ValueError, match="Sort must be a list"):
            SqlValidator.validate_sort_list(("username", "ASC"))

    def test_invalid_not_tuple(self):
        """Test that non-tuple items are rejected"""
        with pytest.raises(ValueError, match="must be a \\(column, direction\\) tuple"):
            SqlValidator.validate_sort_list(["username", "ASC"])

    def test_invalid_tuple_wrong_length(self):
        """Test that tuples with wrong length are rejected"""
        with pytest.raises(ValueError, match="must be a \\(column, direction\\) tuple"):
            SqlValidator.validate_sort_list([("username",)])

        with pytest.raises(ValueError, match="must be a \\(column, direction\\) tuple"):
            SqlValidator.validate_sort_list([("username", "ASC", "extra")])

    def test_invalid_column_name(self):
        """Test that invalid column names are rejected"""
        with pytest.raises(ValueError, match="Invalid sort column"):
            SqlValidator.validate_sort_list([("user-name", "ASC")])

    def test_invalid_direction(self):
        """Test that invalid directions are rejected"""
        with pytest.raises(ValueError, match="Must be 'ASC' or 'DESC'"):
            SqlValidator.validate_sort_list([("username", "UP")])

    def test_sql_injection_in_column(self):
        """Test that SQL injection in column names is blocked"""
        with pytest.raises(ValueError):
            SqlValidator.validate_sort_list([("username; DROP TABLE", "ASC")])

    def test_sql_injection_in_direction(self):
        """Test that SQL injection in direction is blocked"""
        with pytest.raises(ValueError):
            SqlValidator.validate_sort_list([("username", "ASC; DROP TABLE")])


class TestIntegrationScenarios:
    """Integration tests simulating real-world attack scenarios"""

    def test_classic_limit_injection_blocked(self):
        """Test that classic LIMIT injection is blocked"""
        malicious_limit = "10; DROP TABLE users; --"
        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer(malicious_limit, "limit")

    def test_blind_boolean_offset_injection_blocked(self):
        """Test that blind boolean-based offset injection is blocked"""
        malicious_offset = "CASE WHEN (SELECT COUNT(*) FROM secrets) > 0 THEN 0 ELSE 9999 END"
        with pytest.raises(ValueError, match="must be an integer"):
            SqlValidator.validate_integer(malicious_offset, "offset")

    def test_order_by_injection_blocked(self):
        """Test that ORDER BY injection is blocked"""
        malicious_column = "(CASE WHEN admin=1 THEN username ELSE password END)"
        with pytest.raises(ValueError, match="Invalid sort column"):
            SqlValidator.validate_sort_list([(malicious_column, "ASC")])

    def test_condition_key_injection_blocked(self):
        """Test that condition key injection is blocked"""
        malicious_key = "username OR 1=1 --"
        with pytest.raises(ValueError, match="Invalid column name in condition"):
            SqlValidator.validate_identifier(malicious_key, "column name in condition")

    def test_legitimate_query_allowed(self):
        """Test that legitimate queries with valid inputs pass validation"""
        # All these should succeed without raising exceptions
        table = SqlValidator.validate_identifier("users", "table")
        assert table == "users"

        limit = SqlValidator.validate_integer(10, "limit", min_val=0, max_val=100000)
        assert limit == 10

        offset = SqlValidator.validate_integer(0, "offset", min_val=0)
        assert offset == 0

        sort = SqlValidator.validate_sort_list([("username", "ASC"), ("created_at", "DESC")])
        assert len(sort) == 2

        field = SqlValidator.validate_field_expression("users.username AS name")
        assert field == "users.username AS name"

        join = SqlValidator.validate_join_statement(
            "INNER JOIN orders ON users.id = orders.user_id"
        )
        assert "INNER JOIN" in join

    def test_get_count_table_injection_blocked(self):
        """Test that table name injection in get_count is blocked"""
        malicious_table = "users; DROP TABLE users; --"
        with pytest.raises(ValueError, match="Invalid table name"):
            SqlValidator.validate_identifier(malicious_table, "table name")

    def test_get_save_query_table_injection_blocked(self):
        """Test that table name injection in get_save_query is blocked"""
        malicious_table = "users; DROP TABLE"
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier(malicious_table, "table name")

    def test_get_save_query_column_injection_blocked(self):
        """Test that column name injection in get_save_query is blocked"""
        # Simulate data dict with malicious column name
        malicious_column = "username; DROP TABLE users; --"
        with pytest.raises(ValueError):
            SqlValidator.validate_identifier(malicious_column, "column name")

        # Test with SQL keyword in column
        malicious_column2 = "user_DROP_table"
        with pytest.raises(ValueError, match="contains SQL keyword"):
            SqlValidator.validate_identifier(malicious_column2, "column name")

    def test_multiple_column_validation(self):
        """Test validating multiple column names from data dict"""
        # Legitimate column names should pass
        columns = ["username", "email", "created_at", "user_id"]
        for col in columns:
            validated = SqlValidator.validate_identifier(col, "column name")
            assert validated == col

        # Malicious column name should fail
        malicious_columns = [
            "username",
            "email; DROP TABLE",  # SQL injection
            "created_at"
        ]

        validated = []
        for col in malicious_columns:
            try:
                validated.append(SqlValidator.validate_identifier(col, "column name"))
            except ValueError:
                pass  # Expected for malicious column

        # Should only have 2 valid columns (username and created_at)
        assert len(validated) == 2
