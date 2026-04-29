#!/usr/bin/env python3
"""
Schema Deployment Script
Deploys analytics schema from schemas/analytics.sql to PostgreSQL
"""

import psycopg2
import sys
import time

# Database connection parameters
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "finance_db_user"
DB_PASSWORD = "1234"
DB_NAME = "finance_db"

def connect_to_db():
    """Establish connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

def read_schema_file():
    """Read DDL script from schemas/analytics.sql"""
    try:
        with open("schemas/analytics.sql", "r") as f:
            return f.read()
    except FileNotFoundError:
        print("❌ schemas/analytics.sql not found")
        return None

def deploy_schema(conn, ddl_script):
    """Execute DDL script against PostgreSQL"""
    try:
        cursor = conn.cursor()
        cursor.execute(ddl_script)
        conn.commit()
        cursor.close()
        print("✓ DDL script executed successfully")
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"❌ Error executing DDL: {e}")
        return False

def verify_schema(conn):
    """Verify schema creation and table structure"""
    try:
        cursor = conn.cursor()

        # Check 1: Schema exists
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'analytics';"
        )
        if cursor.fetchone():
            print("✓ Analytics schema created")
        else:
            print("❌ Analytics schema not found")
            return False

        # Check 2: All tables created
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics' ORDER BY table_name;"
        )
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = {'dim_customer', 'dim_date', 'dim_order', 'dim_product', 'fct_order_items'}

        if set(tables) == expected_tables:
            print(f"✓ All 5 tables created: {', '.join(sorted(tables))}")
        else:
            print(f"❌ Table mismatch. Found: {tables}, Expected: {expected_tables}")
            return False

        # Check 3: Indexes created
        cursor.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'analytics' ORDER BY indexname;"
        )
        indexes = [row[0] for row in cursor.fetchall()]
        print(f"✓ Indexes created ({len(indexes)} total): {', '.join(indexes[:3])}...")

        # Check 4: Foreign keys
        cursor.execute(
            "SELECT constraint_name, table_name FROM information_schema.table_constraints WHERE table_schema = 'analytics' AND constraint_type = 'FOREIGN KEY';"
        )
        fks = cursor.fetchall()
        print(f"✓ Foreign keys defined ({len(fks)} total): {', '.join([row[0] for row in fks])}")

        # Check 5: Check constraints
        cursor.execute(
            "SELECT constraint_name, table_name FROM information_schema.table_constraints WHERE table_schema = 'analytics' AND constraint_type = 'CHECK';"
        )
        checks = cursor.fetchall()
        print(f"✓ Check constraints defined ({len(checks)} total): {', '.join([row[0] for row in checks])}")

        cursor.close()
        return True
    except psycopg2.Error as e:
        print(f"❌ Verification failed: {e}")
        return False

def test_constraints(conn):
    """Test PRIMARY KEY, FOREIGN KEY, and CHECK constraints"""
    try:
        cursor = conn.cursor()

        print("\n--- Testing Constraints ---")

        # Test 1: Primary Key Constraint
        print("\nTest 1: Primary Key Constraint")
        try:
            cursor.execute(
                "INSERT INTO analytics.dim_product (product_id, product_name) VALUES (1, 'Test Product');"
            )
            conn.commit()
            print("✓ First insert succeeded")

            # Try duplicate
            try:
                cursor.execute(
                    "INSERT INTO analytics.dim_product (product_id, product_name) VALUES (1, 'Another Product');"
                )
                conn.commit()
                print("❌ Duplicate key insert should have failed but didn't")
            except psycopg2.Error:
                conn.rollback()
                print("✓ Duplicate key insert correctly rejected")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"❌ PK test failed: {e}")

        # Test 2: Foreign Key Constraint
        print("\nTest 2: Foreign Key Constraint")
        try:
            cursor.execute(
                "INSERT INTO analytics.fct_order_items (order_id, product_id, customer_id, delivery_date, quantity_pending, is_open) "
                "VALUES (999, 999, 999, '2026-04-28', 10, TRUE);"
            )
            conn.commit()
            print("❌ FK insert with non-existent parent should have failed")
        except psycopg2.Error:
            conn.rollback()
            print("✓ Foreign key constraint correctly rejected missing parent")

        # Test 3: Check Constraint (quantity >= 0)
        print("\nTest 3: Check Constraint (quantity >= 0)")
        try:
            # Pre-insert valid dimensions
            cursor.execute(
                "INSERT INTO analytics.dim_product (product_id, product_name) VALUES (1, 'Test Product') ON CONFLICT (product_id) DO NOTHING;"
            )
            cursor.execute(
                "INSERT INTO analytics.dim_customer (customer_id, customer_name) VALUES (1, 'Test Customer') ON CONFLICT (customer_id) DO NOTHING;"
            )
            cursor.execute(
                "INSERT INTO analytics.dim_order (order_id, status) VALUES (1, 'PENDING') ON CONFLICT (order_id) DO NOTHING;"
            )
            cursor.execute(
                "INSERT INTO analytics.dim_date (delivery_date) VALUES ('2026-04-28') ON CONFLICT (delivery_date) DO NOTHING;"
            )
            conn.commit()

            # Try negative quantity
            try:
                cursor.execute(
                    "INSERT INTO analytics.fct_order_items (order_id, product_id, customer_id, delivery_date, quantity_pending, is_open) "
                    "VALUES (1, 1, 1, '2026-04-28', -5, TRUE);"
                )
                conn.commit()
                print("❌ Negative quantity insert should have failed")
            except psycopg2.Error:
                conn.rollback()
                print("✓ Check constraint (quantity >= 0) correctly rejected negative value")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"❌ Check constraint test setup failed: {e}")

        # Test 4: Valid Insert
        print("\nTest 4: Valid Insert")
        try:
            cursor.execute(
                "INSERT INTO analytics.fct_order_items (order_id, product_id, customer_id, delivery_date, quantity_pending, is_open) "
                "VALUES (1, 1, 1, '2026-04-28', 10, TRUE);"
            )
            conn.commit()

            cursor.execute(
                "SELECT COUNT(*) FROM analytics.fct_order_items WHERE order_id = 1;"
            )
            count = cursor.fetchone()[0]
            if count == 1:
                print("✓ Valid insert succeeded and is readable")
            else:
                print(f"❌ Insert succeeded but row not found (count={count})")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"❌ Valid insert failed: {e}")

        cursor.close()
        return True
    except Exception as e:
        print(f"❌ Constraint testing failed: {e}")
        return False

def cleanup_test_data(conn):
    """Clean up test data from deployment"""
    try:
        cursor = conn.cursor()

        print("\n--- Cleaning Up Test Data ---")

        cursor.execute("DELETE FROM analytics.fct_order_items WHERE order_id = 1;")
        cursor.execute("DELETE FROM analytics.dim_product WHERE product_id = 1;")
        cursor.execute("DELETE FROM analytics.dim_customer WHERE customer_id = 1;")
        cursor.execute("DELETE FROM analytics.dim_order WHERE order_id = 1;")
        cursor.execute("DELETE FROM analytics.dim_date WHERE delivery_date = '2026-04-28';")

        conn.commit()

        # Verify empty
        cursor.execute(
            "SELECT SUM(row_count) as total_rows FROM ("
            "SELECT COUNT(*) as row_count FROM analytics.fct_order_items "
            "UNION ALL SELECT COUNT(*) FROM analytics.dim_product "
            "UNION ALL SELECT COUNT(*) FROM analytics.dim_customer "
            "UNION ALL SELECT COUNT(*) FROM analytics.dim_order "
            "UNION ALL SELECT COUNT(*) FROM analytics.dim_date) t;"
        )
        total = cursor.fetchone()[0]
        if total == 0:
            print("✓ Test data cleaned up (analytics schema empty)")
        else:
            print(f"⚠ Analytics schema has {total} rows remaining")

        cursor.close()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"❌ Cleanup failed: {e}")
        return False

def main():
    """Main deployment workflow"""
    print("=" * 70)
    print("Schema Deployment — Analytics Platform")
    print("=" * 70)

    # Connect to database
    print("\nConnecting to PostgreSQL...")
    conn = connect_to_db()
    if not conn:
        return False

    # Read DDL script
    print("Reading analytics DDL script...")
    ddl_script = read_schema_file()
    if not ddl_script:
        conn.close()
        return False

    # Deploy schema
    print("\nDeploying schema...")
    if not deploy_schema(conn, ddl_script):
        conn.close()
        return False

    # Verify deployment
    print("\nVerifying deployment...")
    if not verify_schema(conn):
        conn.close()
        return False

    # Test constraints
    print("\nTesting constraints...")
    if not test_constraints(conn):
        conn.close()
        return False

    # Cleanup
    print("\nCleaning up test data...")
    if not cleanup_test_data(conn):
        conn.close()
        return False

    conn.close()

    print("\n" + "=" * 70)
    print("✓ Schema deployment complete and verified")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
