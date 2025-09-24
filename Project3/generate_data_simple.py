#!/usr/bin/env python3
# 该脚本的作用：生成测试数据

import mysql.connector
import random
import datetime
import argparse
import sys

def create_connection(host, port, user, password, database):
    """Create database connection"""
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

def generate_test_data(host, port, user, password, database, accounts=1000, logins=10000, transactions=20000):
    """Generate test data for the risk analysis system"""
    
    print("Connecting to database...")
    conn = create_connection(host, port, user, password, database)
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        print("Clearing existing test data...")
        cursor.execute("DELETE FROM transactions")
        cursor.execute("DELETE FROM logins")
        cursor.execute("DELETE FROM accounts")
        
        # Generate accounts
        print(f"Generating {accounts} accounts...")
        account_data = []
        for i in range(1, accounts + 1):
            account_data.append((i, f"User_{i:04d}"))
        
        cursor.executemany("INSERT INTO accounts (id, name) VALUES (%s, %s)", account_data)
        
        # Generate logins
        print(f"Generating {logins} login records...")
        login_data = []
        for i in range(1, logins + 1):
            account_id = random.randint(1, accounts)
            # Generate random datetime within last 30 days
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            login_time = datetime.datetime.now() - datetime.timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            login_data.append((i, account_id, login_time))
        
        cursor.executemany("INSERT INTO logins (id, account_id, login_at) VALUES (%s, %s, %s)", login_data)
        
        # Generate transactions
        print(f"Generating {transactions} transactions...")
        transaction_data = []
        for i in range(1, transactions + 1):
            sender_id = random.randint(1, accounts)
            receiver_id = random.randint(1, accounts)
            # Avoid self-transactions
            while receiver_id == sender_id:
                receiver_id = random.randint(1, accounts)
            
            # Generate amount (some large transactions for testing)
            if random.random() < 0.1:  # 10% chance of large transaction
                amount = random.randint(50000, 200000)
            else:
                amount = random.randint(100, 49999)
            
            # Generate random datetime within last 30 days
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            created_at = datetime.datetime.now() - datetime.timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            transaction_data.append((i, sender_id, receiver_id, amount, created_at, 'posted'))
        
        cursor.executemany("INSERT INTO transactions (id, sender_account_id, receiver_account_id, amount, created_at, status) VALUES (%s, %s, %s, %s, %s, %s)", transaction_data)
        
        # Commit all changes
        conn.commit()
        print("Test data generation completed successfully!")
        return True
        
    except mysql.connector.Error as err:
        print(f"Error generating test data: {err}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Generate test data for risk analysis system')
    parser.add_argument('--host', default='localhost', help='MySQL host')
    parser.add_argument('--port', type=int, default=3306, help='MySQL port')
    parser.add_argument('--user', default='root', help='MySQL username')
    parser.add_argument('--password', default='', help='MySQL password')
    parser.add_argument('--database', default='risk_analysis_system', help='Database name')
    parser.add_argument('--accounts', type=int, default=1000, help='Number of accounts to generate')
    parser.add_argument('--logins', type=int, default=10000, help='Number of login records to generate')
    parser.add_argument('--transactions', type=int, default=20000, help='Number of transactions to generate')
    
    args = parser.parse_args()
    
    success = generate_test_data(
        args.host, args.port, args.user, args.password, args.database,
        args.accounts, args.logins, args.transactions
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
