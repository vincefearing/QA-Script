import pymongo
import argparse
import pandas as pd
import json
from datetime import datetime, timedelta
import os

client = pymongo.MongoClient("mongodb://localhost:27017/")
qa_db = client["qa_db"]

collection_1 = qa_db["collection1"]
collection_2 = qa_db["collection2"]

def parse_and_insert(file_path, collection_name):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    qa_db = client["qa_db"]

    collection_1 = qa_db["collection1"]
    collection_2 = qa_db["collection2"]
    df = pd.read_excel(file_path)
    records = df.to_dict('records')
    if collection_name == "collection_1":
        collection_1.insert_many(records)
    elif collection_name == "collection_2":
        collection_2.insert_many(records)

def generate_report(report_type, user=None):
    if report_type == "user_work":
        if not user:
            print("User name (Test Owner) is required for this report.")
            return
    
        # Query for work items by a specific Test Owner from both collections
        work_items = list(collection_1.find({"Test Owner": user}, {'_id': 0})) + \
                     list(collection_2.find({"Test Owner": user}, {'_id': 0}))
        
        seen = set()
        unique_work_items = []
        for item in work_items:
            unique_key = create_unique_key(item)
            if unique_key not in seen:
                seen.add(unique_key)
                unique_work_items.append(item)

        print(f"Work done by {user}: {len(unique_work_items)} items found.")
    
    

    elif report_type == "repeatable_bugs":
        # Adjust the query to check for "yes" in a case-insensitive manner
        query = {"Repeatable?": {"$regex": "^yes$", "$options": "i"}}
    
        # Find all repeatable bugs from both collections
        repeatable_bugs_1 = list(collection_1.find(query, {'_id': 0}))
        repeatable_bugs_2 = list(collection_2.find(query, {'_id': 0}))
    
        # Combine results from both collections
        repeatable_bugs = repeatable_bugs_1 + repeatable_bugs_2

        seen = set()
        unique_repeatable_bugs = []
        for bug in repeatable_bugs:
            unique_key = create_unique_bug_key(bug)
            if unique_key not in seen:
                seen.add(unique_key)
                unique_repeatable_bugs.append(bug)

        print(f"Unique repeatable bugs: {len(unique_repeatable_bugs)} items found.")


    elif report_type == "blocker_bugs":
        # Find all blocker bugs
        query = {"Blocker?": {"$regex": "^yes$", "$options": "i"}}
        blocker_bugs_1 = list(collection_1.find(query, {'_id': 0}))
        blocker_bugs_2 = list(collection_2.find(query, {'_id': 0}))

        combined_bugs = blocker_bugs_1 + blocker_bugs_2

        seen = set()
        unique_blocker_bugs = []
        for bug in combined_bugs:
            unique_key = create_unique_bug_key(bug)
            if unique_key not in seen:
                seen.add(unique_key)
                unique_blocker_bugs.append(bug)

        print(f"Unique blocker bugs: {len(unique_blocker_bugs)} items found.")

    elif report_type == "on_date":
        specific_date_str = "2024-03-19"
        # Parse the specific_date_str and create a datetime object
        specific_date = datetime.strptime(specific_date_str, '%Y-%m-%d')
        
        start_of_day = specific_date
        end_of_day = specific_date + timedelta(days=1)
    
        query = {
            "Build #": {
                "$gte": start_of_day,
                "$lt": end_of_day
            }
        }
    
        items_on_date_1 = list(collection_1.find(query, {'_id': 0}))
        items_on_date_2 = list(collection_2.find(query, {'_id': 0}))
    
        print(f"Checking documents with 'Build #' on {specific_date_str}")
        print(f"Found in collection 1: {len(items_on_date_1)}")
        print(f"Found in collection 2: {len(items_on_date_2)}")
    
        combined_items = items_on_date_1 + items_on_date_2

        seen = set()
        unique_items_on_date = []
        for item in combined_items:
            unique_key = create_unique_bug_key(item)
            if unique_key not in seen:
                seen.add(unique_key)
                unique_items_on_date.append(item)
    
        print(f"Unique items on {specific_date_str}: {len(unique_items_on_date)} found.")


    elif report_type == "report_back":
        print("Report_back test")
        # Fetch all documents from collection_2
        all_tests = list(collection_2.find({}, {'_id': 0}))
    
        if not all_tests:
            print("No tests found in collection 2.")
            return
    
        # Calculate the positions of the first, middle, and last tests
        total_tests = len(all_tests)
        middle_index = total_tests // 2
    
        # Extract the first, middle, and last test based on their position in the list
        first_test = all_tests[0] if all_tests else None
        middle_test = all_tests[middle_index] if total_tests > 1 else None
        last_test = all_tests[-1] if all_tests else None
    
        # Report these specific tests
        print("First test case details:")
        if first_test:
            print_test_details(first_test)
        
        print("Middle test case details:")
        if middle_test:
            print_test_details(middle_test)
        
        print("Last test case details:")
        if last_test:
            print_test_details(last_test)

    else:
        print("Invalid report type specified.")

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

def create_unique_key(item):
        return (
            item.get("Test Case", ""),
            item.get("Expected Result", ""),
            item.get("Actual Result", ""),
            item.get("Category", ""),
            item.get("Build #", ""),
            item.get("Repeatable?", "").lower(),  # Normalize to lower case for consistency
            item.get("Blocker?", "").lower(),
        )

def create_unique_bug_key(bug):
        return (
            bug.get("Test Case", ""),
            bug.get("Expected Result", ""),
            bug.get("Actual Result", ""),
            bug.get("Category", ""),
            bug.get("Build #", ""),
        )

def print_test_details(test):
        print("Test #:", test.get("Test #", "N/A"))
        print("Category:", test.get("Category", "N/A"))
        print("Test Case:", test.get("Test Case", "N/A"))
        print("Expected Result:", test.get("Expected Result", "N/A"))
        print("Actual Result:", test.get("Actual Result", "N/A"))
        print("Repeatable?:", test.get("Repeatable?", "N/A"))
        print("Blocker?:", test.get("Blocker?", "N/A"))
        print("Test Owner:", test.get("Test Owner", "N/A"))
        print("------")

import pymongo
import pandas as pd
import os

def export_user_work_to_csv(user, output_csv):
    # Initialize MongoDB connection inside the function
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    qa_db = client["qa_db"]  # Make sure this matches your database name
    collection_2 = qa_db["collection2"]  # Ensure this matches your collection name

    print(f"Fetching work items for user: '{user}' from collection_2...")
    query = {"Test Owner": user}
    print(f"Executing query: {query}")

    work_items = list(collection_2.find(query, {'_id': 0}))
    print(f"Total items found for '{user}': {len(work_items)}")

    if work_items:
        df = pd.DataFrame(work_items)
        try:
            df.to_csv(output_csv, index=False)
            print(f"Successfully exported {len(df)} items to '{output_csv}'.")
            print(f"CSV file location: {os.path.abspath(output_csv)}")
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
    else:
        print(f"No work items found for the specified user '{user}' in collection_2.")



def test_query():
    results = list(qa_db.collection2.find({"Test Owner": "Kevin Chaja"}, {'_id': 0}))
    print(f"Found {len(results)} results")
    for doc in results:
        print(doc)

def main():
    parser = argparse.ArgumentParser(description="QA Report Management Tool")
    # Arguments for data insertion
    parser.add_argument("--weekly", type=str, help="Path to the weekly QA CSV file")
    parser.add_argument("--dbdump", type=str, help="Path to the DB dump Excel file")
    # Argument for report generation
    parser.add_argument("--report", type=str, help="Type of report to generate. Options: [user_work --user], repeatable_bugs, blocker_bugs, on_date, report_back")
    parser.add_argument("--user", type=str, help="User name for user-specific reports")
    parser.add_argument("--date", type=str, help="Date for build-specific reports")
    parser.add_argument("--output_csv", type=str, help="Output CSV file name for exporting user work")
    args = parser.parse_args()

    if args.weekly or args.dbdump:
        if args.weekly:
            parse_and_insert(args.weekly, "collection_1")
            print(f"Weekly QA report inserted into collection_1 from {args.weekly}")
        if args.dbdump:
            parse_and_insert(args.dbdump, "collection_2")
            print(f"DB dump inserted into collection_2 from {args.dbdump}")
    elif args.output_csv and args.user:
        export_user_work_to_csv(args.user, args.output_csv)
    elif args.report:
        generate_report(args.report, user=args.user)

if __name__ == "__main__":
    main()
