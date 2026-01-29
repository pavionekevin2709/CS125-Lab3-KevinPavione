"""
Sales Data Processing Tool
Assignment 3 - CSV Data Analysis and Transformation Application
"""

import csv
import os
from datetime import datetime
from collections import defaultdict

# Constants
VALID_DEPARTMENTS = {"Electronics", "Clothing", "Home", "Sports"}
DATE_FORMAT = "%Y-%m-%d"

OUTPUT_CLEANED = "cleaned_data.csv"
OUTPUT_DEPT_SUMMARY = "department_summary.csv"
ERROR_LOG = "errors.txt"


def main():
    print("=" * 40)
    print("SALES DATA PROCESSING TOOL")
    print("=" * 40)
    print()

    filename = input("Enter CSV filename: ").strip()
    if not filename:
        print("No filename entered. Exiting.")
        return

    print("\nLoading data...")
    valid_records, invalid_count, total_processed = load_and_validate_data(filename)

    if total_processed == 0:
        print("No records were processed. Check file and try again.")
        return

    print(f"Valid records: {len(valid_records)}")
    print(f"Invalid records: {invalid_count} (details in {ERROR_LOG})")

    print("\nCalculating statistics...")
    stats = compute_statistics(valid_records)

    display_report(stats, total_processed, invalid_count, len(valid_records))

    print("\nExporting results...")
    export_cleaned(valid_records)
    export_department_summary(stats["departments"])

    print(f"  Cleaned data → {OUTPUT_CLEANED}")
    print(f"  Department summary → {OUTPUT_DEPT_SUMMARY}")
    print(f"  Errors logged to → {ERROR_LOG}")

    print("\nProcessing complete!")
    print("=" * 40)


def load_and_validate_data(filename):
    """Read CSV → validate & clean each row → return valid records + counts"""
    valid = []
    invalid_count = 0
    total = 0

    # Clear error log
    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("=== Sales Data Validation Errors ===\n\n")

    if not os.path.isfile(filename):
        print(f"Error: File '{filename}' not found.")
        return [], 0, 0

    try:
        with open(filename, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != ["employee_id", "employee_name", "department", "sales_amount", "date"]:
                print("Warning: CSV headers do not exactly match expected format.")

            for row_num, row in enumerate(reader, start=2):
                total += 1
                cleaned_row, error = clean_and_validate_row(row, row_num)
                if cleaned_row:
                    valid.append(cleaned_row)
                else:
                    invalid_count += 1
                    log_error(row_num, error, row)

        return valid, invalid_count, total

    except PermissionError:
        print(f"Permission denied: Cannot read '{filename}'")
        return [], 0, 0
    except csv.Error as e:
        print(f"CSV parsing error: {e}")
        return [], 0, 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        return [], 0, 0


def clean_and_validate_row(row, line):
    """Validate one row → return cleaned dict or (None, error message)"""
    try:
        cleaned = {k: (v or "").strip() for k, v in row.items()}

        # employee_id
        try:
            eid = int(cleaned["employee_id"])
            if eid <= 0:
                raise ValueError
            cleaned["employee_id"] = str(eid)
        except:
            return None, "employee_id must be a positive integer"

        # employee_name
        if not cleaned["employee_name"]:
            return None, "employee_name cannot be empty"

        # department
        dept = cleaned["department"].title()
        if dept not in VALID_DEPARTMENTS:
            return None, f"department must be one of: {', '.join(VALID_DEPARTMENTS)}"
        cleaned["department"] = dept

        # sales_amount
        amt_str = cleaned["sales_amount"].replace("$", "").replace(",", "").strip()
        try:
            amt = float(amt_str)
            if amt <= 0:
                raise ValueError
            cleaned["sales_amount"] = amt  # keep as float for calculations
        except:
            return None, "sales_amount must be a positive number"

        # date
        try:
            dt = datetime.strptime(cleaned["date"], DATE_FORMAT)
            cleaned["date"] = dt.strftime(DATE_FORMAT)
        except:
            return None, "date must be in YYYY-MM-DD format"

        return cleaned, None

    except Exception as e:
        return None, f"unexpected error: {str(e)}"


def log_error(line, reason, original_row):
    """Append error to errors.txt"""
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"Line {line}: {reason} → {original_row}\n")


def compute_statistics(records):
    """Calculate all required metrics"""
    if not records:
        return {}

    total_sales = 0.0
    dept_total = defaultdict(float)
    dept_count = defaultdict(int)
    emp_total = defaultdict(float)
    dates = []

    for r in records:
        amt = r["sales_amount"]
        total_sales += amt
        dept = r["department"]
        name = r["employee_name"]
        dept_total[dept] += amt
        dept_count[dept] += 1
        emp_total[name] += amt
        dates.append(r["date"])

    avg_overall = total_sales / len(records) if records else 0

    top_emps = sorted(emp_total.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        "total_sales": total_sales,
        "avg_sale": avg_overall,
        "departments": dict(dept_total),
        "dept_counts": dict(dept_count),
        "top_employees": top_emps,
        "date_range": f"{min(dates)} to {max(dates)}" if dates else "N/A"
    }


def display_report(stats, total_processed, invalid, valid_count):
    """Print formatted report"""
    print("=" * 40)
    print("SALES ANALYSIS REPORT")
    print("=" * 40)
    print(f"Total Records Processed: {total_processed}")
    print(f"Valid Records: {valid_count}")
    print(f"Invalid Records: {invalid}\n")

    print("OVERALL STATISTICS:")
    print(f"Total Sales:     ${stats['total_sales']:,.2f}")
    print(f"Average Sale:    ${stats['avg_sale']:,.2f}\n")

    print("SALES BY DEPARTMENT:")
    for dept, total in sorted(stats["departments"].items(), key=lambda x: x[1], reverse=True):
        count = stats["dept_counts"].get(dept, 0)
        avg = total / count if count > 0 else 0
        print(f"{dept:<12} ${total:,.2f}  ({count} sales, avg ${avg:,.2f})")

    print("\nTOP 3 EMPLOYEES:")
    for i, (name, amt) in enumerate(stats["top_employees"], 1):
        print(f"{i}. {name:<15} ${amt:,.2f}")

    print(f"\nDate Range: {stats['date_range']}")
    print("=" * 40)


def export_cleaned(records):
    """Write cleaned valid data to CSV"""
    if not records:
        return

    fieldnames = ["employee_id", "employee_name", "department", "sales_amount", "date"]

    with open(OUTPUT_CLEANED, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            row = r.copy()
            row["sales_amount"] = f"{r['sales_amount']:.2f}"
            writer.writerow(row)


def export_department_summary(dept_totals):
    """Write department summary CSV (sorted by total descending)"""
    rows = []
    for dept, total in dept_totals.items():
        rows.append({"department": dept, "total_sales": total})

    rows.sort(key=lambda x: x["total_sales"], reverse=True)

    with open(OUTPUT_DEPT_SUMMARY, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["department", "total_sales"])
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "department": row["department"],
                "total_sales": f"{row['total_sales']:.2f}"
            })


if __name__ == "__main__":
    main()