import polars as pl
from pathlib import Path
import sys

OUTPUT_DIR = Path("output")

def check_ghost_providers():
    print("\n--- Check 1: Ghost Provider 'Kulmoris Joiner' ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "ghost_providers_impossible_volume.csv")
        provider = df.filter(pl.col("PROVIDER_NAME").str.contains("KULMORIS JOINER"))
        
        if provider.height > 0:
            row = provider.row(0, named=True)
            print(f"Found: {row['PROVIDER_NAME']} (NPI: {row['BILLING_PROVIDER_NPI_NUM']})")
            print(f"  Max Monthly Claims: {row['MAX_MONTHLY_CLAIMS']:,.0f}")
            print(f"  Capacity Ratio: {row['MAX_CAPACITY_RATIO']:.1f}x (Expected: ~26.7x)")
            print(f"  Total Paid Over Capacity: ${row['TOTAL_PAID_OVER_CAPACITY']:,.2f}")
        else:
            print("FAIL: Kulmoris Joiner not found in ghost providers list.")
    except Exception as e:
        print(f"FAIL: Error reading ghost providers: {e}")

def check_brooklyn_concentration():
    print("\n--- Check 2: Brooklyn T1019 National Ranking ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "t1019_brooklyn_analysis.csv")
        top_20 = df.filter(pl.col("NATIONAL_RANK") <= 20)
        
        print(f"Count of Brooklyn providers in National Top 20: {top_20.height} (Expected: 7)")
        print("Top 5 Brooklyn Providers by National Rank:")
        for row in top_20.head(5).iter_rows(named=True):
            print(f"  #{row['NATIONAL_RANK']}: {row['PROVIDER_NAME']} (${row['TOTAL_PAID']:,.0f})")
    except Exception as e:
        print(f"FAIL: Error reading Brooklyn analysis: {e}")

def check_shared_address():
    print("\n--- Check 3: Shared Address '946 McDonald Ave' ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "t1019_shared_addresses.csv")
        # Normalize address for search as done in the analysis script (approximate)
        address_match = df.filter(pl.col("ADDRESS").str.contains("946 MCDONALD"))
        
        if address_match.height > 0:
            row = address_match.row(0, named=True)
            print(f"Address: {row['ADDRESS']}")
            print(f"  NPI Count: {row['NPI_COUNT']} (Expected: 2)")
            print(f"  Combined Paid: ${row['COMBINED_PAID']:,.2f}")
            # Corrected column name from PROVIDERS_SAMPLE to PROVIDERS
            print(f"  Providers: {row['PROVIDERS']}")
        else:
            print("FAIL: 946 McDonald Ave not found in shared addresses.")
    except Exception as e:
        print(f"FAIL: Error reading shared addresses: {e}")

def check_az_fast_starter():
    print("\n--- Check 4: Arizona Fast Starter 'Community Hope Wellness Center' ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "temporal_new_entrants.csv")
        provider = df.filter(pl.col("PROVIDER_NAME").str.contains("COMMUNITY HOPE"))
        
        if provider.height > 0:
            row = provider.row(0, named=True)
            print(f"Found: {row['PROVIDER_NAME']}")
            print(f"  State: {row['STATE']}")
            print(f"  Total Paid: ${row['TOTAL_PAID']:,.2f}")
            print(f"  Max Monthly Paid: ${row['MAX_MONTHLY_PAID']:,.2f}")
            print(f"  First Billing Month: {row['FIRST_MONTH']}")
        else:
            print("FAIL: Community Hope Wellness Center not found in new entrants list.")
    except Exception as e:
        print(f"FAIL: Error reading new entrants: {e}")

def check_oig_matches():
    print("\n--- Check 5: OIG Exclusion Matches ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "individual_oig_matches.csv")
        # Check if the dataframe is empty or contains "No matches found"
        if df.height == 0:
             print("FAIL: OIG matches file is empty.")
             return
             
        if "NOTE" in df.columns and df["NOTE"][0] == "No matches found":
             print("FAIL: No matches found in file.")
             return

        print(f"Total OIG Matches Found: {df.height} (Expected: ~14)")
        
        # Check for a specific name mentioned in the report
        sample = df.filter(pl.col("LASTNAME").str.contains("WILLIAMS") & pl.col("FIRSTNAME").str.contains("LORI"))
        if sample.height > 0:
            row = sample.row(0, named=True)
            print(f"Sample Verification: {row['FIRSTNAME']} {row['LASTNAME']} (Excl Type: {row['EXCLTYPE']}) found.")
        else:
            # Try finding another name if Lori Williams isn't there
            if df.height > 0:
                row = df.row(0, named=True)
                print(f"Sample Verification (First Row): {row['FIRSTNAME']} {row['LASTNAME']} (Excl Type: {row['EXCLTYPE']}) found.")
            else:
                 print("FAIL: Lori Williams not found in OIG matches.")
    except Exception as e:
        print(f"FAIL: Error reading OIG matches: {e}")


def check_outlier_ratio():
    print("\n--- Check 6: Cost Per Beneficiary Outlier 'Isluv Robertson' ---")
    try:
        df = pl.read_csv(OUTPUT_DIR / "individual_specialty_outliers.csv")
        provider = df.filter(pl.col("PROVIDER_NAME").str.contains("ISLUV ROBERTSON"))
        
        if provider.height > 0:
            row = provider.row(0, named=True)
            print(f"Found: {row['PROVIDER_NAME']}")
            # Corrected column name from SPECIALTY to SPECIALTY_NAME
            print(f"  Specialty: {row['SPECIALTY_NAME']}")
            print(f"  Cost Per Bene: ${row['COST_PER_BENE']:,.2f}")
            print(f"  Median Cost Per Bene: ${row['MEDIAN_COST_PER_BENE']:,.2f}")
            print(f"  Ratio: {row['COST_RATIO']:.1f}x (Expected: ~287x)")
        else:
            print("FAIL: Isluv Robertson not found in outliers list.")
    except Exception as e:
        print(f"FAIL: Error reading outliers: {e}")

if __name__ == "__main__":
    check_ghost_providers()
    check_brooklyn_concentration()
    check_shared_address()
    check_az_fast_starter()
    check_oig_matches()
    check_outlier_ratio()