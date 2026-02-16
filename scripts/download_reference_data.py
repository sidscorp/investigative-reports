#!/usr/bin/env python3
"""
Download reference datasets for Medicaid investigation enrichment.
Downloads NPI Registry, HCPCS codes, and other CMS reference files.
"""

import requests
import zipfile
import gzip
import shutil
from pathlib import Path
from typing import Optional
import time
from datetime import datetime


class DataDownloader:
    """Handles downloading and extracting CMS reference data."""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Medicaid Research Project)'
        })

    def download_file(self, url: str, output_path: Path, chunk_size: int = 8192) -> bool:
        """
        Download a file with progress tracking.

        Args:
            url: URL to download from
            output_path: Where to save the file
            chunk_size: Download chunk size in bytes

        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"\n📥 Downloading from: {url}")
            print(f"   Saving to: {output_path}")

            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            mb_downloaded = downloaded / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024)
                            print(f"\r   Progress: {progress:.1f}% ({mb_downloaded:.1f} MB / {mb_total:.1f} MB)", end='')

            print(f"\n   ✓ Downloaded successfully: {output_path.stat().st_size / (1024**2):.1f} MB")
            return True

        except Exception as e:
            print(f"\n   ✗ Error downloading: {e}")
            if output_path.exists():
                output_path.unlink()
            return False

    def extract_zip(self, zip_path: Path, extract_to: Optional[Path] = None) -> bool:
        """Extract a ZIP file."""
        if extract_to is None:
            extract_to = zip_path.parent

        try:
            print(f"\n📦 Extracting: {zip_path.name}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            print(f"   ✓ Extracted to: {extract_to}")
            return True
        except Exception as e:
            print(f"   ✗ Error extracting: {e}")
            return False

    def extract_gzip(self, gz_path: Path, output_path: Optional[Path] = None) -> bool:
        """Extract a GZIP file."""
        if output_path is None:
            output_path = gz_path.with_suffix('')

        try:
            print(f"\n📦 Extracting: {gz_path.name}")
            with gzip.open(gz_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"   ✓ Extracted to: {output_path}")
            return True
        except Exception as e:
            print(f"   ✗ Error extracting: {e}")
            return False

    def download_npi_registry(self) -> bool:
        """
        Download the NPI Registry full file.
        This is a large file (~6-7 GB compressed).
        """
        print("\n" + "=" * 100)
        print("🏥 DOWNLOADING NPI REGISTRY (National Provider Identifier)")
        print("=" * 100)
        print("\n⚠️  This is a LARGE file (~6-7 GB compressed, ~10-15 GB uncompressed)")
        print("   Download may take 10-30 minutes depending on your connection speed.")

        # CMS provides the file as a weekly update
        # The filename follows pattern: NPPES_Data_Dissemination_MMYYYY.zip
        # We'll try to get the latest by checking the main download page

        # For now, using the direct link to the current file
        # Note: This URL may change; CMS updates it weekly
        base_url = "https://download.cms.gov/nppes"

        # Try to find the latest file - CMS typically names it with month/year
        # We'll try a few common patterns
        possible_files = [
            "NPPES_Data_Dissemination_February_2026.zip",
            "NPPES_Data_Dissemination_January_2026.zip",
            "NPPES_Data_Dissemination_December_2025.zip",
        ]

        zip_path = self.data_dir / "npi_registry.zip"
        csv_path = self.data_dir / "npi_registry.csv"

        # Check if already downloaded
        if csv_path.exists():
            print(f"\n✓ NPI Registry already exists: {csv_path}")
            print(f"  File size: {csv_path.stat().st_size / (1024**3):.2f} GB")
            return True

        print("\n🔍 Attempting to locate the latest NPI file...")

        # Try each possible filename
        for filename in possible_files:
            url = f"{base_url}/{filename}"
            print(f"\n   Trying: {url}")

            try:
                # Quick HEAD request to check if file exists
                response = self.session.head(url, timeout=10)
                if response.status_code == 200:
                    print(f"   ✓ Found: {filename}")

                    # Download the file
                    if self.download_file(url, zip_path):
                        # Extract it
                        if self.extract_zip(zip_path):
                            # Find the main CSV file (usually npidata_pfile_*.csv)
                            extracted_files = list(self.data_dir.glob("npidata_pfile*.csv"))
                            if extracted_files:
                                # Rename to standard name
                                extracted_files[0].rename(csv_path)
                                print(f"\n   ✓ NPI Registry ready: {csv_path}")

                                # Clean up zip file to save space
                                zip_path.unlink()
                                print(f"   ✓ Cleaned up temporary files")
                                return True
                    break
            except Exception as e:
                print(f"   ✗ Not available: {e}")
                continue

        print("\n⚠️  AUTOMATIC DOWNLOAD FAILED")
        print("\n📋 MANUAL DOWNLOAD INSTRUCTIONS:")
        print("   1. Visit: https://download.cms.gov/nppes/NPI_Files.html")
        print("   2. Download: 'NPPES Data Dissemination' (Full Replacement Monthly File)")
        print("   3. Extract the ZIP file")
        print("   4. Rename the main CSV file (npidata_pfile_*.csv) to: npi_registry.csv")
        print(f"   5. Place it in: {self.data_dir.absolute()}")
        return False

    def download_hcpcs_codes(self) -> bool:
        """
        Download HCPCS code descriptions.
        """
        print("\n" + "=" * 100)
        print("💊 DOWNLOADING HCPCS CODE DESCRIPTIONS")
        print("=" * 100)

        # HCPCS codes are typically provided in quarterly files
        # For a comprehensive list, we'll try to download from CMS

        output_path = self.data_dir / "hcpcs_codes.csv"

        if output_path.exists():
            print(f"\n✓ HCPCS codes already exist: {output_path}")
            return True

        print("\n⚠️  HCPCS codes require manual download from CMS")
        print("\n📋 MANUAL DOWNLOAD INSTRUCTIONS:")
        print("   1. Visit: https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system")
        print("   2. Download: HCPCS Level II Code File (most recent quarter)")
        print("   3. Extract the file (usually Excel or text format)")
        print("   4. Convert to CSV with columns: CODE, SHORT_DESCRIPTION, LONG_DESCRIPTION")
        print(f"   5. Save as: {output_path.absolute()}")
        print("\n   Alternative: Use the HCPCS API or download from data.cms.gov")

        return False

    def create_sample_hcpcs_file(self) -> bool:
        """
        Create a sample HCPCS file with common codes found in our dataset.
        This is a fallback if the full file isn't available.
        """
        print("\n" + "=" * 100)
        print("📝 CREATING SAMPLE HCPCS REFERENCE FILE")
        print("=" * 100)

        output_path = self.data_dir / "hcpcs_codes_sample.csv"

        # Common codes from our dataset analysis
        sample_codes = """CODE,SHORT_DESCRIPTION,LONG_DESCRIPTION,CATEGORY
T1019,Personal Care Services,Personal care services per 15 minutes,Home Health
T1015,Clinic Visit,Clinic visit/encounter all-inclusive,Clinic Services
T2016,Habilitation Services,Habilitation training hourly per diem,Behavioral Health
99213,Office Visit,Office/outpatient visit est patient 20-29 min,Office Visits
S5125,Attendant Care,Attendant care services per hour,Home Health
S9123,Nursing Care,Nursing care in the home by RN per hour,Home Health
99214,Office Visit,Office/outpatient visit est patient 30-39 min,Office Visits
H2015,Comprehensive Community Support,Comprehensive community support services per diem,Behavioral Health
H0015,Alcohol/Drug Services,Alcohol and/or drug services intensive outpatient,Behavioral Health
H2017,Psychosocial Rehabilitation,Psychosocial rehabilitation services per 15 minutes,Behavioral Health
T1017,Targeted Case Management,Targeted case management per month,Case Management
T1020,Personal Care Services,Personal care services per diem,Home Health
90999,Unlisted Immunization,Unlisted vaccine/toxoid,Immunizations
A0427,Ambulance Service,Ambulance service ALS emergency transport level 1,Transportation
99232,Hospital Visit,Subsequent hospital care per day 25 min,Hospital Services
J2326,Injection Nusinersen,Injection nusinersen 0.1 mg,Drugs/Biologicals
A4657,Syringe,Syringe with needle for external insulin pump,Supplies
S9124,Nurse Aide,Nursing care in the home by LPN per hour,Home Health
8888888,Unknown/Invalid,Unknown or invalid procedure code,Unknown
S5100,Day Habilitation,Day habilitation per hour,Behavioral Health
99509,Home Visit,Home visit for assistance with ADLs,Home Health
T2041,Respite Care,Respite care services per diem,Home Health"""

        try:
            with open(output_path, 'w') as f:
                f.write(sample_codes)
            print(f"\n✓ Created sample HCPCS file: {output_path}")
            print(f"  Contains {len(sample_codes.split(chr(10))) - 1} common codes")
            print("\n⚠️  This is a SAMPLE file - not comprehensive!")
            print("  For full analysis, download the complete HCPCS file from CMS")
            return True
        except Exception as e:
            print(f"\n✗ Error creating sample file: {e}")
            return False

    def download_all(self):
        """Download all reference datasets."""
        print("\n" + "=" * 100)
        print("📥 CMS REFERENCE DATA DOWNLOADER")
        print("=" * 100)
        print(f"\nData directory: {self.data_dir.absolute()}")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        results = {}

        # 1. NPI Registry
        print("\n\n[1/2] NPI Registry...")
        results['npi'] = self.download_npi_registry()

        # 2. HCPCS Codes
        print("\n\n[2/2] HCPCS Codes...")
        results['hcpcs'] = self.download_hcpcs_codes()

        # If HCPCS failed, create sample file
        if not results['hcpcs']:
            print("\n   Creating sample HCPCS file as fallback...")
            results['hcpcs_sample'] = self.create_sample_hcpcs_file()

        # Summary
        print("\n\n" + "=" * 100)
        print("📊 DOWNLOAD SUMMARY")
        print("=" * 100)

        for dataset, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED (Manual download required)"
            print(f"   {dataset.upper()}: {status}")

        print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # List downloaded files
        print("\n📁 Files in data directory:")
        for file in sorted(self.data_dir.glob("*")):
            if file.is_file():
                size_mb = file.stat().st_size / (1024**2)
                print(f"   • {file.name} ({size_mb:.1f} MB)")

        print("\n" + "=" * 100)


if __name__ == "__main__":
    downloader = DataDownloader()
    downloader.download_all()
