#!/usr/bin/env python3
"""
Utility to enrich NPI numbers with provider names and types using the CMS NPI Registry API.
This demonstrates how to humanize the data and distinguish between institutions vs individuals.
"""

import requests
import time
from typing import Dict, Optional


def lookup_npi(npi: str) -> Optional[Dict]:
    """
    Look up an NPI in the CMS NPI Registry API.

    Args:
        npi: The National Provider Identifier number

    Returns:
        Dictionary with provider information or None if not found
    """
    url = "https://npiregistry.cms.hhs.gov/api/"
    params = {
        "number": npi,
        "version": "2.1"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("result_count", 0) > 0:
            result = data["results"][0]

            # Determine if organization or individual
            entity_type = "Organization" if result["enumeration_type"] == "NPI-2" else "Individual"

            # Get name based on entity type
            if entity_type == "Organization":
                name = result.get("basic", {}).get("organization_name", "Unknown")
            else:
                first_name = result.get("basic", {}).get("first_name", "")
                last_name = result.get("basic", {}).get("last_name", "")
                name = f"{first_name} {last_name}".strip() or "Unknown"

            # Get primary taxonomy (specialty)
            taxonomies = result.get("taxonomies", [])
            specialty = "Unknown"
            if taxonomies:
                specialty = taxonomies[0].get("desc", "Unknown")

            # Get address
            addresses = result.get("addresses", [])
            state = "Unknown"
            if addresses:
                for addr in addresses:
                    if addr.get("address_purpose") == "LOCATION":
                        state = addr.get("state", "Unknown")
                        break

            return {
                "npi": npi,
                "name": name,
                "entity_type": entity_type,
                "specialty": specialty,
                "state": state,
                "raw_data": result
            }

    except Exception as e:
        print(f"Error looking up NPI {npi}: {e}")
        return None


def investigate_top_npis():
    """
    Look up the key NPIs from our investigation.
    """
    print("=" * 100)
    print("🔍 NPI ENRICHMENT - Identifying the Whales")
    print("=" * 100)

    # Key NPIs from our investigation
    npis_to_check = {
        "1376609297": "The $118M whale - appears in all top 20 payments",
        "1417262056": "Top overall spender - $7.18 billion total",
        "1699703827": "2nd top spender - $6.78 billion",
        "1528351285": "Highest cost per beneficiary - $288K per patient",
        "1336117670": "2nd highest cost per beneficiary - $215K per patient",
        "1710183058": "Largest reversal - $183K clawback",
        "1417409509": "Highest claims per patient - 1,454 claims/patient"
    }

    print("\n🎯 ENRICHMENT RESULTS:\n")

    for npi, description in npis_to_check.items():
        print("-" * 100)
        print(f"\n📌 {description}")
        print(f"   NPI: {npi}")

        info = lookup_npi(npi)

        if info:
            print(f"   Name: {info['name']}")
            print(f"   Type: {info['entity_type']}")
            print(f"   Specialty: {info['specialty']}")
            print(f"   State: {info['state']}")

            # Provide context based on entity type
            if info['entity_type'] == 'Organization':
                print(f"\n   ✓ Context: This is an ORGANIZATION - high spending may be normal")
            else:
                print(f"\n   ⚠️  Context: This is an INDIVIDUAL - high spending warrants investigation")
        else:
            print("   ❌ NPI not found in registry")

        # Be nice to the API - rate limit
        time.sleep(0.5)

    print("\n" + "=" * 100)
    print("✅ ENRICHMENT COMPLETE")
    print("=" * 100)
    print("""
    🎯 KEY INSIGHT: Now you can write stories like:

    ❌ BAD (without enrichment):
       "NPI 1376609297 received $118 million in a single payment!"

    ✅ GOOD (with enrichment):
       "XYZ Health Plan (an insurance organization) received $118M in managed care
        capitation payments, representing coverage for 39,765 Medicaid beneficiaries.
        This translates to $2,990 per patient - within normal range for comprehensive
        managed care plans."

    or

    ❌ BAD (without enrichment):
       "NPI 1528351285 charged $288,000 per patient!"

    ✅ GOOD (with enrichment):
       "ABC Specialty Pharmacy charged an average of $288K per patient - 2,600x the
        median. Investigation shows this is a specialty pharmacy dispensing ultra-rare
        gene therapies with per-treatment costs exceeding $2M."

    💡 The enrichment transforms RAW DATA into RESPONSIBLE JOURNALISM.
    """)


if __name__ == "__main__":
    print("\n🌐 This script uses the free CMS NPI Registry API")
    print("   No API key required - rate limited to ~5 requests/second\n")

    investigate_top_npis()
