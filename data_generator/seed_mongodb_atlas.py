"""Seed MongoDB Atlas with raw semi-structured polling/device events.

This script will eventually:
- read selected generated source CSVs
- create richer JSON-style event documents
- use deterministic event IDs
- upsert into MongoDB Atlas to avoid accidental duplicates
"""

def main() -> None:
    print("TODO: Seed MongoDB Atlas with enriched raw event documents.")

if __name__ == "__main__":
    main()
