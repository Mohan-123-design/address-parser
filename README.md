# Ultra Address Parser v2.0

A robust US address parser that splits messy, combined address strings into structured components (street, city, state, zip) using 12 cascading NLP-based strategies. Every output row includes a status and reason column for easy quality review.

## What It Does

Input: A CSV with messy address strings like:
```
"123 Main St Suite 4, Austin, TX 78701"
"Dr. John Smith, 456 Oak Ave, Dallas TX"
"P.O. Box 789, New York, NY, 10001-1234"
```

Output: Structured columns with:
```
Street | City | State | Zip | Status | Reason
123 Main St Suite 4 | Austin | TX | 78701 | parsed | usaddress-primary
```

## 12 Parsing Strategies (in order of confidence)

1. `usaddress` NLP library — primary strategy
2. Comma-segment analysis
3. State-anchor detection
4. ZIP code anchor parsing
5. Regex pattern matching
6. Token-based splitting
7. ...and 6 more fallback strategies

If strategy 1 fails, it tries 2. If 2 fails, it tries 3 — and so on. Every row gets the best result available.

## Usage

```bash
pip install usaddress pandas openpyxl colorama

# Place your input file as: input.xlsx or input.csv
python addresssplit1.py
```

## Output Columns

| Column | Description |
|--------|-------------|
| `Street` | Full street address |
| `City` | City name |
| `State` | 2-letter state code |
| `Zip` | ZIP or ZIP+4 code |
| `Status` | `parsed` / `partial` / `failed` |
| `Reason` | Which strategy succeeded (or why it failed) |

## Why the Status/Reason columns?

Makes manual correction fast. Filter by `Status = failed` to find only the rows that need human review — instead of checking every record.

## Tech Stack

- Python
- usaddress (NLP address parsing library)
- Pandas, openpyxl
- Colorama (progress display)
- Regular expressions
