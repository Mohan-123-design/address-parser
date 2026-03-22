"""
ULTRA-ROBUST ADDRESS PARSER v2.0
─────────────────────────────────────────────────────────
• usaddress NLP library as primary parser
• Comma-segment classification engine
• 12 cascading parse strategies with confidence scoring
• Per-component validation & post-processing
• Status & Reason columns in single output file
• Row-by-row debug tracing
─────────────────────────────────────────────────────────
pip install usaddress pandas openpyxl tqdm colorama
"""

import pandas as pd
import re
import os
import shutil
from datetime import datetime
from pathlib import Path
import json
from tqdm import tqdm
from colorama import init, Fore, Style

init(autoreset=True)

# ── Try importing usaddress ──────────────────────────────
try:
    import usaddress

    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False


# ══════════════════════════════════════════════════════════
#  ULTRA ADDRESS PARSER  v2.0
# ══════════════════════════════════════════════════════════
class UltraAddressParser:
    """
    12-method cascading address parser.
    Priority:  usaddress.tag → usaddress.parse → comma-classify
             → 7 regex strategies → fallback
    Every method is scored; the highest-scoring result wins.
    """

    # usaddress label groups ──────────────────────────────
    STREET_TAGS = {
        "AddressNumber",
        "AddressNumberPrefix",
        "AddressNumberSuffix",
        "StreetNamePreDirectional",
        "StreetNamePreModifier",
        "StreetNamePreType",
        "StreetName",
        "StreetNamePostDirectional",
        "StreetNamePostModifier",
        "StreetNamePostType",
        "SubaddressType",
        "SubaddressIdentifier",
        "OccupancyType",
        "OccupancyIdentifier",
        "BuildingName",
        "CornerOf",
        "IntersectionSeparator",
        "LandmarkName",
        "USPSBoxType",
        "USPSBoxID",
        "USPSBoxGroupType",
        "USPSBoxGroupID",
    }

    SKIP_TAGS = {"Recipient", "NotAddress", "CountryName"}

    # ─────────────────────────────────────────────────────
    #  INIT
    # ─────────────────────────────────────────────────────
    def __init__(self, input_file, address_column="Address", debug=True):
        self.input_file = input_file
        self.address_column = address_column
        self.debug = debug
        self.output_file = (
            f"Addresses_Split_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        self.progress_file = "address_parse_progress.json"
        self.backup_folder = "address_backups"

        self.stats = {
            "total_rows": 0,
            "processed": 0,
            "fully_parsed": 0,
            "partially_parsed": 0,
            "failed": 0,
            "empty": 0,
        }

        Path(self.backup_folder).mkdir(exist_ok=True)
        self._init_reference_data()
        self._compile_patterns()

    # ─────────────────────────────────────────────────────
    #  REFERENCE DATA
    # ─────────────────────────────────────────────────────
    def _init_reference_data(self):

        self.us_states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC", "PR", "VI", "GU", "AS", "MP",
        }

        self.state_name_to_abbr = {
            "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
            "california": "CA", "colorado": "CO", "connecticut": "CT",
            "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
            "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
            "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
            "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
            "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
            "montana": "MT", "nebraska": "NE", "nevada": "NV",
            "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
            "new york": "NY", "north carolina": "NC", "north dakota": "ND",
            "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
            "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
            "south dakota": "SD", "tennessee": "TN", "texas": "TX",
            "utah": "UT", "vermont": "VT", "virginia": "VA",
            "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
            "wyoming": "WY", "district of columbia": "DC", "puerto rico": "PR",
        }

        self.street_suffixes = {
            "st", "street", "ave", "avenue", "rd", "road", "dr", "drive",
            "ln", "lane", "blvd", "boulevard", "ct", "court", "cir", "circle",
            "way", "pl", "place", "pkwy", "parkway", "hwy", "highway",
            "pike", "ter", "terrace", "trl", "trail", "aly", "alley",
            "brg", "bridge", "expy", "expressway", "fwy", "freeway",
            "sq", "square", "tpke", "turnpike", "run", "row", "path", "mall",
            "loop", "cres", "crescent", "xing", "crossing", "jct", "junction",
            "pt", "point", "cove", "cv", "crk", "creek", "est", "estates",
            "grn", "green", "holw", "hollow", "isle", "knl", "knoll",
            "lk", "lake", "lgt", "light", "mdw", "meadow", "mtn", "mountain",
            "pass", "riv", "river", "spg", "spring", "vly", "valley",
            "vw", "view", "vis", "vista", "walk", "ways", "commons",
        }

        self.unit_keywords_set = {
            "suite", "ste", "apt", "apartment", "unit", "floor", "fl",
            "bldg", "building", "room", "rm", "dept", "department",
            "lot", "space", "slip", "pier", "dock", "hangar",
            "trailer", "trlr", "upper", "lower", "rear", "front",
            "penthouse", "ph", "basement", "bsmt", "lobby", "lbby",
            "office", "ofc", "wing",
        }

        self.noise_keywords = [
            "family medicine", "internal medicine", "pediatrics", "cardiology",
            "neurology", "oncology", "orthopedics", "psychiatry", "radiology",
            "emergency medicine", "general surgery", "obstetrics", "gynecology",
            "dermatology", "urology", "ophthalmology", "anesthesiology",
            "pathology", "primary care", "urgent care", "walk-in clinic",
            "health center", "medical center", "hospital", "clinic",
            "nyc health", "hospitals", "health alliance", "fetal care center",
            "women's health", "women's specialists", "ob/gyn", "maternit",
            "associates", "physicians", "medical group", "healthcare",
            "wellness center", "rehabilitation", "rehab center",
            "specialty center", "diagnostic", "imaging center",
            "ambulatory", "outpatient", "inpatient",
            "department of", "division of", "section of",
            "accepting new patients", "board certified",
            "phone:", "fax:", "tel:", "email:", "website:",
        ]

    # ─────────────────────────────────────────────────────
    #  COMPILED REGEX PATTERNS  (built once, reused)
    # ─────────────────────────────────────────────────────
    def _compile_patterns(self):

        suf = "|".join(re.escape(s) for s in self.street_suffixes)
        unit = "|".join(re.escape(u) for u in self.unit_keywords_set)

        self.re_street_suffix = re.compile(
            rf"\b({suf})\.?\b", re.IGNORECASE
        )
        self.re_unit_start = re.compile(
            rf"^(#|{unit})\b", re.IGNORECASE
        )
        self.re_has_unit = re.compile(
            rf"\b({unit})\s+[\w\-]+", re.IGNORECASE
        )
        self.re_state_zip = re.compile(
            r"^([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$"
        )
        self.re_zip_only = re.compile(r"^(\d{5}(?:-\d{4})?)$")
        self.re_state_only = re.compile(r"^([A-Z]{2})$")
        self.re_city_state_zip = re.compile(
            r"^([A-Za-z][A-Za-z\s.\-']+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$"
        )
        self.re_city_state = re.compile(
            r"^([A-Za-z][A-Za-z\s.\-']+?)\s+([A-Z]{2})$"
        )
        self.re_standard = re.compile(
            r"^(.+?),\s*(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$"
        )
        self.re_standard_nozip = re.compile(
            r"^(.+?),\s*(.+?),\s*([A-Z]{2})\s*$"
        )
        self.re_zip_end = re.compile(r"\b(\d{5}(?:-\d{4})?)\s*$")
        self.re_state_end = re.compile(r"\b([A-Z]{2})\s*$")
        self.re_csz_anywhere = re.compile(
            r"([A-Za-z][A-Za-z\s.\-']+?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$"
        )
        self.re_zip_anywhere = re.compile(r"(\d{5}(?:-\d{4})?)")
        self.re_state_word = re.compile(r"\b([A-Z]{2})\b")
        self.re_po_box = re.compile(r"\b(P\.?O\.?\s*Box)\b", re.IGNORECASE)
        self.re_has_number = re.compile(r"\d")
        self.re_has_street_kw = re.compile(
            r"\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|boulevard|blvd|"
            r"parkway|pkwy|highway|hwy|suite|ste|floor|fl|building|bldg|"
            r"court|ct|circle|cir|plaza|way|place|pl|box)\b",
            re.IGNORECASE,
        )
        self.re_state_zip_inline = re.compile(
            r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b"
        )
        self.re_distance_prefix = re.compile(
            r"^\d+\.?\d*\s*mi\s*[-–—]\s*", re.IGNORECASE
        )
        self.re_addr_prefix = re.compile(
            r"^(Address|Location|Addr)\s*:\s*", re.IGNORECASE
        )
        self.re_phone_line = re.compile(
            r"(Phone|Fax|Tel|Email|Website)\s*:.+", re.IGNORECASE
        )
        self.re_city_candidate = re.compile(
            r"^[A-Za-z][A-Za-z\s.\-']+$"
        )

    # ─────────────────────────────────────────────────────
    #  ADDRESS CLEANING
    # ─────────────────────────────────────────────────────
    def clean_address(self, address):
        """Deep-clean raw address text before parsing."""
        if pd.isna(address) or not address:
            return ""

        text = str(address).strip().strip("\"'")

        # Treat explicit N/A values as empty
        if text.lower() in ("n/a", "na", "none", "null", "-", "--", ".", ".."):
            return ""

        # Remove bullet points → keep only text before first bullet
        text = re.split(r"[•●▪▸►]", text)[0].strip()

        # Remove distance prefix  ("22.1 mi – ")
        text = self.re_distance_prefix.sub("", text)

        # Remove "Address:" / "Location:" prefix
        text = self.re_addr_prefix.sub("", text)

        # Remove phone / fax / email lines
        text = self.re_phone_line.sub("", text)

        # Normalize whitespace characters
        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", ", ")

        # ── Per-line noise filtering ──
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        filtered = []
        for line in lines:
            lower = line.lower()

            is_noise = any(kw in lower for kw in self.noise_keywords)
            has_number = bool(self.re_has_number.search(line))
            has_street_kw = bool(self.re_has_street_kw.search(line))
            has_state_zip = bool(self.re_state_zip_inline.search(line))

            if has_number or has_street_kw or has_state_zip:
                filtered.append(line)
            elif not is_noise and len(line) > 2:
                # potential city name or other useful info
                filtered.append(line)

        if not filtered:
            filtered = [ln for ln in lines if self.re_has_number.search(ln)]
        if not filtered:
            filtered = lines

        text = ", ".join(filtered)

        # Collapse multiple commas / spaces
        text = re.sub(r",\s*,+", ",", text)
        text = re.sub(r"\s+", " ", text)
        text = text.strip(" ,")

        return text

    # ─────────────────────────────────────────────────────
    #  COMMA-SEGMENT CLASSIFICATION
    # ─────────────────────────────────────────────────────
    def _classify_segment(self, seg):
        """
        Classify a single comma-separated segment.
        Returns  (label, {extracted_data})
        Labels:  STATE_ZIP | ZIP | STATE | CITY_STATE_ZIP |
                 CITY_STATE | UNIT | STREET | ORG | CITY | UNKNOWN
        """
        seg = seg.strip()
        if not seg:
            return "EMPTY", {}

        # STATE_ZIP  "IL 62704"
        m = self.re_state_zip.match(seg)
        if m and m.group(1) in self.us_states:
            return "STATE_ZIP", {"state": m.group(1), "zip": m.group(2)}

        # ZIP only  "62704"
        m = self.re_zip_only.match(seg)
        if m:
            return "ZIP", {"zip": m.group(1)}

        # STATE only  "IL"
        m = self.re_state_only.match(seg)
        if m and m.group(1) in self.us_states:
            return "STATE", {"state": m.group(1)}

        # CITY_STATE_ZIP  "Springfield IL 62704"
        m = self.re_city_state_zip.match(seg)
        if m and m.group(2) in self.us_states:
            return "CITY_STATE_ZIP", {
                "city": m.group(1).strip(),
                "state": m.group(2),
                "zip": m.group(3),
            }

        # CITY_STATE  "Springfield IL"
        m = self.re_city_state.match(seg)
        if m and m.group(2) in self.us_states:
            return "CITY_STATE", {
                "city": m.group(1).strip(),
                "state": m.group(2),
            }

        # UNIT  "Suite 200", "#301"
        if self.re_unit_start.match(seg):
            return "UNIT", {"unit": seg}

        # STREET  starts with digit, has suffix, or PO Box
        starts_digit = seg and seg[0].isdigit()
        has_suffix = bool(self.re_street_suffix.search(seg))
        has_po = bool(self.re_po_box.search(seg))
        if starts_digit or has_suffix or has_po:
            return "STREET", {"street": seg}

        # ORG / noise
        lower = seg.lower()
        if any(kw in lower for kw in self.noise_keywords):
            return "ORG", {"org": seg}

        # Likely CITY  (alpha words, reasonable length)
        if self.re_city_candidate.match(seg) and len(seg) < 45:
            return "CITY", {"city": seg}

        return "UNKNOWN", {"text": seg}

    def _parse_by_comma_classification(self, text):
        """Split on commas, classify each segment, assemble components."""
        segments = [s.strip() for s in text.split(",") if s.strip()]
        if not segments:
            return None

        classifications = [
            (seg, *self._classify_segment(seg)) for seg in segments
        ]

        street_parts = []
        unit_parts = []
        city = None
        state = None
        zipcode = None

        for _seg, label, data in classifications:
            if label == "STREET":
                street_parts.append(data["street"])
            elif label == "UNIT":
                unit_parts.append(data["unit"])
            elif label == "CITY" and city is None:
                city = data["city"]
            elif label == "STATE" and state is None:
                state = data["state"]
            elif label == "ZIP" and zipcode is None:
                zipcode = data["zip"]
            elif label == "STATE_ZIP":
                state = state or data["state"]
                zipcode = zipcode or data["zip"]
            elif label == "CITY_STATE":
                city = city or data["city"]
                state = state or data["state"]
            elif label == "CITY_STATE_ZIP":
                city = city or data["city"]
                state = state or data["state"]
                zipcode = zipcode or data["zip"]
            elif label == "UNKNOWN":
                # use as street if we don't have one, else city
                if not street_parts:
                    street_parts.append(data.get("text", _seg))
                elif city is None:
                    city = data.get("text", _seg)
            # ORG / EMPTY → skip

        if unit_parts:
            street_parts.extend(unit_parts)

        street = ", ".join(street_parts) if street_parts else None

        if street or city or state or zipcode:
            return street, city, state, zipcode
        return None

    # ─────────────────────────────────────────────────────
    #  USADDRESS INTEGRATION
    # ─────────────────────────────────────────────────────
    def _usaddress_tag(self, text):
        """Primary usaddress method — usaddress.tag()."""
        if not HAS_USADDRESS:
            return None
        try:
            tagged, _addr_type = usaddress.tag(text)
            return self._resolve_tags_ordered(tagged)
        except usaddress.RepeatedLabelError:
            return None  # will fall through to parse()
        except Exception:
            return None

    def _usaddress_parse(self, text):
        """Secondary usaddress method — usaddress.parse() handles repeated labels."""
        if not HAS_USADDRESS:
            return None
        try:
            parsed = usaddress.parse(text)
            street_tokens = []
            city_tokens = []
            state_tokens = []
            zip_tokens = []

            for token, tag in parsed:
                tok = token.strip(" ,")
                if not tok or tag in self.SKIP_TAGS:
                    continue
                if tag in self.STREET_TAGS:
                    street_tokens.append(tok)
                elif tag == "PlaceName":
                    city_tokens.append(tok)
                elif tag == "StateName":
                    state_tokens.append(tok)
                elif tag == "ZipCode":
                    zip_tokens.append(tok)

            street = " ".join(street_tokens) if street_tokens else None
            city = " ".join(city_tokens) if city_tokens else None
            state = self._normalize_state(" ".join(state_tokens)) if state_tokens else None
            zipcode = " ".join(zip_tokens) if zip_tokens else None

            if street or city or state or zipcode:
                return street, city, state, zipcode
            return None
        except Exception:
            return None

    def _resolve_tags_ordered(self, tagged_dict):
        """Convert usaddress OrderedDict → (street, city, state, zip)."""
        street_parts = []
        for tag, value in tagged_dict.items():
            if tag in self.STREET_TAGS:
                street_parts.append(value)

        street = " ".join(street_parts) if street_parts else None
        city = tagged_dict.get("PlaceName")
        state = self._normalize_state(tagged_dict.get("StateName"))
        zipcode = tagged_dict.get("ZipCode")

        if street:
            street = street.strip(" ,")
        if city:
            city = city.strip(" ,")

        if street or city or state or zipcode:
            return street, city, state, zipcode
        return None

    def _normalize_state(self, state):
        """Normalize state to 2-letter abbreviation."""
        if not state:
            return None
        s = state.strip(" ,.")
        upper = s.upper()
        if upper in self.us_states:
            return upper
        # full name lookup
        normalized = re.sub(r"\s+", " ", s.lower().strip())
        abbr = self.state_name_to_abbr.get(normalized)
        if abbr:
            return abbr
        return upper if len(s) == 2 else None

    # ─────────────────────────────────────────────────────
    #  REGEX PARSING METHODS
    # ─────────────────────────────────────────────────────
    def _regex_standard(self, text):
        """Street, City, ST ZIP"""
        m = self.re_standard.match(text)
        if m and m.group(3) in self.us_states:
            return m.group(1).strip(), m.group(2).strip(), m.group(3), m.group(4)
        return None

    def _regex_standard_nozip(self, text):
        """Street, City, ST  (no zip)"""
        m = self.re_standard_nozip.match(text)
        if m and m.group(3) in self.us_states:
            return m.group(1).strip(), m.group(2).strip(), m.group(3), None
        return None

    def _regex_backwards(self, text):
        """Extract ZIP end → state → split remainder."""
        zm = self.re_zip_end.search(text)
        if not zm:
            return None
        zipcode = zm.group(1)
        remain = text[: zm.start()].strip().rstrip(",").strip()

        sm = self.re_state_end.search(remain)
        if not sm or sm.group(1) not in self.us_states:
            return None
        state = sm.group(1)
        remain = remain[: sm.start()].strip().rstrip(",").strip()

        parts = [p.strip() for p in remain.split(",") if p.strip()]
        if len(parts) >= 2:
            city = parts[-1]
            street = ", ".join(parts[:-1])
        elif len(parts) == 1:
            idx = self._find_street_end(remain)
            if 0 < idx < len(remain) - 3:
                street = remain[:idx].strip()
                city = remain[idx:].strip().lstrip(",").strip()
            else:
                street = remain
                city = None
        else:
            street, city = remain or None, None

        return street, city, state, zipcode

    def _regex_csz_anywhere(self, text):
        """Find City, ST ZIP anywhere."""
        m = self.re_csz_anywhere.search(text)
        if m and m.group(2) in self.us_states:
            city = m.group(1).strip().rstrip(",")
            state = m.group(2)
            zipcode = m.group(3)
            street = text[: m.start()].strip().rstrip(",")
            if street:
                return street, city, state, zipcode
        return None

    def _regex_full_state_name(self, text):
        """Handle full state names (e.g. 'New York')."""
        for full_name, abbr in self.state_name_to_abbr.items():
            pattern = r"\b" + re.escape(full_name) + r"\b"
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                before = text[: m.start()].strip().rstrip(",")
                after = text[m.end() :].strip().lstrip(",").strip()
                zm = re.match(r"^(\d{5}(?:-\d{4})?)", after)
                zipcode = zm.group(1) if zm else None

                parts = [p.strip() for p in before.split(",") if p.strip()]
                if len(parts) >= 2:
                    city = parts[-1]
                    street = ", ".join(parts[:-1])
                elif parts:
                    street, city = parts[0], None
                else:
                    street, city = before or None, None

                return street, city, abbr, zipcode
        return None

    def _regex_state_only(self, text):
        """Find first valid state abbreviation."""
        for m in self.re_state_word.finditer(text):
            if m.group(1) in self.us_states:
                state = m.group(1)
                before = text[: m.start()].strip().rstrip(",")
                after = text[m.end() :].strip()
                zm = re.match(r"^(\d{5}(?:-\d{4})?)", after)
                zipcode = zm.group(1) if zm else None

                parts = [p.strip() for p in before.split(",") if p.strip()]
                if len(parts) >= 2:
                    city = parts[-1]
                    street = ", ".join(parts[:-1])
                elif parts:
                    street, city = parts[0], None
                else:
                    street, city = before or None, None

                return street, city, state, zipcode
        return None

    def _regex_aggressive(self, text):
        """Extract any zip & state found anywhere, reconstruct."""
        zm = self.re_zip_anywhere.search(text)
        zipcode = zm.group(1) if zm else None

        state = None
        for m in self.re_state_word.finditer(text):
            if m.group(1) in self.us_states:
                state = m.group(1)
                break

        if not state and not zipcode:
            return None

        remaining = text
        if zipcode:
            remaining = remaining.replace(zipcode, "", 1)
        if state:
            remaining = re.sub(r"\b" + re.escape(state) + r"\b", "", remaining, count=1)
        remaining = re.sub(r",\s*,", ",", remaining).strip(" ,")

        parts = [p.strip() for p in remaining.split(",") if p.strip()]
        if len(parts) >= 2:
            street = ", ".join(parts[:-1])
            city = parts[-1]
        elif parts:
            street, city = parts[0], None
        else:
            street, city = remaining or None, None

        return street, city, state, zipcode

    def _regex_multiline_reconstruct(self, text):
        """
        Extra method: if the cleaned text still has 'line-like' structure
        (segments that look like  street | city-state-zip ), handle it.
        """
        parts = [p.strip() for p in text.split(",") if p.strip()]
        if len(parts) < 2:
            return None

        # Try treating the last part as city-state-zip
        last = parts[-1]
        m = self.re_city_state_zip.match(last)
        if m and m.group(2) in self.us_states:
            street = ", ".join(parts[:-1])
            return street, m.group(1).strip(), m.group(2), m.group(3)

        # Try last part as state-zip, second-to-last as city
        m = self.re_state_zip.match(last)
        if m and m.group(1) in self.us_states and len(parts) >= 3:
            city = parts[-2]
            street = ", ".join(parts[:-2])
            return street, city, m.group(1), m.group(2)

        return None

    def _method_fallback(self, text):
        """Last resort — return text + whatever state/zip we can find."""
        zm = self.re_zip_anywhere.search(text)
        zipcode = zm.group(1) if zm else None

        state = None
        for m in self.re_state_word.finditer(text):
            if m.group(1) in self.us_states:
                state = m.group(1)
                break

        return text, None, state, zipcode

    # ─────────────────────────────────────────────────────
    #  HELPERS
    # ─────────────────────────────────────────────────────
    def _find_street_end(self, text):
        """Heuristic: index where the street portion likely ends."""
        last_pos = 0
        for m in self.re_street_suffix.finditer(text):
            last_pos = max(last_pos, m.end())
        um = self.re_has_unit.search(text)
        if um:
            last_pos = max(last_pos, um.end())
        return last_pos

    # ─────────────────────────────────────────────────────
    #  VALIDATION  &  POST-PROCESSING
    # ─────────────────────────────────────────────────────
    def _validate_state(self, state):
        if not state:
            return None
        s = state.strip().upper()
        return s if s in self.us_states else None

    def _validate_zip(self, zipcode):
        if not zipcode:
            return None
        z = str(zipcode).strip()
        return z if re.match(r"^\d{5}(-\d{4})?$", z) else None

    def _validate_result(self, street, city, state, zipcode):
        """Post-validation & cross-component cleanup."""
        state = self._validate_state(state)
        zipcode = self._validate_zip(zipcode)

        # Remove state/zip that leaked into street
        if street and state:
            street = re.sub(r"\b" + re.escape(state) + r"\s*$", "", street).strip(" ,")
        if street and zipcode:
            street = street.replace(zipcode, "").strip(" ,")

        # Remove state/zip that leaked into city
        if city and state:
            city = re.sub(r"\b" + re.escape(state) + r"\s*$", "", city).strip(" ,")
        if city and zipcode:
            city = city.replace(zipcode, "").strip(" ,")

        # City that is actually a state abbreviation
        if city and city.upper() in self.us_states and not state:
            state = city.upper()
            city = None

        # City that is actually a zip code
        if city and re.match(r"^\d{5}(-\d{4})?$", city):
            zipcode = zipcode or city
            city = None

        # Strip noise from city
        if city:
            cl = city.lower()
            if any(kw in cl for kw in self.noise_keywords):
                city = None

        # Empty checks
        if street and not street.strip():
            street = None
        if city and not city.strip():
            city = None

        return street, city, state, zipcode

    # ─────────────────────────────────────────────────────
    #  SCORING
    # ─────────────────────────────────────────────────────
    @staticmethod
    def _score_result(street, city, state, zipcode):
        """
        Score a parse result so we can pick the best across methods.
        Max theoretical score ≈ 10.
        """
        score = 0
        if street:
            score += 3
            if re.search(r"\d", street):  # has house number
                score += 1
        if city:
            score += 2
        if state:
            score += 2
        if zipcode:
            score += 1
        return score

    PERFECT_SCORE = 9  # all components + number in street

    # ─────────────────────────────────────────────────────
    #  MASTER PARSE ORCHESTRATOR
    # ─────────────────────────────────────────────────────
    def parse_address(self, address, row_num=None):
        """
        Cascade through 12 methods.
        Returns (street, city, state, zipcode, method_name).
        """
        cleaned = self.clean_address(address)
        if not cleaned:
            return None, None, None, None, "EMPTY"

        methods = [
            ("USADDRESS_TAG", self._usaddress_tag),
            ("USADDRESS_PARSE", self._usaddress_parse),
            ("REGEX_STANDARD", self._regex_standard),
            ("COMMA_CLASSIFY", self._parse_by_comma_classification),
            ("REGEX_BACKWARD", self._regex_backwards),
            ("REGEX_CSZ_ANYWHERE", self._regex_csz_anywhere),
            ("REGEX_MULTILINE", self._regex_multiline_reconstruct),
            ("REGEX_STANDARD_NOZIP", self._regex_standard_nozip),
            ("REGEX_FULL_STATE", self._regex_full_state_name),
            ("REGEX_STATE_ONLY", self._regex_state_only),
            ("REGEX_AGGRESSIVE", self._regex_aggressive),
            ("FALLBACK", self._method_fallback),
        ]

        best_result = None
        best_method = None
        best_score = -1

        for method_name, func in methods:
            try:
                result = func(cleaned)
                if result is None:
                    continue

                street, city, state, zipcode = result
                street, city, state, zipcode = self._validate_result(
                    street, city, state, zipcode
                )

                sc = self._score_result(street, city, state, zipcode)
                if sc > best_score:
                    best_score = sc
                    best_result = (street, city, state, zipcode)
                    best_method = method_name

                # Perfect → stop early
                if sc >= self.PERFECT_SCORE:
                    break

            except Exception as exc:
                if self.debug and row_num and row_num <= 5:
                    print(Fore.RED + f"    {method_name} error: {exc}")
                continue

        if best_result:
            if self.debug and row_num and row_num <= 20:
                print(
                    f"\n  Row {row_num}: {best_method}  "
                    f"(score {best_score})"
                )
            return (*best_result, best_method)

        return cleaned, None, None, None, "FAILED_ALL_METHODS"

    # ─────────────────────────────────────────────────────
    #  COMPONENT CLEANING
    # ─────────────────────────────────────────────────────
    @staticmethod
    def clean_component(value):
        if not value:
            return None
        v = str(value).strip().strip(",;:")
        v = re.sub(r"\s+", " ", v)
        return v if v else None

    # ─────────────────────────────────────────────────────
    #  STATUS  &  REASON
    # ─────────────────────────────────────────────────────
    def get_status_and_reason(
        self, address, street, city, state, zipcode, method
    ):
        if pd.isna(address) or str(address).strip() == "":
            return "Not Split", "Address cell is empty or null"

        if method == "EMPTY":
            return "Not Split", "Address empty after cleaning (only noise/org names)"

        if method == "ERROR":
            return "Not Split", "Runtime error during parsing"

        if method == "FAILED_ALL_METHODS":
            return (
                "Not Split",
                "No standard address pattern detected — all 12 methods failed",
            )

        has = {
            "Street": bool(street),
            "City": bool(city),
            "State": bool(state),
            "Zipcode": bool(zipcode),
        }

        if all(has.values()):
            return "Split", f"All components extracted ({method})"

        # ── partial — build reason ──
        missing = [k for k, v in has.items() if not v]
        missing_str = ", ".join(missing)

        hints = []
        addr_str = str(address)

        if not has["City"] and not has["State"]:
            hints.append("no City/State pattern found (missing comma separators?)")
        elif not has["City"]:
            hints.append("city boundary could not be determined")

        if not has["State"]:
            hints.append("no valid 2-letter US state abbreviation detected")

        if not has["Zipcode"]:
            hints.append("no 5-digit or 5+4 ZIP code found")

        if not has["Street"]:
            hints.append("no street number or name detected")

        if "\n" in addr_str or "\r" in addr_str:
            hints.append("multi-line format may have confused parsing")

        if addr_str.count(",") == 0:
            hints.append("no commas — hard to separate components")

        if any(kw in addr_str.lower() for kw in self.noise_keywords):
            hints.append("org/medical keywords mixed with address")

        if len(addr_str.strip()) < 10:
            hints.append("address text too short")

        hint_str = "; ".join(hints) if hints else "partial match"
        return "Not Split", f"Missing: [{missing_str}] — {hint_str}"

    # ─────────────────────────────────────────────────────
    #  BACKUP  /  PROGRESS
    # ─────────────────────────────────────────────────────
    def create_backup(self):
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = os.path.join(self.backup_folder, f"backup_{ts}.xlsx")
            shutil.copy2(self.input_file, dest)
            print(Fore.GREEN + f"  ✓ Backup: {dest}")
        except Exception as e:
            print(Fore.YELLOW + f"  ⚠ Backup failed: {e}")

    def save_progress(self, idx, df):
        data = {
            "last_processed_index": idx,
            "timestamp": datetime.now().isoformat(),
            "input_file": self.input_file,
            "stats": self.stats,
        }
        with open(self.progress_file, "w") as f:
            json.dump(data, f, indent=4)
        try:
            df.to_excel(self.output_file, index=False)
        except Exception:
            pass

    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file) as f:
                    data = json.load(f)
                if data.get("input_file") == self.input_file:
                    last = data["last_processed_index"]
                    print(
                        Fore.YELLOW
                        + f"\n  ⚡ Previous session found — last row {last}"
                    )
                    resp = input(Fore.CYAN + "  Resume? (y/n): ").strip().lower()
                    if resp == "y":
                        self.stats = data.get("stats", self.stats)
                        return last
            except Exception:
                pass
        return 0

    # ─────────────────────────────────────────────────────
    #  MAIN PROCESSING PIPELINE
    # ─────────────────────────────────────────────────────
    def process(self):

        print("\n" + "=" * 90)
        title = "  🔍 ULTRA ADDRESS PARSER v2.0 — usaddress + Comma Classification  "
        print(Fore.CYAN + Style.BRIGHT + title.center(90))
        print("=" * 90)

        # usaddress check
        if HAS_USADDRESS:
            print(Fore.GREEN + "\n  ✓ usaddress library loaded")
        else:
            print(
                Fore.RED
                + "\n  ✗ usaddress NOT installed  →  pip install usaddress"
            )
            print(Fore.YELLOW + "    Falling back to regex + comma-classify only\n")

        # ── Step 1: verify input ──
        print(Fore.CYAN + "\n📂 STEP 1 — Verify Input")
        if not os.path.exists(self.input_file):
            print(Fore.RED + f"  ✗ Not found: {self.input_file}")
            return None
        print(Fore.GREEN + f"  ✓ Found: {self.input_file}")

        # ── Step 2: backup ──
        print(Fore.CYAN + "\n💾 STEP 2 — Backup")
        self.create_backup()

        # ── Step 3: load ──
        print(Fore.CYAN + "\n📊 STEP 3 — Load Excel")
        try:
            df = pd.read_excel(self.input_file)
            print(Fore.GREEN + f"  ✓ {len(df)} rows × {len(df.columns)} cols")
        except Exception as e:
            print(Fore.RED + f"  ✗ {e}")
            return None

        # ── Step 4: locate column ──
        print(Fore.CYAN + f"\n🔍 STEP 4 — Locate '{self.address_column}'")
        if self.address_column not in df.columns:
            print(Fore.RED + f"  ✗ '{self.address_column}' not found")
            print(Fore.YELLOW + f"  Available: {list(df.columns)}")
            addr_cols = [c for c in df.columns if "address" in c.lower()]
            if addr_cols:
                self.address_column = addr_cols[0]
                print(Fore.GREEN + f"  ✓ Auto-selected: '{self.address_column}'")
            else:
                return None
        else:
            print(Fore.GREEN + "  ✓ Column found")

        # ── Step 5: resume check ──
        print(Fore.CYAN + "\n⚡ STEP 5 — Resume Check")
        start_index = self.load_progress()

        # ── Init output columns ──
        for col in [
            "Street", "City", "State", "Zipcode",
            "Parse_Method", "Status", "Reason",
        ]:
            if col not in df.columns:
                df[col] = None

        # ── Step 6: parse ──
        print(Fore.CYAN + "\n🔄 STEP 6 — Parse Addresses")
        if self.debug:
            print(Fore.YELLOW + "  Debug ON — first 20 rows traced\n")

        self.stats["total_rows"] = len(df)

        with tqdm(
            total=len(df) - start_index,
            desc="  Parsing",
            unit="addr",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ) as pbar:

            for idx in range(start_index, len(df)):
                try:
                    address = df.loc[idx, self.address_column]

                    street, city, state, zipcode, method = self.parse_address(
                        address, idx + 1
                    )

                    street = self.clean_component(street)
                    city = self.clean_component(city)
                    state = self.clean_component(state)
                    zipcode = self.clean_component(zipcode)

                    status, reason = self.get_status_and_reason(
                        address, street, city, state, zipcode, method
                    )

                    df.loc[idx, "Street"] = street
                    df.loc[idx, "City"] = city
                    df.loc[idx, "State"] = state
                    df.loc[idx, "Zipcode"] = zipcode
                    df.loc[idx, "Parse_Method"] = method
                    df.loc[idx, "Status"] = status
                    df.loc[idx, "Reason"] = reason

                    # stats
                    self.stats["processed"] += 1
                    if not address or str(address).strip() == "":
                        self.stats["empty"] += 1
                    elif status == "Split":
                        self.stats["fully_parsed"] += 1
                    elif street or city or state or zipcode:
                        self.stats["partially_parsed"] += 1
                    else:
                        self.stats["failed"] += 1

                    if (idx + 1) % 100 == 0:
                        self.save_progress(idx, df)

                    pbar.update(1)

                except KeyboardInterrupt:
                    print(Fore.YELLOW + "\n\n  ⚠ Interrupted — saving progress…")
                    self.save_progress(idx, df)
                    return df

                except Exception as e:
                    df.loc[idx, "Street"] = (
                        str(address) if not pd.isna(address) else None
                    )
                    df.loc[idx, "Parse_Method"] = "ERROR"
                    df.loc[idx, "Status"] = "Not Split"
                    df.loc[idx, "Reason"] = f"Runtime error: {e}"
                    self.stats["failed"] += 1
                    pbar.update(1)

        # ── Step 7: save ──
        print(Fore.CYAN + "\n💾 STEP 7 — Save Output")
        try:
            df.to_excel(self.output_file, index=False)
            print(Fore.GREEN + f"  ✓ Saved: {self.output_file}")
        except Exception as e:
            print(Fore.RED + f"  ✗ Excel error: {e}")
            csv = self.output_file.replace(".xlsx", ".csv")
            df.to_csv(csv, index=False)
            print(Fore.YELLOW + f"  ⚠ Saved CSV: {csv}")

        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)

        self.display_results(df)
        return df

    # ─────────────────────────────────────────────────────
    #  RESULTS DISPLAY
    # ─────────────────────────────────────────────────────
    def display_results(self, df):

        print("\n" + "=" * 90)
        print(Fore.GREEN + Style.BRIGHT + "  ✓ PARSING COMPLETE  ".center(90))
        print("=" * 90)

        split_ct = len(df[df["Status"] == "Split"])
        not_split_ct = len(df[df["Status"] == "Not Split"])

        print(Fore.CYAN + "\n📊 STATISTICS:")
        print(f"   Total Rows:       {self.stats['total_rows']}")
        print(f"   Processed:        {self.stats['processed']}")
        print(Fore.GREEN + f"   ✓ Fully Split:    {split_ct}")
        print(Fore.YELLOW + f"   ⚠ Partial:        {self.stats['partially_parsed']}")
        print(Fore.RED + f"   ✗ Not Split:      {not_split_ct}")
        print(Fore.YELLOW + f"   ⊝ Empty:          {self.stats['empty']}")

        if self.stats["processed"] > 0:
            rate = split_ct / self.stats["processed"] * 100
            print(Fore.CYAN + f"\n   Success Rate:     {rate:.1f}%")

        # Method breakdown
        print(Fore.CYAN + "\n📊 METHOD BREAKDOWN:")
        print("-" * 55)
        for method, count in df["Parse_Method"].value_counts().items():
            pct = count / len(df) * 100
            tag = "🟢" if "USADDRESS" in str(method) else "⚪"
            print(f"   {tag} {str(method):<28s} {count:>6d}  ({pct:5.1f}%)")

        # Reason summary for failures
        not_split_df = df[df["Status"] == "Not Split"]
        if len(not_split_df) > 0:
            print(Fore.YELLOW + f"\n📋 NOT-SPLIT REASONS (top 15):")
            print("-" * 90)
            for reason, cnt in (
                not_split_df["Reason"].value_counts().head(15).items()
            ):
                print(Fore.YELLOW + f"   [{cnt:>4d}] {str(reason)[:80]}")

        # Comma classification summary
        print(Fore.CYAN + "\n📊 COMMA-COUNT DISTRIBUTION (original addresses):")
        print("-" * 55)
        comma_counts = df[self.address_column].apply(
            lambda x: str(x).count(",") if pd.notna(x) else -1
        )
        for n_commas, cnt in comma_counts.value_counts().sort_index().head(8).items():
            label = "no commas" if n_commas == 0 else (
                "empty/null" if n_commas == -1 else f"{n_commas} comma(s)"
            )
            print(f"   {label:<20s} {cnt:>6d} rows")

        # Sample rows
        print(Fore.CYAN + "\n📋 SAMPLE RESULTS (first 10):")
        print("-" * 90)
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            orig = str(row[self.address_column])[:65]
            color = Fore.GREEN if row["Status"] == "Split" else Fore.RED

            print(f"\n  {i + 1}. {orig}")
            print(f"     Method: {row['Parse_Method']}")
            print(color + f"     Status: {row['Status']}")
            if row["Status"] == "Not Split":
                print(Fore.RED + f"     Reason: {row['Reason']}")
            for fld in ("Street", "City", "State", "Zipcode"):
                val = row[fld]
                if val and str(val).strip():
                    print(Fore.GREEN + f"     {fld:<8s} {val}")

        print("\n" + "=" * 90)
        print(Fore.CYAN + f"\n📁 Output: {os.path.abspath(self.output_file)}")
        print("=" * 90 + "\n")


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
def main():

    print(
        Fore.CYAN
        + Style.BRIGHT
        + """
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║                                                                          ║
    ║        ULTRA-ROBUST ADDRESS PARSER  v2.0                                 ║
    ║                                                                          ║
    ║   • usaddress NLP library  (primary parser)                              ║
    ║   • Comma-segment classification engine                                  ║
    ║   • 12 cascading strategies with confidence scoring                      ║
    ║   • Per-component validation & cross-field cleanup                       ║
    ║   • Status & Reason columns in single output file                        ║
    ║   • Comma-count distribution analysis                                    ║
    ║                                                                          ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    """
    )

    input_file = input(Fore.WHITE + "  Excel file path: ").strip().strip("\"'")
    if not input_file:
        input_file = "addresses.xlsx"

    address_column = input(
        Fore.WHITE + "  Address column (default 'Address'): "
    ).strip()
    if not address_column:
        address_column = "Address"

    debug_in = input(
        Fore.WHITE + "  Debug mode? (y/n, default y): "
    ).strip().lower()
    debug = debug_in != "n"

    lib_status = "usaddress ✓" if HAS_USADDRESS else "regex-only (pip install usaddress)"

    print(Fore.CYAN + "\n" + "=" * 70)
    print(f"  Input:    {input_file}")
    print(f"  Column:   {address_column}")
    print(f"  Debug:    {debug}")
    print(f"  Library:  {lib_status}")
    print("=" * 70)

    input(Fore.GREEN + "\n  Press ENTER to start…\n")

    parser = UltraAddressParser(
        input_file=input_file,
        address_column=address_column,
        debug=debug,
    )

    result = parser.process()

    if result is not None:
        print(Fore.GREEN + Style.BRIGHT + "\n  🎉 DONE! 🎉\n")
    else:
        print(Fore.RED + "\n  ❌ FAILED\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n\n  ⚠ Terminated by user")
    except Exception as e:
        print(Fore.RED + f"\n  ❌ FATAL: {e}")
        import traceback

        traceback.print_exc()
    finally:
        input(Fore.CYAN + "\n  Press ENTER to exit…")