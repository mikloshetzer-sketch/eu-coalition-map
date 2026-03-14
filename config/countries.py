# config/countries.py

COUNTRIES = {
    # EU27
    "AT": {
        "name": "Austria",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "austria",
            "austrian",
        ],
    },
    "BE": {
        "name": "Belgium",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "belgium",
            "belgian",
        ],
    },
    "BG": {
        "name": "Bulgaria",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "bulgaria",
            "bulgarian",
        ],
    },
    "HR": {
        "name": "Croatia",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "croatia",
            "croatian",
        ],
    },
    "CY": {
        "name": "Cyprus",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "cyprus",
            "cypriot",
        ],
    },
    "CZ": {
        "name": "Czech Republic",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "czech republic",
            "czechia",
            "czech",
        ],
    },
    "DK": {
        "name": "Denmark",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "denmark",
            "danish",
        ],
    },
    "EE": {
        "name": "Estonia",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "estonia",
            "estonian",
        ],
    },
    "FI": {
        "name": "Finland",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "finland",
            "finnish",
        ],
    },
    "FR": {
        "name": "France",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "france",
            "french",
        ],
    },
    "DE": {
        "name": "Germany",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "germany",
            "german",
            "deutschland",
        ],
    },
    "GR": {
        "name": "Greece",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "greece",
            "greek",
        ],
    },
    "HU": {
        "name": "Hungary",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "hungary",
            "hungarian",
        ],
    },
    "IE": {
        "name": "Ireland",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "ireland",
            "irish",
        ],
    },
    "IT": {
        "name": "Italy",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "italy",
            "italian",
        ],
    },
    "LV": {
        "name": "Latvia",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "latvia",
            "latvian",
        ],
    },
    "LT": {
        "name": "Lithuania",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "lithuania",
            "lithuanian",
        ],
    },
    "LU": {
        "name": "Luxembourg",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "luxembourg",
            "luxembourgish",
        ],
    },
    "MT": {
        "name": "Malta",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "malta",
            "maltese",
        ],
    },
    "NL": {
        "name": "Netherlands",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "netherlands",
            "dutch",
            "the netherlands",
            "holland",
        ],
    },
    "PL": {
        "name": "Poland",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "poland",
            "polish",
        ],
    },
    "PT": {
        "name": "Portugal",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "portugal",
            "portuguese",
        ],
    },
    "RO": {
        "name": "Romania",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "romania",
            "romanian",
        ],
    },
    "SK": {
        "name": "Slovakia",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "slovakia",
            "slovak",
        ],
    },
    "SI": {
        "name": "Slovenia",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "slovenia",
            "slovenian",
        ],
    },
    "ES": {
        "name": "Spain",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "spain",
            "spanish",
        ],
    },
    "SE": {
        "name": "Sweden",
        "group": "EU",
        "priority": "core",
        "aliases": [
            "sweden",
            "swedish",
        ],
    },

    # External high-priority actors
    "US": {
        "name": "United States",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "united states",
            "united states of america",
            "usa",
            "u.s.",
            "u.s.a.",
            "american",
        ],
    },
    "GB": {
        "name": "United Kingdom",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "united kingdom",
            "uk",
            "u.k.",
            "britain",
            "great britain",
            "british",
        ],
    },
    "RU": {
        "name": "Russia",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "russia",
            "russian",
            "russian federation",
            "kremlin",
        ],
    },
    "UA": {
        "name": "Ukraine",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "ukraine",
            "ukrainian",
            "kyiv",
            "kiev",
        ],
    },
    "CN": {
        "name": "China",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "china",
            "chinese",
            "prc",
            "people's republic of china",
        ],
    },
    "TR": {
        "name": "Turkey",
        "group": "EXTERNAL",
        "priority": "high",
        "aliases": [
            "turkey",
            "turkish",
            "turkiye",
            "türkiye",
            "ankara",
        ],
    },
}


EU_COUNTRY_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR",
    "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
    "SI", "ES", "SE",
]

EXTERNAL_COUNTRY_CODES = [
    "US", "GB", "RU", "UA", "CN", "TR",
]


IGNORED_ENTITIES = [
    "eu",
    "european union",
    "e.u.",
    "europe",
    "european commission",
    "commission",
    "european council",
    "council",
    "council of the european union",
    "european parliament",
    "parliament",
    "brussels",
]
