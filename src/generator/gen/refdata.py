"""Static reference data: metros, climate classes, product lines, colorways, text fragments."""

# (city, state/region, country, lat, lon, weight, postal prefixes, area codes, climate)
METROS = [
    ("New York", "NY", "US", 40.71, -74.01, 19.5, ["100", "101", "102", "103", "104", "112", "113", "114", "070", "071"], ["212", "718", "917", "646", "201"], "northeast"),
    ("Los Angeles", "CA", "US", 34.05, -118.24, 13.0, ["900", "902", "903", "904", "905", "906", "907", "908", "910", "911", "912", "913", "914", "915", "916", "917", "918"], ["213", "310", "323", "818", "626"], "mediterranean"),
    ("Chicago", "IL", "US", 41.88, -87.63, 9.4, ["600", "601", "603", "604", "606", "607", "608"], ["312", "773", "847", "630"], "continental"),
    ("Dallas", "TX", "US", 32.78, -96.80, 7.6, ["750", "751", "752", "753", "760", "761"], ["214", "972", "469", "817"], "south_hot"),
    ("Houston", "TX", "US", 29.76, -95.37, 7.1, ["770", "771", "772", "773", "774", "775"], ["713", "281", "832"], "south_humid"),
    ("Washington", "DC", "US", 38.91, -77.04, 6.3, ["200", "201", "202", "203", "204", "205", "206", "207", "208", "209", "220", "221", "222", "223"], ["202", "703", "301", "571"], "northeast"),
    ("Philadelphia", "PA", "US", 39.95, -75.17, 6.2, ["190", "191", "080", "081"], ["215", "267", "610", "856"], "northeast"),
    ("Miami", "FL", "US", 25.76, -80.19, 6.1, ["330", "331", "332", "333", "334"], ["305", "786", "954", "561"], "south_humid"),
    ("Atlanta", "GA", "US", 33.75, -84.39, 6.1, ["300", "301", "302", "303", "311"], ["404", "770", "678"], "south_humid"),
    ("Boston", "MA", "US", 42.36, -71.06, 4.9, ["021", "022", "018", "019", "017"], ["617", "781", "978", "508"], "northeast"),
    ("Phoenix", "AZ", "US", 33.45, -112.07, 4.9, ["850", "852", "853"], ["602", "480", "623"], "desert"),
    ("San Francisco", "CA", "US", 37.77, -122.42, 4.7, ["940", "941", "944", "945", "946"], ["415", "510", "650", "925"], "mediterranean"),
    ("Detroit", "MI", "US", 42.33, -83.05, 4.3, ["480", "481", "482", "483"], ["313", "248", "586", "734"], "continental"),
    ("Seattle", "WA", "US", 47.61, -122.33, 4.0, ["980", "981", "983", "984"], ["206", "425", "253"], "pnw"),
    ("Minneapolis", "MN", "US", 44.98, -93.27, 3.7, ["550", "551", "553", "554"], ["612", "651", "952", "763"], "continental_cold"),
    ("San Diego", "CA", "US", 32.72, -117.16, 3.3, ["919", "920", "921"], ["619", "858", "760"], "mediterranean"),
    ("Tampa", "FL", "US", 27.95, -82.46, 3.2, ["335", "336", "337", "346"], ["813", "727"], "south_humid"),
    ("Denver", "CO", "US", 39.74, -104.99, 3.0, ["800", "801", "802", "803"], ["303", "720"], "mountain"),
    ("Baltimore", "MD", "US", 39.29, -76.61, 2.8, ["210", "211", "212", "214"], ["410", "443"], "northeast"),
    ("St. Louis", "MO", "US", 38.63, -90.20, 2.8, ["630", "631", "633", "620"], ["314", "636"], "continental"),
    ("Orlando", "FL", "US", 28.54, -81.38, 2.7, ["327", "328", "347"], ["407", "321"], "south_humid"),
    ("Charlotte", "NC", "US", 35.23, -80.84, 2.7, ["280", "281", "282"], ["704", "980"], "south_humid"),
    ("San Antonio", "TX", "US", 29.42, -98.49, 2.6, ["780", "781", "782"], ["210", "726"], "south_hot"),
    ("Portland", "OR", "US", 45.52, -122.68, 2.5, ["970", "971", "972", "986"], ["503", "971", "360"], "pnw"),
    ("Sacramento", "CA", "US", 38.58, -121.49, 2.4, ["956", "957", "958"], ["916", "530"], "mediterranean"),
    ("Pittsburgh", "PA", "US", 40.44, -79.99, 2.4, ["150", "151", "152"], ["412", "724"], "northeast"),
    ("Austin", "TX", "US", 30.27, -97.74, 2.4, ["786", "787", "789"], ["512", "737"], "south_hot"),
    ("Las Vegas", "NV", "US", 36.17, -115.14, 2.3, ["889", "890", "891"], ["702", "725"], "desert"),
    ("Cincinnati", "OH", "US", 39.10, -84.51, 2.3, ["450", "451", "452", "410"], ["513", "859"], "continental"),
    ("Kansas City", "MO", "US", 39.10, -94.58, 2.2, ["640", "641", "660", "661"], ["816", "913"], "continental"),
    ("Columbus", "OH", "US", 39.96, -83.00, 2.1, ["430", "431", "432"], ["614", "740"], "continental"),
    ("Indianapolis", "IN", "US", 39.77, -86.16, 2.1, ["460", "461", "462"], ["317", "463"], "continental"),
    ("Cleveland", "OH", "US", 41.50, -81.69, 2.1, ["440", "441", "442"], ["216", "440", "330"], "continental"),
    ("Nashville", "TN", "US", 36.16, -86.78, 2.0, ["370", "371", "372"], ["615", "629"], "south_humid"),
    ("Milwaukee", "WI", "US", 43.04, -87.91, 1.6, ["530", "531", "532"], ["414", "262"], "continental_cold"),
    ("Raleigh", "NC", "US", 35.78, -78.64, 1.4, ["275", "276", "277"], ["919", "984"], "south_humid"),
    ("Oklahoma City", "OK", "US", 35.47, -97.52, 1.4, ["730", "731"], ["405"], "south_hot"),
    ("Salt Lake City", "UT", "US", 40.76, -111.89, 1.3, ["840", "841"], ["801", "385"], "mountain"),
    ("New Orleans", "LA", "US", 29.95, -90.07, 1.3, ["700", "701"], ["504"], "south_humid"),
    ("Louisville", "KY", "US", 38.25, -85.76, 1.3, ["400", "401", "402"], ["502"], "continental"),
    ("Richmond", "VA", "US", 37.54, -77.44, 1.3, ["230", "231", "232"], ["804"], "northeast"),
    ("Buffalo", "NY", "US", 42.89, -78.88, 1.1, ["140", "142"], ["716"], "continental_cold"),
    ("Honolulu", "HI", "US", 21.31, -157.86, 1.0, ["967", "968"], ["808"], "tropical"),
    ("Albuquerque", "NM", "US", 35.08, -106.65, 0.9, ["870", "871"], ["505"], "desert"),
    ("Boise", "ID", "US", 43.62, -116.20, 0.8, ["836", "837"], ["208"], "mountain"),
    # UK: postal prefixes are postcode areas
    ("London", "Greater London", "UK", 51.51, -0.13, 9.5, ["E", "N", "SE", "SW", "W", "NW", "EC", "WC", "CR", "HA", "UB", "TW", "KT", "BR", "IG", "RM", "EN"], ["20"], "uk_maritime"),
    ("Birmingham", "West Midlands", "UK", 52.49, -1.89, 2.9, ["B", "WS", "WV", "CV"], ["121"], "uk_maritime"),
    ("Manchester", "Greater Manchester", "UK", 53.48, -2.24, 2.8, ["M", "SK", "OL", "BL", "WA"], ["161"], "uk_maritime"),
    ("Leeds", "West Yorkshire", "UK", 53.80, -1.55, 1.9, ["LS", "BD", "WF", "HX"], ["113"], "uk_maritime"),
    ("Glasgow", "Scotland", "UK", 55.86, -4.25, 1.7, ["G", "PA", "ML"], ["141"], "uk_maritime"),
    ("Liverpool", "Merseyside", "UK", 53.41, -2.98, 1.4, ["L", "CH", "WN"], ["151"], "uk_maritime"),
    ("Sheffield", "South Yorkshire", "UK", 53.38, -1.47, 1.3, ["S", "DN"], ["114"], "uk_maritime"),
    ("Newcastle", "Tyne and Wear", "UK", 54.98, -1.61, 1.1, ["NE", "SR", "DH"], ["191"], "uk_maritime"),
    ("Bristol", "Bristol", "UK", 51.45, -2.59, 1.0, ["BS", "BA"], ["117"], "uk_maritime"),
    ("Edinburgh", "Scotland", "UK", 55.95, -3.19, 0.9, ["EH"], ["131"], "uk_maritime"),
    # DE: postal prefixes are 2-digit leading zones
    ("Berlin", "Berlin", "DE", 52.52, 13.40, 3.7, ["10", "12", "13", "14"], ["30"], "de_continental"),
    ("Hamburg", "Hamburg", "DE", 53.55, 9.99, 1.9, ["20", "21", "22"], ["40"], "de_continental"),
    ("München", "Bayern", "DE", 48.14, 11.58, 1.5, ["80", "81", "82", "85"], ["89"], "de_continental"),
    ("Köln", "Nordrhein-Westfalen", "DE", 50.94, 6.96, 1.1, ["50", "51"], ["221"], "de_continental"),
    ("Frankfurt am Main", "Hessen", "DE", 50.11, 8.68, 0.8, ["60", "61", "63", "65"], ["69"], "de_continental"),
    ("Düsseldorf", "Nordrhein-Westfalen", "DE", 51.23, 6.77, 0.65, ["40", "41", "42"], ["211"], "de_continental"),
    ("Stuttgart", "Baden-Württemberg", "DE", 48.78, 9.18, 0.6, ["70", "71", "73"], ["711"], "de_continental"),
    ("Leipzig", "Sachsen", "DE", 51.34, 12.37, 0.6, ["04"], ["341"], "de_continental"),
    ("Dortmund", "Nordrhein-Westfalen", "DE", 51.51, 7.47, 0.6, ["44", "45"], ["231"], "de_continental"),
    ("Nürnberg", "Bayern", "DE", 49.45, 11.08, 0.52, ["90", "91"], ["911"], "de_continental"),
    # CA: postal prefixes are FSA first two chars
    ("Toronto", "ON", "CA", 43.65, -79.38, 6.2, ["M4", "M5", "M6", "L4", "L5"], ["416", "647", "905"], "canada_cold"),
    ("Montréal", "QC", "CA", 45.50, -73.57, 4.3, ["H1", "H2", "H3", "H4"], ["514", "438"], "canada_cold"),
    ("Vancouver", "BC", "CA", 49.28, -123.12, 2.6, ["V5", "V6", "V7"], ["604", "778"], "pnw"),
    ("Calgary", "AB", "CA", 51.05, -114.07, 1.5, ["T2", "T3"], ["403", "587"], "canada_cold"),
    ("Ottawa", "ON", "CA", 45.42, -75.70, 1.4, ["K1", "K2"], ["613", "343"], "canada_cold"),
    ("Edmonton", "AB", "CA", 53.55, -113.49, 1.4, ["T5", "T6"], ["780", "825"], "canada_cold"),
]

POSTAL_TARGET = {"US": 3000, "UK": 420, "DE": 380, "CA": 220}

# climate: mean C, amplitude C, rain prob winter, rain prob summer, mean rain mm, snow possible
CLIMATE = {
    "northeast": (12.5, 12.0, 0.33, 0.30, 9, True),
    "mediterranean": (17.0, 5.0, 0.25, 0.03, 10, False),
    "continental": (10.5, 14.0, 0.30, 0.32, 9, True),
    "continental_cold": (7.5, 16.0, 0.28, 0.33, 8, True),
    "south_hot": (20.5, 9.5, 0.22, 0.20, 12, False),
    "south_humid": (21.0, 7.5, 0.30, 0.42, 13, False),
    "desert": (23.5, 10.0, 0.08, 0.10, 6, False),
    "pnw": (11.5, 7.5, 0.60, 0.15, 7, False),
    "mountain": (10.5, 12.0, 0.18, 0.25, 6, True),
    "tropical": (25.5, 2.0, 0.35, 0.25, 6, False),
    "uk_maritime": (10.5, 6.5, 0.55, 0.38, 5, False),
    "de_continental": (10.0, 9.5, 0.42, 0.40, 5, True),
    "canada_cold": (6.5, 15.5, 0.33, 0.35, 7, True),
}

FLAGSHIP_CITIES = {"New York", "Los Angeles", "Chicago", "San Francisco", "Seattle", "Boston",
                   "London", "Manchester", "Berlin", "München", "Toronto", "Vancouver"}

# ---------------- products ----------------
# category code -> (division, category, sport, lines[(name, base_msrp, tech)], versions, genders, variants[(suffix, msrp_delta, weight)])
FOOTWEAR = {
    "RUN": ("Footwear", "Running", "Running",
            [("CloudStride", 140, "ForgeFoam"), ("AeroPulse", 250, "CarbonPlate"), ("TempoFlex", 130, "ForgeFoam"),
             ("StabilityMax", 150, "StableRail"), ("UltraGlide", 170, "ForgeFoam+"), ("VeloRun", 110, "ForgeFoam"),
             ("SwiftLite", 100, "LiteFoam"), ("PaceRunner", 90, "LiteFoam"), ("RoadForge", 120, "ForgeFoam"),
             ("DistanceX", 180, "ForgeFoam+")],
            3, ["Men", "Women"], [("", 0, 1.0), (" GTX", 20, 0.22), (" SE", 10, 0.3), (" Pro", 40, 0.18)]),
    "TRL": ("Footwear", "Trail", "Trail Running",
            [("Ridgeline", 150, "GripTrax"), ("Summit", 170, "GripTrax"), ("TrailBlaze", 130, "GripTrax"),
             ("Crestline", 160, "GripTrax"), ("Switchback", 120, "GripTrax")],
            3, ["Men", "Women"], [("", 0, 1.0), (" GTX", 25, 0.55), (" Mid", 15, 0.25)]),
    "BBL": ("Footwear", "Basketball", "Basketball",
            [("Mercer", 180, "BounceCore"), ("Skyhook", 140, "BounceCore"), ("Courtvision", 120, "BounceCore"),
             ("Apex Rise", 160, "BounceCore"), ("Hardwood", 100, "BounceCore")],
            4, ["Men", "Women"], [("", 0, 1.0), (" Low", -10, 0.4), (" TB", 0, 0.12)]),
    "TRN": ("Footwear", "Training", "Training",
            [("CrossCore", 120, "StableBase"), ("Liftbase", 140, "StableBase"), ("HIIT Flex", 100, "LiteFoam"),
             ("Gym Forge", 110, "StableBase"), ("Power Stack", 130, "StableBase")],
            3, ["Men", "Women"], [("", 0, 1.0), (" Wide", 0, 0.15), (" SE", 10, 0.3)]),
    "LIF": ("Footwear", "Lifestyle", "Lifestyle",
            [("Retro 87", 100, "Classic"), ("Street Classic", 90, "Classic"), ("Terrace", 110, "Classic"),
             ("Volta", 100, "Classic"), ("Forge Low", 120, "Classic"), ("Court Heritage", 95, "Classic"),
             ("Runner 72", 110, "Classic"), ("Campus Club", 100, "Classic")],
            1, ["Men", "Women", "Unisex"], [("", 0, 1.0), (" Premium", 30, 0.35), (" Suede", 20, 0.4),
                                             (" Leather", 15, 0.35), (" Mid", 10, 0.2), (" Vintage", 20, 0.25)]),
    "FTB": ("Footwear", "Football", "Football",
            [("StrikeForce", 220, "ControlSkin"), ("Velocity Elite", 260, "SpeedFrame"),
             ("Pitch Master", 120, "ControlSkin"), ("Copa Forge", 150, "Leather")],
            2, ["Men", "Women"], [(" FG", 0, 1.0), (" AG", 0, 0.35), (" TF", -40, 0.45), (" IN", -50, 0.3)]),
    "KDF": ("Footwear", "Kids Footwear", "Multi-Sport",
            [("CloudStride Kids", 70, "ForgeFoam"), ("Skyhook Kids", 75, "BounceCore"), ("Retro 87 Kids", 60, "Classic"),
             ("Street Classic Kids", 55, "Classic"), ("StrikeForce Kids", 65, "ControlSkin"),
             ("Volta Kids", 60, "Classic"), ("TrailBlaze Kids", 70, "GripTrax"), ("HIIT Flex Kids", 55, "LiteFoam"),
             ("Mercer Kids", 85, "BounceCore"), ("Campus Club Kids", 60, "Classic")],
            2, ["Kids"], [(" Big Kids", 10, 1.0), (" Little Kids", 0, 0.7), (" Toddler", -10, 0.4)]),
}

APPAREL = [  # (name, sub_category, sport, base_msrp)
    ("Run Tech Tee", "Tops", "Running", 35), ("Running Singlet", "Tops", "Running", 30),
    ("Training Tank", "Tops", "Training", 28), ("Essential Hoodie", "Tops", "Lifestyle", 65),
    ("Track Jacket", "Outerwear", "Lifestyle", 80), ("Quarter-Zip Run Top", "Tops", "Running", 60),
    ("Graphic Tee", "Tops", "Lifestyle", 30), ("Club Jersey", "Tops", "Football", 90),
    ("Run Shorts 5in", "Bottoms", "Running", 40), ("Run Shorts 7in", "Bottoms", "Running", 40),
    ("Running Tights", "Bottoms", "Running", 65), ("Joggers", "Bottoms", "Lifestyle", 60),
    ("Track Pants", "Bottoms", "Lifestyle", 65), ("Training Shorts", "Bottoms", "Training", 35),
    ("Leggings", "Bottoms", "Training", 60), ("Rain Shell", "Outerwear", "Running", 120),
    ("Puffer Jacket", "Outerwear", "Lifestyle", 180), ("Windbreaker", "Outerwear", "Running", 85),
    ("Trail Shell GTX", "Outerwear", "Trail Running", 220), ("Thermal Long Sleeve", "Base Layers", "Running", 55),
    ("Base Layer Tights", "Base Layers", "Training", 50), ("Basketball Shorts", "Bottoms", "Basketball", 45),
]
APPAREL_VARIANTS = ["", " Pro", " Essentials", " Seamless", " Heritage"]

ACCESSORIES = [  # (name, sub_category, base_msrp, sized)
    ("Cushion Crew Socks 3-Pack", "Socks", 20, True), ("No-Show Run Socks 3-Pack", "Socks", 18, True),
    ("Trail Socks", "Socks", 18, True), ("Compression Socks", "Socks", 28, True),
    ("Run Cap", "Headwear", 28, False), ("Dad Hat", "Headwear", 25, False), ("Beanie", "Headwear", 25, False),
    ("Gym Duffel", "Bags", 55, False), ("Backpack 25L", "Bags", 65, False), ("Hydration Vest", "Bags", 90, False),
    ("Water Bottle 750ml", "Equipment", 15, False), ("Headband 3-Pack", "Equipment", 12, False),
    ("Run Gloves", "Equipment", 25, True), ("Shin Guards", "Equipment", 30, True),
]
ACCESSORY_VARIANTS = ["", " Pro", " Essentials"]

COLLAB_PARTNERS = ["Kazuki Studio", "Harlow & Vine", "Neon Archive", "J. Mercer", "Atelier Noor",
                   "Club Rotterdam", "Grayline Supply", "Maison Lumière"]

COLORWAYS = [
    ("Core Black / Cloud White", "Black"), ("Cloud White / Core Black", "White"), ("Triple Black", "Black"),
    ("Triple White", "White"), ("Grey Five / Solar Orange", "Grey"), ("Collegiate Navy / Gold", "Navy"),
    ("Solar Red / Night Navy", "Red"), ("Volt / Carbon", "Yellow"), ("Pulse Lime / Grey", "Green"),
    ("Wonder Beige / Gum", "Beige"), ("Shadow Olive / Sand", "Green"), ("Lucid Blue / Silver", "Blue"),
    ("Pink Spark / White", "Pink"), ("Royal Blue / White", "Blue"), ("Burgundy / Cream", "Red"),
    ("Clay Brown / Off White", "Brown"), ("Halo Silver / Black", "Grey"), ("Turbo Orange / Black", "Orange"),
    ("Ice Mint / White", "Green"), ("Night Indigo / Coral", "Purple"),
]
APPAREL_COLORS = [("Black", "Black"), ("White", "White"), ("Heather Grey", "Grey"), ("Navy", "Navy"),
                  ("Olive", "Green"), ("Burgundy", "Red"), ("Royal Blue", "Blue"), ("Sand", "Beige")]

MEN_SIZES = [x / 2 for x in range(8, 29)] + [15.0]          # 4.0 .. 14.0, 15
WOMEN_SIZES = [x / 2 for x in range(10, 25)]                 # 5.0 .. 12.0
KID_SIZES = ["10C", "10.5C", "11C", "11.5C", "12C", "12.5C", "13C", "13.5C", "1Y", "1.5Y", "2Y", "2.5Y",
             "3Y", "3.5Y", "4Y", "4.5Y", "5Y", "5.5Y", "6Y", "6.5Y", "7Y"]
APPAREL_SIZES = ["XS", "S", "M", "L", "XL", "XXL"]
KID_APPAREL_SIZES = ["YXS", "YS", "YM", "YL", "YXL"]
SOCK_SIZES = ["S", "M", "L", "XL"]

# ---------------- text fragments ----------------
NICKNAMES = {"Robert": "Bob", "William": "Bill", "Elizabeth": "Liz", "Katherine": "Kate", "Michael": "Mike",
             "Jennifer": "Jen", "Christopher": "Chris", "Matthew": "Matt", "Joseph": "Joe", "Daniel": "Dan",
             "Jessica": "Jess", "Thomas": "Tom", "Anthony": "Tony", "Alexander": "Alex", "Samantha": "Sam",
             "Nicholas": "Nick", "Rebecca": "Becky", "Benjamin": "Ben", "Margaret": "Maggie", "James": "Jim",
             "Richard": "Rick", "Patricia": "Pat", "Stephanie": "Steph", "Andrew": "Andy", "Jonathan": "Jon"}

EMAIL_DOMAINS = {
    "US": [("gmail.com", 0.45), ("yahoo.com", 0.13), ("outlook.com", 0.09), ("icloud.com", 0.12),
           ("hotmail.com", 0.08), ("aol.com", 0.03), ("comcast.net", 0.03), ("proton.me", 0.02), ("att.net", 0.05)],
    "CA": [("gmail.com", 0.48), ("hotmail.com", 0.14), ("outlook.com", 0.1), ("yahoo.ca", 0.08),
           ("icloud.com", 0.1), ("shaw.ca", 0.05), ("rogers.com", 0.05)],
    "UK": [("gmail.com", 0.42), ("hotmail.co.uk", 0.15), ("outlook.com", 0.1), ("yahoo.co.uk", 0.08),
           ("icloud.com", 0.1), ("btinternet.com", 0.08), ("sky.com", 0.07)],
    "DE": [("gmail.com", 0.3), ("gmx.de", 0.22), ("web.de", 0.2), ("t-online.de", 0.1), ("outlook.de", 0.06),
           ("icloud.com", 0.07), ("yahoo.de", 0.05)],
}
DOMAIN_TYPOS = {"gmail.com": "gmial.com", "yahoo.com": "yaho.com", "hotmail.com": "hotmial.com",
                "outlook.com": "outlok.com", "icloud.com": "icloud.co", "gmx.de": "gmx.d", "web.de": "wbe.de"}

STREET_ABBR = [(" Street", " St"), (" Avenue", " Ave"), (" Road", " Rd"), (" Drive", " Dr"),
               (" Boulevard", " Blvd"), (" Lane", " Ln"), (" Court", " Ct"), ("straße", "str.")]

SEARCH_TERMS = {
    "Running": ["running shoes", "cloudstride 3", "cloudstride", "marathon shoes", "carbon plate shoes",
                "aeropulse", "stability running shoes", "wide running shoes", "running shoes women", "tempoflex"],
    "Trail Running": ["trail running shoes", "waterproof trail shoes", "ridgeline gtx", "summit gtx", "hiking shoes"],
    "Basketball": ["basketball shoes", "mercer 4", "skyhook", "low top basketball", "court shoes"],
    "Training": ["gym shoes", "crosscore", "lifting shoes", "training shoes", "hiit shoes"],
    "Lifestyle": ["retro 87", "white sneakers", "suede sneakers", "terrace", "volta", "collab drop"],
    "Football": ["football boots", "strikeforce fg", "astro turf boots", "indoor football shoes"],
    "Apparel": ["running shorts", "hoodie", "rain jacket", "running tights", "joggers", "track jacket", "puffer jacket"],
    "Accessories": ["running socks", "cap", "backpack", "water bottle"],
}
