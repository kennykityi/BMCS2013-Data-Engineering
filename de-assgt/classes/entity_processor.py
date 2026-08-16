"""
--------------------------
Author: Kenny Loh Kit Yi |
--------------------------

"""

import requests

class EntityProcessor:
    @staticmethod
    def get_entity_summaries(titles):
        url = "https://ms.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "format": "json",
            "titles": "|".join(titles),
            "prop": "categories|extracts",
            "cllimit": "max",
            "exintro": True,
            "explaintext": True
        }
        response = requests.get(url, params=params)
        data = response.json()

        results = {}
        for page_id, page in data["query"]["pages"].items():
            if "missing" in page:
                continue

            # Extract categories
            categories = [cat["title"] for cat in page.get("categories", [])]

            # Define category keywords
            location_keywords = ["bandar", "kampung", "negeri", "wilayah", "daerah", "kampung", "jalan"]
            people_keywords = ["orang", "individu", "lahir", "biografi", "kelahiran"]
            organization_keywords = ["syarikat", "perusahaan", "organisasi", "korporasi"]
            product_keywords = ["produk", "jenama", "barang"]

            # Determine entity type
            is_location = any(keyword in cat.lower() for cat in categories for keyword in location_keywords)
            is_people = any(keyword in cat.lower() for cat in categories for keyword in people_keywords)
            is_organization = any(keyword in cat.lower() for cat in categories for keyword in organization_keywords)
            is_product = any(keyword in cat.lower() for cat in categories for keyword in product_keywords)

            # Extract summary
            summary = page.get("extract", "")
            first_paragraph = summary.split("\n")[0] if summary else ""

            # Assign entity type using match
            title = page["title"]
            match (is_people, is_location, is_organization, is_product):
                case (True, _, _, _):
                    results[title] = ("People", first_paragraph, categories)
                case (_, True, _, _):
                    results[title] = ("Location", first_paragraph, categories)
                case (_, _, True, _):
                    results[title] = ("Organisation", first_paragraph, categories)
                case (_, _, _, True):
                    results[title] = ("Product", first_paragraph, categories)
                case _:
                    results[title] = (None, None, None)

        return results

    @staticmethod
    def normalize_word(word):
        return word[0].upper() + word[1:].lower()

    @staticmethod
    def generate_phrases(words, max_length):
        phrases = []
        for i in range(len(words)):
            for length in range(2, max_length + 1):
                if i + length <= len(words):
                    phrase = "_".join([EntityProcessor.normalize_word(words[j]) for j in range(i, i + length)])
                    phrases.append(phrase)
        return phrases
