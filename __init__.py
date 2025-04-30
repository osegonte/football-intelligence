__version__ = '0.1.0'
__author__ = 'osegonte'

# Make important modules available at package level
from scrapers.sofascore_scraper import AdvancedSofaScoreScraper
from scrapers.fbref_scraper import FBrefScraper
from scrapers.scraper_utils import print_match_statistics

# Define what's available when using "from football_intelligence import *"
__all__ = [
    'AdvancedSofaScoreScraper',
    'FBrefScraper',
    'print_match_statistics'
]